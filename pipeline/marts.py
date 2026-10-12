"""lake 날짜 테이블 전체에서 상품 이력(dim_product, SCD2)과 마트 두 개를 다시 계산한다. 설계는 ADR 0007 결정 3·4·6.

순서: 입력 스냅숏 고정 → 신선도 관문 → 원하는 dim 계산·검사 1~4 → 차이만 MERGE → 검사 5(대조) → 퍼널 → 코호트.
성공하든 실패하든 마지막에 모든 lake 테이블의 옛 스냅숏을 만료한다.

    python -m pipeline.marts --warehouse data/warehouse/events
"""
import argparse
import os
from datetime import date, datetime, timezone

from pyspark.sql import Window, functions as F

from pipeline.lake import NS, TABLES, overwrite
from pipeline.model_daily import input_hash
from pipeline.spark import get_spark

FOREVER = date(9999, 12, 31)  # 현재 버전의 valid_to
TRACKED = ["category_id", "category_code", "brand"]  # 하나라도 바뀌면 새 버전. price는 사실(events)에 있다(결정 3)
DIM_COLS = ["product_sk", "product_id", "category_id", "category_code", "category_l1", "brand",
            "valid_from", "valid_to", "is_current"]


def current_snapshot(spark, table: str):
    """main 브랜치가 가리키는 스냅숏 id. 커밋이 한 번도 없으면 None."""
    r = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()
    return r[0] if r else None


def pinned(spark, table: str):
    # 관문과 이후의 모든 계산이 같은 시점을 읽는다. 검사한 뒤 읽기 전에 lake_daily 커밋이 끼어드는 틈을 없앤다.
    sid = current_snapshot(spark, table)
    if sid is None:
        raise ValueError(f"{table}이 비어 있다. lake_daily를 먼저 돌린다")
    return sid, spark.sql(f"SELECT * FROM {table} VERSION AS OF {sid}")


def stale_dates(warehouse_root: str, sessions, products) -> list[date]:
    """신선도 관문(결정 6). lake가 지금의 events 폴더를 그대로 반영하지 않는 날짜 목록.

    날짜마다 두 테이블에 모두 있고, src_build 값이 하나이며 서로 같고, 그 입력 해시가 지금 폴더의 파일 이름 해시와 같아야 한다.
    시각은 비교하지 않는다. 만료가 파티션의 갱신 시각을 지우고, events와 lake의 시계도 서로 다르다.
    """
    want = {}
    for name in os.listdir(warehouse_root):
        if name.startswith("dt="):
            d = date.fromisoformat(name[3:])
            want[d] = input_hash(warehouse_root, d)

    def builds(df):
        return {r.dt: r.b for r in df.groupBy("dt").agg(F.collect_set("src_build").alias("b")).collect()}

    s, p = builds(sessions), builds(products)
    return sorted(d for d in want.keys() | s.keys() | p.keys()
                  if not (d in want and len(s.get(d, ())) == 1 and s[d] == p.get(d)
                          and s[d][0].startswith(want[d] + "-")))


def spans(ds: list[date]) -> str:
    """연속한 날짜를 'a~b'로 묶는다. 다시 돌릴 백필 범위를 바로 읽을 수 있게."""
    out, start = [], ds[0]
    for a, b in zip(ds, ds[1:] + [None]):
        if b is None or (b - a).days != 1:
            out.append(f"{start}~{a}" if start != a else str(a))
            start = b
    return ", ".join(out)


def desired_dim(products):
    """원하는 dim. product_daily 전체의 순수 함수다(gaps & islands). 추적 속성이 앞 관측일과 다르면 새 버전을 연다."""
    w = Window.partitionBy("product_id").orderBy("dt")
    # brand의 null은 '그날 관측 못 함'이라 마지막 값을 이어 쓴다. 그래서 값→null은 변화가 아니고 null→값은 새 버전이다.
    # 과거로 거슬러 채우지는 않는다. 원천이 몰랐던 기간까지 브랜드를 알았던 것처럼 되기 때문이다.
    ff = products.withColumn("brand", F.last("brand", ignorenulls=True).over(
        w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    start = F.lag("dt").over(w).isNull()  # 그 상품의 첫 관측일
    for c in TRACKED:
        start = start | ~F.col(c).eqNullSafe(F.lag(c).over(w))  # null도 하나의 값('미분류')으로 비교한다
    # 관측이 없는 날은 앞 버전이 덮는다. 다음 버전의 첫날이 이 버전의 끝(그날은 포함하지 않음)이다.
    valid_to = F.coalesce(F.lead("dt").over(w), F.lit(FOREVER))
    return (ff.withColumn("is_start", start).filter("is_start").select(
        # 속성 해시가 아니라 시작일로 키를 만든다. 값이 되돌아가도(acer→lenovo→acer) 두 버전이 다른 키를 갖는다.
        F.xxhash64("product_id", "dt").alias("product_sk"), "product_id", "category_id", "category_code",
        F.substring_index("category_code", ".", 1).alias("category_l1"), "brand",
        F.col("dt").alias("valid_from"), valid_to.alias("valid_to"), (valid_to == F.lit(FOREVER)).alias("is_current")))


def check_dim(want, products) -> list[str]:
    """MERGE 전 검사 1~4(결정 6). 실패 메시지 목록을 돌려준다."""
    failed = []
    n, n_sk = want.agg(F.count("*"), F.count_distinct("product_sk")).first()
    if n != n_sk:
        failed.append(f"product_sk가 유일하지 않다: 행 {n:,}, 고유 키 {n_sk:,}(검사 1)")
    w = Window.partitionBy("product_id").orderBy("valid_from")
    same_as_prev = F.lit(True)
    for c in TRACKED:
        same_as_prev = same_as_prev & F.col(c).eqNullSafe(F.lag(c).over(w))
    r = (want.withColumn("same_as_prev", same_as_prev)
         .withColumn("n_cur", F.count_if("is_current").over(Window.partitionBy("product_id")))
         .agg(F.count_if(F.col("n_cur") != 1).alias("cur"),
              F.count_if(F.col("is_current") != (F.col("valid_to") == F.lit(FOREVER))).alias("cur_flag"),
              F.count_if(F.col("valid_from") >= F.col("valid_to")).alias("empty"),
              F.count_if("same_as_prev").alias("same")).first())
    if r.cur or r.cur_flag:
        failed.append(f"현재 버전이 상품마다 하나가 아니거나 valid_to와 어긋난다: {r.cur + r.cur_flag:,}행(검사 2)")
    if r.empty or r.same:
        failed.append(f"빈 기간 {r.empty:,}행, 앞 버전과 속성이 같은 버전 {r.same:,}행(검사 3)")
    # as-of 대조: product_daily의 모든 (dt, 상품)이 버전 하나에만 맞고, 카테고리와 (관측했다면) brand가 같아야 한다
    p, d = products.alias("p"), want.alias("d")
    j = p.join(d, (F.col("p.product_id") == F.col("d.product_id")) & (F.col("p.dt") >= F.col("d.valid_from"))
               & (F.col("p.dt") < F.col("d.valid_to")), "left")
    r = j.agg(
        F.count_if(F.col("d.product_sk").isNull()).alias("miss"),
        F.count_if(F.col("d.product_sk").isNotNull()).alias("hit"),
        F.count_if(~F.col("p.category_id").eqNullSafe(F.col("d.category_id"))
                   | ~F.col("p.category_code").eqNullSafe(F.col("d.category_code"))).alias("cat"),
        F.count_if(F.col("p.brand").isNotNull() & ~F.col("p.brand").eqNullSafe(F.col("d.brand"))).alias("brand"),
    ).first()
    n_p = products.count()
    if r.miss or r.hit != n_p or r.cat or r.brand:
        failed.append(f"as-of 대조: 버전 없음 {r.miss:,}, 맞은 수 {r.hit:,}/{n_p:,}, "
                      f"카테고리 불일치 {r.cat:,}, brand 불일치 {r.brand:,}(검사 4)")
    return failed


def merge_diff(spark, want, table: str) -> dict:
    """원하는 상태와 지금 dim의 차이만 MERGE 한 번으로 반영한다. 차이가 없으면 커밋하지 않는다(멱등성의 증거)."""
    s, t = want.alias("s"), spark.table(table).alias("t")
    same = F.lit(True)
    for c in DIM_COLS[1:]:
        same = same & F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
    op = F.when(F.col("t.product_sk").isNull(), "I").when(F.col("s.product_sk").isNull(), "D").when(~same, "U")
    changes = (s.join(t, F.col("s.product_sk") == F.col("t.product_sk"), "full_outer")
               .select(F.coalesce("s.product_sk", "t.product_sk").alias("product_sk"), op.alias("op"),
                       *[F.col(f"s.{c}").alias(c) for c in DIM_COLS[1:]])
               .filter("op IS NOT NULL")
               .localCheckpoint())  # 센 숫자와 MERGE가 같은 차이를 본다
    n = {r.op: r["count"] for r in changes.groupBy("op").count().collect()}
    if n:
        changes.createOrReplaceTempView("dim_changes")
        # 모든 절에 op 조건을 붙인다. 조건 없는 절이 있으면 차이 계산의 버그(예: 지울 행)가 INSERT로 조용히 반영된다.
        # 조건이 있으면 그런 행은 아무 일도 하지 않고, 검사 5가 불일치로 잡는다.
        spark.sql(f"""
            MERGE INTO {table} t USING dim_changes s ON t.product_sk = s.product_sk
            WHEN MATCHED AND s.op = 'D' THEN DELETE
            WHEN MATCHED AND s.op = 'U' THEN UPDATE SET {", ".join(f"{c} = s.{c}" for c in DIM_COLS[1:])}
            WHEN NOT MATCHED AND s.op = 'I' THEN INSERT ({", ".join(DIM_COLS)})
              VALUES ({", ".join(f"s.{c}" for c in DIM_COLS)})""")
    return {"inserted": n.get("I", 0), "updated": n.get("U", 0), "deleted": n.get("D", 0)}


def same_rows(a, b) -> bool:
    a, b = a.select(DIM_COLS), b.select(DIM_COLS)
    return a.exceptAll(b).isEmpty() and b.exceptAll(a).isEmpty()


def funnel(sessions):
    """날짜별 세션 단위 존재 기준 퍼널(결정 4). 그 유형의 이벤트가 세션 안에 있는가만 보고, 순서와 상품은 보지 않는다.

    비율은 저장하지 않는다. 날짜를 합칠 때 비율의 평균을 내는 실수를 부른다.
    """
    v, c, p = (F.col(f"n_{t}") > 0 for t in ("view", "cart", "purchase"))
    return sessions.groupBy("dt").agg(
        F.count("*").alias("sessions"),
        F.count_if(v).alias("s_view"),
        F.count_if(v & c).alias("s_view_cart"),
        F.count_if(v & c & p).alias("s_view_cart_purchase"),
        F.count_if(p).alias("s_purchase"),
        F.count_if(p & ~c).alias("s_purchase_no_cart"),
        F.count_distinct("user_id").alias("users"),
        F.count_distinct(F.when(p, F.col("user_id"))).alias("buyers"),
        F.sum("n_cart").alias("cart_events"),
        F.sum("n_purchase").alias("purchase_events"),
        F.coalesce(F.sum("purchase_amount"), F.lit(0)).alias("revenue"),
    )


def cohort(sessions):
    """첫 세션이 있었던 달(UTC) 코호트 × 활동 달(결정 4).

    데이터 첫 달의 코호트에는 그 전부터 오던 사용자가 섞여 있다(좌측 절단). 행은 남기고 표시만 한다.
    """
    um = (sessions.groupBy("user_id", F.trunc("dt", "month").alias("activity_month"))
          .agg(F.max(F.col("n_purchase") > 0).alias("bought"))
          .withColumn("cohort_month", F.min("activity_month").over(Window.partitionBy("user_id"))))
    cells = um.groupBy("cohort_month", "activity_month").agg(
        F.count("*").alias("active_users"), F.count_if("bought").alias("buyers"))
    first_month = F.min("cohort_month").over(Window.partitionBy())  # 칸이 수십 개라 한 파티션 윈도로 충분하다
    return cells.select(
        "cohort_month", "activity_month",
        F.months_between("activity_month", "cohort_month").cast("int").alias("months_since"),
        # 첫 달에는 코호트 전원이 활동한다. 그 칸의 활동 사용자 수가 코호트 크기다
        F.max(F.when(F.col("activity_month") == F.col("cohort_month"), F.col("active_users")))
        .over(Window.partitionBy("cohort_month")).alias("cohort_users"),
        "active_users", "buyers",
        (F.col("cohort_month") == first_month).alias("is_left_censored"))


def expire(spark, ns: str) -> None:
    """모든 lake 테이블의 옛 스냅숏을 지우고 마지막 10개만 남긴다(ADR 0006 결정 7)."""
    # 세션 시간대가 UTC다. 마이크로초까지 넘긴다. 초 단위로 자르면 같은 초에 커밋한 스냅숏이 '아직 오래되지 않음'이 되어
    # 남는다(테스트가 가끔 12개를 봤다, 2026-10-12)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    for t in TABLES:
        # 카탈로그까지 붙여 넘긴다. 'dw.x'로 주면 Iceberg가 dw를 카탈로그로 먼저 찾다가 테이블마다 경고 스택을 남긴다
        spark.sql(f"CALL {ns.split('.')[0]}.system.expire_snapshots(table => '{ns}.{t}', "
                  f"older_than => TIMESTAMP '{now}', retain_last => 10)")


def run(spark, warehouse_root: str, ns: str = NS) -> dict:
    # 만료를 같은 프로세스의 finally에 둔다. 별도 태스크로 두면 관문·검사가 실패해도 run이 성공으로 보인다(결정 5).
    try:
        sid_s, sessions = pinned(spark, f"{ns}.fct_sessions")
        sid_p, products = pinned(spark, f"{ns}.product_daily")
        stale = stale_dates(warehouse_root, sessions, products)
        if stale:
            raise ValueError(f"신선도 관문 실패: 낡은 날짜 {len(stale)}개 ({spans(stale)}). 그 범위의 lake_daily를 "
                             "다시 돌린다. events에 없는 날짜라면 lake에서 그 날짜를 지운다")
        want = desired_dim(products).localCheckpoint()  # 검사, 차이 계산, 대조가 같은 결과를 본다
        failed = check_dim(want, products)
        if failed:
            raise ValueError("dim 검사 실패: " + "; ".join(failed))
        dim_t = f"{ns}.dim_product"
        before = current_snapshot(spark, dim_t)
        m = merge_diff(spark, want, dim_t)
        if not same_rows(spark.table(dim_t), want):
            raise ValueError("dim 대조 실패: MERGE 뒤의 dim이 원하는 상태와 다르다(검사 5)")
        overwrite(funnel(sessions), f"{ns}.mart_funnel_daily", F.lit(True))
        overwrite(cohort(sessions), f"{ns}.mart_cohort_monthly", F.lit(True))
        return {**m, "versions": want.count(), "pinned": (sid_s, sid_p),
                "dim_snapshot": (before, current_snapshot(spark, dim_t))}
    finally:
        expire(spark, ns)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--warehouse", required=True, help="1단계 events Parquet 루트. 관문이 날짜 폴더를 본다")
    p.add_argument("--landing", help="쓰지 않는다. DAG의 spark_job()이 모든 작업에 붙여서 받기만 한다")
    a = p.parse_args()
    m = run(get_spark("marts", lake=True), a.warehouse)
    print(f"입력 스냅숏 fct_sessions={m['pinned'][0]} product_daily={m['pinned'][1]}")
    print(f"dim 버전 {m['versions']:,}  추가 {m['inserted']:,}  변경 {m['updated']:,}  삭제 {m['deleted']:,}")
    print(f"dim 스냅숏 {m['dim_snapshot'][0]} → {m['dim_snapshot'][1]}")
