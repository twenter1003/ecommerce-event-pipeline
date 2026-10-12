"""날짜별 events를 세션(fct_sessions)과 상품 하루 스냅숏(product_daily)으로 만든다. 설계는 ADR 0007 결정 2·6.

결과는 그날 events만의 순수 함수다. 검사 4개를 먼저 계산하고 모두 통과할 때만 두 테이블의 그 날짜를 덮어쓴다.

    python -m pipeline.model_daily --start 2019-11-01 [--end 2019-11-30] --warehouse data/warehouse/events
    python -m pipeline.model_daily --start 2019-11-01 --end 2019-11-30 --warehouse ... --dry-run --gap-minutes 60
"""
import argparse
import hashlib
import os
import uuid
from datetime import date

from pyspark.sql import Window, functions as F

from pipeline.lake import NS, overwrite
from pipeline.load_events import date_range
from pipeline.spark import get_spark

# 규칙 2의 바닥(ADR 0007 결정 6, 3-3에서 다시 정함). 원천 user_session은 나누는 방식이 기간마다 달라서
# (2020-03-25부터 원천 세션이 길어짐) 일치율로 우리 세션을 채점하지 않는다. 고장 난 세션만 막는다.
# 실측: 213일 최저 정밀도 0.732·재현율 0.136, 모두 쪼개면 정밀도 0.05~0.08, 모두 뭉치면 재현율 0.
P_MIN, R_MIN = 0.50, 0.05


def input_hash(warehouse_root: str, d: date) -> str:
    """events 날짜 폴더의 파일 이름 목록 해시. Spark는 쓸 때마다 이름에 새 UUID를 붙여서, 다시 쓰면 값이 바뀐다."""
    names = sorted(n for n in os.listdir(f"{warehouse_root}/dt={d}") if not n.startswith((".", "_")))
    return hashlib.sha256("\n".join(names).encode()).hexdigest()[:16]


def model(spark, warehouse_root: str, dates: list[date], ns: str = NS,
          gap_minutes: int = 30, dry_run: bool = False) -> dict:
    if not dates:
        raise ValueError("빈 날짜 범위 (--start가 --end보다 늦다)")
    # 읽기 직전에 파일 이름을 본다. 읽은 뒤에 events가 다시 써지면 lake_marts 관문에서 해시가 어긋나 '낡음'이 된다.
    run_id = uuid.uuid4().hex[:8]
    builds = {d: f"{input_hash(warehouse_root, d)}-{run_id}" for d in dates}
    ev = (spark.read.option("basePath", warehouse_root)
          .parquet(*[f"{warehouse_root}/dt={d}" for d in dates]))

    # 세션: 같은 사용자의 앞 이벤트와 간격이 gap을 넘으면 새 세션. dt로 나눠서 UTC 자정에서 저절로 끊긴다.
    # 정렬 키 다섯 개: 완전 중복은 1단계에서 지웠으므로 이 키면 순서가 하나로 정해져, 다시 계산해도 결과가 같다.
    w = Window.partitionBy("user_id", "dt").orderBy("event_time", "user_session", "event_type", "product_id", "price")
    prev_t = F.lag("event_time").over(w)
    new = prev_t.isNull() | (F.unix_timestamp("event_time") - F.unix_timestamp(prev_t) > gap_minutes * 60)
    # user_session도 바뀌었나. 그날 사용자의 첫 이벤트는 비교할 앞이 없어 NULL. 빈 user_session도 값으로 비교한다.
    changed = F.when(prev_t.isNotNull(), ~F.col("user_session").eqNullSafe(F.lag("user_session").over(w)))
    marked = (ev.withColumn("is_new", new).withColumn("src_changed", changed)
              .withColumn("seq", F.sum(F.col("is_new").cast("int")).over(w)))

    def n_type(t):
        return F.count_if(F.col("event_type") == t)

    sessions = (
        marked.groupBy("user_id", "dt", "seq").agg(
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.count("*").alias("n_events"),
            n_type("view").alias("n_view"), n_type("cart").alias("n_cart"), n_type("purchase").alias("n_purchase"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))).alias("purchase_amount"),
            F.max(F.when(F.col("is_new"), F.col("src_changed"))).alias("src_changed_at_start"),
            F.count_if(~F.col("is_new") & F.col("src_changed")).alias("src_changes_inside"),
        )
        # 같은 사용자의 두 세션은 시작이 gap 넘게 떨어져 있어 구조상 유일하다. 다시 돌려도 같은 값이다.
        .withColumn("session_id", F.concat_ws("-", "user_id", F.unix_timestamp("session_start")))
        .persist()
    )
    # 상품 하루 스냅숏. 카테고리는 하루 안에서 하나라는 전제(규칙 3)로 max를 쓴다.
    # brand는 그날 마지막 non-null 값. max_by는 순서 값이 NULL인 행을 건너뛴다. 같은 시각이면 brand로 순서를 정한다.
    cat = F.struct("category_id", "category_code")
    products = (
        ev.groupBy("dt", "product_id").agg(
            F.max(cat).alias("cat"),
            F.size(F.collect_set(cat)).alias("n_cat"),
            F.max_by("brand", F.when(F.col("brand").isNotNull(), F.struct("event_time", "brand"))).alias("brand"),
        )
        .select("dt", "product_id", "cat.category_id", "cat.category_code", "brand", "n_cat")
        .persist()
    )
    try:
        m = check(ev, sessions, products, dates)
        if not dry_run:
            m.raise_for_failures()
            src = F.element_at(F.create_map(*[F.lit(x) for d, b in builds.items() for x in (d, b)]), F.col("dt"))
            cond = F.col("dt").isin(dates)
            # 두 커밋 사이에 죽으면 두 테이블의 실행 id가 달라진다. lake_marts 관문이 그 날짜를 낡음으로 본다.
            overwrite(sessions.withColumn("src_build", src), f"{ns}.fct_sessions", cond)
            overwrite(products.withColumn("src_build", src), f"{ns}.product_daily", cond)
        return m
    finally:
        sessions.unpersist()
        products.unpersist()


class Metrics(dict):
    def raise_for_failures(self):
        if self["failed"]:
            raise ValueError("model_daily 검사 실패: " + "; ".join(self["failed"]))


def check(ev, sessions, products, dates) -> Metrics:
    """쓰기 전 검사 4개(ADR 0007 결정 6). 날짜별로 본다."""
    s = {r.dt: r for r in sessions.groupBy("dt").agg(
        F.count("*").alias("sessions"),
        F.sum("n_events").alias("n_events"),
        F.sum(F.col("n_view") + F.col("n_cart") + F.col("n_purchase")).alias("n_typed"),
        F.count_if(F.col("src_changed_at_start")).alias("tp"),
        F.count_if(~F.col("src_changed_at_start")).alias("fp"),
        F.sum("src_changes_inside").alias("fn"),
    ).collect()}
    rows = {r.dt: r[1] for r in ev.groupBy("dt").count().collect()}
    multi_cat = {r.dt: r[1] for r in products.filter("n_cat > 1").groupBy("dt").count().collect()}

    def ratio(a, b):
        return a / b if b else 1.0  # 판단할 경계가 없으면 통과. 세션이 한 덩어리로 뭉치는 회귀는 재현율이 잡는다

    failed, per_day = [], {}
    for d in dates:
        if d not in s:
            failed.append(f"{d} 세션 0개(규칙 1)")
            continue
        r = s[d]
        p, rc = ratio(r.tp, r.tp + r.fp), ratio(r.tp, r.tp + r.fn)
        per_day[d] = (r.sessions, p, rc)
        if p < P_MIN or rc < R_MIN:
            failed.append(f"{d} 경계 정밀도 {p:.3f}(기준 {P_MIN}) 재현율 {rc:.3f}(기준 {R_MIN}) (규칙 2)")
        if multi_cat.get(d):
            failed.append(f"{d} 카테고리가 둘 이상인 상품 {multi_cat[d]:,}개(규칙 3)")
        if not rows[d] == r.n_events == r.n_typed:
            failed.append(f"{d} 행 수 불일치 events {rows[d]:,} / 세션 합 {r.n_events:,} / 유형 합 {r.n_typed:,}(규칙 4)")
    tp, fp, fn = (sum(r[k] for r in s.values()) for k in ("tp", "fp", "fn"))
    return Metrics(sessions=sum(r.sessions for r in s.values()), precision=ratio(tp, tp + fp),
                   recall=ratio(tp, tp + fn), per_day=per_day, failed=failed)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, help="포함. 생략하면 start 하루만")
    p.add_argument("--warehouse", required=True, help="1단계 events Parquet 루트")
    p.add_argument("--landing", help="쓰지 않는다. DAG의 spark_job()이 모든 작업에 붙여서 받기만 한다")
    p.add_argument("--dry-run", action="store_true", help="쓰지 않고 숫자만 본다")
    p.add_argument("--gap-minutes", type=int, default=30)
    a = p.parse_args()
    if a.gap_minutes != 30 and not a.dry_run:
        p.error("--gap-minutes는 --dry-run과만 쓴다. 테이블의 세션 정의는 30분이다(ADR 0007 결정 2)")
    m = model(get_spark("model_daily", lake=True), a.warehouse, date_range(a.start, a.end or a.start),
              gap_minutes=a.gap_minutes, dry_run=a.dry_run)
    for d, (n, prec, rec) in m["per_day"].items():
        print(f"{d}  세션 {n:,}  정밀도 {prec:.3f}  재현율 {rec:.3f}")
    print(f"합계  gap {a.gap_minutes}분  세션 {m['sessions']:,}  정밀도 {m['precision']:.3f}  재현율 {m['recall']:.3f}")
    for f in m["failed"]:
        print(f"실패  {f}")
    m.raise_for_failures()
