"""하루치 warehouse 파티션을 검사한다. 규칙을 하나라도 어기면 실패한다(Airflow 태스크 실패 → 알림).

    python -m pipeline.quality --date 2019-10-01 \
        --landing data/landing/events --warehouse data/warehouse/events
"""
import argparse
from datetime import date

from pyspark.sql import functions as F

from pipeline.spark import get_spark

# user_session은 원본 10월에도 빈 값이 2행 있어 필수로 두지 않는다.
REQUIRED = ["event_time", "event_type", "product_id", "category_id", "price", "user_id"]
EVENT_TYPES = ["view", "cart", "purchase"]


def check(spark, landing_root: str, warehouse_root: str, d: date) -> list[tuple[str, bool, str]]:
    """(규칙, 통과 여부, 실제 값) 목록을 돌려준다."""
    landing = spark.read.option("header", True).csv(f"{landing_root}/dt={d}")  # 원문 문자열 그대로
    # 루트를 읽으면 모든 날짜 폴더를 나열하고 첫 폴더(10/1)의 파일로 스키마를 정한다. 그 폴더를 다른 적재가
    # 갈아 끼우는 순간이면 데이터가 멀쩡해도 "파일 없음"으로 실패한다. 그 날짜 폴더만 직접 읽는다.
    wh = spark.read.option("basePath", warehouse_root).parquet(f"{warehouse_root}/dt={d}")

    def n(cond):
        return F.sum(cond.cast("int"))

    m = wh.agg(
        F.count("*").alias("rows"),
        *[n(F.col(c).isNull()).alias(f"null_{c}") for c in REQUIRED],
        n(~F.col("event_type").isin(EVENT_TYPES)).alias("bad_type"),
        n(F.col("price") < 0).alias("neg_price"),
        n(F.to_date("event_time") != F.lit(d)).alias("wrong_dt"),
    ).first()
    rows = m["rows"]
    # 여러 컬럼 COUNT(DISTINCT ...)는 null이 하나라도 있는 행을 빼고 센다. brand가 자주 비어 있어 distinct()를 쓴다.
    lm = landing.distinct().agg(
        F.count("*").alias("rows"),
        # 적재는 가격을 DECIMAL(10,2)로 읽으며 셋째 자리를 반올림한다(12.345 → 12.35). 다른 행과 합쳐지지 않으면
        # 행 수 대조로는 안 잡혀서 값을 직접 비교한다. 자릿수만 보면 값이 그대로인 12.340까지 잡는다.
        n(F.col("price").cast("decimal(38,10)") != F.col("price").cast("decimal(10,2)")).alias("rounded"),
    ).first()
    landing_distinct = lm["rows"]
    wh_distinct = wh.distinct().count()
    nulls = {c: m[f"null_{c}"] or 0 for c in REQUIRED}

    return [
        ("행이 하나 이상 있다", rows > 0, f"{rows:,}"),
        # 적재가 행을 조용히 잃거나(빈 event_time, 가격 반올림으로 합쳐짐) 옛 데이터가 남으면 여기서 어긋난다.
        ("warehouse 행 수 = landing 고유 행 수", rows == landing_distinct, f"{rows:,} vs {landing_distinct:,}"),
        ("완전 중복 행이 없다", rows == wh_distinct, f"{rows - wh_distinct:,}행 중복"),
        ("필수 컬럼에 null이 없다", not any(nulls.values()), str({c: v for c, v in nulls.items() if v})),
        ("event_type이 view/cart/purchase 중 하나다", not m["bad_type"], f"{m['bad_type'] or 0:,}행"),
        ("가격이 0 이상이다", not m["neg_price"], f"{m['neg_price'] or 0:,}행"),
        ("가격이 적재 때 반올림되지 않았다", not lm["rounded"], f"{lm['rounded'] or 0:,}행"),
        ("event_time의 UTC 날짜가 파티션 날짜와 같다", not m["wrong_dt"], f"{m['wrong_dt'] or 0:,}행"),
    ]


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", type=date.fromisoformat, required=True)
    p.add_argument("--landing", required=True)
    p.add_argument("--warehouse", required=True)
    a = p.parse_args()
    results = check(get_spark("quality"), a.landing, a.warehouse, a.date)
    for name, ok, actual in results:
        print(f"{'통과' if ok else '실패'}  {name}  ({actual})")
    failed = [name for name, ok, _ in results if not ok]
    if failed:
        raise SystemExit(f"{a.date} 품질 검사 실패: {failed}")
