"""일별 landing CSV를 타입을 입혀 날짜 파티션 Parquet으로 적재한다.

같은 날짜를 몇 번 다시 돌려도 결과가 같다(멱등). 요청한 날짜 파티션만 통째로 갈아 끼우고,
완전히 똑같은 행은 하나만 남긴다. 요청한 날짜 중 행이 하나도 없는 날이 있으면 쓰지 않고 실패한다.

    python -m pipeline.load_events --start 2019-10-01 --end 2019-10-31 \
        --landing data/landing/events --warehouse data/warehouse/events
"""
import argparse
from datetime import date, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, LongType, StringType, StructField, StructType, TimestampType,
)

from pipeline.spark import get_spark

SCHEMA = StructType([
    StructField("event_time", TimestampType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),  # 2053013555631882655처럼 int 범위를 넘는다
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DecimalType(10, 2)),  # 돈은 double의 반올림 오차를 피한다
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def load(spark, landing_root: str, warehouse_root: str, dates: list[date]) -> None:
    if not dates:
        raise ValueError("빈 날짜 범위 (--start가 --end보다 늦다)")
    df = (
        spark.read.schema(SCHEMA)
        .option("header", True)
        # 헤더 이름을 SCHEMA와 대조한다. 끄면 컬럼 순서가 바뀐 파일이 위치대로 읽혀 값이 뒤바뀐다.
        .option("enforceSchema", False)
        # 원문 시각은 "2019-10-01 00:00:00 UTC". 세션 시간대가 UTC라 그대로 UTC로 읽힌다.
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss 'UTC'")
        # 시각 형식·숫자·컬럼 수가 깨진 행은 바로 실패시킨다. 단 빈 event_time(null이 돼 날짜 필터에서 빠짐)과
        # 소수 셋째 자리 가격(반올림됨)은 통과한다. 둘은 2단계 품질 검사가 잡는다(행 수 대조, 반올림 규칙).
        .option("mode", "FAILFAST")
        .csv([f"{landing_root}/dt={d}" for d in dates])  # landing 폴더가 없으면 여기서 실패한다
    )
    typed = (
        df.withColumn("dt", F.to_date("event_time"))
        # 요청 밖 날짜의 행이 섞이면 그 날짜 파티션이 이 행들로 통째로 덮어써진다. 그래서 걸러 낸다.
        .filter(F.col("dt").isin(dates))
    )
    # dynamic overwrite는 행이 없는 날짜를 건드리지 않는다. 빈 파일로 정정된 날짜가
    # 옛 데이터를 그대로 둔 채 성공하지 않도록 막는다. event_time만 읽는 스캔이 한 번 더 든다.
    missing = set(dates) - {r.dt for r in typed.select("dt").distinct().collect()}
    if missing:
        raise ValueError(f"행이 없는 날짜: {sorted(map(str, missing))}")
    (
        typed
        # 완전 중복은 event_time까지 같으니 늘 같은 날에 모인다. dt로 모으면 셔플 한 번에 날짜당 파일 하나가 된다.
        .repartition("dt")
        .dropDuplicates()
        .write.partitionBy("dt")
        # 이번에 쓰는 날짜 파티션만 갈아 끼운다. 다른 날짜는 그대로 둔다.
        .option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .option("compression", "zstd")
        .parquet(warehouse_root)
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, help="포함. 생략하면 start 하루만")
    # 기본값을 두지 않는다. 상대 경로는 Airflow(임시 폴더)·Dataproc(클러스터 HDFS)에서 지워지는 곳을 가리킨다.
    p.add_argument("--landing", required=True)
    p.add_argument("--warehouse", required=True)
    a = p.parse_args()
    load(get_spark("load_events"), a.landing, a.warehouse, date_range(a.start, a.end or a.start))
