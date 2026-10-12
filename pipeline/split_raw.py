"""월별 원본 CSV를 일별 landing 파일로 나눈다.

원천 시스템이 하루에 파일 하나씩 떨궈 준다고 가정하기 위한 1회성 시뮬레이션이다.
값은 문자열 그대로 두고 중복도 지우지 않는다(정제는 load_events가 한다).

    python -m pipeline.split_raw data/raw/2019-Oct.csv.gz --landing data/landing/events
"""
import argparse

from pyspark.sql import functions as F

from pipeline.spark import get_spark


def split(spark, raw_path: str, landing_root: str) -> None:
    df = spark.read.option("header", True).csv(raw_path)  # 스키마 없이 읽으면 전부 문자열
    # 다른 달 행이 섞여 있으면, 먼저 나눠 둔 그 달 landing이 이 파일의 몇 행으로 통째로 덮어써진다.
    # 1회성 단계라 gz를 한 번 더 읽는 비용은 감수한다.
    months = [r[0] for r in df.select(F.substring("event_time", 1, 7)).distinct().collect()]
    if len(months) != 1:
        raise ValueError(f"한 파일에 여러 달이 섞여 있다: {sorted(months)}")
    (
        df.withColumn("dt", F.substring("event_time", 1, 10))  # "2019-10-01 00:00:00 UTC" → "2019-10-01"
        # 하루치가 한 태스크에 모이게 해서 날짜당 파일 하나, 날짜끼리는 병렬로 압축한다.
        .repartition("dt")
        .write.partitionBy("dt")
        # 다른 달 landing은 건드리지 않고, 이번 파일에 있는 날짜만 덮어쓴다.
        .option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .option("header", True)
        # CSV 쓰기는 기본으로 값 앞뒤 공백을 지운다. 원본 그대로 두려고 끈다.
        .option("ignoreLeadingWhiteSpace", False)
        .option("ignoreTrailingWhiteSpace", False)
        .option("compression", "gzip")
        .csv(landing_root)
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("raw_path")
    # 기본값을 두지 않는다. 상대 경로는 Airflow(임시 폴더)·Dataproc(클러스터 HDFS)에서 지워지는 곳을 가리킨다.
    p.add_argument("--landing", required=True)
    a = p.parse_args()
    split(get_spark("split_raw"), a.raw_path, a.landing)
