"""3단계 lake(Iceberg) 테이블 정의와 쓰기 함수. 설계는 ADR 0006·0007.

    python -m pipeline.lake create             네임스페이스와 테이블 만들기(IF NOT EXISTS, lake-init이 부른다)
    python -m pipeline.lake reset              테이블을 지우고(PURGE) 다시 만들기. 스키마를 바꿀 때 쓴다(lake는 파생 데이터)
    python -m pipeline.lake sql "<SQL>" ...    lake 세션에서 SQL을 차례로 실행하고 결과를 보인다(확인·실험·질문지용)
"""
import argparse

from pyspark.sql import Column, DataFrame, functions as F

from pipeline.spark import get_spark

NS = "lake.dw"
# 재시도 10회: K개 날짜가 같은 테이블에 동시에 커밋한다(기본 4회는 빠듯하다).
# 옛 metadata.json은 expire_snapshots가 지우지 않아 커밋할 때 지운다.
PROPS = """TBLPROPERTIES ('format-version'='2', 'commit.retry.num-retries'='10',
  'write.metadata.delete-after-commit.enabled'='true', 'write.metadata.previous-versions-max'='20')"""
# src_build = '<읽은 events 파일 이름 해시>-<실행 id>'. lake_marts의 신선도 관문이 쓴다(ADR 0007 결정 6).
TABLES = {
    "fct_sessions": """CREATE TABLE IF NOT EXISTS {ns}.fct_sessions (
        session_id STRING, user_id BIGINT, session_start TIMESTAMP, session_end TIMESTAMP,
        n_events BIGINT, n_view BIGINT, n_cart BIGINT, n_purchase BIGINT, purchase_amount DECIMAL(18,2),
        src_changed_at_start BOOLEAN, src_changes_inside BIGINT, src_build STRING, dt DATE)
        USING iceberg PARTITIONED BY (dt) {props}""",
    "product_daily": """CREATE TABLE IF NOT EXISTS {ns}.product_daily (
        dt DATE, product_id BIGINT, category_id BIGINT, category_code STRING, brand STRING, src_build STRING)
        USING iceberg PARTITIONED BY (dt) {props}""",
    "dim_product": """CREATE TABLE IF NOT EXISTS {ns}.dim_product (
        product_sk BIGINT, product_id BIGINT, category_id BIGINT, category_code STRING, category_l1 STRING,
        brand STRING, valid_from DATE, valid_to DATE, is_current BOOLEAN)
        USING iceberg {props}""",
    "mart_funnel_daily": """CREATE TABLE IF NOT EXISTS {ns}.mart_funnel_daily (
        dt DATE, sessions BIGINT, s_view BIGINT, s_view_cart BIGINT, s_view_cart_purchase BIGINT,
        s_purchase BIGINT, s_purchase_no_cart BIGINT, users BIGINT, buyers BIGINT,
        cart_events BIGINT, purchase_events BIGINT, revenue DECIMAL(18,2))
        USING iceberg {props}""",
    "mart_cohort_monthly": """CREATE TABLE IF NOT EXISTS {ns}.mart_cohort_monthly (
        cohort_month DATE, activity_month DATE, months_since INT, cohort_users BIGINT,
        active_users BIGINT, buyers BIGINT, is_left_censored BOOLEAN)
        USING iceberg {props}""",
    # 4단계 실시간 1분 퍼널(ADR 0009 결정 4). Spark 스트리밍이 append로만 쓴다. 윈도는 워터마크가 지나야 한 번 나온다.
    # topic은 계보 컬럼이다. 재생마다 시각을 '지금'으로 옮겨서, 연달아 한 실험끼리 윈도 시간대가 겹친다. 토픽으로 가른다
    "rt_funnel_minute": """CREATE TABLE IF NOT EXISTS {ns}.rt_funnel_minute (
        window_start TIMESTAMP, window_end TIMESTAMP, events BIGINT, views BIGINT, carts BIGINT, purchases BIGINT,
        revenue DECIMAL(18,2), topic STRING)
        USING iceberg {props}""",
    # 4-3 실시간 세션(ADR 0009 결정 7). 세션이 닫혀야(워터마크가 '마지막 이벤트 + 30분'을 지나야) 한 번 나온다
    "rt_sessions": """CREATE TABLE IF NOT EXISTS {ns}.rt_sessions (
        user_id BIGINT, session_start TIMESTAMP, session_end TIMESTAMP, n_events BIGINT, topic STRING)
        USING iceberg {props}""",
}


def create_tables(spark, ns: str = NS) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    for ddl in TABLES.values():
        spark.sql(ddl.format(ns=ns, props=PROPS))


def reset(spark, ns: str = NS) -> None:
    for name in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {ns}.{name} PURGE")
    create_tables(spark, ns)


def overwrite(df: DataFrame, table: str, cond: Column) -> None:
    """필터 덮어쓰기: cond에 맞는 파일을 지우고 새 파일을 넣는 커밋 1번. 새 행이 0개여도 삭제를 커밋한다.

    overwritePartitions()와 dynamic INSERT OVERWRITE는 새 파일이 0개면 커밋을 건너뛰어 옛 날짜가 남는다(ADR 0006 결정 5).
    """
    # cond 밖의 행이 섞이면 쓰지 않는다(1단계 load_events와 같은 이유).
    # 표 스키마의 순서와 타입으로 맞춘다. Spark 4는 ANSI가 기본이라 넘치면 null이 아니라 오류가 난다.
    cols = [F.col(f.name).cast(f.dataType).alias(f.name) for f in df.sparkSession.table(table).schema]
    df.filter(cond).select(cols).writeTo(table).overwrite(cond)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["create", "reset", "sql"])
    p.add_argument("statements", nargs="*", help="sql일 때 실행할 문장들")
    a = p.parse_args()
    spark = get_spark(f"lake-{a.cmd}", lake=True)
    if a.cmd == "create":
        create_tables(spark)
    elif a.cmd == "reset":
        reset(spark)
    else:
        for s in a.statements:
            print(f"> {s}")
            spark.sql(s).show(1000, truncate=False)
