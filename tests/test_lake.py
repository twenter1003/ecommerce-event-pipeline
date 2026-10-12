from datetime import date

from pyspark.sql import functions as F

from pipeline.lake import TABLES, create_tables, overwrite

D1, D2 = date(2019, 11, 1), date(2019, 11, 2)


def insert_products(spark, t, rows):
    # rows: [(dt, product_id, brand)] → SQL VALUES (Python 워커를 쓰지 않는다)
    vals = ", ".join(f"(DATE '{d}', {p}, 1, 'a.b', '{b}', 'x')" for d, p, b in rows)
    spark.sql(f"INSERT INTO {t} VALUES {vals}")


def snapshots(spark, t):
    return spark.sql(f"SELECT count(*) FROM {t}.snapshots").first()[0]


def test_create_tables_makes_all_tables_and_is_idempotent(spark, ns):
    create_tables(spark, ns)  # 두 번째 실행
    names = {r.tableName for r in spark.sql(f"SHOW TABLES IN {ns}").collect()}
    assert names == set(TABLES)


def test_empty_rewrite_trap_dynamic_keeps_old_day_filter_overwrite_clears_it(spark, ns):
    # 학습 테스트(ADR 0006 결정 5): 빈 결과로 날짜를 다시 쓸 때 두 방식의 차이
    t = f"{ns}.product_daily"
    insert_products(spark, t, [(D1, 1, "a")])
    empty = spark.table(t).limit(0)
    empty.writeTo(t).overwritePartitions()          # 새 파일 0개 → 커밋을 건너뛴다
    assert spark.table(t).count() == 1              # 옛 행이 남았다
    overwrite(empty, t, F.col("dt") == F.lit(D1))   # 필터 덮어쓰기는 삭제를 커밋한다
    assert spark.table(t).count() == 0


def test_overwrite_replaces_only_requested_day_and_is_idempotent(spark, ns):
    t = f"{ns}.product_daily"
    insert_products(spark, t, [(D1, 1, "a"), (D2, 2, "b")])
    new = spark.sql(
        f"SELECT DATE '{D1}' AS dt, 9L AS product_id, 1L AS category_id, 'a.b' AS category_code, "
        "'z' AS brand, 'y' AS src_build")
    before = snapshots(spark, t)
    for _ in range(2):
        overwrite(new, t, F.col("dt") == F.lit(D1))
    rows = sorted((r.dt, r.product_id) for r in spark.table(t).collect())
    assert rows == [(D1, 9), (D2, 2)]
    assert snapshots(spark, t) == before + 2         # 실행마다 커밋 1번


def test_overwrite_ignores_rows_outside_condition(spark, ns):
    t = f"{ns}.product_daily"
    insert_products(spark, t, [(D2, 2, "b")])
    mixed = spark.sql(
        f"SELECT * FROM VALUES (DATE '{D1}', 1L, 1L, 'a.b', 'a', 'x'), (DATE '{D2}', 3L, 1L, 'a.b', 'c', 'x') "
        "AS v(dt, product_id, category_id, category_code, brand, src_build)")
    overwrite(mixed, t, F.col("dt") == F.lit(D1))
    rows = sorted((r.dt, r.product_id) for r in spark.table(t).collect())
    assert rows == [(D1, 1), (D2, 2)]                # D2의 행 3은 쓰이지 않았고, D2의 기존 행 2는 그대로다


def test_overwrite_aligns_to_table_schema_and_drops_extra_columns(spark, ns):
    t = f"{ns}.mart_funnel_daily"
    df = spark.sql(
        f"SELECT CAST(12.5 AS DECIMAL(20,2)) AS revenue, DATE '{D1}' AS dt, 1 AS sessions, 1 AS s_view, "
        "0 AS s_view_cart, 0 AS s_view_cart_purchase, 0 AS s_purchase, 0 AS s_purchase_no_cart, 1 AS users, "
        "0 AS buyers, 0 AS cart_events, 0 AS purchase_events, 'junk' AS extra")  # 표에 없는 컬럼은 버려야 한다
    overwrite(df, t, F.lit(True))
    r = spark.table(t).first()
    assert (r.dt, r.sessions, str(r.revenue)) == (D1, 1, "12.50")


def test_merge_with_op_conditions_runs_on_this_spark(spark, ns):
    # Iceberg SQL 확장과 MERGE가 PySpark 4.1.2에서 도는지(ADR 0006 확인 목록 1번)
    t = f"{ns}.dim_product"
    spark.sql(f"INSERT INTO {t} VALUES (1, 10, 1, 'a.b', 'a', 'x', DATE '{D1}', DATE '9999-12-31', true)")
    spark.sql(
        "SELECT * FROM VALUES (1L, 'U', 10L, 1L, 'a.b', 'a', 'y'), (2L, 'I', 20L, 1L, 'a.b', 'a', 'z') "
        "AS v(product_sk, op, product_id, category_id, category_code, category_l1, brand)"
    ).createOrReplaceTempView("changes")
    spark.sql(f"""
        MERGE INTO {t} t USING changes s ON t.product_sk = s.product_sk
        WHEN MATCHED AND s.op = 'D' THEN DELETE
        WHEN MATCHED AND s.op = 'U' THEN UPDATE SET brand = s.brand
        WHEN NOT MATCHED AND s.op = 'I' THEN INSERT
          (product_sk, product_id, category_id, category_code, category_l1, brand, valid_from, valid_to, is_current)
          VALUES (s.product_sk, s.product_id, s.category_id, s.category_code, s.category_l1, s.brand,
                  DATE '{D1}', DATE '9999-12-31', true)""")
    assert sorted((r.product_sk, r.brand) for r in spark.table(t).collect()) == [(1, "y"), (2, "z")]
