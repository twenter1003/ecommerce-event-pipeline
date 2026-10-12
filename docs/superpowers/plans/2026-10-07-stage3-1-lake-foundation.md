# 3-1 레이크 기반 (SeaweedFS + Iceberg JDBC 카탈로그) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 3단계 테이블을 SeaweedFS 위 Iceberg에 만들고, 원자적 커밋·빈 날짜 함정·체크섬 우회·강제 종료를 실제로 확인할 수 있는 기반을 갖춘다.

**Architecture:** `get_spark(lake=True)`가 Iceberg 확장과 `lake` 카탈로그를 붙인다. 컨테이너에서는 Postgres JDBC 카탈로그와 S3FileIO(SeaweedFS)를, 호스트 테스트에서는 임시 폴더의 hadoop 카탈로그를 쓴다. 테이블 정의와 쓰기 함수는 `pipeline/lake.py` 한 곳에 둔다. lake 준비는 일회성 서비스 `lake-init`이 하고, Airflow 기동은 여기에 의존하지 않는다.

**Tech Stack:** PySpark 4.1.2(local), Iceberg 1.12.0(iceberg-spark-runtime-4.1_2.13, iceberg-aws-bundle), PostgreSQL JDBC 42.7.13, SeaweedFS 4.48(`weed mini`), Docker Compose, pytest

**Spec:** `docs/decisions/0006-stage3-storage.md`, `docs/decisions/0007-stage3-modeling.md`(결정 1의 테이블 5개·계보 컬럼, 결정 7의 3-1 행과 통합 확인 I1~I4)

## Global Constraints
- 이 프로젝트는 git 저장소가 아니다. 커밋 단계는 없다. 되돌릴 수 없는 수정 전에는 대상 파일을 먼저 본다.
- 1·2단계 동작을 바꾸지 않는다: `load_events.py`, `quality.py`, `split_raw.py`, `docker/airflow.Dockerfile`, `events_daily` DAG는 그대로. `get_spark(app)`(lake 없음)의 설정도 그대로다.
- jar는 `jars/`의 세 파일만 쓴다(`jars/SHA256SUMS`로 고정). `spark.jars.packages`를 쓰지 않는다.
- 세션 설정에 `spark.sql.sources.partitionOverwriteMode=dynamic`을 넣지 않는다.
- Iceberg에 쓰는 방법은 `lake.overwrite()`(필터 덮어쓰기)와 MERGE 둘뿐이다.
- 비밀값(S3 키)은 `.env`에만 둔다. `.env.example`에는 빈 값과 만드는 명령만 둔다.
- 테스트 데이터는 SQL `VALUES`나 CSV로 만든다. `createDataFrame(파이썬 리스트)`와 Python UDF는 쓰지 않는다.
- 호스트 Spark는 `SPARK_LOCAL_IP=127.0.0.1`(conftest)로 띄운다.
- 문서·주석·출력은 한국어, 기존 문체(짧은 문장, 이유를 함께)를 따른다.

## Review Focus
1. **jar가 없는 새 clone**: `bash jars/fetch.sh` 없이 pytest나 compose를 돌리면, 이해할 수 있는 메시지로 바로 멈춰야 한다(조용한 ClassNotFound가 아님). → Task 2의 conftest 검사, Task 4의 lake-init 실패가 Airflow를 막지 않는지 확인.
2. **빈 결과로 날짜를 다시 쓰기**: 옛 데이터가 남으면 안 된다. → Task 3 학습 테스트.
3. **다른 날짜 행이 섞인 DataFrame**: 요청한 날짜 밖의 행이 쓰이거나 다른 날짜를 지우면 안 된다. → Task 3 테스트.
4. **`lake create`를 두 번 실행**: 오류 없이 그대로여야 한다. → Task 3 테스트, I1.
5. **기존 1·2단계 테스트**: lake jar가 붙은 세션에서도 21개가 그대로 통과해야 한다. → 모든 Task의 전체 pytest 실행.

---

### Task 1: `get_spark(lake=)` — Iceberg 카탈로그 붙이기

**Files:**
- Modify: `pipeline/spark.py`

**Interfaces:**
- Produces: `get_spark(app_name: str, lake: bool = False) -> SparkSession`. `lake=True`면 카탈로그 `lake`가 생긴다. 환경 변수: `LAKE_JARS`(필수), `LAKE_CATALOG_URI`(있으면 JDBC + S3FileIO), 없으면 `LAKE_WAREHOUSE`(hadoop 카탈로그 폴더). S3 엔드포인트는 `http://seaweedfs:8333` 상수다.

- [ ] **Step 1: 구현**

```python
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str, lake: bool = False) -> SparkSession:
    # master는 정하지 않는다. `python`으로 실행하면 local[*], Dataproc에 제출하면 YARN이 된다.
    b = (
        SparkSession.builder.appName(app_name)
        # 날짜 파티션은 UTC 기준. 노트북 시간대(KST)에 따라 dt가 바뀌면 안 된다.
        .config("spark.sql.session.timeZone", "UTC")
        # (기존 주석 그대로)
        .config("spark.driver.memory", "8g")
    )
    if lake:  # 3단계 lake(ADR 0006). 1·2단계 작업의 JVM 구성은 바꾸지 않는다
        b = (
            b
            # 시스템 클래스패스에 올려야 JDBC 카탈로그의 DriverManager가 postgres 드라이버를 찾는다.
            # python -m으로 띄우면 빌더 설정이 JVM 시작 때 넘어간다(spark.driver.memory와 같은 경로).
            .config("spark.driver.extraClassPath", f"{os.environ['LAKE_JARS']}/*")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        )
        uri = os.environ.get("LAKE_CATALOG_URI")
        conf = (
            {"type": "jdbc", "uri": uri, "warehouse": "s3://warehouse/",
             "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
             "s3.endpoint": os.environ.get("LAKE_S3_ENDPOINT", "http://seaweedfs:8333"),
             "s3.path-style-access": "true"}
            if uri else  # 호스트 테스트: 로컬 폴더의 hadoop 카탈로그(로컬 rename은 원자적이다)
            {"type": "hadoop", "warehouse": os.environ["LAKE_WAREHOUSE"]}
        )
        for k, v in conf.items():
            b = b.config(f"spark.sql.catalog.lake.{k}", v)
    return b.getOrCreate()
```
S3 키와 리전은 Spark 설정에 넣지 않는다. S3FileIO가 AWS 기본 자격 증명 체인(`AWS_ACCESS_KEY_ID` 등 환경 변수)으로 읽으므로 명령줄과 이벤트 로그에 남지 않는다.

- [ ] **Step 2: 기존 테스트로 lake 없는 경로가 그대로인지 확인**

Run: `.venv/bin/python -m pytest -q`
Expected: `21 passed`

### Task 2: conftest를 lake 세션으로, jar 없으면 바로 멈추기

**Files:**
- Modify: `tests/conftest.py`

**Interfaces:**
- Consumes: Task 1 `get_spark(..., lake=True)`
- Produces: fixture `spark`(세션 하나, lake 카탈로그 = 임시 폴더 hadoop), fixture `ns`(함수마다 새 네임스페이스 `lake.t<8자>`에 테이블 5개)

- [ ] **Step 1: conftest 수정**

```python
import os
import uuid
from pathlib import Path

import pytest

from pipeline.spark import get_spark

JARS = Path(__file__).resolve().parent.parent / "jars"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # (기존 SPARK_LOCAL_IP 주석 그대로)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    if not list(JARS.glob("*.jar")):
        pytest.exit("lake jar가 없다. 먼저 `bash jars/fetch.sh`를 실행한다", returncode=1)
    os.environ.setdefault("LAKE_JARS", str(JARS))
    os.environ.pop("LAKE_CATALOG_URI", None)  # 호스트는 도커 카탈로그에 닿지 않는다. hadoop 카탈로그를 쓴다
    os.environ["LAKE_WAREHOUSE"] = str(tmp_path_factory.mktemp("lake"))
    # 프로세스에 세션은 하나다. 1·2단계 테스트도 이 세션을 같이 쓰므로, lake jar가 그 결과를 바꾸지 않는지도 함께 확인된다.
    s = get_spark("test", lake=True)
    s.conf.set("spark.sql.shuffle.partitions", "4")
    return s


@pytest.fixture
def ns(spark):
    from pipeline.lake import create_tables
    name = f"lake.t{uuid.uuid4().hex[:8]}"
    create_tables(spark, name)
    return name
```

- [ ] **Step 2: 기존 21개가 lake 세션에서도 통과하는지 확인**

Run: `.venv/bin/python -m pytest -q`
Expected: `21 passed` (Task 3 전이라 `ns`는 아직 아무도 쓰지 않는다)

### Task 3: `pipeline/lake.py` — 테이블 정의, 필터 덮어쓰기, CLI

**Files:**
- Create: `pipeline/lake.py`
- Create: `tests/test_lake.py`

**Interfaces:**
- Consumes: Task 1, Task 2 fixture
- Produces:
  - `NS = "lake.dw"`, `TABLES: dict[str, str]`(이름 → DDL 템플릿, `{ns}`·`{props}` 자리)
  - `create_tables(spark, ns: str = NS) -> None`
  - `overwrite(df, table: str, cond: Column) -> None`
  - `reset(spark, ns: str = NS) -> None`
  - CLI `python -m pipeline.lake create | reset | sql "<SQL>" ["<SQL>" ...]`

- [ ] **Step 1: 실패하는 테스트 작성** (`tests/test_lake.py`)

```python
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
    new = spark.sql(f"SELECT DATE '{D1}' AS dt, 9L AS product_id, 1L AS category_id, 'a.b' AS category_code, 'z' AS brand, 'y' AS src_build")
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


def test_overwrite_casts_to_table_types_and_column_order(spark, ns):
    t = f"{ns}.mart_funnel_daily"
    df = spark.sql(
        f"SELECT CAST(12.5 AS DECIMAL(20,2)) AS revenue, DATE '{D1}' AS dt, 1 AS sessions, 1 AS s_view, 0 AS s_view_cart, "
        "0 AS s_view_cart_purchase, 0 AS s_purchase, 0 AS s_purchase_no_cart, 1 AS users, 0 AS buyers, 0 AS cart_events, 0 AS purchase_events")
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
```

- [ ] **Step 2: 실패 확인**

Run: `.venv/bin/python -m pytest -q tests/test_lake.py`
Expected: FAIL (`ModuleNotFoundError: pipeline.lake`)

- [ ] **Step 3: 구현** (`pipeline/lake.py`)

```python
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
# src_build = '<읽은 events 파일 이름 해시>-<실행 id>'. lake_marts 신선도 관문이 쓴다(ADR 0007 결정 6).
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
```

- [ ] **Step 4: 통과 확인**

Run: `.venv/bin/python -m pytest -q`
Expected: `27 passed` (기존 21 + 새 6)

### Task 4: compose — seaweedfs, lake-init, 환경 변수, jar 마운트

**Files:**
- Modify: `docker-compose.yml`, `.env.example`, `.env`(로컬, 키 생성)

**Interfaces:**
- Consumes: Task 1~3, `jars/`
- Produces: 서비스 `seaweedfs`(S3 8333, 내부 전용), 일회성 서비스 `lake-init`, Airflow 컨테이너의 `LAKE_*`·`AWS_*` 환경 변수

- [ ] **Step 1: `x-airflow` 환경 변수와 마운트 추가**

`environment: &env` 끝에:
```yaml
    # 3단계 lake(ADR 0006). 계정은 위 Airflow DB와 같은 로컬 전용 airflow/airflow, 포트는 열지 않는다
    LAKE_JARS: /opt/airflow/project/jars
    LAKE_CATALOG_URI: jdbc:postgresql://postgres:5432/iceberg?user=airflow&password=airflow
    # S3FileIO가 AWS 기본 자격 증명 체인으로 읽는다. Spark 설정에 넣지 않아 명령줄과 이벤트 로그에 남지 않는다.
    # .env에는 AWS_* 이름을 쓰지 않는다. 셸에 진짜 AWS 키가 export돼 있으면 compose 치환이 그 값을 먼저 집는다.
    AWS_ACCESS_KEY_ID: ${LAKE_S3_ACCESS_KEY:?.env에 넣는다(.env.example 참고)}
    AWS_SECRET_ACCESS_KEY: ${LAKE_S3_SECRET_KEY:?.env에 넣는다(.env.example 참고)}
    AWS_REGION: us-east-1
    # SeaweedFS 4.48 버그 #11582 우회: SDK 2.30 이상의 기본 체크섬 trailer 업로드가 약 50% 확률로 400 InvalidDigest.
    # 수정이 들어간 버전(4.49 이상)으로 올리면 두 줄을 지운다(ADR 0006 결정 2).
    AWS_REQUEST_CHECKSUM_CALCULATION: WHEN_REQUIRED
    AWS_RESPONSE_CHECKSUM_VALIDATION: WHEN_REQUIRED
```
`volumes:` 끝에 `- ./jars:/opt/airflow/project/jars:ro`.

- [ ] **Step 2: 서비스 두 개와 볼륨 추가**

```yaml
  seaweedfs:
    image: chrislusf/seaweedfs:4.48
    restart: on-failure
    entrypoint: ["/bin/sh", "-c"]
    # 키 파일을 저장소에도 호스트에도 두지 않는다. 시작할 때마다 env로 컨테이너 /tmp에 만든다.
    # 키 없이 띄우면 누구나 읽고 쓸 수 있게 열린다(allow-all). 그 방식은 쓰지 않는다.
    command:
      - |
        printf '{"identities":[{"name":"spark","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Read","Write","List"]}]}' "$$S3_ACCESS_KEY" "$$S3_SECRET_KEY" > /tmp/s3.json
        exec weed mini -dir=/data -s3.config=/tmp/s3.json -bucket=warehouse
    environment:
      S3_ACCESS_KEY: ${LAKE_S3_ACCESS_KEY:?.env에 넣는다}
      S3_SECRET_KEY: ${LAKE_S3_SECRET_KEY:?.env에 넣는다}
    volumes:
      - seaweedfs-data:/data
    ports:
      - "127.0.0.1:23646:23646"   # 관리 화면만 연다. S3(8333)는 compose 네트워크 안에서만 쓴다

  # lake 준비(ADR 0006 결정 6). Airflow 서비스는 이것에 의존하지 않는다: 실패해도 1·2단계는 돈다.
  lake-init:
    <<: *airflow
    restart: "no"
    entrypoint: /bin/bash
    command:
      - -c
      - |
        set -e
        # 카탈로그 DB. 기존 Postgres 볼륨에서는 initdb 스크립트가 다시 돌지 않아 여기서 만든다. 두 번 돌려도 안전하다
        python -c "import psycopg2; c = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow'); c.autocommit = True; k = c.cursor(); k.execute(\"select 1 from pg_database where datname = 'iceberg'\"); k.fetchone() or k.execute('create database iceberg')"
        # 버킷이 아직 없을 수 있다(weed mini 시작 직후). 테이블 만들기를 몇 번 다시 시도한다
        for i in 1 2 3 4 5; do python -m pipeline.lake create && exit 0; echo "lake create 재시도 $$i" >&2; sleep 10; done
        exit 1
    depends_on:
      postgres:
        condition: service_healthy
      seaweedfs:
        condition: service_started
```
`volumes:`에 `seaweedfs-data:`를 더한다.

- [ ] **Step 3: `.env.example`과 `.env`에 키 추가**

`.env.example` 끝에:
```
# 3단계 lake: SeaweedFS S3 키(필수). 아래 명령으로 만든 값을 넣는다
#   LAKE_S3_ACCESS_KEY: openssl rand -hex 16    LAKE_S3_SECRET_KEY: openssl rand -hex 32
LAKE_S3_ACCESS_KEY=
LAKE_S3_SECRET_KEY=
```
`.env`에는 실제 값을 붙인다:
```bash
printf 'LAKE_S3_ACCESS_KEY=%s\nLAKE_S3_SECRET_KEY=%s\n' "$(openssl rand -hex 16)" "$(openssl rand -hex 32)" >> .env
```

- [ ] **Step 4: 설정 검증**

Run: `docker compose config -q && echo ok`
Expected: `ok`

### Task 5: 통합 확인 I1·I2 — 기동과 smoke

**Files:**
- Create: `bench/lake_commits.py` (I3에서 씀, 여기서는 만들기만)

- [ ] **Step 1: I1 기동**

Run: `docker compose up -d && sleep 60 && docker compose ps -a --format '{{.Name}} {{.Status}}'`
Expected: postgres·apiserver·scheduler·dag-processor `healthy`, seaweedfs `Up`, airflow-init·lake-init `Exited (0)`

- [ ] **Step 2: I1 두 번째 init**

Run: `docker compose run --rm lake-init`
Expected: 종료 코드 0

- [ ] **Step 3: I2 smoke** (`lake.lab` 네임스페이스)

```bash
docker compose exec -T airflow-scheduler python -m pipeline.lake sql \
  "CREATE NAMESPACE IF NOT EXISTS lake.lab" \
  "CREATE TABLE lake.lab.smoke (id BIGINT, dt DATE) USING iceberg PARTITIONED BY (dt)" \
  "INSERT INTO lake.lab.smoke VALUES (1, DATE '2019-11-01'), (2, DATE '2019-11-02')" \
  "INSERT OVERWRITE lake.lab.smoke PARTITION (dt = DATE '2019-11-01') SELECT 3" \
  "INSERT OVERWRITE lake.lab.smoke PARTITION (dt = DATE '2019-11-01') SELECT 4" \
  "MERGE INTO lake.lab.smoke t USING (SELECT 2L AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE" \
  "SELECT * FROM lake.lab.smoke ORDER BY dt" \
  "SELECT snapshot_id, operation FROM lake.lab.smoke.snapshots ORDER BY committed_at" \
  "SELECT partition, file_path FROM lake.lab.smoke.files"
```
Expected: 최종 행 `(4, 2019-11-01)` 하나. 스냅숏 5개(append, overwrite, overwrite, overwrite(merge)…). 파티션당 파일 1개, 경로가 `s3://warehouse/lab/smoke/data/dt=…`.

이어서 시간 여행과 카탈로그 커밋 보기:
```bash
docker compose exec -T airflow-scheduler python -m pipeline.lake sql \
  "SELECT * FROM lake.lab.smoke VERSION AS OF $(첫 스냅숏 id)"
docker compose exec -T postgres psql -U airflow -d iceberg -c "select table_namespace, table_name, metadata_location from iceberg_tables"
```
Expected: 첫 스냅숏에는 행 1·2가 보인다. `metadata_location`이 `…/metadata/0000N-….metadata.json`이고 커밋마다 N이 는다.

### Task 6: 통합 확인 I3 — 체크섬 버그 재현과 우회

**Files:**
- Create: `bench/lake_commits.py`

- [ ] **Step 1: 스크립트**

```python
"""작은 커밋 N번을 해서 실패 수를 센다. SeaweedFS 체크섬 버그(#11582) 재현·우회 확인용. ADR 0006 결정 2.

    docker compose exec -T airflow-scheduler python bench/lake_commits.py 200
    (우회 끄기) docker compose exec -T -e AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_SUPPORTED \
        -e AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_SUPPORTED airflow-scheduler python bench/lake_commits.py 20
"""
import os
import sys

from pipeline.spark import get_spark

n = int(sys.argv[1])
spark = get_spark("lake-commits", lake=True)
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.lab")
spark.sql("CREATE TABLE IF NOT EXISTS lake.lab.ping (i INT) USING iceberg")
failed = []
for i in range(n):
    try:
        spark.sql(f"INSERT INTO lake.lab.ping VALUES ({i})")
    except Exception as e:  # 실패 종류를 센다(첫 줄만)
        failed.append(str(e).splitlines()[0][:160])
print(f"checksum={os.environ.get('AWS_REQUEST_CHECKSUM_CALCULATION')} commits={n} failed={len(failed)}")
for f in sorted(set(failed)):
    print(f"  {failed.count(f)}× {f}")
```
컨테이너에는 `bench/`가 마운트돼 있지 않다. Step 2에서 `docker compose cp`로 넣는다(또는 `-v`로 실행). 같은 파일을 넣는다.

- [ ] **Step 2: 실행**

```bash
docker compose cp bench/lake_commits.py airflow-scheduler:/tmp/lake_commits.py
docker compose exec -T -e AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_SUPPORTED -e AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_SUPPORTED airflow-scheduler python /tmp/lake_commits.py 20
docker compose exec -T airflow-scheduler python /tmp/lake_commits.py 200
```
Expected: 끄면 `failed > 0`이고 `InvalidDigest`가 보인다. 켜면 `failed=0`. 재현되지 않으면(수정이 이미 들어간 빌드 등) 그대로 기록하고 우회는 둔다.

### Task 7: 통합 확인 I4 — 커밋 중 강제 종료

- [ ] **Step 1: 11월 events를 실험 테이블에 쓰다가 죽이기**

```bash
docker compose exec -T airflow-scheduler python -m pipeline.lake sql \
  "CREATE TABLE IF NOT EXISTS lake.lab.events_nov USING iceberg PARTITIONED BY (dt) AS SELECT * FROM parquet.\`/opt/airflow/data/warehouse/events\` WHERE dt = '2019-11-01'"
docker compose exec -T airflow-scheduler python -m pipeline.lake sql "SELECT count(*) FROM lake.lab.events_nov"   # 실행 전 값
docker compose exec -d airflow-scheduler python -m pipeline.lake sql \
  "INSERT INTO lake.lab.events_nov SELECT * FROM parquet.\`/opt/airflow/data/warehouse/events\` WHERE dt BETWEEN '2019-11-02' AND '2019-11-30'"
# 데이터 파일을 쓰는 중(약 20~40초)에 JVM을 죽인다
sleep 25; docker compose exec -T airflow-scheduler bash -c 'kill -9 $(pidof java)'
docker compose exec -T airflow-scheduler python -m pipeline.lake sql "SELECT count(*) FROM lake.lab.events_nov"   # 실행 후 값
```
Expected: 실행 후 행 수 = 실행 전 행 수(커밋 전 상태 그대로).

- [ ] **Step 2: 고아 파일 보기(dry run)**

```bash
docker compose exec -T airflow-scheduler python - <<'PY'
from pipeline.spark import get_spark
s = get_spark("orphans", lake=True)
s.conf.set("spark.testing", "true")   # 24시간 안전장치를 이 실험 세션에서만 끈다(ADR 0006 결정 7)
s.sql("CALL lake.system.remove_orphan_files(table => 'lab.events_nov', older_than => current_timestamp(), dry_run => true, prefix_listing => true)").show(5, truncate=False)
print("고아 파일 수:", s.sql("CALL lake.system.remove_orphan_files(table => 'lab.events_nov', older_than => current_timestamp(), dry_run => true, prefix_listing => true)").count())
PY
```
Expected: 고아 파일이 1개 이상 보인다(죽기 전에 올린 데이터 파일). 프로시저 인자가 `current_timestamp()`를 받지 않으면 Python에서 만든 `TIMESTAMP '…'` 리터럴로 바꾼다.

- [ ] **Step 3: 실험 정리**

```bash
docker compose exec -T airflow-scheduler python -m pipeline.lake sql "DROP TABLE lake.lab.events_nov PURGE" "DROP TABLE lake.lab.smoke PURGE" "DROP TABLE lake.lab.ping PURGE"
```

### Task 8: 기록 — ADR 0006 채택, README, 질문지 04

**Files:**
- Modify: `docs/decisions/0006-stage3-storage.md`(측정 결과 절, 상태 채택), `README.md`(3단계 진행 상황, 초기화 절차, `jars/fetch.sh`)
- Create: `docs/blog-prompts/04-lakehouse-iceberg.md`

- [ ] **Step 1: ADR 0006에 '측정 (3-1)' 절**: I1~I4 결과, seaweedfs 쉴 때 메모리(`docker stats --no-stream`), 확인 목록 각 줄의 결과(맞음/틀림·대응). 상태를 `채택`으로.
- [ ] **Step 2: README**: 진행 상황 3단계 `진행 중(3-1 완료)`, 실행 방법에 `bash jars/fetch.sh`와 `.env` 키, ADR 0006의 초기화 절차 표를 옮긴다.
- [ ] **Step 3: 질문지 04**: CLAUDE.md 형식(개념 3~5, 질문 5개 안팎, 실험 1~2개, 자료 2~3개를 열어서 확인). 개념: 테이블 포맷과 스냅숏, 카탈로그 CAS 커밋, 오브젝트 저장소에 rename이 없음, 필터 덮어쓰기 vs dynamic(빈 날짜 함정), S3 호환 저장소의 체크섬 비호환. 실험: I2(psql로 metadata_location 보기), I4.
- [ ] **Step 4: 전체 확인**

Run: `.venv/bin/python -m pytest -q && docker compose ps -a --format '{{.Name}} {{.Status}}'`
Expected: `27 passed`, 서비스 상태 정상
