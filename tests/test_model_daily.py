from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

import pipeline.model_daily as md
from pipeline.load_events import SCHEMA
from pipeline.model_daily import input_hash, model

D1, D2 = date(2019, 11, 1), date(2019, 11, 2)


def ev(ts, *, user=7, session="s1", etype="view", product=1, cat=10, code="a.b", brand="x", price="10.00"):
    return (datetime.fromisoformat(ts + "+00:00"), etype, product, cat, code, brand, price, user, session)


def put(spark, wh, rows):
    # 1단계 적재와 같은 모양(dt 폴더 아래 Parquet)으로 events를 만든다
    from decimal import Decimal
    rows = [(*r[:6], Decimal(r[6]), *r[7:]) for r in rows]
    (spark.createDataFrame(rows, SCHEMA).withColumn("dt", F.to_date("event_time"))
     .write.partitionBy("dt").mode("overwrite").option("partitionOverwriteMode", "dynamic").parquet(wh))


@pytest.fixture(autouse=True)
def loose_thresholds(monkeypatch):
    # 몇 줄짜리 데이터는 user_session과의 일치율이 기준(규칙 2)에 못 미친다. 규칙 2를 보는 테스트만 기준을 되돌린다
    monkeypatch.setattr(md, "P_MIN", 0.0)
    monkeypatch.setattr(md, "R_MIN", 0.0)


@pytest.fixture
def wh(tmp_path):
    return str(tmp_path / "events")


def sessions(spark, ns):
    return sorted(spark.table(f"{ns}.fct_sessions").collect(), key=lambda r: r.session_start)


def test_gap_of_exactly_1800s_stays_in_session_1801s_splits(spark, ns, wh):
    put(spark, wh, [ev("2019-11-01 10:00:00"), ev("2019-11-01 10:30:00"),       # 정확히 1,800초 → 같은 세션
                    ev("2019-11-01 11:00:01", etype="purchase", price="5.50")])  # 1,801초 → 새 세션
    model(spark, wh, [D1], ns)
    s = sessions(spark, ns)
    assert [(r.n_events, r.n_view, r.n_purchase) for r in s] == [(2, 2, 0), (1, 0, 1)]
    assert s[0].session_id == "7-1572602400"   # "{user_id}-{세션 시작 epoch초}"
    assert str(s[1].purchase_amount) == "5.50"


def test_session_is_cut_at_utc_midnight(spark, ns, wh):
    put(spark, wh, [ev("2019-11-01 23:50:00"), ev("2019-11-02 00:10:00")])  # 20분 간격이지만 날짜가 다르다
    model(spark, wh, [D1, D2], ns)
    assert [(r.dt, r.n_events) for r in sessions(spark, ns)] == [(D1, 1), (D2, 1)]


def test_validation_columns_count_tp_fp_fn_against_user_session(spark, ns, wh):
    put(spark, wh, [
        ev("2019-11-01 10:00:00", session="a"),
        ev("2019-11-01 10:05:00", session="b"),   # user_session만 바뀜 → 우리 세션 안의 변화(FN)
        ev("2019-11-01 12:00:00", session="c"),   # 둘 다 나눔 → TP
        ev("2019-11-01 14:00:00", session="c"),   # 우리만 나눔 → FP
    ])
    model(spark, wh, [D1], ns)
    s = sessions(spark, ns)
    assert [(r.src_changed_at_start, r.src_changes_inside) for r in s] == [(None, 1), (True, 0), (False, 0)]


def test_product_snapshot_takes_last_non_null_brand_of_the_day(spark, ns, wh):
    put(spark, wh, [
        ev("2019-11-01 10:00:00", product=1, brand="acer"),
        ev("2019-11-01 11:00:00", product=1, brand="lenovo"),
        ev("2019-11-01 12:00:00", product=1, brand=None),   # null은 '관측 못 함'이라 마지막 값이 아니다
        ev("2019-11-01 10:00:00", product=2, brand=None, code=None),
    ])
    model(spark, wh, [D1], ns)
    rows = sorted((r.product_id, r.category_code, r.brand) for r in spark.table(f"{ns}.product_daily").collect())
    assert rows == [(1, "a.b", "lenovo"), (2, None, None)]


def test_two_categories_for_one_product_day_fails_and_writes_nothing(spark, ns, wh):
    put(spark, wh, [ev("2019-11-01 10:00:00", cat=10), ev("2019-11-01 11:00:00", cat=11)])  # 규칙 3 위반
    with pytest.raises(ValueError, match="카테고리"):
        model(spark, wh, [D1], ns)
    assert spark.table(f"{ns}.fct_sessions").count() == 0
    assert spark.table(f"{ns}.product_daily").count() == 0


def test_low_precision_fails_check(spark, ns, wh, monkeypatch):
    monkeypatch.setattr(md, "P_MIN", 0.9)
    # user_session이 한 번도 안 바뀌는데 우리는 나눈다 → 정밀도 0 (규칙 2)
    put(spark, wh, [ev("2019-11-01 10:00:00"), ev("2019-11-01 12:00:00"), ev("2019-11-01 14:00:00")])
    with pytest.raises(ValueError, match="정밀도"):
        model(spark, wh, [D1], ns)


def test_rerun_is_idempotent_and_multi_day_equals_day_by_day(spark, ns, wh):
    put(spark, wh, [ev("2019-11-01 10:00:00", user=1), ev("2019-11-01 23:59:00", user=2, session="z"),
                    ev("2019-11-02 00:01:00", user=2, session="z")])

    def snap():
        return sorted((r.session_id, r.dt, r.n_events) for r in spark.table(f"{ns}.fct_sessions").collect())

    model(spark, wh, [D1, D2], ns)
    multi = snap()
    model(spark, wh, [D1], ns)
    model(spark, wh, [D2], ns)
    assert snap() == multi and len(multi) == 3


def test_src_build_is_input_hash_plus_run_id_and_changes_when_events_rewritten(spark, ns, wh):
    put(spark, wh, [ev("2019-11-01 10:00:00")])
    h1 = input_hash(wh, D1)
    model(spark, wh, [D1], ns)
    builds = {r.src_build for t in ("fct_sessions", "product_daily") for r in spark.table(f"{ns}.{t}").collect()}
    assert len(builds) == 1 and builds.pop().startswith(h1 + "-")   # 두 테이블이 같은 값 하나
    put(spark, wh, [ev("2019-11-01 10:00:00")])                     # 내용은 같아도 다시 쓰면 파일 이름이 바뀐다
    assert input_hash(wh, D1) != h1


def test_dry_run_writes_nothing_and_returns_metrics(spark, ns, wh):
    put(spark, wh, [ev("2019-11-01 10:00:00"), ev("2019-11-01 10:20:00")])
    m = model(spark, wh, [D1], ns, gap_minutes=10, dry_run=True)
    assert m["sessions"] == 2
    assert spark.table(f"{ns}.fct_sessions").count() == 0
