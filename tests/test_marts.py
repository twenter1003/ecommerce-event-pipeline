import shutil
from datetime import date

import pytest

import pipeline.marts as mt
from pipeline.marts import FOREVER, run, stale_dates
from pipeline.model_daily import input_hash

D1, D2, D3, D4, D5 = (date(2019, 11, d) for d in range(1, 6))


def lit(v):
    return "NULL" if v is None else f"DATE '{v}'" if isinstance(v, date) else f"'{v}'" if isinstance(v, str) else str(v)


@pytest.fixture
def wh(tmp_path):
    return tmp_path / "events"


def day(spark, ns, wh, d, products=((1, 10, "a.b", "x"),), sessions=((7, 1, 0, 0, None),), run="r1", s_run=None):
    """하루치 events 폴더와, 그 폴더 해시를 src_build로 단 lake 행을 넣는다. 관문은 파일 이름만 보므로 빈 파일이면 된다.

    products: (product_id, category_id, category_code, brand)
    sessions: (user_id, n_view, n_cart, n_purchase, purchase_amount 문자열)
    """
    folder = wh / f"dt={d}"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "part-0.parquet").touch()
    h = input_hash(str(wh), d)
    rows = ", ".join(f"({lit(d)}, {p}, {c}, {lit(code)}, {lit(b)}, '{h}-{run}')" for p, c, code, b in products)
    spark.sql(f"INSERT INTO {ns}.product_daily VALUES {rows}")
    rows = ", ".join(
        f"('{u}-{i}', {u}, TIMESTAMP '{d} 10:00:00', TIMESTAMP '{d} 10:10:00', {v + c + p}, {v}, {c}, {p}, "
        f"{amt or 'NULL'}, NULL, 0, '{h}-{s_run or run}', {lit(d)})"
        for i, (u, v, c, p, amt) in enumerate(sessions))
    spark.sql(f"INSERT INTO {ns}.fct_sessions VALUES {rows}")


def dim(spark, ns):
    return sorted((r.product_id, r.valid_from, r.valid_to, r.is_current, r.category_id, r.category_l1, r.brand)
                  for r in spark.table(f"{ns}.dim_product").collect())


def test_scd2_versions_follow_category_and_brand_rules(spark, ns, wh):
    day(spark, ns, wh, D1, products=[(1, 10, "a.b", None), (2, 20, None, "acer")])
    day(spark, ns, wh, D2, products=[(1, 10, "a.b", "acer"), (2, 20, None, "lenovo")])  # 1: null→값은 새 버전
    day(spark, ns, wh, D3, products=[(1, 10, "a.b", None), (2, 20, None, "acer")])      # 1: 값→null은 변화 없음
    day(spark, ns, wh, D4, products=[(2, 20, None, "acer")])                             # 1: 관측 없는 날은 앞 버전이 덮는다
    day(spark, ns, wh, D5, products=[(1, 11, "c.d", None)])                              # 1: 카테고리 변경, brand는 이어 쓴다
    run(spark, str(wh), ns)
    assert dim(spark, ns) == [
        (1, D1, D2, False, 10, "a", None),
        (1, D2, D5, False, 10, "a", "acer"),
        (1, D5, FOREVER, True, 11, "c", "acer"),
        (2, D1, D2, False, 20, None, "acer"),
        (2, D2, D3, False, 20, None, "lenovo"),
        (2, D3, FOREVER, True, 20, None, "acer"),  # acer로 되돌아와도 첫 acer 버전과 키가 다르다
    ]
    assert spark.table(f"{ns}.dim_product").select("product_sk").distinct().count() == 6


def test_second_run_commits_nothing_to_dim(spark, ns, wh):
    day(spark, ns, wh, D1)
    first = run(spark, str(wh), ns)
    before = mt.current_snapshot(spark, f"{ns}.dim_product")
    second = run(spark, str(wh), ns)
    assert (first["inserted"], second["inserted"], second["updated"], second["deleted"]) == (1, 0, 0, 0)
    assert mt.current_snapshot(spark, f"{ns}.dim_product") == before  # 차이가 0이면 MERGE를 하지 않는다


def test_late_day_rewrites_history_with_insert_update_delete(spark, ns, wh):
    # 섞인 순서의 백필: D1·D3을 먼저 처리하고 D2가 나중에 온다
    day(spark, ns, wh, D1, products=[(1, 10, "a.b", "acer")])
    day(spark, ns, wh, D3, products=[(1, 10, "a.b", "lenovo")])
    run(spark, str(wh), ns)
    day(spark, ns, wh, D2, products=[(1, 10, "a.b", "lenovo")])
    m = run(spark, str(wh), ns)
    assert (m["inserted"], m["updated"], m["deleted"]) == (1, 1, 1)  # D2 버전 추가, D1 버전 닫는 날 변경, D3 버전 삭제
    assert dim(spark, ns) == [(1, D1, D2, False, 10, "a", "acer"), (1, D2, FOREVER, True, 10, "a", "lenovo")]


def test_gate_finds_every_kind_of_stale_day(spark, ns, wh):
    for d in (D1, D2, D3):
        day(spark, ns, wh, d)
    day(spark, ns, wh, D4, s_run="r2")                       # 두 테이블의 실행 id가 다르다(한쪽만 커밋하고 죽음)
    (wh / f"dt={D2}" / "part-0.parquet").rename(wh / f"dt={D2}" / "part-1.parquet")  # model 뒤에 events가 다시 써짐
    shutil.rmtree(wh / f"dt={D3}")                           # lake에만 있는 날짜
    (wh / f"dt={D5}").mkdir()                                # events에는 있는데 lake에 없는 날짜
    stale = stale_dates(str(wh), spark.table(f"{ns}.fct_sessions"), spark.table(f"{ns}.product_daily"))
    assert stale == [D2, D3, D4, D5]
    assert mt.spans([D1, D2, D4]) == "2019-11-01~2019-11-02, 2019-11-04"  # 실패 메시지는 다시 돌릴 범위로 묶는다


def test_stale_gate_writes_nothing_and_still_expires_snapshots(spark, ns, wh):
    day(spark, ns, wh, D1)
    for _ in range(12):
        spark.sql(f"INSERT INTO {ns}.mart_cohort_monthly VALUES ({lit(D1)}, {lit(D1)}, 0, 1, 1, 0, true)")
    (wh / f"dt={D1}" / "part-0.parquet").rename(wh / f"dt={D1}" / "part-1.parquet")
    with pytest.raises(ValueError, match=f"신선도 관문.*{D1}"):
        run(spark, str(wh), ns)
    assert spark.table(f"{ns}.dim_product").count() == 0
    assert spark.table(f"{ns}.mart_funnel_daily").count() == 0
    # 만료는 finally에서 돈다. 실패한 run에서도 스냅숏이 10개로 줄어야 한다
    assert spark.sql(f"SELECT count(*) FROM {ns}.mart_cohort_monthly.snapshots").first()[0] == 10


def test_dim_checks_run_before_merge(spark, ns, wh, monkeypatch):
    day(spark, ns, wh, D1)
    real = mt.desired_dim
    monkeypatch.setattr(mt, "desired_dim", lambda p: real(p).unionAll(real(p)))  # 같은 버전이 두 번 → 키 중복
    with pytest.raises(ValueError, match="검사 1"):
        run(spark, str(wh), ns)
    assert spark.table(f"{ns}.dim_product").count() == 0


def test_reconciliation_catches_a_merge_that_did_not_apply(spark, ns, wh, monkeypatch):
    day(spark, ns, wh, D1)
    monkeypatch.setattr(mt, "merge_diff", lambda spark, want, table: {"inserted": 1, "updated": 0, "deleted": 0})
    with pytest.raises(ValueError, match="검사 5"):
        run(spark, str(wh), ns)


def test_funnel_counts_sessions_by_presence_of_each_event_type(spark, ns, wh):
    day(spark, ns, wh, D1, sessions=[
        (1, 2, 0, 0, None),     # 보기만
        (1, 1, 1, 1, "10.50"),  # 보기 → 장바구니 → 구매
        (2, 0, 0, 1, "5.00"),   # 장바구니 없이 구매
        (3, 0, 2, 0, None),     # 장바구니만
    ])
    run(spark, str(wh), ns)
    r = spark.table(f"{ns}.mart_funnel_daily").first()
    assert (r.dt, r.sessions, r.s_view, r.s_view_cart, r.s_view_cart_purchase, r.s_purchase, r.s_purchase_no_cart,
            r.users, r.buyers, r.cart_events, r.purchase_events, str(r.revenue)) == (
        D1, 4, 2, 1, 1, 2, 1, 3, 2, 3, 2, "15.50")


def test_cohort_is_first_session_month_and_first_month_is_left_censored(spark, ns, wh):
    nov, dec = date(2019, 11, 5), date(2019, 12, 3)
    day(spark, ns, wh, nov, sessions=[(1, 1, 0, 0, None), (3, 1, 0, 0, None)])
    day(spark, ns, wh, dec, sessions=[(1, 0, 0, 1, "9.00"), (2, 1, 0, 0, None)])
    run(spark, str(wh), ns)
    N, D = date(2019, 11, 1), date(2019, 12, 1)
    # (코호트 달, 활동 달, 지난 달 수, 코호트 크기, 활동 사용자, 구매자, 좌측 절단)
    assert sorted(tuple(r) for r in spark.table(f"{ns}.mart_cohort_monthly").collect()) == [
        (N, N, 0, 2, 2, 0, True), (N, D, 1, 2, 1, 1, True), (D, D, 0, 1, 1, 0, False)]
