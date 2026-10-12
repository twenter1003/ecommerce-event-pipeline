from datetime import date

import os
import shutil

import pytest

from pipeline.load_events import SCHEMA, load
from pipeline.quality import check
from pipeline.split_raw import split

HEADER = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session"
D1, D2 = date(2019, 10, 1), date(2019, 10, 2)


def row(ts="2019-10-01 10:00:00", session="s1"):
    return f"{ts} UTC,view,1,2053013555631882655,a.b,x,10.50,7,{session}"


@pytest.fixture
def paths(tmp_path):
    return str(tmp_path / "landing"), str(tmp_path / "warehouse")


def put(landing, d, rows):
    path = f"{landing}/dt={d}"
    os.makedirs(path, exist_ok=True)
    with open(f"{path}/part-0.csv", "w") as f:
        f.write("\n".join([HEADER, *rows]) + "\n")


def count(spark, warehouse, d):
    return spark.read.parquet(warehouse).filter(f"dt = '{d}'").count()


def test_only_exact_duplicates_are_removed(spark, paths):
    landing, wh = paths
    put(landing, D1, [row(), row(), row(session="s2")])
    load(spark, landing, wh, [D1])
    assert count(spark, wh, D1) == 2


def test_rerun_gives_same_result(spark, paths, tmp_path):
    landing, wh = paths
    put(landing, D1, [row(), row(session="s2")])
    load(spark, landing, wh, [D1])
    load(spark, landing, wh, [D1])
    assert count(spark, wh, D1) == 2
    assert len(list((tmp_path / "warehouse" / f"dt={D1}").glob("*.parquet"))) == 1


def test_reload_replaces_only_requested_day(spark, paths):
    landing, wh = paths
    put(landing, D1, [row()])
    put(landing, D2, [row("2019-10-02 10:00:00")])
    load(spark, landing, wh, [D1, D2])
    put(landing, D1, [row(), row(session="s2")])  # 원천이 10/1 파일을 고쳐서 다시 보냈다
    load(spark, landing, wh, [D1])
    assert count(spark, wh, D1) == 2
    assert count(spark, wh, D2) == 1


def test_stray_row_does_not_overwrite_other_day(spark, paths):
    landing, wh = paths
    put(landing, D2, [row("2019-10-02 10:00:00"), row("2019-10-02 11:00:00")])
    load(spark, landing, wh, [D2])
    put(landing, D1, [row(), row("2019-10-02 12:00:00")])  # 10/1 파일에 10/2 행이 섞였다
    load(spark, landing, wh, [D1])
    assert count(spark, wh, D1) == 1
    assert count(spark, wh, D2) == 2


def test_event_time_is_read_as_utc(spark, paths):
    landing, wh = paths
    put(landing, D1, [row("2019-10-01 23:59:59")])  # KST로는 10/2 08:59
    load(spark, landing, wh, [D1])
    # dt만 보면 세션 시간대가 KST여도 10/1로 맞게 나온다(읽기와 날짜 계산이 같은 시간대라서).
    # 틀어지는 건 저장된 절대 시각이라, 시간대와 무관한 epoch 초로 확인한다.
    (epoch,) = spark.read.parquet(wh).selectExpr("unix_timestamp(event_time)").first()
    assert epoch == 1569974399  # 2019-10-01T23:59:59Z
    assert count(spark, wh, D1) == 1


def test_malformed_row_fails_loudly(spark, paths):
    landing, wh = paths
    put(landing, D1, [row(), "not-a-time,view,1,1,,,1.00,1,s"])
    with pytest.raises(Exception):
        load(spark, landing, wh, [D1])


def test_split_keeps_every_raw_row(spark, tmp_path):
    raw = tmp_path / "raw.csv"
    raw.write_text("\n".join([HEADER, row(), row(), row("2019-10-02 00:00:00")]) + "\n")
    landing = str(tmp_path / "landing")
    split(spark, str(raw), landing)
    df = spark.read.option("header", True).csv(landing)
    assert df.filter(f"dt = '{D1}'").count() == 2  # 중복도 그대로 남긴다
    assert df.filter(f"dt = '{D2}'").count() == 1


def test_empty_resend_fails_instead_of_keeping_old_day(spark, paths):
    landing, wh = paths
    put(landing, D1, [row(), row(session="s2")])
    load(spark, landing, wh, [D1])
    put(landing, D1, [])  # 원천이 10/1을 헤더만 있는 빈 파일로 다시 보냈다
    with pytest.raises(ValueError, match="행이 없는 날짜"):
        load(spark, landing, wh, [D1])
    assert count(spark, wh, D1) == 2  # 실패했으니 옛 데이터는 그대로다


def test_reordered_header_fails(spark, paths):
    landing, wh = paths
    swapped = HEADER.replace("product_id", "TMP").replace("user_id", "product_id").replace("TMP", "user_id")
    os.makedirs(f"{landing}/dt={D1}")
    with open(f"{landing}/dt={D1}/part-0.csv", "w") as f:
        f.write(f"{swapped}\n{row()}\n")
    with pytest.raises(Exception, match="header"):
        load(spark, landing, wh, [D1])


def test_split_rejects_file_with_two_months(spark, tmp_path):
    raw = tmp_path / "raw.csv"
    raw.write_text("\n".join([HEADER, row(), row("2019-11-01 00:00:00")]) + "\n")
    with pytest.raises(ValueError, match="여러 달"):
        split(spark, str(raw), str(tmp_path / "landing"))


def test_split_keeps_surrounding_spaces(spark, tmp_path):
    raw = tmp_path / "raw.csv"
    raw.write_text("\n".join([HEADER, row().replace(",x,", ", x ,")]) + "\n")
    landing = str(tmp_path / "landing")
    split(spark, str(raw), landing)
    (brand,) = spark.read.option("header", True).csv(landing).select("brand").first()
    assert brand == " x "


def failed_checks(spark, landing, wh, d):
    return [name for name, ok, _ in check(spark, landing, wh, d) if not ok]


def test_quality_passes_on_clean_day(spark, paths):
    landing, wh = paths
    put(landing, D1, [row(), row(), row(session="s2")])  # 완전 중복은 적재가 지우니 문제가 아니다
    load(spark, landing, wh, [D1])
    assert failed_checks(spark, landing, wh, D1) == []


def test_quality_catches_row_silently_dropped_by_load(spark, paths):
    landing, wh = paths
    put(landing, D1, [row(), ",view,1,1,,,1.00,1,s"])  # 빈 event_time은 null이 돼 적재에서 조용히 빠진다
    load(spark, landing, wh, [D1])
    assert failed_checks(spark, landing, wh, D1) == ["warehouse 행 수 = landing 고유 행 수"]


def test_quality_catches_rows_merged_by_price_rounding(spark, paths):
    landing, wh = paths
    put(landing, D1, [row().replace("10.50", "12.345"), row().replace("10.50", "12.35")])  # 둘 다 12.35가 된다
    load(spark, landing, wh, [D1])
    assert failed_checks(spark, landing, wh, D1) == ["warehouse 행 수 = landing 고유 행 수", "가격이 적재 때 반올림되지 않았다"]


def test_quality_catches_price_rounded_without_merging(spark, paths):
    landing, wh = paths
    # 12.345는 12.35로 바뀐다. 합쳐질 짝이 없어 행 수 대조는 통과하므로 반올림 규칙이 잡아야 한다.
    # 12.340은 자릿수는 셋이지만 값이 그대로라 실패로 치지 않는다.
    put(landing, D1, [row().replace("10.50", "12.345"), row(session="s2").replace("10.50", "12.340")])
    load(spark, landing, wh, [D1])
    assert failed_checks(spark, landing, wh, D1) == ["가격이 적재 때 반올림되지 않았다"]


def test_quality_catches_stale_partition(spark, paths):
    landing, wh = paths
    put(landing, D1, [row(), row(session="s2")])
    load(spark, landing, wh, [D1])
    put(landing, D1, [row()])  # landing이 바뀌었는데 다시 적재하지 않았다
    assert failed_checks(spark, landing, wh, D1) == ["warehouse 행 수 = landing 고유 행 수"]


def test_quality_catches_domain_violations(spark, paths):
    landing, wh = paths
    bad = row().replace(",view,", ",refund,").replace("10.50", "-1.00")
    put(landing, D1, [row(), bad])
    load(spark, landing, wh, [D1])
    assert failed_checks(spark, landing, wh, D1) == ["event_type이 view/cart/purchase 중 하나다", "가격이 0 이상이다"]


def test_quality_catches_missing_required_value(spark, paths):
    landing, wh = paths
    put(landing, D1, [row().replace(",7,", ",,")])  # user_id가 비었다
    load(spark, landing, wh, [D1])
    assert failed_checks(spark, landing, wh, D1) == ["필수 컬럼에 null이 없다"]


def test_quality_catches_corrupted_partition(spark, paths, tmp_path):
    landing, wh = paths
    put(landing, D1, [row()])
    put(landing, D2, [row("2019-10-02 10:00:00")])
    load(spark, landing, wh, [D1, D2])
    d1, d2 = tmp_path / "warehouse" / f"dt={D1}", tmp_path / "warehouse" / f"dt={D2}"
    # 적재를 거치지 않고 파일을 손으로 복사해 망가뜨린다: 10/1에 같은 파일 한 벌, 10/2 파일 한 벌
    shutil.copy(next(d1.glob("*.parquet")), d1 / "dup.parquet")
    shutil.copy(next(d2.glob("*.parquet")), d1 / "stray.parquet")
    assert failed_checks(spark, landing, wh, D1) == [
        "warehouse 행 수 = landing 고유 행 수",
        "완전 중복 행이 없다",
        "event_time의 UTC 날짜가 파티션 날짜와 같다",
    ]


def test_quality_fails_loudly_when_day_was_never_loaded(spark, paths):
    landing, wh = paths
    put(landing, D2, [row("2019-10-02 10:00:00")])
    load(spark, landing, wh, [D2])
    put(landing, D1, [row()])  # landing은 왔는데 10/1 적재는 한 번도 안 돌았다
    with pytest.raises(Exception, match="PATH_NOT_FOUND"):
        check(spark, landing, wh, D1)


def test_quality_catches_empty_partition(spark, paths):
    landing, wh = paths
    put(landing, D1, [])  # landing이 비었다
    empty = spark.read.schema(SCHEMA).option("header", True).csv(f"{landing}/dt={D1}")
    empty.write.parquet(f"{wh}/dt={D1}")  # 행 0개짜리 날짜 폴더
    # 행 수 대조는 0 = 0으로 통과해 버린다. "한 행 이상" 규칙이 잡아야 한다.
    assert failed_checks(spark, landing, wh, D1) == ["행이 하나 이상 있다"]
