import json
import os

from pipeline.stream_sessions import start


def ev(t, user=1, etype="view"):
    return {"event_time": f"2026-10-12T{t}Z", "event_type": etype, "price": None, "user_id": user}


def stream_files(spark, ns, tmp_path, files, watermark=10):
    src = tmp_path / "in"
    src.mkdir(exist_ok=True)
    for i, events in enumerate(files):
        f = src / f"f{i:02d}.json"
        f.write_text("".join(json.dumps(e) + "\n" for e in events))
        os.utime(f, (1_000_000 + i, 1_000_000 + i))
    q = start(spark.readStream.option("maxFilesPerTrigger", 1).text(str(src)), "t", watermark, str(tmp_path / "ckpt"),
              ns, availableNow=True)
    q.awaitTermination()
    return [tuple(r) for r in spark.sql(
        f"SELECT user_id, date_format(session_start, 'HH:mm:ss'), date_format(session_end, 'HH:mm:ss'), n_events "
        f"FROM {ns}.rt_sessions ORDER BY user_id, session_start").collect()]


def test_gap_of_exactly_1800s_stays_in_session_and_1801s_splits_like_batch(spark, ns, tmp_path):
    rows = stream_files(spark, ns, tmp_path, [
        [ev("10:00:00"), ev("10:30:00"), ev("11:00:01")],  # 1,800초 → 같은 세션, 1,801초 → 새 세션(3단계 규칙)
        [ev("12:30:00", user=0, etype="tick")],             # 워터마크를 12:20으로 밀어 두 세션을 닫는다
    ])
    assert rows == [(1, "10:00:00", "10:30:00", 2), (1, "11:00:01", "11:00:01", 1)]


def test_late_event_inside_watermark_bridges_two_sessions_into_one(spark, ns, tmp_path):
    rows = stream_files(spark, ns, tmp_path, [
        [ev("10:00:00", user=2), ev("10:40:00", user=2)],  # 40분 차이 → 아직은 세션 둘
        [ev("10:20:00", user=2)],                         # 늦게 온 10:20이 둘을 잇는다(각 20분 차이)
        [ev("12:00:00", user=0, etype="tick")],
    ])
    assert rows == [(2, "10:00:00", "10:40:00", 3)]
