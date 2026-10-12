import json
import os

from pipeline.stream_funnel import dropped, progress, start


def ev(t, etype="view", price=None):
    return {"event_time": f"2026-10-12T{t}Z", "event_type": etype, "price": price, "user_id": 1}


def tick(t):
    return ev(t, "tick")


def stream_files(spark, ns, tmp_path, files, watermark=10, name="ckpt"):
    """파일 하나 = 마이크로배치 하나로 흘려 보내고, 들어온 것을 다 처리하면 멈춘다(availableNow)."""
    src = tmp_path / "in"
    src.mkdir(exist_ok=True)
    for i, events in enumerate(files):
        f = src / f"f{i:02d}.json"
        if not f.exists():
            f.write_text("".join(json.dumps(e) + "\n" for e in events))
            os.utime(f, (1_000_000 + i, 1_000_000 + i))  # 파일 소스는 수정 시각 순서로 읽는다
    raw = spark.readStream.option("maxFilesPerTrigger", 1).text(str(src))
    q = start(raw, "t", watermark, str(tmp_path / name), ns, availableNow=True)
    q.awaitTermination()
    # collect()는 타임스탬프를 파이썬 쪽 지역 시간(KST)으로 바꾼다. 세션 시간대(UTC) 그대로 문자열로 뽑는다
    rows = spark.sql(f"SELECT date_format(window_start, 'HH:mm') AS w, events, views, carts, purchases, revenue "
                     f"FROM {ns}.rt_funnel_minute ORDER BY window_start").collect()
    return q, [(r.w, r.events, r.views, r.carts, r.purchases, str(r.revenue)) for r in rows]


def test_minute_funnel_and_tick_flushes_last_windows_without_being_counted(spark, ns, tmp_path):
    _, rows = stream_files(spark, ns, tmp_path, [
        [ev("10:00:05"), ev("10:00:30"), ev("10:00:59", "cart"),
         ev("10:01:10", "purchase", "10.50"), ev("10:01:20", "purchase", "5.00")],
        [tick("10:31:20")],  # 워터마크를 10:21:20으로 밀어 두 윈도를 내보낸다
    ])
    assert rows == [("10:00", 3, 2, 1, 0, "0.00"), ("10:01", 2, 0, 0, 2, "15.50")]  # 틱 윈도는 없다


def test_event_later_than_watermark_is_dropped_and_reported(spark, ns, tmp_path):
    q, rows = stream_files(spark, ns, tmp_path, [
        [ev("10:00:00"), ev("10:30:00")],  # 이 배치가 끝나면 워터마크 = 10:30 − 10분 = 10:20
        [ev("10:31:00")],                  # 늦은 이벤트를 거를 때는 한 배치 전의 워터마크를 쓴다(Spark 3.4+)
        [ev("10:05:00"), ev("10:25:00")],  # 그래서 여기서야 10:20이 적용된다. 10:05는 버리고, 10:25는 늦게 와도 넣는다
        [tick("11:30:00")],
    ])
    assert rows == [("10:00", 1, 1, 0, 0, "0.00"), ("10:25", 1, 1, 0, 0, "0.00"),
                    ("10:30", 1, 1, 0, 0, "0.00"), ("10:31", 1, 1, 0, 0, "0.00")]
    assert dropped(progress(q)) == 1


def test_late_event_right_after_watermark_moves_is_still_kept(spark, ns, tmp_path):
    # 워터마크가 오른 바로 다음 배치에서는 아직 옛 워터마크로 거른다. 그래서 10:05가 들어간다(위 테스트와 대조)
    q, rows = stream_files(spark, ns, tmp_path, [
        [ev("10:00:00"), ev("10:30:00")],
        [ev("10:05:00")],
        [tick("11:30:00")],
    ])
    assert [r[0] for r in rows] == ["10:00", "10:05", "10:30"]
    assert dropped(progress(q)) == 0


def test_restart_from_checkpoint_writes_only_new_windows(spark, ns, tmp_path):
    files = [[ev("10:00:00")], [tick("10:30:00")]]
    stream_files(spark, ns, tmp_path, files)
    _, again = stream_files(spark, ns, tmp_path, files)          # 같은 체크포인트, 새 입력 없음 → 아무것도 더 쓰지 않는다
    assert again == [("10:00", 1, 1, 0, 0, "0.00")]
    _, more = stream_files(spark, ns, tmp_path, files + [[ev("10:31:00")], [tick("11:00:00")]])
    assert more == [("10:00", 1, 1, 0, 0, "0.00"), ("10:31", 1, 1, 0, 0, "0.00")]


def test_dropped_metric_counts_pre_aggregated_rows_not_events(spark, ns, tmp_path):
    # 같은 윈도의 늦은 이벤트 3개가 한 배치로 오면, 상태 연산 앞의 부분 집계가 먼저 1행으로 묶는다.
    # 그래서 Spark가 보고하는 '워터마크로 버린 행'은 이벤트 수(3)가 아니라 묶인 행 수(1)다
    q, rows = stream_files(spark, ns, tmp_path, [
        [ev("10:00:00"), ev("10:30:00")],
        [ev("10:31:00")],
        [ev("10:05:00"), ev("10:05:10"), ev("10:05:20")],
        [tick("11:30:00")],
    ])
    assert [r[0] for r in rows] == ["10:00", "10:30", "10:31"]  # 10:05의 세 이벤트는 모두 사라졌다
    assert dropped(progress(q)) == 1


def test_judge_allows_missing_events_only_where_events_were_sent_past_the_watermark():
    from collections import Counter

    from pipeline.stream_funnel import judge
    w = lambda e, v=0: (e, v, 0, 0, 0)  # (events, views, carts, purchases, revenue)
    very = Counter({"10:01": 2})
    assert judge([("10:00", w(5, 5), w(5, 5)), ("10:01", w(7, 7), w(5, 5))], very)[0]   # 10:01은 2개까지 빠져도 된다
    assert not judge([("10:01", w(7, 7), w(4, 4))], very)[0]                            # 3개 빠짐 > 늦춘 2개
    assert not judge([("10:00", w(5, 5), w(6, 6))], very)[0]                            # 실시간이 더 크다 = 중복
    assert not judge([("10:00", w(5, 5), w(5, 4))], very)[0]                            # 늦춘 것 없는데 값이 다르다
    assert not judge([("10:02", None, w(1, 1))], very)[0]                               # 배치에 없는 윈도
