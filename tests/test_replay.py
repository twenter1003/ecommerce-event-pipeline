from datetime import datetime, timedelta, timezone

from pipeline.replay import LATE, VERY_LATE, plan, with_tick

T0 = datetime(2019, 11, 16, 8, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 12, 6, 0, tzinfo=timezone.utc)


def rows(n):
    # 1초 간격 이벤트 n개. i로 원래 위치를 되찾는다
    return [(T0 + timedelta(seconds=i), {"i": i, "user_id": 100 + i % 7, "event_type": "view"}) for i in range(n)]


def lags(messages, speed):
    # 늦춘 길이(이벤트 시간, 초) = 보낸 시점 × k − 원래 위치
    return [at * speed - v["i"] for at, _k, v in messages]


def test_shift_is_one_constant_delta_and_order_is_kept_without_lateness():
    messages, s = plan(rows(50), NOW, speed=60, late=0, very_late=0, seed=1)
    assert [v["i"] for _at, _k, v in messages] == list(range(50))
    assert [at for at, _k, _v in messages] == [i / 60 for i in range(50)]  # 보내는 간격만 1/k로 준다
    first, last = messages[0][2], messages[-1][2]
    assert (first["event_time"], first["src_event_time"]) == ("2026-10-12T06:00:00Z", "2019-11-16T08:00:00Z")
    assert last["event_time"] == "2026-10-12T06:00:49Z"  # 간격(49초)이 그대로다
    assert s == {"rows": 50, "n_late": 0, "n_very_late": 0, "delta_seconds": (NOW - T0).total_seconds()}


def test_lateness_is_seeded_bounded_and_keeps_event_time():
    n, speed = 20_000, 60
    messages, s = plan(rows(n), NOW, speed, late=0.05, very_late=0.005, seed=1)
    # 비율: 기댓값 ± 4σ (late 1000±31, very_late 100±10)
    assert 870 <= s["n_late"] <= 1130 and 60 <= s["n_very_late"] <= 140
    lag = lags(messages, speed)
    on_time = sum(1 for x in lag if abs(x) < 1e-6)
    late = [x for x in lag if 1e-6 <= x <= LATE[1]]
    very = [x for x in lag if VERY_LATE[0] <= x <= VERY_LATE[1]]
    assert on_time + len(late) + len(very) == n           # 늦춤은 두 범위 중 하나에만 든다
    assert len(very) == s["n_very_late"]
    # 늦게 보내도 이벤트 시각은 원래 시각 + Δ 그대로다
    for _at, _k, v in messages[:200]:
        assert v["event_time"] == (T0 + timedelta(seconds=v["i"]) + (NOW - T0)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # 같은 시드면 같고, 다른 시드면 다르다
    assert plan(rows(n), NOW, speed, 0.05, 0.005, seed=1)[0] == messages
    assert plan(rows(n), NOW, speed, 0.05, 0.005, seed=2)[0] != messages


def test_late_events_arrive_after_later_events_and_key_is_user_id():
    messages, _ = plan(rows(2_000), NOW, speed=60, late=0.05, very_late=0.005, seed=3)
    order = [v["i"] for _at, _k, v in messages]
    assert order != sorted(order)                         # 순서가 실제로 뒤바뀐다
    assert all(k == v["user_id"] for _at, k, v in messages)  # 같은 사용자는 같은 키 → 같은 파티션


def test_tick_goes_last_and_jumps_event_time_past_the_watermark():
    messages, _ = plan(rows(100), NOW, speed=60, late=0.05, very_late=0.005, seed=4)
    ticked = with_tick(messages, minutes=30)
    at, key, tick = ticked[-1]
    assert ticked[:-1] == messages and key == "tick" and at == messages[-1][0]  # 모든 이벤트를 보낸 뒤에 간다
    last = max(v["event_time"] for _a, _k, v in messages)
    assert (last, tick["event_time"]) == ("2026-10-12T06:01:39Z", "2026-10-12T06:31:39Z")  # 마지막 이벤트 + 30분
