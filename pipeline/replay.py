"""warehouse의 한 시간 구간을 Kafka 토픽 events로 재생한다. 설계는 ADR 0009 결정 2·3.

모든 이벤트 시각에 같은 Δ를 더해 '지금'으로 옮기고, 보내는 간격만 1/k로 줄인다(k배속).
일부 이벤트는 일부러 늦게 보낸다. 시드가 같으면 어떤 이벤트를 얼마나 늦출지도 같다.

    python -m pipeline.replay --warehouse /opt/airflow/data/warehouse/events --date 2019-11-16 --start 08:00 --minutes 120
    python -m pipeline.replay ... --dry-run     보내지 않고 계획의 숫자만 본다(Kafka 불필요)
"""
import argparse
import json
import os
import random
import time
from datetime import date, datetime, timedelta, timezone

TOPIC = "events"
PARTITIONS = 6  # 같은 그룹의 컨슈머를 6개까지 늘려 볼 수 있다
# 늦추는 길이(이벤트 시간, 초). 실시간 집계의 워터마크 10분을 사이에 둔다(ADR 0009 결정 3·4).
LATE = (0, 8 * 60)              # 워터마크보다 짧다: 순서는 뒤바뀌어도 집계에 들어가야 한다
VERY_LATE = (15 * 60, 20 * 60)  # 워터마크보다 길다: 버려져야 한다
COLUMNS = ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id",
           "user_session"]


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")  # 원천 시각은 초 단위다


def plan(rows, start: datetime, speed: float, late: float, very_late: float, seed: int):
    """rows: 이벤트 시각 순서의 (시각, 레코드). start: 옮긴 뒤 첫 이벤트의 시각.

    보낼 시점(재생 시작부터 초) 순서의 (초, 키, 값) 목록과 숫자 요약을 돌려준다.
    """
    rng = random.Random(seed)
    t0 = rows[0][0]
    delta = start - t0  # 모든 이벤트에 같은 Δ를 더해 간격을 지킨다. 그래서 윈도 집계가 배치와 같아야 한다
    out, n_late, n_very = [], 0, 0
    for t, rec in rows:
        u = rng.random()
        if u < very_late:
            lag, n_very = rng.uniform(*VERY_LATE), n_very + 1
        elif u < very_late + late:
            lag, n_late = rng.uniform(*LATE), n_late + 1
        else:
            lag = 0.0
        # 이벤트 시각은 그대로 두고 보내는 시점만 뒤로 민다. 그리고 보내는 간격만 1/k로 줄인다
        send = ((t - t0).total_seconds() + lag) / speed
        out.append((send, rec["user_id"], {**rec, "event_time": iso(t + delta), "src_event_time": iso(t)}))
    out.sort(key=lambda m: m[0])  # 안정 정렬: 같은 시점이면 원래 순서를 지킨다
    # 개수는 n_으로 시작한다. 인자 이름(late, very_late)과 겹치면 재생 기록에서 비율을 덮어쓴다(2026-10-12 겪음)
    return out, {"rows": len(rows), "n_late": n_late, "n_very_late": n_very, "delta_seconds": delta.total_seconds()}


def with_tick(messages, minutes: int):
    """맨 끝에 워터마크를 밀어 주는 틱 하나를 붙인다(이벤트 시각 = 마지막 이벤트 + minutes).

    append 모드는 워터마크가 윈도 끝을 지나야 그 윈도를 내보낸다. 재생이 끝나 입력이 멈추면 워터마크도 멈춰
    마지막 윈도들이 갇힌다. Spark의 워터마크는 모든 파티션을 통틀어 본 가장 늦은 시각이라 틱 하나면 된다.
    """
    last = max(datetime.strptime(v["event_time"], "%Y-%m-%dT%H:%M:%SZ") for _at, _k, v in messages)
    tick = {"event_type": "tick", "event_time": iso(last + timedelta(minutes=minutes)), "user_id": 0}
    return messages + [(messages[-1][0], "tick", tick)]


def read(warehouse_root: str, d: date, start: str, minutes: int):
    import pyarrow.dataset as ds
    t0 = datetime.combine(d, datetime.strptime(start, "%H:%M").time())  # Parquet 시각은 시간대 없는 UTC 값이다
    t1 = t0 + timedelta(minutes=minutes)
    f = ds.field("event_time")
    tbl = ds.dataset(f"{warehouse_root}/dt={d}", format="parquet").to_table(columns=COLUMNS, filter=(f >= t0) & (f < t1))
    # 1단계가 중복을 지우며 섞어 썼다. 시각 순서와 같은 시각 안의 순서를 정해야 같은 시드가 같은 결과를 낸다
    tbl = tbl.sort_by([(c, "ascending") for c in ["event_time", "user_id", "user_session", "event_type", "product_id"]])
    rows = []
    for r in tbl.to_pylist():
        t = r["event_time"]  # pandas가 있으면 pandas.Timestamp, 없으면 datetime으로 나온다
        t = (t.to_pydatetime() if hasattr(t, "to_pydatetime") else t).replace(tzinfo=timezone.utc)
        rows.append((t, {**r, "price": str(r["price"])}))  # 돈은 문자열로 보내 소수 오차를 피한다
    return rows


def send(messages, bootstrap: str, topic_name: str = TOPIC):
    from confluent_kafka import KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    topic = NewTopic(topic_name, PARTITIONS, 1, config={
        # retention.bytes는 토픽 전체가 아니라 파티션마다 적용된다. 상한은 256MB × 6이다.
        # 지우는 단위가 세그먼트라, 세그먼트가 보존 크기보다 작아야 실제로 지워진다
        "retention.bytes": str(256 << 20), "segment.bytes": str(64 << 20)})
    admin = AdminClient({"bootstrap.servers": bootstrap})  # 변수로 붙잡아 둔다. 바로 버리면 결과를 받기 전에 연결이 닫힌다
    for fut in admin.create_topics([topic]).values():
        try:
            fut.result()
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,       # 재시도해도 브로커에 두 번 쓰이지 않는다(보내는 쪽의 정확히 한 번)
        "partitioner": "murmur2_random",  # 자바 클라이언트와 같은 해시. librdkafka 기본(CRC32)이면 같은 키가 다른 파티션에 간다
        "linger.ms": 20,
    })
    failed = []
    t_start = time.monotonic()
    for at, key, value in messages:
        wait = at - (time.monotonic() - t_start)
        if wait > 0.001:
            time.sleep(wait)
        while True:
            try:
                producer.produce(topic_name, key=str(key), value=json.dumps(value),
                                 on_delivery=lambda err, _msg: err and failed.append(str(err)))
                break
            except BufferError:  # 보내기 큐가 차면 비울 때까지 기다린다
                producer.poll(0.1)
        producer.poll(0)
    producer.flush()
    return len(messages) - len(failed), failed


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--warehouse", required=True, help="1단계 events Parquet 루트")
    p.add_argument("--date", type=date.fromisoformat, default=date(2019, 11, 16))
    p.add_argument("--start", default="08:00", help="구간 시작(UTC, HH:MM)")
    p.add_argument("--minutes", type=int, default=120)
    p.add_argument("--speed", type=float, default=60.0)
    p.add_argument("--late", type=float, default=0.05)
    p.add_argument("--very-late", type=float, default=0.005)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--topic", default=TOPIC, help="실험마다 새 토픽을 쓰면 서로 섞이지 않는다")
    p.add_argument("--tick-minutes", type=int, default=30, help="마지막 틱의 여유(분). 워터마크보다 길어야 한다. 0이면 보내지 않는다")
    p.add_argument("--out", default="/opt/airflow/data/stream", help="재생 기록(JSON)을 남길 폴더")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    rows = read(a.warehouse, a.date, a.start, a.minutes)
    if not rows:
        raise SystemExit(f"{a.date} {a.start}부터 {a.minutes}분 구간에 행이 없다")
    started = datetime.now(timezone.utc).replace(microsecond=0)
    # 다음 정각 분으로 옮긴다. 그러면 옮긴 1분 윈도가 원래 1분과 그대로 대응한다(08:05 ↔ 옮긴 시각의 hh:05+n)
    shift_to = (started + timedelta(minutes=1)).replace(second=0)
    messages, summary = plan(rows, shift_to, a.speed, a.late, a.very_late, a.seed)
    if a.tick_minutes:
        messages = with_tick(messages, a.tick_minutes)
    print(f"구간 {a.date} {a.start}부터 {a.minutes}분: {summary['rows']:,}행, 늦춤 {summary['n_late']:,}, "
          f"워터마크 넘게 늦춤 {summary['n_very_late']:,}, 마지막 전송 {messages[-1][0]:.0f}초 뒤")
    if a.dry_run:
        raise SystemExit
    sent, failed = send(messages, a.bootstrap, a.topic)
    record = {**vars(a), "date": str(a.date), **summary, "sent": sent, "failed": len(failed), "shift_to": iso(shift_to),
              "started_at": iso(started), "finished_at": iso(datetime.now(timezone.utc))}
    os.makedirs(a.out, exist_ok=True)
    path = f"{a.out}/replay-{started:%Y%m%dT%H%M%S}.json"
    with open(path, "w") as fh:
        json.dump(record, fh, ensure_ascii=False, indent=1)
    print(f"보냄 {sent:,}, 실패 {len(failed)}, 기록 {path}")
    if failed:
        raise SystemExit(f"전송 실패 {len(failed)}건: {failed[:3]}")
