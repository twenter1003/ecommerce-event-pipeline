"""Kafka의 이벤트를 1분 이벤트 시간 윈도로 집계해 lake의 rt_funnel_minute에 append한다. 설계는 ADR 0009 결정 4·5.

    python -m pipeline.stream_funnel run --topic events_e1 [--watermark-minutes 10] [--idle-stop 30]
    python -m pipeline.stream_funnel check --manifest /opt/airflow/data/stream/replay-<시각>.json

run은 입력이 idle-stop초 동안 없으면 멈춘다(0이면 계속 돈다). 체크포인트와 진행 기록은 data/stream/ckpt/<토픽>에 둔다.
check는 같은 구간을 warehouse에서 배치로 다시 세어 실시간 결과와 대조한다.
"""
import argparse
import json
import os
import time
from collections import Counter
from datetime import date, datetime, timezone

from pyspark.sql import functions as F

from pipeline.lake import NS
from pipeline.spark import get_spark

STREAM_ROOT = "/opt/airflow/data/stream"
SCHEMA = "event_time TIMESTAMP, event_type STRING, price DECIMAL(10,2), user_id BIGINT"


def parse(raw):
    """Kafka 메시지(테스트에서는 텍스트 파일의 줄)의 value JSON을 컬럼으로 편다."""
    return raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")


def funnel_minute(ev, watermark_minutes: int):
    """1분 텀블링 윈도의 퍼널. append 모드라 워터마크가 윈도 끝을 지나야 그 윈도를 한 번 내보낸다."""
    def n(t):
        return F.count_if(F.col("event_type") == t)

    return (ev.withWatermark("event_time", f"{watermark_minutes} minutes")
            # 틱은 거르지 않고 셀 때만 뺀다. filter로 거르면 Spark가 그 필터를 워터마크 아래로 내린다
            # (이벤트 시각을 쓰지 않는 조건이라서). 그러면 틱이 워터마크에 닿지 못해 마지막 윈도들이 갇힌다
            .groupBy(F.window("event_time", "1 minute"))
            .agg(F.count_if(F.col("event_type") != "tick").alias("events"),
                 n("view").alias("views"), n("cart").alias("carts"), n("purchase").alias("purchases"),
                 # 합계는 DECIMAL(20,2)로 넓어지는데 Iceberg 스트리밍 쓰기는 표의 (18,2)로 좁혀 주지 않는다. 직접 맞춘다
                 F.coalesce(F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))), F.lit(0))
                 .cast("decimal(18,2)").alias("revenue"))
            .filter("events > 0")  # 틱만 든 윈도는 버린다. 집계 뒤의 필터라 워터마크와 상관없다
            .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                    "events", "views", "carts", "purchases", "revenue"))


def start(raw, topic: str, watermark_minutes: int, checkpoint: str, ns: str = NS, **trigger):
    # Iceberg 스트리밍 쓰기는 커밋마다 쿼리 id와 배치 번호를 스냅숏에 남기고, 이미 커밋한 배치는 다시 쓰지 않는다.
    # 그래서 체크포인트(Kafka 오프셋)와 합쳐 이 테이블에는 정확히 한 번 들어간다(ADR 0009 결정 5)
    out = funnel_minute(parse(raw), watermark_minutes).withColumn("topic", F.lit(topic))
    return (out.writeStream.format("iceberg").outputMode("append")
            .option("checkpointLocation", checkpoint).trigger(**trigger).toTable(f"{ns}.rt_funnel_minute"))


def progress(q) -> list[dict]:
    # Spark 4는 진행 기록을 객체로 준다(.json에 원문). 3.x는 dict였다
    return [json.loads(p.json) if hasattr(p, "json") else p for p in q.recentProgress]


def dropped(progress_lines) -> int:
    """진행 기록에 남은 '워터마크보다 늦어 버린 행'의 합."""
    return sum(op.get("numRowsDroppedByWatermark", 0) for p in progress_lines for op in p.get("stateOperators", []))


def run(spark, bootstrap: str, topic: str, watermark_minutes: int, idle_stop: int, ns: str = NS,
        start_fn=start, name: str | None = None):
    """Kafka 토픽을 읽어 start_fn의 쿼리를 돌린다. 체크포인트와 진행 기록은 data/stream/ckpt/<name, 기본은 토픽>."""
    # 처음 실행할 때 체크포인트에 고정된다. 기본 200이면 배치마다 상태 저장소 200개를 연다
    spark.conf.set("spark.sql.shuffle.partitions", "6")
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap).option("subscribe", topic)
           # 체크포인트가 없을 때만 쓰인다. 다시 뜨면 체크포인트에 적힌 오프셋부터 읽는다
           .option("startingOffsets", "earliest").load())
    checkpoint = f"{STREAM_ROOT}/ckpt/{name or topic}"
    os.makedirs(f"{STREAM_ROOT}/ckpt", exist_ok=True)
    q = start_fn(raw, topic, watermark_minutes, checkpoint, ns, processingTime="5 seconds")
    seen, last_input = -1, None
    with open(f"{checkpoint}.progress.jsonl", "a") as log:
        while q.isActive:
            time.sleep(2)
            for p in progress(q):
                if p["batchId"] > seen:
                    log.write(json.dumps(p) + "\n")
                    log.flush()
                    seen = p["batchId"]
                    if p["numInputRows"]:
                        last_input = time.monotonic()
            if idle_stop and last_input and time.monotonic() - last_input > idle_stop:
                q.stop()
    print(f"멈춤: 마지막 배치 {seen}, 진행 기록 {checkpoint}.progress.jsonl")


def droppable_by_window(m: dict, warehouse_root: str, watermark_minutes: int) -> Counter:
    """재생 계획을 같은 시드로 다시 세워, 워터마크보다 오래 늦춰 보낸 이벤트를 옮긴 시각의 분마다 센다.

    워터마크보다 덜 늦은 이벤트는 버려질 수 없다. 그 이벤트보다 먼저 보낸 메시지의 이벤트 시각은
    '그 이벤트 시각 + 늦춘 길이'를 넘지 못하므로, 워터마크도 '그 이벤트 시각 + 늦춘 길이 − W'를 넘지 못한다.
    """
    from pipeline.replay import plan, read
    rows = read(warehouse_root, date.fromisoformat(m["date"]), m["start"], m["minutes"])
    shift_to = datetime.strptime(m["shift_to"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    messages, _ = plan(rows, shift_to, m["speed"], m["late"], m["very_late"], m["seed"])
    t0, out = rows[0][0], Counter()
    for at, _key, v in messages:
        src = datetime.strptime(v["src_event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if at * m["speed"] - (src - t0).total_seconds() > watermark_minutes * 60:  # 늦춘 길이(이벤트 시간, 초)
            out[v["event_time"][:16].replace("T", " ")] += 1
    return out


def judge(windows, very: Counter):
    """windows: [(분, 배치 값 5개 또는 None, 실시간 값 5개 또는 None)].

    윈도마다 0 ≤ 배치 − 실시간 ≤ 그 분에 워터마크보다 늦게 보낸 이벤트 수여야 한다. 실시간이 더 크면 중복이다.
    그런 이벤트가 없는 윈도는 다섯 값이 모두 같아야 한다. 워터마크보다 늦은 이벤트는 버려질 수도 있고 아닐 수도 있어
    (늦은 이벤트를 거르는 워터마크는 한 배치 전의 값이다) 윈도마다 정확한 값이 아니라 범위로 본다.
    """
    bad = []
    for w, b, s in windows:
        diff, v = (b or (0,))[0] - (s or (0,))[0], very.get(w, 0)
        if not 0 <= diff <= v or (v == 0 and b != s):
            bad.append((w, b, s, v))
    return not bad, bad


def check(spark, manifest: str, warehouse_root: str, watermark_minutes: int, ns: str = NS) -> bool:
    """같은 구간을 warehouse에서 배치로 다시 세어(옮긴 시각 기준 1분) 실시간 결과와 윈도마다 맞춘다."""
    m = json.load(open(manifest))
    d, delta, minutes = m["date"], int(m["delta_seconds"]), m["minutes"]
    t0 = f"TIMESTAMP '{d} {m['start']}:00'"
    lo = f"timestampadd(SECOND, {delta}, {t0})"
    truth = spark.sql(f"""
        SELECT date_trunc('minute', timestampadd(SECOND, {delta}, event_time)) AS window_start,
               count(*) AS events, count_if(event_type = 'view') AS views, count_if(event_type = 'cart') AS carts,
               count_if(event_type = 'purchase') AS purchases,
               coalesce(sum(CASE WHEN event_type = 'purchase' THEN price END), 0) AS revenue
        FROM parquet.`{warehouse_root}/dt={d}`
        WHERE event_time >= {t0} AND event_time < {t0} + INTERVAL {minutes} MINUTES
        GROUP BY 1""")
    stream = spark.table(f"{ns}.rt_funnel_minute").where(
        f"topic = '{m['topic']}' AND window_start >= {lo} AND window_start < {lo} + INTERVAL {minutes} MINUTES")
    cols = ["events", "views", "carts", "purchases", "revenue"]
    rows = (truth.alias("b").join(stream.alias("s"), "window_start", "full_outer")
            .select(F.date_format("window_start", "yyyy-MM-dd HH:mm").alias("w"),
                    *[F.col(f"b.{c}").alias(f"b_{c}") for c in cols], *[F.col(f"s.{c}").alias(f"s_{c}") for c in cols])
            .collect())
    windows = [(r.w, None if r.b_events is None else tuple(r[f"b_{c}"] for c in cols),
                None if r.s_events is None else tuple(r[f"s_{c}"] for c in cols)) for r in rows]
    very = droppable_by_window(m, warehouse_root, watermark_minutes)
    ok, bad = judge(windows, very)
    e_b = sum(b[0] for _w, b, _s in windows if b)
    e_s = sum(s[0] for _w, _b, s in windows if s)
    clean = [(b, s) for w, b, s in windows if very.get(w, 0) == 0]
    with open(f"{STREAM_ROOT}/ckpt/{m['topic']}.progress.jsonl") as fh:
        n_dropped = dropped(json.loads(line) for line in fh)
    print(f"윈도: 배치 {sum(1 for _w, b, _s in windows if b)} / 실시간 {sum(1 for _w, _b, s in windows if s)} / "
          f"다섯 값이 모두 같은 윈도 {sum(1 for _w, b, s in windows if b == s)}")
    print(f"워터마크({watermark_minutes}분)보다 늦게 보낸 이벤트가 없던 윈도 {len(clean)}개 중 다섯 값이 같은 윈도 "
          f"{sum(1 for b, s in clean if b == s)}개")
    print(f"이벤트: 배치 {e_b:,} / 실시간 {e_s:,} / 차이 {e_b - e_s:,} "
          f"(워터마크보다 늦게 보낸 이벤트 {sum(very.values()):,}개 안에서)")
    print(f"참고: Spark가 보고한 '워터마크로 버린 행' {n_dropped:,}. 부분 집계된 행 수라 이벤트 수가 아니다")
    for w, b, s, v in bad[:5]:
        print(f"위반 {w}: 배치 {b} / 실시간 {s} / 늦춘 수 {v}")
    print("대조 통과" if ok else f"대조 실패: 위반 윈도 {len(bad)}개")
    return ok


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["run", "check"])
    p.add_argument("--topic", default="events")
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--watermark-minutes", type=int, default=10)
    p.add_argument("--idle-stop", type=int, default=30, help="입력이 이만큼(초) 없으면 멈춘다. 0이면 계속 돈다")
    p.add_argument("--manifest", help="check: 재생기가 남긴 기록(JSON)")
    p.add_argument("--warehouse", default="/opt/airflow/data/warehouse/events")
    a = p.parse_args()
    spark = get_spark(f"stream_funnel-{a.cmd}", lake=True)
    if a.cmd == "run":
        run(spark, a.bootstrap, a.topic, a.watermark_minutes, a.idle_stop)
    elif not check(spark, a.manifest, a.warehouse, a.watermark_minutes):
        raise SystemExit(1)
