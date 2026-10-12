"""Kafka의 이벤트를 사용자별 세션 윈도(30분 비활동)로 묶어 lake의 rt_sessions에 append한다. 설계는 ADR 0009 결정 7(4-3).

    python -m pipeline.stream_sessions run --topic events_s1 [--watermark-minutes 10] [--idle-stop 30]
    python -m pipeline.stream_sessions check --manifest /opt/airflow/data/stream/replay-<시각>.json

규칙은 3단계 배치(model_daily)와 같다. 같은 사용자의 이벤트 간격이 30분을 넘으면 새 세션이고, 정확히 30분은 같은 세션이다.
재생 구간이 하루 안이라 자정 절단은 상관없다.
"""
import argparse
import json

from pyspark.sql import functions as F

from pipeline.lake import NS
from pipeline.spark import get_spark
from pipeline.stream_funnel import parse, run

# Spark 세션 윈도는 다음 이벤트가 '마지막 이벤트 + 간격'과 같은 시각이어도 같은 세션으로 본다(경계 포함).
# 그래서 간격 30분이 배치 규칙(1,800초까지 같은 세션, 1,801초부터 새 세션)과 그대로 맞는다. 테스트로 고정했다.
# 처음에는 경계를 빼는 줄 알고 1,801초를 줬다가, 1,801초 떨어진 이벤트까지 한 세션으로 묶였다(2026-10-12)
GAP = "30 minutes"


def sessions(ev, watermark_minutes: int):
    return (ev.withWatermark("event_time", f"{watermark_minutes} minutes")
            .groupBy("user_id", F.session_window("event_time", GAP))
            # 틱은 사용자 0의 세션으로 따로 묶인다. 셀 때 빼고, 이벤트가 없는 세션은 집계 뒤에 버린다
            .agg(F.count_if(F.col("event_type") != "tick").alias("n_events"), F.max("event_time").alias("session_end"))
            .filter("n_events > 0")
            .select("user_id", F.col("session_window.start").alias("session_start"), "session_end", "n_events"))


def start(raw, topic: str, watermark_minutes: int, checkpoint: str, ns: str = NS, **trigger):
    out = sessions(parse(raw), watermark_minutes).withColumn("topic", F.lit(topic))
    return (out.writeStream.format("iceberg").outputMode("append")
            .option("checkpointLocation", checkpoint).trigger(**trigger).toTable(f"{ns}.rt_sessions"))


def check(spark, manifest: str, warehouse_root: str, ns: str = NS) -> bool:
    """같은 구간을 warehouse에서 배치 규칙으로 세션화해(옮긴 시각 기준) 실시간 세션과 하나씩 맞춘다."""
    m = json.load(open(manifest))
    d, delta, minutes = m["date"], int(m["delta_seconds"]), m["minutes"]
    t0 = f"TIMESTAMP '{d} {m['start']}:00'"
    truth = spark.sql(f"""
        WITH e AS (SELECT user_id, timestampadd(SECOND, {delta}, event_time) AS t FROM parquet.`{warehouse_root}/dt={d}`
                   WHERE event_time >= {t0} AND event_time < {t0} + INTERVAL {minutes} MINUTES),
             g AS (SELECT user_id, t, CASE WHEN lag(t) OVER w IS NULL
                                            OR unix_timestamp(t) - unix_timestamp(lag(t) OVER w) > 1800 THEN 1 END AS new
                   FROM e WINDOW w AS (PARTITION BY user_id ORDER BY t)),
             s AS (SELECT user_id, t, count(new) OVER (PARTITION BY user_id ORDER BY t
                                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sid FROM g)
        SELECT user_id, min(t) AS session_start, max(t) AS session_end, count(*) AS n_events FROM s GROUP BY user_id, sid""")
    stream = spark.table(f"{ns}.rt_sessions").where(f"topic = '{m['topic']}'")
    same = (F.col("b.n_events") == F.col("s.n_events")) & (F.col("b.session_end") == F.col("s.session_end"))
    r = (truth.alias("b").join(stream.alias("s"), ["user_id", "session_start"], "full_outer").agg(
        F.count("b.n_events").alias("truth"), F.count("s.n_events").alias("stream"), F.count_if(same).alias("equal"),
        F.count_if(F.col("s.n_events").isNull()).alias("truth_only"),
        F.count_if(F.col("b.n_events").isNull()).alias("stream_only"),
        F.count_if(F.col("b.n_events").isNotNull() & F.col("s.n_events").isNotNull() & ~same).alias("differ"),
        F.sum("b.n_events").alias("e_truth"), F.sum("s.n_events").alias("e_stream")).first())
    ok = r.truth == r.stream == r.equal
    print(f"세션: 배치 {r.truth:,} / 실시간 {r.stream:,} / 시작·끝·이벤트 수가 모두 같은 세션 {r.equal:,}")
    print(f"이벤트: 배치 {r.e_truth:,} / 실시간 {r.e_stream or 0:,}")
    if not ok:
        # 배치에만 있으면: 아직 닫히지 않았거나(틱 여유 < 워터마크 + 30분) 이벤트가 버려져 세션이 갈라졌다.
        # 실시간에만 있거나 값이 다르면: 워터마크보다 늦은 이벤트가 버려져 세션이 갈라지거나 시작이 바뀌었다
        print(f"배치에만 있는 세션 {r.truth_only:,} / 실시간에만 있는 세션 {r.stream_only:,} / 값이 다른 세션 {r.differ:,}")
    print("대조 통과" if ok else "대조 실패")
    return ok


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["run", "check"])
    p.add_argument("--topic", default="events")
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--watermark-minutes", type=int, default=10)
    p.add_argument("--idle-stop", type=int, default=30)
    p.add_argument("--manifest")
    p.add_argument("--warehouse", default="/opt/airflow/data/warehouse/events")
    a = p.parse_args()
    spark = get_spark(f"stream_sessions-{a.cmd}", lake=True)
    if a.cmd == "run":
        run(spark, a.bootstrap, a.topic, a.watermark_minutes, a.idle_stop, start_fn=start, name=f"{a.topic}-sessions")
    elif not check(spark, a.manifest, a.warehouse):
        raise SystemExit(1)
