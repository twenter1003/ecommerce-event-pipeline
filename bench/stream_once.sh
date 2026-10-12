#!/usr/bin/env bash
# 재생 한 번 + 실시간 집계 한 번을 돌리고 배치와 대조한다. 설계는 ADR 0009 결정 5.
#   bench/stream_once.sh <토픽> [--kill-after 초] [-- 재생기 인자...]
#   예: bench/stream_once.sh events_e1
#       bench/stream_once.sh events_e2 --kill-after 60 -- --very-late 0     도중에 강제 종료(SIGKILL) 후 재시작
#       WATERMARK=20 bench/stream_once.sh events_w20
#       JOB=sessions bench/stream_once.sh events_s1 -- --very-late 0 --tick-minutes 45   실시간 세션(4-3)
# 매번 토픽과 체크포인트를 비우고 시작한다(같은 토픽 이름이면 앞 실험 결과를 지운다).
set -euo pipefail
cd "$(dirname "$0")/.."
TOPIC=$1
shift
KILL=0
if [ "${1:-}" = "--kill-after" ]; then KILL=$2; shift 2; fi
[ "${1:-}" = "--" ] && shift
WM=${WATERMARK:-10}
JOB=${JOB:-funnel}  # funnel → rt_funnel_minute, sessions → rt_sessions
TABLE=$([ "$JOB" = sessions ] && echo rt_sessions || echo rt_funnel_minute)
name=stream-$TOPIC
kt() { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@" 2>/dev/null; }

docker rm -f "$name" >/dev/null 2>&1 || true
kt --delete --if-exists --topic "$TOPIC" >/dev/null
until ! kt --list | grep -qx "$TOPIC"; do sleep 1; done  # 토픽 삭제는 비동기다
rm -rf "data/stream/ckpt/$TOPIC" "data/stream/ckpt/$TOPIC".progress.jsonl "data/stream/ckpt/$TOPIC-sessions"*
# 같은 토픽 이름으로 앞서 쓴 결과 행도 지운다. 재생마다 시각을 '지금'으로 옮겨, 연달아 돌리면 윈도 시간대가 겹친다
docker compose exec -T airflow-scheduler python -m pipeline.lake sql \
  "DELETE FROM lake.dw.$TABLE WHERE topic = '$TOPIC'" > /dev/null 2>&1

# 재생기가 토픽을 만들고 보내기 시작한 뒤에 잡을 띄운다. 잡은 earliest부터 읽어 늦게 떠도 빠뜨리지 않는다
log="data/stream/replay-$TOPIC.log"
docker compose exec -T airflow-scheduler python -m pipeline.replay \
  --warehouse /opt/airflow/data/warehouse/events --topic "$TOPIC" "$@" > "$log" 2>&1 &
replay=$!
sleep 5
docker compose --profile stream run -d --name "$name" --entrypoint python stream -m "pipeline.stream_$JOB" \
  run --topic "$TOPIC" --watermark-minutes "$WM" > /dev/null
echo "실시간 잡($JOB) 시작 $(date +%T) 워터마크 ${WM}분"
if [ "$KILL" -gt 0 ]; then
  sleep "$KILL"
  docker kill "$name" > /dev/null && echo "강제 종료(SIGKILL) $(date +%T)"
  docker start "$name" > /dev/null && echo "같은 체크포인트로 다시 시작 $(date +%T)"
fi
wait $replay
tail -1 "$log"
docker wait "$name" > /dev/null  # 입력이 30초 동안 없으면 잡이 스스로 멈춘다
echo "실시간 잡 끝 $(date +%T)"
manifest=$(grep -o '/opt/airflow/data/stream/replay-[0-9T]*\.json' "$log")
docker compose exec -T airflow-scheduler python -m "pipeline.stream_$JOB" check --manifest "$manifest" \
  --watermark-minutes "$WM" 2>/dev/null
