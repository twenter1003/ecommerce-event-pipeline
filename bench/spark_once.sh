#!/usr/bin/env bash
# Spark 작업 하나를 Airflow 없이 스케줄러 컨테이너 안에서 돌려, 걸린 시간과 컨테이너 최대 메모리를 잰다. 설계는 ADR 0005 ③⑤.
#   bench/spark_once.sh load_events --start 2019-11-01 --end 2019-11-30
#   bench/spark_once.sh quality --date 2019-11-05
#   bench/spark_once.sh model_daily --start 2019-11-16
# Spark 이벤트 로그를 data/bench/eventlogs/에 남긴다. 단계별로 보려면: python3 bench/stages.py <로그 파일>
set -euo pipefail
cd "$(dirname "$0")/.."
# Airflow run과 같은 날짜를 동시에 쓰면 결과가 깨질 수 있다(ADR 0003 한계 2). 돌고 있는 run이 없을 때만 실행한다.
if for d in events_daily lake_daily; do docker compose exec -T airflow-scheduler airflow dags list-runs $d -o plain 2>/dev/null; done |
  awk '{print $3}' | grep -qE "running|queued"; then
  echo "Airflow run이 돌고 있다. 끝난 뒤 다시 실행한다." >&2; exit 1
fi
module=$1; shift
# 드라이버 메모리는 DAG 태스크와 같게 2g. 시간은 컨테이너 안에서 잰다(docker exec 비용을 빼려고).
docker compose exec -T -e MODULE="$module" -e ARGS="$*" airflow-scheduler bash -c '
  dir=/opt/airflow/data/bench/eventlogs; mkdir -p $dir
  # Spark 4.0부터 이벤트 로그가 기본으로 압축되고 여러 파일로 나뉜다(rolling). 둘 다 꺼야 bench/stages.py가 파일 하나로 읽는다.
  # 진행 막대는 줄바꿈 없이 덮어써서 뒤따르는 출력 줄과 섞인다. 끈다.
  export PYSPARK_SUBMIT_ARGS="--driver-memory 2g --conf spark.ui.showConsoleProgress=false --conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false --conf spark.eventLog.rolling.enabled=false --conf spark.eventLog.dir=file:$dir pyspark-shell"
  s=$(date +%s%3N)
  python -m pipeline.$MODULE $ARGS --landing $EVENTS_LANDING --warehouse $EVENTS_WAREHOUSE
  e=$(date +%s%3N); f=$(ls -t $dir | head -1)
  # Python이 끝난 직후에는 JVM이 아직 로그를 닫는 중이라 .inprogress가 붙어 있다. 닫히면 꼬리를 뗀 이름이 된다.
  echo "걸린 시간 $(( e - s ))ms, 이벤트 로그 data/bench/eventlogs/${f%.inprogress}"' &
job=$!
peak=0
while kill -0 $job 2>/dev/null; do
  mem=$(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' |
    awk '/scheduler/{v=$2; if (v ~ /GiB/) {sub("GiB","",v); v*=1024} else sub("MiB","",v); print int(v)}')
  (( mem > peak )) && peak=$mem
  sleep 1
done
rc=0; wait $job || rc=$?
echo "스케줄러 컨테이너 최대 메모리 ${peak}MiB"
exit $rc
