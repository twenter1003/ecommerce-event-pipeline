#!/usr/bin/env bash
# 백필 한 번을 재고 결과를 bench/results.csv에 한 줄 남긴다. 설계는 ADR 0005.
#   bench/backfill.sh <시작일> <종료일> <동시 실행 수> [dag_id, 기본 events_daily]
#   예: bench/backfill.sh 2019-11-01 2019-11-30 2 lake_daily
# 같은 날짜를 다시 돌리므로(reprocess completed) 결과 데이터는 바뀌지 않는다(적재·모델이 멱등).
# events_daily와 lake_daily의 백필을 동시에 열지 않는다(ADR 0007 결정 5). 두 DAG가 모두 조용해질 때까지 기다린다.
set -euo pipefail
cd "$(dirname "$0")/.."
FROM=$1 TO=$2 K=$3 DAG=${4:-events_daily}
af() { docker compose exec -T airflow-scheduler airflow "$@" 2>/dev/null; }
runs_busy() { af dags list-runs "$1" -o plain | awk 'NR>1{print $3}' | grep -qE "running|queued"; }
busy() { runs_busy events_daily || runs_busy lake_daily; }
# 마지막 run이 끝나도 백필은 몇십 초 뒤에야 '끝남'으로 정리되고, 그 전에는 새 백필이 거절된다(ADR 0004 알려진 한계).
# CLI로는 백필 목록을 볼 수 없어 DB를 직접 본다.
backfill_open() {
  [ "$(docker compose exec -T postgres psql -U airflow -d airflow -tAc \
    'select count(*) from backfill where completed_at is null')" != 0 ]
}

while busy || backfill_open; do echo "앞 run·백필이 끝나길 기다린다." >&2; sleep 10; done
trap 'af pools set spark 2 "Spark local mode tasks" >/dev/null' EXIT  # 중간에 죽어도 평소 값으로 되돌린다
af pools set spark "$K" "Spark local mode tasks" >/dev/null

start=$(date +%s)
af backfill create --dag-id "$DAG" --from-date "$FROM" --to-date "$TO" \
  --max-active-runs "$K" --reprocess-behavior completed >/dev/null
peak_mem=0 peak_cpu=0 peak_sw=0
sleep 5
while busy; do
  # 스케줄러(Spark JVM이 도는 곳)의 메모리·CPU와, lake가 쓰는 seaweedfs의 메모리
  read -r mem cpu sw < <(docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' |
    awk 'function mib(v) {if (v ~ /GiB/) {sub("GiB","",v); return v*1024} sub("MiB","",v); return v}
         /scheduler/{m=mib($2); c=$NF; sub("%","",c)} /seaweedfs/{w=mib($2)} END{print int(m), int(c), int(w)}')
  (( mem > peak_mem )) && peak_mem=$mem
  (( cpu > peak_cpu )) && peak_cpu=$cpu
  (( sw > peak_sw )) && peak_sw=$sw
  sleep 3
done
wall=$(( $(date +%s) - start ))

# 이번 범위의 run 결과 (logical_date 기준)
read -r ok failed < <(af dags list-runs "$DAG" -o json | python3 -c "
import json, sys
rs = [r for r in json.load(sys.stdin) if '$FROM' <= r['logical_date'][:10] <= '$TO']
print(sum(r['state'] == 'success' for r in rs), sum(r['state'] == 'failed' for r in rs))")

[ -f bench/results.csv ] || echo "측정시각,시작일,종료일,동시실행,걸린초,성공run,실패run,최대메모리MiB,최대CPU%,DAG,seaweedfs최대MiB" > bench/results.csv
echo "$(date '+%F %T'),$FROM,$TO,$K,$wall,$ok,$failed,$peak_mem,$peak_cpu,$DAG,$peak_sw" | tee -a bench/results.csv
