#!/usr/bin/env bash
# 버킷의 원본 7개월을 일별 landing(gs://<버킷>/landing/events/dt=…)으로 나눈다. 한 번만 돌린다(ADR 0008 결정 2).
# Dataproc 클러스터가 떠 있고 infra/deploy.sh로 코드를 올린 뒤에 실행한다. 달마다 잡 하나라 서로 다른 날짜만 쓴다.
#   infra/split.sh [달 ...]    달을 주면 그 달만(예: 2019-Dec 2020-Jan)
set -euo pipefail
cd "$(dirname "$0")/.."
B=$(terraform -chdir=infra output -raw bucket)
for m in ${@:-2019-Oct 2019-Nov 2019-Dec 2020-Jan 2020-Feb 2020-Mar 2020-Apr}; do
  # --async: 한꺼번에 내고, 동시 4개를 넘는 잡은 Dataproc이 줄을 세운다(max-concurrent-jobs)
  gcloud dataproc jobs submit pyspark "gs://$B/code/pipeline/split_raw.py" --cluster study --region us-central1 \
    --py-files "gs://$B/code/pipeline.zip" --async \
    -- "gs://$B/raw/data.rees46.com/datasets/marketplace/$m.csv.gz" --landing "gs://$B/landing/events"
done
