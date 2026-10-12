#!/usr/bin/env bash
# 스프린트 코드 올리기(설계는 ADR 0008). Composer가 떠 있을 때 실행한다. 여러 번 돌려도 같다.
#   infra/deploy.sh
#   - Spark 모듈: gs://<버킷>/code/ (Dataproc 잡의 main 파일과 import용 zip)
#   - DAG: events_daily와 common/만 Composer DAG 폴더로. lake DAG는 compose 전용 의존성이 있어 올리지 않는다(ADR 0007 결정 5)
#   - spark 풀: 동시에 Dataproc에 낼 잡 수
set -euo pipefail
cd "$(dirname "$0")/.."
B=$(terraform -chdir=infra output -raw bucket)
DAGS=$(terraform -chdir=infra output -raw composer_dag_prefix)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
zip -qr "$tmp/pipeline.zip" pipeline -x '*/__pycache__/*'
gcloud storage cp "$tmp/pipeline.zip" "gs://$B/code/pipeline.zip"
gcloud storage cp pipeline/*.py "gs://$B/code/pipeline/"
gcloud storage cp dags/events_daily.py dags/.airflowignore "$DAGS/"
gcloud storage cp dags/common/airflow_utils.py "$DAGS/common/"
# 노드 하나(8코어)에 앱마다 AM 1코어 + 실행기 1코어. 4개면 코어가 꽉 찬다(Dataproc max-concurrent-jobs와 같다)
gcloud composer environments run study --location us-central1 pools set -- spark 4 "Dataproc jobs"
