"""DAG 파일들이 같이 쓰는 헬퍼. DAG를 정의하지 않는 모듈이라 import해도 DAG가 두 번 등록되지 않는다(ADR 0007 결정 5).

.airflowignore로 DAG 파싱 대상에서 뺀다. 스프린트(Composer)에는 events_daily.py와 이 폴더를 함께 올린다.
같은 DAG 코드가 로컬과 Composer에서 돈다. 갈리는 곳은 환경 변수 둘이다(ADR 0008).
  - EVENTS_LANDING이 gs://로 시작하면 landing 도착을 GCS 목록으로 확인한다
  - DATAPROC_CLUSTER가 있으면 Spark 작업을 Dataproc에 낸다. 없으면 이 컨테이너 안에서 local 모드로 돈다
"""
import glob
import logging
import os
import re
from datetime import timedelta

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.standard.operators.bash import BashOperator

LANDING = os.environ.get("EVENTS_LANDING", "/opt/airflow/data/landing/events")
WAREHOUSE = os.environ.get("EVENTS_WAREHOUSE", "/opt/airflow/data/warehouse/events")
DATAPROC_CLUSTER = os.environ.get("DATAPROC_CLUSTER")
log = logging.getLogger(__name__)


def alert_on_failure(context):
    # 재시도를 다 쓴 뒤 한 번만 불린다. 품질 검사 실패도 태스크 실패라 같은 길로 온다.
    ti = context["ti"]
    # 날짜 없이 수동 실행한 run에는 ds가 없다. 그때도 알림은 가야 한다.
    text = f"[실패] {ti.dag_id}.{ti.task_id} 처리일={context.get('ds')} 시도={ti.try_number}\n{ti.log_url}"
    try:
        # 여기서 import한다. Composer 3.3.1 이미지에는 Slack provider가 없어, 맨 위에 두면 DAG 파싱이 깨진다.
        from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
        SlackWebhookNotifier(slack_webhook_conn_id="slack_alert", text=text).notify(context)
    except ImportError:
        log.warning("Slack provider가 없어 알림을 로그로 남김: %s", text)
    except AirflowNotFoundException as e:
        # 형식이 틀린 값(JSON 오타, URL만 넣음)도 "없음"과 같은 예외가 난다. 값이 있는지로 둘을 가른다.
        if os.environ.get("AIRFLOW_CONN_SLACK_ALERT"):
            log.error("slack_alert가 설정돼 있지만 읽지 못해 알림을 못 보냄(형식 확인): %s | %s", e, text)
        else:  # 웹훅을 아직 넣지 않았으면 로그 한 줄로 대신한다
            log.warning("slack_alert 연결이 없어 알림을 로그로 남김: %s", text)


DEFAULT_ARGS = {
    "retries": 2,  # 일시적인 실패(메모리, 디스크)용. 적재가 멱등이라 다시 돌려도 안전하다
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,
}


def landed(ds):
    if LANDING.startswith("gs://"):
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        bucket, _, prefix = LANDING.removeprefix("gs://").partition("/")
        return any(n.endswith(".csv.gz") for n in GCSHook().list(bucket, prefix=f"{prefix}/dt={ds}/"))
    return bool(glob.glob(f"{LANDING}/dt={ds}/*.csv.gz"))


def spark_job(task_id, module, args, **kwargs):
    common = dict(
        task_id=task_id,
        pool="spark",
        # 메모리가 바닥나면 JVM이 죽어 태스크가 실패한다. 더 나쁜 건 죽지 않고 멈추는 경우다. 하루치는 10~20초라 30분이면
        # 멈춘 것으로 보고 끝낸다. 그러면 재시도와 알림 경로로 간다(Dataproc 잡은 연산자가 취소한다).
        execution_timeout=timedelta(minutes=30),
        **kwargs,
    )
    if not DATAPROC_CLUSTER:  # 로컬: 이 컨테이너 안에서 local 모드
        return BashOperator(
            bash_command=f"python -m pipeline.{module} {args} --landing {LANDING} --warehouse {WAREHOUSE}", **common)
    # 스프린트: 같은 모듈을 Dataproc에 PySpark 잡으로 낸다. 코드는 infra/deploy.sh가 PIPELINE_CODE에 올린다.
    from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
    code = os.environ["PIPELINE_CODE"]
    return DataprocSubmitJobOperator(
        region=os.environ["DATAPROC_REGION"],
        job={"placement": {"cluster_name": DATAPROC_CLUSTER},
             "pyspark_job": {
                 "main_python_file_uri": f"{code}/pipeline/{module}.py",
                 "python_file_uris": [f"{code}/pipeline.zip"],  # main이 `from pipeline...`을 찾는 곳
                 # "{{ ds }}"처럼 공백이 든 템플릿을 인자 하나로 남긴다. job은 템플릿 필드라 Airflow가 렌더링한다.
                 "args": [*re.findall(r"\{\{.*?\}\}|\S+", args), "--landing", LANDING, "--warehouse", WAREHOUSE]}},
        **common,
    )
