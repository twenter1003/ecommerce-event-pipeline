"""3단계 lake DAG. 설계는 ADR 0007 결정 5.

lake_daily: events_daily.quality 통과를 기다린 뒤 그날 세션과 상품 스냅숏을 lake(Iceberg)에 쓴다.
lake만 다시 만들 때는 이 DAG만 백필한다(events는 건드리지 않는다):
    airflow backfill create --dag-id lake_daily --from-date 2019-11-01 --to-date 2019-11-30 --max-active-runs 2

lake_marts: 날짜 테이블 전체에서 상품 이력(SCD2)과 마트 두 개를 다시 계산한다. 시간표 없이 손으로 돌린다:
    airflow dags trigger lake_marts

최상위에서 pipeline이나 Spark를 import하지 않는다. lake 코드의 문제가 DAG 파싱을 깨지 않게, BashOperator로 `python -m`만 부른다.
"""
import pendulum
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, CronDataIntervalTimetable
from common.airflow_utils import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="lake_daily",
    # events_daily와 시간표가 같아야 한다. 센서는 run_id가 아니라 logical_date로 짝을 찾는다.
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=pendulum.datetime(2019, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2020, 4, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["stage3"],
):
    wait_events = ExternalTaskSensor(
        task_id="wait_events",
        external_dag_id="events_daily",
        external_task_id="quality",
        allowed_states=["success"],
        # 없으면 events가 실패해도 timeout까지 기다리며 백필 자리를 차지한다
        failed_states=["failed", "upstream_failed"],
        mode="reschedule",  # 기다리는 동안 풀 칸과 실행 슬롯을 잡지 않는다
        poke_interval=60,
        timeout=6 * 60 * 60,
        # events의 실패는 events_daily의 quality 알림이 이미 알린다. 센서는 재시도도 알림도 하지 않는다.
        # events를 고쳐 다시 처리한 뒤 lake_daily를 --reprocess-behavior failed로 다시 돌린다.
        retries=0,
        on_failure_callback=None,
    )
    model = spark_job("model", "model_daily", "--start {{ ds }}")

    wait_events >> model

with DAG(
    dag_id="lake_marts",
    schedule=None,  # 처리일이 없다. lake 백필이 끝난 뒤나 lake_daily를 다시 돌린 뒤에 trigger한다
    max_active_runs=1,  # trigger를 두 번 해도 차례로 돈다. 두 run이 같은 dim에 차이를 MERGE하지 않는다
    default_args=DEFAULT_ARGS,
    tags=["stage3"],
):
    # 태스크 하나. 관문·검사가 실패해도 만료는 같은 프로세스의 finally에서 돈다(ADR 0007 결정 5).
    # JVM이 약 2.4GiB라 lake_daily 한 칸(약 2.3GiB)보다 크다. 2칸을 잡아 풀 한 칸이 JVM 하나 분량을 넘지 않게 한다.
    spark_job("marts", "marts", "", pool_slots=2)
