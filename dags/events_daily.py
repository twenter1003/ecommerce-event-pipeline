"""REES46 이벤트 일 단위 파이프라인: landing 도착 대기 → 적재 → 품질 검사. 설계는 ADR 0004.

과거 날짜는 백필로 돌린다:
    airflow backfill create --dag-id events_daily --from-date 2019-10-01 --to-date 2019-10-31 --max-active-runs 2
"""
import pendulum
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, CronDataIntervalTimetable
from common.airflow_utils import DEFAULT_ARGS, landed, spark_job


with DAG(
    dag_id="events_daily",
    # "@daily"만 쓰면 3.x 기본값(CronTriggerTimetable)이 되어 10/1 run이 10/1 0시, 하루가 끝나기 전에 돈다.
    # 하루가 끝난 뒤 그 하루를 처리하도록 구간 방식을 명시한다. 그러면 처리일 = {{ ds }}.
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=pendulum.datetime(2019, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2020, 4, 30, tz="UTC"),  # 원본 7개월의 마지막 날
    catchup=False,  # 과거 날짜는 스케줄러가 몰아서 만들지 않고, 백필로 돌린다
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["stage2"],
):
    wait = PythonSensor(
        task_id="wait_for_landing",
        python_callable=landed,
        poke_interval=60,
        timeout=6 * 60 * 60,  # 6시간 안에 안 오면 실패 → 알림. 늦게 오는 것과 고장을 구분한다
        mode="reschedule",  # 기다리는 동안 실행 슬롯을 비워 둔다
    )
    load = spark_job("load", "load_events", "--start {{ ds }}")
    # 검사도 load처럼 재시도한다. 데이터가 틀려 실패하면 재시도해도 또 실패해 알림이 약 6분 늦을 뿐이지만,
    # 메모리 부족으로 JVM이 죽은 실패는 재시도로 풀린다(ADR 0005 ②에서 실제로 겪음).
    # ponytail: 데이터 실패만 바로 알리려면 검사를 @task로 옮겨 실패 종류를 예외 클래스로 드러내고 retry_policy로 나눈다.
    # BashOperator는 종료 코드와 상관없이 같은 예외를 던져서, 지금 구조로는 retry_policy가 둘을 구분하지 못한다.
    check = spark_job("quality", "quality", "--date {{ ds }}")

    wait >> load >> check
