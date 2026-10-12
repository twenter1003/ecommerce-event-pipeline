# 05. 3-2 세션화: 30분 규칙, 쓰기 전 검사, DAG 사이 센서, 셔플

- 대상 코드: `pipeline/model_daily.py`, `tests/test_model_daily.py`, `dags/lake.py`, `dags/common/airflow_utils.py`, `bench/backfill.sh`, `bench/stages.py`
- 설계 기록: [ADR 0007](../decisions/0007-stage3-modeling.md) 결정 2·5·6, '3-2 결과' 절
- 상태: 미답변

## 다룬 개념
- **세션화(sessionization)**: 한 사용자의 이벤트를 시간순으로 세우고, 앞 이벤트와 30분 넘게 떨어지면 새 세션으로 나눈다. Spark에서는 창 함수 `lag`(앞 행 보기)와 누적합(번호표 매기기)으로 한다.
- **창 함수의 파티션 = 자정 절단**: `partitionBy(user_id, dt)`로 줄을 날짜별로 따로 세우면, 세션이 자정을 넘지 못한다. 그래서 하루치를 돌리든 여러 날을 돌리든 결과가 같다.
- **정밀도·재현율과 모집단**: 우리가 나눈 경계 중 원천도 나눈 비율이 정밀도이고, 원천이 나눈 경계 중 우리도 나눈 비율이 재현율이다. 무엇을 "쌍"으로 세느냐에 따라 값이 크게 바뀐다.
- **쓰기 전 검사(write-audit-publish의 축소판)**: 결과를 메모리에 잡아 두고(persist) 검사한다. 통과할 때만 커밋한다. 실패한 날의 데이터가 마트로 흘러가지 않는다.
- **셔플과 row group**: Spark는 같은 키를 한곳에 모으려고 데이터를 섞어 다시 나눈다(셔플). 읽기를 몇 태스크로 나눌 수 있는지는 Parquet 파일 안의 행 묶음(row group) 수가 정한다.

## 질문 (답은 블로그에)
1. 첫 실행에서 검사 2(정밀도 ≥ 0.90)가 실패했다. 코드 버그가 아니라 ADR의 기준값이 틀렸다는 것을 어떤 순서로 확인했나? 정렬 키를 바꿔 본 것, 비교 기준을 바꿔 본 것, 사용자-월과 사용자-일로 나눠 잰 것이 각각 무엇을 배제했나? "자정을 넘는 쌍"이 왜 두 값을 모두 부풀리나?
2. 간격 비교표(10/30/60/120분)에서 정밀도와 재현율이 반대로 움직인다. 이유는 무엇인가? 그런데도 일치율이 가장 높은 간격을 고르지 않고 30분을 유지한 이유를 설명하라. `user_session`을 그대로 세션으로 쓰지 않은 이유도 함께 적어라.
3. `session_id`를 `row_number()`가 아니라 `"{user_id}-{시작 epoch초}"`로 만들었다. 같은 날짜를 두 번 돌리면 각각 무엇이 달라지나? 같은 사용자의 두 세션이 같은 id를 가질 수 없는 이유는?
4. `model_daily`는 두 테이블에 차례로 커밋한다. 첫 커밋 뒤 JVM이 죽으면 무엇이 남나? `src_build`(`<입력 파일 해시>-<실행 id>`)가 이 상태를 어떻게 드러내나? 왜 시각 비교가 아니라 파일 이름 해시인가?
5. (면접형) `lake_daily`의 센서에 `failed_states`, `retries=0`, `on_failure_callback=None`을 준 이유는 각각 무엇인가? 하나라도 빼면 events가 실패한 날에 어떤 일이 생기나?
6. (면접형) 셔플 표에서 11/16의 한 단계가 '쏠림 148배'였다. 원인이 데이터 쏠림(특정 사용자에 몰림)이 아니라 파일 구조라는 것을 어떻게 알았나? 1단계에서 "날짜당 파일 하나"를 고른 것과 어떻게 이어지나? 지금 고치지 않은 이유와, 고친다면 무엇을 바꿀지 설명하라.

## 직접 돌려 볼 실험
1. **간격을 바꾸면 무엇이 변하나** (호스트, 스택 불필요)
   ```bash
   SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON=$PWD/.venv/bin/python LAKE_JARS=$PWD/jars LAKE_WAREHOUSE=/tmp/lw .venv/bin/python -m pipeline.model_daily --start 2019-11-05 --dry-run --gap-minutes 5 --warehouse data/warehouse/events
   ```
   볼 것: 세션 수, 정밀도, 재현율이 30분 값(318,039 / 0.892 / 0.434)에서 어느 방향으로 움직이는지. `--dry-run` 없이 `--gap-minutes 5`를 주면 왜 거절되는지 코드에서 찾아라.
2. **쏠림의 원인 확인** (스택이 떠 있을 때)
   ```bash
   docker compose exec -T airflow-scheduler python -c "import glob, pyarrow.parquet as pq; f=glob.glob('/opt/airflow/data/warehouse/events/dt=2019-11-16/*.parquet')[0]; print(pq.ParquetFile(f).metadata)"
   ```
   볼 것: `num_row_groups`가 1이다. 그다음 `python3 bench/stages.py <11/16 이벤트 로그>`에서 태스크 10개짜리 단계의 '쏠림'이 왜 큰지 이 숫자와 연결해 본다.

## 학습 자료
- [Spark 4.1.2 — Window Functions](https://spark.apache.org/docs/4.1.2/sql-ref-syntax-qry-select-window.html): 문법과 `LAG` 예제만 본다.
  > 요약: 창 함수는 행 묶음(창) 위에서 계산하지만, 행마다 값을 하나씩 돌려준다. `PARTITION BY`로 묶고 `ORDER BY`로 줄 세운다. `LAG`는 줄에서 앞 행의 값을 가져온다. 프레임(`ROWS BETWEEN …`)으로 누적합 범위를 정한다.
- [Spark 4.1.2 — Performance Tuning](https://spark.apache.org/docs/4.1.2/sql-performance-tuning.html): 'Adaptive Query Execution' 절만 본다.
  > 요약: 셔플 파티션 수의 기본값은 200이다. AQE는 실행 중 통계를 보고 작은 셔플 파티션을 합치고, 쏠린 조인 파티션을 쪼개고, 조인 방식을 바꾼다.
  > (덧붙임, 문서 밖 내용) 이번 쏠림은 셔플 뒤가 아니라 파일을 읽는 단계에서 생겼다. 그래서 AQE가 손댈 수 있는 곳이 아니다.
- [Airflow providers-standard — ExternalTaskSensor](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/sensors/external_task_sensor.html): 첫 절과 `ExternalTaskSensor` 절만 본다.
  > 요약: 다른 DAG의 태스크가 같은 논리 날짜(logical date)에 끝났는지 기다린다. 시간표가 다르면 `execution_delta`로 날짜를 맞춘다. `allowed_states`가 통과 조건이고, `failed_states`에 든 상태를 보면 바로 실패한다.
