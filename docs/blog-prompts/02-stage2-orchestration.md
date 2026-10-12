# 02. 2단계 자동화: 매일 돌리기·지난 날짜 몰아서 돌리기(백필)·재시도·품질 검사·알림

- 대상 코드: `dags/events_daily.py`, `pipeline/quality.py`, `docker-compose.yml`, `docker/airflow.Dockerfile`
- 설계 기록: [ADR 0004](../decisions/0004-stage2-orchestration.md)
- 상태: 미답변

## 다룬 개념
- **DAG(작업 순서도)**: "도착 확인 → 적재 → 검사"처럼 순서가 있는 작업 묶음이다. Airflow는 이것을 정해진 시간에 실행한다.
- **처리 구간(data interval)**: 오늘 새벽에 도는 작업은 "어제 하루"를 처리한다. run이 실행되는 시각과 처리하는 날짜는 다르다.
- **백필(backfill)**: 지난 날짜들을 한꺼번에 몰아서 처리한다. 처음 시스템을 켰을 때, 또는 로직을 고친 뒤 과거를 다시 만들 때 쓴다.
- **재시도와 멱등성**: 실패하면 같은 날짜로 다시 돌린다. 두 번 돌려도 결과가 같아야 재시도가 안전하다.
- **데이터 품질 검사와 정합성 대조**: 처리 결과가 원천과 맞는지 숫자로 확인한다. 핵심은 "입고 칸 고유 행 수 = 창고 행 수"다.

## 질문 (답은 블로그에)
1. Airflow 3.3에서 `schedule="@daily"`라고만 쓰면 10/1 run이 언제 돌고 무엇을 처리하게 되나? 우리는 왜 `CronDataIntervalTimetable`을 일부러 적었나?
2. 10/1을 처리한 run의 이름은 `backfill__2019-10-02T00:00:00`이었다. 왜 이름은 10/2인데 처리한 날짜는 10/1인가?
3. 처음에는 적재만 2번 다시 시도하고, 품질 검사는 다시 시도하지 않게 했다. 그런데 성능 측정 중에 품질 검사가 메모리 부족으로 죽자 둘 다 재시도하도록 바꿨다(ADR 0004 결정 4 정정). 처음 설계는 실패 원인을 어떻게 가정했고, 무엇을 놓쳤나? "재시도해도 안전하다"는 무엇 덕분인가?
4. 실패 훈련에서 빈 시각 행 1개를 넣었더니 적재는 "성공", 품질 검사는 "실패"했다. 왜 적재 단계에서는 못 잡았고, 어떤 규칙이 잡았나? 그 규칙이 없었다면 무슨 일이 생겼을까?
5. 파일 도착을 기다리는 센서를 따로 둔 이유는 무엇인가? 센서 없이 적재가 "파일 없음" 오류로 실패하게 두면 무엇이 불편한가?
6. (면접형) "운영에서도 Airflow 워커 안에서 Spark를 돌리나요?"라는 질문에 어떻게 답할까? 로컬과 클라우드(Dataproc)에서 무엇을 다르게 했나?
7. (면접형) 같은 날짜를 두 작업이 동시에 쓰면 무슨 일이 생기나(ADR 0003 한계)? Airflow 안에서는 그게 왜 일어나지 않나?

## 직접 돌려 볼 실험
1. **Airflow 화면에서 백필 보기**: `docker compose up -d`를 실행하고 http://localhost:8080 에 들어간다. `events_daily` DAG에서 31개 run과 각 태스크의 로그를 연다.
   볼 것: 품질 검사 로그의 "통과/실패" 여덟 줄, 그리고 run 이름과 처리일이 하루 어긋나는 것.
2. **하루만 다시 처리하기**:
   ```bash
   docker compose exec airflow-scheduler airflow backfill create --dag-id events_daily --from-date 2019-10-15 --to-date 2019-10-15 --reprocess-behavior completed
   ```
   볼 것: `data/warehouse/events/dt=2019-10-15/`의 파일 이름은 바뀌지만, 파일 수와 행 수는 그대로다.

## 학습 자료
- [Airflow 3.3.1 — DAG Runs](https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/dag-run.html): 'Data Interval' 부분만 본다.
  > 요약: 각 run은 "처리할 시간 구간"을 갖는다. 하루 단위 작업은 구간이 끝난 뒤(다음 날 0시)에 실행된다. 그래서 run이 실행되는 시각과 처리하는 날짜가 다르다.
- [Airflow 3.3.1 — Timetables](https://airflow.apache.org/docs/apache-airflow/3.3.1/authoring-and-scheduling/timetable.html): `CronTriggerTimetable`과 `CronDataIntervalTimetable`을 비교한 부분만 본다.
  > 요약: 앞의 것은 "그 시각에 실행"만 하고 처리 구간이 없다. 뒤의 것은 "구간이 끝나면 그 구간을 처리"한다. Airflow 3부터 cron 문자열의 기본값이 앞의 것으로 바뀌었다.
- [Airflow 3.3.1 — Backfill](https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/backfill.html): 처음부터 'reprocess behavior'까지 본다.
  > 요약: 백필은 스케줄러가 실행하고, 지정한 날짜 범위의 run을 만든다. 이미 있는 run을 건너뛸지(none), 실패한 것만 다시 할지(failed), 전부 다시 할지(completed) 고를 수 있다.
