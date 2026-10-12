# 03. 성능 측정: 동시 실행 수, 하루씩 vs 한 번에, 병목, 그리고 메모리 부족

- 대상 코드: `bench/backfill.sh`, `bench/spark_once.sh`, `bench/stages.py`, `dags/events_daily.py`(quality 재시도), `pipeline/load_events.py`
- 설계 기록: [ADR 0005](../decisions/0005-benchmark.md), [ADR 0004 결정 4 정정과 실패 훈련 2](../decisions/0004-stage2-orchestration.md)
- 상태: 미답변

## 다룬 개념
- **동시 실행의 수확 체감**: 동시에 돌리는 수(K)를 늘리면 빨라진다. 하지만 빠른 코어·메모리 같은 공유 자원을 나눠 쓰느라 하나하나가 느려져서, 늘릴 때마다 얻는 효과가 줄어든다.
- **메모리 예산과 OOM 킬러**: JVM은 힙(`--driver-memory`) 밖에서도 메모리를 쓴다. 머신 전체 메모리가 바닥나면 커널이 가장 큰 프로세스를 강제 종료하고, 프로그램 안에는 오류 메시지가 남지 않는다.
- **재시도할 실패와 하지 않을 실패**: 일시적 실패(메모리, 네트워크)는 다시 하면 풀린다. 결정적 실패(데이터가 틀림)는 다시 해도 같다. 재시도 설정은 "실패 원인이 무엇일 것"이라는 가정 위에 서 있다.
- **분할할 수 없는 파일과 병렬성**: gzip 파일 하나는 태스크 하나가 처음부터 끝까지 읽어야 한다. 그래서 파일 수가 병렬성의 상한이 된다.
- **고정 비용과 처리 단위**: 작업마다 드는 시작 비용(JVM, 실행 계획, 커밋)은 데이터가 작을수록 비중이 커진다.

## 질문 (답은 블로그에)
1. 동시 실행 수를 1→2→3→4로 늘렸더니 906→528→401→346초가 걸렸다. 왜 2배·3배·4배로 줄지 않았나? ADR 0005 ⑤의 표(실효 동시 실행 0.89→3.44, load 평균 13.9→20.8초)로 설명해 보라. 이 맥의 코어 구성(빠른 코어 4 + 효율 코어 6)은 이 결과와 어떻게 이어지나? 가설과 확인된 사실을 구분해서 써 보라.
2. K=4에서 품질 검사가 실패했다. 로그에는 `OutOfMemoryError`가 없고 `Answer from Java side is empty`만 있었다. 어떤 순서로 원인이 메모리 부족임을 확인했나? 힙은 2g였는데 왜 실제로는 2.2GiB를 썼고, 왜 Java가 아니라 커널이 프로세스를 죽였나?
3. 처음에는 품질 검사를 재시도 없이 두었다가 재시도하도록 바꿨다. 바꾸면서 잃은 것은 무엇인가? 둘 다 얻으려면 어떻게 해야 하나(`retry_policy`와 BashOperator의 한계)? 강제 종료 실험에서 재시도 대기가 2분이 아니라 10분이었던 이유는?
4. 11월 적재를 하루씩 30번 하면 418초, 한 번에 하면 90초였다. 하루치 적재는 왜 코어 10개 중 1개만 쓰나(gzip, `repartition("dt")`)? 그런데도 하루 단위 구조를 유지한 이유는?
5. (면접형) "7개월치를 다시 처리해야 하는데 빨리 끝내려면 어떻게 하시겠어요?" 이번 측정 숫자를 근거로, 지금 설정만 바꿔 할 수 있는 것과 구조를 바꿔야 하는 것을 나눠 답해 보라.
6. (면접형) 측정 스크립트가 앞 백필이 닫히기 전에 새 백필을 만들려다 거절당했고, 풀을 4로 올린 채 죽었다. 그대로 두었다면 무엇이 위험했나? `trap ... EXIT`는 무엇을 막나?

## 직접 돌려 볼 실험
1. **하루치 작업이 코어를 몇 개 쓰는지 보기** (각 10~30초)
   ```bash
   docker compose up -d
   bench/spark_once.sh load_events --start 2019-11-05
   ```
   출력 마지막 줄의 이벤트 로그 경로를 넣어 단계별로 본다.
   ```bash
   python3 bench/stages.py data/bench/eventlogs/<출력된 로그 파일 이름>
   ```
   볼 것: 모든 단계의 태스크 수가 1이고 코어 사용이 10%, CPU 비율이 90% 이상이다. `--start 2019-11-01 --end 2019-11-07`로 바꿔 돌리면 태스크 수와 코어 사용이 어떻게 바뀌는지 비교한다.
2. **동시 실행 수를 바꿔 재기** (일주일치, 각 2~3분)
   ```bash
   bench/backfill.sh 2019-11-01 2019-11-07 1
   ```
   ```bash
   bench/backfill.sh 2019-11-01 2019-11-07 3
   ```
   ```bash
   tail -2 bench/results.csv
   ```
   볼 것: K=3인데 걸린 시간이 3분의 1로 줄지 않는다. 최대 메모리가 K에 따라 얼마나 늘어나는지도 본다.

## 학습 자료
- [Spark 4.1.2 — Configuration](https://spark.apache.org/docs/4.1.2/configuration.html): `spark.driver.memory`, `spark.driver.memoryOverhead` 두 항목만 본다.
  > 요약: 드라이버는 힙 말고도 힙 밖 메모리(VM 오버헤드, 네이티브 메모리, PySpark의 Python 프로세스)를 쓴다. YARN·Kubernetes의 클러스터 모드에서는 이 몫(힙의 10%, 최소 384MiB)을 컨테이너 크기에 더해 준다. 힙 2g면 약 2.4GiB를 잡는데, 우리가 잰 실제 사용량 2.2GiB가 그 안에 들어간다. local 모드에는 그런 장치가 없어서 우리가 직접 계산해야 한다.
- [Linux 커널 문서 — Concepts overview](https://docs.kernel.org/admin-guide/mm/concepts.html): 맨 끝의 'OOM killer' 절만 본다.
  > 요약: 메모리를 다 썼는데 되찾을 수도 없으면, 커널은 프로세스 하나를 골라 죽여 시스템 전체를 살린다.
- [Spark 4.1.2 — Monitoring and Instrumentation](https://spark.apache.org/docs/4.1.2/monitoring.html): 'Viewing After the Fact' 부분만 본다.
  > 요약: `spark.eventLog.enabled`를 켜면 Spark UI에 보이는 정보가 이벤트 로그로 남아서, 작업이 끝난 뒤에도 History Server로 다시 볼 수 있다. `bench/stages.py`는 이 로그를 직접 읽는다. Spark 4.0부터는 이 로그가 기본으로 압축되고 여러 파일로 나뉜다(마이그레이션 가이드).
