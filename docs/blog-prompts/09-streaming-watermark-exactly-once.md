# 09. 4-2 실시간 집계: 워터마크, append와 하트비트, 정확히 한 번, 대조 기준 설계

- 대상 코드: `pipeline/stream_funnel.py`, `tests/test_stream_funnel.py`, `bench/stream_once.sh`, `pipeline/replay.py`의 `with_tick`
- 설계 기록: [ADR 0009](../decisions/0009-stage4-streaming.md) 결정 4·5, '4-2 결과' 절
- 상태: 미답변

## 다룬 개념
- **이벤트 시간 윈도와 워터마크**: 1분 윈도를 이벤트 시각으로 나눈다. 워터마크(= 지금까지 본 가장 늦은 이벤트 시각 − W)는 "이보다 이른 윈도는 끝났다"는 판단선이다.
- **append 모드와 하트비트**: append는 워터마크가 윈도 끝을 지나야 그 윈도를 한 번 내보낸다. 입력이 끊기면 워터마크도 멈춰 마지막 윈도들이 갇힌다. 틱(하트비트)이 워터마크를 밀어 준다.
- **한 배치 늦은 워터마크**: Spark는 늦은 이벤트를 거를 때 한 배치 전의 워터마크를 쓴다. 그래서 워터마크보다 늦은 이벤트도 일부는 집계에 들어간다.
- **정확히 한 번(end-to-end)**: 체크포인트가 Kafka 오프셋을 기억하고, Iceberg가 이미 커밋한 배치 번호를 건너뛴다. 둘이 맞물려 죽었다 살아나도 결과가 같다.
- **대조 기준 설계**: 실시간 결과를 무엇과, 어떤 허용 범위로 맞출지 정하는 일이다. 엔진이 주는 지표(`numRowsDroppedByWatermark`)가 무엇을 세는지 확인하지 않으면 기준이 틀린다.

## 질문 (답은 블로그에)
1. append 모드에서 재생이 끝나면 마지막 10분어치 윈도가 나오지 않는다.
   - 왜 그런가?
   - 틱을 `filter`로 걸러 내면 왜 틱이 소용없어지나? Spark 최적화기가 무엇을 하는지 적어라.
   - Flink는 파티션마다 워터마크를 따로 두고 그중 가장 작은 값을 쓴다. Flink라면 틱 하나로 충분할까?
2. (면접형) 워터마크 5·10·20분 표에서 손실, 결과가 나오는 지연, 상태 크기가 어떻게 맞바뀌나?
   - 이커머스 실시간 매출 대시보드라면 몇 분으로 하겠나?
   - 결제 이상 탐지처럼 늦게 알면 소용없는 일이라면?
   - 각각 이유를 적어라.
3. (면접형) 워터마크(10분)보다 늦게 보낸 3,744건 가운데 1,295건(35%)만 버려졌다.
   - 왜 일부만 버려졌나?
   - "워터마크가 10분이면 10분 넘게 늦은 이벤트는 반드시 버려진다"는 말은 왜 틀렸나? 공식 문서의 어떤 문장이 이것을 말하나?
4. Spark가 보고한 `numRowsDroppedByWatermark`는 95인데, 실제로 버려진 이벤트는 1,295건이었다.
   - 이 지표는 무엇을 세나? 테스트로 어떻게 확인했나?
   - 처음 대조 기준("차이 = 이 지표의 합")은 무엇이 틀렸나?
   - 고친 기준(윈도마다 0 ≤ 차이 ≤ 워터마크보다 늦게 보낸 수)의 근거를 설명하라. 워터마크보다 덜 늦은 이벤트는 왜 버려질 수 없나?
5. (면접형) 재생 도중 `SIGKILL`로 죽였다 살려도 120개 윈도가 배치와 똑같았다.
   - Kafka 오프셋(체크포인트)과 Iceberg 커밋(배치 번호)은 각각 무엇을 맡나?
   - 싱크가 배치 번호를 보지 않는 Postgres `INSERT`였다면 어떻게 됐을까?
   - 재생기를 실수로 두 번 돌리면(생산자 쪽 중복) 이 보장이 막아 주나?
6. 트리거를 5초로 두었는데, Iceberg 문서는 스트리밍 쓰기의 트리거를 최소 1분으로 권한다.
   - 5초로 두면 무엇이 나빠지나? 스냅숏, 작은 파일, 매니페스트를 떠올려라.
   - 운영이라면 트리거를 어떻게 정하고, 쌓인 것은 무엇으로 정리하나?

## 직접 돌려 볼 실험
1. **다른 때에 죽여 보기** (`docker compose --profile stream up -d kafka` 뒤)
   ```bash
   bench/stream_once.sh events_k --kill-after 40 -- --very-late 0
   ```
   ```bash
   ls data/stream/ckpt/events_k/offsets data/stream/ckpt/events_k/commits
   ```
   볼 것: 대조는 통과해야 한다. `offsets`에는 있는데 `commits`에는 없었던 번호가 있었는지 진행 기록(`data/stream/ckpt/events_k.progress.jsonl`)의 `runId`와 `batchId`로 찾아본다. 있었다면 그 배치는 재시작 뒤 다시 실행됐고, Iceberg는 그 배치 번호가 이미 커밋됐으면 건너뛴다.
2. **워터마크를 더 줄이기**
   ```bash
   WATERMARK=3 bench/stream_once.sh events_w3
   ```
   볼 것: 버려진 이벤트가 5분일 때(3,543)보다 많은지 본다. 그래도 판정은 통과해야 한다. 버려진 이벤트가 모두 "3분보다 늦게 보낸 이벤트" 안에 있기 때문이다. 상태에 든 윈도 최대값을 진행 기록의 `stateOperators.numRowsTotal`에서 찾아 표에 한 줄을 더한다.

## 학습 자료
링크는 2026-10-12에 열리는 것을 확인했다.
- [Spark 4.1.2 — Structured Streaming(데이터프레임 API)](https://spark.apache.org/docs/4.1.2/streaming/apis-on-dataframes-and-datasets.html): 'Handling Late Data and Watermarking'과 'Semantic Guarantees of Aggregation with Watermarking' 절만 본다.
  > 요약: 워터마크는 "지금까지 본 가장 늦은 이벤트 시각 − 임계값"이고, 그보다 이른 윈도의 상태를 지운다. 임계값 안에 든 늦은 데이터는 반드시 집계된다. 하지만 임계값보다 늦은 데이터는 버려진다고 보장되지 않고, 집계될 수도 있다. append 모드는 워터마크가 지난 윈도만 내보낸다.
- [Spark 4.1.2 — Structured Streaming + Kafka Integration](https://spark.apache.org/docs/4.1.2/streaming/structured-streaming-kafka-integration.html): 'Creating a Kafka Source for Streaming Queries'와 'Kafka Specific Configurations' 절만 본다.
  > 요약: 읽는 오프셋은 Spark가 직접 관리하고 Kafka에 커밋하지 않는다. `startingOffsets`는 새 쿼리를 시작할 때만 쓰이고, 다시 시작하면 멈춘 곳부터 읽는다. 컨슈머 그룹 id는 쿼리마다 자동으로 만든다.
- [Iceberg 1.12.0 — Spark Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/): 'Streaming Writes'와 'Maintenance for streaming tables' 절만 본다(확인 시점의 latest가 1.12.0).
  > 요약: append와 complete 모드를 지원하고, 표는 미리 있어야 한다. 커밋이 잦으면 스냅숏과 작은 파일, 매니페스트가 빠르게 쌓인다. 그래서 트리거는 최소 1분을 권하고, 스냅숏 만료·파일 압축·매니페스트 재작성으로 정리한다.
