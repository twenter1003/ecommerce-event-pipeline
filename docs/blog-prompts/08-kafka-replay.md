# 08. 4-1 Kafka와 재생기: 파티션과 키, 오프셋과 컨슈머 그룹, 이벤트 시간과 처리 시간

- 대상 코드: `pipeline/replay.py`, `tests/test_replay.py`, `docker-compose.yml`의 `kafka` 서비스
- 설계 기록: [ADR 0009](../decisions/0009-stage4-streaming.md) 결정 2·3·6, '4-1 결과' 절
- 상태: 미답변

## 다룬 개념
- **토픽·파티션·키**: 토픽은 파티션 여러 개로 나뉜다. 키가 같은 메시지는 같은 파티션에 들어가고, 파티션 안에서만 순서가 지켜진다.
- **오프셋과 컨슈머 그룹**: 오프셋은 파티션 안의 위치 번호다. 그룹은 파티션을 나눠 갖고, 파티션마다 어디까지 읽었는지(커밋한 오프셋)를 Kafka에 남긴다. 끝 오프셋과 커밋한 오프셋의 차이가 lag(남은 양)이다.
- **리밸런싱**: 그룹에 컨슈머가 들어오거나 나가면 파티션을 다시 나눈다.
- **이벤트 시간과 처리 시간**: 메시지 안의 `event_time`은 일이 일어난 시각이다. Kafka 레코드 시각(CreateTime)은 보낸 시각이다. 재생 속도를 바꾸면 둘이 벌어진다.
- **멱등 생산자**: `enable.idempotence`를 켜면 생산자 id와 순번으로, 재시도한 메시지가 로그에 두 번 쓰이지 않는다.

## 질문 (답은 블로그에)
1. 재생기는 모든 이벤트 시각에 같은 Δ를 더하고, 보내는 간격만 1/k로 줄였다.
   - 이벤트 시각까지 1/k로 줄였다면, 실시간 1분 집계는 배치로 센 값과 어떻게 달라지나?
   - 메시지 하나에 `event_time`, `src_event_time`, Kafka 레코드 시각이 함께 있다. 각각 무엇을 뜻하고, 어디에 쓰이나?
2. 키를 `user_id`로 했다.
   - 키 없이 보내면(파티션을 돌아가며 고름) 무엇이 깨지나? 3단계의 세션화를 떠올려라.
   - 파티션을 6개에서 12개로 늘리면, 이미 들어간 사용자 이벤트의 순서에 어떤 일이 생기나?
   - confluent-kafka 기본 해시(CRC32)와 자바 클라이언트 해시(murmur2)가 다르다는 것이 왜 문제가 될 수 있나?
3. (면접형) 컨슈머 그룹 실험 결과가 이렇다. 30만 개만 읽고 멈추자 lag 합계가 451,420이었다. 두 컨슈머는 파티션을 3개씩 나눠 가졌다. 하나를 끄자 남은 하나가 6개를 다 가져갔다.
   - 커밋한 오프셋은 어디에 저장되나?
   - 컨슈머가 처리는 했는데 커밋하기 전에 죽으면 무엇이 다시 읽히나? 이것을 무슨 보장이라고 부르나?
   - 같은 그룹의 컨슈머를 7개로 늘리면 일곱 번째는 무엇을 하나?
4. 재생기는 `enable.idempotence`를 켰다.
   - 이 설정이 막는 중복은 어떤 중복인가?
   - 막지 못하는 중복은 무엇인가? 재생기를 같은 매개변수로 두 번 돌리면 토픽에 무엇이 쌓이나?
5. 보존을 `retention.bytes` 256MB, `segment.bytes` 64MB로 정했다.
   - 왜 이 값이 토픽 전체가 아니라 파티션마다라는 점이 중요한가?
   - 지우는 단위가 세그먼트라는 것은 무슨 뜻인가?
   - 실시간 잡이 아직 읽지 못한 오프셋이 보존 한도에 걸려 지워지면 어떻게 되나?
6. 늦춤을 두 종류로 넣었다. p는 0~8분이고, q는 15~20분이다.
   - 왜 실시간 집계의 워터마크 10분 양옆에 두었나?
   - 시드를 고정한 이유는?

## 직접 돌려 볼 실험
1. **메시지 하나에 든 시각 세 가지 보기** (`docker compose --profile stream up -d kafka` 뒤, 재생을 한 번 돌린 상태에서)
   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events --from-beginning --max-messages 3 --formatter-property print.timestamp=true --formatter-property print.partition=true --formatter-property print.key=true
   ```
   볼 것: `CreateTime`(보낸 시각, 1970년부터 밀리초), `event_time`(옮긴 이벤트 시각), `src_event_time`(2019년 원래 시각)을 나란히 놓고 Δ와 재생 속도를 계산해 본다.
2. **오프셋 커밋과 다시 읽기**
   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events --group mine --from-beginning --max-messages 1000 > /dev/null
   ```
   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group mine
   ```
   볼 것: `CURRENT-OFFSET`의 합이 1000이고, `LAG`가 나머지다. 첫 명령을 한 번 더 돌리고 다시 describe한다. `--from-beginning`을 줬는데도 처음부터 읽지 않는 이유를 찾아라. 힌트는 커밋한 오프셋이 있을 때와 없을 때의 차이다.

## 학습 자료
링크는 2026-10-12에 열리는 것을 확인했다. 모두 이 프로젝트의 Kafka 4.3 문서다.
- [Kafka 4.3 — Introduction](https://kafka.apache.org/43/getting-started/introduction/): 'Main Concepts and Terminology' 절만 본다.
  > 요약: 이벤트는 토픽에 쌓이고, 토픽은 파티션으로 나뉘어 여러 브로커에 흩어진다. 키가 같은 이벤트(예: 고객 id)는 같은 파티션에 쓰이고, 파티션 안에서는 쓴 순서대로 읽힌다.
- [Kafka 4.3 — Design](https://kafka.apache.org/43/design/design/): 'Consumer Position'과 'Message Delivery Semantics' 절만 본다.
  > 요약: 컨슈머는 파티션마다 정수 하나(오프셋)로 어디까지 읽었는지를 기억하고, 이것을 주기적으로 커밋한다. 기본 보장은 '적어도 한 번'이다. 멱등 전송을 켜면 생산자가 다시 보내도 로그에 중복이 생기지 않는다.
- [Kafka 4.3 — Consumer Rebalance Protocol](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/): 'Overview'만 본다.
  > 요약: 그룹 구성원이 바뀌면 파티션을 다시 나누는데, 새 프로토콜(KIP-848)은 이 일을 브로커가 맡아 리밸런싱을 덜 멈추게 한다. 자바 컨슈머의 기본값은 아직 옛 방식(classic)이고, `group.protocol=consumer`로 켠다. 이 프로젝트의 실험은 기본값(classic)으로 했다.
