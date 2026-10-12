#!/usr/bin/env bash
# lake(3단계) jar 3개와 Kafka 커넥터(4단계) jar 4개를 Maven Central에서 받고, 커밋한 SHA256SUMS와 대조한다.
# 설계는 ADR 0006 결정 4, ADR 0009 결정 1.
# 호스트 테스트와 컨테이너(읽기 전용 마운트)가 같은 파일을 쓴다. jar 자체는 커밋하지 않는다.
#   bash jars/fetch.sh
set -euo pipefail
cd "$(dirname "$0")"
M=https://repo1.maven.org/maven2
for p in org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0 \
         org/apache/iceberg/iceberg-aws-bundle/1.12.0 \
         org/postgresql/postgresql/42.7.13 \
         org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.2 \
         org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.2 \
         org/apache/kafka/kafka-clients/3.9.1 \
         org/apache/commons/commons-pool2/2.12.1; do
  f="$(basename "$(dirname "$p")")-$(basename "$p").jar"
  # 끝까지 받은 파일만 최종 이름을 얻는다. 중간에 끊기면 .part가 남고 다음 실행이 처음부터 다시 받는다
  [ -f "$f" ] || { curl -fsSLo "$f.part" "$M/$p/$f" && mv "$f.part" "$f"; }
done
shasum -a 256 -c SHA256SUMS || { echo "체크섬이 다른 jar를 지우고 다시 실행한다" >&2; exit 1; }
# 클래스패스는 jars/*라서, 버전을 바꾼 뒤 남은 옛 jar가 섞여 오르면 안 된다
if ! diff <(ls *.jar) <(awk '{print $2}' SHA256SUMS | sort) >/dev/null; then
  echo "SHA256SUMS에 없는 jar가 있다. 지우고 다시 실행한다:" >&2
  comm -23 <(ls *.jar) <(awk '{print $2}' SHA256SUMS | sort) >&2
  exit 1
fi
