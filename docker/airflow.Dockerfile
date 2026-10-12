# Airflow 3.3.1(Composer 3와 같은 버전) + Java 21 + PySpark 4.1.2. 설계는 ADR 0004.
# Debian bookworm 저장소에는 openjdk-21이 없어 공식 Temurin 이미지에서 JRE만 복사한다.
FROM eclipse-temurin:21.0.12.1_1-jre AS jre

FROM apache/airflow:3.3.1-python3.11
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"
# apache-spark provider는 넣지 않는다. 그 의존성 pyspark-client가 pyspark 파일을 덮어써 import가 깨진다.
# confluent-kafka는 4단계 재생기용이다. 버전은 Airflow 제약 파일이 정한다(2.15.0, ADR 0009 결정 7).
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark==4.1.2" confluent-kafka \
    --constraint "${HOME}/constraints.txt"
