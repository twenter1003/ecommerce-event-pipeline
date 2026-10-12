import os

from pyspark.sql import SparkSession


def get_spark(app_name: str, lake: bool = False) -> SparkSession:
    # master는 정하지 않는다. `python`으로 실행하면 local[*], Dataproc에 제출하면 YARN이 된다.
    b = (
        SparkSession.builder.appName(app_name)
        # 날짜 파티션은 UTC 기준. 노트북 시간대(KST)에 따라 dt가 바뀌면 안 된다.
        .config("spark.sql.session.timeZone", "UTC")
        # 로컬 모드는 드라이버 JVM 하나가 전부라 셔플 메모리도 여기서 나온다.
        # `python -m`으로 띄울 때만 적용된다. spark-submit(Dataproc 포함)은 JVM이 먼저 떠서 무시되니 --driver-memory로 준다.
        .config("spark.driver.memory", "8g")
    )
    if lake:  # 3단계 lake(ADR 0006). 1·2단계 작업(load, quality)의 JVM 구성은 그대로 둔다
        b = (
            b
            # 시스템 클래스패스에 올려야 JDBC 카탈로그의 DriverManager가 postgres 드라이버를 찾는다(spark.jars는 자식 클래스로더).
            # spark.driver.memory처럼 JVM이 뜰 때 넘어가는 설정이라 첫 getOrCreate 전에만 효과가 있다.
            .config("spark.driver.extraClassPath", f"{os.environ['LAKE_JARS']}/*")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        )
        uri = os.environ.get("LAKE_CATALOG_URI")
        conf = (
            # S3 키와 리전은 넣지 않는다. S3FileIO가 AWS 기본 자격 증명 체인(환경 변수)으로 읽어 명령줄·이벤트 로그에 남지 않는다
            {"type": "jdbc", "uri": uri, "warehouse": "s3://warehouse/",
             "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
             "s3.endpoint": "http://seaweedfs:8333",  # compose 서비스 이름
             "s3.path-style-access": "true"}
            if uri else  # 호스트 테스트: 로컬 폴더의 hadoop 카탈로그(로컬 rename은 원자적이다)
            {"type": "hadoop", "warehouse": os.environ["LAKE_WAREHOUSE"]}
        )
        for k, v in conf.items():
            b = b.config(f"spark.sql.catalog.lake.{k}", v)
    return b.getOrCreate()
