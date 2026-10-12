import os
import sys
import uuid
from pathlib import Path

import pytest

from pipeline.spark import get_spark

JARS = Path(__file__).resolve().parent.parent / "jars"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # 노트북 호스트 이름이 네트워크에 따라 남의 IP로 해석되면 드라이버가 포트를 못 연다(2026-10-07 겪음).
    # 테스트에서만 루프백에 묶는다. 파이프라인 코드에 넣으면 클러스터에서 드라이버 주소가 깨진다.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    # Python 워커(createDataFrame 등)가 PATH의 다른 python3를 집으면 버전이 달라 실패한다. 드라이버와 같은 것을 쓴다
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    listed = {line.split()[1] for line in (JARS / "SHA256SUMS").read_text().splitlines()}
    if {p.name for p in JARS.glob("*.jar")} != listed:
        pytest.exit("lake jar가 SHA256SUMS와 다르다. 먼저 `bash jars/fetch.sh`를 실행한다", returncode=1)
    os.environ.setdefault("LAKE_JARS", str(JARS))
    os.environ.pop("LAKE_CATALOG_URI", None)  # 호스트는 도커 카탈로그에 닿지 않는다. hadoop 카탈로그를 쓴다
    os.environ["LAKE_WAREHOUSE"] = str(tmp_path_factory.mktemp("lake"))
    # 프로세스에 세션은 하나다. 1·2단계 테스트도 이 세션을 같이 써서, lake jar가 그 결과를 바꾸지 않는지도 함께 확인된다.
    s = get_spark("test", lake=True)
    s.conf.set("spark.sql.shuffle.partitions", "4")
    return s


@pytest.fixture
def ns(spark):
    from pipeline.lake import create_tables
    name = f"lake.t{uuid.uuid4().hex[:8]}"
    create_tables(spark, name)
    return name
