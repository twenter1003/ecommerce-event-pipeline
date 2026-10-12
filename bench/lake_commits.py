"""작은 커밋 N번을 하고 실패를 센다. SeaweedFS 체크섬 버그(#11582) 재현과 우회 확인용. 설계는 ADR 0006 결정 2.

컨테이너에는 bench/가 마운트돼 있지 않아 복사해서 돌린다.
    docker compose cp bench/lake_commits.py airflow-scheduler:/tmp/lake_commits.py
    docker compose exec -T airflow-scheduler python /tmp/lake_commits.py 200
    # 우회를 끈 채로(재현):
    docker compose exec -T -e AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_SUPPORTED \
        -e AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_SUPPORTED airflow-scheduler python /tmp/lake_commits.py 20
"""
import os
import sys

from pipeline.spark import get_spark

n = int(sys.argv[1])
spark = get_spark("lake-commits", lake=True)
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.lab")
spark.sql("CREATE TABLE IF NOT EXISTS lake.lab.ping (i INT) USING iceberg")
failed = []
for i in range(n):
    try:
        spark.sql(f"INSERT INTO lake.lab.ping VALUES ({i})")
    except Exception as e:  # 실패 종류만 센다(메시지 첫 줄)
        failed.append(str(e).splitlines()[0][:200])
print(f"checksum={os.environ.get('AWS_REQUEST_CHECKSUM_CALCULATION')} commits={n} failed={len(failed)}")
for f in sorted(set(failed)):
    print(f"  {failed.count(f)}x {f}")
