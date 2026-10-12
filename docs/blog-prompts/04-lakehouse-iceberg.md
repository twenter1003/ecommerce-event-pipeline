# 04. 3단계 레이크 기반: SeaweedFS 위의 Iceberg, 원자적 커밋, 빈 날짜 함정

- 대상 코드: `pipeline/lake.py`, `pipeline/spark.py`(`lake=True`), `docker-compose.yml`(`seaweedfs`, `lake-init`), `jars/fetch.sh`, `bench/lake_commits.py`, `tests/test_lake.py`
- 설계 기록: [ADR 0006](../decisions/0006-stage3-storage.md)(측정 절 포함), [ADR 0007](../decisions/0007-stage3-modeling.md) 결정 1
- 상태: 미답변

## 다룬 개념
- **테이블 포맷(Iceberg)**: Parquet 파일 위에 "이 테이블은 지금 어떤 파일들로 이루어졌나"를 적은 메타데이터 층을 올린 것이다. 폴더 목록이 아니라 메타데이터가 테이블의 진실이다.
- **스냅숏과 시간 여행**: 커밋할 때마다 테이블 전체 상태(파일 목록)가 새 스냅숏으로 남는다. 옛 스냅숏을 지정해 그때의 테이블을 읽을 수 있다.
- **카탈로그와 CAS 커밋**: 카탈로그는 "테이블 이름 → 현재 메타데이터 파일 위치"를 들고 있다. 커밋은 그 포인터를 "내가 본 값일 때만" 새 값으로 바꾸는 한 번의 교체(compare-and-swap)다. 다른 쪽이 먼저 바꿨으면 최신 상태 위에서 다시 시도한다(낙관적 동시성).
- **오브젝트 저장소에는 rename이 없다**: 폴더를 통째로 옮기는 원자적 이름 바꾸기가 없다. 그래서 "임시 폴더에 쓰고 옮기는" 커밋 방식(1단계)은 안전하지 않고, Iceberg는 포인터 교체로 커밋한다.
- **필터 덮어쓰기 vs dynamic 덮어쓰기**: 둘 다 "그날만 갈아 끼우기"지만, 결과가 비었을 때 다르게 동작한다.

## 질문 (답은 블로그에)
1. 1단계(로컬 Parquet)와 Iceberg에서 "하루치 다시 쓰기"는 각각 어떤 순서로 일어나나? 쓰는 도중에 죽으면 각각 무엇이 남나? I4 실험에서 행 수는 그대로였는데 938MB가 남은 이유와, `DROP TABLE … PURGE`가 그것을 지우지 못한 이유를 설명하라.
2. 테스트 `test_empty_rewrite_trap…`에서 `overwritePartitions()`는 옛 행을 남기고 `lake.overwrite()`는 지웠다. 왜 이런 차이가 생기고, 매일 도는 파이프라인에서 어느 쪽이 위험한가? 1단계 ADR 0003에서 같은 함정을 어떻게 막았는지와 비교하라.
3. 카탈로그를 Postgres(JDBC)에 두었다. 두 태스크가 같은 테이블의 서로 다른 날짜를 동시에 커밋하면 무슨 일이 생기나? 같은 날짜라면? "S3 위의 hadoop 카탈로그"는 왜 안 되나?
4. SeaweedFS 체크섬 버그는 이슈에서 "약 50%"로 실패했다는데, 우리 환경에서는 우회를 끄고 220번 커밋해도 0번이었다. 이 결과로 "버그가 없다"고 말할 수 있나? 그런데도 우회를 남긴 이유는 무엇인가?
5. (면접형) "왜 7GB 데이터에 Iceberg를 썼나요?" 성능이 아니라면 무엇을 얻었고, 무엇을 대가로 냈나(부품, 설정, 고아 파일 관리)? 이 규모에서 안 쓰는 선택은 어떻게 변호할 수 있나?
6. (면접형) lake 준비(`lake-init`)를 Airflow 기동(`airflow-init`)과 분리한 이유는? 붙여 두면 어떤 장애가 1·2단계까지 번지나?

## 직접 돌려 볼 실험
1. **커밋마다 바뀌는 포인터 보기** (스택이 떠 있을 때)
   ```bash
   docker compose exec -T airflow-scheduler python -m pipeline.lake sql "CREATE NAMESPACE IF NOT EXISTS lake.lab" "CREATE TABLE lake.lab.t (id BIGINT) USING iceberg" "INSERT INTO lake.lab.t VALUES (1)" "INSERT INTO lake.lab.t VALUES (2)" "SELECT snapshot_id, operation FROM lake.lab.t.snapshots"
   ```
   ```bash
   docker compose exec -T postgres psql -U airflow -d iceberg -c "select table_name, metadata_location, previous_metadata_location from iceberg_tables where table_name='t'"
   ```
   볼 것: `metadata_location`의 번호(`0000N-…`)가 커밋 수만큼 늘었고, `previous_metadata_location`이 바로 앞 번호다. 첫 스냅숏 id로 `SELECT * FROM lake.lab.t VERSION AS OF <id>`를 하면 1만 보인다. 끝나면 `DROP TABLE lake.lab.t PURGE`.
2. **빈 날짜 함정 테스트만 돌려 보기**
   ```bash
   .venv/bin/python -m pytest -q tests/test_lake.py -k empty_rewrite
   ```
   볼 것: 테스트 코드에서 두 번의 `assert`가 각각 무엇을 증명하는지. `overwrite(..., F.col("dt") == F.lit(D1))`을 `overwritePartitions()`로 바꾸면 어느 줄에서 깨지는지.

## 학습 자료
- [Iceberg 1.12 — Spark Writes](https://iceberg.apache.org/docs/latest/spark-writes/): 'INSERT OVERWRITE'와 'MERGE INTO' 절만 본다.
  > 요약: INSERT OVERWRITE에는 데이터에서 파티션을 정하는 dynamic 방식과, 미리 정한 파티션을 지우는 static 방식이 있다. MERGE INTO는 조건에 따라 행을 고치고, 지우고, 넣는 것을 커밋 한 번에 한다.
- [Iceberg JDBC Catalog](https://iceberg.apache.org/docs/latest/jdbc/): 첫 문단과 설정 표만 본다.
  > 요약: 관계형 DB의 테이블 하나로 Iceberg 테이블들을 관리한다. DB가 원자적 트랜잭션을 지원해야 Iceberg 커밋이 원자적이 된다. 설정은 `uri`와 `jdbc.`으로 시작하는 연결 속성이다.
- [Iceberg Table Spec](https://iceberg.apache.org/spec/): 'Overview', 'Snapshots', 'Optimistic Concurrency' 절만 본다.
  > 요약: 테이블 상태가 바뀔 때마다 새 메타데이터 파일을 만들고, 옛 것을 원자적 교체로 갈아 끼운다. 스냅숏은 그 시점의 데이터 파일 목록이다. 커밋하려는데 기준 스냅숏이 이미 현재가 아니면, 새 현재 상태를 기준으로 다시 시도한다.
