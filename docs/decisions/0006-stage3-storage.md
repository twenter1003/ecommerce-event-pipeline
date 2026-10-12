# 0006. 3단계 저장 계층: SeaweedFS + Iceberg(JDBC 카탈로그)

- 날짜: 2026-10-07
- 단계: 3
- 상태: 채택 (2026-10-07, 3-1 통합 검증 완료)

## 배경
3단계(세션·퍼널, fact/dim, SCD2, 마트)의 결과를 어디에, 어떤 형식으로 둘지 정해야 한다.

**사용자 결정 (2026-10-07).** 3단계에 SeaweedFS(S3 호환 오브젝트 저장소)와 Apache Iceberg를 도입한다.
- Claude는 둘 다 넣지 말자고 추천했다. 이유는 두 가지였다. 이 규모(7GB)에서는 성능 이득이 없고, 사용자가 빨리 끝내고 싶어 한다.
- 사용자는 학습 가치를 골랐다. 오브젝트 저장소의 특성, 그리고 MERGE·원자적 커밋·시간 여행을 배우려는 것이다.

**전제가 바뀐 점.** ADR 0001은 "3단계는 체험판이 끝난 뒤라 S3 특성은 SeaweedFS로 배운다"고 적었다. 그런데 2단계가 일찍 끝나서, 3단계가 GCP 스프린트보다 먼저 오게 됐다(사용자 결정). 스프린트에는 1·2단계 DAG만 올린다(ADR 0007 결정 5).

**어떻게 정했나.** 2026-10-07에 다음 순서로 정했다.
1. 조사 담당 2명이 SeaweedFS·Iceberg의 버전과 설정을 공식 문서와 소스로 확인했다.
2. 관점이 다른 설계안 3개를 만들었다. 관점은 멱등성, 단순함, 학습 가치다.
3. 심사 2명이 모두 '단순함' 안을 골랐다.
4. 그 안을 바탕으로 하나로 합쳤다.
5. 반박 검증 4명이 멱등성, 기술 실현성, 범위, Airflow 동작을 공격했다. 중대 이슈 8개를 모두 반영했다.

## 결정 1. 무엇을 어디에 두나
| 데이터 | 위치 | 형식 |
|---|---|---|
| raw, landing, events | `data/` 로컬 (그대로) | gz, csv.gz, zstd Parquet |
| 3단계 테이블 5개 | `s3://warehouse/dw/<table>` (SeaweedFS) | Iceberg v2, Parquet zstd |
| 카탈로그 | Postgres 안의 DB `iceberg` | Iceberg JDBC 카탈로그 |
| 호스트 테스트 | pytest 임시 폴더 | Iceberg hadoop 카탈로그 (로컬 파일) |

lake는 **전부 파생 데이터**다. 지워도 events에서 다시 만들 수 있다. 그래서 백업도, 스키마 마이그레이션도 하지 않는다. 스키마를 바꿀 때는 지우고 다시 계산한다(`pipeline.lake reset`).

**기각한 대안**
- events까지 Iceberg로 옮기기: 1·2단계 코드와 테스트 21개를 다시 짜고 7.2GB를 다시 써야 한다. 그런데 새로 배우는 3단계 개념이 없다.
- landing을 S3에 두고 S3A로 읽기: hadoop-aws가 쓰는 AWS SDK(2.29.52)와 Iceberg가 쓰는 SDK(2.54.17)가 한 클래스패스에서 부딪힌다. iceberg-aws-bundle은 패키지 이름을 바꾸지 않고 그대로 담기 때문이다.

## 결정 2. SeaweedFS: `weed mini` 컨테이너 하나
- 이미지는 `chrislusf/seaweedfs:4.48`(2026-09-28)이고 arm64 이미지가 있다. `weed mini` 하나가 master, volume, filer, S3 게이트웨이를 모두 띄운다. 분산 구성은 이번 단계에서 배울 것이 아니다.
- 호스트에 포트를 하나도 열지 않는다. S3(8333)는 compose 네트워크 안에서만 쓴다.
  - 관리 화면(23646)도 열지 않는다. `weed mini`의 기본값이 인증 없음(`-admin.password` 빈 값)이라, 열면 S3 키 없이 버킷을 고치거나 새 자격 증명을 만들 수 있다(3-1 검토에서 찾음). 관리는 `docker compose exec seaweedfs weed shell`로 한다.
- **자격 증명:** 키는 `.env`의 `LAKE_S3_ACCESS_KEY`·`LAKE_S3_SECRET_KEY`에 둔다. 컨테이너가 시작할 때 이 값으로 `s3.json`을 컨테이너 안 `/tmp`에 만든다. 키 파일이 저장소에도 호스트에도 남지 않는다.
  - 키 없이 띄우면 누구나 읽고 쓸 수 있게 열린다(allow-all). 이 방식은 쓰지 않는다.
  - 권한은 Read/Write/List부터 준다. 모자라면 Admin을 더하고 이유를 여기에 적는다.
- 버킷은 `warehouse` 하나다(`-bucket=warehouse`). 실험 테이블은 네임스페이스 `lake.lab`에 둔다.
- 데이터는 이름 있는 볼륨 `seaweedfs-data`에 둔다. 그래서 `docker compose down -v`를 하면 카탈로그(Postgres 볼륨)와 lake 파일이 **함께** 지워져 서로 어긋나지 않는다.
  - 바인드 마운트(`./data/lake`)는 기각했다. 호스트에서 크기를 보기는 쉽다. 하지만 `down -v`가 카탈로그만 지워 파일이 고아로 남고, 볼륨 파일(.dat/.idx)이라 열어 봐도 읽을 수 없다.

**체크섬 버그 우회.** SeaweedFS 4.48에는 버그 #11582가 있다.
- 증상: AWS SDK for Java v2 2.30 이상은 업로드에 체크섬 trailer(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`)를 기본으로 붙인다. SeaweedFS 4.48은 이 업로드를 약 50% 확률로 `400 InvalidDigest`로 거부한다.
- 수정(PR #11583)은 2026-10-03에 병합됐지만, 아직 정식 릴리스에 들어가지 않았다.
- Iceberg 1.12.0은 SDK 2.54.17을 쓴다. 그리고 S3FileIO에는 이 체크섬을 끄는 속성이 없다(`s3.checksum-enabled`는 eTag 검사다).
- 그래서 compose 공통 환경 변수로 끈다: `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED`, `AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED`.
- 확인 방법: 우회를 끄고 작은 커밋을 20번 하면 버그가 재현돼야 한다(실패율을 기록). 켜고 200번 하면 0건이어야 한다.
- 수정이 들어간 버전(4.49 이상)으로 올리면 두 줄을 지운다.

## 결정 3. 카탈로그: Postgres 안의 DB `iceberg` (JDBC)
- **커밋 방식:** 커밋은 `UPDATE iceberg_tables SET metadata_location=새값 WHERE … AND metadata_location=이전값` 한 문장이다. 다른 쪽이 먼저 바꿨으면 바뀐 행이 0개가 된다. 그러면 Iceberg가 최신 메타데이터 위에서 다시 시도한다(낙관적 동시성, CAS).
  - 재시도 횟수는 테이블 속성 `commit.retry.num-retries=10`이다. 기본 4회는 K개 날짜가 동시에 커밋할 때 빠듯하다.
- 카탈로그용 테이블(`iceberg_tables`, `iceberg_namespace_properties`)은 처음 연결할 때 Iceberg가 스스로 만든다. `jdbc.schema-version`은 기본값 V0으로 둔다(뷰를 쓰지 않는다).
- 계정은 `airflow`를 같이 쓴다. Spark가 Airflow 스케줄러 컨테이너 안에서 돌고, 그 컨테이너에는 이미 Airflow DB 접속 정보가 있다. 롤을 따로 만들어도 격리 효과가 없고 비밀값만 하나 는다.

| 기각한 후보 | 이유 |
|---|---|
| S3 위 hadoop 카탈로그 | 커밋이 원자적 rename에 기대는데 S3에는 rename이 없다. 이번에 배울 함정 그 자체라, 로컬 파일인 호스트 테스트에서만 쓴다 |
| SeaweedFS 내장 Iceberg REST 카탈로그 | 카탈로그가 저장소 제품에 묶인다. 커밋이 원자적이라는 근거를 우리가 직접 확인할 수 없다 |
| Nessie, Polaris, Hive Metastore | 상시 서비스와 메모리가 는다. K=3의 메모리 여유가 이미 약 1.2GiB뿐이다(ADR 0005) |

## 결정 4. jar: 받아서 마운트한다, 이미지는 그대로
| jar | 버전 |
|---|---|
| `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13` | 1.12.0 (Spark 4.1.3으로 빌드됨. 4.1.2에서 안 돌면 1.11.x로 내린다) |
| `org.apache.iceberg:iceberg-aws-bundle` | 1.12.0 (S3FileIO, AWS SDK 2.54.17) |
| `org.postgresql:postgresql` | 42.7.13 |

- `jars/fetch.sh`가 Maven Central에서 받고, 커밋한 `jars/SHA256SUMS`와 대조한다. jar 파일 자체는 커밋하지 않는다.
- 컨테이너에는 `./jars`를 읽기 전용으로 마운트한다. 호스트 테스트도 같은 파일을 쓴다. Dockerfile은 바꾸지 않는다.
- `spark.driver.extraClassPath=<jars>/*`로 시스템 클래스패스에 올린다. `spark.jars`로 올리면 자식 클래스로더에 들어가서, JDBC의 `DriverManager`가 postgres 드라이버를 못 찾을 위험이 있다("No suitable driver").
- `get_spark(app, lake=False)`에 플래그를 둔다. 1·2단계 작업(load, quality)의 JVM 구성과 메모리는 바뀌지 않는다.

| 기각한 후보 | 이유 |
|---|---|
| `spark.jars.packages`(Ivy) | 태스크마다 원격 저장소를 확인한다. 네트워크가 없으면 실패하고, 동시 태스크가 같은 캐시에 쓰다 깨질 수 있다 |
| Dockerfile에 COPY | 이미지를 다시 빌드해야 한다. 마운트면 0줄이다 |
| `pyspark/jars`에 복사 | 설정이 코드에 보이지 않고, `uv sync`를 하면 사라진다 |

## 결정 5. Iceberg에 쓰는 방법은 두 가지뿐이다
1. **필터 덮어쓰기** `lake.overwrite(df, table, cond)`: 내부에서 `df.filter(cond)` 뒤에 `writeTo(table).overwrite(cond)`를 한다. cond에 맞는 파일을 지우고 새 파일을 넣는 커밋 한 번이다.
2. **MERGE**: dim_product에만 쓴다(ADR 0007 결정 3).

**빈 날짜 함정 (ADR 0003과 같은 함정).**
- `overwritePartitions()`와 dynamic `INSERT OVERWRITE`는 새 파일이 0개면 커밋을 건너뛴다. 그래서 다시 처리한 날의 결과가 비어도 옛 데이터가 남은 채 '성공'한다.
- 필터 덮어쓰기는 새 파일이 0개여도 삭제를 커밋한다.
- 세션 설정에 `spark.sql.sources.partitionOverwriteMode=dynamic`을 넣지 않는다. Spark 4.1에서는 이 값이 dynamic이면, 정적 `PARTITION` 절을 적어도 dynamic으로 바꿔 처리한다.
- 호스트 학습 테스트로 두 방식의 차이를 고정한다.

**동시 실행과 원자성**
- 서로 다른 dt를 동시에 덮으면 둘 다 성공한다. 덮어쓰기의 기본 격리 수준이 충돌로 보지 않고, CAS가 실패해도 새 base 위에서 다시 적용하기 때문이다.
- 같은 dt를 두 번 쓰면 마지막 커밋이 이긴다. 다시 적용할 때 필터가 앞 커밋의 파일까지 지우므로 중복이 남지 않는다.
- 커밋은 메타데이터 포인터를 바꾸는 한 번의 CAS다. 그래서 테이블은 늘 옛 스냅숏이거나 새 스냅숏이고, 중간 상태가 없다.
- 이로써 lake 테이블에서는 ADR 0003의 한계 1(파티션 교체가 원자적이지 않다)과 한계 2(같은 날짜 동시 쓰기)가 풀린다. events(로컬 Parquet)에는 그 한계가 그대로 남는다.

## 결정 6. lake 준비를 Airflow 기동에서 떼어 낸다
일회성 서비스 `lake-init`을 따로 둔다. Airflow 이미지를 쓰고 `restart: "no"`다. 하는 일은 셋이다.
1. 마운트된 jar가 `SHA256SUMS`와 같은지 확인한다. 받다가 끊긴 jar 때문에 원인 모를 클래스 오류가 나지 않게 하려는 것이다.
2. Postgres에 DB `iceberg`가 없으면 만든다(psycopg2, 두 번 돌려도 안전).
3. `python -m pipeline.lake create`로 네임스페이스와 테이블 5개를 만든다(`IF NOT EXISTS`). 10초 간격으로 5번까지 다시 시도한다.
   - `weed mini`가 막 떴을 때는 버킷이 아직 없을 수 있다. 그러면 메타데이터를 쓰다 실패하고 카탈로그에는 아무것도 남지 않으므로, 다시 시도해도 안전하다.
   - 처음 설계는 `HEAD /warehouse`로 버킷을 기다리는 것이었다. 하지만 인증을 켜면 익명 요청이 버킷이 있든 없든 403을 받아 구별이 안 된다. 재시도가 더 단순하다.

- scheduler·api-server·dag-processor는 `lake-init`에 의존하지 않는다. jar가 없거나 SeaweedFS가 늦게 떠도 1·2단계는 그대로 돈다. 테이블이 없으면 lake 태스크만 시끄럽게 실패한다.
- 처음 설계는 이 일을 `airflow-init` 끝에 붙였다. 반박 검증이 막아 냈다. 그대로 두면 lake 준비가 실패할 때 Airflow 전체가 뜨지 않는다.
- 테이블을 백필 태스크 안에서 만들지 않는다. K개가 동시에 만들다 부딪히지 않게 하려는 것이다.

## 결정 7. 유지보수
- `lake_marts`가 끝날 때(성공이든 실패든) 테이블마다 `expire_snapshots(older_than => 지금, retain_last => 10)`를 실행한다.
  - 기본값(5일)이나 '지금 − 1일'로 두면, 같은 날 백필을 반복하는 동안 아무것도 지우지 못한다. 그러면 옛 fct_sessions 파일이 두 배로 쌓인다.
  - 시간 여행 실험은 최근 10개 스냅숏 안에서 한다.
- 만료는 `.partitions`의 `last_updated_at`을 지운다. 그 값은 파일을 추가한 스냅숏이 살아 있을 때만 계산되기 때문이다. 그래서 신선도 판단에는 쓰지 않고, 데이터에 실은 계보 컬럼을 쓴다(ADR 0007 결정 6).
- 옛 `metadata.json`은 `expire_snapshots`가 지우지 않는다. 그래서 테이블 속성 `write.metadata.delete-after-commit.enabled=true`, `write.metadata.previous-versions-max=20`으로 커밋할 때 지운다.
- `remove_orphan_files`는 일정에 넣지 않는다. 쓰기와 동시에 돌면 쓰는 중인 파일을 지울 수 있다.
  - 3-1 강제 종료 실험에서는 고아 파일을 프로시저가 아니라 **목록 비교로 셌다**. 저장소 목록(`weed shell`의 `fs.du`·`fs.ls`)과 Iceberg가 아는 파일(`.all_files`·`.files`)을 비교했다(측정 절).
  - 프로시저를 실험에 쓰지 못한 이유: 24시간보다 가까운 `older_than`은 dry run이어도 거부된다. 이 안전장치를 끄는 `spark.testing`은 실행 중에 바꿀 수 없는 코어 설정이고(`CANNOT_MODIFY_CONFIG`), 시작할 때 주면 JVM 시스템 속성이 되어 Spark 전체가 테스트 모드가 된다.
  - 운영에서 고아 파일을 지울 때는 기본 안전장치(24시간보다 오래된 것만)를 그대로 두고, S3A가 없으므로 `prefix_listing => true`를 준다.
  - 실험 세션에서만 `spark.conf.set("spark.testing", "true")`: Iceberg는 24시간보다 가까운 `older_than`을 dry_run이어도 거부한다. 실험에서만 이 안전장치를 일부러 끈다. Iceberg는 이 값을 세션 설정에서 읽는다. JVM 속성(`-Dspark.testing`)으로 주면 Spark 내부의 테스트 전용 동작까지 켜지므로 그렇게 하지 않는다.
- `rewrite_data_files`도 넣지 않는다. 날짜 파티션마다 파일이 1개라 작은 파일 문제가 없다.

## 초기화 절차 (README에도 적는다)
| 목적 | 명령 | 결과 |
|---|---|---|
| lake만 다시 만들기 | `python -m pipeline.lake reset` 뒤 `lake_daily` 백필 → `lake_marts` | 테이블을 지우고(PURGE) 파일까지 지운 뒤 다시 만든다. events는 건드리지 않는다 |
| 전부 초기화 | `docker compose down -v` → `up -d` → events_daily 백필 → lake_daily 백필 | Postgres 볼륨(Airflow 메타데이터 + 카탈로그)과 `seaweedfs-data`가 함께 지워진다. `./data`는 남는다 |
| 하면 안 되는 것 | 볼륨 하나만 `docker volume rm` | 카탈로그와 파일이 어긋난다. 그러면 '전부 초기화'로 맞춘다 |

## 3-1에서 확인할 것 (확인 전까지는 가정)
| 가정 | 틀리면 |
|---|---|
| Iceberg 1.12.0이 PySpark 4.1.2에서 필터 덮어쓰기, MERGE, 프로시저까지 돈다 | 1.11.x로 내린다 |
| `extraClassPath` 와일드카드로 JDBC 드라이버가 잡힌다 | jar 경로를 하나씩 나열한다 |
| 환경 변수로 체크섬 우회가 S3FileIO에 먹힌다 | 수정이 들어간 SeaweedFS 이미지로 바꾼다 |
| 필터 덮어쓰기가 새 파일 0개에서도 삭제를 커밋한다 | 먼저 `DELETE FROM t WHERE dt=D`를 실행한다 |
| 파티션 테이블의 기본 쓰기 분배가 hash라 dt마다 파일이 1개다 | `write.distribution-mode=hash`를 명시한다 |
| `VERSION AS OF <snapshot id>`(또는 읽기 옵션 `snapshot-id`)로 입력을 한 스냅숏에 고정할 수 있다(ADR 0007 신선도 관문) | 스냅숏 id 대신 실행 시작 전의 커밋 수를 비교한다 |
| Read/Write/List 권한으로 충분하다 | Admin을 더한다 |
| `weed mini`가 재시작해도 `-bucket`이 멱등이고, 볼륨 수 상한에 걸리지 않는다 | `weed shell`로 만들거나 볼륨 크기 플래그를 조정한다 |

## 측정 (3-1, 2026-10-07, Docker VM 9.7GiB)
| 확인 | 결과 |
|---|---|
| 호스트 테스트 | 27개 통과(기존 21 + 새 6). 1·2단계 테스트도 lake jar가 붙은 세션에서 그대로 통과한다 |
| I1 기동 | 모든 서비스 정상. `lake-init`이 카탈로그 DB와 테이블 5개를 만들고, 두 번째 실행도 성공(종료 코드 0, 테이블 그대로 5개). SeaweedFS는 쉴 때 **96MiB**를 쓴다 |
| I2 smoke | 만들기 → 넣기 → 같은 날짜 두 번 덮어쓰기 → MERGE 삭제 → 최종 1행. 스냅숏 4개(append +2, overwrite +1/−1, overwrite +1/−1, delete −1). 파티션당 파일 1개. 첫 스냅숏으로 시간 여행하면 옛 행이 보인다. 카탈로그 DB의 `metadata_location`이 `00000`부터 `00004`까지 커밋마다 바뀌고, `previous_metadata_location`이 직전 값을 가리킨다 |
| I3 체크섬 버그 | **재현하지 못했다.** 우회를 끄고 220번 커밋했는데 실패가 0건이다(실패율이 있다면 95% 신뢰 상한 약 1.4%). 이슈 원문에 따르면 trailer 줄이 별도 TCP 조각으로 도착할 때만 생기고, 보고자도 macOS에서는 대개 성공했다. 같은 VM 안 컨테이너끼리는 이 조건이 잘 생기지 않는 것으로 본다. SDK가 실제로 trailer 방식으로 보냈는지는 패킷으로 확인하지 못했다. 우회는 비용이 없으니 그대로 둔다 |
| I4 강제 종료 | 11/01(1,444,754행)이 든 실험 테이블에 29일치를 넣는 도중 25초에 JVM을 `kill -9`로 죽였다. 행 수와 스냅숏 수(1개), 카탈로그 포인터가 그대로였다. **원자적이다.** 대신 저장소에는 커밋되지 않은 데이터 파일 약 26개, 938MB가 고아로 남았다(테이블이 아는 파일은 25MB 1개). `DROP TABLE … PURGE` 뒤에도 그 938MB가 남았다. PURGE는 메타데이터가 아는 파일만 지우기 때문이다. `weed shell`의 `fs.rm`으로 지웠다 |

비교: ADR 0003 한계 1. 로컬 Parquet은 커밋할 때 날짜 폴더를 지운 뒤 staging 폴더를 옮긴다. 그래서 그 사이에 죽으면 그 날짜가 비거나 일부만 남는다. Iceberg는 테이블이 그대로이고, 대가로 고아 파일이 남는다.

**확인 목록 결과**
| 가정 | 결과 |
|---|---|
| Iceberg 1.12.0이 PySpark 4.1.2에서 필터 덮어쓰기·MERGE까지 돈다 | 맞음(호스트 테스트, I2). 1.11로 내리지 않는다 |
| `extraClassPath` 와일드카드로 JDBC 드라이버가 잡힌다 | 맞음(I1) |
| 필터 덮어쓰기가 새 파일 0개에서도 삭제를 커밋한다 | 맞음. `overwritePartitions()`는 옛 행을 남긴다(호스트 학습 테스트) |
| 파티션 테이블의 기본 쓰기 분배로 dt마다 파일이 1개다 | 맞음(I2, I4) |
| `VERSION AS OF <snapshot id>` | 맞음(I2) |
| 환경 변수로 체크섬 우회가 S3FileIO에 먹힌다 | 확인하지 못했다. 우회를 꺼도 버그가 재현되지 않아(I3), 켜서 고쳐지는지를 볼 수 없었다. 우회를 켠 채 200번 커밋해 실패 0건인 것만 확인했다 |
| Read/Write/List 권한으로 충분하다 | 맞음. 만들기·쓰기·덮어쓰기·MERGE·DROP PURGE 모두 성공 |
| `weed mini -bucket`이 재시작해도 멱등이다 | 맞음(스택을 다시 올려도 lake-init 성공) |
| `remove_orphan_files` dry run | 쓰지 못했다(결정 7). 고아 파일은 목록 비교로 직접 셌다 |

**덤으로 겪은 것.** Docker Desktop의 자격 증명 도우미(`docker-credential-desktop get`)가 macOS 키체인 접근에서 멈춰, 이미지 받기가 아무 출력 없이 걸렸다. 공개 이미지는 자격 증명이 필요 없으므로, 그 명령에만 빈 임시 Docker 설정(`DOCKER_CONFIG`)을 써서 익명으로 받았다.

## 다시 검토할 조건
- SeaweedFS에 #11582 수정이 정식으로 들어오면 → 체크섬 환경 변수 두 줄을 지운다.
- 4단계에서 Kafka 이벤트를 Iceberg에 바로 쓰게 되면 → events를 Iceberg로 옮기는 것을 다시 본다.
- lake 백필의 K를 정할 때는 `1.1GiB(쉴 때) + seaweedfs + lake JVM 최대 RSS × K < 9.7GiB − 1.0GiB(안전 여유)`를 만족해야 한다. K=2에서도 만족하지 않으면 → `weed mini` 메모리 옵션을 보거나 Docker VM 메모리를 늘린다.

## 출처 (2026-10-07 조사 담당이 열어서 확인)
- SeaweedFS: [releases](https://github.com/seaweedfs/seaweedfs/releases), [Quick Start with weed mini](https://github.com/seaweedfs/seaweedfs/wiki/Quick-Start-with-weed-mini), [S3 Credentials](https://github.com/seaweedfs/seaweedfs/wiki/S3-Credentials), [Amazon S3 API](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API), [이슈 #11582](https://github.com/seaweedfs/seaweedfs/issues/11582), [PR #11583](https://github.com/seaweedfs/seaweedfs/pull/11583)
- Iceberg 1.12.0(git 태그 `apache-iceberg-1.12.0`의 문서 원문): spark-writes.md(덮어쓰기·MERGE), jdbc.md, configuration.md, spark-procedures.md, `S3FileIOProperties.java`, `RemoveOrphanFilesProcedure.java`
- [AWS SDK 데이터 무결성 설정](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html), [Spark 4.1.2 pom.xml(Hadoop 3.4.2)](https://raw.githubusercontent.com/apache/spark/v4.1.2/pom.xml)
