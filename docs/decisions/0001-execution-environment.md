# 0001. 실행 환경: 로컬 vs GCP vs AWS

- 날짜: 2026-10-03
- 단계: 공통 (1~4)
- 상태: 채택

## 배경
데이터를 받기 전에 어디서 돌릴지 정해야 한다. 이번 결정은 아래 제약 조건 안에서 이뤄진다.

- **장기 비용은 0원.** 단, GCP 무료 체험 크레딧 ₩414,984(USD 300)가 남아 있다. 체험은 2026-12-08 전후에 끝난다(시작일로부터 3개월).
- **시간은 주 10시간 정도.** 체험 기간 안에 실제로 쓸 수 있는 시간은 70~90시간으로, 1·2단계를 마치고 3단계에 들어가는 정도다.
- **노트북:** Apple M5 10코어, RAM 24GB, 디스크 여유 147GB, Docker Desktop(VM에 현재 9.7GiB 할당).
- **데이터 규모.** 7개월치가 41,170만 행, CSV로 56.6GB, gz로 17.7GB다. 2019-10 한 달은 4,245만 행, CSV 5.67GB다. 10월을 zstd Parquet로 저장하면 약 0.73GB다(Databricks에서 측정한 값이라 로컬에서는 다를 수 있음).
  - 이 정도면 노트북 한 대로 충분하다. 10월 한 달은 DuckDB/Polars로도 쉽게 처리하고, 7개월 전체도 나눠서 처리하면 단일 노드에서 된다.
  - **Spark와 Kafka는 데이터 규모 때문에 쓰는 게 아니다.** 실행 모델을 배우고 채용 공고에 맞추려고 쓴다. 이 점을 숨기지 않고 기록해 둔다.

## 후보
| 후보 | 장점 | 단점 |
|---|---|---|
| **A. 로컬 전용** (Docker) | 영구적으로 0원이다. 노트북(10코어/24GB)이 체험판 상한(동시 8 vCPU)보다 크다. 엔진 내부(Spark UI, 셔플, 오프셋)에 집중할 수 있다. 국내 B2C 회사들도 오픈소스를 직접 운영하는 곳이 많아서 로컬 포트폴리오도 통한다. | 클라우드 경험(IAM, 관리형 서비스, 비용 감각)이 0이다. 오브젝트 스토리지의 특성(원자적 rename 없음 등)을 겪으려면 로컬 S3 서버를 따로 띄워야 한다. 이력서에 클라우드 한 줄이 없다. |
| **B. 로컬 우선 + GCP 체험판 단기 스프린트** | 업그레이드만 안 하면 과금이 구조적으로 불가능해서 0원이 유지된다. GCS로 진짜 오브젝트 스토리지 특성을 겪을 수 있어 로컬 S3 서버가 필요 없다. Composer, Dataproc, BigQuery를 실제로 돌려 보고 비용을 기록하면 포트폴리오에 클라우드 경험이 생긴다. 체험이 끝나도 로컬에서 재현된다. | 12/8이라는 마감이 있다. 8 vCPU 제한 때문에 Serverless Spark는 못 쓴다. 관리형 서비스를 켜 두고 잊으면 크레딧이 일찍 바닥나고, 그 순간 체험이 끝나 데이터가 삭제된다. 로컬용·클라우드용 설정 두 벌을 관리해야 한다. |
| **C. GCP 중심** | 모든 단계를 클라우드에서 경험한다. | 66일 뒤 끝나므로 3·4단계는 어차피 로컬로 옮겨야 한다. 클라우드 쪽 연산이 노트북보다 작다(8 vCPU). Composer를 계속 켜 두면 약 17일 만에 크레딧이 바닥난다. 체험 이후 계속 쓰려면 업그레이드해야 하는데, 그건 0원 원칙 위반이다. |
| **D. AWS** (새 계정 Free plan) | 국내 점유율 1위다(클라우드 이용 기업의 60.2%). 커머스·배달 회사 상당수(쿠팡, 배민, 오늘의집, 무신사)가 AWS를 쓴다. Free plan도 크레딧이 바닥나면 계정이 닫히는 하드캡 구조다. | 크레딧이 $100~200로 적다. EMR, MSK, Kinesis는 Free plan에서 쓸 수 없다. MWAA는 월 $244 이상, MSK Serverless는 놀아도 월 $672이다. 지금 손에 있는 GCP 크레딧을 버리게 된다. |

### 판단에 영향을 준 사실 (2026-10-03 기준으로 검증)
- **GCP 체험판**
  - Compute Engine은 체험 계정 전체에서 **동시에 8코어**까지만 쓸 수 있고, 할당량 증설 요청도 안 된다.
  - Serverless Spark 배치는 최소 12 vCPU가 필요하므로 **체험판에서는 실행이 불가능**하다.
  - 크레딧이 바닥나는 순간 체험이 끝난다. 모든 리소스가 멈추고 30일 뒤 삭제되며, 두 번째 체험은 받을 수 없다.
  - **예산 알림은 기본값으로 크레딧을 차감한 금액을 보기 때문에 체험 중에는 울리지 않는다.** `EXCLUDE_ALL_CREDITS`로 바꿔야 울린다.
- **관리형 서비스 비용** (us-central1 기준, ₩1,383/USD)
  - Composer 3 small: 하루 약 ₩2.4만. 일시정지가 안 되고 삭제만 된다. 생성에 25분이 걸리고 그 시간도 과금된다. 기본 이미지가 Airflow 2.11이라 3.3.1을 직접 지정해야 하며, Python은 3.11.8이다.
  - Managed Kafka 최소 구성: 하루 약 ₩0.9만~1.1만. 생성에 20~30분 걸린다.
  - BigQuery: 매달 쿼리 1TiB와 저장 10GiB가 무료라 이 규모에서는 사실상 0원이다.
- **로컬 스택**
  - Airflow 3.3.2와 PySpark 4.2는 Python 3.14를 지원한다. 다만 PySpark를 쓰려면 `pandas<3`을 직접 고정해야 한다.
  - Spark 4.2와 Kafka 4.3은 Java 21에서 돈다.
  - bitnami 이미지는 2025-08에 배포가 중단돼, 이를 쓰는 튜토리얼은 그대로 실행되지 않는다.
  - MinIO 커뮤니티판은 2025-10에 배포를 멈췄고, 2026-04에 저장소가 보관 처리됐으며, 2026-09-11에 Docker Hub 저장소가 삭제됐다. 지금은 Chainguard 재빌드판이나 bitnamilegacy 이미지 정도로만 받을 수 있고, 보안 패치는 더 이상 나오지 않는다.
- **채용 시장**
  - 토스·카카오뱅크 공고는 Spark skew/OOM, Kafka 운영처럼 오픈소스 내부를 깊게 본다.
  - 컬리, 배민, 오늘의집은 관리형(MWAA/Composer/EMR)에서 K8s 위 직접 운영으로 옮겼다.
  - 신입 공고는 전체의 1% 미만이고, SQL과 CS 기초로 거른다.
  - 클라우드 경력은 대개 'AWS/GCP 경험 **또는 그에 준하는 지식**'으로 적혀 있다. IaC(Terraform)는 우대 사항으로 자주 나온다.

## 추천 (Claude)
**B를 추천한다.** 체험판의 가치는 연산 자원이 아니라(노트북이 더 크다) **관리형 서비스를 다뤄 본 경험, 오브젝트 스토리지의 특성, 비용을 기록해 본 경험**에 있다. 그래서 크레딧을 이 세 가지에만 쓰고 나머지는 남긴다. 다 쓰는 게 아니라 제대로 쓰는 게 목표다.
- 리전은 **us-central1**으로 한다.
  - Composer가 서울보다 약 22% 싸다.
  - us-central1 단일 리전 버킷은 Always Free 대상이라, 북미에서 나가는 전송이 월 100GB까지(미국 3개 리전 합산) 무료다. 넘으면 $0.12/GiB다. 증거 자료 정도는 무료 범위 안이다.
  - 배치 작업이라 지연 시간은 상관없다.
- AWS는 지금은 제외한다. GCP 체험이 끝난 뒤 다시 검토한다(아래 '다시 검토할 조건' 참고).

## 결정
로컬 우선 + GCP 체험판 단기 스프린트(후보 B)를 채택한다. GCP 리소스는 us-central1 단일 리전에 만든다(US 멀티 리전이 아님).

## 이유
- 얻는 것: 로컬에서 만든 DAG를 Composer·Dataproc으로 옮겨 7개월 백필을 돌려 보고, GCS로 오브젝트 스토리지 특성을 겪고, 실제 비용 기록을 남긴다. 클라우드 서비스를 다뤄 본 경험을 현금 0원으로 얻는다.
- 포기하는 것:
  - 시간: 주 10시간 중 GCP 세팅·스프린트·정리에 쓰는 만큼 1·2단계를 깊게 팔 시간이 준다.
  - 마감: 12/8까지 끝내야 해서, 2단계가 늦으면 덜 된 상태로 스프린트를 한다.
  - 위험: 크레딧이 바닥나면 체험이 즉시 끝나고 데이터가 삭제된다(부록 A 규칙으로 막는다).
  - 복잡도: 로컬용·클라우드용 설정 두 벌을 관리한다.
- 리전(us-central1): 배치 작업이라 지연 시간이 상관없고, Composer가 서울보다 약 22% 싸며, Cloud Storage Always Free 대상 리전이다. 실제 한국 서비스 데이터였다면 원천 데이터와 같은 리전(asia-northeast3)을 기본으로 했을 것이다. 리전 간 전송 비용과 국외 이전 근거(개인정보 보호법 제28조의8)를 따져야 하기 때문이다.

## 다시 검토할 조건
- 체험 크레딧이나 기간이 끝나면 → 그 뒤로는 A(로컬 전용)로 운영한다. 클라우드 경험을 더 쌓고 싶다면 그때 D(AWS Free plan, Glue와 Athena는 Free plan에서 사용 가능)를 다시 검토한다.
- Docker VM 12GiB에서도 한 단계를 돌리기 벅차면 → 단계별로 compose profile을 분리하고, 그래도 부족하면 OrbStack이나 Colima를 검토한다.
- 체험 계정에서 Composer 생성이 막히면(악용 방지 제한) → VM에 docker-compose로 같은 스택을 올린다(e2-standard-4, 하루 약 ₩4.4천).
- 지연 시간이 중요한 작업(4단계)을 클라우드에서 하게 되면 → 리전을 다시 정한다. 버킷·데이터셋 위치는 바꿀 수 없으니 새로 만들고 복사한다.

---
## 부록 A. GCP 체험판 운용 규칙 (B 채택 시)
1. **'업그레이드' 또는 'Activate' 버튼은 절대 누르지 않는다.** 할당량 증설, GPU, Marketplace 화면에서 이 버튼을 유도하는 경우가 많다. 이 버튼만 안 누르면 과금이 불가능하다.
2. **1일차에 할 일**
   - 예산: `credit_types_treatment=EXCLUDE_ALL_CREDITS`로 설정하고, 기간은 체험 기간 전체로 잡는다. 알림은 ₩10만/₩20만/₩30만에 단다.
   - 결제 데이터 내보내기: BigQuery US 멀티리전으로 보낸다(나중에 포트폴리오용 비용 기록에 쓴다). **단일 리전 원칙의 유일한 예외다.** 멀티리전 데이터셋만 지난달 1일치부터 소급해 채워 주므로, 9월부터 쓴 체험 비용까지 기록에 남는다. 단일 리전은 설정한 날부터만 쌓인다.
   - 스모크 테스트: 체험 계정에서 Composer를 만들 수 있는지 gcloud로 생성 직후 바로 삭제해 확인한다(₩1천 이하). Managed Kafka는 4단계(체험 후)에 쓰므로 확인하지 않는다.
   - 진행 기록 (2026-10-07): gcloud 로그인과 기본 인증(ADC)을 마쳤다. 기존 기본 프로젝트(2026-09-07 생성)를 그대로 쓴다. 예산은 ₩30만, 크레딧 제외, 기간 2026-09-01~12-31, 알림 ₩10만/20만/30만으로 만들었다(Budget API 활성화 포함). 결제 데이터 내보내기와 스모크 테스트는 아직 하지 않았다.
   - 진행 기록 (2026-10-12): 결제 내보내기용 BigQuery 데이터셋(US)을 Terraform으로 만들었다. 콘솔에서 연결하는 일은 사용자 몫으로 남았다. 스모크 테스트는 통과했다. Composer 생성에 21분이 걸렸고, 새벽 삭제 안전장치를 강제로 실행해 실제로 지워지는 것까지 확인했다. 같은 날 스프린트를 마치고 관리형 서비스를 모두 지웠다(ADR 0008).
3. **관리형 서비스는 같은 날 만들고 같은 날 지운다.** 밤을 넘기지 않는다. Terraform의 `enable_*` 플래그로 켜고 끄고, 세션 끝에는 `list` 명령으로 결과가 비어 있는지 확인한다.
   - **안전장치:** Cloud Scheduler 작업이 매일 02:00(KST)에 Composer 환경 DELETE API를 호출한다. Composer는 일시정지도 자동 만료도 없고, 첫 예산 알림(₩10만)은 약 4일 뒤에야 울리기 때문이다. 사람이 잊어도 피해가 하루치(약 ₩2.4만)로 끝난다. Scheduler는 결제 계정당 작업 3개까지 무료다. 스프린트용 Composer를 처음 만들기 전에 설정한다.
4. **Dataproc 클러스터 설정**
   - 단일 노드, 8 vCPU 이하.
   - 부팅 디스크는 100GB로 지정한다(기본값 500GB).
   - `--delete-max-idle`과 `--delete-max-age`를 둘 다 건다. 작업이 걸려 있으면 idle 삭제가 동작하지 않기 때문이다.
5. **GCS 설정**
   - 단일 리전 버킷을 쓰고, 수시로 덮어쓰는 버킷은 soft delete를 0으로 둔다(기본 7일 동안 삭제된 데이터도 과금된다).
   - 노트북에서 `gs://`를 직접 읽지 않는다. 다운로드는 $0.12/GiB씩 과금된다.
6. **데이터 반입:** Storage Transfer Service의 URL 목록 기능(에이전트 없는 방식)으로 data.rees46.com의 7개월치 gz를 GCS에 넣는다. 반입 비용은 0원이다.
7. **종료:** 증거(Spark 이벤트 로그, DAG 실행 기록, 화면 캡처)는 스프린트 당일에 바로 받아 둔다. 결제 CSV는 9주 차에 받고, 12/6까지 `terraform destroy`를 실행한다. 레이크 데이터는 내려받지 않고 로컬에서 다시 만든다.

## 부록 B. 체험 기간 일정 (주 10시간 기준)
순서는 고정이고 날짜는 **늦어도 이때까지**라는 상한선이다. 앞 단계가 끝나면 날짜를 기다리지 않고 바로 다음으로 넘어간다. 스프린트도 2단계가 끝나는 즉시 한다. 일찍 끝낼수록 체험 종료일("12/8 전후"로 정확하지 않음)과의 여유가 커진다.

| 늦어도 | 로컬 | GCP | 예상 크레딧 |
|---|---|---|---|
| 10/11 | Docker와 uv 세팅, 10월 데이터 받기 | 1일차 세팅, GCS 반입, 스모크 테스트 | ~₩5천 |
| 11/1 | 1단계: 멱등 적재, 파티셔닝, 중복 제거 | — | ~₩2천 (저장) |
| 11/22 | 2단계: Airflow DAG, 백필, 재시도, 품질 검사 | BigQuery 적재 실습, 2단계 마무리 무렵 Terraform 작성(Composer·Dataproc `enable_*` 플래그, Scheduler 안전장치) | ~₩0 |
| 11/29 | — | **스프린트**: 같은 DAG를 Composer에 올려 7개월 백필(Dataproc 단일 노드). 최대 2회, 회당 하루 이내 | ~₩6만~10만 |
| 12/6 | README에 비용 기록 정리 | 결제 CSV 수집, destroy | — |
| (선택) 여유가 있으면 | — | Dataproc 다중 노드(마스터 1 + 워커 3, 각 2 vCPU)로 노드 사이 셔플 관찰. 노트북으로는 못 보는 장면이다 | ~₩1만 |

2단계가 11/22까지 끝나지 않으면, 그때까지 된 것만 가지고 스프린트를 한다. 합계 상한은 ₩33만(80%)이다.

## 부록 C. 로컬 스택 기본값 (결정 후 환경 구성 때 적용)
- **Docker Desktop:** 메모리 12GiB, 스왑 2GB, CPU 8개. 단계별 compose profile로 한 번에 한 단계만 띄운다.
  - 예상 피크: 한 단계씩이면 8~10GB, 전부 동시에 띄우면 14~19GB.
- **Python:** uv로 프로젝트 전용 Python 3.11을 고정한다(Composer와 같은 버전). 체험이 끝난 뒤 올릴지는 그때 정한다.
- **Airflow:** 3.3.x에 LocalExecutor, `parallelism=4`로 둔다. 체험 중(~12/8)에는 이미지를 Composer와 같은 `3.3.1-python3.11`로 맞춘다.
- **Spark:** pip로 설치한 pyspark를 local 모드로 쓰고 `pandas<3`을 고정한다. standalone 클러스터는 나중에 선택 사항으로 남긴다. 이미지는 bitnami 대신 apache/spark를 쓴다.
- **Kafka:** apache/kafka 4.3.x 단일 노드 KRaft에 힙 512m. `retention.bytes`는 파티션별로 적용된다는 점에 주의한다.
- **오브젝트 스토리지:** 1·2단계는 `file://`로 한다. GCS 특성은 스프린트(체험 중)에서 겪는다. 3단계는 체험이 끝난 뒤라, S3 특성을 배울 때는 SeaweedFS를 쓴다. MinIO는 쓰지 않는다.
  - **정정 (2026-10-07):** 2단계가 일찍 끝나서, 사용자가 3단계를 스프린트보다 먼저 하기로 정했다. "3단계는 체험이 끝난 뒤"라는 전제는 더 이상 맞지 않는다. 그래도 사용자는 3단계에서 SeaweedFS와 Iceberg를 도입하기로 했다(ADR 0006). 스프린트에는 1·2단계 DAG(`events_daily`)만 올린다. lake DAG는 Composer에서 돌 수 없는 compose 전용 의존성을 갖기 때문이다(ADR 0007 결정 5). 부록 B의 순서("스프린트도 2단계가 끝나는 즉시")는 이 결정으로 바뀌었다. 체험 종료일(12월 초)은 그대로라 스프린트 날짜를 따로 잡아야 한다.
- **데이터:** `curl -C - -O`로 data.rees46.com에서 받는다(로그인 없음, 끊겨도 이어받기 가능). 라이선스상 재배포가 안 되므로 git과 공개 버킷에 올리지 않고, README에는 출처만 적는다.

## 부록 D. 단계 1~3에서 직접 확인할 것 (조사 중 발견, 정답 숫자는 일부러 뺐다)
- 월마다 중복률이 같을까? 품질 검사 임계값을 10월 하나로 고정해도 될까?
- 이벤트 유형 설명에 있는 값이 실제 데이터에 다 등장할까?
- 같은 product_id의 category_id/category_code가 월마다 그대로일까? (힌트: 3단계 SCD2의 재료가 된다)
- Spark는 `.csv.gz` 파일 하나를 몇 개의 태스크로 읽을까? 그 이유는?

## 출처 (2026-10-03에 열어서 확인)
- 데이터: [Kaggle 데이터셋](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), [data.rees46.com 예시 파일](https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz)
- GCP 체험판: [Free cloud features](https://docs.cloud.google.com/free/docs/free-cloud-features), [Free Trial 약관](https://cloud.google.com/terms/free-trial), [Serverless Spark 할당량](https://docs.cloud.google.com/managed-spark/quotas-serverless), [예산 설정](https://docs.cloud.google.com/billing/docs/how-to/budgets)
- GCP 가격: [Composer](https://cloud.google.com/composer/pricing), [Managed Kafka](https://cloud.google.com/managed-service-for-apache-kafka/pricing), [BigQuery](https://cloud.google.com/bigquery/pricing), [Storage](https://cloud.google.com/storage/pricing), [Composer 버전](https://docs.cloud.google.com/composer/docs/composer-versions)
- 로컬 스택: [apache-airflow PyPI](https://pypi.org/project/apache-airflow/), [pyspark PyPI](https://pypi.org/pypi/pyspark/json), [Spark 클라우드 연동(S3 커미터)](https://spark.apache.org/docs/latest/cloud-integration.html), [Kafka Java 버전](https://kafka.apache.org/43/operations/java-version/)
- 오브젝트 스토리지: [minio/minio (보관됨)](https://github.com/minio/minio), [SeaweedFS](https://github.com/seaweedfs/seaweedfs), [Garage](https://garagehq.deuxfleurs.fr/)
- AWS: [Free tier plans](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html), [신규 가입 시 지원 서비스](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html)
- 채용·기술 블로그: [배민 Data on EKS](https://aws.amazon.com/ko/blogs/tech/woowa-brothers-amazon-data-on-eks-data-platform), [오늘의집 Spark on K8s](https://www.bucketplace.com/post/2025-05-23-%EC%98%A4%EB%8A%98%EC%9D%98%EC%A7%91-spark-on-kubernetes-%EB%8F%84%EC%9E%85-%EB%B0%8F-%EA%B0%9C%EC%84%A0-%EC%97%AC%EC%A0%95/), [컬리 Airflow](https://helloworld.kurly.com/blog/airflow-1/), [쏘카 Airflow](https://tech.socar.kr/data/2022/11/09/advanced-airflow-for-databiz), [당근 데이터 직무](https://careers.daangn.com/jobs/role/4300801003/)
