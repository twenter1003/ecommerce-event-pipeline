# 0008. GCP 스프린트: 같은 DAG를 Composer + Dataproc에 올린다

- 날짜: 2026-10-12
- 단계: 1·2단계의 클라우드 스프린트
- 상태: 채택 (2026-10-12 스프린트 완료. 남은 일: 결제 기록, 12/6까지 `terraform destroy`)

## 배경
이 ADR은 앞선 세 결정을 구체화한다.
- ADR 0001(후보 B, 부록 A의 체험판 운용 규칙)
- ADR 0004 결정 1: "`spark_job()` 하나만 Dataproc 연산자로 바꾼다"
- ADR 0007 결정 5: 스프린트에는 `events_daily`만 올린다

3단계가 끝나 사용자가 스프린트를 다음 순서로 골랐다(2026-10-12). 체험은 12/6~12/8 무렵 끝난다.

설계 전에 확인한 사실(2026-10-12):

| 사실 | 내용 |
|---|---|
| Composer 이미지 | `composer-3-airflow-3.3.1-build.3`. Airflow 3.3.1, Python 3.11.8, google provider 22.4.0, standard 1.18.0이 들어 있다. **Slack provider는 없다.** 로컬 이미지는 google 22.3.0, slack 9.10.2, standard 1.17.0이다 |
| Dataproc 이미지 | 3.0 계열 이름은 `3.0-debian13`, `3.0-ubuntu24`, `3.0-rocky9`다. Spark 4.1.2로, 로컬과 같다(ADR 0002) |
| 할당량 | `CPUS_ALL_REGIONS` 32, us-central1의 N2 200이다. 체험 약관의 '동시 8코어'는 할당량에 보이지 않지만 그대로 따른다 |
| 기본 네트워크 | 기본 VPC의 us-central1 서브넷은 Private Google Access가 꺼져 있다. 내부 IP만 쓰는 Dataproc 노드는 이 서브넷에서 GCS에 닿지 못한다 |
| 원천 서버 | data.rees46.com은 `Content-Length`, 이어받기(206), `ETag`를 지원하고 robots.txt가 없다. 그래서 Storage Transfer(STS)의 URL 목록 반입 조건을 맞춘다 |

## 결정 1. DAG 코드는 하나, 갈리는 곳은 환경 변수 둘
| 후보 | 장점 | 단점 |
|---|---|---|
| **A. 환경 변수로 분기 (채택)** | DAG 파일이 하나다. 로컬 컨테이너에서 Composer 경로까지 미리 검사할 수 있다(환경 변수만 흉내) | 공용 코드에 분기가 두 곳 생긴다 |
| B. Composer용 DAG 파일을 따로 둠 | 파일마다 단순하다 | 같은 로직이 두 벌이라 언젠가 어긋난다 |
| C. KubernetesPodOperator로 지금 컨테이너를 그대로 실행 | 로컬과 실행 환경이 같다 | Spark가 Composer 안에서 돌아 Dataproc을 배우지 못한다. 클라우드로 옮기는 의미가 줄어든다 |

- `EVENTS_LANDING`이 `gs://`로 시작하면, landing 도착을 `GCSHook` 목록으로 확인한다.
- `DATAPROC_CLUSTER`가 있으면 `spark_job()`이 `DataprocSubmitJobOperator`를 만든다.
  - main 파일은 `code/pipeline/<모듈>.py`이고, import용으로 `pipeline.zip`을 함께 낸다.
  - 인자는 `{{ ds }}`처럼 공백이 든 템플릿을 하나로 남기도록 정규식으로 나눈다.
- Slack import를 알림 함수 안으로 옮겼다. Composer 이미지에 provider가 없어, 맨 위에 두면 DAG 파싱이 깨진다. provider가 없으면 알림을 로그로 남긴다.
- 확인: 로컬 컨테이너에서 두 모드로 DagBag을 읽었다.
  - 로컬 모드에서는 `load`·`quality`가 `BashOperator`다.
  - 흉내 모드에서는 `DataprocSubmitJobOperator`이고, 인자가 `['--start', '2019-10-01', '--landing', 'gs://…', …]`로 렌더링된다.
  - 두 모드 모두 import 오류가 없다.

## 결정 2. 원본 반입: Storage Transfer URL 목록 → Dataproc에서 일별 분할
| 후보 | 장점 | 단점 |
|---|---|---|
| **A. STS URL 목록 (채택)** | Google 쪽에서 받아 노트북 대역폭을 쓰지 않는다. 0원이다. 목록에 크기와 MD5를 넣으면 STS가 원본과 대조한다 | 일별로 나누는 잡이 하나 더 필요하다. `split_raw`를 그대로 Dataproc에 낸다 |
| B. 로컬 landing(17GB)을 업로드 | 분할이 필요 없다 | 노트북 업로드 시간이 든다. 클라우드 반입을 겪지 못한다 |

- URL 목록에는 로컬 `data/raw`에서 잰 바이트 수와 base64 MD5를 넣었다.
- 목록 파일은 버킷 안에 두고, STS 서비스 에이전트에만 읽기 권한을 준다(공개하지 않는다).

## 결정 3. Dataproc: Terraform 스위치로 켜고 끄는 단일 노드, 수명 제한 두 개
- 이미지 `3.0-debian13`, `n2-standard-8` 단일 노드, 부팅 디스크 100GB(기본값 500GB)를 쓴다.
- 잡 하나가 노드를 다 차지하지 않게 고정한다. 동적 할당을 끄고 실행기 1개 × 2코어 × 4g로 둔다. `spark` 풀은 3이다.
- 수명 제한을 둘 다 건다: 잡 없이 30분이 지나면 삭제하고, 만든 뒤 8시간이 지나면 잡이 있어도 삭제한다.
  - Terraform의 `auto_delete_time`은 시각만 받는다. 그래서 만들 때 `plantimestamp() + 8h`로 정하고, 이후 변경은 무시한다.
- 네트워크: 스프린트 전용 VPC를 만들고, 서브넷에 Private Google Access를 켠다. 노드는 내부 IP만 쓰고, SSH 규칙은 두지 않는다. Spark와 YARN 화면은 콘솔의 Component Gateway로 본다.
- 이벤트 로그는 `gs://<버킷>/eventlogs`에 압축과 분할 없이 쓴다. 클러스터가 지워져도 남고, 로컬 `bench/stages.py`로 읽는다.

| 기각한 후보 | 이유 |
|---|---|
| DAG가 run마다 클러스터를 만들고 지움 | run 213개 × 생성 약 90초. 시간과 돈이 생성에 들어간다 |
| Serverless Spark | 체험판에서 실행할 수 없다(최소 12 vCPU, ADR 0001) |
| 기본 VPC 서브넷에 PGA 켜기 | 기본 네트워크를 바꾸게 되고, `terraform destroy`로 되돌릴 수 없다 |
| 노드에 외부 IP | 노드가 인터넷에 노출된다. 기본 VPC에는 SSH를 전 세계에 여는 규칙이 있다 |

## 결정 4. 안전장치와 권한
- Cloud Scheduler가 매일 02:00(KST)에 Composer 환경 DELETE API를 부른다. 이 계정은 삭제 전용 사용자 지정 역할(`composer.environments.delete`·`get`, `composer.operations.get`)만 가진다.
- Composer 계정에는 `composer.worker`와 Dataproc 잡 제출용 `dataproc.editor`를 준다. Dataproc 노드 계정에는 `dataproc.worker`를 준다. 사람 계정으로 돌지 않는다.
- Composer 3도 서비스 에이전트에 환경 계정의 `composer.ServiceAgentV2Ext` 역할이 필요하다.
- 스모크 테스트에서 안전장치를 실제로 실행해, Composer가 지워지는지까지 확인한다.

## 결정 5. 상태 파일과 비밀
- Terraform 상태는 로컬(`infra/terraform.tfstate`)에 두고 git에서 뺀다. 혼자 쓰는 단기 인프라라 원격 상태(GCS 백엔드)를 두지 않는다.
- 프로젝트 id는 `infra/terraform.tfvars`(git 제외)에만 둔다. 저장소가 공개될 수 있다(ADR 0001 부록 A 진행 기록).

## 스프린트 순서
1. 상시 자원: `terraform -chdir=infra apply`
2. 당일 아침: `terraform -chdir=infra apply -var enable_composer=true -var enable_dataproc=true` → `infra/deploy.sh`
3. 일별 분할: `infra/split.sh`가 `split_raw`를 Dataproc에 달마다 하나씩, 7개를 한꺼번에 낸다
4. 백필: Composer에서 `events_daily`를 2019-10-01~2020-04-30로 백필한다(동시 3)
5. 증거: 이벤트 로그, run 기록, 화면, 비용
6. 같은 날 끝: `terraform -chdir=infra apply`로 관리형 서비스를 지운다. `gcloud composer environments list`와 `gcloud dataproc clusters list`가 비었는지 확인한다

## 준비와 스모크 테스트 결과 (2026-10-12)
| 항목 | 결과 |
|---|---|
| 상시 자원 | `terraform apply`로 19개를 만들었다 |
| 원본 반입(STS) | 7개 파일, 17.67GB를 24분 52초에 받았다(평균 약 11.8MB/s). 7개 모두 크기와 MD5가 로컬 원본과 같다. 객체 이름은 URL의 호스트와 경로를 그대로 따라 `raw/data.rees46.com/datasets/marketplace/<월>.csv.gz`다 |
| Composer 생성 | 21분 12초가 걸렸다. 체험 계정에서도 막히지 않는다 |
| DAG 배포 | `infra/deploy.sh`가 28초 걸렸다. `events_daily`가 import 오류 없이, 일시정지가 풀린 채 등록됐다. lake DAG는 올라가지 않았고, `spark` 풀은 3칸이다 |
| 권한 | `composer.worker`와 `dataproc.worker` 모두 `storage.objects.get`·`list`·`create`를 포함한다. landing 센서와 Spark 잡이 버킷을 읽고 쓸 수 있다 |
| 안전장치 | Scheduler 작업을 강제로 실행하자 환경이 RUNNING에서 DELETING으로 바뀌었고, 7분 23초 뒤 지워졌다. 삭제 전용 역할 세 권한으로 충분하다 |
| 정리 | 삭제 뒤 `terraform apply`(스위치 끔)의 결과는 변경 0이다. Composer와 Dataproc 목록이 모두 비었다 |
| 비용 | Composer가 산 시간은 약 32분이다. 금액은 결제 내보내기를 연결한 뒤 기록한다 |

## 스프린트 결과 (2026-10-12)
**YARN 코어 굶주림 (분할 첫 시도)**
- 처음 설정은 실행기 2코어·4g였고, 분할 잡 7개를 한꺼번에(`--async`) 냈다.
- 10월은 11분, 11월은 18분 만에 끝났다. 나머지 5개는 27분 동안 "Initial job has not accepted any resources" 경고만 남겼다.
- 원인: Dataproc의 YARN은 `DominantResourceCalculator`라서 메모리뿐 아니라 코어도 센다(노드 8코어).
  - Spark 앱 하나는 관리자(AM) 1코어와 실행기 2코어, 합쳐 3코어가 있어야 일할 수 있다.
  - 앱 7개의 AM이 먼저 7코어를 잡자, 남은 1코어로는 2코어짜리 실행기를 띄울 수 없었다. 코어를 쥔 AM들이 서로를 기다렸다.
- 고친 것:
  - 실행기를 1코어·3g로, 드라이버를 2g로 줄였다(Dataproc 기본값은 8g). 앱 하나가 2코어라 8코어에 4개가 꼭 맞는다. 하루치 잡은 gzip 하나를 한 코어로 읽는 시간이 대부분이라 실행기 1코어로 충분하다(ADR 0005의 병목).
  - `dataproc:dataproc.scheduler.max-concurrent-jobs=4`를 줬다. 넘치는 잡은 Dataproc이 QUEUED로 줄 세운다. AM이 코어를 쥐고 기다리는 일이 없다.
  - Composer `spark` 풀도 4칸으로 맞췄다.
  - 클러스터 설정은 만들 때만 정할 수 있어서 다시 만들었다(삭제 53초, 생성 3분 19초).
- 결과: 4개가 YARN에서 돌고, 다섯 번째는 "Awaiting execution"으로 기다린다. 경고는 0건이다.
- 노트북의 `local[*]`에는 없던 장면이다. 자원 관리자가 끼면 "동시에 몇 개를 낼까"가 메모리만이 아니라 코어 단위 계산이 된다.

**Dataproc 3.0 기본 커밋 방식이 데이터를 조용히 잃었다 (분할 두 번째 시도)**
- 재분할 다섯 잡이 모두 성공(DONE)했다. 그런데 landing을 세어 보니 날짜가 186개뿐이었다.
  - 2020년 1월은 31일 중 4일만 있었다.
  - 12월은 5일에 csv.gz가 2개씩 있었다. 한 번 잘린 첫 실행의 파일과, 두 번째 실행의 파일이다.
- 1월 잡의 드라이버 로그: `ManifestCommitter … destination gs://…/landing/events`, 이어서 `Committing job with file count: 4`.
  - 31개 날짜를 썼는데 커밋된 파일은 4개였다. 오류는 없었다.
- 원인: Dataproc 3.0은 Spark 쓰기를 클라우드용 경로로 바꿔 둔다. 작은 PySpark 잡으로 실제 설정을 출력해 확인했다.
  - `spark.sql.sources.commitProtocolClass` = `PathOutputCommitProtocol`
  - `mapreduce.outputcommitter.factory.scheme.gs` = `ManifestCommitterFactory`
  - FileOutputCommitter 알고리즘 2
- 이 조합에서 생긴 일은 두 가지다.
  1. **같은 폴더에 동시에 쓰면 데이터가 사라진다.** 작업 공간이 Spark의 잡별 임시 폴더가 아니라 루트의 `_temporary`다. 2월 잡(03:24:40 종료)이 커밋 뒤 정리하면서 이 폴더를 지운 것으로 보인다. 1분 뒤 커밋한 1월 잡에는 그 뒤에 쓴 태스크 4개의 결과만 남았다.
  2. **날짜 덮어쓰기(dynamic)가 덧붙이기가 됐다.** 12월 5일에 옛 파일이 그대로 남은 채 새 파일이 더해졌다.
- 공식 문서도 같은 말을 한다. Dataproc 3.0의 Hadoop은 3.5.0이고(Spark 4.1.2), 그 버전의 Manifest Committer 문서 "Support for concurrent jobs to the same directory" 절은 이렇게 적는다.
  - 같은 디렉터리에 여러 잡을 동시에 쓰는 것은 "NOT BEEN TESTED"다.
  - 하려면 `_temporary` 정리를 꺼야 한다(`mapreduce.fileoutputcommitter.cleanup.skipped=true`).
  - Spark의 동적 파티션 덮어쓰기를 쓰면 "data may be lost"다.
  - 즉 이 커미터는 "한 폴더에 한 번에 한 잡"을 전제로 한다. 우리 사용 방식(날짜별 잡 여러 개가 같은 루트에 동시에 덮어씀)과 맞지 않는다.
- 두 가지 모두 이 파이프라인의 핵심 약속을 깬다. 하루치 적재는 멱등이어야 하고, 동시에 도는 날짜끼리 서로 독립이어야 한다(ADR 0003·0004). 이대로 백필했다면 날짜가 조용히 비거나 두 배가 됐다. 그나마 품질 검사(landing 고유 행 수 = warehouse 행 수)가 일부를 실패로 드러냈을 것이다.
- 고친 것: 클러스터 속성으로 Spark 기본 커밋 방식을 되돌렸다.
  - `SQLHadoopMapReduceCommitProtocol`, `ParquetOutputCommitter`, gs용 `FileOutputCommitterFactory`
  - 기본 방식은 잡마다 `.spark-staging-<uuid>`에 쓰고, 커밋 때 그 날짜 폴더만 지우고 옮긴다. 로컬과 같은 방식이다.
  - 대가: GCS의 rename은 복사 후 삭제라서 원자적이지 않다. 커밋 도중 죽으면 그 날짜가 비거나 일부만 남을 수 있다. 이는 ADR 0003 한계 1과 같고, 다시 돌리면 고쳐진다.
- 같은 날 Terraform이 apply마다 클러스터를 다시 만드는 문제도 고쳤다. Dataproc이 `service_account_scopes`에 기본 범위를 덧붙여 저장해서 생긴 차이였다. `ignore_changes`에 넣었다.

**7개월 백필 (Composer + Dataproc 단일 노드, 동시 4)**
| 항목 | 결과 |
|---|---|
| run | 213개 모두 성공. 태스크 639개 중 재시도 0 |
| 걸린 시간 | 12,617초(3시간 30분). 로컬은 동시 3에서 2,516초였다(ADR 0005 ④). 약 5배 느리다 |
| 태스크 시간 | load와 quality 모두 중앙값 111.6초, p90 약 132초. 로컬은 load 10~15초, quality 약 4.5초였다. 센서는 중앙값 0.5초다 |
| 결과 대조 | 날짜별 행 수를 Dataproc과 로컬에서 같은 스크립트로 셌다. **213일 모두 같고, 합계 410,325,314 = 로컬 410,325,314**다 |
| 기록 | run별 시간은 `bench/sprint_events_daily.csv`에 있다(프로젝트 id 없음) |

느린 까닭은 이벤트 로그(11/06 load 앱)로 나눠 볼 수 있다. 앱 하나가 74.8초다.
- 단계 안 53.0초: gzip 하나를 실행기 1코어로 두 번 읽고(20.8초 + 14.1초), 중복을 지우고 쓴다(17.7초). 로컬에서는 같은 일을 10코어 노트북의 빠른 코어로 한다.
- 단계 밖 21.8초: 드라이버 혼자 GCS 목록·메타데이터를 다루고 실행기를 기다린다. 로컬에서는 약 3초다.
- 태스크 111.6초와 앱 74.8초의 차이 약 37초: Dataproc 잡 제출, YARN 앱과 컨테이너 시작, 연산자의 상태 확인 간격이다.
- 즉 하루치가 작은 이 데이터에서는 클러스터의 고정 비용이 계산보다 크다. ADR 0001의 "이 규모에 Spark 클러스터는 과하다"를 숫자로 확인한 셈이다. 그래도 같은 코드가 손대지 않고 클라우드에서 돌았고, 결과가 로컬과 행 단위까지 같다.

**정리와 비용**
- 18:02에 `terraform apply`(스위치 끔)로 Composer(7분 43초)와 Dataproc(55초)을 지웠다. 목록이 모두 비었다.
- 남은 상시 자원은 버킷(원본·landing·warehouse 약 42GB, 월 약 ₩1.2천), Scheduler 작업(무료 범위), 빈 BigQuery 데이터셋, VPC다.
- 비용 추정(단가 × 켜 둔 시간): Composer 약 7.2시간(스모크 테스트 0.5시간 포함) ≈ ₩7.2천, Dataproc 약 6.5시간 ≈ ₩4.4천, 합계 약 ₩1.16만이다. 사용자가 승인한 범위(약 ₩1.06만, ₩1.2만을 넘으면 다시 묻기) 안이다.
- 실제 금액은 결제 내보내기를 콘솔에서 연결한 뒤 BigQuery로 확인해 적는다. 연결은 아직이다.

## 다시 검토할 조건
- 체험 계정에서 Composer 생성이 막히면 → ADR 0001의 대안(VM에 docker compose)으로 간다.
- Dataproc 노드에서 잡 3개가 메모리나 코어를 다투면 → 풀을 2로 줄이거나, 실행기 코어를 1로 낮춘다.
