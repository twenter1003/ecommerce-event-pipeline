# 07. GCP 스프린트: Terraform, Composer + Dataproc, 오브젝트 스토리지의 커밋

- 대상 코드: `infra/main.tf`, `infra/deploy.sh`, `infra/split.sh`, `dags/common/airflow_utils.py`
- 설계 기록: [ADR 0008](../decisions/0008-gcp-sprint.md) 전체, [ADR 0001](../decisions/0001-execution-environment.md) 부록 A
- 상태: 미답변

## 다룬 개념
- **IaC(Terraform)**: 인프라를 코드로 적고, `plan`으로 바뀔 것을 미리 본 뒤 `apply`로 맞춘다. 실제 상태는 상태 파일(`terraform.tfstate`)에 남는다. `count` 스위치로 비싼 자원을 그날만 켠다.
- **관리형 오케스트레이션과 클러스터**: Composer는 Airflow를, Dataproc은 Spark를 대신 운영해 준다. 같은 DAG 코드가 환경 변수에 따라 로컬 모드와 Dataproc 모드로 갈린다.
- **YARN의 자원 계산**: 클러스터 자원 관리자다. Dataproc은 메모리와 코어를 함께 센다(`DominantResourceCalculator`). Spark 앱 하나는 관리자(AM) 컨테이너 1코어와 실행기 코어를 쓴다.
- **오브젝트 스토리지의 커밋**: GCS에는 진짜 "폴더 이름 바꾸기"가 없다(복사 후 삭제). 그래서 커밋 방식(커미터)이 여럿이고, 각각 전제가 다르다.
- **비용 안전장치**: 삭제 권한만 가진 계정이 매일 새벽 Composer를 지운다. Dataproc은 수명 제한 두 개(30분 유휴, 8시간)를 건다. 예산 알림은 크레딧을 빼고 계산한다.

## 질문 (답은 블로그에)
1. 상시 자원과 "그날만 켜는" 자원을 `count` 스위치로 나눴다.
   - Composer는 일시정지가 안 된다. 그런데 사람이 잊어도 피해가 하루치로 끝나게 한 장치는 무엇인가?
   - 스모크 테스트에서 그 장치를 일부러 실행해 본 이유는?
   - 그 계정에 삭제 권한 세 개만 준 이유(최소 권한)는?
2. (면접형) 분할 잡 7개를 한꺼번에 냈더니 27분 동안 "Initial job has not accepted any resources"만 찍혔다.
   - YARN이 코어를 세는 방식으로 원인을 설명하라.
   - 어떤 두 가지로 고쳤나? 각각 무엇을 바꿨나?
   - 노트북의 `local[*]`에서는 왜 이런 일이 없었나?
3. (면접형) 잡 5개가 모두 성공(DONE)했는데, 1월은 31일 중 27일이 사라졌고 12월은 옛 파일과 새 파일이 겹쳤다.
   - Dataproc 3.0 기본 커밋 방식의 무엇이 "같은 폴더에 여러 잡이 동시에 덮어쓰기"와 부딪혔나? 공식 문서의 어떤 문장이 이것을 경고하나?
   - Spark 기본 방식(`.spark-staging-<uuid>`)은 왜 이 패턴에 안전한가? 대신 무엇을 포기했나?
   - "잡이 성공했다"는 표시만 믿었다면 언제, 어떻게 알게 됐을까?
4. 같은 DAG 파일이 로컬과 Composer에서 돈다.
   - 갈리는 두 곳은 어디인가?
   - Slack import를 함수 안으로 옮긴 이유는?
   - Composer를 만들기 전에, 로컬 컨테이너에서 Composer 경로를 어떻게 미리 검사했나?
5. Terraform이 `apply`할 때마다 Dataproc 클러스터를 새로 만들었다.
   - 원인은 무엇이고 어떻게 고쳤나?
   - `ignore_changes`를 남용하면 어떤 위험이 있나?
6. (면접형) 같은 213일을 노트북(K=3, 42분)과 클라우드(K=4, 약 3시간 반)에서 돌렸다.
   - 클라우드가 느린 이유를 이벤트 로그 숫자로 설명하라. 단계 안과 밖, 실행기 1코어, 잡마다 드는 YARN 시작 비용을 쓴다.
   - 결과가 로컬과 같다는 것은 어떻게 증명했나?
   - 그래도 회사가 관리형 서비스를 쓰는 이유는 무엇인가?

## 직접 돌려 볼 실험
1. **Terraform이 무엇을 만들려 하는지 읽기** (비용 없음, plan만 한다)
   ```bash
   terraform -chdir=infra plan -var enable_dataproc=true
   ```
   볼 것: 만들 자원이 1개(Dataproc 클러스터)인지 본다. `override_properties`의 설정이 각각 어떤 문제를 막는지 ADR 0008과 짝지어 본다. 코어 굶주림을 막는 설정과 데이터 유실을 막는 설정이 있다. `apply`는 하지 않는다.
2. **로컬에서 Composer 경로 흉내 내기** (로컬 스택이 떠 있을 때)
   ```bash
   docker compose exec -T -e DATAPROC_CLUSTER=study -e DATAPROC_REGION=us-central1 -e PIPELINE_CODE=gs://b/code -e EVENTS_LANDING=gs://b/landing/events -e EVENTS_WAREHOUSE=gs://b/warehouse/events airflow-scheduler python -c "
   import sys; sys.path.insert(0, '/opt/airflow/dags')
   from airflow.dag_processing.dagbag import DagBag
   d = DagBag(dag_folder='/opt/airflow/dags').dags['events_daily']
   op = d.get_task('load'); print(type(op).__name__, op.render_template(op.job, {'ds': '2019-10-01'})['pyspark_job']['args'])"
   ```
   볼 것: 연산자가 `DataprocSubmitJobOperator`이고, 인자에 `2019-10-01`이 한 덩어리로 들어간다. 그다음 `-e` 옵션을 모두 빼고 같은 명령을 돌려, 연산자가 무엇으로 바뀌는지 본다. `.job` 속성이 없어 오류가 나는 것이 정상이다.

## 학습 자료
링크는 2026-10-12에 열리는 것을 확인했다.
- [Hadoop 3.5.0 — Manifest Committer](https://hadoop.apache.org/docs/r3.5.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/manifest_committer.html): 'Support for concurrent jobs to the same directory' 절만 본다. Dataproc 3.0의 Hadoop 버전이다.
  > 요약: 같은 디렉터리에 여러 잡을 동시에 쓰는 것은 시험하지 않았다. 하려면 `_temporary` 정리를 꺼야 하고(`mapreduce.fileoutputcommitter.cleanup.skipped=true`), 파일 이름이 겹치지 않아야 한다. Spark의 동적 파티션 덮어쓰기를 쓰면 데이터를 잃을 수 있다.
- [Spark 4.1.2 — Integration with Cloud Infrastructures](https://spark.apache.org/docs/4.1.2/cloud-integration.html): 'Committing work into cloud storage safely and fast'와 'Cloud Committers and `INSERT OVERWRITE TABLE`' 두 절만 본다.
  > 요약: Spark의 기본 커밋은 rename에 기대는데, 오브젝트 스토리지에서 rename은 느리고 실패하면 상태를 알 수 없게 만든다. GCS는 디렉터리 rename이 원자적이지 않아 Manifest Committer를 권한다. 동적 파티션 덮어쓰기는 커미터가 지원하는지 확인한 뒤에만 허용된다.
- [Terraform — lifecycle meta-argument](https://developer.hashicorp.com/terraform/language/meta-arguments/lifecycle): `ignore_changes` 절만 본다. 문서 버전 v1.16.x가 설치한 1.16.5와 맞다.
  > 요약: `ignore_changes`에 적은 속성은 처음 만들 때만 쓰이고, 그 뒤 계획에서는 차이를 무시한다. 다른 프로세스가 바꾸는 값(예: 클라우드가 덧붙이는 기본값)에 쓴다. `all`을 쓰면 그 자원은 갱신을 아예 제안하지 않는다.
