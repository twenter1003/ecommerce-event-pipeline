# GCP 스프린트 인프라. 설계는 ADR 0008, 체험판 운용 규칙은 ADR 0001 부록 A.
#   terraform -chdir=infra apply                                   상시 자원(버킷, 네트워크, 계정, 안전장치, 반입)
#   terraform -chdir=infra apply -var enable_composer=true -var enable_dataproc=true   스프린트 당일에만
#   terraform -chdir=infra apply                                   같은 날 끝: 관리형 서비스를 지운다
# 프로젝트 id는 저장소에 두지 않는다(공개 대비). infra/terraform.tfvars(git 제외)에 project = "..."를 적는다.
terraform {
  required_version = ">= 1.16"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 8.6" }
  }
}

variable "project" { type = string }
variable "region" { default = "us-central1" } # 단일 리전(ADR 0001)
variable "enable_composer" { default = false }
variable "enable_dataproc" { default = false }

provider "google" {
  project = var.project
  region  = var.region
}

data "google_project" "this" {}

locals {
  bucket = "${var.project}-study"
  # 원본 7개월: 이름 → [바이트, base64 MD5]. 로컬 data/raw에서 잰 값이라 STS가 받은 파일을 원본과 대조한다.
  raw = {
    "2019-Oct" = ["1741928540", "kF2uIUAwmFS0TjFVq9xhVQ=="]
    "2019-Nov" = ["2890421023", "ubBeUw0zO0SPu5lSB2j2/A=="]
    "2019-Dec" = ["2951549375", "jOy73jxNm/OSqCNiMEAGIg=="]
    "2020-Jan" = ["2390063923", "iC5IdVBm/a+nKjF8r9wkUA=="]
    "2020-Feb" = ["2347087112", "HgbB80T+lxZizRMoTXbHkg=="]
    "2020-Mar" = ["2417362205", "GfQcBf8QrJbEGhh1lMD+tg=="]
    "2020-Apr" = ["2929933389", "KGbbvKq+qhphJP6l7Jj3fw=="]
  }
}

# ── 저장소 ───────────────────────────────────────────────
resource "google_storage_bucket" "study" {
  name                        = local.bucket
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true # 끝날 때 통째로 지운다. 원본은 rees46에 있고 나머지는 다시 만들 수 있다
  soft_delete_policy {
    retention_duration_seconds = 0 # 날짜를 덮어쓰는 버킷이라, 지운 데이터를 7일 동안 과금하지 않게 끈다
  }
}

# 결제 데이터 내보내기용. 단일 리전 원칙의 유일한 예외: US 멀티 리전만 지난달 1일치부터 소급해 채운다(ADR 0001 부록 A 2).
# 연결(어느 데이터셋으로 보낼지)은 콘솔에서만 할 수 있다.
resource "google_bigquery_dataset" "billing" {
  dataset_id                 = "billing_export"
  location                   = "US"
  delete_contents_on_destroy = true # destroy 전에 결제 CSV를 받아 둔다(ADR 0001 부록 A 7)
}

# ── 원본 반입: rees46 → gs://<버킷>/raw/ (한 번, Google 쪽에서 받아 비용 0원) ──
resource "google_storage_bucket_object" "url_list" {
  bucket  = google_storage_bucket.study.name
  name    = "meta/rees46-urls.tsv"
  content = "TsvHttpData-1.0\n${join("", [for m, v in local.raw : "https://data.rees46.com/datasets/marketplace/${m}.csv.gz\t${v[0]}\t${v[1]}\n"])}"
}

data "google_storage_transfer_project_service_account" "sts" {}

resource "google_storage_bucket_iam_member" "sts" {
  for_each = toset(["roles/storage.legacyBucketWriter", "roles/storage.objectViewer"]) # 목록 읽기 + raw/ 쓰기
  bucket   = google_storage_bucket.study.name
  role     = each.key
  member   = data.google_storage_transfer_project_service_account.sts.member
}

resource "google_storage_transfer_job" "rees46" {
  description = "rees46 7 months raw import (once)"
  transfer_spec {
    http_data_source { list_url = "gs://${google_storage_bucket.study.name}/${google_storage_bucket_object.url_list.name}" }
    gcs_data_sink {
      bucket_name = google_storage_bucket.study.name
      path        = "raw/"
    }
  }
  schedule { # 시작일 = 종료일이면 한 번만 돈다
    schedule_start_date {
      year  = 2026
      month = 10
      day   = 12
    }
    schedule_end_date {
      year  = 2026
      month = 10
      day   = 12
    }
  }
  depends_on = [google_storage_bucket_iam_member.sts]
}

# ── 네트워크: 기본 VPC를 건드리지 않는 스프린트 전용 VPC ──
resource "google_compute_network" "study" {
  name                    = "study"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "study" {
  name                     = "study-${var.region}"
  network                  = google_compute_network.study.id
  ip_cidr_range            = "10.10.0.0/24"
  private_ip_google_access = true # 외부 IP 없는 Dataproc 노드가 GCS에 닿는 길. SSH 규칙은 두지 않는다
}

resource "google_compute_firewall" "internal" {
  name          = "study-internal" # Dataproc 노드끼리의 통신
  network       = google_compute_network.study.name
  source_ranges = [google_compute_subnetwork.study.ip_cidr_range]
  allow { protocol = "tcp" }
  allow { protocol = "udp" }
  allow { protocol = "icmp" }
}

# ── 서비스 계정: 사람 계정 대신 역할을 좁힌 계정으로 돈다 ──
resource "google_service_account" "composer" {
  account_id   = "study-composer"
  display_name = "Composer environment (sprint)"
}

resource "google_service_account" "dataproc" {
  account_id   = "study-dataproc"
  display_name = "Dataproc cluster (sprint)"
}

resource "google_project_iam_member" "roles" {
  for_each = {
    composer_worker = ["roles/composer.worker", google_service_account.composer.member] # 환경 운영 + 객체 읽기·쓰기
    composer_jobs   = ["roles/dataproc.editor", google_service_account.composer.member] # Dataproc에 잡 제출
    dataproc_worker = ["roles/dataproc.worker", google_service_account.dataproc.member] # 클러스터 운영 + 객체 읽기·쓰기
  }
  project = var.project
  role    = each.value[0]
  member  = each.value[1]
}

# Composer 서비스 에이전트가 환경 계정으로 자원을 만들 수 있어야 한다(Composer 2·3 필수).
resource "google_service_account_iam_member" "composer_agent" {
  service_account_id = google_service_account.composer.name
  role               = "roles/composer.ServiceAgentV2Ext"
  member             = "serviceAccount:service-${data.google_project.this.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# ── 안전장치: 매일 02:00(KST)에 Composer를 지운다 ──
# Composer는 일시정지도 자동 만료도 없고, 첫 예산 알림은 약 4일 뒤에야 울린다. 잊어도 피해가 하루치로 끝난다.
resource "google_service_account" "killer" {
  account_id   = "study-composer-killer"
  display_name = "Nightly Composer delete (safety net)"
}

resource "google_project_iam_custom_role" "killer" {
  role_id     = "studyComposerKiller"
  title       = "Delete Composer environments only"
  permissions = ["composer.environments.delete", "composer.environments.get", "composer.operations.get"]
}

resource "google_project_iam_member" "killer" {
  project = var.project
  role    = google_project_iam_custom_role.killer.id
  member  = google_service_account.killer.member
}

resource "google_cloud_scheduler_job" "composer_kill" {
  name      = "delete-composer-nightly"
  region    = var.region
  schedule  = "0 2 * * *"
  time_zone = "Asia/Seoul"
  http_target {
    http_method = "DELETE" # 환경이 없으면 404로 끝난다. 그 실패 기록은 무해하다
    uri         = "https://composer.googleapis.com/v1/projects/${var.project}/locations/${var.region}/environments/study"
    oauth_token { service_account_email = google_service_account.killer.email }
  }
  depends_on = [google_project_iam_member.killer]
}

# ── 스프린트 당일에만 켜는 관리형 서비스 ──
resource "google_composer_environment" "study" {
  count  = var.enable_composer ? 1 : 0
  name   = "study"
  region = var.region
  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"
    software_config {
      image_version = "composer-3-airflow-3.3.1-build.3" # 로컬과 같은 Airflow 3.3.1
      # DAG가 읽는 환경 변수. DATAPROC_CLUSTER가 있으면 spark_job()이 Dataproc에 잡을 낸다(dags/common/airflow_utils.py)
      env_variables = {
        EVENTS_LANDING   = "gs://${local.bucket}/landing/events"
        EVENTS_WAREHOUSE = "gs://${local.bucket}/warehouse/events"
        PIPELINE_CODE    = "gs://${local.bucket}/code"
        DATAPROC_CLUSTER = "study"
        DATAPROC_REGION  = var.region
      }
      airflow_config_overrides = {
        core-dags_are_paused_at_creation = "False" # 로컬과 같다. 백필은 일시정지면 진행되지 않는다
      }
    }
    node_config { service_account = google_service_account.composer.email }
  }
  # 안전장치가 먼저 있어야 한다(ADR 0001 부록 A 3)
  depends_on = [google_service_account_iam_member.composer_agent, google_project_iam_member.roles, google_cloud_scheduler_job.composer_kill]
}

resource "google_dataproc_cluster" "study" {
  count  = var.enable_dataproc ? 1 : 0
  name   = "study"
  region = var.region
  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-standard-8"         # 체험판 '동시 8코어' 규칙 안(ADR 0001 부록 A 4)
      disk_config { boot_disk_size_gb = 100 } # 기본 500GB
    }
    software_config {
      image_version = "3.0-debian13" # Spark 4.1.2, 로컬과 같은 버전(ADR 0002)
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true" # 단일 노드
        # YARN은 코어도 센다(DominantResourceCalculator, 노드 8코어). Spark 앱 하나 = 관리자(AM) 1코어 + 실행기 코어.
        # 처음에 실행기 2코어로 분할 잡 7개를 한꺼번에 내자, AM 7개가 7코어를 잡아 실행기가 뜨지 못했다(ADR 0008 결과).
        # 실행기 1코어면 앱 하나가 2코어라 4개가 동시에 든다. 하루치 잡은 gzip 하나를 한 코어로 읽는 시간이 대부분이다.
        "spark:spark.dynamicAllocation.enabled"           = "false"
        "spark:spark.executor.instances"                  = "1"
        "spark:spark.executor.cores"                      = "1"
        "spark:spark.executor.memory"                     = "3g"
        "spark:spark.driver.memory"                       = "2g" # 기본 8g. 드라이버는 YARN 밖 같은 노드에서 돈다
        "dataproc:dataproc.scheduler.max-concurrent-jobs" = "4"  # 넘치는 잡은 Dataproc이 줄 세운다(AM이 코어를 잡고 기다리지 않게)
        # Spark 기본 커밋 방식으로 되돌린다. Dataproc 3.0 기본값(PathOutputCommitProtocol + GCS용 ManifestCommitter)은
        # 같은 폴더에 동시에 쓰면 서로의 _temporary를 지워 데이터가 조용히 사라졌고, 날짜 덮어쓰기(dynamic)도 덧붙이기가 됐다(ADR 0008 결과).
        # 기본 방식은 잡마다 .spark-staging-<uuid>에 쓰고, 커밋 때 그 날짜 폴더만 지우고 옮긴다(로컬과 같다).
        "spark:spark.sql.sources.commitProtocolClass"                    = "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
        "spark:spark.sql.parquet.output.committer.class"                 = "org.apache.parquet.hadoop.ParquetOutputCommitter"
        "spark:spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs" = "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory"
        # 증거: 이벤트 로그를 버킷에 쓴다. 클러스터가 지워져도 남고, 로컬 bench/stages.py로 읽는다
        "spark:spark.eventLog.dir"             = "gs://${local.bucket}/eventlogs"
        "spark:spark.history.fs.logDirectory"  = "gs://${local.bucket}/eventlogs"
        "spark:spark.eventLog.compress"        = "false"
        "spark:spark.eventLog.rolling.enabled" = "false"
      }
    }
    gce_cluster_config {
      subnetwork             = google_compute_subnetwork.study.id
      internal_ip_only       = true
      service_account        = google_service_account.dataproc.email
      service_account_scopes = ["cloud-platform"]
    }
    endpoint_config { enable_http_port_access = true } # 콘솔에서 Spark·YARN 화면을 본다(SSH 없이)
    lifecycle_config {
      idle_delete_ttl  = "1800s"                        # 잡 없이 30분이면 지운다
      auto_delete_time = timeadd(plantimestamp(), "8h") # 잡이 걸려 있어도 8시간이면 지운다(idle만으로는 부족)
    }
  }
  lifecycle {
    ignore_changes = [
      cluster_config[0].lifecycle_config[0].auto_delete_time, # 만든 때 정한 값을 유지한다
      # Dataproc이 기본 범위를 덧붙여 저장한다. 비교하면 apply마다 클러스터를 새로 만든다(2026-10-12 겪음)
      cluster_config[0].gce_cluster_config[0].service_account_scopes,
    ]
  }
  depends_on = [google_project_iam_member.roles]
}

output "bucket" { value = google_storage_bucket.study.name }
output "composer_dag_prefix" { value = one(google_composer_environment.study[*].config[0].dag_gcs_prefix) }
output "airflow_uri" { value = one(google_composer_environment.study[*].config[0].airflow_uri) }
