# 06. 3-3 차원 모델과 마트: SCD2 차이 MERGE, 신선도 관문, 퍼널·코호트

- 대상 코드: `pipeline/marts.py`, `tests/test_marts.py`, `dags/lake.py`(`lake_marts`), `pipeline/model_daily.py`(규칙 2의 바닥)
- 설계 기록: [ADR 0007](../decisions/0007-stage3-modeling.md) 결정 1·3·4·6, '3-3 결과' 절
- 상태: 미답변

## 다룬 개념
- **사실·차원·마트**: 사실(fact)은 일어난 일 하나가 한 행인 표다(`fct_sessions`). 차원(dimension)은 사실에 붙이는 설명이다(`dim_product`: 상품의 카테고리·브랜드). 마트(mart)는 질문 하나에 맞춰 미리 계산해 둔 작은 표다(`mart_funnel_daily`, `mart_cohort_monthly`).
- **SCD2(천천히 바뀌는 차원, 2형)**: 속성이 바뀌면 옛 행을 닫고(`valid_to`) 새 행을 연다. 버전마다 대리 키(`product_sk`)가 따로 있다. 이 프로젝트는 사실에 키를 박지 않고, 날짜로 그때의 버전을 찾아 붙인다(as-of 조인).
- **원하는 상태 계산 + 차이 MERGE**: 매번 `product_daily` 전체에서 '정답 dim'을 처음부터 계산한다. 그다음 지금 dim과 비교해 다른 행만 반영한다. 처리 순서가 섞여도 같은 상태로 모이고, 차이가 0이면 커밋이 없다.
- **스냅숏 고정과 신선도 관문**: 읽을 시점을 스냅숏 id 하나로 못 박는다. 그다음 계보 컬럼(`src_build`)으로 lake가 지금의 events를 그대로 반영하는지 확인하고, 그때만 쓴다.
- **검증 기준의 표류(reference drift)**: 비교 기준으로 삼은 원천 `user_session`도 바뀔 수 있다. 그래서 일치율로 채점하지 않고, 고장 난 세션만 막는 바닥을 둔다.

## 질문 (답은 블로그에)
1. `dim_product`를 날마다 "어제 열린 버전과 오늘을 비교"하는 증분 MERGE로 만들지 않았다. 대신 매번 전체에서 원하는 상태를 계산해 차이만 MERGE했다.
   - 증분 방식이면 다음 세 경우에 각각 무엇이 깨지나? ① 섞인 순서의 백필 ② brand 이어 쓰기 ③ 두 실행이 겹칠 때(read skew)
   - `test_late_day_rewrites_history_with_insert_update_delete`에서 추가·변경·삭제가 하나씩 나오는 이유를 날짜 그림으로 설명하라.
2. MERGE의 모든 절에 `op` 조건을 붙였고, MERGE 뒤에 원하는 상태와 양방향으로 대조했다(검사 5).
   - 조건 없는 `WHEN NOT MATCHED THEN INSERT`를 쓰면 차이 계산의 버그가 어떻게 조용히 반영되나?
   - "두 번째 실행에는 커밋이 없다"를 스냅숏 개수가 아니라 스냅숏 id로 증명한 이유는?
   - 관문보다 먼저 입력 스냅숏을 고정했다. 고정하지 않으면 어떤 틈이 생기나?
3. brand의 null을 "그날 관측 못 함"으로 보고 마지막 값을 이어 썼다. 그런데 null→값을 첫 버전까지 거슬러 채우지는 않았다.
   - 거슬러 채우면 어떤 과거 보고서가 거짓말을 하게 되나?
   - `product_sk`를 속성 해시가 아니라 `(product_id, valid_from)`으로 만든 이유를 acer→lenovo→acer 예로 설명하라.
   - 돌연변이 실험에서 키를 속성 해시로 바꾸면 테스트가 실패한다. 어떤 검사가 먼저 걸리나?
4. (면접형) 7개월 사전 점검에서 세션 검증(정밀도 ≥ 0.85, 재현율 ≥ 0.30)이 61일 실패했다. 3/24에서 3/25로 하루 만에 두 값이 계단처럼 바뀌었다.
   - 우리 코드의 버그가 아니라 원천의 세션 정의가 바뀐 것이라는 근거를 두 가지 대라.
   - 바닥을 0.50/0.05로 정한 근거는 무엇인가?
   - 이 바닥이 놓치는 고장은 무엇이고, 그건 어디서 잡나?
5. (면접형) SCD2 비교 실험에서 현재 카테고리(Type-1)로 보면 11월 매출의 61%가 "건설 공구"로 옮겨 갔다.
   - 무슨 일이 있었나?
   - 이 숫자로 "SCD2가 왜 필요한가"를 1분 안에 설명하라.
   - 반대로 12월 이후의 카테고리 분석에는 SCD2만으로 부족하다. 이유는?
6. (면접형) 2020-02-27은 이벤트가 평소의 10%였는데 2단계 품질 검사 8개를 모두 통과했다.
   - 어떤 규칙이 있었다면 잡았을까?
   - 그 규칙은 "날짜 결과는 그날 데이터만의 순수 함수"라는 원칙과 어떻게 부딪히나?

## 직접 돌려 볼 실험
1. **두 번 돌려도 dim에 커밋이 없나** (스택이 떠 있을 때)
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger lake_marts
   ```
   run이 끝나면(약 1분) 스냅숏 목록을 본다.
   ```bash
   docker compose exec airflow-scheduler python -m pipeline.lake sql "SELECT committed_at, operation, summary['added-records'] AS added FROM lake.dw.dim_product.snapshots ORDER BY committed_at"
   ```
   볼 것: run을 몇 번 돌려도 `dim_product`의 스냅숏은 첫 실행의 1개뿐이다. 테이블 이름을 `mart_funnel_daily`로 바꾸면 실행마다 하나씩 는다. 두 테이블은 왜 다른가?
2. **같은 달을 Type-1과 Type-2로 집계하기** (스택이 떠 있을 때)
   ```bash
   docker compose exec airflow-scheduler python -m pipeline.lake sql "CREATE OR REPLACE TEMP VIEW p AS SELECT product_id, dt, price FROM parquet.\`/opt/airflow/data/warehouse/events\` WHERE event_type = 'purchase' AND dt BETWEEN DATE '2019-11-01' AND DATE '2019-11-30'" "SELECT v2.category_l1 AS then_l1, v1.category_l1 AS now_l1, round(sum(p.price)) AS revenue FROM p JOIN lake.dw.dim_product v2 ON p.product_id = v2.product_id AND p.dt >= v2.valid_from AND p.dt < v2.valid_to JOIN lake.dw.dim_product v1 ON p.product_id = v1.product_id AND v1.is_current GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5"
   ```
   볼 것: `then_l1`과 `now_l1`이 다른 줄 중 가장 큰 것이 electronics → construction이다. 조인 조건의 `p.dt < v2.valid_to`를 `<=`로 바꾸면 무엇이 생길지 먼저 예상하고 돌려 본다. 힌트는 버전이 바뀌는 날의 구매다.

## 학습 자료
링크는 2026-10-12에 열리는 것을 확인했다. Iceberg 문서는 버전 번호가 들어간 주소(`/docs/1.12.0/`)가 404여서 `latest`를 준다. 확인 시점의 latest가 이 프로젝트의 버전 1.12.0이다.
- [Kimball Group — Type 2: Add New Row](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
  > 요약: 속성이 바뀌면 새 행을 추가해 옛 행의 이력을 보존한다. 한 상품이 여러 행을 가지므로 자연 키만으로는 부족하다. 행마다 새 대리 키를 주고, 사실 테이블은 변경 시점부터 새 키를 쓴다. 시작일, 종료일, 현재 행 표시의 세 컬럼이 최소다.
  > (덧붙임, 문서 밖 내용) 이 프로젝트는 사실에 키를 박지 않고 날짜로 버전을 찾는다(as-of). 섞인 순서의 백필과 맞추기 위해서다(ADR 0007 결정 3).
- [Iceberg 1.12.0 — Spark Writes](https://iceberg.apache.org/docs/latest/spark-writes/): 'MERGE INTO' 절만 본다.
  > 요약: 원천 쿼리로 대상 테이블을 갱신한다. 바뀐 행이 든 데이터 파일만 다시 쓴다. `WHEN MATCHED AND <조건>`처럼 절마다 조건을 붙일 수 있고, 처음 맞는 절이 쓰인다. 대상 한 행을 원천 여러 행이 갱신하려 하면 오류가 난다.
- [Iceberg 1.12.0 — Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/): 'expire_snapshots' 절만 본다.
  > 요약: `older_than`(기본 5일 전)보다 오래된 스냅숏과, 그 스냅숏들만 쓰던 데이터 파일을 지운다. `retain_last`(기본 1)만큼은 나이와 상관없이 남긴다. 브랜치나 태그가 가리키는 스냅숏은 지우지 않는다.
