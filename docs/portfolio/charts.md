# 그림으로 보는 결과

`python3 docs/portfolio/build_viz.py`가 [data/](data/)의 집계 CSV에서 다시 그린다. 원본 이벤트는 재배포하지 않는다.
값을 짚어 보거나 표로 보려면 [report.html](report.html)을 내려받아 브라우저로 연다.

## 배치 적재와 품질 (1·2단계)
일 단위 멱등 적재, 품질 검사 8개, 7개월 백필. 동시 실행 수는 재서 정했다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/daily-dark.svg">
  <img alt="하루 이벤트 수와 구매 수 (2019-10 ~ 2020-04, 213일). 품질 검사 8개를 모두 통과한 날 가운데 원천 결함이 있었다. 이벤트가 평소의 10%인 날이 하루, 구매가 거의 없는 날이 4일이다" src="img/daily-light.svg" width="720">
</picture>

어떻게 셌나: warehouse에 적재된 날짜별 행 수와 구매 이벤트 수. 구매가 빠진 날은 원천 월별 압축 파일을 직접 세어 원천에도 없음을 확인했다(우리 적재 문제가 아니다) · 데이터: [daily_events.csv](data/daily_events.csv), [funnel_daily.csv](data/funnel_daily.csv)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/concurrency-dark.svg">
  <img alt="동시 실행 수 정하기 (11월 30일치 백필). 동시 4개가 가장 빠르지만 Docker VM 메모리 한도에 닿아 1건이 실패했다. 그래서 상한을 3으로 정했다" src="img/concurrency-light.svg" width="720">
</picture>

어떻게 셌나: 11월 30일치 events_daily 백필을 동시 1~4개로 돌린 시간과 docker stats로 본 컨테이너 메모리 합계의 최대(ADR 0005) · 데이터: [concurrency_nov.csv](data/concurrency_nov.csv)

## 레이크하우스 모델링 (3단계)
Iceberg 위에 세션, 상품 이력(SCD2), 퍼널·코호트 마트를 만들었다. 원천 데이터의 함정 세 가지를 숫자로 찾았다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/session_pr-dark.svg">
  <img alt="우리 세션 경계 vs 원천 user_session (날짜별). 3/25 하루 만에 정밀도가 0.87→0.75, 재현율이 0.35→0.58로 계단처럼 바뀐다. 우리 코드가 아니라 원천의 세션 정의가 바뀌었다. 그래서 일치율 채점을 버리고 고장 감지 바닥을 세웠다" src="img/session_pr-light.svg" width="720">
</picture>

어떻게 셌나: 같은 사용자의 같은 날 연속 이벤트 쌍마다 '여기서 세션이 끊기나'를 우리 규칙(30분 비활동)과 원천 user_session으로 각각 판정해 날짜별로 비교했다(ADR 0007 결정 2) · 데이터: [session_pr_daily.csv](data/session_pr_daily.csv)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/scd2-dark.svg">
  <img alt="SCD2가 지킨 11월 매출 (상위 카테고리별, 로그 축). 현재 카테고리로 덮어쓰면(Type-1) 11월 매출의 61%가 전자제품에서 '건설 공구'로 옮겨 간다. 원천이 12/1부터 카테고리를 다시 매겼기 때문이다. SCD2는 11월 구매를 11월의 카테고리로 센다" src="img/scd2-light.svg" width="720">
</picture>

어떻게 셌나: 11월 구매를 그때 버전(as-of 조인)과 현재 버전의 상위 카테고리로 각각 합산했다. 두 합계는 같다. 61%는 electronics→construction으로 옮겨 간 금액(1억 6,739만)의 비중이다(ADR 0007) · 데이터: [scd2_nov_revenue.csv](data/scd2_nov_revenue.csv)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/funnel-dark.svg">
  <img alt="10월의 함정과 기본 지표 고르기 (달별). 10월은 장바구니가 덜 기록돼 '구매 1건당 장바구니'가 1.2다(다른 달 2.5~3.0). 장바구니를 거치지 않는 세션 전환율은 10월도 다른 달과 비슷하다. 그래서 달끼리 비교하는 기본 지표를 세션 전환율로 정했다" src="img/funnel-light.svg" width="720">
</picture>

어떻게 셌나: mart_funnel_daily를 달별로 합산했다. 구매가 빠진 4일(원천 결함)은 뺐다(ADR 0007) · 데이터: [funnel_daily.csv](data/funnel_daily.csv)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/cohort-dark.svg">
  <img alt="월 코호트: 첫 방문 뒤 다시 온 사용자 비율. 다음 달에 다시 오는 비율은 20%~36%이고, 늦게 들어온 코호트일수록 낮다. 10월 코호트는 데이터 시작 전부터 오던 사용자가 섞여 비율이 부풀었다(좌측 절단). 그래서 회색으로 뺐다" src="img/cohort-light.svg" width="720">
</picture>

어떻게 셌나: mart_cohort_monthly: 첫 방문 달로 사용자를 묶고, 그 뒤 달마다 한 번이라도 온 사용자 수를 셌다. 코호트 크기 합계 15,639,803명은 독립 쿼리와 같다(ADR 0007) · 데이터: [cohort.csv](data/cohort.csv)

## GCP로 옮기기
같은 DAG를 Composer와 Dataproc으로 옮겨 213일을 돌렸다. 날짜별 행 수가 로컬과 모두 같았다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/breakdown-dark.svg">
  <img alt="같은 하루치 잡의 시간 분해: 노트북 vs Dataproc (2019-11-05). Dataproc에서는 계산도 8~9배 느리지만, 잡마다 계산 밖의 시간이 38~50초씩 더 붙는다. 하루치가 작은 이 데이터에서는 클러스터의 고정 비용이 계산보다 크다" src="img/breakdown-light.svg" width="720">
</picture>

어떻게 셌나: Spark 이벤트 로그의 단계 시간과 Airflow 태스크 시간으로 나눴다. 노트북은 ADR 0005, Dataproc은 ADR 0008의 측정이다 · 데이터: [job_breakdown_1105.csv](data/job_breakdown_1105.csv)

## 실시간 (4단계)
Kafka로 이벤트를 다시 보내고 Spark Structured Streaming으로 1분 퍼널과 세션을 셌다. 결과는 배치와 윈도마다 대조했다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/watermark-dark.svg">
  <img alt="워터마크 5·10·20분 비교 (같은 재생: 5%는 0~8분, 0.5%는 15~20분 늦게 보냄). 길게 잡을수록 덜 버리지만 윈도를 상태에 더 오래 들고 있다. 그만큼 결과도 늦게 확정된다. 워터마크보다 늦은 이벤트가 모두 버려지지는 않는다(Spark는 한 배치 전 워터마크로 거른다)" src="img/watermark-light.svg" width="720">
</picture>

어떻게 셌나: 같은 2시간 구간을 60배속으로 재생해 1분 퍼널을 세고, 배치 결과와 윈도마다 대조했다(ADR 0009) · 데이터: [watermark.csv](data/watermark.csv)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/stream-dark.svg">
  <img alt="실시간 1분 집계에서 빠진 이벤트 (실험 E1, 워터마크 10분, 120개 윈도). 120/120개 윈도에서 빠진 수(막대)가 '워터마크보다 늦게 보낸 수'(선)를 넘지 않는다. 이것이 대조 기준이다. 실제로 빠진 것은 늦게 보낸 3,744건 중 1,295건이다" src="img/stream-light.svg" width="720">
</picture>

어떻게 셌나: 같은 구간을 배치로 다시 세고, 재생 계획을 같은 시드로 다시 세워 윈도마다 '버려질 수 있었던 이벤트' 수를 구했다(ADR 0009) · 데이터: [stream_e1_windows.csv](data/stream_e1_windows.csv)
