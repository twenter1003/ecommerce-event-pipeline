# 01. 1단계 적재: 멱등 적재·날짜 파티션·완전 중복 제거

- 대상 코드: `pipeline/split_raw.py`, `pipeline/load_events.py`, `pipeline/spark.py`, `tests/test_pipeline.py`
- 설계 기록: [ADR 0003](../decisions/0003-stage1-ingest.md)
- 상태: 미답변

## 다룬 개념
- **멱등성(idempotency)**: 같은 입력으로 몇 번을 다시 돌려도 결과가 같다. 재시도·백필이 안전해지는 조건이다.
- **파티셔닝(Hive 스타일 `dt=YYYY-MM-DD`)**: 날짜별로 폴더를 나눠, 읽을 때는 필요한 날짜만 읽고(partition pruning) 쓸 때는 날짜 단위로 갈아 끼운다.
- **dynamic partition overwrite**: 덮어쓰기는 하되, 이번에 쓰는 데이터에 있는 파티션만 교체한다. SQL의 `INSERT OVERWRITE ... PARTITION`과 같은 개념이다.
- **컬럼 포맷(Parquet)**: 컬럼별로 묶어 압축한다. 필요한 컬럼만 읽고, 타입이 파일에 같이 저장된다.
- **landing과 정제 계층의 분리**: 원본은 손대지 않고 보관하고, 정제는 그다음 단계가 맡는다.

## 질문 (답은 블로그에)
1. landing에는 값을 문자열 그대로, 중복도 지우지 않고 남겼다. 분할하면서 바로 중복을 지웠다면 나중에 무엇을 할 수 없게 되나?
2. 같은 날짜를 두 번 적재해도 행이 두 배가 되지 않는 이유를 `mode("overwrite")`와 `partitionOverwriteMode=dynamic` 두 가지로 설명하라. dynamic을 빼면 무엇이 깨지나?
3. 10/1 landing 파일에 10/2 행이 하나 섞여 들어왔다. `load_events.py`에 날짜 필터가 없다면 무슨 일이 생기나? 이건 왜 "중복"보다 무서운 문제인가?
4. 날짜 안에서만 중복을 지웠는데도 "테이블 전체에 완전 중복이 없다"고 말할 수 있는 이유는? 중복의 정의를 "event_time만 다르고 나머지가 같으면 중복"으로 바꾸면 이 주장은 여전히 맞나?
5. `spark.sql.session.timeZone=UTC`를 빼도 dt는 맞게 나온다. 그럼 무엇이 틀어지고, 이 버그는 언제 드러나나?
6. (면접형) 31일치를 적재하다가 20일째에서 프로세스가 죽었다. 지금 warehouse는 어떤 상태이고, 무엇을 다시 돌리면 되나?
7. 원천이 10/1 파일을 헤더만 있는 빈 파일로 정정해 보냈다. 검사가 없던 처음 코드는 성공으로 끝났는데, warehouse의 10/1에는 무엇이 남아 있었나? "몇 번을 돌려도 결과가 같다"와 "결과가 늘 최신 입력을 반영한다"는 어떻게 다른가? (힌트: ADR 0003 결정 3의 보완)
8. (면접형) 이 구조는 ETL인가 ELT인가? "원본이 목적지에 남는가", "정리는 어디서 계산하는가" 두 기준으로 답하라. 스프린트에서 BigQuery에 넣을 때는 답이 어떻게 달라지나?

## 직접 돌려 볼 실험
1. **멱등성을 눈으로 확인하기**
   ```bash
   ls -la data/warehouse/events/dt=2019-10-15/
   .venv/bin/python -m pipeline.load_events --start 2019-10-15 --landing data/landing/events --warehouse data/warehouse/events
   ls -la data/warehouse/events/dt=2019-10-15/
   ```
   볼 것: 파일 이름(`part-...`)은 바뀌지만 파일 수는 1개 그대로이고, 크기도 거의 같다. 다른 날짜 폴더의 수정 시각은 바뀌지 않는다.
2. **안전장치를 하나 빼고 테스트 돌리기**: `pipeline/spark.py`의 `spark.sql.session.timeZone` 줄을 주석 처리하고 `.venv/bin/python -m pytest -q`를 돌린다. 끝나면 주석을 되돌린다.
   볼 것: 어느 테스트가 깨지는지, 그리고 그 테스트가 왜 dt가 아니라 epoch 초를 보는지.

## 학습 자료
- [Spark 4.1.2 — CSV Files](https://spark.apache.org/docs/4.1.2/sql-data-sources-csv.html): 'Data Source Option' 표에서 `mode`, `timestampFormat`, `header`만 본다.
  > 요약: CSV 읽기 옵션 목록이다. `mode`는 깨진 행을 어떻게 처리할지 정한다. PERMISSIVE는 null로 바꾸고, DROPMALFORMED는 버리고, FAILFAST는 바로 실패시킨다. `timestampFormat`은 시각 문자열을 해석하는 패턴이다.
- [Spark 4.1.2 — Parquet Files](https://spark.apache.org/docs/4.1.2/sql-data-sources-parquet.html): 'Partition Discovery' 섹션만 본다.
  > 요약: `key=value` 이름의 폴더를 읽으면 Spark가 그 값을 컬럼으로 되살린다. 우리 `dt` 컬럼이 Parquet 파일 안이 아니라 폴더 이름에만 있는데도 읽히는 이유다.
- [Spark 4.1.2 — Configuration](https://spark.apache.org/docs/4.1.2/configuration.html): 페이지에서 `partitionOverwriteMode`를 검색해 그 한 항목만 읽는다.
  > 요약: static(기본값)은 덮어쓸 때 대상 경로 아래 파티션을 모두 지운다. dynamic은 이번에 쓰는 파티션만 교체한다. `.option("partitionOverwriteMode", ...)`처럼 쓰기 옵션으로 주면 이 설정보다 우선한다.
