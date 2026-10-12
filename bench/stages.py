"""Spark 이벤트 로그에서 단계(stage)별 걸린 시간·태스크 수·코어 사용률을 뽑는다. 설계는 ADR 0005 ⑤.

    python3 bench/stages.py <이벤트 로그 파일> [코어 수, 기본 10]

이벤트 로그는 압축하지 않고 파일 하나로 남겨야 읽힌다(Spark 4.0부터 기본이 압축 + 여러 파일로 나눔). bench/spark_once.sh가 이렇게 남긴다:
    --conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false --conf spark.eventLog.rolling.enabled=false
셔플 쓰기 = 그 단계가 다음 단계로 넘기려고 쓴 양(Exchange 하나가 셔플 쓰기 단계 하나를 만든다).
내려 씀 = 메모리가 모자라 정렬·집계 중간 결과를 디스크로 내려 쓴 양(spill). 쏠림 = 가장 긴 태스크 ÷ 태스크 시간 중앙값.
코어 사용률 = 그 단계 태스크들이 일한 시간의 합 / (단계 시간 × 코어 수). 태스크 1개짜리 단계는 많아야 1/코어 수다.
CPU 비율 = 태스크가 CPU를 실제로 쓴 시간 / 태스크 실행 시간. 1에 가까우면 계산이 병목, 낮으면 디스크 등 입출력을 기다린 것이다.
"""
import json
import statistics
import sys
from collections import defaultdict

path = sys.argv[1]
cores = int(sys.argv[2]) if len(sys.argv) > 2 else 10
app, stages, busy, run, cpu = {}, [], defaultdict(int), defaultdict(int), defaultdict(int)
shuf, spill, times = defaultdict(int), defaultdict(int), defaultdict(list)
for line in open(path):
    e = json.loads(line)
    ev = e["Event"]
    if ev == "SparkListenerApplicationStart":
        app["start"] = e["Timestamp"]
    elif ev == "SparkListenerApplicationEnd":
        app["end"] = e["Timestamp"]
    elif ev == "SparkListenerTaskEnd":
        t = e["Task Info"]
        i, m = e["Stage ID"], e["Task Metrics"]
        busy[i] += t["Finish Time"] - t["Launch Time"]
        times[i].append(t["Finish Time"] - t["Launch Time"])
        run[i] += m["Executor Run Time"]  # ms
        cpu[i] += m["Executor CPU Time"] / 1e6  # ns → ms
        shuf[i] += m["Shuffle Write Metrics"]["Shuffle Bytes Written"]
        spill[i] += m["Disk Bytes Spilled"]
    elif ev == "SparkListenerStageCompleted":
        stages.append(e["Stage Info"])

total = (app["end"] - app["start"]) / 1000
in_stages = 0
print(f"{'stage':>5} {'태스크':>5} {'시간(초)':>8} {'코어 사용':>8} {'CPU 비율':>8} {'셔플쓰기MB':>9} {'내려씀MB':>8} {'쏠림':>5}  이름")
for s in stages:
    dur = s["Completion Time"] - s["Submission Time"]
    in_stages += dur
    use = busy[s["Stage ID"]] / (dur * cores) if dur else 0
    i = s["Stage ID"]
    ratio = cpu[i] / run[i] if run[i] else 0
    skew = max(times[i]) / (statistics.median(times[i]) or 1) if times[i] else 0
    print(f"{i:>5} {s['Number of Tasks']:>5} {dur / 1000:>8.1f} {use:>8.0%} {ratio:>8.0%} "
          f"{shuf[i] / 2**20:>9.0f} {spill[i] / 2**20:>8.0f} {skew:>5.1f}  {s['Stage Name'][:50]}")
print(f"셔플 쓰기 단계 {sum(1 for v in shuf.values() if v)}개, 셔플 쓰기 합 {sum(shuf.values()) / 2**20:.0f}MB, "
      f"디스크로 내려 쓴 양 합 {sum(spill.values()) / 2**20:.0f}MB")
# 단계는 겹치지 않고 차례로 돈다고 본다(우리 작업은 그렇다). 겹치면 합이 앱 시간보다 커진다.
print(f"앱 {total:.1f}초 = 단계 안 {in_stages / 1000:.1f}초 + 단계 밖(드라이버 혼자 계획·파일 나열·결과 수집) {total - in_stages / 1000:.1f}초")
