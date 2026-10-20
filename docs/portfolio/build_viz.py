"""포트폴리오 그림 생성기. data/*.csv(집계값) → img/*.svg + charts.md + report.html

    python3 docs/portfolio/build_viz.py [--artifact 경로]

- img/<차트>-light.svg, -dark.svg: README와 charts.md에 <picture>로 넣는 정적 그림. GitHub 테마에 맞는 쪽이 보인다
- report.html: 마우스를 올리면 값이 보이고 표로도 볼 수 있는 인터랙티브 버전. 파일 하나로 열린다
- --artifact: 같은 리포트를 문서 뼈대(doctype·head·body) 없이 쓴다(claude.ai 아티팩트 게시용)
표준 라이브러리만 쓴다. 색은 dataviz 지침의 검증된 팔레트다(범주 3색은 validate_palette.js로 두 테마 통과).
차트의 숫자는 모두 data/*.csv에서 온다. 그 CSV는 warehouse·lake·측정 기록에서 뽑은 집계값이고, 원본 이벤트는 재배포하지 않는다.
머리말 숫자(KPIS)는 README '분석 결과'·'파이프라인 성능'과 같은 값이다.
"""
import csv
import html
import json
import math
import os
import statistics
import sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
DATA, IMG = os.path.join(HERE, "data"), os.path.join(HERE, "img")
W = 720  # SVG 좌표 폭. 화면에서는 너비에 맞춰 늘고 준다
FONT = 'system-ui, -apple-system, "Apple SD Gothic Neo", "Noto Sans KR", "Segoe UI", sans-serif'
SEQ = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7", "#3987e5", "#2a78d6", "#256abf",
       "#1c5cab", "#184f95", "#104281", "#0d366b"]  # 파랑 100~700
NQ = 10  # 히트맵에 쓰는 순차 단계 수


def lum(c):
    def lin(i):
        v = int(c[i:i + 2], 16) / 255
        return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
    return 0.2126 * lin(1) + 0.7152 * lin(3) + 0.0722 * lin(5)


def ink_on(fill):
    """채운 색 위에 쓸 글자색: 검정·흰색 중 대비가 큰 쪽."""
    l = lum(fill)
    return "#0b0b0b" if (l + 0.05) / (lum("#0b0b0b") + 0.05) >= 1.05 / (l + 0.05) else "#ffffff"


def theme(light):
    # 순차 색은 바탕과 2:1 이상 떨어진 단계만 쓴다. 밝은 바탕 250→700, 어두운 바탕 600→150(0에 가까울수록 바탕 쪽)
    seq = SEQ[3:] if light else SEQ[1:11][::-1]
    cat = ["#2a78d6", "#eb6834", "#1baf7a"] if light else ["#3987e5", "#d95926", "#199e70"]
    keys = ["surface", "page", "ink", "ink2", "muted", "grid", "axis", "border", "strong", "soft", "deemph"]
    vals = (["#fcfcfb", "#f9f9f7", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "rgba(11,11,11,.10)",
             "#184f95", "#86b6ef", "#c3c2b7"] if light else
            ["#1a1a19", "#0d0d0d", "#ffffff", "#c3c2b7", "#898781", "#2c2c2a", "#383835", "rgba(255,255,255,.10)",
             "#9ec5f4", "#256abf", "#52514e"])
    v = dict(zip(keys, vals), critical="#d03b3b")  # 상태색 critical은 테마와 무관하게 고정
    v.update({f"s{i}": c for i, c in enumerate(cat, 1)})
    v.update({f"on{i}": ink_on(c) for i, c in enumerate(cat, 1)})
    v.update({f"q{i}": c for i, c in enumerate(seq)})
    v.update({f"qt{i}": ink_on(c) for i, c in enumerate(seq)})
    return v


THEMES = {"light": theme(True), "dark": theme(False)}
CLASSES = (
    ".bg{fill:var(--surface)}.frame{stroke:var(--border);stroke-width:1}"
    ".grid{stroke:var(--grid);stroke-width:1}.axis{stroke:var(--axis);stroke-width:1}.ref{stroke:var(--ink2);stroke-width:1}"
    ".t1{fill:var(--ink)}.t2{fill:var(--ink2)}.t3{fill:var(--muted)}.white{fill:#fff}"
    ".s1f{fill:var(--s1)}.s2f{fill:var(--s2)}.s3f{fill:var(--s3)}.s1a{fill:var(--s1);fill-opacity:.1}"
    ".on1{fill:var(--on1)}.on2{fill:var(--on2)}.on3{fill:var(--on3)}"
    ".strongf{fill:var(--strong)}.softf{fill:var(--soft)}.deemf{fill:var(--deemph)}.critf{fill:var(--critical)}"
    ".s1l,.s2l{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}.s1l{stroke:var(--s1)}.s2l{stroke:var(--s2)}"
    ".ring{stroke:var(--surface);stroke-width:2}.link{stroke:var(--axis);stroke-width:2}"
    + "".join(f".q{i}{{fill:var(--q{i})}}.qt{i}{{fill:var(--qt{i})}}" for i in range(NQ)))


def esc(s):
    return html.escape(str(s), quote=True)


def read(name):
    with open(os.path.join(DATA, name), newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def man(n):
    """한국어 큰 수: 4.10억, 649만, 19.6만, 9,875."""
    n = float(n)
    if abs(n) >= 1e8:
        return f"{n / 1e8:.2f}억"
    if abs(n) >= 1e6:
        return f"{n / 1e4:,.0f}만"
    if abs(n) >= 1e4:
        return f"{n / 1e4:.1f}".removesuffix(".0") + "만"
    return f"{n:,.0f}"


def tw(s, size):
    """글자 폭 어림값(px). 한글 0.9em, 나머지 0.55em. 글꼴마다 달라서 조금 넉넉하게 잡는다."""
    return sum(size * (0.9 if ord(c) >= 0x1100 else 0.55) for c in s)


def wrap(s, size, width):
    """줄 수는 폭에 맞춘 최소로 하고, 그 줄 수 안에서 줄 길이를 고르게 나눈다."""
    def greedy(wd):
        lines, cur = [], ""
        for word in s.split(" "):
            nxt = f"{cur} {word}".strip()
            if cur and tw(nxt, size) > wd:
                lines.append(cur)
                cur = word
            else:
                cur = nxt
        return lines + [cur]
    lines = greedy(width)
    even = greedy(tw(s, size) / len(lines) * 1.06)
    return even if len(even) == len(lines) else lines


class Chart:
    """SVG 한 장. 좌표는 W × h, 그림 영역은 여백(l, r, t, b) 안쪽. static이면 제목·부제를 그림 안에 넣는다."""

    def __init__(self, key, h, title, subtitle, note, src, static, l=64, r=24, t=16, b=36):
        self.key, self.static, self.title, self.subtitle, self.note, self.src = key, static, title, subtitle, note, src
        self.sub_lines = wrap(subtitle, 12.5, W - 32) if static else []
        self.top = 44 + 18 * len(self.sub_lines) if static else 0
        self.h, self.l, self.r, self.t, self.b = h + self.top, l, r, t + self.top, b
        self.parts, self.table, self.cross = [], None, None

    @property
    def pw(self):
        return W - self.l - self.r

    @property
    def ph(self):
        return self.h - self.t - self.b

    def text(self, x, y, s, cls="t3", anchor="start", size=12, weight=400):
        self.parts.append(f'<text x="{x:.1f}" y="{y:.1f}" dy="0.35em" class="{cls}" font-size="{size}" '
                          f'font-weight="{weight}" text-anchor="{anchor}">{esc(s)}</text>')

    def line(self, x1, y1, x2, y2, cls):
        self.parts.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" class="{cls}"/>')

    def bar(self, x, y0, y1, w, cls, tip=None, horizontal=False):
        """끝만 4px 둥근 막대. 세로면 x=왼쪽, y0=기준선, y1=끝. 가로면 x가 위쪽 모서리, y0·y1이 가로 위치."""
        if not horizontal:
            top, bottom = min(y0, y1), max(y0, y1)
            r = min(4, bottom - top, w / 2)
            d = (f"M{x:.1f},{bottom:.1f}V{top + r:.1f}Q{x:.1f},{top:.1f} {x + r:.1f},{top:.1f}"
                 f"H{x + w - r:.1f}Q{x + w:.1f},{top:.1f} {x + w:.1f},{top + r:.1f}V{bottom:.1f}Z")
            hit = (x - 3, top - 6, w + 6, bottom - top + 6)
        else:
            left, right = min(y0, y1), max(y0, y1)
            r = min(4, right - left, w / 2)
            d = (f"M{left:.1f},{x:.1f}H{right - r:.1f}Q{right:.1f},{x:.1f} {right:.1f},{x + r:.1f}"
                 f"V{x + w - r:.1f}Q{right:.1f},{x + w:.1f} {right - r:.1f},{x + w:.1f}H{left:.1f}Z")
            hit = (left, x - 3, right - left + 6, w + 6)
        self.mark(f'<path d="{d}" class="{cls}"/>', tip, hit)

    def dot(self, x, y, cls, tip=None, r=4.5):
        self.mark(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" class="{cls} ring"/>', tip, (x - 12, y - 12, 24, 24))

    def mark(self, shape, tip, hit):
        """tip = [머리글, [값, 설명], ...]. 인터랙티브 버전에서만 표시보다 넓은 투명 영역을 붙인다."""
        if tip is None or self.static:
            self.parts.append(shape)
            return
        hx, hy, hw, hh = hit
        label = " · ".join([tip[0]] + [f"{v} {k}" for v, k, *_ in tip[1:]])
        self.parts.append(f'<g class="hit" tabindex="0" aria-label="{esc(label)}" '
                          f'data-tip="{esc(json.dumps(tip, ensure_ascii=False))}">'
                          f'<rect x="{hx:.1f}" y="{hy:.1f}" width="{max(hw, 8):.1f}" height="{max(hh, 8):.1f}" '
                          f'fill="transparent"/>{shape}</g>')

    def legend(self, items, x=None, y=None):
        """items: [(이름, 'rect'|'line'|'dot', 클래스)]. 범례 모양은 표시 모양을 따른다."""
        x = self.l if x is None else x
        y = self.t - 20 if y is None else y
        for name, kind, cls in items:
            if kind == "line":
                self.line(x, y, x + 16, y, cls)
                x += 22
            elif kind == "dot":
                self.parts.append(f'<circle cx="{x + 5}" cy="{y}" r="5" class="{cls}"/>')
                x += 15
            else:
                self.parts.append(f'<rect x="{x}" y="{y - 5}" width="10" height="10" rx="2" class="{cls}"/>')
                x += 15
            self.text(x, y, name, "t2", size=12)
            x += tw(name, 12) + 20

    def svg(self, theme=None):
        style, head, frame = "", "", ""
        if theme:  # 정적 파일: 그 테마의 실제 색을 넣고, 제목·부제를 그림 안에 쓴다
            style = ("<style>:root{" + ";".join(f"--{k}:{c}" for k, c in THEMES[theme].items()) + "}"
                     + CLASSES + f"text{{font-family:{FONT}}}</style>")
            head = (f'<text x="16" y="28" class="t1" font-size="16" font-weight="600">{esc(self.title)}</text>'
                    + "".join(f'<text x="16" y="{52 + 18 * i}" class="t2" font-size="12.5">{esc(s)}</text>'
                              for i, s in enumerate(self.sub_lines)))
            frame = " frame"
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {self.h}" role="img" '
                f'aria-label="{esc(self.title)}" font-family="{esc(FONT)}">{style}'
                f'<rect x="0.5" y="0.5" width="{W - 1}" height="{self.h - 1}" rx="10" class="bg{frame}"/>'
                f'{head}{"".join(self.parts)}</svg>')


def x_dates(ch, days):
    """날짜 축: 첫날~마지막 날을 그림 폭에 놓고, 매달 1일에 눈금."""
    d0, span = days[0], (days[-1] - days[0]).days

    def x(d):
        return ch.l + ch.pw * (d - d0).days / span

    for d in days:
        if d.day == 1:
            ch.line(x(d), ch.t + ch.ph, x(d), ch.t + ch.ph + 4, "axis")
            ch.text(x(d) + 3, ch.t + ch.ph + 15, f"{d.year % 100}.{d.month:02d}", "t3", size=11)
    return x


def y_axis(ch, ticks, fmt, box=None):
    """세로 축. ticks[0]이 기준선, ticks[-1]이 맨 위. box=(왼쪽, 위, 폭, 높이)로 작은 차트 하나를 그린다."""
    l, top, w, h = box or (ch.l, ch.t, ch.pw, ch.ph)
    lo, hi = ticks[0], ticks[-1]

    def y(v):
        return top + h * (1 - (v - lo) / (hi - lo))

    for v in ticks:
        ch.line(l, y(v), l + w, y(v), "axis" if v == lo else "grid")
        ch.text(l - 8, y(v), fmt(v), "t3", anchor="end", size=11)
    return y


def broken_days():
    """구매가 하루 중앙값의 1%도 안 되는 날. 원천 월별 파일에도 구매 행이 없다(2026-10-12 확인)."""
    rows = read("funnel_daily.csv")
    med = statistics.median(int(r["purchase_events"]) for r in rows)
    return {r["dt"]: int(r["purchase_events"]) for r in rows if int(r["purchase_events"]) < med / 100}


# ── 1·2단계 ───────────────────────────────────────────────────────
def c_daily(static):
    ev = read("daily_events.csv")
    buy = {r["dt"]: int(r["purchase_events"]) for r in read("funnel_daily.csv")}
    days = [date.fromisoformat(r["dt"]) for r in ev]
    events = [int(r["events"]) for r in ev]
    buys = [buy[r["dt"]] for r in ev]
    broken = broken_days()
    i_max, i_dip = events.index(max(events)), events.index(min(events))
    ch = Chart("daily", 360, "하루 이벤트 수와 구매 수 (2019-10 ~ 2020-04, 213일)",
               f"품질 검사 8개를 모두 통과한 날 가운데 원천 결함이 있었다. 이벤트가 평소의 10%인 날이 하루, "
               f"구매가 거의 없는 날이 {len(broken)}일이다",
               "warehouse에 적재된 날짜별 행 수와 구매 이벤트 수. 구매가 빠진 날은 원천 월별 압축 파일을 직접 세어 "
               "원천에도 없음을 확인했다(우리 적재 문제가 아니다)",
               ["daily_events.csv", "funnel_daily.csv"], static, t=30, b=30)
    gap = 46
    h1 = (ch.ph - gap) * 0.6
    h2 = ch.ph - gap - h1
    t2 = ch.t + h1 + gap
    x = x_dates(ch, days)
    # 위: 전체 이벤트
    ch.text(ch.l, ch.t - 16, "이벤트(보기·장바구니·구매)", "t2", size=12, weight=600)
    y1 = y_axis(ch, [0, 2.5e6, 5e6, 7.5e6], lambda v: man(v) if v else "0", (ch.l, ch.t, ch.pw, h1))
    pts = " ".join(f"{x(d):.1f},{y1(v):.1f}" for d, v in zip(days, events))
    ch.parts.append(f'<polygon points="{x(days[0]):.1f},{y1(0):.1f} {pts} {x(days[-1]):.1f},{y1(0):.1f}" class="s1a"/>')
    ch.parts.append(f'<polyline points="{pts}" class="s1l"/>')
    d, v = days[i_max], events[i_max]
    ch.dot(x(d), y1(v), "s1f")
    ch.text(x(d) + 9, y1(v), f"{d.month}/{d.day} 최고 {man(v)}건(세일)", "t1", size=12, weight=500)
    d, v = days[i_dip], events[i_dip]
    ch.line(x(d), y1(v) - 6, x(d), y1(4.4e6) + 8, "ref")
    ch.dot(x(d), y1(v), "s1f")
    ch.text(x(d) - 6, y1(4.4e6), f"{d.month}/{d.day} {man(v)}건 · 평소의 10%", "t1", anchor="end", size=12, weight=500)
    # 아래: 구매
    ch.text(ch.l, t2 - 16, "그중 구매", "t2", size=12, weight=600)
    y2 = y_axis(ch, [0, 1e5, 2e5], lambda v: man(v) if v else "0", (ch.l, t2, ch.pw, h2))
    ch.parts.append(f'<polyline points="{" ".join(f"{x(d):.1f},{y2(v):.1f}" for d, v in zip(days, buys))}" class="s1l"/>')
    for dt in broken:
        d = date.fromisoformat(dt)
        ch.dot(x(d), y2(broken[dt]), "critf")
    label = "구매가 거의 없는 날: " + " · ".join(f"{int(k[5:7])}/{int(k[8:])}({v}건)" for k, v in sorted(broken.items()))
    x0 = ch.l + ch.pw - tw(label, 12)
    ch.parts.append(f'<circle cx="{x0 - 10:.1f}" cy="{t2 + 4:.1f}" r="4.5" class="critf ring"/>')
    ch.text(x0, t2 + 4, label, "t1", size=12, weight=500)
    ch.table = (["날짜", "이벤트", "구매"], [(r["dt"], f"{a:,}", f"{b:,}") for r, a, b in zip(ev, events, buys)])
    ch.cross = {"x": [round(x(d), 1) for d in days], "label": [r["dt"] for r in ev],
                "series": [{"name": "이벤트", "values": [f"{v:,}" for v in events], "cls": "s1"},
                           {"name": "구매", "values": [f"{v:,}" for v in buys], "cls": "s1"}]}
    return ch


def c_concurrency(static):
    rows = read("concurrency_nov.csv")
    ch = Chart("concurrency", 250, "동시 실행 수 정하기 (11월 30일치 백필)",
               "동시 4개가 가장 빠르지만 Docker VM 메모리 한도에 닿아 1건이 실패했다. 그래서 상한을 3으로 정했다",
               "11월 30일치 events_daily 백필을 동시 1~4개로 돌린 시간과 docker stats로 본 컨테이너 메모리 합계의 최대(ADR 0005)",
               ["concurrency_nov.csv"], static, l=0, r=0, t=36, b=48)
    panels = [("걸린 시간", 0, "seconds", [0, 400, 800, 1200], lambda v: f"{v:,.0f}초"),
              ("최대 메모리", W / 2, "peak_mem_mib", [0, 5120, 10240], lambda v: f"{v / 1024:.1f}GiB")]
    for name, ox, col, ticks, fmt in panels:
        l, pw = ox + 64, W / 2 - 88
        y = y_axis(ch, ticks, lambda v, fmt=fmt: fmt(v).replace(".0", "") if v else "0", (l, ch.t, pw, ch.ph))
        ch.text(l, ch.t - 20, name, "t2", size=12, weight=600)
        if col == "peak_mem_mib":
            vm = 9.7 * 1024
            ch.line(l, y(vm), l + pw, y(vm), "ref")
            ch.text(l + 4, y(vm) + 10, "Docker VM 한도 9.7GiB", "t2", size=11)
        for i, r in enumerate(rows):
            cx, v = l + pw * (i + 0.5) / len(rows), int(r[col])
            ch.bar(cx - 12, y(0), y(v), 24, "s1f",
                   [f"동시 {r['concurrency']}개", [fmt(v), name], [f"{r['ok_runs']} / 30", "성공한 날"]])
            ch.text(cx, y(v) - 10, fmt(v), "t1", anchor="middle", size=11.5, weight=500)
            ch.text(cx, ch.t + ch.ph + 14, f"동시 {r['concurrency']}", "t3", anchor="middle", size=11)
            if r["failed_runs"] != "0":  # 상태색은 아이콘·글과 함께만 쓴다
                yy = ch.t + ch.ph + 33
                ch.parts.append(f'<circle cx="{cx - 26:.1f}" cy="{yy:.1f}" r="7" class="critf"/>')
                ch.text(cx - 26, yy, "!", "white", anchor="middle", size=11, weight=700)
                ch.text(cx - 15, yy, f"{r['failed_runs']}건 실패", "t1", size=11.5, weight=500)
    ch.table = (["동시 실행", "걸린 시간(초)", "성공", "실패", "최대 메모리(MiB)"],
                [(r["concurrency"], r["seconds"], r["ok_runs"], r["failed_runs"], r["peak_mem_mib"]) for r in rows])
    return ch


# ── 3단계 ─────────────────────────────────────────────────────────
def c_session_pr(static):
    rows = read("session_pr_daily.csv")
    days = [date.fromisoformat(r["dt"]) for r in rows]
    p = [float(r["precision"]) for r in rows]
    rc = [float(r["recall"]) for r in rows]
    k = [r["dt"] for r in rows].index("2020-03-25")
    ch = Chart("session_pr", 270, "우리 세션 경계 vs 원천 user_session (날짜별)",
               f"3/25 하루 만에 정밀도가 {p[k - 1]:.2f}→{p[k]:.2f}, 재현율이 {rc[k - 1]:.2f}→{rc[k]:.2f}로 계단처럼 바뀐다. "
               "우리 코드가 아니라 원천의 세션 정의가 바뀌었다. 그래서 일치율 채점을 버리고 고장 감지 바닥을 세웠다",
               "같은 사용자의 같은 날 연속 이벤트 쌍마다 '여기서 세션이 끊기나'를 우리 규칙(30분 비활동)과 원천 "
               "user_session으로 각각 판정해 날짜별로 비교했다(ADR 0007 결정 2)",
               ["session_pr_daily.csv"], static, r=96, t=44)
    ch.legend([("정밀도", "line", "s1l"), ("재현율", "line", "s2l")])
    x = x_dates(ch, days)
    y = y_axis(ch, [0, 0.25, 0.5, 0.75, 1.0], lambda v: f"{v:.2f}")
    xc = x(days[k])
    ch.line(xc, ch.t, xc, ch.t + ch.ph, "ref")
    ch.text(xc - 6, ch.t + 8, "3/25 원천 세션 정의 변경", "t2", anchor="end", size=12, weight=500)
    for vals, cls in [(p, "s1l"), (rc, "s2l")]:
        ch.parts.append(f'<polyline points="{" ".join(f"{x(d):.1f},{y(v):.1f}" for d, v in zip(days, vals))}" class="{cls}"/>')
    for vals, name, fcls in [(p, "정밀도", "s1f"), (rc, "재현율", "s2f")]:
        ch.dot(x(days[-1]), y(vals[-1]), fcls)
        ch.text(x(days[-1]) + 10, y(vals[-1]), f"{name} {vals[-1]:.2f}", "t1", size=12, weight=500)
    ch.table = (["날짜", "세션", "정밀도", "재현율"], [(r["dt"], f'{int(r["sessions"]):,}', r["precision"], r["recall"]) for r in rows])
    ch.cross = {"x": [round(x(d), 1) for d in days], "label": [r["dt"] for r in rows],
                "series": [{"name": "정밀도", "values": [f"{v:.3f}" for v in p], "cls": "s1"},
                           {"name": "재현율", "values": [f"{v:.3f}" for v in rc], "cls": "s2"}]}
    return ch


def c_scd2(static):
    allrows = read("scd2_nov_revenue.csv")
    rows = sorted(allrows, key=lambda r: -max(int(r["then_revenue"]), int(r["now_revenue"])))[:10]
    n = len(rows)
    ch = Chart("scd2", 34 * n + 54, "SCD2가 지킨 11월 매출 (상위 카테고리별, 로그 축)",
               "현재 카테고리로 덮어쓰면(Type-1) 11월 매출의 61%가 전자제품에서 '건설 공구'로 옮겨 간다. "
               "원천이 12/1부터 카테고리를 다시 매겼기 때문이다. SCD2는 11월 구매를 11월의 카테고리로 센다",
               "11월 구매를 그때 버전(as-of 조인)과 현재 버전의 상위 카테고리로 각각 합산했다. 두 합계는 같다. "
               "61%는 electronics→construction으로 옮겨 간 금액(1억 6,739만)의 비중이다(ADR 0007)",
               ["scd2_nov_revenue.csv"], static, l=112, r=40, t=36, b=30)
    ch.legend([("11월 당시 카테고리(SCD2)", "dot", "s1f"), ("현재 카테고리로 덮어씀(Type-1)", "dot", "s2f")])
    lo, hi = 5, 9  # 10만 ~ 10억

    def x(v):
        return ch.l + ch.pw * (math.log10(max(v, 10 ** lo)) - lo) / (hi - lo)

    for e, lab in [(5, "10만"), (6, "100만"), (7, "1,000만"), (8, "1억"), (9, "10억")]:
        ch.line(x(10 ** e), ch.t, x(10 ** e), ch.t + ch.ph, "grid")
        ch.text(x(10 ** e), ch.t + ch.ph + 14, lab, "t3", anchor="middle", size=11)
    row_h = ch.ph / n
    for i, r in enumerate(rows):
        yc = ch.t + row_h * (i + 0.5)
        a, b = int(r["then_revenue"]), int(r["now_revenue"])
        name = r["category_l1"].replace("(none)", "(코드 없음)")
        ch.text(ch.l - 10, yc, name, "t2", anchor="end", size=12)
        ch.line(x(a), yc, x(b), yc, "link")
        tip = [name, [man(a), "11월 당시 카테고리(SCD2)", "s1"], [man(b), "현재 카테고리로(Type-1)", "s2"]]
        ch.dot(x(b), yc, "s2f", tip)
        ch.dot(x(a), yc, "s1f", tip)
        if r["category_l1"] in ("electronics", "construction"):
            ch.text(max(x(a), x(b)) + 10, yc, f"{man(a)} → {man(b)}", "t1", size=12, weight=500)
    ch.table = (["상위 카테고리", "11월 당시(SCD2)", "현재 카테고리로(Type-1)"],
                [(r["category_l1"], f'{int(r["then_revenue"]):,}', f'{int(r["now_revenue"]):,}') for r in allrows])
    return ch


def c_funnel(static):
    rows = read("funnel_daily.csv")
    broken = broken_days()
    months = sorted({r["dt"][:7] for r in rows})
    agg = []
    for m in months:
        rs = [r for r in rows if r["dt"].startswith(m) and r["dt"] not in broken]
        s = {k: sum(int(r[k]) for r in rs) for k in ("sessions", "s_purchase", "cart_events", "purchase_events")}
        agg.append((m, s["cart_events"] / s["purchase_events"], s["s_purchase"] / s["sessions"] * 100, len(rs)))
    rest = agg[1:]
    ch = Chart("funnel", 230, "10월의 함정과 기본 지표 고르기 (달별)",
               f"10월은 장바구니가 덜 기록돼 '구매 1건당 장바구니'가 {agg[0][1]:.1f}다(다른 달 "
               f"{min(a[1] for a in rest):.1f}~{max(a[1] for a in rest):.1f}). 장바구니를 거치지 않는 세션 전환율은 "
               "10월도 다른 달과 비슷하다. 그래서 달끼리 비교하는 기본 지표를 세션 전환율로 정했다",
               f"mart_funnel_daily를 달별로 합산했다. 구매가 빠진 {len(broken)}일(원천 결함)은 뺐다(ADR 0007)",
               ["funnel_daily.csv"], static, l=0, r=0, t=36, b=30)
    panels = [("구매 1건당 장바구니 담기", 0, 1, [0, 2, 4], lambda v: f"{v:.1f}"),
              ("세션 전환율(구매가 있는 세션 비율)", W / 2, 2, [0, 5, 10], lambda v: f"{v:.1f}%")]
    for name, ox, col, ticks, fmt in panels:
        l, pw = ox + 56, W / 2 - 80
        y = y_axis(ch, ticks, lambda v, fmt=fmt: fmt(v).replace(".0", "") if v else "0", (l, ch.t, pw, ch.ph))
        ch.text(l, ch.t - 20, name, "t2", size=12, weight=600)
        for i, (m, *vals) in enumerate(agg):
            cx, v = l + pw * (i + 0.5) / len(agg), vals[col - 1]
            cls = "s1f" if i == 0 else "deemf"  # 10월만 강조, 나머지 달은 비교용 회색
            ch.bar(cx - 12, y(0), y(v), 24, cls, [f"{m} ({vals[2]}일)", [fmt(v), name]])
            ch.text(cx, y(v) - 10, fmt(v), "t1" if i == 0 else "t2", anchor="middle", size=11, weight=600 if i == 0 else 400)
            ch.text(cx, ch.t + ch.ph + 14, f"{int(m[5:])}월", "t3", anchor="middle", size=11)
    ch.table = (["달", "날짜 수", "구매 1건당 장바구니", "세션 전환율(%)"], [(m, nd, f"{a:.2f}", f"{b:.2f}") for m, a, b, nd in agg])
    return ch


def c_cohort(static):
    rows = read("cohort.csv")
    cell = {(r["cohort_month"], int(r["months_since"])): r for r in rows}
    cohorts = sorted({c for c, m in cell if (c, 1) in cell})  # 다음 달이 아직 없는 마지막 코호트는 뺀다
    size = {c: int(cell[(c, 0)]["cohort_users"]) for c in cohorts}
    cens = {c: cell[(c, 0)]["left_censored"] == "True" for c in cohorts}
    rate = {(c, m): int(r["active_users"]) / size[c] for (c, m), r in cell.items() if m > 0 and c in size}
    vmax = max(v for (c, m), v in rate.items() if not cens[c])
    m1 = [rate[(c, 1)] for c in cohorts if not cens[c]]
    ch = Chart("cohort", 40 * len(cohorts) + 40, "월 코호트: 첫 방문 뒤 다시 온 사용자 비율",
               f"다음 달에 다시 오는 비율은 {min(m1):.0%}~{max(m1):.0%}이고, 늦게 들어온 코호트일수록 낮다. "
               "10월 코호트는 데이터 시작 전부터 오던 사용자가 섞여 비율이 부풀었다(좌측 절단). 그래서 회색으로 뺐다",
               "mart_cohort_monthly: 첫 방문 달로 사용자를 묶고, 그 뒤 달마다 한 번이라도 온 사용자 수를 셌다. "
               "코호트 크기 합계 15,639,803명은 독립 쿼리와 같다(ADR 0007)",
               ["cohort.csv"], static, l=204, r=24, t=34, b=12)
    cw, chh = ch.pw / 6, (ch.ph - 6) / len(cohorts)
    for m in range(1, 7):
        ch.text(ch.l + cw * (m - 0.5), ch.t - 14, f"{m}개월 뒤", "t3", anchor="middle", size=11)
    for i, c in enumerate(cohorts):
        y0 = ch.t + chh * i
        ch.text(ch.l - 10, y0 + chh / 2, f"{c} · {man(size[c])}명" + (" · 좌측 절단" if cens[c] else ""),
                "t2", anchor="end", size=12)
        for m in range(1, 7):
            if (c, m) not in rate:
                continue
            v, r, x0 = rate[(c, m)], cell[(c, m)], ch.l + cw * (m - 1)
            q = min(NQ - 1, round(v / vmax * (NQ - 1)))
            fill, ink = ("deemf", "t1") if cens[c] else (f"q{q}", f"qt{q}")
            tip = [f"{c} 코호트 · {m}개월 뒤", [f"{v:.1%}", "다시 온 비율"], [f"{int(r['active_users']):,}명", "다시 온 사용자"],
                   [f"{int(r['buyers']):,}명", "그중 구매자"]]
            ch.mark(f'<rect x="{x0 + 1:.1f}" y="{y0 + 1:.1f}" width="{cw - 2:.1f}" height="{chh - 2:.1f}" rx="3" class="{fill}"/>',
                    tip, (x0, y0, cw, chh))
            ch.text(x0 + cw / 2, y0 + chh / 2, f"{v:.1%}", ink, anchor="middle", size=12)
    ch.table = (["코호트", "사용자", "개월 뒤", "다시 온 사용자", "비율", "좌측 절단"],
                [(c, f"{size[c]:,}", m, f'{int(cell[(c, m)]["active_users"]):,}', f"{rate[(c, m)]:.1%}", "예" if cens[c] else "")
                 for c in cohorts for m in range(1, 7) if (c, m) in rate])
    return ch


# ── GCP ───────────────────────────────────────────────────────────
def c_breakdown(static):
    rows = read("job_breakdown_1105.csv")
    segs = [("in_stages_s", "계산(Spark 단계 안)", "s1f", "on1"), ("driver_only_s", "드라이버 혼자(계획·파일 목록)", "s2f", "on2"),
            ("outside_app_s", "앱 바깥(시작·제출·대기)", "s3f", "on3")]
    r = {(x["job"], x["env"]): x for x in rows}
    slow = [float(r[(j, "dataproc")]["in_stages_s"]) / float(r[(j, "local")]["in_stages_s"]) for j in ("load", "quality")]
    fixed = [float(r[(j, "dataproc")]["driver_only_s"]) + float(r[(j, "dataproc")]["outside_app_s"]) for j in ("load", "quality")]
    ch = Chart("breakdown", 220, "같은 하루치 잡의 시간 분해: 노트북 vs Dataproc (2019-11-05)",
               f"Dataproc에서는 계산도 {min(slow):.0f}~{max(slow):.0f}배 느리지만, 잡마다 계산 밖의 시간이 "
               f"{min(fixed):.0f}~{max(fixed):.0f}초씩 더 붙는다. 하루치가 작은 이 데이터에서는 클러스터의 고정 비용이 계산보다 크다",
               "Spark 이벤트 로그의 단계 시간과 Airflow 태스크 시간으로 나눴다. 노트북은 ADR 0005, Dataproc은 ADR 0008의 측정이다",
               ["job_breakdown_1105.csv"], static, l=132, r=64, t=40, b=30)
    ch.legend([(n, "rect", c) for _k, n, c, _o in segs], x=16)
    top = 120

    def x(v):
        return ch.l + ch.pw * v / top

    for t in range(0, top + 1, 30):
        ch.line(x(t), ch.t, x(t), ch.t + ch.ph, "axis" if t == 0 else "grid")
        ch.text(x(t), ch.t + ch.ph + 14, f"{t}초", "t3", anchor="middle", size=11)
    bh, names = 22, {"load": "적재", "quality": "품질 검사", "local": "노트북", "dataproc": "Dataproc"}
    for i, row in enumerate(rows):
        yc = ch.t + ch.ph * (i + 0.5) / len(rows)
        ch.text(ch.l - 10, yc, f"{names[row['job']]} · {names[row['env']]}", "t2", anchor="end", size=12)
        total, acc = sum(float(row[k]) for k, *_ in segs), 0.0
        for j, (k, n, c, on) in enumerate(segs):
            v = float(row[k])
            x0, x1 = x(acc) + (1 if j else 0), x(acc + v) - 1  # 조각 사이 2px 바탕 틈
            tip = [f"{names[row['job']]} · {names[row['env']]}", [f"{v:.1f}초", n, c[:2]], [f"{total:.1f}초", "합계"]]
            if j == len(segs) - 1 and x1 - x0 > 2:
                ch.bar(yc - bh / 2, x0, x1, bh, c, tip, horizontal=True)
            elif x1 - x0 > 0.5:
                ch.mark(f'<rect x="{x0:.1f}" y="{yc - bh / 2:.1f}" width="{x1 - x0:.1f}" height="{bh}" class="{c}"/>',
                        tip, (x0, yc - bh / 2 - 3, max(x1 - x0, 8), bh + 6))
            if x1 - x0 > 40:  # 칸 안에 들어갈 때만 숫자를 넣는다
                ch.text((x0 + x1) / 2, yc, f"{v:.1f}", on, anchor="middle", size=11.5, weight=600)
            acc += v
        ch.text(x(total) + 8, yc, f"{total:.1f}초", "t1", size=12, weight=600)
    ch.table = (["잡", "환경", "계산(단계 안)", "드라이버 혼자", "앱 바깥", "출처"],
                [(x["job"], x["env"], x["in_stages_s"], x["driver_only_s"], x["outside_app_s"], x["source"]) for x in rows])
    return ch


# ── 4단계 ─────────────────────────────────────────────────────────
def c_watermark(static):
    rows = read("watermark.csv")
    ch = Chart("watermark", 236, "워터마크 5·10·20분 비교 (같은 재생: 5%는 0~8분, 0.5%는 15~20분 늦게 보냄)",
               "길게 잡을수록 덜 버리지만 윈도를 상태에 더 오래 들고 있다. 그만큼 결과도 늦게 확정된다. "
               "워터마크보다 늦은 이벤트가 모두 버려지지는 않는다(Spark는 한 배치 전 워터마크로 거른다)",
               "같은 2시간 구간을 60배속으로 재생해 1분 퍼널을 세고, 배치 결과와 윈도마다 대조했다(ADR 0009)",
               ["watermark.csv"], static, l=0, r=0, t=58, b=30)
    ch.legend([("워터마크보다 늦게 보낸 이벤트", "rect", "softf"), ("실제로 버려진 이벤트", "rect", "strongf")], x=64, y=ch.t - 40)
    l, pw = 64, W / 2 - 88
    ch.text(l, ch.t - 16, "이벤트 수", "t2", size=12, weight=600)
    y = y_axis(ch, [0, 10000, 20000], lambda v: man(v) if v else "0", (l, ch.t, pw, ch.ph))
    for i, r in enumerate(rows):
        cx = l + pw * (i + 0.5) / len(rows)
        sent, drop, wm = int(r["sent_later_than_watermark"]), int(r["dropped_events"]), r["watermark_min"]
        tip = [f"워터마크 {wm}분", [f"{sent:,}건", "워터마크보다 늦게 보냄", "soft"], [f"{drop:,}건", "실제로 버려짐", "strong"]]
        ch.bar(cx - 23, y(0), y(sent), 22, "softf", tip)
        ch.bar(cx + 1, y(0), y(drop), 22, "strongf", tip)
        ch.text(cx - 12, y(sent) - 10, f"{sent:,}", "t2", anchor="middle", size=11)
        ch.text(cx + 27, min(y(drop), y(0) - 8), f"{drop:,}", "t1", size=11, weight=600)
        ch.text(cx, ch.t + ch.ph + 14, f"{wm}분", "t3", anchor="middle", size=11)
    l2 = W / 2 + 64
    ch.text(l2, ch.t - 16, "상태에 든 윈도(최대)", "t2", size=12, weight=600)
    y2 = y_axis(ch, [0, 15, 30], str, (l2, ch.t, pw, ch.ph))
    for i, r in enumerate(rows):
        cx, v = l2 + pw * (i + 0.5) / len(rows), int(r["state_windows_max"])
        ch.bar(cx - 12, y2(0), y2(v), 24, "s1f", [f"워터마크 {r['watermark_min']}분", [f"{v}개", "상태에 든 윈도(최대)"]])
        ch.text(cx, y2(v) - 10, str(v), "t1", anchor="middle", size=11.5, weight=600)
        ch.text(cx, ch.t + ch.ph + 14, f"{r['watermark_min']}분", "t3", anchor="middle", size=11)
    ch.table = (["워터마크(분)", "워터마크보다 늦게 보낸 이벤트", "실제로 버려진 이벤트", "상태에 든 윈도 최대"],
                [(r["watermark_min"], r["sent_later_than_watermark"], r["dropped_events"], r["state_windows_max"]) for r in rows])
    return ch


def c_stream(static):
    rows = read("stream_e1_windows.csv")
    miss = [int(r["batch_events"]) - int(r["stream_events"]) for r in rows]
    bound = [int(r["sent_later_than_watermark"]) for r in rows]
    ok = sum(0 <= a <= b for a, b in zip(miss, bound))
    n = len(rows)
    ch = Chart("stream", 250, f"실시간 1분 집계에서 빠진 이벤트 (실험 E1, 워터마크 10분, {n}개 윈도)",
               f"{ok}/{n}개 윈도에서 빠진 수(막대)가 '워터마크보다 늦게 보낸 수'(선)를 넘지 않는다. 이것이 대조 기준이다. "
               f"실제로 빠진 것은 늦게 보낸 {sum(bound):,}건 중 {sum(miss):,}건이다",
               "같은 구간을 배치로 다시 세고, 재생 계획을 같은 시드로 다시 세워 윈도마다 '버려질 수 있었던 이벤트' 수를 구했다(ADR 0009)",
               ["stream_e1_windows.csv"], static, t=40, b=40)
    ch.legend([("실시간에서 빠진 이벤트(배치 − 실시간)", "rect", "s1f"), ("워터마크보다 늦게 보낸 이벤트", "line", "s2l")])
    top = math.ceil(max(bound + miss) / 20) * 20
    y = y_axis(ch, list(range(0, top + 1, 20)), lambda v: str(int(v)))

    def x(i):
        return ch.l + ch.pw * (i + 0.5) / n

    bw = ch.pw / n - 1.5
    for i, v in enumerate(miss):
        if v > 0:
            ch.bar(x(i) - bw / 2, y(0), y(v), bw, "s1f")
    ch.parts.append(f'<polyline points="{" ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(bound))}" class="s2l"/>')
    for i in range(0, n + 1, 30):
        ch.text(ch.l + ch.pw * i / n, ch.t + ch.ph + 14, f"{8 + i // 60:02d}:{i % 60:02d}", "t3", anchor="middle", size=11)
    ch.text(ch.l + ch.pw, ch.t + ch.ph + 30, "원래 시각(2019-11-16, UTC)", "t3", anchor="end", size=11)
    ch.table = (["윈도(원래 시각)", "배치", "실시간", "빠진 수", "워터마크보다 늦게 보낸 수"],
                [(f"{8 + i // 60:02d}:{i % 60:02d}", r["batch_events"], r["stream_events"], m, b)
                 for i, (r, m, b) in enumerate(zip(rows, miss, bound))])
    ch.cross = {"x": [round(x(i), 1) for i in range(n)], "label": [f"{8 + i // 60:02d}:{i % 60:02d} 윈도" for i in range(n)],
                "series": [{"name": "빠진 이벤트", "values": [f"{v}건" for v in miss], "cls": "s1"},
                           {"name": "늦게 보낸 이벤트", "values": [f"{v}건" for v in bound], "cls": "s2"}]}
    return ch


CHARTS = [c_daily, c_concurrency, c_session_pr, c_scd2, c_funnel, c_cohort, c_breakdown, c_watermark, c_stream]
STAGES = [
    ("배치 적재와 품질 (1·2단계)", "일 단위 멱등 적재, 품질 검사 8개, 7개월 백필. 동시 실행 수는 재서 정했다.",
     ["daily", "concurrency"]),
    ("레이크하우스 모델링 (3단계)", "Iceberg 위에 세션, 상품 이력(SCD2), 퍼널·코호트 마트를 만들었다. 원천 데이터의 함정 세 가지를 숫자로 찾았다.",
     ["session_pr", "scd2", "funnel", "cohort"]),
    ("GCP로 옮기기", "같은 DAG를 Composer와 Dataproc으로 옮겨 213일을 돌렸다. 날짜별 행 수가 로컬과 모두 같았다.", ["breakdown"]),
    ("실시간 (4단계)", "Kafka로 이벤트를 다시 보내고 Spark Structured Streaming으로 1분 퍼널과 세션을 셌다. 결과는 배치와 윈도마다 대조했다.",
     ["watermark", "stream"]),
]
KPIS = [("410,325,314", "원천 이벤트 (213일)"), ("42분", "7개월 백필 (노트북, 동시 3)"),
        ("213 / 213", "GCP 결과가 로컬과 같은 날"), ("120 / 120", "강제 종료 뒤에도 배치와 같은 1분 윈도"),
        ("103,053", "배치와 똑같은 실시간 세션"), ("57", "자동 테스트")]

PAGE_CSS = """
:root{@LIGHT@;color-scheme:light}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){@DARK@;color-scheme:dark}}
:root[data-theme="dark"]{@DARK@;color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);font:15px/1.65 @FONT@}
main{max-width:880px;margin:0 auto;padding:40px 16px 64px}
a{color:var(--ink2)}
.eyebrow{margin:0;color:var(--muted);font-size:13px;font-weight:600}
h1{margin:4px 0 8px;font-size:28px;line-height:1.25}
.lede{margin:0;color:var(--ink2);max-width:62ch}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:28px 0 0;padding:0;list-style:none}
.kpis li{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px 14px 12px}
.kpis b{display:block;font-size:22px;line-height:1.2;font-variant-numeric:tabular-nums}
.kpis span{display:block;margin-top:4px;color:var(--ink2);font-size:12.5px;line-height:1.4}
.toc{display:flex;flex-wrap:wrap;gap:4px 16px;margin:20px 0 0;font-size:13.5px}
h2{margin:52px 0 4px;font-size:20px;line-height:1.35}
.stage{margin:0;color:var(--ink2);font-size:14px}
figure{margin:16px 0 0;background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:18px 18px 12px}
figure h3{margin:0;font-size:16.5px;line-height:1.4}
.sub{margin:4px 0 12px;color:var(--ink2);font-size:14px;line-height:1.55}
.plot{overflow-x:auto}
.plot svg{display:block;width:100%;min-width:600px;height:auto}
.plot svg text{pointer-events:none}
.note{margin:8px 0 0;color:var(--muted);font-size:12.5px;line-height:1.5}
details{margin-top:6px;font-size:13px}
summary{cursor:pointer;color:var(--ink2);padding:4px 0;width:max-content}
.tbl{max-height:300px;overflow:auto;border:1px solid var(--border);border-radius:8px;margin-top:6px}
table{border-collapse:collapse;width:100%;font-size:12.5px;font-variant-numeric:tabular-nums}
th,td{padding:5px 10px;text-align:right;border-bottom:1px solid var(--grid);white-space:nowrap}
th:first-child,td:first-child{text-align:left}
thead th{position:sticky;top:0;background:var(--surface);color:var(--ink2);font-weight:600}
.hit{outline:none}
.hit:hover>:last-child,.hit:focus-visible>:last-child{stroke:var(--ink);stroke-width:1.5px}
.xhair{stroke:var(--ink2);stroke-width:1;pointer-events:none}
#tip{position:fixed;left:0;top:0;z-index:10;pointer-events:none;background:var(--surface);color:var(--ink);
  border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,.14);padding:8px 10px;
  font-size:12.5px;line-height:1.45;max-width:280px}
#tip[hidden]{display:none}
#tip .h{font-weight:600;margin-bottom:2px}
#tip .r{display:flex;align-items:center;gap:6px}
#tip .r b{font-variant-numeric:tabular-nums}
#tip .r span{color:var(--ink2)}
#tip i{width:10px;height:3px;border-radius:2px;flex:none}
footer{margin-top:52px;color:var(--muted);font-size:12.5px}
@media (max-width:520px){h1{font-size:23px}figure{padding:14px 12px 10px}}
"""

JS = """
(() => {
  const tip = document.getElementById('tip');
  const fill = rows => {
    tip.replaceChildren();
    const h = document.createElement('div'); h.className = 'h'; h.textContent = rows[0]; tip.append(h);
    for (const [v, label, key] of rows.slice(1)) {
      const r = document.createElement('div'); r.className = 'r';
      if (key) { const i = document.createElement('i'); i.style.background = `var(--${key})`; r.append(i); }
      const b = document.createElement('b'); b.textContent = v;
      const s = document.createElement('span'); s.textContent = label;
      r.append(b, s); tip.append(r);
    }
    tip.hidden = false;
  };
  const place = (x, y) => {
    const w = tip.offsetWidth, h = tip.offsetHeight;
    let left = x + 14, top = y + 14;
    if (left + w > innerWidth - 8) left = x - w - 14;
    if (top + h > innerHeight - 8) top = y - h - 14;
    tip.style.transform = `translate(${Math.max(8, left)}px, ${Math.max(8, top)}px)`;
  };
  const hide = () => { tip.hidden = true; };
  // 막대·점·칸: 표시마다 툴팁
  const hitOf = e => e.target.closest && e.target.closest('.hit');
  document.addEventListener('pointerover', e => { const g = hitOf(e); if (g) { fill(JSON.parse(g.dataset.tip)); place(e.clientX, e.clientY); } });
  document.addEventListener('pointermove', e => { if (hitOf(e)) place(e.clientX, e.clientY); });
  document.addEventListener('pointerout', e => { if (hitOf(e)) hide(); });
  document.addEventListener('focusin', e => { const g = hitOf(e); if (g) { const b = g.getBoundingClientRect(); fill(JSON.parse(g.dataset.tip)); place(b.right, b.top); } });
  document.addEventListener('focusout', e => { if (hitOf(e)) hide(); });
  // 선 차트: 가장 가까운 날짜에 세로선과 모든 계열 값
  for (const fig of document.querySelectorAll('figure[data-cross]')) {
    const c = JSON.parse(fig.dataset.cross), svg = fig.querySelector('svg'), n = c.x.length;
    const ln = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    ln.setAttribute('class', 'xhair'); ln.setAttribute('y1', c.top); ln.setAttribute('y2', c.bottom); ln.style.display = 'none';
    svg.append(ln);
    const off = () => { ln.style.display = 'none'; hide(); };
    svg.addEventListener('pointermove', e => {
      const p = new DOMPoint(e.clientX, e.clientY).matrixTransform(svg.getScreenCTM().inverse());
      if (p.x < c.x[0] - 8 || p.x > c.x[n - 1] + 8 || p.y < c.top - 8 || p.y > c.bottom + 8) return off();
      let lo = 0, hi = n - 1;
      while (hi - lo > 1) { const m = (lo + hi) >> 1; if (c.x[m] < p.x) lo = m; else hi = m; }
      const i = p.x - c.x[lo] < c.x[hi] - p.x ? lo : hi;
      ln.setAttribute('x1', c.x[i]); ln.setAttribute('x2', c.x[i]); ln.style.display = '';
      fill([c.label[i], ...c.series.map(s => [s.values[i], s.name, s.cls])]); place(e.clientX, e.clientY);
    });
    svg.addEventListener('pointerleave', off);
  }
})();
"""


def figure(ch):
    cross = ""
    if ch.cross:
        cross = f' data-cross="{esc(json.dumps({**ch.cross, "top": ch.t, "bottom": ch.t + ch.ph}, ensure_ascii=False))}"'
    head, rows = ch.table
    table = ("<table><thead><tr>" + "".join(f"<th>{esc(h)}</th>" for h in head) + "</tr></thead><tbody>"
             + "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in rows) + "</tbody></table>")
    return (f'<figure id="{ch.key}"{cross}><h3>{esc(ch.title)}</h3><p class="sub">{esc(ch.subtitle)}</p>'
            f'<div class="plot">{ch.svg()}</div><p class="note">어떻게 셌나: {esc(ch.note)}</p>'
            f'<details><summary>표로 보기 ({len(rows)}행)</summary><div class="tbl">{table}</div></details></figure>')


def report(charts, fragment):
    by = {ch.key: ch for ch in charts}
    css = (PAGE_CSS.replace("@LIGHT@", ";".join(f"--{k}:{c}" for k, c in THEMES["light"].items()))
           .replace("@DARK@", ";".join(f"--{k}:{c}" for k, c in THEMES["dark"].items())).replace("@FONT@", FONT) + CLASSES)
    body = (
        '<main><p class="eyebrow">B2C 이벤트 로그 데이터 파이프라인</p><h1>숫자와 그림으로 보는 결과</h1>'
        '<p class="lede">REES46 이커머스 행동 로그 7개월치(2019-10 ~ 2020-04)로 만든 배치·레이크하우스·실시간 파이프라인의 '
        '측정값입니다. 그림마다 무엇을 보면 되는지와 숫자를 어떻게 셌는지를 함께 적었습니다. 표시에 마우스를 올리면 값이 보이고, '
        '"표로 보기"를 열면 그림의 숫자를 모두 볼 수 있습니다.</p>'
        f'<ul class="kpis">{"".join(f"<li><b>{esc(v)}</b><span>{esc(t)}</span></li>" for v, t in KPIS)}</ul>'
        f'<nav class="toc">{"".join(f"<a href=\"#st{i}\">{esc(s[0])}</a>" for i, s in enumerate(STAGES))}</nav>'
        + "".join(f'<section id="st{i}"><h2>{esc(name)}</h2><p class="stage">{esc(note)}</p>'
                  + "".join(figure(by[k]) for k in keys) + "</section>" for i, (name, note, keys) in enumerate(STAGES))
        + '<footer>데이터: REES46 eCommerce behavior data(재배포하지 않음). 그림은 저장소의 docs/portfolio/build_viz.py가 '
          'docs/portfolio/data/의 집계 CSV에서 만든다.</footer></main>'
          f'<div id="tip" role="tooltip" hidden></div><script>{JS}</script>')
    head = f"<title>이벤트 파이프라인 리포트</title><style>{css}</style>"
    if fragment:
        return head + body
    return (f'<!doctype html><html lang="ko"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">{head}</head><body>{body}</body></html>')


def picture(ch, prefix):
    return (f'<picture>\n  <source media="(prefers-color-scheme: dark)" srcset="{prefix}{ch.key}-dark.svg">\n'
            f'  <img alt="{esc(ch.title + ". " + ch.subtitle).replace("&#x27;", "'")}" src="{prefix}{ch.key}-light.svg" width="720">\n</picture>')


def charts_md(charts):
    by = {ch.key: ch for ch in charts}
    out = ["# 그림으로 보는 결과", "",
           "`python3 docs/portfolio/build_viz.py`가 [data/](data/)의 집계 CSV에서 다시 그린다. 원본 이벤트는 재배포하지 않는다.",
           "값을 짚어 보거나 표로 보려면 [report.html](report.html)을 내려받아 브라우저로 연다.", ""]
    for name, note, keys in STAGES:
        out += [f"## {name}", note, ""]
        for k in keys:
            ch = by[k]
            srcs = ", ".join(f"[{s}](data/{s})" for s in ch.src)
            out += [picture(ch, "img/"), "", f"어떻게 셌나: {ch.note} · 데이터: {srcs}", ""]
    return "\n".join(out)


def write(path, text):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


if __name__ == "__main__":
    assert ink_on("#0d366b") == "#ffffff" and ink_on("#cde2fb") == "#0b0b0b"
    assert wrap("가 나 다", 12, 30) == ["가 나", "다"]
    os.makedirs(IMG, exist_ok=True)
    for f in CHARTS:
        for mode in THEMES:
            ch = f(True)
            write(os.path.join(IMG, f"{ch.key}-{mode}.svg"), ch.svg(mode))
    live = [f(False) for f in CHARTS]
    write(os.path.join(HERE, "report.html"), report(live, fragment=False))
    write(os.path.join(HERE, "charts.md"), charts_md(live))
    if "--artifact" in sys.argv:
        write(sys.argv[sys.argv.index("--artifact") + 1], report(live, fragment=True))
    print(f"img/*.svg {len(CHARTS) * 2}개, charts.md, report.html")
