"""
RAAS 온톨로지 사용 예시 v5 (Phase 4.5: 캘린더/일자유형)

평일/주말/공휴일 패턴 차이를 인식하는 분석:
20. 특정 날짜의 일자 유형 자동 판정
21. 평일 vs 주말 vs 공휴일 청취 패턴 비교 (DAU)
22. 공휴일 청취 패턴 사례 — 5/1 근로자의 날
23. 같은 DayType끼리만 비교 (정확한 WoW)
24. 비즈니스 룰 v1 vs v2 비교
"""
import re
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from validate import parse_turtle
from raas_paths import get_csv_path, get_ontology_paths

ONTO_FILES = get_ontology_paths()
CSV_PATH = get_csv_path()


# ─── 통합 온톨로지 로더 ─────────────────────────────────────────────────────

class Ontology:
    def __init__(self, paths):
        self.triples = []
        for p in paths:
            self.triples.extend(parse_turtle(p))
        self.spo = defaultdict(lambda: defaultdict(list))
        self.types = defaultdict(list)
        for s, p, o in self.triples:
            self.spo[s][p].append(o)
            if p in ("a", "rdf:type"):
                self.types[o].append(s)

    def get(self, subject, predicate):
        return self.spo.get(subject, {}).get(predicate, [])

    def get_one(self, subject, predicate, default=None):
        vals = self.get(subject, predicate)
        return vals[0] if vals else default

    def instances_of(self, cls):
        return self.types.get(cls, [])

    def label_ko(self, subject):
        labels = self.get(subject, "rdfs:label")
        for lbl in labels:
            if "@ko" in lbl:
                return re.sub(r'^"|"@ko$', '', lbl)
        return labels[0] if labels else subject

    def value_str(self, raw):
        if not raw:
            return ""
        return re.sub(r'^"|"(?:@\w+)?$', '', raw)


# ─── DayType 판정 헬퍼 ─────────────────────────────────────────────────────

def get_day_type(onto, date_str):
    """YYYY/MM/DD 또는 YYYY-MM-DD → (DayType, weekday_kr, holiday_name)"""
    # 정규화
    norm_date = date_str.replace("/", "-")
    try:
        dt = datetime.strptime(norm_date, "%Y-%m-%d")
    except ValueError:
        return None, None, None

    weekday = ["월","화","수","목","금","토","일"][dt.weekday()]

    # 온톨로지에서 공휴일 인스턴스 찾기
    for cd in onto.instances_of("raas:CalendarDay"):
        cd_date = onto.value_str(onto.get_one(cd, "raas:dateValue"))
        if cd_date == norm_date:
            day_type = onto.get_one(cd, "raas:dayType")
            holiday_name = onto.value_str(onto.get_one(cd, "raas:holidayName"))
            return day_type, weekday, holiday_name

    # 공휴일 인스턴스가 없으면 요일로 판정
    if dt.weekday() < 5:
        return "raas:Weekday", weekday, None
    else:
        return "raas:Weekend", weekday, None


# ─── CSV 로더: 모든 일자의 KPI ─────────────────────────────────────────────

def load_all_data(target_codes=None):
    """모든 (date, code) → KPI dict 반환."""
    if CSV_PATH is None or not CSV_PATH.exists():
        return {}

    result = defaultdict(dict)  # (date, code) → {field: value}
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            date = row[header.index("DATE")]
            code = row[header.index("PGM_CODE")]
            if target_codes and code not in target_codes:
                continue
            if not date or not code:
                continue
            kpi = {}
            for i, col in enumerate(header):
                val = row[i]
                if val and val not in ("None", "null"):
                    try:
                        kpi[col] = float(val)
                    except ValueError:
                        kpi[col] = val
            result[(date, code)] = kpi
    return result


# ─── 데모들 ─────────────────────────────────────────────────────────────────

def demo20_classify_dates(onto):
    """특정 날짜들의 일자 유형 자동 판정"""
    print(f"\n[데모 20] 일자 유형 자동 판정")
    print("-" * 60)

    test_dates = [
        ("2026-04-30", "평일 목요일"),
        ("2026-05-01", "근로자의 날 (금)"),
        ("2026-05-02", "토요일"),
        ("2026-05-04", "평일 월요일"),
        ("2026-05-05", "어린이날 (화)"),
        ("2026-05-25", "대체공휴일 (월)"),
    ]

    for date, expected in test_dates:
        dt, wd, holiday = get_day_type(onto, date)
        dt_label = onto.label_ko(dt) if dt else "?"
        holiday_str = f" — {holiday}" if holiday else ""
        print(f"  {date} ({wd}) → {dt_label}{holiday_str}")
        print(f"    예상: {expected}")


def demo21_pattern_comparison(onto):
    """평일 vs 주말 vs 공휴일 청취 패턴 비교"""
    print(f"\n[데모 21] 평일 vs 주말 vs 공휴일 청취 패턴 비교 (F09 컬투쇼)")
    print("-" * 60)

    data = load_all_data(target_codes=["F09"])
    if not data:
        print(f"  [건너뜀] CSV 데이터 없음")
        return

    # DayType별로 DAU 분류
    by_daytype = defaultdict(list)
    for (date, code), kpi in data.items():
        dt, wd, _ = get_day_type(onto, date)
        if dt and "dau" in kpi:
            by_daytype[dt].append((date, kpi["dau"]))

    # 출력
    order = ["raas:Weekday", "raas:Weekend", "raas:HolidayDay", "raas:SubstituteDay"]
    print(f"  {'일자유형':12s} {'일수':>4s}  {'평균 DAU':>10s}  {'최소':>8s}  {'최대':>8s}")
    print(f"  {'-'*12} {'-'*4}  {'-'*10}  {'-'*8}  {'-'*8}")
    base_avg = None
    for dt in order:
        rows = by_daytype.get(dt, [])
        if not rows:
            continue
        daus = [d for _, d in rows]
        avg = sum(daus) / len(daus)
        if base_avg is None:
            base_avg = avg
        diff_pct = (avg - base_avg) / base_avg * 100 if base_avg else 0
        label = onto.label_ko(dt)
        print(f"  {label:12s} {len(rows):>4d}  {avg:>10,.0f}  {min(daus):>8,.0f}  {max(daus):>8,.0f}  ({diff_pct:+.1f}% vs 평일)")


def demo22_holiday_case_study(onto):
    """공휴일이 청취에 미치는 영향 사례"""
    print(f"\n[데모 22] 공휴일 효과 사례 — 5/1 근로자의 날 (금)")
    print("-" * 60)

    # 평일 금요일 vs 5/1 비교
    data = load_all_data(target_codes=["F05", "F09"])  # 김영철의 파워FM, 컬투쇼
    if not data:
        print(f"  [건너뜀] CSV 데이터 없음")
        return

    for code in ["F05", "F09"]:
        print(f"\n  [{code}]")
        # 모든 금요일 DAU 추출
        friday_daus = []
        for (date, c), kpi in data.items():
            if c != code or "dau" not in kpi:
                continue
            dt, wd, holiday = get_day_type(onto, date)
            if wd == "금":
                friday_daus.append((date, kpi["dau"], dt, holiday))

        friday_daus.sort()
        for date, dau, dt, holiday in friday_daus[-6:]:
            dt_label = onto.label_ko(dt) if dt else "?"
            note = f" ⚠ {holiday}" if holiday else ""
            print(f"    {date} (금)  DAU {dau:>7,.0f}  [{dt_label}]{note}")


def demo23_same_daytype_comparison(onto):
    """같은 DayType끼리만 비교 (정확한 WoW)"""
    print(f"\n[데모 23] 같은 DayType끼리 비교 (정확한 WoW)")
    print("-" * 60)
    print(f"  목표: 토요일 5/2 데이터를 어떤 날과 비교해야 정확한가?")
    print()

    data = load_all_data(target_codes=["F09"])
    if not data:
        print(f"  [건너뜀] CSV 데이터 없음")
        return

    # 5/2 토요일의 KPI
    target = ("2026/05/02", "F09")
    if target not in data:
        print(f"  [건너뜀] 5/2 데이터 없음")
        return

    target_dau = data[target].get("dau")

    print(f"  기준일: 2026/05/02 (토) — F09 컬투쇼 DAU {target_dau:,.0f}\n")

    # 단순 7일 전 비교 (4/25 토요일)
    prev_7d = ("2026/04/25", "F09")
    if prev_7d in data:
        prev_dau = data[prev_7d].get("dau")
        if prev_dau:
            wow = (target_dau - prev_dau) / prev_dau * 100
            dt_p, wd_p, _ = get_day_type(onto, "2026/04/25")
            print(f"  [단순 7일 전 비교]")
            print(f"    2026/04/25 ({wd_p}) DAU {prev_dau:,.0f} → WoW {wow:+.1f}%")
            print(f"    같은 DayType ({onto.label_ko(dt_p)})이라 비교 의미 있음 ✓")

    # 부정확 비교 (직전일 5/1)
    prev_1d = ("2026/05/01", "F09")
    if prev_1d in data:
        prev_dau = data[prev_1d].get("dau")
        if prev_dau:
            d1 = (target_dau - prev_dau) / prev_dau * 100
            dt_p, wd_p, holiday = get_day_type(onto, "2026/05/01")
            print(f"\n  [직전일 비교 — 부정확]")
            print(f"    2026/05/01 ({wd_p}) DAU {prev_dau:,.0f} → DoD {d1:+.1f}%")
            print(f"    하지만 5/1은 {onto.label_ko(dt_p)}({holiday}), 5/2는 주말")
            print(f"    DayType 다름 → 비교 부정확 ✗")

    # 평일 비교 (4/30)
    target_wd = ("2026/04/30", "F09")
    if target_wd in data:
        wd_dau = data[target_wd].get("dau")
        if wd_dau:
            d2 = (target_dau - wd_dau) / wd_dau * 100
            dt_p, wd_p, _ = get_day_type(onto, "2026/04/30")
            print(f"\n  [평일과 비교 — 매우 부정확]")
            print(f"    2026/04/30 ({wd_p}) DAU {wd_dau:,.0f} → 비교값 {d2:+.1f}%")
            print(f"    {onto.label_ko(dt_p)} vs 주말 → DayType 완전히 다름 ✗")
            print(f"    이런 비교가 현재 RAAS에서 발생할 수 있음")


def demo24_rule_v1_vs_v2(onto):
    """비즈니스 룰 v1 vs v2 비교"""
    print(f"\n[데모 24] AtRiskProgram v1 vs v2 비교")
    print("-" * 60)

    for cls in ["raas:AtRiskProgram_v1", "raas:AtRiskProgram_v2"]:
        label = onto.label_ko(cls)
        cond = onto.value_str(onto.get_one(cls, "raas:condition"))
        status = onto.value_str(onto.get_one(cls, "raas:status"))
        limitation = onto.value_str(onto.get_one(cls, "raas:limitation"))
        action = onto.value_str(onto.get_one(cls, "raas:recommendedAction"))
        requires = onto.value_str(onto.get_one(cls, "raas:requiresField"))

        print(f"\n  [{cls.replace('raas:', '')}] ({status})")
        print(f"    이름: {label}")
        print(f"    조건: {cond}")
        if limitation:
            print(f"    한계: {limitation}")
        if requires:
            print(f"    필요 필드: {requires}")
        if action:
            print(f"    조치: {action}")

    print(f"\n  → v1은 현재 동작하지만 부정확. v2는 SPL 수정 후 활성화.")
    print(f"    온톨로지가 '무엇을 개선해야 하는지' 명시 → 향후 작업 가이드")


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("RAAS 온톨로지 사용 예시 v5 (Phase 4.5: 캘린더/일자유형)")
    print("=" * 70)

    onto = Ontology(ONTO_FILES)
    print(f"\n온톨로지 로드: {len(onto.triples)}개 트리플")

    demo20_classify_dates(onto)
    demo21_pattern_comparison(onto)
    demo22_holiday_case_study(onto)
    demo23_same_daytype_comparison(onto)
    demo24_rule_v1_vs_v2(onto)

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
