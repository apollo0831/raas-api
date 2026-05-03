"""
RAAS 온톨로지 사용 예시 v3 (Phase 2-C-2 추가)

시간 정보(TimeSlot, Daypart, ScheduleType)를 활용한 데모:
10. 특정 시간대의 모든 프로그램 조회 (광고 영업 활용)
11. Daypart별 청취 데이터 집계 (광고 단가 연구 활용)
12. 같은 시간대의 채널 간 경쟁 프로그램 비교
13. 주말 전용 프로그램의 특성 분석
14. Daypart별 광고 가치 등급 매핑 (PoC)
"""
import re
import csv
from pathlib import Path
from collections import defaultdict
from validate import parse_turtle
from raas_paths import get_csv_path, get_ontology_paths

ONTO_FILES = get_ontology_paths()
CSV_PATH = get_csv_path()


# ─── 통합 온톨로지 로더 (이전과 동일) ───────────────────────────────────────

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

    def find_subjects(self, predicate, obj):
        return [s for s, p, o in self.triples if p == predicate and o == obj]

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


# ─── CSV 데이터 로더 ───────────────────────────────────────────────────────

def load_latest_dau(target_codes):
    """주어진 PGM_CODE들의 최신 DAU 조회. {code: dau} 반환."""
    if CSV_PATH is None or not CSV_PATH.exists():
        return {}

    result = {}
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        date_idx = header.index("DATE")
        code_idx = header.index("PGM_CODE")
        dau_idx = header.index("dau")

        rows_by_code = defaultdict(list)
        for row in reader:
            code = row[code_idx]
            if code not in target_codes:
                continue
            if row[dau_idx]:
                rows_by_code[code].append((row[date_idx], row[dau_idx]))

    for code, rows in rows_by_code.items():
        rows.sort(key=lambda x: x[0], reverse=True)
        if rows:
            try:
                result[code] = int(float(rows[0][1]))
            except (ValueError, TypeError):
                pass
    return result


# ─── 새 데모들 ─────────────────────────────────────────────────────────────

def demo10_programs_at_time(onto, target_hour):
    """특정 시간(시 단위)에 방송되는 모든 프로그램 조회"""
    print(f"\n[데모 10] {target_hour}시에 방송되는 프로그램")
    print("-" * 60)

    timeslots = onto.instances_of("raas:TimeSlot")
    matches = []
    for ts in timeslots:
        start = onto.value_str(onto.get_one(ts, "raas:startTime"))
        end = onto.value_str(onto.get_one(ts, "raas:endTime"))
        if not start or not end:
            continue

        # 시작/종료에서 시간만 추출
        try:
            sh = int(start.split(":")[0])
            eh = int(end.split(":")[0])
        except (ValueError, IndexError):
            continue

        # 자정 넘는 경우 처리 (예: 23:00~01:00)
        is_in_range = False
        if sh < eh:
            is_in_range = sh <= target_hour < eh
        else:
            is_in_range = target_hour >= sh or target_hour < eh

        if is_in_range:
            programs = onto.find_subjects("raas:hasTimeSlot", ts)
            for prog in programs:
                matches.append((prog, ts, start, end))

    if not matches:
        print(f"  {target_hour}시에 방송되는 프로그램 없음")
        return

    print(f"  {target_hour}시 방송 프로그램 {len(matches)}개:")
    for prog, ts, start, end in matches:
        prog_label = onto.label_ko(prog)
        channel = onto.get_one(prog, "raas:belongsToChannel")
        ch_label = onto.label_ko(channel) if channel else "?"
        schedule = onto.get_one(prog, "raas:hasScheduleType")
        sch_label = onto.label_ko(schedule) if schedule else "?"
        daypart = onto.get_one(ts, "raas:hasDaypart")
        dp_label = onto.label_ko(daypart) if daypart else "?"
        print(f"    [{ch_label}] {start}~{end} | {prog_label}")
        print(f"      편성: {sch_label} | Daypart: {dp_label}")


def demo11_daypart_aggregation(onto):
    """Daypart별 청취 데이터 집계 — 광고 분석의 핵심"""
    print(f"\n[데모 11] Daypart별 청취 집계 (광고 가치 분석)")
    print("-" * 60)

    # Daypart → 프로그램 코드 목록
    daypart_to_programs = defaultdict(list)
    timeslots = onto.instances_of("raas:TimeSlot")
    for ts in timeslots:
        daypart = onto.get_one(ts, "raas:hasDaypart")
        if not daypart:
            continue
        progs = onto.find_subjects("raas:hasTimeSlot", ts)
        for p in progs:
            code = onto.value_str(onto.get_one(p, "raas:code"))
            if code:
                daypart_to_programs[daypart].append(code)

    # 각 daypart의 프로그램들의 DAU 합산
    all_codes = set()
    for codes in daypart_to_programs.values():
        all_codes.update(codes)
    dau_data = load_latest_dau(all_codes)

    # Daypart 순서대로 정렬 (시간순)
    daypart_order = []
    for dp in onto.instances_of("raas:Daypart"):
        h = onto.value_str(onto.get_one(dp, "raas:hourStart"))
        try:
            daypart_order.append((int(h), dp))
        except (ValueError, TypeError):
            pass
    daypart_order.sort()

    print(f"  {'Daypart':22s} {'시간':12s} {'광고가치':10s} {'프로그램수':>8s}  {'합계 DAU':>12s}")
    print(f"  {'-'*22} {'-'*12} {'-'*10} {'-'*8}  {'-'*12}")
    for _, dp in daypart_order:
        dp_label = onto.label_ko(dp)
        h_start = onto.value_str(onto.get_one(dp, "raas:hourStart"))
        h_end = onto.value_str(onto.get_one(dp, "raas:hourEnd"))
        ad_value = onto.value_str(onto.get_one(dp, "raas:adValueLevel"))

        codes = daypart_to_programs.get(dp, [])
        total_dau = sum(dau_data.get(c, 0) for c in codes)

        print(f"  {dp_label:22s} {h_start}-{h_end:>3s}     {ad_value:10s} {len(codes):>8d}  {total_dau:>12,}")

    print(f"\n  → 광고 단가가 가장 높은 Drive 시간대(Morning + Afternoon)의 청취자 합계")
    drive_total = 0
    for _, dp in daypart_order:
        dp_label = onto.label_ko(dp)
        if "출근" in dp_label or "퇴근" in dp_label:
            for c in daypart_to_programs.get(dp, []):
                drive_total += dau_data.get(c, 0)
    print(f"     {drive_total:,}명")


def demo12_competing_programs(onto, target_hour):
    """같은 시간대의 채널 간 경쟁 프로그램 비교"""
    print(f"\n[데모 12] {target_hour}시 채널 간 경쟁 프로그램")
    print("-" * 60)

    # 그 시간대 프로그램 찾기
    timeslots = onto.instances_of("raas:TimeSlot")
    competing_progs = []
    for ts in timeslots:
        start = onto.value_str(onto.get_one(ts, "raas:startTime"))
        end = onto.value_str(onto.get_one(ts, "raas:endTime"))
        try:
            sh = int(start.split(":")[0])
            eh = int(end.split(":")[0])
        except:
            continue

        is_in_range = (sh <= target_hour < eh) if sh < eh else (target_hour >= sh or target_hour < eh)
        if is_in_range:
            for p in onto.find_subjects("raas:hasTimeSlot", ts):
                competing_progs.append(p)

    if len(competing_progs) < 2:
        print(f"  경쟁 프로그램 부족 (1개 이하)")
        return

    # DAU 조회
    codes = [onto.value_str(onto.get_one(p, "raas:code")) for p in competing_progs]
    dau_data = load_latest_dau(set(codes))

    # 채널별 정리, DAU 내림차순
    rows = []
    for p in competing_progs:
        code = onto.value_str(onto.get_one(p, "raas:code"))
        label = onto.label_ko(p)
        ch = onto.get_one(p, "raas:belongsToChannel")
        ch_label = onto.label_ko(ch) if ch else "?"
        sch = onto.get_one(p, "raas:hasScheduleType")
        sch_label = onto.label_ko(sch) if sch else "?"
        dau = dau_data.get(code, 0)
        rows.append((dau, ch_label, code, label, sch_label))

    rows.sort(reverse=True)

    print(f"  {'채널':10s} {'코드':6s} {'프로그램':25s} {'편성':10s}  {'DAU':>8s}")
    print(f"  {'-'*10} {'-'*6} {'-'*25} {'-'*10}  {'-'*8}")
    for dau, ch, code, label, sch in rows:
        print(f"  {ch:10s} {code:6s} {label[:25]:25s} {sch:10s}  {dau:>8,}")


def demo13_weekend_only_analysis(onto):
    """주말 전용 프로그램의 특성 분석"""
    print(f"\n[데모 13] 주말 전용 프로그램 특성 분석")
    print("-" * 60)

    weekend_progs = onto.find_subjects("raas:hasScheduleType", "raas:WeekendOnly")
    if not weekend_progs:
        print(f"  주말 전용 프로그램 없음")
        return

    codes = [onto.value_str(onto.get_one(p, "raas:code")) for p in weekend_progs]
    dau_data = load_latest_dau(set(codes))

    print(f"  주말 전용 프로그램 {len(weekend_progs)}개:")
    total_dau = 0
    for p in weekend_progs:
        code = onto.value_str(onto.get_one(p, "raas:code"))
        label = onto.label_ko(p)
        ts = onto.get_one(p, "raas:hasTimeSlot")
        start = onto.value_str(onto.get_one(ts, "raas:startTime")) if ts else "?"
        end = onto.value_str(onto.get_one(ts, "raas:endTime")) if ts else "?"
        daypart = onto.get_one(ts, "raas:hasDaypart") if ts else None
        dp_label = onto.label_ko(daypart) if daypart else "?"
        dau = dau_data.get(code, 0)
        total_dau += dau
        print(f"    {code} {start}~{end} [{dp_label}] {label}: DAU {dau:,}")

    print(f"\n  주말 전용 합계 DAU: {total_dau:,}명")


def demo14_ad_value_mapping(onto):
    """Daypart별 광고 가치 등급 매핑 — 광고 영업 직접 활용"""
    print(f"\n[데모 14] Daypart별 광고 가치 등급 + 청취자 규모")
    print("-" * 60)
    print(f"  → 광고 영업 시 단가 산정의 기초 데이터")
    print()

    # 광고 가치 등급별 daypart 분류
    by_ad_value = defaultdict(list)
    for dp in onto.instances_of("raas:Daypart"):
        ad_value = onto.value_str(onto.get_one(dp, "raas:adValueLevel"))
        by_ad_value[ad_value].append(dp)

    # 등급 순서
    order = ["premium", "high", "medium", "low"]
    grade_label = {
        "premium": "최고급 (Premium)",
        "high":    "고급 (High)",
        "medium":  "중급 (Medium)",
        "low":     "저급 (Low)",
    }

    # daypart → 프로그램 매핑
    daypart_to_codes = defaultdict(list)
    for ts in onto.instances_of("raas:TimeSlot"):
        dp = onto.get_one(ts, "raas:hasDaypart")
        for p in onto.find_subjects("raas:hasTimeSlot", ts):
            code = onto.value_str(onto.get_one(p, "raas:code"))
            if dp and code:
                daypart_to_codes[dp].append(code)

    all_codes = set()
    for codes in daypart_to_codes.values():
        all_codes.update(codes)
    dau_data = load_latest_dau(all_codes)

    for grade in order:
        if grade not in by_ad_value:
            continue
        dps = by_ad_value[grade]
        print(f"  [{grade_label.get(grade, grade)}]")
        for dp in dps:
            dp_label = onto.label_ko(dp)
            h_start = onto.value_str(onto.get_one(dp, "raas:hourStart"))
            h_end = onto.value_str(onto.get_one(dp, "raas:hourEnd"))
            codes = daypart_to_codes.get(dp, [])
            total_dau = sum(dau_data.get(c, 0) for c in codes)
            print(f"    {dp_label} ({h_start}-{h_end}시): "
                  f"{len(codes)}개 프로그램, 총 {total_dau:,} DAU")
        print()


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("RAAS 온톨로지 사용 예시 v3 (Phase 2-C-2: 시간 정보 활용)")
    print("=" * 70)

    onto = Ontology(ONTO_FILES)
    print(f"\n온톨로지 로드: {len(onto.triples)}개 트리플")

    demo10_programs_at_time(onto, 8)   # 출근 시간
    demo10_programs_at_time(onto, 15)  # 컬투쇼 시간

    demo11_daypart_aggregation(onto)

    demo12_competing_programs(onto, 14)  # 컬투쇼 vs 정엽
    demo12_competing_programs(onto, 7)   # 김영철 vs 김태현 정치쇼

    demo13_weekend_only_analysis(onto)

    demo14_ad_value_mapping(onto)

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
