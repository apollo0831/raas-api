"""
RAAS 온톨로지 통합 검증 v3
- raas_kpi_ontology.ttl (Phase 2-A + 2-B)
- raas_domain_entities.ttl (Phase 2-C-1)
두 파일을 모두 로드해서 통합 검증.
"""
import re
import csv
from pathlib import Path
from collections import defaultdict
from validate import parse_turtle  # 기존 파서 재사용
from raas_paths import get_csv_path, get_ontology_paths

ONTO_FILES = get_ontology_paths()
CSV_PATH = get_csv_path()


def main():
    print("=" * 70)
    print("RAAS 온톨로지 통합 검증 (Phase 2-A + 2-B + 2-C-1)")
    print("=" * 70)

    # 모든 파일 로드
    all_triples = []
    for f in ONTO_FILES:
        if not f.exists():
            print(f"[오류] 파일 없음: {f}")
            return
        ts = parse_turtle(f)
        print(f"  {f.name}: {len(ts)}개 트리플")
        all_triples.extend(ts)
    print(f"\n[1] 총 트리플: {len(all_triples)}개")

    # 인스턴스 분류
    instances_by_class = defaultdict(list)
    spo = defaultdict(lambda: defaultdict(list))
    for s, p, o in all_triples:
        spo[s][p].append(o)
        if p in ("a", "rdf:type"):
            instances_by_class[o].append(s)

    # ─── [2] 핵심 클래스 인스턴스 카운트 ───
    print(f"\n[2] 핵심 클래스 인스턴스 카운트")
    expected = {
        "raas:Metric":           (11, "베이스 metric"),
        "raas:MetricVariant":    (40, "metric 변형"),
        "raas:Platform":         (1,  "플랫폼"),
        "raas:Channel":          (4,  "채널"),
        "raas:Program":          (31, "프로그램 (L04 결번 제외)"),
        "raas:TimeSlot":         (31, "방송 시간대"),
        "raas:Daypart":          (7,  "광고 시간대 분류"),
        "raas:ScheduleType":     (4,  "편성 유형"),
        "raas:MeasureType":      (2,  "측정 종류"),
        "raas:CalendarGranularity": (3, "캘린더 시간 단위"),
        "raas:RollingGranularity":  (2, "롤링 시간 단위"),
        "raas:Variant":          (6,  "변형"),
        "raas:Cohort":           (2,  "코호트"),
        "raas:RetentionWindow":  (4,  "유지율 윈도우"),
        "raas:PlatformAlert":    (9,  "플랫폼 알림 룰"),
        "raas:SparqlQuery":      (2,  "SPARQL 쿼리"),
        "raas:DayOfWeek":        (7,  "요일"),
        "raas:DayType":          (4,  "일자 유형"),
        "raas:HolidayType":      (3,  "공휴일 분류"),
        "raas:CalendarDay":      (20, "2026년 한국 공휴일"),
    }
    pass_count = 0
    for cls, (exp, desc) in expected.items():
        actual = len(instances_by_class.get(cls, []))
        mark = "PASS" if actual == exp else "WARN"
        if mark == "PASS":
            pass_count += 1
        print(f"  [{mark}] {cls:30s} 기대 {exp:3d} / 실제 {actual:3d}  ({desc})")
    print(f"\n  → {pass_count}/{len(expected)} PASS")

    # ─── [3] CSV의 PGM_CODE와 온톨로지 매칭 ───
    print(f"\n[3] CSV의 모든 PGM_CODE가 온톨로지에 정의되어 있는가")
    if CSV_PATH is None or not CSV_PATH.exists():
        print(f"  [건너뜀] CSV 없음 (raas_kpi_latest.csv를 같은 폴더 또는 ../data/에 두세요)")
    else:
        pgm_codes_in_csv = set()
        with open(CSV_PATH, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            pgm_idx = header.index("PGM_CODE")
            for row in reader:
                if row[pgm_idx]:
                    pgm_codes_in_csv.add(row[pgm_idx])

        # 온톨로지에 정의된 코드 (Platform, Channel, Program 모두)
        defined_codes = set()
        for cls in ("raas:Platform", "raas:Channel", "raas:Program"):
            for inst in instances_by_class.get(cls, []):
                # raas:F09 같은 prefix 형식에서 코드 추출
                code = inst.replace("raas:", "")
                defined_codes.add(code)

        only_csv = pgm_codes_in_csv - defined_codes
        only_onto = defined_codes - pgm_codes_in_csv

        print(f"  CSV의 PGM_CODE 종류: {len(pgm_codes_in_csv)}개")
        print(f"  온톨로지에 정의된 코드: {len(defined_codes)}개")
        if only_csv:
            print(f"\n  [WARN] CSV에만 있는 코드 (온톨로지 누락): {sorted(only_csv)}")
        if only_onto:
            print(f"\n  [INFO] 온톨로지에만 있는 코드: {sorted(only_onto)}")
        if not only_csv and not only_onto:
            print(f"  [PASS] 완전히 일치")

    # ─── [4] 채널-프로그램 관계 검증 ───
    print(f"\n[4] 채널 소속 프로그램 카운트")
    channel_programs = defaultdict(list)
    for s, p, o in all_triples:
        if p == "raas:belongsToChannel":
            channel_programs[o].append(s)
    for ch in ("raas:F00", "raas:L00", "raas:G00", "raas:P00"):
        progs = channel_programs.get(ch, [])
        ch_label = spo.get(ch, {}).get("rdfs:label", ["?"])[0]
        ch_label = re.sub(r'^"|"@\w+$', '', ch_label)
        print(f"  {ch} ({ch_label}): {len(progs)}개 프로그램")
        for p in sorted(progs):
            p_label = spo.get(p, {}).get("rdfs:label", ["?"])[0]
            p_label = re.sub(r'^"|"@\w+$', '', p_label)
            print(f"    {p}: {p_label}")

    # ─── [5] altLabel 통계 ───
    print(f"\n[5] altLabel 통계 (별칭/약어)")
    alt_count = defaultdict(int)
    for s, p, o in all_triples:
        if p == "raas:altLabel":
            alt_count[s] += 1
    total_alt = sum(alt_count.values())
    pgms_with_alt = sum(1 for s in alt_count if s.startswith("raas:F") or s.startswith("raas:L") or s.startswith("raas:M"))
    print(f"  총 altLabel 트리플: {total_alt}개")
    print(f"  altLabel을 가진 Program: {pgms_with_alt}개")
    print(f"\n  [샘플 — Top 5 별칭 보유]")
    top5 = sorted(alt_count.items(), key=lambda x: -x[1])[:5]
    for s, n in top5:
        labels = spo.get(s, {}).get("rdfs:label", [])
        label_str = re.sub(r'^"|"@\w+$', '', labels[0]) if labels else s
        alts = []
        for trip_s, trip_p, trip_o in all_triples:
            if trip_s == s and trip_p == "raas:altLabel":
                clean = re.sub(r'^"|"(?:@\w+)?$', '', trip_o)
                alts.append(clean)
        print(f"    {s} ({label_str}): {n}개")
        print(f"      → {', '.join(alts)}")

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
