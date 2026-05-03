"""
RAAS KPI 온톨로지 사용 예시

이 스크립트는 온톨로지를 실제로 어떻게 활용할 수 있는지 보여줍니다.
RAAS 코드(briefing_engine.py 등)가 이런 식으로 온톨로지를 참조하면,
지금 코드에 흩어진 메타지식이 단일 출처로 통합됩니다.

데모 시나리오:
1. 필드명으로 metric 정보 조회 (현재 코드의 field_map 대체)
2. metric의 모든 시간 단위 변형 조회
3. 자연어 별칭으로 metric 검색 (현재 KEYWORD_TO_CODE 대체)
4. 파생 metric 추적 (depth-first)
5. CSV row → Measurement 객체로 변환
"""
import re
import csv
from pathlib import Path
from collections import defaultdict
from validate import parse_turtle  # 검증 스크립트의 파서 재사용
from raas_paths import get_csv_path

ONTO_PATH = Path(__file__).parent / "raas_kpi_ontology.ttl"
CSV_PATH = get_csv_path()


# ─── 온톨로지 로더 ──────────────────────────────────────────────────────────

class Ontology:
    """Turtle 파일을 로드해 조회 가능한 형태로 만드는 어댑터."""

    def __init__(self, path):
        self.triples = parse_turtle(path)

        # 인덱스 구축
        self.spo = defaultdict(lambda: defaultdict(list))  # subject → pred → [obj]
        self.types = defaultdict(list)                      # class → [instance]
        for s, p, o in self.triples:
            self.spo[s][p].append(o)
            if p in ("a", "rdf:type"):
                self.types[o].append(s)

    def get(self, subject, predicate):
        """특정 주어의 특정 술어 값 (단일 또는 리스트)."""
        return self.spo.get(subject, {}).get(predicate, [])

    def get_one(self, subject, predicate, default=None):
        vals = self.get(subject, predicate)
        return vals[0] if vals else default

    def instances_of(self, cls):
        """클래스의 모든 인스턴스."""
        return self.types.get(cls, [])

    def find_subjects(self, predicate, obj):
        """술어=목적어 조건의 모든 주어."""
        return [s for s, p, o in self.triples if p == predicate and o == obj]

    def label_ko(self, subject):
        """한글 레이블 추출."""
        labels = self.get(subject, "rdfs:label")
        for lbl in labels:
            if "@ko" in lbl:
                return re.sub(r'^"|"@ko$', '', lbl)
        return labels[0] if labels else subject

    def value_str(self, raw):
        """문자열 리터럴에서 따옴표/언어 태그 제거."""
        if not raw:
            return ""
        return re.sub(r'^"|"(?:@\w+)?$', '', raw)


# ─── 데모 함수들 ────────────────────────────────────────────────────────────

def demo1_field_to_metric(onto, field_name):
    """필드명 → metric 정보 (현재 코드의 field_map 대체)"""
    print(f"\n[데모 1] 필드명 '{field_name}' 정보 조회")
    print("-" * 60)

    # 모든 정의된 fieldName 추출 (긴 것부터 시도해 정확 매칭)
    all_field_names = []
    for s, p, o in onto.triples:
        if p == "raas:fieldName":
            fn_clean = re.sub(r'^"|"(?:@\w+)?$', '', o)
            all_field_names.append(fn_clean)
    all_field_names.sort(key=len, reverse=True)  # 긴 것 우선

    # 가장 긴 매칭 fieldName 찾기 (예: dau_week_chg → dau_week)
    base_fn = None
    variant_suffix = ""
    for fn in all_field_names:
        if field_name == fn:
            base_fn = fn
            break
        if field_name.startswith(fn + "_"):
            remainder = field_name[len(fn):]
            # 알려진 변형 접미사인지 확인
            if remainder in ("_prev", "_chg", "_diff", "_share", "_d2"):
                base_fn = fn
                variant_suffix = remainder
                break

    if not base_fn:
        print(f"  [실패] '{field_name}'에 매칭되는 MetricVariant 없음")
        return

    # 베이스 fieldName으로 MetricVariant 찾기
    variants = onto.find_subjects("raas:fieldName", f'"{base_fn}"')
    if not variants:
        print(f"  [실패] '{base_fn}'에 매칭되는 MetricVariant 없음")
        return

    mv = variants[0]
    metric = onto.get_one(mv, "raas:ofMetric")
    granularity = onto.get_one(mv, "raas:granularity")
    window = onto.get_one(mv, "raas:hasWindow")
    cohort = onto.get_one(mv, "raas:hasCohort")

    print(f"  필드명: {field_name}")
    print(f"  ├─ MetricVariant: {mv}")
    print(f"  ├─ 베이스 Metric: {metric} ({onto.label_ko(metric)})")
    if granularity:
        print(f"  ├─ 시간 단위: {granularity} ({onto.label_ko(granularity)})")
    if window:
        print(f"  ├─ 유지율 윈도우: {window} ({onto.label_ko(window)})")
    if cohort:
        print(f"  ├─ 코호트: {cohort} ({onto.label_ko(cohort)})")
    if variant_suffix:
        # variant 찾기
        variants_with_suf = onto.find_subjects("raas:variantSuffix", f'"{variant_suffix}"')
        if variants_with_suf:
            v = variants_with_suf[0]
            print(f"  ├─ 변형: {v} ({onto.label_ko(v)})")

    # Metric 정보
    unit = onto.value_str(onto.get_one(metric, "raas:unit"))
    formula = onto.value_str(onto.get_one(metric, "raas:formula"))
    definition = onto.value_str(onto.get_one(metric, "raas:definition"))
    measure_type = onto.get_one(metric, "raas:hasMeasureType")

    print(f"  └─ 단위: {unit}")
    if measure_type:
        print(f"     측정 종류: {measure_type}")
    if formula:
        print(f"     계산식: {formula}")
    if definition:
        print(f"     정의: {definition}")


def demo2_all_variants(onto, metric):
    """특정 metric의 모든 시간 단위 변형 조회"""
    print(f"\n[데모 2] '{metric}'의 모든 변형")
    print("-" * 60)

    variants = onto.find_subjects("raas:ofMetric", metric)
    print(f"  베이스 metric '{metric}' ({onto.label_ko(metric)})의 변형 {len(variants)}개:")
    for v in variants:
        fn = onto.value_str(onto.get_one(v, "raas:fieldName"))
        gran = onto.get_one(v, "raas:granularity")
        win = onto.get_one(v, "raas:hasWindow")
        coh = onto.get_one(v, "raas:hasCohort")
        dims = []
        if gran: dims.append(f"granularity={onto.label_ko(gran)}")
        if win:  dims.append(f"window={onto.label_ko(win)}")
        if coh:  dims.append(f"cohort={onto.label_ko(coh)}")
        print(f"    {v}")
        print(f"      fieldName: \"{fn}\"")
        print(f"      차원: {' | '.join(dims)}")


def demo3_search_by_label(onto, keyword):
    """자연어 별칭으로 metric 검색 (현재 KEYWORD_TO_CODE 대체)"""
    print(f"\n[데모 3] 키워드 '{keyword}'로 metric 검색")
    print("-" * 60)

    matches = []
    for metric in onto.instances_of("raas:Metric"):
        # rdfs:label 확인
        labels = onto.get(metric, "rdfs:label")
        alt_labels = onto.get(metric, "raas:altLabel")
        all_labels = labels + alt_labels
        for lbl in all_labels:
            if keyword.lower() in lbl.lower():
                matches.append((metric, lbl))
                break

    if matches:
        print(f"  매칭된 metric {len(matches)}개:")
        for m, lbl in matches:
            unit = onto.value_str(onto.get_one(m, "raas:unit"))
            print(f"    {m} ({onto.label_ko(m)}, 단위: {unit})")
            print(f"      매칭 라벨: {onto.value_str(lbl)}")
    else:
        print(f"  매칭된 metric 없음")


def demo4_derived_chain(onto, metric):
    """파생 metric 추적"""
    print(f"\n[데모 4] '{metric}'의 파생 관계 추적")
    print("-" * 60)

    derived = onto.get(metric, "raas:derivedFrom")
    if derived:
        print(f"  '{metric}'은 다음에서 파생됨:")
        for d in derived:
            print(f"    → {d} ({onto.label_ko(d)})")
        print(f"\n  의미: 이 metric은 원천 metric만 있어도 자동 계산 가능합니다.")
    else:
        print(f"  '{metric}'은 직접 측정되는 베이스 metric입니다 (파생 아님).")


def demo5_csv_to_measurement(onto, target_pgm_code, target_date):
    """CSV row를 Measurement 객체로 변환 (어댑터 시연)"""
    print(f"\n[데모 5] CSV row → 의미적 Measurement 변환")
    print(f"  대상: {target_pgm_code}, 날짜: {target_date}")
    print("-" * 60)

    if CSV_PATH is None or not CSV_PATH.exists():
        print(f"  [건너뜀] CSV 파일 없음 (raas_kpi_latest.csv를 같은 폴더 또는 ../data/에 두세요)")
        return

    # CSV에서 해당 row 찾기
    target_row = None
    header = None
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if (row[header.index("PGM_CODE")] == target_pgm_code and
                row[header.index("DATE")] == target_date):
                target_row = row
                break

    if not target_row:
        print(f"  [실패] 해당 데이터 없음")
        return

    # KPI 컬럼만 추출 (값이 있는 것만)
    META_COLS = {"DATE", "DATE_WEEK", "DATE_MON", "PGM_CODE", "PGM_NAME",
                 "STIME", "program_title", "guestname",
                 "daily_corner", "weekly_corner"}

    # 모든 fieldName 미리 수집 (긴 것 우선)
    all_field_names = []
    for s, p, o in onto.triples:
        if p == "raas:fieldName":
            fn_clean = re.sub(r'^"|"(?:@\w+)?$', '', o)
            all_field_names.append(fn_clean)
    all_field_names.sort(key=len, reverse=True)

    measurements = []
    SUFFIXES = ["_prev", "_chg", "_diff", "_share", "_d2"]

    for i, col in enumerate(header):
        if col in META_COLS:
            continue
        val = target_row[i]
        if not val or val in ("None", "null"):
            continue

        # 베이스 fieldName과 변형 suffix 분리 (긴 매칭 우선)
        base_fn = None
        variant_suf = ""
        for fn in all_field_names:
            if col == fn:
                base_fn = fn
                break
            if col.startswith(fn + "_"):
                rem = col[len(fn):]
                if rem in SUFFIXES:
                    base_fn = fn
                    variant_suf = rem
                    break

        if not base_fn:
            continue

        # MetricVariant 조회
        mvs = onto.find_subjects("raas:fieldName", f'"{base_fn}"')
        if not mvs:
            continue

        mv = mvs[0]
        metric = onto.get_one(mv, "raas:ofMetric")
        measurements.append({
            "field": col,
            "value": val,
            "metric": metric,
            "metric_label": onto.label_ko(metric),
            "variant_suffix": variant_suf,
        })

    print(f"  추출된 의미적 측정값: {len(measurements)}개")
    print(f"\n  [샘플 10개]")
    for m in measurements[:10]:
        var_str = f" [변형: {m['variant_suffix']}]" if m['variant_suffix'] else ""
        print(f"    {m['field']:30s} = {m['value']:>10s} | "
              f"{m['metric_label']}{var_str}")


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("RAAS KPI 온톨로지 사용 예시")
    print("=" * 70)

    onto = Ontology(ONTO_PATH)
    print(f"\n온톨로지 로드 완료: {len(onto.triples)}개 트리플")

    # 데모 실행
    demo1_field_to_metric(onto, "wau_chg")       # 주간 DAU 변동률 (신 명명)
    demo1_field_to_metric(onto, "new_d7_ret")    # 신규 D+7 유지율
    demo1_field_to_metric(onto, "deep_rate_diff") # 일간 깊은청취율 차이

    demo2_all_variants(onto, "raas:DAU")
    demo2_all_variants(onto, "raas:RetentionRate")

    demo3_search_by_label(onto, "유지율")
    demo3_search_by_label(onto, "DAU")
    demo3_search_by_label(onto, "이탈")

    demo4_derived_chain(onto, "raas:DeepListenRate")
    demo4_derived_chain(onto, "raas:DAU")

    demo5_csv_to_measurement(onto, "F09", "2026/03/25")  # 컬투쇼

    print()
    print("=" * 70)
    print("완료")
    print("=" * 70)


if __name__ == "__main__":
    main()
