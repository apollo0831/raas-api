"""
RAAS Phase 5 회귀 테스트 (Regression Test)

Phase 5-A 설계 문서의 챕터 7 검증 계획을 자동화.
Claude Code에서 Phase 5-B 진행 시 매 Step마다 실행.

사용법:
    cd raas_onto/
    python3 phase5_regression_test.py

확장:
- 운영 통합 시 RAAS의 실제 briefing_engine, query_engine과 비교
- pytest로 마이그레이션 권장
"""
import sys
import os
from pathlib import Path

# 어댑터 import (현재 디렉토리에 있다고 가정)
sys.path.insert(0, str(Path(__file__).parent))
from raas_ontology_adapter import get_adapter, OntologyAdapter

# 통계
PASS_COUNT = 0
FAIL_COUNT = 0
FAIL_DETAILS = []


def assert_eq(test_id: str, expected, actual, note: str = ""):
    """단순 동등성 체크."""
    global PASS_COUNT, FAIL_COUNT
    if expected == actual:
        PASS_COUNT += 1
        print(f"  [PASS] {test_id}")
    else:
        FAIL_COUNT += 1
        FAIL_DETAILS.append({
            "test_id": test_id,
            "expected": expected,
            "actual": actual,
            "note": note,
        })
        print(f"  [FAIL] {test_id}")
        print(f"         expected: {expected}")
        print(f"         actual:   {actual}")


def assert_in(test_id: str, expected_member, container, note: str = ""):
    """포함 관계 체크."""
    global PASS_COUNT, FAIL_COUNT
    if expected_member in container:
        PASS_COUNT += 1
        print(f"  [PASS] {test_id}")
    else:
        FAIL_COUNT += 1
        FAIL_DETAILS.append({
            "test_id": test_id,
            "expected_in": expected_member,
            "container": container,
            "note": note,
        })
        print(f"  [FAIL] {test_id}: '{expected_member}' not in {container}")


# =============================================================================
# RT — 회귀 테스트 (Regression Tests)
# =============================================================================

def test_kpi_aliases(adapter: OntologyAdapter):
    """RT-01 ~ RT-04: KPI 변환 회귀."""
    print("\n[RT-01~04] KPI 변환 회귀")

    # RT-01: dau alias
    info = adapter.get_field_info("dau")
    assert_eq("RT-01", "raas:DAU", info["metric"]["id"] if info else None,
              "dau가 DAU metric으로 매핑")

    # RT-02: wau alias
    info = adapter.get_field_info("wau")
    assert_eq("RT-02-1", "raas:DAU", info["metric"]["id"] if info else None,
              "wau도 DAU metric으로 매핑")
    assert_eq("RT-02-2", "raas:Week", info["granularity"]["id"] if info else None,
              "wau는 주간 granularity")

    # RT-03: 모든 핵심 변형 매핑
    test_fields = [
        ("dau_prev",         "raas:DAU",            "raas:Previous"),
        ("dau_chg",          "raas:DAU",            "raas:Change"),
        ("wau_chg",          "raas:DAU",            "raas:Change"),
        ("mau",              "raas:DAU",            None),  # variant: Current
        ("dau_r7",           "raas:DAU",            None),
        ("deep_rate_diff",   "raas:DeepListenRate", "raas:Difference"),
        ("habit_rate_week",  "raas:HabitRate",      None),
        ("new_d7_ret",       "raas:RetentionRate",  None),
    ]
    for field, expected_metric, expected_variant in test_fields:
        info = adapter.get_field_info(field)
        if info is None:
            assert_eq(f"RT-03-{field}", expected_metric, None,
                      f"필드 {field} 매칭 실패")
            continue
        assert_eq(f"RT-03-{field}-metric", expected_metric, info["metric"]["id"])
        if expected_variant is not None:
            actual = info.get("variant", {}).get("id")
            assert_eq(f"RT-03-{field}-variant", expected_variant, actual)

    # RT-04: 미존재 필드 처리
    info = adapter.get_field_info("nonexistent_field_xyz")
    assert_eq("RT-04", None, info, "미존재 필드는 None 반환")


def test_program_matching(adapter: OntologyAdapter):
    """RT-05 ~ RT-08: 프로그램 매칭 회귀."""
    print("\n[RT-05~08] 프로그램 매칭 회귀")

    # RT-05: 정식 명칭
    matches = adapter.find_program_by_keyword("두시탈출 컬투쇼")
    assert_eq("RT-05", "F09", matches[0]["code"] if matches else None)

    # RT-06: 별칭
    matches = adapter.find_program_by_keyword("컬투")
    assert_eq("RT-06", "F09", matches[0]["code"] if matches else None)

    # RT-07: 영문명
    matches = adapter.find_program_by_keyword("PowerFM")
    assert_eq("RT-07", "F00", matches[0]["code"] if matches else None)

    # RT-08: 미존재 키워드
    matches = adapter.find_program_by_keyword("없는프로그램XYZ123")
    assert_eq("RT-08", [], matches, "미존재 키워드는 빈 리스트")


def test_business_rules(adapter: OntologyAdapter):
    """RT-09 ~ RT-12: 비즈니스 룰 회귀."""
    print("\n[RT-09~12] 비즈니스 룰 회귀")

    # RT-09: 알림 룰 평가
    test_kpi = {
        "dau_chg": -12,         # DauPlunge 발동 (≤-10)
        "deep_rate_diff": -5,   # DeepRatePlunge 발동 (≤-3)
        "habit_rate": 35,       # HabitRateAchieved 발동 (≥30)
        "react_rate": 6,        # ReactRateAchieved 발동 (≥5)
    }
    alerts = adapter.evaluate_platform_alerts(test_kpi)
    rule_ids = {a["rule_id"] for a in alerts}
    assert_in("RT-09-DauPlunge",         "raas:Alert_DauPlunge", rule_ids)
    assert_in("RT-09-DeepRatePlunge",    "raas:Alert_DeepRatePlunge", rule_ids)
    assert_in("RT-09-HabitRateAchieved", "raas:Alert_HabitRateAchieved", rule_ids)
    assert_in("RT-09-ReactRateAchieved", "raas:Alert_ReactRateAchieved", rule_ids)

    # RT-10: AtRiskProgram 분류
    all_kpi = {
        "F05": {"dau": 23327, "churn_rate": 44.0, "dau_chg": -11.4},  # 위험
        "F09": {"dau": 22920, "churn_rate": 22.0, "dau_chg": -3.0},   # 정상
    }
    risks = adapter.find_at_risk_programs(all_kpi)
    risk_codes = {r["code"] for r in risks}
    assert_in("RT-10-F05-included", "F05", risk_codes)
    assert_eq("RT-10-F09-not-included", False, "F09" in risk_codes,
              "F09는 위험 아님")

    # RT-11: 임계값 경계 (DAU = 999, 1000, 1001)
    boundary_test = {
        "P1": {"dau": 999, "churn_rate": 35, "dau_chg": -10},   # DAU 미달
        "P2": {"dau": 1000, "churn_rate": 35, "dau_chg": -10},  # 경계 = 위험
        "P3": {"dau": 1001, "churn_rate": 35, "dau_chg": -10},  # 위험
    }
    risks = adapter.find_at_risk_programs(boundary_test)
    # P1, P2, P3는 raas:P1 등의 IRI가 없어서 매칭 안 됨 (실제 운영 시는 다름)
    # 대신 classify_program으로 임계값 동작 확인
    assert_eq("RT-11-DAU-999",  False,
              "raas:AtRiskProgram" in adapter.classify_program("P1", boundary_test["P1"]))
    assert_eq("RT-11-DAU-1000", True,
              "raas:AtRiskProgram" in adapter.classify_program("P2", boundary_test["P2"]))
    assert_eq("RT-11-DAU-1001", True,
              "raas:AtRiskProgram" in adapter.classify_program("P3", boundary_test["P3"]))

    # RT-12: 빈 KPI 처리
    alerts = adapter.evaluate_platform_alerts({})
    assert_eq("RT-12", [], alerts, "빈 KPI에서는 알림 없음")


# =============================================================================
# NT — 신규 능력 테스트 (New capability Tests)
# =============================================================================

def test_new_capabilities(adapter: OntologyAdapter):
    """NT-01 ~ NT-06: 신규 능력 테스트."""
    print("\n[NT-01~06] 신규 능력 테스트")

    # NT-01: get_schema_dump
    schema = adapter.get_schema_dump()
    for key in ["metrics", "channels", "programs", "dayparts", "concepts"]:
        assert_in(f"NT-01-{key}", key, schema)

    # NT-02: get_concept_definition - AtRiskProgram
    concept = adapter.get_concept_definition("AtRiskProgram")
    assert_eq("NT-02-1", "이탈 위험 프로그램",
              concept["label"] if concept else None)
    assert_in("NT-02-2", "DAU 1,000",
              concept["definition"] if concept else "")

    # NT-03: 미존재 개념
    concept = adapter.get_concept_definition("NonexistentXYZ")
    assert_eq("NT-03", None, concept)

    # NT-04: find_program_by_keyword score
    matches = adapter.find_program_by_keyword("컬투")
    assert_eq("NT-04", 1.0, matches[0]["score"] if matches else None,
              "정확 매칭은 score 1.0")

    # NT-05: DayType 판정
    info = adapter.get_day_type("2026-05-01")
    assert_eq("NT-05-1", "공휴일", info.get("day_type"))
    assert_eq("NT-05-2", "근로자의 날", info.get("holiday_name"))

    info = adapter.get_day_type("2026-05-02")
    assert_eq("NT-05-3", "주말", info.get("day_type"))

    # NT-06: LLM 컨텍스트 자동 추출
    ctx = adapter.get_llm_context_for_query("위험 프로그램 알려줘")
    assert_in("NT-06", "AtRiskProgram", ctx)


# =============================================================================
# PT — 성능 테스트
# =============================================================================

def test_performance(adapter: OntologyAdapter):
    """PT-01~04: 성능 테스트 (단순 측정)."""
    print("\n[PT-01~04] 성능 테스트")
    import time

    # PT-02: get_field_info 캐시 히트 (이미 로드된 상태)
    iterations = 10000
    start = time.perf_counter()
    for _ in range(iterations):
        adapter.get_field_info("dau")
    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000
    print(f"  [INFO] PT-02 get_field_info x{iterations}: {avg_ms:.4f}ms 평균")
    assert_eq("PT-02", True, avg_ms < 1.0, f"<1ms (실제 {avg_ms:.4f}ms)")

    # PT-03: get_schema_dump
    start = time.perf_counter()
    schema = adapter.get_schema_dump()
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"  [INFO] PT-03 get_schema_dump: {elapsed_ms:.1f}ms")
    assert_eq("PT-03", True, elapsed_ms < 50,
              f"<50ms (실제 {elapsed_ms:.1f}ms)")


# =============================================================================
# P3 — Phase 2-C-3 Person 검증 (신규)
# =============================================================================

def test_phase2c3_person(adapter: OntologyAdapter):
    """P3-01 ~ P3-08: Person/ProgramType 신규 능력."""
    print("\n[P3-01~08] Phase 2-C-3 Person 검증")

    # P3-01: get_program_meta가 main_host 포함하는지
    meta = adapter.get_program_meta("F05")
    assert_in("P3-01", "main_host", meta or {})
    assert_eq("P3-01-name", "김영철", meta.get("main_host", {}).get("label") if meta else None)

    # P3-02: F09 김태균 (사용자 도메인 정보 반영 확인)
    meta = adapter.get_program_meta("F09")
    assert_eq("P3-02", "김태균", meta.get("main_host", {}).get("label") if meta else None)

    # P3-03: M10 / M11 공유 진행자 (DJ래피)
    m10 = adapter.get_program_meta("M10")
    m11 = adapter.get_program_meta("M11")
    m10_host = m10.get("main_host", {}).get("label") if m10 else None
    m11_host = m11.get("main_host", {}).get("label") if m11 else None
    assert_eq("P3-03", m10_host, m11_host, "M10/M11 같은 진행자")

    # P3-04: F03 자동 음악 프로그램 (진행자 없음)
    meta = adapter.get_program_meta("F03")
    assert_eq("P3-04-no-host", None, meta.get("main_host") if meta else "?")

    # P3-05: F03 ProgramType
    ptype = adapter.get_program_type("F03")
    assert_eq("P3-05", True, ptype.get("is_automated") if ptype else None,
              "F03은 AutomatedProgram")

    # P3-06: F05 ProgramType은 Hosted
    ptype = adapter.get_program_type("F05")
    assert_eq("P3-06", False, ptype.get("is_automated") if ptype else None)

    # P3-07: 사람 이름으로 검색 — 정상근 (RegularGuest)
    matches = adapter.find_person_by_name("정상근")
    assert_eq("P3-07-found", 1, len(matches))
    if matches:
        assert_in("P3-07-regular", "F05", matches[0].get("as_regular_guest", []))

    # P3-08: 김영철 (MainHost)
    matches = adapter.find_person_by_name("김영철")
    if matches:
        assert_in("P3-08", "F05", matches[0].get("as_main_host", []))

    # P3-09: F05 RegularGuest 포함 확인
    meta = adapter.get_program_meta("F05")
    regulars = meta.get("regular_guests", []) if meta else []
    regular_names = [r.get("label") for r in regulars]
    assert_in("P3-09", "정상근", regular_names)

    # P3-10: guestname 정책 조회
    policy = adapter.get_guestname_policy("F05")
    assert_in("P3-10", "고정 게스트", policy.get("label") if policy else "")


# =============================================================================
# 메인
# =============================================================================

def main():
    print("=" * 70)
    print("RAAS Phase 5 회귀 테스트")
    print("=" * 70)

    # PT-01: 콜드 스타트
    import time
    start = time.perf_counter()
    adapter = get_adapter()
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"\n[PT-01] 어댑터 콜드 스타트: {elapsed_ms:.1f}ms (기대 <500ms)")
    cold_start_ok = elapsed_ms < 500

    test_kpi_aliases(adapter)
    test_program_matching(adapter)
    test_business_rules(adapter)
    test_new_capabilities(adapter)
    test_performance(adapter)
    test_phase2c3_person(adapter)

    # 최종 요약
    print("\n" + "=" * 70)
    print(f"테스트 결과: {PASS_COUNT} PASS / {FAIL_COUNT} FAIL")
    print(f"  콜드 스타트: {'OK' if cold_start_ok else 'SLOW'} ({elapsed_ms:.1f}ms)")

    if FAIL_DETAILS:
        print("\n실패 상세:")
        for f in FAIL_DETAILS:
            print(f"  - {f['test_id']}: {f.get('note', '')}")
            print(f"    expected: {f.get('expected', f.get('expected_in'))}")
            print(f"    actual:   {f.get('actual', f.get('container'))}")

    print("=" * 70)
    return 0 if FAIL_COUNT == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
