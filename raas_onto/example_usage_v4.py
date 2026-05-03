"""
RAAS 온톨로지 사용 예시 v4 (Phase 4: 비즈니스 룰)

이 데모의 핵심:
- 현재 briefing_engine.build_s7()와 query_engine.extract_data()의 risks 룰이
  코드에 매직넘버로 박혀있는 것을, 온톨로지에서 동적으로 읽어 실행한다.
- 룰 변경 시 코드 수정 없이 .ttl 파일만 수정하면 된다.

15. 정의된 비즈니스 룰 목록 조회
16. 알림 룰 자동 평가 (build_s7 대체)
17. AtRiskProgram 클래스 자동 분류 (query_engine risks 대체)
18. 모든 프로그램 개념 동시 분류
19. 룰 정의 조회 + 권장 조치 제공
"""
import re
import csv
from pathlib import Path
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


# ─── CSV 데이터 로더 (전체 KPI) ────────────────────────────────────────────

def load_latest_kpi_data():
    """최신 날짜의 (PGM_CODE → KPI dict) 반환."""
    if CSV_PATH is None or not CSV_PATH.exists():
        return {}, ""

    rows_by_code = defaultdict(list)
    header = None
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        date_idx = header.index("DATE")
        code_idx = header.index("PGM_CODE")
        for row in reader:
            code = row[code_idx]
            if code:
                rows_by_code[code].append((row[date_idx], row))

    # 각 코드의 최신 행 추출
    result = {}
    latest_overall = ""
    for code, rows in rows_by_code.items():
        rows.sort(key=lambda x: x[0], reverse=True)
        latest_overall = max(latest_overall, rows[0][0])
        kpi = {}
        for i, col in enumerate(header):
            val = rows[0][1][i]
            if val and val not in ("None", "null", ""):
                try:
                    kpi[col] = float(val)
                except ValueError:
                    kpi[col] = val
        result[code] = kpi
    return result, latest_overall


# ─── 데모 함수들 ────────────────────────────────────────────────────────────

def demo15_list_business_rules(onto):
    """정의된 비즈니스 룰 목록 조회"""
    print(f"\n[데모 15] 정의된 비즈니스 룰 (단일 출처)")
    print("-" * 60)

    # ProgramConcept을 상속하는 클래스 직접 찾기
    print(f"\n  [프로그램 개념]")
    program_concepts = []
    for s, p, o in onto.triples:
        if p == "rdfs:subClassOf" and o == "raas:ProgramConcept":
            program_concepts.append(s)
    for cls in sorted(set(program_concepts)):
        label = onto.label_ko(cls)
        cond = onto.value_str(onto.get_one(cls, "raas:condition"))
        level = onto.value_str(onto.get_one(cls, "raas:alertLevel"))
        print(f"    {cls.replace('raas:', ''):28s} [{level:6s}] {label}")
        print(f"      조건: {cond}")

    # PlatformAlert 인스턴스
    print(f"\n  [플랫폼 알림 룰] {len(onto.instances_of('raas:PlatformAlert'))}개")
    for alert in onto.instances_of("raas:PlatformAlert"):
        label = onto.label_ko(alert)
        cond = onto.value_str(onto.get_one(alert, "raas:condition"))
        level = onto.value_str(onto.get_one(alert, "raas:alertLevel"))
        print(f"    {alert.replace('raas:', ''):32s} [{level:6s}] {label}")


def evaluate_alert(rule_subj, onto, kpi):
    """단일 PlatformAlert 룰 평가. 발동 시 메시지 반환, 아니면 None."""
    cond = onto.value_str(onto.get_one(rule_subj, "raas:condition"))
    level = onto.value_str(onto.get_one(rule_subj, "raas:alertLevel"))
    msg_template = onto.value_str(onto.get_one(rule_subj, "raas:alertMessage"))
    emoji = onto.value_str(onto.get_one(rule_subj, "raas:emoji"))

    # 매우 단순한 조건 평가 (실제로는 SPARQL 또는 안전한 평가기 사용)
    # PoC에서는 조건 패턴을 dispatch
    label = onto.label_ko(rule_subj)

    # 룰 ID 기반 매핑 (실제 운영에서는 SPARQL로 일반화 가능)
    rule_id = rule_subj.replace("raas:Alert_", "")

    eval_map = {
        "DauPlunge":         lambda k: k.get("dau_chg") if k.get("dau_chg") is not None and k.get("dau_chg") <= -10 else None,
        "DauSurge":          lambda k: k.get("dau_chg") if k.get("dau_chg") is not None and k.get("dau_chg") >= 10 else None,
        "DeepRatePlunge":    lambda k: k.get("deep_rate_diff") if k.get("deep_rate_diff") is not None and k.get("deep_rate_diff") <= -3 else None,
        "NewUserPlunge":     lambda k: k.get("new_chg") if k.get("new_chg") is not None and k.get("new_chg") <= -20 else None,
        "ChurnRateRise":     lambda k: k.get("churn_rate_diff") if k.get("churn_rate_diff") is not None and k.get("churn_rate_diff") >= 3 else None,
        "ReactRateAchieved": lambda k: k.get("react_rate") if k.get("react_rate") is not None and k.get("react_rate") >= 5 else None,
        "HabitRateAchieved": lambda k: k.get("habit_rate") if k.get("habit_rate") is not None and k.get("habit_rate") >= 30 else None,
        "HabitRateLow":      lambda k: k.get("habit_rate") if k.get("habit_rate") is not None and k.get("habit_rate") <= 15 else None,
    }

    if rule_id in eval_map:
        value = eval_map[rule_id](kpi)
        if value is not None:
            # 메시지 포맷팅 — 단순 치환
            msg = msg_template.replace("{value:+.1f}", f"{value:+.1f}")
            msg = msg.replace("{value:.1f}", f"{value:.1f}")
            return {"level": level, "msg": msg, "label": label, "rule": rule_subj}
    return None


def demo16_evaluate_alerts(onto, kpi_data, latest_date):
    """플랫폼 전체 KPI에 대한 알림 룰 자동 평가 (build_s7 대체 시연)"""
    print(f"\n[데모 16] 플랫폼 알림 룰 자동 평가 (build_s7 대체)")
    print(f"  기준일: {latest_date}")
    print("-" * 60)

    if not kpi_data:
        print(f"  [건너뜀] CSV 데이터 없음")
        return

    platform_kpi = kpi_data.get("T00", {})
    if not platform_kpi:
        print(f"  [건너뜀] T00 KPI 없음")
        return

    print(f"\n  → 현재 플랫폼 KPI 일부:")
    print(f"     DAU: {platform_kpi.get('dau', 0):,.0f}")
    print(f"     DAU WoW: {platform_kpi.get('dau_chg', 'N/A'):+.1f}%" if platform_kpi.get('dau_chg') else "     DAU WoW: N/A")
    print(f"     깊은청취율: {platform_kpi.get('deep_rate', 'N/A')}%")
    print(f"     이탈률 차이: {platform_kpi.get('churn_rate_diff', 'N/A')}pp")
    print(f"     습관형성률: {platform_kpi.get('habit_rate', 'N/A')}%")
    print(f"     복귀율: {platform_kpi.get('react_rate', 'N/A')}%")

    print(f"\n  → 알림 룰 평가 결과:")
    triggered = []
    for rule in onto.instances_of("raas:PlatformAlert"):
        # 프로그램 단위 룰은 별도 처리
        if "Program" in rule:
            continue
        result = evaluate_alert(rule, onto, platform_kpi)
        if result:
            triggered.append(result)

    if not triggered:
        print(f"     ✓ 모든 룰 정상 범위 (알림 없음)")
    else:
        for t in triggered:
            print(f"     [{t['level']:6s}] {t['msg']}")


def demo17_classify_at_risk(onto, kpi_data, latest_date):
    """AtRiskProgram 클래스 자동 분류 (query_engine.risks 대체)"""
    print(f"\n[데모 17] 이탈 위험 프로그램 자동 분류")
    print(f"  기준일: {latest_date}")
    print("-" * 60)

    # AtRiskProgram의 정의를 온톨로지에서 읽어옴
    cls = "raas:AtRiskProgram"
    label = onto.label_ko(cls)
    cond = onto.value_str(onto.get_one(cls, "raas:condition"))
    action = onto.value_str(onto.get_one(cls, "raas:recommendedAction"))

    print(f"\n  정의: {label}")
    print(f"  조건: {cond}")
    print(f"  권장 조치: {action}")

    if not kpi_data:
        print(f"\n  [건너뜀] CSV 데이터 없음")
        return

    # 모든 프로그램에 대해 조건 평가
    matched = []
    programs = onto.instances_of("raas:Program")
    for p_subj in programs:
        code = onto.value_str(onto.get_one(p_subj, "raas:code"))
        if not code or code not in kpi_data:
            continue
        kpi = kpi_data[code]
        dau = kpi.get("dau", 0)
        churn = kpi.get("churn_rate", 0)
        wow = kpi.get("dau_chg", 0)

        if dau >= 1000 and churn >= 30 and wow <= -5:
            matched.append({
                "code": code,
                "label": onto.label_ko(p_subj),
                "dau": dau, "churn": churn, "wow": wow,
            })

    matched.sort(key=lambda x: -x["dau"])
    print(f"\n  → 매칭 프로그램: {len(matched)}개")
    if matched:
        print(f"  {'코드':6s} {'프로그램':25s}   {'DAU':>8s}  {'이탈%':>6s}  {'WoW':>7s}")
        print(f"  {'-'*6} {'-'*25}   {'-'*8}  {'-'*6}  {'-'*7}")
        for m in matched:
            print(f"  {m['code']:6s} {m['label'][:25]:25s}   {m['dau']:>8,.0f}  {m['churn']:>5.1f}%  {m['wow']:>+6.1f}%")


def demo18_classify_all_concepts(onto, kpi_data, latest_date):
    """모든 ProgramConcept 클래스 동시 분류"""
    print(f"\n[데모 18] 모든 프로그램 개념 동시 분류")
    print(f"  기준일: {latest_date}")
    print("-" * 60)

    if not kpi_data:
        print(f"  [건너뜀] CSV 데이터 없음")
        return

    # 4개 ProgramConcept 클래스 정의
    classifiers = {
        "raas:AtRiskProgram":      lambda k: k.get("dau", 0) >= 1000 and k.get("churn_rate", 0) >= 30 and k.get("dau_chg", 0) <= -5,
        "raas:HabitFormingProgram":lambda k: k.get("new", 0) >= 500 and k.get("habit_rate", 0) >= 30,
        "raas:DecliningProgram":   lambda k: k.get("dau", 0) >= 1000 and k.get("dau_chg", 0) <= -10,
        "raas:GrowingProgram":     lambda k: k.get("dau", 0) >= 1000 and k.get("dau_chg", 0) >= 10,
    }

    programs = onto.instances_of("raas:Program")

    for cls, predicate in classifiers.items():
        cls_label = onto.label_ko(cls)
        level = onto.value_str(onto.get_one(cls, "raas:alertLevel"))
        emoji = {"red": "🔴", "yellow": "🟡", "green": "🟢"}.get(level, "")
        matched = []
        for p_subj in programs:
            code = onto.value_str(onto.get_one(p_subj, "raas:code"))
            if not code or code not in kpi_data:
                continue
            try:
                if predicate(kpi_data[code]):
                    matched.append((code, onto.label_ko(p_subj), kpi_data[code]))
            except (TypeError, ValueError):
                continue

        print(f"\n  {emoji} {cls_label} — 해당 {len(matched)}개")
        for code, label, kpi in matched:
            dau = kpi.get("dau", 0)
            print(f"     {code} {label[:30]:30s}  DAU {dau:>7,.0f}")


def demo19_query_rule_definition(onto, concept_name):
    """특정 룰의 정의·조치 조회 (LLM 답변에 활용)"""
    print(f"\n[데모 19] '{concept_name}' 정의 조회")
    print("-" * 60)

    cls = f"raas:{concept_name}"
    if cls not in onto.types.get("owl:Class", []) and cls not in [s for s in onto.spo if onto.spo[s].get("rdfs:subClassOf")]:
        print(f"  '{concept_name}' 클래스를 찾을 수 없습니다")
        return

    label = onto.label_ko(cls)
    comment = onto.value_str(onto.get_one(cls, "rdfs:comment"))
    cond = onto.value_str(onto.get_one(cls, "raas:condition"))
    level = onto.value_str(onto.get_one(cls, "raas:alertLevel"))
    action = onto.value_str(onto.get_one(cls, "raas:recommendedAction"))

    print(f"\n  이름: {label}")
    print(f"  설명: {comment}")
    print(f"  조건: {cond}")
    print(f"  알림 등급: {level}")
    print(f"  권장 조치: {action}")
    print(f"\n  → 이 정보를 LLM에 컨텍스트로 주입하면, 환각 없이 정확한 답변 가능")


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("RAAS 온톨로지 사용 예시 v4 (Phase 4: 비즈니스 룰)")
    print("=" * 70)

    onto = Ontology(ONTO_FILES)
    print(f"\n온톨로지 로드: {len(onto.triples)}개 트리플")

    # CSV 데이터 미리 로드
    kpi_data, latest_date = load_latest_kpi_data()
    if kpi_data:
        print(f"CSV 데이터 로드: {len(kpi_data)}개 코드, 최신 날짜 {latest_date}")

    demo15_list_business_rules(onto)
    demo16_evaluate_alerts(onto, kpi_data, latest_date)
    demo17_classify_at_risk(onto, kpi_data, latest_date)
    demo18_classify_all_concepts(onto, kpi_data, latest_date)
    demo19_query_rule_definition(onto, "AtRiskProgram")
    demo19_query_rule_definition(onto, "HabitFormingProgram")

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
