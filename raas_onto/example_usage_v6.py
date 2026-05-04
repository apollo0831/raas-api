"""
RAAS 온톨로지 사용 예시 v6 (Phase 2-C-3: Person/Host)

Person 정보 활용 시나리오:
25. 프로그램 진행자 조회 (단일 출처)
26. 진행자 이름으로 프로그램 검색 (자연어 매칭 확장)
27. ProgramType 분류 — HostedProgram vs AutomatedProgram
28. RegularGuest 조회 (정상근이 출연하는 프로그램)
29. guestname 해석 정책 조회 (프로그램별 다른 의미)
30. 진행자 정보 + KPI 통합 답변 (LLM 컨텍스트 시뮬레이션)
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
        return list(set(self.types.get(cls, [])))

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


# ─── 데모 ──────────────────────────────────────────────────────────────────

def demo25_program_host(onto):
    """프로그램 → 진행자 조회"""
    print(f"\n[데모 25] 프로그램별 진행자 조회 (단일 출처)")
    print("-" * 60)

    test_programs = ["F05", "F09", "F12", "L11", "M10", "F03"]
    for code in test_programs:
        prog = f"raas:{code}"
        prog_label = onto.label_ko(prog)
        host = onto.get_one(prog, "raas:hasMainHost")
        prog_type = onto.get_one(prog, "raas:hasProgramType")
        type_label = onto.label_ko(prog_type) if prog_type else "?"

        if host:
            host_name = onto.label_ko(host)
            stage = onto.value_str(onto.get_one(host, "raas:stageName"))
            occupation = onto.value_str(onto.get_one(host, "raas:personOccupation"))
            stage_str = f" (예명: {stage})" if stage and stage != host_name else ""
            occ_str = f" — {occupation}" if occupation else ""
            print(f"  {code} {prog_label[:25]:25s} → {host_name}{stage_str}{occ_str}")
        else:
            print(f"  {code} {prog_label[:25]:25s} → 진행자 없음 [{type_label}]")


def demo26_search_by_person(onto, person_keyword):
    """사람 이름으로 프로그램 검색 — 자연어 매칭 확장"""
    print(f"\n[데모 26] '{person_keyword}'으로 프로그램 검색")
    print("-" * 60)

    # 모든 Person을 순회하여 이름 매칭
    matched_persons = []
    for person in onto.instances_of("raas:Person"):
        labels = []
        for label_pred in ("rdfs:label", "raas:personName", "raas:stageName"):
            for v in onto.get(person, label_pred):
                clean = onto.value_str(v)
                if clean:
                    labels.append(clean)

        for label in labels:
            if person_keyword in label:
                matched_persons.append((person, label))
                break

    if not matched_persons:
        print(f"  '{person_keyword}' 매칭 결과 없음")
        return

    print(f"  매칭된 Person {len(matched_persons)}명:")
    for person, label in matched_persons:
        person_name = onto.label_ko(person)
        # 이 사람이 진행하는 프로그램
        as_host = onto.find_subjects("raas:hasMainHost", person)
        as_regular = onto.find_subjects("raas:hasRegularGuest", person)

        roles = []
        if as_host:
            for prog in as_host:
                prog_label = onto.label_ko(prog)
                roles.append(f"진행자: {prog_label}")
        if as_regular:
            for prog in as_regular:
                prog_label = onto.label_ko(prog)
                roles.append(f"고정게스트: {prog_label}")

        print(f"    {person_name} ('{label}' 매칭)")
        for r in roles:
            print(f"      → {r}")


def demo27_program_type_analysis(onto):
    """ProgramType 분류 — HostedProgram vs AutomatedProgram"""
    print(f"\n[데모 27] 프로그램 유형 분류")
    print("-" * 60)

    by_type = defaultdict(list)
    for prog in onto.instances_of("raas:Program"):
        ptype = onto.get_one(prog, "raas:hasProgramType")
        if ptype:
            by_type[ptype].append(prog)

    for ptype in sorted(by_type.keys()):
        type_label = onto.label_ko(ptype)
        comment = onto.value_str(onto.get_one(ptype, "rdfs:comment"))
        print(f"\n  [{type_label}] — {len(by_type[ptype])}개")
        print(f"  {comment[:80]}...")
        for prog in sorted(by_type[ptype]):
            code = onto.value_str(onto.get_one(prog, "raas:code"))
            label = onto.label_ko(prog)
            print(f"    {code}: {label}")


def demo28_regular_guest_lookup(onto, person_keyword):
    """정상근이 출연하는 프로그램 조회 + 데이터 검증"""
    print(f"\n[데모 28] '{person_keyword}' 출연 프로그램")
    print("-" * 60)

    # Person 찾기
    target_person = None
    for person in onto.instances_of("raas:Person"):
        if person_keyword in onto.label_ko(person):
            target_person = person
            break

    if not target_person:
        print(f"  '{person_keyword}' Person 인스턴스 없음")
        return

    person_name = onto.label_ko(target_person)
    print(f"  Person: {person_name} ({target_person})")

    # 어떤 역할로 어떤 프로그램에?
    roles = []

    as_host = onto.find_subjects("raas:hasMainHost", target_person)
    for prog in as_host:
        roles.append((prog, "MainHost", onto.label_ko(prog)))

    as_regular = onto.find_subjects("raas:hasRegularGuest", target_person)
    for prog in as_regular:
        roles.append((prog, "RegularGuest", onto.label_ko(prog)))

    if not roles:
        print(f"  연결된 프로그램 없음")
        return

    for prog, role, label in roles:
        print(f"\n  [{role}] {prog.replace('raas:', '')} {label}")

        # CSV에서 실제 등장 횟수 검증
        if CSV_PATH and CSV_PATH.exists():
            count_appearances = 0
            with open(CSV_PATH, encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                pgm_idx = header.index("PGM_CODE")
                gn_idx = header.index("guestname")
                code = prog.replace("raas:", "")
                for row in reader:
                    if row[pgm_idx] == code and gn_idx < len(row):
                        if person_keyword in row[gn_idx]:
                            count_appearances += 1
            print(f"    CSV에서 {person_keyword} 등장: {count_appearances}회")


def demo29_guestname_policy(onto):
    """guestname 해석 정책 조회 — 같은 텍스트의 다른 의미"""
    print(f"\n[데모 29] guestname 해석 정책 (프로그램별 다른 의미)")
    print("-" * 60)

    # 정책별로 적용된 프로그램 분류
    policies = onto.instances_of("raas:GuestnameInterpretationPolicy")
    for policy in sorted(policies):
        policy_label = onto.label_ko(policy)
        comment = onto.value_str(onto.get_one(policy, "rdfs:comment"))
        example = onto.value_str(onto.get_one(policy, "raas:exampleText"))

        applied_progs = onto.find_subjects("raas:guestnameInterpretation", policy)

        print(f"\n  [{policy_label}]")
        print(f"  설명: {comment[:80]}")
        if example:
            print(f"  예시: {example[:80]}")

        if applied_progs:
            print(f"  적용 프로그램:")
            for prog in applied_progs:
                code = onto.value_str(onto.get_one(prog, "raas:code"))
                label = onto.label_ko(prog)
                print(f"    {code}: {label}")
        else:
            print(f"  적용 프로그램: (없음 — 향후 확장)")


def demo30_llm_context_with_person(onto):
    """진행자 정보 + KPI 통합 — LLM 답변 컨텍스트 시뮬레이션"""
    print(f"\n[데모 30] LLM 컨텍스트 시뮬레이션 — 진행자 + KPI 통합")
    print("-" * 60)

    test_question = "김영철의 파워FM 어제 청취자 어땠어?"
    print(f"  사용자 질문: \"{test_question}\"\n")

    # 1단계: 키워드로 프로그램 매칭
    matched_prog = None
    matched_label = None
    for prog in onto.instances_of("raas:Program"):
        labels = [onto.label_ko(prog)]
        labels.extend([onto.value_str(v) for v in onto.get(prog, "raas:altLabel")])
        for label in labels:
            if "김영철" in label or "파워FM" in test_question and "파워FM" == label:
                if "김영철" in label:
                    matched_prog = prog
                    matched_label = label
                    break
        if matched_prog:
            break

    if not matched_prog:
        print(f"  매칭된 프로그램 없음")
        return

    code = onto.value_str(onto.get_one(matched_prog, "raas:code"))
    prog_label = onto.label_ko(matched_prog)

    # 2단계: 진행자 정보
    host = onto.get_one(matched_prog, "raas:hasMainHost")
    host_name = onto.label_ko(host) if host else "?"
    host_occ = onto.value_str(onto.get_one(host, "raas:personOccupation")) if host else ""

    # 3단계: 시간/Daypart 정보
    ts = onto.get_one(matched_prog, "raas:hasTimeSlot")
    daypart = onto.get_one(ts, "raas:hasDaypart") if ts else None
    dp_label = onto.label_ko(daypart) if daypart else "?"
    dp_value = onto.value_str(onto.get_one(daypart, "raas:adValueLevel")) if daypart else ""

    # 4단계: 고정 게스트
    regular_guests = onto.find_subjects("raas:hasMainHost", host)  # 같은 진행자가 다른 프로그램?
    regulars_for_prog = []
    for s, p, o in onto.triples:
        if s == matched_prog and p == "raas:hasRegularGuest":
            regulars_for_prog.append(onto.label_ko(o))

    # 5단계: CSV에서 5/2 KPI
    kpi = {}
    if CSV_PATH and CSV_PATH.exists():
        with open(CSV_PATH, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            pgm_idx = header.index("PGM_CODE")
            date_idx = header.index("DATE")
            dau_idx = header.index("dau")
            for row in reader:
                if row[pgm_idx] == code and row[date_idx] == "2026/05/02":
                    try:
                        kpi["dau"] = int(float(row[dau_idx]))
                    except (ValueError, IndexError):
                        pass

    # LLM에 전달할 컨텍스트 출력
    print(f"  → LLM에 전달될 컨텍스트:")
    print(f"  ┌─────────────────────────────────────────")
    print(f"  │ 프로그램: {prog_label} ({code})")
    print(f"  │ 진행자: {host_name}{f' ({host_occ})' if host_occ else ''}")
    if regulars_for_prog:
        print(f"  │ 고정 게스트: {', '.join(regulars_for_prog)}")
    print(f"  │ 방송 시간대: {dp_label} ({dp_value} 광고 가치)")
    if kpi.get("dau"):
        print(f"  │ 5/2 DAU: {kpi['dau']:,}명")

        # DayType 정보
        # 5/2는 토요일이라 컬투쇼 같은 평일 프로그램은 보통 안 함
        # 김영철의 파워FM은 평일 프로그램 (출근 시간대) — 토요일에는?
        print(f"  │ ⚠ 5/2는 토요일 (주말)")
    print(f"  └─────────────────────────────────────────")
    print(f"\n  → 이 컨텍스트를 받은 LLM은:")
    print(f"     - 진행자명, 직업, 고정 게스트까지 답변에 활용 가능")
    print(f"     - 출근 시간대 정보로 'Morning Drive 시간대' 언급 가능")
    print(f"     - 토요일 vs 평일 차이 인식 가능")
    print(f"     - 환각 없는 정확한 답변 생성")


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("RAAS 온톨로지 사용 예시 v6 (Phase 2-C-3: Person/Host)")
    print("=" * 70)

    onto = Ontology(ONTO_FILES)
    print(f"\n온톨로지 로드: {len(onto.triples)}개 트리플")

    demo25_program_host(onto)
    demo26_search_by_person(onto, "김영철")
    demo26_search_by_person(onto, "정상근")
    demo27_program_type_analysis(onto)
    demo28_regular_guest_lookup(onto, "정상근")
    demo29_guestname_policy(onto)
    demo30_llm_context_with_person(onto)

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
