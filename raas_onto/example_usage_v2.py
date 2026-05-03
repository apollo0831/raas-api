"""
RAAS 온톨로지 사용 예시 v2 (Phase 2-C-1 추가)

도메인 엔티티(Channel, Program, Platform)를 활용한 데모 추가:
6. 프로그램 별칭으로 코드 검색 (KEYWORD_TO_CODE 대체)
7. 채널의 모든 프로그램 조회
8. 프로그램의 메타정보 조회
9. CSV의 PGM_CODE → 의미적 정보 통합
"""
import re
import csv
from pathlib import Path
from collections import defaultdict
from validate import parse_turtle
from raas_paths import get_csv_path, get_ontology_paths

# v2는 KPI + 도메인 2개만 사용 (시간 스키마 제외)
ONTO_FILES = [p for p in get_ontology_paths()
              if p.name in ("raas_kpi_ontology.ttl", "raas_domain_entities.ttl")]
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

    def all_labels(self, subject):
        """rdfs:label + raas:altLabel 모두 (텍스트만)"""
        result = []
        for p in ("rdfs:label", "raas:altLabel"):
            for v in self.get(subject, p):
                clean = re.sub(r'^"|"(?:@\w+)?$', '', v)
                result.append(clean)
        return result

    def value_str(self, raw):
        if not raw:
            return ""
        return re.sub(r'^"|"(?:@\w+)?$', '', raw)


# ─── 새 데모들 ─────────────────────────────────────────────────────────────

def demo6_search_program_by_keyword(onto, keyword):
    """프로그램 별칭으로 코드 검색 (현재 KEYWORD_TO_CODE의 if문 체인 대체)"""
    print(f"\n[데모 6] 키워드 '{keyword}'로 프로그램/채널 검색")
    print("-" * 60)

    # Platform/Channel/Program 모두 대상
    all_subjects = (onto.instances_of("raas:Platform")
                    + onto.instances_of("raas:Channel")
                    + onto.instances_of("raas:Program"))

    matches = []
    for subj in all_subjects:
        for lbl in onto.all_labels(subj):
            if keyword.lower() in lbl.lower():
                matches.append((subj, lbl))
                break

    if matches:
        print(f"  매칭 {len(matches)}건:")
        for subj, lbl in matches:
            code = onto.value_str(onto.get_one(subj, "raas:code"))
            cls = "?"
            for c in ("raas:Platform", "raas:Channel", "raas:Program"):
                if subj in onto.instances_of(c):
                    cls = c.replace("raas:", "")
                    break
            main_label = onto.label_ko(subj)
            print(f"    [{cls}] {code} ({main_label}) — 매칭: \"{lbl}\"")
    else:
        print(f"  매칭 없음")


def demo7_programs_of_channel(onto, channel_code):
    """채널의 모든 프로그램 조회"""
    print(f"\n[데모 7] 채널 {channel_code}의 모든 프로그램")
    print("-" * 60)

    channel_subj = f"raas:{channel_code}"
    ch_label = onto.label_ko(channel_subj)
    nature = onto.value_str(onto.get_one(channel_subj, "raas:channelNature"))

    print(f"  채널: {channel_subj} ({ch_label})")
    if nature:
        print(f"  성격: {nature}")

    progs = onto.find_subjects("raas:belongsToChannel", channel_subj)
    print(f"  소속 프로그램: {len(progs)}개")

    if progs:
        # 시작 시간으로 정렬
        progs_with_time = []
        for p in progs:
            t = onto.value_str(onto.get_one(p, "raas:airTimeStr"))
            progs_with_time.append((t, p))
        progs_with_time.sort()

        for t, p in progs_with_time:
            label = onto.label_ko(p)
            code = onto.value_str(onto.get_one(p, "raas:code"))
            note = onto.value_str(onto.get_one(p, "raas:scheduleNote"))
            note_str = f"  [{note}]" if note else ""
            print(f"    {t}  {code}: {label}{note_str}")
    else:
        print(f"  (이 채널은 프로그램 단위 편성이 없음)")


def demo8_program_full_info(onto, program_code):
    """프로그램의 모든 메타정보 통합 조회"""
    print(f"\n[데모 8] 프로그램 {program_code}의 모든 정보")
    print("-" * 60)

    p = f"raas:{program_code}"
    label = onto.label_ko(p)
    code = onto.value_str(onto.get_one(p, "raas:code"))
    channel = onto.get_one(p, "raas:belongsToChannel")
    air_time = onto.value_str(onto.get_one(p, "raas:airTimeStr"))
    schedule = onto.value_str(onto.get_one(p, "raas:scheduleNote"))
    nature = onto.value_str(onto.get_one(p, "raas:broadcastNature"))
    alts = [onto.value_str(v) for v in onto.get(p, "raas:altLabel")]

    print(f"  코드: {code}")
    print(f"  정식 명칭: {label}")
    if channel:
        ch_label = onto.label_ko(channel)
        print(f"  소속 채널: {channel} ({ch_label})")
    print(f"  방송 시작: {air_time}")
    if schedule:
        print(f"  편성: {schedule}")
    if nature:
        print(f"  성격: {nature}")
    if alts:
        print(f"  별칭 ({len(alts)}개):")
        for a in alts:
            print(f"    - {a}")


def demo9_query_simulation(onto, natural_question):
    """자연어 질문 시뮬레이션 — 키워드로 대상을 찾아서 KPI 데이터 추출"""
    print(f"\n[데모 9] 자연어 질의 시뮬레이션")
    print(f"  질문: \"{natural_question}\"")
    print("-" * 60)

    # 1) 질문에서 도메인 엔티티 키워드 추출
    all_subjects = (onto.instances_of("raas:Platform")
                    + onto.instances_of("raas:Channel")
                    + onto.instances_of("raas:Program"))

    found_subjects = []
    for subj in all_subjects:
        for lbl in onto.all_labels(subj):
            if len(lbl) >= 2 and lbl in natural_question:
                found_subjects.append((subj, lbl))
                break

    if not found_subjects:
        print(f"  → 인식된 대상 없음")
        return
    print(f"  → 인식된 대상 {len(found_subjects)}개:")
    for subj, matched in found_subjects:
        code = onto.value_str(onto.get_one(subj, "raas:code"))
        main_label = onto.label_ko(subj)
        print(f"     {code} ({main_label}) — \"{matched}\" 키워드 매칭")

    # 2) 첫 번째 대상의 최근 DAU 조회
    if found_subjects and CSV_PATH is not None and CSV_PATH.exists():
        target_subj, _ = found_subjects[0]
        target_code = onto.value_str(onto.get_one(target_subj, "raas:code"))
        target_label = onto.label_ko(target_subj)

        latest_date = None
        latest_dau = None
        with open(CSV_PATH, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            date_idx = header.index("DATE")
            code_idx = header.index("PGM_CODE")
            dau_idx = header.index("dau")

            rows = []
            for row in reader:
                if row[code_idx] == target_code and row[dau_idx]:
                    rows.append((row[date_idx], row[dau_idx]))

            if rows:
                rows.sort(key=lambda x: x[0], reverse=True)
                latest_date, latest_dau = rows[0]

        print(f"\n  → 최신 DAU 조회 결과:")
        if latest_dau:
            print(f"     {target_label} ({target_code}): {int(latest_dau):,}명 ({latest_date})")
        else:
            print(f"     데이터 없음")


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("RAAS 온톨로지 사용 예시 v2 (Phase 2-C-1 데모)")
    print("=" * 70)

    onto = Ontology(ONTO_FILES)
    print(f"\n온톨로지 로드: {len(onto.triples)}개 트리플")

    # 도메인 엔티티 활용 데모
    demo6_search_program_by_keyword(onto, "컬투")
    demo6_search_program_by_keyword(onto, "김영철")
    demo6_search_program_by_keyword(onto, "드라이브")
    demo6_search_program_by_keyword(onto, "정엽")

    demo7_programs_of_channel(onto, "F00")
    demo7_programs_of_channel(onto, "G00")  # 비편성 채널

    demo8_program_full_info(onto, "F09")    # 컬투쇼
    demo8_program_full_info(onto, "M10")    # 주말 전용

    demo9_query_simulation(onto, "어제 컬투쇼 청취자 얼마나 됐어?")
    demo9_query_simulation(onto, "정엽 프로그램 추세 어때?")
    demo9_query_simulation(onto, "파워FM 전체 DAU 알려줘")

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
