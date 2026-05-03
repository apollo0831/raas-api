"""
RAAS KPI 온톨로지 검증 스크립트 v2 (자체 Turtle 파서)

목적:
1. Turtle 파일을 정확히 파싱 (문자열 내 마침표 처리)
2. 정의된 모든 클래스/속성/인스턴스 카운트
3. 모든 fieldName 추출 → CSV 컬럼과 매칭 검증
"""
import re
import csv
from pathlib import Path
from collections import defaultdict
from raas_paths import get_csv_path

ONTO_PATH = Path(__file__).parent / "raas_kpi_ontology.ttl"
CSV_PATH = get_csv_path()


# ─── Turtle 토크나이저 ──────────────────────────────────────────────────────

def tokenize(text):
    """Turtle 텍스트 → 토큰 리스트.
    토큰 종류: TERM, STR, PUNCT (.,;)"""
    tokens = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c in " \t\r\n":
            i += 1
            continue
        if c == "#":
            while i < n and text[i] != "\n":
                i += 1
            continue
        # @prefix 등 디렉티브는 통째로 스킵
        # IRI(<...>) 안에 마침표가 있을 수 있으므로 IRI를 인식하면서 스킵
        if c == "@":
            j = i
            while j < n:
                if text[j] == "<":
                    # IRI는 통째로 건너뜀
                    while j < n and text[j] != ">":
                        j += 1
                    j += 1  # > 자체도 넘김
                    continue
                if text[j] == ".":
                    break
                j += 1
            i = j + 1
            continue
        # 문자열
        if c == '"':
            j = i + 1
            while j < n:
                if text[j] == "\\" and j + 1 < n:
                    j += 2; continue
                if text[j] == '"':
                    j += 1; break
                j += 1
            # 언어 태그
            if j < n and text[j] == "@":
                while j < n and text[j] not in " \t\r\n,;.":
                    j += 1
            tokens.append(("STR", text[i:j]))
            i = j
            continue
        # 구두점
        if c in ".,;":
            tokens.append(("PUNCT", c))
            i += 1
            continue
        # IRI
        if c == "<":
            j = i + 1
            while j < n and text[j] != ">":
                j += 1
            tokens.append(("TERM", text[i:j + 1]))
            i = j + 1
            continue
        # 일반 토큰
        j = i
        while j < n and text[j] not in " \t\r\n.,;\"<>#":
            j += 1
        tokens.append(("TERM", text[i:j]))
        i = j
    return tokens


# ─── 토큰 → 트리플 ─────────────────────────────────────────────────────────

def parse_triples(tokens):
    """단순 Turtle 구조: subject pred obj [;pred obj]* [, obj]* . 만 처리."""
    triples = []
    i, n = 0, len(tokens)

    while i < n:
        if tokens[i][0] != "TERM":
            i += 1
            continue
        subject = tokens[i][1]
        i += 1

        current_pred = None
        while i < n:
            t_type, t_val = tokens[i]
            if t_type == "PUNCT":
                if t_val == ".":
                    i += 1
                    break
                if t_val == ";":
                    current_pred = None
                    i += 1
                    continue
                if t_val == ",":
                    i += 1
                    continue
            else:
                if current_pred is None:
                    current_pred = t_val
                else:
                    triples.append((subject, current_pred, t_val))
                i += 1

    return triples


def parse_turtle(path):
    text = path.read_text(encoding="utf-8")
    tokens = tokenize(text)
    return parse_triples(tokens)


# ─── 메인 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("RAAS KPI 온톨로지 검증")
    print("=" * 70)

    if not ONTO_PATH.exists():
        print(f"[오류] 온톨로지 파일 없음: {ONTO_PATH}")
        return

    triples = parse_turtle(ONTO_PATH)
    print(f"\n[1] 파싱 결과: {len(triples)}개 트리플")

    # 인스턴스 분류
    instances_by_class = defaultdict(list)
    for s, p, o in triples:
        if p in ("a", "rdf:type"):
            instances_by_class[o].append(s)

    # ─── [2] 클래스별 인스턴스 검증 ───
    print(f"\n[2] 클래스별 인스턴스 카운트")
    expected = {
        "owl:Class":             18,
        "owl:ObjectProperty":    17,
        "owl:DatatypeProperty":  17,
        "raas:Metric":           11,
        "raas:MetricVariant":    40,
        "raas:MeasureType":       2,
        "raas:CalendarGranularity": 3,
        "raas:RollingGranularity":  2,
        "raas:Variant":           6,
        "raas:Cohort":            2,
        "raas:RetentionWindow":   4,
    }
    pass_count = 0
    for cls, exp in expected.items():
        actual = len(instances_by_class.get(cls, []))
        mark = "PASS" if actual == exp else ("WARN" if actual > 0 else "FAIL")
        if mark == "PASS":
            pass_count += 1
        print(f"  [{mark}] {cls:30s} 기대 {exp:3d} / 실제 {actual:3d}")

    print(f"\n  → {pass_count}/{len(expected)} 항목 PASS")

    # ─── [3] fieldName 추출 ───
    print(f"\n[3] fieldName 추출")
    field_names = []
    for s, p, o in triples:
        if p == "raas:fieldName":
            # 따옴표 제거 + 언어 태그 제거
            o_clean = re.sub(r'^"|"(?:@\w+)?$', '', o)
            field_names.append((s, o_clean))
    print(f"  총 {len(field_names)}개")

    # 중복 체크
    fn_subjects = defaultdict(list)
    for s, fn in field_names:
        fn_subjects[fn].append(s)
    dups = {fn: ss for fn, ss in fn_subjects.items() if len(ss) > 1}
    if dups:
        print(f"  [WARN] 중복 {len(dups)}건:")
        for fn, ss in list(dups.items())[:5]:
            print(f"    \"{fn}\": {ss}")
    else:
        print(f"  [PASS] 중복 없음")

    # ─── [4] CSV 매칭 ───
    print(f"\n[4] CSV 컬럼 매칭")
    if CSV_PATH is None or not CSV_PATH.exists():
        print(f"  [건너뜀] CSV 없음 (raas_kpi_latest.csv를 같은 폴더 또는 ../data/에 두세요)")
        return

    with open(CSV_PATH, encoding="utf-8") as f:
        header = next(csv.reader(f))

    META_COLS = {"DATE", "DATE_WEEK", "DATE_MON", "PGM_CODE", "PGM_NAME",
                 "STIME", "program_title", "guestname",
                 "daily_corner", "weekly_corner"}
    kpi_cols = [c for c in header if c not in META_COLS]
    print(f"  CSV KPI 컬럼: {len(kpi_cols)}개")

    SUFFIXES = ["", "_prev", "_chg", "_diff", "_share", "_d2"]
    base_fns = set(fn for _, fn in field_names)

    expressible = set()
    for fn in base_fns:
        for suf in SUFFIXES:
            expressible.add(fn + suf)

    matched = [c for c in kpi_cols if c in expressible]
    unmatched = [c for c in kpi_cols if c not in expressible]

    print(f"  매칭 성공: {len(matched)}개 ({len(matched)/len(kpi_cols)*100:.1f}%)")
    print(f"  매칭 실패: {len(unmatched)}개")

    if unmatched:
        print(f"\n  [미매칭 컬럼]")
        # 패턴별 그룹화
        pattern_groups = defaultdict(list)
        for c in unmatched:
            # _diff, _chg 등 접미사 추출
            for suf in ["_diff", "_chg", "_prev", "_share", "_d2"]:
                if c.endswith(suf):
                    base = c[:-len(suf)]
                    pattern_groups[suf].append((c, base))
                    break
            else:
                pattern_groups["_base"].append((c, c))

        for suf, items in sorted(pattern_groups.items()):
            print(f"    [{suf}] {len(items)}개")
            for c, base in items[:5]:
                in_onto = "✓" if base in base_fns else "✗"
                print(f"      {c:30s} (base: {base} {in_onto})")
            if len(items) > 5:
                print(f"      ... 외 {len(items) - 5}개")

    # ─── [5] 정의된 베이스 fieldName 목록 ───
    print(f"\n[5] 정의된 베이스 fieldName 목록 ({len(base_fns)}개)")
    for fn in sorted(base_fns):
        in_csv = "✓" if fn in kpi_cols else " "
        print(f"  [{in_csv}] {fn}")

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
