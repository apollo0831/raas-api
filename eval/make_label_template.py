"""
Phase 0-3 준비: baseline_v1_answers.jsonl + baseline_v1.jsonl을 합쳐서
엑셀에서 라벨링할 수 있는 CSV 템플릿을 만든다.

- UTF-8 BOM 인코딩 (Excel 한글 안전)
- 매뉴얼 컬럼은 비워두고, 자동 추정값은 사전 채움
- 컬럼 순서: 핵심 정보 → 매뉴얼 평가 → 부가 정보

산출물: eval/label_v1_template.csv
"""
import csv
import json
from pathlib import Path

EVAL_DIR = Path(__file__).parent
QUESTIONS = EVAL_DIR / "baseline_v1.jsonl"
ANSWERS = EVAL_DIR / "baseline_v1_answers.jsonl"
OUTPUT = EVAL_DIR / "label_v1_template.csv"

REFUSE_KEYWORDS = [
    "해당 데이터가 없",
    "죄송합니다",
    "제공된 데이터",
    "확인할 수 없",
    "포함되어 있지 않",
    "데이터로는",
    "답변할 수 없",
]


def is_refuse(answer: str) -> bool:
    head = (answer or "")[:250]
    return any(kw in head for kw in REFUSE_KEYWORDS)


def suggest_failure_mode(row_q: dict, row_a: dict) -> str:
    """자동 추정 실패 모드 (사람이 검토·수정)"""
    if not row_a["ok"]:
        return "system_error"
    answer = row_a.get("answer", "") or ""
    if is_refuse(answer):
        # 카테고리로 data vs context 가설 분기
        cat = row_q["category"]
        if cat in ("ontology_meta", "reorganization_effect", "holiday_event"):
            return "data_missing"
        if cat in ("risk",):
            # risk는 s5_rankings에 데이터 있는데 거부 → 컨텍스트 누락 의심
            return "context_missing"
        if cat in ("causal",):
            return "data_missing"
        return "data_missing"
    if row_a["answer_len"] < 50:
        return "format_bad"
    return ""  # 사람이 채워야 함


def main():
    questions = {json.loads(l)["id"]: json.loads(l)
                 for l in QUESTIONS.open(encoding="utf-8")}
    answers = {json.loads(l)["id"]: json.loads(l)
               for l in ANSWERS.open(encoding="utf-8")}

    columns = [
        # 핵심 정보 (좌측)
        "id", "category", "difficulty", "source",
        "question", "answer",
        # 매뉴얼 평가 (사람이 채움)
        "accuracy_0_1_2", "completeness_0_1_2", "usefulness_0_1_2",
        "format_0_1_2", "failure_mode", "note",
        # 자동 추정 / 부가 정보
        "failure_mode_suggest", "ok", "has_chart", "answer_len",
        "elapsed_sec", "auto_refuse",
        "expected_data", "hypothesis",
    ]

    with OUTPUT.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(columns)
        for qid in sorted(questions.keys()):
            q = questions[qid]
            a = answers.get(qid, {})
            answer = a.get("answer", "") or (f"[ERROR] {a.get('error','no answer')}"
                                              if a else "[MISSING]")
            row = [
                qid,
                q["category"],
                q["difficulty"],
                q.get("source", ""),
                q["question"],
                answer,
                # 매뉴얼 칸 (빈칸)
                "", "", "", "", "", "",
                # 자동 채움
                suggest_failure_mode(q, a) if a else "missing",
                "Y" if a.get("ok") else "N",
                "Y" if a.get("has_chart") else "",
                a.get("answer_len", 0),
                a.get("elapsed_sec", ""),
                "Y" if is_refuse(answer) else "",
                "|".join(q.get("expected_data", [])),
                q.get("notes", ""),
            ]
            w.writerow(row)

    print(f"생성 완료: {OUTPUT}")
    print(f"행 수: {len(questions)}")


if __name__ == "__main__":
    main()
