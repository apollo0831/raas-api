"""플래너 shadow 평가 (P-1a) — raas_planner가 대표 질의를 올바른 PlanRequest로 푸는가.

실제 Haiku LLM 호출(플래너만). 프로덕션 경로는 안 건드림 — plan 정확도만 측정해 P-1b 전환 판단.
코어 필드(intent·domain·metric·entity.code) 일치율을 보고, 케이스로 회귀 고정.
실행: python tests/eval_planner.py   (LLM 변동성 있으므로 exit는 항상 0, 점수만 출력)
"""
import io, sys, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import raas_planner as P

# (질의, 기대 코어). 값이 set이면 그 중 하나면 정답(모호 허용). None은 채점 제외.
CASES = [
    ("어제 컬투쇼 DAU는?",              {"intent": {"snapshot", "series"}, "domain": "daily", "metric": "dau", "code": "F09"}),
    ("이번 주 전체 DAU 추이 보여줘",     {"intent": {"series"}, "domain": "daily", "metric": "dau", "code": "T00"}),
    ("프로그램별 DAU 순위",            {"intent": "ranking", "domain": "daily", "metric": "dau"}),
    ("남성 비율 가장 높은 프로그램",     {"intent": "ranking", "domain": "daily", "metric": "gender_dist"}),
    ("지금 동시사용자 몇 명이야?",       {"intent": {"snapshot", "series"}, "domain": "realtime", "metric": "concurrent"}),
    ("2026-03-20 파워FM 분당 동시자 추이", {"intent": {"series"}, "domain": "realtime", "metric": "concurrent", "code": "F00"}),
    ("2026년 4월1일 컬투쇼 동시사용자",   {"domain": "realtime", "metric": "concurrent", "code": "F09"}),
    ("컬투쇼 문자 참여 얼마나 돼?",      {"domain": "daily", "metric": "sms", "code": "F09"}),
    ("컬투쇼 어제 성별 분포",           {"domain": "daily", "metric": "gender_dist", "code": "F09"}),
    ("컬투쇼 DAU가 무엇과 상관 있나",    {"intent": "correlate", "domain": "daily", "metric": "dau", "code": "F09"}),
    ("파워FM vs 러브FM 비교",          {"intent": "compare", "domain": "daily"}),
    ("동시사용자 뽑아줘",              {"intent": "extract", "domain": "realtime", "metric": "concurrent", "format": "extract"}),
    ("러브FM 편성 변화 알려줘",         {"intent": "editorial", "code": "L00"}),
    ("RAAS에 어떤 데이터 있어?",        {"intent": "meta"}),
    ("철파엠 이탈률 어때?",            {"domain": "daily", "metric": "churn_rate", "code": "F05"}),
    ("지난 30일 컬투쇼 연령대 분포",     {"domain": "daily", "metric": "age_dist", "code": "F09"}),
    ("2026-04-01 파워FM 성별 동시추이",  {"domain": "realtime", "metric": "sex_ratio", "code": "F00"}),
    ("어제 방송 특이사항 알려줘",        {"intent": "digest"}),
]


def _match(got, exp):
    if exp is None:
        return True
    return got in exp if isinstance(exp, set) else got == exp


def _get(plan, key):
    if key == "code":
        return (plan.get("entity") or {}).get("code")
    return plan.get(key)


def main():
    print(f"플래너 shadow 평가 — {len(CASES)}케이스 (실제 Haiku 호출)\n")
    field_hit = {}; field_tot = {}
    full_ok = 0
    for q, exp in CASES:
        r = P.plan(q)
        plan = r.get("plan") or {}
        per = {}
        for k, ev in exp.items():
            got = _get(plan, k)
            hit = _match(got, ev)
            per[k] = (hit, got)
            field_hit[k] = field_hit.get(k, 0) + (1 if hit else 0)
            field_tot[k] = field_tot.get(k, 0) + 1
        allhit = all(h for h, _ in per.values())
        full_ok += 1 if allhit else 0
        mark = "✓" if allhit else "✗"
        miss = " ".join(f"{k}={g!r}!={exp[k]!r}" for k, (h, g) in per.items() if not h)
        print(f"  {mark} {q}")
        if not allhit:
            print(f"      {miss}   (conf={plan.get('confidence')}, issues={r.get('issues')})")
    print(f"\n전체 plan 정답(코어 전필드): {full_ok}/{len(CASES)} ({full_ok/len(CASES)*100:.0f}%)")
    print("필드별 정확도:", {k: f"{field_hit[k]}/{field_tot[k]}" for k in field_tot})


if __name__ == "__main__":
    main()
