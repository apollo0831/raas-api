"""LLM 질의 플래너 (P-1a) — raas_planner.

자연어 질의 → 검증된 구조 요청(PlanRequest). 흩어진 키워드 감지기 대신 LLM이 질의+카탈로그를
읽고 '무엇을 뽑을지'를 구조로 낸다. **값은 만들지 않는다**(실행기·접근자가 결정적으로 계산).

P-1a 범위: 스키마 + 플래너(LLM) + 검증기 + shadow 평가. **프로덕션 미연결**(assemble 안 바꿈).
카탈로그(메뉴)는 레지스트리·온톨로지에서 자동 구성 → TTL/레지스트리 확장이 곧 플래너 능력 확장.
"""
import json
import re

_CODE_RX = re.compile(r"^[TFLGPMR][0-9]{2}$")

# ── 스키마 열거 ──────────────────────────────────────────────────────────────
INTENTS = ("series", "snapshot", "ranking", "compare", "correlate",
           "extract", "editorial", "meta", "digest")
DOMAINS = ("daily", "realtime")
FORMATS = ("answer", "chart", "extract")

# 일간 지표 어휘(키→라벨) — 실행기가 아는 정규 키. (P-1b에서 레지스트리/온톨로지 유도로 대체 예정)
_DAILY_METRICS = {
    "dau": "활성사용자(DAU/WAU/MAU)", "new": "신규사용자", "react": "복귀사용자",
    "churn_rate": "이탈률", "react_rate": "복귀율", "real_rate": "실청취율",
    "deep_rate": "깊은청취율", "engage_rate": "참여율", "habit_rate": "습관형성률",
    "d7_ret": "유지율(D1/D7/W1/M1)", "sms": "문자참여", "gg": "공감로그참여",
    "gender_dist": "성별 분포", "age_dist": "연령대 분포", "device_dist": "디바이스 분포",
}
_CHANNELS = {"전체": "T00", "전사": "T00", "파워FM": "F00", "러브FM": "L00",
             "고릴라M": "G00", "고릴라엠": "G00", "픽채널": "P00"}


def _rt_metric_desc():
    try:
        import raas_rt_series as RT
        d = {"concurrent": "실시간 동시사용자", "device": "실시간 디바이스별", "viewradio": "보는라디오",
             "sex_ratio": "인증 성별비율", "age_ratio": "인증 연령비율",
             "msg_sms": "분당 문자", "msg_gg": "분당 공감로그", "inflow": "분당 유입"}
        return {k: d.get(k, k) for k in RT.RT_METRICS}
    except Exception:
        return {}


def build_catalog() -> str:
    """플래너 프롬프트에 넣는 컴팩트 메뉴 — 레지스트리·온톨로지에서 구성(확장 시 자동 반영)."""
    rt = _rt_metric_desc()
    lines = ["[지표 어휘 — metric 키는 반드시 아래에서]"]
    lines.append("· 일간(domain=daily): " + ", ".join(f"{k}({v})" for k, v in _DAILY_METRICS.items()))
    lines.append("· 실시간(domain=realtime): " + ", ".join(f"{k}({v})" for k, v in rt.items()))
    lines.append("[제약] 실시간 sex_ratio·age_ratio·msg_*는 파워FM(F00)·러브FM(L00)만. "
                 "성별·연령은 '동시자 수'가 아니라 인증자 '비율(%)'만 존재.")
    lines.append("[엔티티] 채널: " + ", ".join(f"{n}={c}" for n, c in _CHANNELS.items() if n not in ('전사', '고릴라엠'))
                 + ". 프로그램은 이름/별칭 그대로 entity.ref에 (예: '컬투쇼','철파엠','아침봉') — 코드는 검증기가 해석.")
    lines.append("[차원 dims] device(SP/PC/PW/MWEB/WATCH/CAR/AI) · sex(F/M) · age(0_19…60) · depth(ALL/1MIN/10MIN, 분포)")
    return "\n".join(lines)


PLAN_SYSTEM = (
    "너는 SBS 고릴라 라디오 분석 질의의 '플래너'다. 사용자 질의를 읽고 '무엇을 어떻게 뽑을지'를 "
    "JSON PlanRequest로만 출력한다. **값(숫자)은 만들지 마라** — 데이터는 실행기가 뽑는다.\n"
    "스키마: {intent, domain, metric, entity:{kind,ref}, time:{when,date?,lookback_days?}, "
    "period?(1D|1W|1M), resolution?(1|10), dims?:{device?,sex?,age?,depth?}, format, "
    "compare_with?[], factors?, confidence(0~1)}\n"
    f"intent∈{INTENTS} · domain∈{DOMAINS} · format∈{FORMATS}.\n"
    "규칙: 지금/실시간/동시사용자/분당 → domain=realtime. 순위/가장 높은 → intent=ranking. "
    "무엇과 상관/같이 움직 → correlate. 뽑아/엑셀/표로 → format=extract. 편성 변화/역대 → editorial. "
    "어떤 지표/데이터 있나 → meta. 어제 방송 특이사항/이상 → digest. "
    "성별·연령·디바이스 '분포/순위/비교'(프로그램 단위)는 domain=daily(gender_dist/age_dist/device_dist); "
    "실시간 sex_ratio/age_ratio는 '지금/실시간/특정일 채널 추이'일 때만. "
    "entity.kind∈{program,channel,all}, ref는 이름/별칭 그대로. "
    "특정일이면 time.when=date, time.date='YYYY-MM-DD'(연도 없으면 생략). "
    "확신 낮으면 confidence를 낮게. JSON만, 설명 금지.\n\n"
    "[예시]\n"
    "Q: 어제 컬투쇼 DAU → {\"intent\":\"snapshot\",\"domain\":\"daily\",\"metric\":\"dau\","
    "\"entity\":{\"kind\":\"program\",\"ref\":\"컬투쇼\"},\"time\":{\"when\":\"relative\",\"lookback_days\":1},"
    "\"format\":\"answer\",\"confidence\":0.9}\n"
    "Q: 2026-04-10 아침봉 1분 성별 동시추이 뽑아줘 → {\"intent\":\"extract\",\"domain\":\"realtime\","
    "\"metric\":\"sex_ratio\",\"entity\":{\"kind\":\"program\",\"ref\":\"아침봉\"},"
    "\"time\":{\"when\":\"date\",\"date\":\"2026-04-10\"},\"resolution\":1,\"dims\":{\"sex\":\"F\"},"
    "\"format\":\"extract\",\"confidence\":0.85}\n"
    "Q: 남성 비율 가장 높은 프로그램 → {\"intent\":\"ranking\",\"domain\":\"daily\",\"metric\":\"gender_dist\","
    "\"entity\":{\"kind\":\"all\",\"ref\":\"전체\"},\"dims\":{\"sex\":\"M\"},\"format\":\"answer\",\"confidence\":0.8}"
)


def _resolve_entity(ref, kind):
    """entity.ref(이름/별칭) → 코드. 프로그램은 extract_program, 채널은 맵. 실패 None."""
    ref = (ref or "").strip()
    if not ref or ref in ("전체", "전사", "all", "T00"):
        return "T00"
    if _CODE_RX.match(ref.upper()):          # 플래너가 코드를 직접 낸 경우 그대로
        return ref.upper()
    if ref in _CHANNELS:
        return _CHANNELS[ref]
    try:
        from raas_storyline_router import extract_program
        p = extract_program(ref)
        if p:
            return p["code"]
    except Exception:
        pass
    return None


def validate(raw: dict, question: str) -> tuple:
    """LLM plan → (정규화 plan, issues[]). 등록된 것만 통과, 엔티티·시간 결정적 해석."""
    issues = []
    p = dict(raw or {})
    intent = p.get("intent")
    if intent not in INTENTS:
        issues.append(f"unknown intent:{intent}")
    domain = p.get("domain") if p.get("domain") in DOMAINS else None
    if not domain:
        issues.append(f"unknown domain:{p.get('domain')}")
    metric = p.get("metric")
    known = set(_DAILY_METRICS) | set(_rt_metric_desc())
    if intent not in ("meta", "digest", "editorial") and metric not in known:
        issues.append(f"unknown metric:{metric}")
    # 엔티티 해석
    ent = p.get("entity") or {}
    code = _resolve_entity(ent.get("ref"), ent.get("kind"))
    if code is None and intent not in ("meta", "digest", "ranking"):
        issues.append(f"unresolved entity:{ent.get('ref')}")
    # 시간 결정적 확정
    t = p.get("time") or {}
    when = t.get("when")
    date = None
    if when == "date":
        try:
            from raas_grounding import _parse_abs_date
            cand = _parse_abs_date(t.get("date") or question)
            if cand and cand[0]:
                date = "%04d-%02d-%02d" % cand
        except Exception:
            pass
        if t.get("date"):
            date = date or t["date"]
    norm = {
        "intent": intent, "domain": domain, "metric": metric,
        "entity": {"kind": ent.get("kind"), "ref": ent.get("ref"), "code": code},
        "time": {"when": when, "date": date, "lookback_days": t.get("lookback_days")},
        "period": p.get("period"), "resolution": p.get("resolution"),
        "dims": p.get("dims") or {}, "format": p.get("format") if p.get("format") in FORMATS else "answer",
        "compare_with": p.get("compare_with") or [], "factors": p.get("factors"),
        "confidence": p.get("confidence"),
    }
    return norm, issues


def plan(question: str, call_claude=None) -> dict:
    """질의 → {plan, issues, raw, ok}. call_claude 없으면 raas_llm 사용. 값은 만들지 않음."""
    if call_claude is None:
        from raas_llm import call_claude as _cc, HAIKU_MODEL
        call_claude = lambda s, u: _cc(s, u, max_tokens=350, model=HAIKU_MODEL)
    user = build_catalog() + f"\n\n질의: {question}\nPlanRequest(JSON):"
    raw = {}
    try:
        text, _ = call_claude(PLAN_SYSTEM, user)
        raw = json.loads(text[text.find("{"): text.rfind("}") + 1])
    except Exception as e:
        return {"plan": None, "issues": [f"planner error:{e}"], "raw": None, "ok": False}
    norm, issues = validate(raw, question)
    return {"plan": norm, "issues": issues, "raw": raw, "ok": not issues}
