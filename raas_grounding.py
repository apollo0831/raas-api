# -*- coding: utf-8 -*-
"""RAAS 검색·Grounding 레이어 (docs/grounding_retrieval_design.md)

목표: 어떤 자유 질의가 오든, 관련 데이터·온톨로지를 LLM context에 골라 넣어
LLM이 본연의 성능으로 답하게 한다. 고정 출력 템플릿 없음.

흐름: 질문 → 엔티티 해석 → (LLM이) provider 선택 → fetch(구조화 데이터)
      → 온톨로지 grounding 팩 → 맥락 조립 → (서버가) LLM 생성
"""
from __future__ import annotations
import json
import re
import random
from typing import Optional

import raas_storyline_engine as S
from raas_storyline_router import extract_program

try:
    from raas_query_engine import call_claude, HAIKU_MODEL
except Exception:
    call_claude = None
    HAIKU_MODEL = None


# ─── 엔티티 해석 ────────────────────────────────────────────────────────────
_PERIOD_KO = {"day": "일간", "week": "주간", "month": "월간"}

def _detect_period(q: str) -> str:
    t = q or ""
    if any(k in t for k in ("월간", "전월", "지난달", "이번달", "MAU", "mau")):
        return "month"
    if any(k in t for k in ("주간", "전주", "지난주", "이번주", "WAU", "wau", "일주일", "주별")):
        return "week"
    return "day"


# 시계열 로드 범위를 질문 의도에 맞춰 결정. 0 = 가용 데이터 전부.
_LOOKBACK_UNIT = {"년": 366, "개월": 31, "달": 31, "주일": 7, "주": 7, "일": 1}

def _detect_lookback(q: str) -> int:
    """질문에서 필요한 시계열 범위(일) 추론. 명시 없으면 0(전체)."""
    t = q or ""
    m = re.search(r"(\d+)\s*(년|개월|달|주일|주|일)", t)
    if m:
        return _LOOKBACK_UNIT[m.group(2)] * int(m.group(1))
    if any(k in t for k in ("일주일", "지난주", "이번주", "최근 주")):
        return 28          # 최근 주 맥락 + 비교용 여유
    if any(k in t for k in ("한달", "이번달", "지난달", "최근 한 달")):
        return 62
    return 0               # 추이·전체·기본 → 가용 데이터 전부 (LLM이 필요한 만큼 사용)


# 채널 감지 — 특정 프로그램이 아닌 채널/전사 단위 질의 식별 (scope 확장)
_CH_NAME_BY_CODE = {v: k for k, v in S._CHANNEL_CODE.items()}
_CH_NAME_BY_CODE["T00"] = "전체"

def _detect_channel(q: str):
    t = q or ""
    for name, code in S._CHANNEL_CODE.items():        # 파워FM/러브FM/고릴라M/픽채널
        if name in t:
            return code, name
    for code in ("F00", "L00", "G00", "P00"):          # 코드 직접 언급
        if code in t:
            return code, _CH_NAME_BY_CODE.get(code, code)
    if any(k in t for k in ("전사", "전체 채널", "네트워크", "앱 전체", "고릴라 전체")):
        return "T00", "전체"
    return None, None


def resolve_entities(question: str) -> dict:
    """질문 → {program, code, name, channel, period, metric, lookback, row, history, date, scope_kind}.
       scope_kind: 'program'(특정 프로그램) | 'channel'(채널/전사) | None(미식별→기존 엔진)."""
    prog = extract_program(question)
    ent = {"program": prog, "period": _detect_period(question),
           "metric": S.extract_kpi_metric(question),
           "lookback": _detect_lookback(question), "scope_kind": None}
    code = None
    if prog:
        ent["scope_kind"] = "program"
        code = prog["code"]
        ent.update(code=code, name=prog["name"], channel=prog["channel"])
    else:
        ch_code, ch_name = _detect_channel(question)
        if ch_code:
            ent["scope_kind"] = "channel"
            code = ch_code
            ent.update(code=code, name=ch_name, channel=ch_code)
    if code:
        ent["row"] = S._load_program_latest_row(code)
        # 질문 의도 범위만큼(기본 전체) 로드. 분해·baseline provider도 이 history 공유.
        ent["history"] = S._load_program_history(code, ent["lookback"])
        ent["date"] = ((ent["row"] or {}).get("DATE") or "").replace("/", "-")
    return ent


# ─── Provider 레지스트리 ────────────────────────────────────────────────────
# 각 provider: name, desc(LLM 선택용), needs(필요 엔티티), fetch(ent)->data(JSON 직렬화 가능)
def _p_program_kpi(ent):
    r = ent.get("row") or {}
    keys = ["DATE", "STIME", "dau", "dau_chg", "wau", "wau_chg", "mau", "mau_chg",
            "new", "new_chg", "react", "react_chg", "churn_rate", "churn_rate_diff",
            "real_rate", "real_rate_diff", "deep_rate", "deep_rate_diff",
            "engage_rate", "habit_rate", "d1_ret", "d7_ret",
            # 채널 scope에서 의미 있는 주간/월간 변형(프로그램 행엔 없으면 자동 제외)
            "deep_rate_week", "deep_rate_mon", "real_rate_week", "real_rate_mon",
            "engage_rate_week", "engage_rate_mon", "churn_rate_week", "churn_rate_mon",
            "react_rate", "react_rate_week", "react_rate_mon",
            "habit_rate_week", "habit_rate_mon", "w1_ret", "m1_ret"]
    return {k: r.get(k) for k in keys if r.get(k) not in (None, "")}


def _p_metric_timeseries(ent):
    hist = ent.get("history") or []
    if len(hist) > 400:          # 안전 상한(데이터가 매우 길 때만). 보통 전체 제공.
        hist = hist[-400:]
    flds = ["dau", "wau", "mau", "deep_rate", "real_rate", "new", "churn_rate"]
    return [
        {"date": (h.get("DATE") or "").replace("/", "-"),
         **{f: h.get(f) for f in flds if h.get(f) not in (None, "")}}
        for h in hist
    ]


def _p_flow(ent):
    return S._compute_flow_decomposition(ent.get("row") or {}, ent.get("history"))


def _p_cohort(ent):
    return S._compute_cohort(ent.get("row") or {})


def _p_stickiness(ent):
    return S._compute_stickiness(ent.get("row") or {}, ent.get("history"))


def _p_programming(ent):
    return S._compute_programming_impact(ent.get("row") or {}, ent.get("history"))


def _p_revision(ent):
    rev = S._detect_program_revision(ent["code"]) if ent.get("code") else {}
    out = {k: rev.get(k) for k in ("daily_change", "signal")}
    out["new_corners"] = sorted(rev.get("new_c") or [])[:10]
    out["dropped_corners"] = sorted(rev.get("dropped_c") or [])[:10]
    return out


def _p_weekday(ent):
    return S._compute_weekday_pattern_check(ent.get("row") or {}, ent.get("history"))


def _p_schedule(ent):
    return S.build_program_schedule(ent["code"]) if ent.get("code") else None


def _p_calendar(ent):
    if not ent.get("date"):
        return None
    try:
        from raas_onto import get_adapter
        return get_adapter().get_day_type(ent["date"])
    except Exception:
        return None


PROVIDERS = [
    {"name": "program_kpi", "needs": "program",
     "desc": "프로그램의 최신 핵심 KPI 스냅샷(DAU/WAU/MAU·신규·복귀·이탈·실청취·깊은청취·유지율과 증감)",
     "fetch": _p_program_kpi},
    {"name": "metric_timeseries", "needs": "program",
     "desc": "주요 지표 시계열(질문 기간만큼, 기본은 가용 전체 — 추이·변동 추세)",
     "fetch": _p_metric_timeseries},
    {"name": "flow_decomp", "needs": "program",
     "desc": "활성사용자 변화를 신규·복귀·이탈로 분해(왜 늘었나/줄었나의 사용자 흐름 구조)",
     "fetch": _p_flow},
    {"name": "cohort", "needs": "program",
     "desc": "신규 사용자 코호트 리텐션(D1/D7/M1) — 신규가 잘 정착하는가",
     "fetch": _p_cohort},
    {"name": "stickiness", "needs": "program",
     "desc": "DAU/MAU 밀착도 — 변화가 확산형인지 기존 사용자 빈도형인지",
     "fetch": _p_stickiness},
    {"name": "programming", "needs": "program",
     "desc": "편성 영향(어제 vs 평소 같은 요일의 게스트·일일/주간 코너·생방송·보는라디오 차이)",
     "fetch": _p_programming},
    {"name": "revision", "needs": "program",
     "desc": "최근 코너 개편 감지(일일코너 변경·신설/폐지 코너)",
     "fetch": _p_revision},
    {"name": "weekday_pattern", "needs": "program",
     "desc": "요일 효과 — 이번 변화가 평소 같은 요일의 반복 패턴인지",
     "fetch": _p_weekday},
    {"name": "schedule", "needs": "program",
     "desc": "주간 편성표(요일별 코너 편성 + 매일 고정 코너)",
     "fetch": _p_schedule},
    {"name": "calendar", "needs": "date",
     "desc": "분석 일자의 유형 판정(평일/주말/공휴일·특일) — 청취 패턴에 영향",
     "fetch": _p_calendar},
]
_PROVIDER_BY_NAME = {p["name"]: p for p in PROVIDERS}


# 채널 scope에서 의미가 약하거나 프로그램 전용인 provider — 제외(노이즈 방지)
_PROGRAM_ONLY = {"programming", "revision", "schedule", "weekday_pattern"}

def _applicable(ent) -> list:
    """엔티티상 호출 가능한 provider만(예: program 필요한데 프로그램 없음 → 제외)."""
    is_channel = ent.get("scope_kind") == "channel"
    ok = []
    for p in PROVIDERS:
        need = p["needs"]
        if need == "program" and not ent.get("code"):
            continue
        if need == "date" and not ent.get("date"):
            continue
        if is_channel and p["name"] in _PROGRAM_ONLY:
            continue
        ok.append(p)
    return ok


# ─── LLM provider 선택 ──────────────────────────────────────────────────────
_SELECT_SYSTEM = (
    "당신은 RAAS 라디오 분석 어시스턴트의 '검색 플래너'입니다.\n"
    "사용자 질문에 가장 좋은 답을 하기 위해 아래 데이터 provider 중 **필요한 것만** 고르세요.\n"
    "- 단순 조회면 최소한(예: 지표 스냅샷/시계열)만. 인과·분석·종합이면 관련 분해 provider를 넉넉히.\n"
    "- 무관한 provider는 넣지 마세요(노이즈는 품질 저하).\n"
    'JSON으로만: {"providers": ["name", ...]}'
)

def select_providers(question: str, ent: dict) -> list:
    cands = _applicable(ent)
    names = [p["name"] for p in cands]
    if not names:
        return []
    if not call_claude:
        return names  # LLM 불가 시 전체(보수적)
    catalog = "\n".join(f"- {p['name']}: {p['desc']}" for p in cands)
    user = f"질문: {question}\n\n사용 가능한 provider:\n{catalog}"
    try:
        text, _ = call_claude(_SELECT_SYSTEM, user, max_tokens=200, model=HAIKU_MODEL)
        s = text[text.find("{"): text.rfind("}") + 1]
        picked = json.loads(s).get("providers") or []
        picked = [n for n in picked if n in _PROVIDER_BY_NAME and n in names]
        return picked or names
    except Exception as e:
        print(f"[grounding] select error: {e}")
        return names


# ─── 온톨로지 grounding 팩 ──────────────────────────────────────────────────
def ontology_pack(ent, provider_names) -> str:
    lines = []
    # 사용 지표 정의
    fields = list((_p_program_kpi(ent) or {}).keys()) if ent.get("row") else []
    defs = S._metric_definitions_lines(fields)
    if defs:
        lines += ["[지표 정의 (온톨로지)]"] + defs
    # 분해 프레임워크(분석형 provider가 선택됐을 때)
    if any(n in provider_names for n in ("flow_decomp", "cohort", "stickiness", "programming")):
        try:
            decs = S._query_decompositions()
            if decs:
                lines += ["", "[변화 분해 프레임워크 (cause 온톨로지)]"]
                lines += [f"- {d.get('label')}: {d.get('purpose')}" for d in decs]
        except Exception:
            pass
    return "\n".join(lines)


# ─── 지식 오버레이 (읽기시 병합) ────────────────────────────────────────────
_KTYPE_KO = {
    "metric_definition": "지표 정의 보정", "field_meaning": "필드 의미",
    "program_note": "프로그램 메모", "guest_policy": "게스트 정책",
    "corner_note": "코너 메모", "decomposition_hint": "분해 힌트", "fact": "사실",
}

def _fetch_overlay(targets, overlay_ctx) -> tuple:
    """주어진 (kind, id) 타깃들에 매칭되는 지식 항목을 끌어와 context 블록 + id 목록 반환.
       모든 scope(program/digest/channel/global …)에서 공유하는 단일 오버레이 게이트."""
    try:
        import raas_history_db as HDB
    except Exception:
        return "", []
    octx = overlay_ctx or {}
    try:
        items = HDB.get_knowledge_items(
            targets, contributor_id=octx.get("user_id"),
            include_candidate=(octx.get("mode") == "requery"))
    except Exception as e:
        print(f"[grounding] overlay error: {e}")
        items = []
    if not items:
        return "", []
    lines = ["## 사용자·운영 도메인 지식 (오버레이)",
             "아래는 사용자가 보강한 도메인 지식입니다. 답변에 적극 반영하세요."]
    for it in items:
        tag = "(본인 미승인)" if it.get("scope") == "candidate" else "(승인됨)"
        tk = _KTYPE_KO.get(it.get("type"), it.get("type"))
        tgt = it.get("target_id") or "전역"
        lines.append(f"- [{tk}·{tgt}] {it.get('content')} {tag}")
    return "\n".join(lines), [it["id"] for it in items]


def _entity_targets(ent) -> list:
    """엔티티 → 오버레이/업로드 매칭 타깃 [(kind,id)]."""
    targets = []
    if ent.get("scope_kind") == "channel":
        if ent.get("code"):
            targets.append(("channel", ent["code"]))
    else:
        if ent.get("code"):
            targets.append(("program", ent["code"]))
        if ent.get("channel"):
            targets.append(("channel", ent["channel"]))
    return targets


def _overlay_block(ent, overlay_ctx) -> tuple:
    """program/channel scope용 — 엔티티에서 타깃을 구성해 _fetch_overlay 호출."""
    targets = list(_entity_targets(ent))
    for f in (_p_program_kpi(ent) or {}).keys():
        targets.append(("field", f))
    return _fetch_overlay(targets, overlay_ctx)


def _fetch_uploads(targets, overlay_ctx) -> tuple:
    """타깃에 매칭되는 사용자 업로드 표 데이터를 markdown 블록으로. 반환 (text, [ids])."""
    try:
        import raas_history_db as HDB
    except Exception:
        return "", []
    octx = overlay_ctx or {}
    try:
        ups = HDB.get_uploaded_data(targets, contributor_id=octx.get("user_id"),
                                    include_candidate=(octx.get("mode") == "requery"))
    except Exception as e:
        print(f"[grounding] uploads error: {e}")
        ups = []
    if not ups:
        return "", []
    lines = ["## 사용자 업로드 데이터", "아래는 사용자가 올린 보조 데이터입니다. 질문과 관련되면 근거로 활용하세요."]
    for u in ups:
        try:
            cols = json.loads(u.get("columns_json") or "[]")
            rows = json.loads(u.get("rows_json") or "[]")
        except Exception:
            cols, rows = [], []
        tag = "(본인 미승인)" if u.get("scope") == "candidate" else "(승인됨)"
        lines.append(f"### {u.get('name') or '업로드'} · 대상 {u.get('target_id') or '전역'} {tag}")
        if cols:
            lines.append("| " + " | ".join(str(c) for c in cols) + " |")
            lines.append("| " + " | ".join("---" for _ in cols) + " |")
        for r in rows[:50]:
            lines.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(lines), [u["id"] for u in ups]


# ─── Compare scope — 두 대상 비교(A vs B) ───────────────────────────────────
#    프로그램·채널을 2~4개 감지해 나란히 비교. 같은 엔진(GROUNDING_SYSTEM·온톨로지·오버레이).
_COMPARE_SPLIT = re.compile(r"\s*(?:vs\.?|VS|대비|비교|차이|와|과|랑|이랑|,|그리고|對)\s*")

def _resolve_one(seg: str):
    """세그먼트 → 단일 엔티티(program 우선, 아니면 channel). 없으면 None."""
    p = extract_program(seg)
    if p:
        return {"kind": "program", "code": p["code"], "name": p["name"], "channel": p["channel"]}
    c, n = _detect_channel(seg)
    if c:
        return {"kind": "channel", "code": c, "name": n, "channel": c}
    return None


def _detect_compare(question: str):
    """비교 구분자로 분할해 서로 다른 엔티티 ≥2개면 비교 scope. 아니면 None."""
    ents, seen = [], set()
    for s in _COMPARE_SPLIT.split(question or ""):
        s = s.strip()
        if not s:
            continue
        e = _resolve_one(s)
        if e and e["code"] not in seen:
            seen.add(e["code"]); ents.append(e)
    return ents[:4] if len(ents) >= 2 else None


def _compare_ts(hist) -> list:
    hist = hist or []
    if len(hist) > 120:
        hist = hist[-120:]
    flds = ["dau", "wau", "mau", "deep_rate", "real_rate", "new", "churn_rate"]
    return [{"date": (h.get("DATE") or "").replace("/", "-"),
             **{f: h.get(f) for f in flds if h.get(f) not in (None, "")}} for h in hist]


def _assemble_compare(question, ents, overlay_ctx=None) -> dict:
    lookback = _detect_lookback(question)
    period = _detect_period(question)
    metric = S.extract_kpi_metric(question)
    blocks, kpi_fields = [], []
    for e in ents:
        row = S._load_program_latest_row(e["code"])
        hist = S._load_program_history(e["code"], lookback)
        el = {"code": e["code"], "row": row, "history": hist, "scope_kind": e["kind"]}
        kpi = _p_program_kpi(el) or {}
        if not kpi_fields:
            kpi_fields = list(kpi.keys())
        blocks.append(f"### {e['name']}({e['code']}) — 최신 KPI 스냅샷\n"
                      + json.dumps(kpi, ensure_ascii=False, default=str))
        blocks.append(f"### {e['name']}({e['code']}) — 시계열(최근)\n"
                      + json.dumps(_compare_ts(hist), ensure_ascii=False, default=str))
    head = ("비교 분석: " + " vs ".join(f"{e['name']}({e['code']})" for e in ents)
            + f" · 기간 힌트: {_PERIOD_KO.get(period, '일간')} · 비교지표: {metric or '핵심 지표'}")
    context = head + "\n\n" + "\n\n".join(blocks)
    defs = S._metric_definitions_lines(kpi_fields) if kpi_fields else []
    if defs:
        context += "\n\n## 온톨로지 근거\n[지표 정의 (온톨로지)]\n" + "\n".join(defs)
    targets = [(("channel" if e["kind"] == "channel" else "program"), e["code"]) for e in ents]
    otext, overlay_ids = _fetch_overlay(targets, overlay_ctx)
    if otext:
        context += "\n\n" + otext
    return {
        "ok": True, "context": context,
        "providers_used": ["compare_kpi", "compare_timeseries"],
        "entities_brief": head,
        "provenance": {"providers": ["compare_kpi", "compare_timeseries"], "scope": "compare",
                       "entities": [e["code"] for e in ents], "overlay_items": overlay_ids},
    }


# ─── Ranking scope — 전 프로그램 순위(프로그램별 X 순위 / X 높은·낮은 프로그램) ──
_RANK_SIGNAL = ("순위", "랭킹", "top", "상위", "하위", "높은 프로그램", "낮은 프로그램",
                "최고", "최저", "best", "worst")
_RANK_FIELD_MAP = [
    (("깊은청취", "깊은 청취", "deep"), "deep_rate", "깊은청취율"),
    (("실청취", "real_rate"), "real_rate", "실청취율"),
    (("mau", "월간", "월활"), "mau", "MAU"),
    (("wau", "주간", "주활"), "wau", "WAU"),
    (("이탈", "churn"), "churn_rate", "이탈률"),
    (("신규",), "new", "신규유입"),
    (("dau", "활성", "일활", "청취자"), "dau", "DAU"),
]
_RANK_EXCLUDE = {"T00", "F00", "L00", "G00", "P00"}

def _to_float(v):
    try:
        return float(str(v).replace(",", "").replace("%", ""))
    except Exception:
        return None

def _detect_ranking(question: str):
    t = question or ""
    tl = t.lower()
    if not any(s.lower() in tl for s in _RANK_SIGNAL):
        return None
    fld, label = "dau", "DAU"
    for keys, f, lab in _RANK_FIELD_MAP:
        if any(k.lower() in tl for k in keys):
            fld, label = f, lab
            break
    asc = any(k in t for k in ("낮은", "최저", "하위", "worst", "적은", "least"))
    return {"field": fld, "label": label, "asc": asc}


def _assemble_ranking(question, spec, overlay_ctx=None) -> dict:
    try:
        rows = S._kpi_rows() or []
    except Exception:
        rows = []
    if not rows:
        return {"ok": False, "reason": "데이터 없음"}
    latest = max((r.get("DATE") for r in rows if r.get("DATE")), default=None)
    fld = spec["field"]
    items, seen = [], set()
    for r in rows:
        if r.get("DATE") != latest:
            continue
        code = r.get("PGM_CODE")
        if not code or code in _RANK_EXCLUDE or code.endswith("00") or code in seen:
            continue
        v = _to_float(r.get(fld))
        if v is None:
            continue
        seen.add(code)
        items.append({"code": code, "name": _resolve_name(code), fld: r.get(fld), "_v": v})
    if not items:
        return {"ok": False, "reason": "지표 데이터 없음"}
    items.sort(key=lambda x: x["_v"], reverse=not spec["asc"])
    top = [{k: v for k, v in it.items() if k != "_v"} for it in items[:15]]
    payload = {"date": latest, "metric": spec["label"], "field": fld,
               "order": "asc(낮은순)" if spec["asc"] else "desc(높은순)",
               "count": len(items), "ranking": top}
    head = (f"순위 분석: 전 프로그램 {spec['label']} "
            f"{'하위' if spec['asc'] else '상위'} (기준일 {latest}, 대상 {len(items)}개)")
    context = (head + f"\n\n### program_ranking — 전 프로그램 {spec['label']} 순위\n"
               + json.dumps(payload, ensure_ascii=False, default=str))
    defs = S._metric_definitions_lines([fld])
    if defs:
        context += "\n\n## 온톨로지 근거\n[지표 정의 (온톨로지)]\n" + "\n".join(defs)
    targets = [("program", it["code"]) for it in top[:8]] + [("global", None)]
    otext, overlay_ids = _fetch_overlay(targets, overlay_ctx)
    if otext:
        context += "\n\n" + otext
    return {
        "ok": True, "context": context,
        "providers_used": ["program_ranking"], "entities_brief": head,
        "provenance": {"providers": ["program_ranking"], "scope": "ranking",
                       "metric": fld, "overlay_items": overlay_ids},
    }


# ─── 메인 — 맥락 조립 ───────────────────────────────────────────────────────
def assemble(question: str, overlay_ctx=None) -> dict:
    """질문 → 근거 context 조립. overlay_ctx={user_id, mode:'normal'|'requery'}.
       반환: {ok, context, providers_used, entities_brief, provenance}"""
    cmp_ents = _detect_compare(question)
    if cmp_ents:
        return _assemble_compare(question, cmp_ents, overlay_ctx)
    rank_spec = _detect_ranking(question)
    if rank_spec:
        _r = _assemble_ranking(question, rank_spec, overlay_ctx)
        if _r.get("ok"):
            return _r
    ent = resolve_entities(question)
    if not ent.get("code"):
        return {"ok": False, "reason": "프로그램 미식별"}   # 비-프로그램 질의는 기존 엔진으로
    names = select_providers(question, ent)
    blocks = []
    used = []
    for n in names:
        p = _PROVIDER_BY_NAME.get(n)
        if not p:
            continue
        try:
            data = p["fetch"](ent)
        except Exception as e:
            print(f"[grounding] provider {n} error: {e}")
            data = None
        if data in (None, {}, []):
            continue
        used.append(n)
        body = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False, default=str)
        blocks.append(f"### {n} — {p['desc']}\n{body}")
    onto = ontology_pack(ent, used)
    if ent.get("scope_kind") == "channel":
        _scope_str = f"{ent.get('name')} 채널 ({ent.get('code')}) — 채널 전체 집계"
    else:
        _scope_str = f"{ent.get('name')} ({ent.get('code')}, {ent.get('channel')})"
    head = (f"분석 대상: {_scope_str} · "
            f"기간 힌트: {_PERIOD_KO.get(ent.get('period'), '일간')} · 기준일: {ent.get('date')}")
    context = head + "\n\n" + "\n\n".join(blocks)
    if onto:
        context += "\n\n## 온톨로지 근거\n" + onto
    overlay_text, overlay_ids = _overlay_block(ent, overlay_ctx)
    if overlay_text:
        context += "\n\n" + overlay_text
    up_text, up_ids = _fetch_uploads(_entity_targets(ent) + [("global", None)], overlay_ctx)
    if up_text:
        context += "\n\n" + up_text
    return {
        "ok": True,
        "context": context,
        "providers_used": used + (["uploaded_data"] if up_ids else []),
        "entities_brief": head,
        "provenance": {"providers": used, "program": ent.get("code"),
                       "scope": ent.get("scope_kind"), "overlay_items": overlay_ids,
                       "uploads": up_ids},
    }


# ─── Digest scope — 어제 방송 특이사항 (비-프로그램, 전사 이상탐지 기반) ───
#    엔진 일원화: 프로그램 1개에 묶이지 않는 scope도 같은 GROUNDING_SYSTEM·온톨로지·
#    오버레이로 답한다. anomalies는 서버(get_cached_anomalies)가 주입 — grounding은 순수.
def _resolve_name(code) -> str:
    if not code:
        return ""
    try:
        from raas_onto import get_adapter
        m = get_adapter().get_program_meta(code)
        return (m or {}).get("label") or code
    except Exception:
        return code


def assemble_digest(ref_date, anomalies, overlay_ctx=None, question=None) -> dict:
    """어제 방송 특이사항 digest 조립. anomalies=서버 주입(get_cached_anomalies()).
       반환 형태는 assemble()과 동일 + drill(드릴다운 후보 프로그램)."""
    anomalies = anomalies or []
    _LV = ("red", "yellow", "green")
    items, codes = [], []
    for a in anomalies:
        it = {k: a.get(k) for k in
              ("level", "code", "field", "label", "value", "z", "msg", "direction")
              if a.get(k) not in (None, "")}
        if a.get("code"):
            it["name"] = _resolve_name(a.get("code"))
        items.append(it)
        c = a.get("code")
        if c and c not in codes and a.get("level") in ("red", "yellow"):
            codes.append(c)
    summary = {lv: sum(1 for a in anomalies if a.get("level") == lv) for lv in _LV}

    # 특이 프로그램 최신 KPI 스냅샷(설명 근거) — 상위 6개
    snaps = {}
    for c in codes[:6]:
        try:
            e = {"code": c, "row": S._load_program_latest_row(c)}
            kpi = _p_program_kpi(e) or {}
            if kpi:
                snaps[c] = {"name": _resolve_name(c), **kpi}
        except Exception:
            pass

    day_type = None
    try:
        from raas_onto import get_adapter
        day_type = get_adapter().get_day_type((ref_date or "").replace("/", "-"))
    except Exception:
        pass

    payload = {"date": ref_date, "day_type": day_type, "summary": summary, "items": items}
    blocks = [f"### daily_anomalies — 어제({ref_date}) 전 프로그램 이상·특이 신호(z-score 탐지)\n"
              + json.dumps(payload, ensure_ascii=False, default=str)]
    used = ["daily_anomalies"]
    if snaps:
        blocks.append("### notable_program_kpi — 특이 프로그램 최신 KPI 스냅샷\n"
                      + json.dumps(snaps, ensure_ascii=False, default=str))
        used.append("notable_program_kpi")
    if day_type:
        used.append("calendar")

    _dt_label = ""
    if isinstance(day_type, dict):
        _dt_label = day_type.get("day_type") or ""
        if day_type.get("holiday_name"):
            _dt_label += f"({day_type['holiday_name']})"
        if day_type.get("day_of_week"):
            _dt_label = f"{day_type['day_of_week']}·{_dt_label}".strip("·")
    elif day_type:
        _dt_label = str(day_type)
    head = (f"분석 대상: 어제({ref_date}) 방송 특이사항 digest · 전 프로그램 이상탐지 기반"
            + (f" · 일자 유형: {_dt_label}" if _dt_label else ""))
    context = head + "\n\n" + "\n\n".join(blocks)

    flds = sorted({a.get("field") for a in anomalies if a.get("field")})
    defs = S._metric_definitions_lines(list(flds)) if flds else []
    if defs:
        context += "\n\n## 온톨로지 근거\n[지표 정의 (온톨로지)]\n" + "\n".join(defs)

    # 오버레이 — 특이 프로그램 + 전역
    overlay_ids = []
    if codes:
        targets = [("program", c) for c in codes] + [("global", None)]
        otext, overlay_ids = _fetch_overlay(targets, overlay_ctx)
        if otext:
            context += "\n\n" + otext

    drill = [{"code": c, "name": _resolve_name(c)} for c in codes[:6]]
    return {
        "ok": True,
        "context": context,
        "providers_used": used,
        "entities_brief": head,
        "drill": drill,
        "provenance": {"providers": used, "scope": "digest",
                       "ref_date": ref_date, "overlay_items": overlay_ids},
    }


# digest 전용 시스템 — 엔진(provider·온톨로지·오버레이·LLM)은 동일, 출력 형태만 digest에 맞춤
DIGEST_SYSTEM = (
    "당신은 SBS 고릴라 라디오의 데이터 분석 어시스턴트입니다.\n"
    "아래 '근거 데이터'(전 프로그램 이상탐지 + 특이 프로그램 KPI + 일자 유형 + 온톨로지)만 사용해, "
    "**어제 방송에서 눈여겨볼 특이사항**을 간결한 브리핑으로 정리하세요.\n"
    "- 가장 중요한 신호부터. red(심각) → yellow(주의) 순.\n"
    "- 각 항목: 무엇이(프로그램·지표) 어떻게(증감·수치) 특이한지 1줄 + 가능한 원인 힌트(편성·요일·특일).\n"
    "- 근거에 없는 수치는 지어내지 말 것. 특이사항이 없으면 '특이사항 없음'으로 명시.\n"
    "- 군더더기·일반론 금지. 마크다운(굵게·목록) 사용 가능. 끝에 '자세히 볼 프로그램'을 묻도록 유도하지 말 것(드릴다운은 UI가 제공).\n"
)


# ─── 개선하기 컨텍스트 (이력 → 개선 화면 재료) ─────────────────────────────
def improve_context(question: str, user_id=None) -> dict:
    """개선하기 화면용 — 이 질문이 쓰는 데이터 필드(의미)·온톨로지 항목·본인 기존 기여."""
    ent = resolve_entities(question)
    if not ent.get("code"):
        return {"ok": False, "reason": "프로그램 미식별"}
    kpi = _p_program_kpi(ent) or {}
    used_fields = []
    for f in kpi.keys():
        mm = S._field_meaning(f)
        used_fields.append({"field": f, "meaning": (mm[1] if mm else ""), "label": (mm[0] if mm else "")})
    onto = []
    try:
        decs = S._query_decompositions()
        onto = [{"label": d.get("label"), "purpose": d.get("purpose")} for d in (decs or [])]
    except Exception:
        pass
    my = []
    try:
        import raas_history_db as HDB
        items = HDB.get_knowledge_items(
            [("program", ent["code"]), ("channel", ent.get("channel"))],
            contributor_id=user_id, include_candidate=True)
        my = [it for it in items if it.get("scope") == "candidate" and str(it.get("contributor_id")) == str(user_id)]
    except Exception:
        pass
    return {
        "ok": True,
        "program": {"code": ent["code"], "name": ent["name"], "channel": ent["channel"]},
        "used_fields": used_fields,
        "ontology_items": onto,
        "my_knowledge": my,
    }


# ─── LLM judge (재질의 A/B 보조 평가) ───────────────────────────────────────
_JUDGE_SYSTEM = (
    "당신은 라디오 데이터 답변 심사관입니다. 같은 질문에 대한 두 답변을 비교하세요.\n"
    "기준: 정확성(근거 일치·환각 없음) · 완결성 · 근거 충실성 · 유용성(실무 인사이트).\n"
    'JSON으로만: {"winner":"1"|"2"|"tie","scores":{"1":0~10,"2":0~10},"reason":"한두 문장"}'
)

def _judge_once(question, answer_a, answer_b, swap):
    """단일 판정 — swap이면 순서 교대(위치편향 제거). 반환 (winner∈A/B/tie, sA, sB, reason)."""
    first, second = (answer_b, answer_a) if swap else (answer_a, answer_b)
    user = f"질문: {question}\n\n[답변1]\n{first}\n\n[답변2]\n{second}"
    try:
        text, _ = call_claude(_JUDGE_SYSTEM, user, max_tokens=400, model=HAIKU_MODEL)
        r = json.loads(text[text.find("{"): text.rfind("}") + 1])
    except Exception as e:
        print(f"[judge] {e}")
        return None
    w = r.get("winner")
    if w == "1":
        winner = "B" if swap else "A"
    elif w == "2":
        winner = "A" if swap else "B"
    else:
        winner = "tie"
    sc = r.get("scores") or {}
    sa = sc.get("2") if swap else sc.get("1")
    sb = sc.get("1") if swap else sc.get("2")
    return winner, sa, sb, (r.get("reason") or "")


def judge(question: str, answer_a: str, answer_b: str, votes: int = 3) -> Optional[dict]:
    """A(원본) vs B(개선) 다수결 판정. 표마다 순서 교대로 위치편향 제거.
       반환: {winner, tally, votes, agreement, scores:{A,B}, reason}."""
    if not call_claude:
        return None
    tally = {"A": 0, "B": 0, "tie": 0}
    sA, sB, reasons = [], [], []
    for i in range(max(1, votes)):
        res = _judge_once(question, answer_a, answer_b, swap=(i % 2 == 1))
        if not res:
            continue
        w, a, b, reason = res
        tally[w] = tally.get(w, 0) + 1
        if isinstance(a, (int, float)):
            sA.append(a)
        if isinstance(b, (int, float)):
            sB.append(b)
        if reason:
            reasons.append(reason)
    total = sum(tally.values())
    if not total:
        return None
    winner = max(tally, key=tally.get)
    return {
        "winner": winner, "tally": tally, "votes": total,
        "agreement": round(tally[winner] / total * 100),
        "scores": {"A": round(sum(sA) / len(sA), 1) if sA else None,
                   "B": round(sum(sB) / len(sB), 1) if sB else None},
        "reason": reasons[0] if reasons else "",
    }


# ─── 수치 환각 검증 게이트 (1E) ─────────────────────────────────────────────
_VERIFY_SYSTEM = (
    "당신은 데이터 답변 검증관입니다. '근거 데이터'와 'AI 답변'을 받아, 답변의 수치 중 "
    "**근거로 뒷받침되지 않거나 근거와 모순되는 것만** 찾으세요.\n"
    "- 근거 수치로부터 계산된 값(증감·차이·비율·합계·평균·순위)은 **정상**으로 간주(환각 아님).\n"
    "- 근거에 전혀 없는 수치, 또는 근거와 명백히 다른 수치만 보고.\n"
    "- 날짜·이름·라벨은 검증 대상 아님(수치만).\n"
    'JSON으로만: {"ok": true|false, "unsupported": ["수치+맥락 한 항목씩"], "note": "한 줄 요약"}'
)

def verify_numbers(context: str, answer: str) -> Optional[dict]:
    """답변 수치가 근거로 뒷받침되는지 경량 검증(Haiku). 파생값은 정상 처리.
       반환: {ok, unsupported:[...], note} 또는 None(검증 불가)."""
    if not call_claude or not answer:
        return None
    # 컨텍스트가 매우 길면 앞부분만(검증 비용·지연 제한). 핵심 근거는 앞쪽에 있음.
    ctx = context if len(context) <= 12000 else context[:12000]
    user = f"[근거 데이터]\n{ctx}\n\n[AI 답변]\n{answer}"
    try:
        text, _ = call_claude(_VERIFY_SYSTEM, user, max_tokens=400, model=HAIKU_MODEL)
        r = json.loads(text[text.find("{"): text.rfind("}") + 1])
    except Exception as e:
        print(f"[verify] {e}")
        return None
    r["ok"] = bool(r.get("ok", True)) and not (r.get("unsupported") or [])
    if not isinstance(r.get("unsupported"), list):
        r["unsupported"] = []
    return r


# 최종 답변용 시스템 프롬프트(서버가 사용)
GROUNDING_SYSTEM = (
    "당신은 SBS 고릴라 라디오의 데이터 분석 어시스턴트입니다.\n"
    "아래 '근거 데이터'와 '온톨로지 근거'만을 사용해 사용자 질문에 **정확하고 분석적으로** 답하세요.\n"
    "- 근거에 없는 수치를 지어내지 말 것. 부족하면 '데이터 없음'을 명시.\n"
    "- 인과·원인 질문이면 흐름(신규/복귀/이탈)·편성·특일 등 제공된 근거로 구조적으로 설명.\n"
    "- 마크다운(굵게·표·목록) 사용 가능. 군더더기·일반론 권고 금지, 결과만.\n"
)
