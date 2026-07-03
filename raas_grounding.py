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
from raas_storyline_router import extract_program, PROGRAM_DIRECTORY

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


# 추이·추세 질의 판별 — 시계열 슬라이싱(1D)에서 전체 vs 최근 구간 결정
_TREND_SIGNAL = ("추이", "추세", "트렌드", "그래프", "장기", "월별", "주별", "일별 추이", "추세선", "흐름 추이")

def _is_trend_query(q: str) -> bool:
    return any(s in (q or "") for s in _TREND_SIGNAL)


# 절대 날짜 파싱 — '6/17', '6월 17일', '2026-06-17' 등 → (year|None, month, day) / None
def _parse_abs_date(q: str):
    t = q or ""
    m = re.search(r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})", t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        m = re.search(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일", t)
        if not m:
            m = re.search(r"(?<!\d)(\d{1,2})\s*/\s*(\d{1,2})(?!\d)", t)
        if not m:
            return None
        y, mo, d = None, int(m.group(1)), int(m.group(2))
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return (y, mo, d)


# ── 용어→필드 해석 (1A-P1): 온톨로지 변형 라벨로 질의어를 필드로 매핑 ──
#    예: '롤링MAU'→dau_r30, 'MAU'→mau. 온톨로지가 채워지면 자동 확장.
_TERM2FIELD = None

def _term_field_map():
    """[(term_lower, field), ...] 길이 내림차순(긴 용어 우선 매칭)."""
    global _TERM2FIELD
    if _TERM2FIELD is not None:
        return _TERM2FIELD
    m = {}
    try:
        from raas_onto import get_adapter
        for field, terms in (get_adapter().get_field_term_map() or {}).items():
            for t in terms:
                m.setdefault((t or "").lower(), field)
    except Exception as e:
        print(f"[grounding] term map error: {e}")
    # 실용 별칭 보강 — 온톨로지 용어가 장황('전체코호트 d7유지율')하거나 없어서('참여율')
    # 실제 말씨와 불일치하던 지표들. setdefault라 온톨로지 정의가 항상 우선.
    for t, f in [("참여율", "engage_rate"), ("습관형성률", "habit_rate"), ("습관형성율", "habit_rate"),
                 ("복귀율", "react_rate"), ("이탈율", "churn_rate"), ("이탈률", "churn_rate"),
                 ("d1 유지율", "d1_ret"), ("d7 유지율", "d7_ret"),
                 ("w1 유지율", "w1_ret"), ("m1 유지율", "m1_ret"), ("유지율", "d7_ret")]:
        m.setdefault(t, f)
    _TERM2FIELD = sorted(((t, f) for t, f in m.items() if t), key=lambda kv: -len(kv[0]))
    return _TERM2FIELD


def _focus_fields(question: str) -> list:
    """질의어에서 온톨로지 용어를 찾아 대응 필드 목록 반환(긴 용어 우선, 소비로 substring 재매칭 방지)."""
    t = (question or "").lower()
    found = []
    for term, field in _term_field_map():
        idx = t.find(term)
        if idx >= 0:
            if field not in found:
                found.append(field)
            t = t[:idx] + (" " * len(term)) + t[idx + len(term):]
    return found


_ANALYTICAL_SIGNAL = ("원인", "왜", "이유", "때문", "분석", "하락", "급락", "급증", "빠졌", "떨어", "부진")

def _is_analytical(q: str) -> bool:
    """원인·분석형 질의 — 필드 narrowing을 끄고 주변 지표까지 넓게 유지."""
    return any(s in (q or "") for s in _ANALYTICAL_SIGNAL)


# 개념·조언형 신호 — '데이터 값 조회'가 아닌 정의/전략/방법 질의. 엔티티 자동확정(완화) 제외용.
#   (명시적 프로그램/채널이 있는 질의는 완화 분기가 아니므로 영향 없음)
_CONCEPT_SIGNAL = (
    "전략", "방안", "방법", "아이디어", "제안", "노하우", "활용법", "활용 방안", "팁",
    "려면", "하려면",                       # 늘리려면/높이려면/개선하려면
    "어떻게 하면", "어떻게 늘", "어떻게 높", "어떻게 줄", "어떻게 개선",
    "뭐야", "뭔가요", "무엇", "정의", "무슨 뜻", "뜻이 뭐", "개념", "의미가 뭐",
)

def _is_concept(q: str) -> bool:
    """정의·전략·방법 등 개념/조언형 질의 — 엔티티 없는 지표 폴백에서 데이터 스코프 강제하지 않음."""
    return any(s in (q or "") for s in _CONCEPT_SIGNAL)


# 캘린더 지표 → 롤링 등가 (하위기간 추이 요청 시): mau=월간확정 → 롤링MAU(일단위)
_CAL_TO_ROLLING = {"mau": "dau_r30", "wau": "dau_r7"}

def _disambiguate_rolling(question: str, fields: list, lookback: int) -> list:
    """캘린더 지표(mau/wau)를 '하위 기간 추이' 요청이면 롤링 등가로 치환.
       예: '4주간 MAU 그래프' → 롤링MAU(dau_r30). '6개월/월별 MAU 추이'는 그대로(진짜 월 추이)."""
    t = question or ""
    lb = lookback or 0
    if any(s in t for s in ("개월", "월별", "분기")):      # 월 단위 추이 → 캘린더 유지
        return fields
    submonth = any(s in t for s in ("주별", "주간", "일별", "일간")) or (0 < lb <= 35)
    subweek = any(s in t for s in ("일별", "일간", "매일")) or (0 < lb <= 8)
    out, seen = [], set()
    for f in fields:
        nf = f
        if f == "mau" and submonth:
            nf = "dau_r30"
        elif f == "wau" and subweek:
            nf = "dau_r7"
        if nf not in seen:
            seen.add(nf); out.append(nf)
    return out


# 채널 감지 — 특정 프로그램이 아닌 채널/전사 단위 질의 식별 (scope 확장)
_CH_NAME_BY_CODE = {v: k for k, v in S._CHANNEL_CODE.items()}
_CH_NAME_BY_CODE["T00"] = "전체"

# 짧은 별칭 — 'M' 없는 '고릴라'는 채널(고릴라M)이 아니라 '고릴라 전체'(앱 전체=T00)를 의미.
#   (정식 채널명 '고릴라M'→G00은 위 채널명 루프에서 먼저 매칭되어 그대로 유지)
_CHANNEL_ALIAS = {"고릴라": "T00"}

def _detect_channel(q: str):
    t = q or ""
    # '전사/전체/앱 전체/고릴라 전체' → 전체(T00)를 먼저 (별칭 '고릴라'보다 우선)
    if any(k in t for k in ("전사", "전체 채널", "네트워크", "앱 전체", "고릴라 전체")):
        return "T00", "전체"
    for name, code in S._CHANNEL_CODE.items():        # 파워FM/러브FM/고릴라M/픽채널
        if name in t:
            return code, name
    for alias, code in _CHANNEL_ALIAS.items():        # '고릴라'(M 없이) 등 축약 별칭
        if alias in t:
            return code, _CH_NAME_BY_CODE.get(code, code)
    for code in ("F00", "L00", "G00", "P00"):          # 코드 직접 언급
        if code in t:
            return code, _CH_NAME_BY_CODE.get(code, code)
    return None, None


_CHANNEL_SET = {"T00", "F00", "L00", "G00", "P00"}

def _default_entity(default_code):
    """엔티티 없는 지표 질의의 기본 대상. 사용자 관심 코드(default_code) 있으면 그걸로, 없으면 전사(T00).
       반환: (code, scope_kind, name, channel)."""
    dc = (default_code or "").strip() or "T00"
    if dc in _CHANNEL_SET:
        return dc, "channel", _CH_NAME_BY_CODE.get(dc, dc), dc
    info = PROGRAM_DIRECTORY.get(dc)
    if info:
        return dc, "program", info.get("name", dc), info.get("channel")
    return "T00", "channel", "전체", "T00"   # 알 수 없는 코드는 전사로


def resolve_entities(question: str, default_code: str = None) -> dict:
    """질문 → {program, code, name, channel, period, metric, lookback, row, history, date, scope_kind}.
       scope_kind: 'program'(특정 프로그램) | 'channel'(채널/전사) | None(미식별→기존 엔진)."""
    prog = extract_program(question)
    lookback = _detect_lookback(question)
    raw_focus = _focus_fields(question)                  # 질의어→필드(롤링MAU→dau_r30)
    focus = _disambiguate_rolling(question, raw_focus, lookback)  # 캘린더→롤링(하위기간 추이)
    ent = {"program": prog, "period": _detect_period(question),
           "metric": S.extract_kpi_metric(question),
           "lookback": lookback,
           "is_trend": _is_trend_query(question),
           "focus_fields": focus,
           "rolling_note": (focus != raw_focus),         # 캘린더→롤링 치환 발생(프레이밍용)
           "analytical": _is_analytical(question),       # 분석형이면 narrowing 끔
           "scope_kind": None}
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
        elif (ent.get("focus_fields") or ent.get("metric")) and not _is_concept(question):
            # 엔티티 없는 지표 질의(추이·단순조회) → 사용자 관심 엔티티(default_code) 또는 전사(T00)
            #   단, 정의·전략·방법형(개념신호)은 데이터 스코프를 강제하지 않고 통과(과잉 캡처 방지).
            code, _sk, _nm, _chn = _default_entity(default_code)
            ent["scope_kind"] = _sk
            ent.update(code=code, name=_nm, channel=_chn)
    cand = _parse_abs_date(question)   # 특정 날짜 지정 여부
    if code:
        ent["row"] = S._load_program_latest_row(code)
        # 질문 의도 범위만큼(기본 전체) 로드. 날짜 지정이면 전체 로드해 그 날짜 포함 보장.
        ent["history"] = S._load_program_history(code, 0 if cand else ent["lookback"])
        ent["date"] = ((ent["row"] or {}).get("DATE") or "").replace("/", "-")
        if cand:
            _yr = cand[0] or int(((ent["row"] or {}).get("DATE") or "2026/01/01")[:4])
            _cd = "%04d/%02d/%02d" % (_yr, cand[1], cand[2])
            # 실제 그 엔티티 데이터에 존재하는 날짜일 때만 지정(오탐·범위밖 무시 → 최신일로 답)
            if _cd in {(h.get("DATE") or "").strip() for h in (ent["history"] or [])}:
                ent["as_of_date"] = _cd
    ent["_question"] = question   # 범용 provider(field_projection)가 질문 원문 참조
    return ent


# ─── Provider 레지스트리 ────────────────────────────────────────────────────
# 각 provider: name, desc(LLM 선택용), needs(필요 엔티티), fetch(ent)->data(JSON 직렬화 가능)
_KPI_BROAD = ["DATE", "STIME", "dau", "dau_chg", "wau", "wau_chg", "mau", "mau_chg",
              "new", "new_chg", "react", "react_chg", "churn_rate", "churn_rate_diff",
              "real_rate", "real_rate_diff", "deep_rate", "deep_rate_diff",
              "engage_rate", "habit_rate", "d1_ret", "d7_ret",
              # 채널 scope에서 의미 있는 주간/월간 변형(프로그램 행엔 없으면 자동 제외)
              "deep_rate_week", "deep_rate_mon", "real_rate_week", "real_rate_mon",
              "engage_rate_week", "engage_rate_mon", "churn_rate_week", "churn_rate_mon",
              "react_rate", "react_rate_week", "react_rate_mon",
              "habit_rate_week", "habit_rate_mon", "w1_ret", "m1_ret"]

# 일자별 편성·속성(텍스트) 필드 — 집계행(채널/전사)엔 대개 비어있음. 프로그램 행에서 유효.
_ATTR_FIELDS = ["guestname", "program_title", "daily_corner", "weekly_corner", "live_yn", "view_radio_yn"]

# 속성 키워드 → 필드(범용 프로젝션 + 스키마 인지 플래너용)
_ATTR_KEYWORDS = [
    ("게스트", ["guestname"]), ("guest", ["guestname"]), ("초대", ["guestname"]),
    ("코너", ["daily_corner", "weekly_corner"]),
    ("제목", ["program_title"]), ("회차", ["program_title"]), ("특집", ["program_title"]),
    ("생방송", ["live_yn"]), ("녹음", ["live_yn"]), ("라이브", ["live_yn"]),
    ("보이는 라디오", ["view_radio_yn"]), ("보라", ["view_radio_yn"]),
]

def _requested_attr_fields(q: str) -> list:
    t = q or ""
    out = []
    for kw, fs in _ATTR_KEYWORDS:
        if kw in t:
            for f in fs:
                if f not in out:
                    out.append(f)
    return out

def _window_slice(hist: list, ent: dict) -> list:
    """의도 기반 기간 슬라이스(속성 provider용) — 명시기간>그만큼 / 추이>최근 90일 / 기본>최근 4주.
    게스트·코너 등 텍스트 행은 토큰이 커서 숫자 시계열(400)보다 상한을 낮게 잡음."""
    lb = ent.get("lookback") or 0
    if lb > 0:
        return hist[-min(lb, 120):]
    if ent.get("is_trend"):
        return hist[-90:]
    return hist[-28:]


def _p_program_kpi(ent):
    """[P1] 의도 기반 필드 선택. 집중 질의(특정 지표·비분석)는 핵심+포커스만(좁힘),
       분석형·일반은 넓게 + 포커스 보강(롤링MAU=dau_r30 같은 용어 필드를 반드시 포함)."""
    r = ent.get("row") or {}
    focus = ent.get("focus_fields") or []
    if focus and not ent.get("analytical"):
        keys = ["DATE", "STIME", "dau"] + [f for f in focus if f != "dau"]
    else:
        keys = _KPI_BROAD + [f for f in focus if f not in _KPI_BROAD]
    return {k: r.get(k) for k in keys if r.get(k) not in (None, "")}


def _p_point_snapshot(ent):
    """특정 날짜 지정 질의(예: '6/17 신규 몇명') — 그 날짜의 지표 스냅샷을 history에서 조회."""
    d = ent.get("as_of_date")
    if not d:
        return None
    row = next((h for h in (ent.get("history") or []) if (h.get("DATE") or "").strip() == d), None)
    if not row:
        return None
    focus = ent.get("focus_fields") or []
    keys = _KPI_BROAD + [f for f in focus if f not in _KPI_BROAD]
    vals = {k: row.get(k) for k in keys if row.get(k) not in (None, "")}
    vals["DATE"] = d
    return {"as_of": d, "values": vals}


def _p_daily_lineup(ent):
    """[범용·a] 일자별 편성·출연 상세 — 게스트·회차제목·일일/주간 코너·생방송·보이는라디오를 날짜별로.
       '지난주 게스트 일자별로'처럼 속성/편성 필드를 기간만큼 조회하는 질의를 커버."""
    hist = _window_slice(ent.get("history") or [], ent)
    rows = []
    for h in hist:
        rec = {"DATE": (h.get("DATE") or "").strip()}
        for f in _ATTR_FIELDS:
            v = h.get(f)
            if v not in (None, ""):
                rec[f] = v
        if len(rec) > 1:                       # DATE 외 값이 하나라도 있을 때만(집계행·빈날 제외)
            rows.append(rec)
    return rows or None


def _p_field_projection(ent):
    """[범용·c] 질문이 지목한 필드(지표+속성)를 일자별 표로 묶음 — 혼합/임의 필드 조회 대응
       (예: 'DAU와 게스트 일자별'). 다른 provider가 못 담는 필드 조합의 안전망."""
    fields = list(ent.get("focus_fields") or [])
    for a in _requested_attr_fields(ent.get("_question") or ""):
        if a not in fields:
            fields.append(a)
    if not fields:
        return None
    hist = _window_slice(ent.get("history") or [], ent)
    rows = []
    for h in hist:
        rec = {"DATE": (h.get("DATE") or "").strip()}
        for f in fields:
            v = h.get(f)
            if v not in (None, ""):
                rec[f] = v
        if len(rec) > 1:
            rows.append(rec)
    return {"fields": fields, "rows": rows} if rows else None


# 채널 코드 → 소속 프로그램 코드 프리픽스 (X00 집계코드는 제외 처리)
_CH_PREFIX = {"F00": ("F",), "L00": ("L", "M"), "G00": ("G",), "P00": ("P",),
              "T00": ("F", "L", "M", "G", "P")}


def _p_channel_programs(ent):
    """[채널 전용·범용] 채널 소속 프로그램별 최신 KPI — '채널 내 프로그램 비교·순위·하락' 질의를
       LLM이 직접 계산하도록 프로그램 행 제공(집계행 F00만으로는 소속 프로그램 질문에 답 불가)."""
    if ent.get("scope_kind") != "channel":
        return None
    prefixes = _CH_PREFIX.get(ent.get("code") or "")
    if not prefixes:
        return None
    try:
        rows = S._kpi_rows() or []
    except Exception:
        return None
    latest = max((r.get("DATE") for r in rows if r.get("DATE")), default=None)
    if not latest:
        return None
    # 핵심 지표 + 포커스(변형 포함) — 프로그램당 필드 수 제한으로 토큰 관리
    keys = ["dau", "dau_chg", "deep_rate", "deep_rate_diff", "real_rate", "real_rate_diff",
            "engage_rate", "engage_rate_diff", "churn_rate", "churn_rate_diff"]
    for f in (ent.get("focus_fields") or []):
        for k in (f, f + "_prev", f + "_diff", f + "_chg"):
            if k not in keys:
                keys.append(k)
    out, seen = [], set()
    for r in rows:
        if r.get("DATE") != latest:
            continue
        code = (r.get("PGM_CODE") or "").upper()
        if not code or code.endswith("00") or code in seen or not code.startswith(prefixes):
            continue
        seen.add(code)
        rec = {"code": code, "name": r.get("PGM_NAME") or _resolve_name(code)}
        for k in keys:
            v = r.get(k)
            if v not in (None, ""):
                rec[k] = v
        out.append(rec)
    return {"as_of": latest, "programs": out} if out else None


def _p_metric_timeseries(ent):
    """의도 기반 슬라이싱(1D): 명시 기간>그만큼 / 추이 질의>전체(상한) / 원인·포인트 질의>최근 4주.
       전체 노이즈·토큰을 줄여 핵심 근거에 집중."""
    hist = ent.get("history") or []
    lb = ent.get("lookback") or 0
    if lb > 0:
        hist = hist[-lb:]                      # 사용자가 명시한 기간만
    elif ent.get("is_trend"):
        if len(hist) > 400:
            hist = hist[-400:]                 # 추이·추세 질의 → 전체(안전 상한)
    else:
        hist = hist[-28:]                      # 원인·포인트·기본 → 최근 4주(같은요일 비교 충분)
    # base — 기간 인지형: 어떤 지표를 물어도 그 기간의 핵심지표가 실리도록(용어 매칭 실패에도 강건).
    # 직렬화가 주간/월간 필드를 변동 시점만 쓰므로 필드 추가의 토큰 비용은 작음.
    base = ["dau", "wau", "mau", "deep_rate", "real_rate", "new", "churn_rate",
            "react", "react_rate", "engage_rate", "habit_rate", "d7_ret"]
    per = ent.get("period")
    if per == "week":
        base += ["new_week", "react_week", "churn_rate_week", "react_rate_week",
                 "real_rate_week", "deep_rate_week", "engage_rate_week", "habit_rate_week", "w1_ret"]
    elif per == "month":
        base += ["new_mon", "react_mon", "churn_rate_mon", "react_rate_mon",
                 "real_rate_mon", "deep_rate_mon", "engage_rate_mon", "habit_rate_mon", "m1_ret"]
    focus = ent.get("focus_fields") or []
    if focus and not ent.get("analytical"):
        flds = ["dau"] + [f for f in focus if f != "dau"]   # 집중 → 핵심+포커스(롤링MAU=dau_r30)
    else:
        flds = base + [f for f in focus if f not in base]   # 넓게 + 포커스 보강
    # 포커스의 기간 변형 자동 추가(참여율+주간 → engage_rate_week)
    suf = {"week": "_week", "month": "_mon"}.get(per)
    if suf:
        last = hist[-1] if hist else {}
        flds += [f + suf for f in list(flds)
                 if not f.endswith(("_week", "_mon")) and (f + suf) in last and (f + suf) not in flds]
    return _ts_csv(hist, flds)   # CSV(키 1회) + 주간/월간 변동시점-only 직렬화


def _clean_onto(v):
    """온톨로지 dict에서 IRI(id)·빈값 제거 → LLM에 깔끔히 주입."""
    if isinstance(v, dict):
        return {k: _clean_onto(x) for k, x in v.items()
                if k not in ("id",) and x not in (None, "", [], {})}
    if isinstance(v, list):
        return [_clean_onto(x) for x in v]
    return v


def _p_ontology(ent):
    """[1A 얇은 generic] 엔티티에 대해 온톨로지가 가진 도메인 사실을 무엇이든 surfacing.
       온톨로지가 채워질수록 추가 코드 없이 답변 품질이 자동 향상."""
    code = ent.get("code")
    if not code:
        return None
    try:
        from raas_onto import get_adapter
        a = get_adapter()
    except Exception:
        return None
    out = {}
    try:
        m = a.get_program_meta(code)
        if m:
            out["program"] = _clean_onto(m)
    except Exception:
        pass
    try:
        gp = a.get_guestname_policy(code)
        if gp:
            out["guest_policy"] = _clean_onto(gp)
    except Exception:
        pass
    return out or None


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


def _p_period_events(ent):
    """[범용] 분석 기간 내 이벤트·공휴일 주석 — 추이의 급등/급락 시점을 특일(예: 고릴라데이)로
       설명할 근거. 기준일 하루만 보는 calendar와 달리 시계열 창 전체를 스캔."""
    hist = _window_slice(ent.get("history") or [], ent)
    if len(hist) < 2:
        return None
    d0 = (hist[0].get("DATE") or "").strip()
    d1 = (hist[-1].get("DATE") or "").strip()
    if not (d0 and d1):
        return None
    try:
        from raas_onto import get_adapter
        items = get_adapter().get_calendar_annotations(d0, d1)
    except Exception:
        return None
    return {"window": f"{d0}~{d1}", "annotations": items} if items else None


PROVIDERS = [
    {"name": "program_kpi", "needs": "program",
     "desc": "프로그램의 최신 핵심 KPI 스냅샷(DAU/WAU/MAU·신규·복귀·이탈·실청취·깊은청취·유지율과 증감)",
     "fetch": _p_program_kpi},
    {"name": "point_snapshot", "needs": "program",
     "desc": "특정 날짜 지정 질의(예: '6/17 신규 몇명') — 그 날짜의 지표 스냅샷",
     "fetch": _p_point_snapshot},
    {"name": "daily_lineup", "needs": "program",
     "desc": "일자별 편성·출연 상세(날짜별 게스트·회차제목·일일/주간 코너·생방송여부·보이는라디오) — '지난주 게스트 일자별로' 등 날짜별 편성/출연 조회",
     "fetch": _p_daily_lineup},
    {"name": "field_projection", "needs": "program",
     "desc": "질문이 지목한 필드(지표+속성)를 일자별 표로 묶음 — 특정 필드 콕 집기/지표+속성 혼합('DAU와 게스트 일자별') 대응",
     "fetch": _p_field_projection},
    {"name": "channel_programs", "needs": "channel",
     "desc": "채널 소속 프로그램별 최신 KPI 나열 — '채널 내 프로그램 비교·순위·많이 하락한 프로그램' 질의(집계행만으론 답 불가)",
     "fetch": _p_channel_programs},
    {"name": "metric_timeseries", "needs": "program",
     "desc": "주요 지표 시계열(의도 기반: 추이 질의는 전체, 원인·포인트 질의는 최근 4주)",
     "fetch": _p_metric_timeseries},
    {"name": "ontology", "needs": "program",
     "desc": "프로그램 도메인 사실(진행자·정규게스트·시간대·편성유형·광고가치·게스트명 해석정책 등 온톨로지)",
     "fetch": _p_ontology},
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
    {"name": "period_events", "needs": "program",
     "desc": "분석 기간 내 이벤트·공휴일 주석(예: 고릴라데이) — 추이의 급등/급락 시점 설명 근거",
     "fetch": _p_period_events},
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
        if need == "channel" and not is_channel:
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
    # 스키마 인지 — 데이터에 어떤 필드가 있는지 플래너에게 알려 올바른 provider를 고르게 함
    field_hint = (
        "\n\n[데이터에 존재하는 필드]\n"
        "· 편성/속성(날짜별): guestname(게스트), program_title(회차·특집 제목), "
        "daily_corner(매일 코너), weekly_corner(주간 코너), live_yn(생방송/녹음), view_radio_yn(보이는 라디오)\n"
        "· 지표(날짜별): dau/wau/mau·신규·복귀·이탈율·실청취율·깊은청취율·참여율·습관형성률·유지율(D1/D7/W1/M1) 등\n"
        "지침: 게스트·코너·제목 같은 '날짜별 편성/출연'은 daily_lineup, 지표+속성 혼합이나 특정 필드 지정은 "
        "field_projection을 고르세요(이 필드들은 실제 데이터에 있으니 '없음'으로 넘기지 말 것)."
    )
    user = f"질문: {question}\n\n사용 가능한 provider:\n{catalog}{field_hint}"
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
def _comparison_basis(info) -> Optional[str]:
    """[Q2] 비교형 변형(_chg/_prev/_diff)의 비교 기준을 granularity 인지로 반환.
       일간=전주 동요일(D-7), 주간=전주, 월간=전월(라디오 청취 요일 패턴 반영)."""
    var_id = ((info or {}).get("variant") or {}).get("id") or ""
    if var_id not in ("raas:Change", "raas:Diff", "raas:Previous"):
        return None
    g = ((info or {}).get("granularity") or {}).get("id") or ""
    if g == "raas:Day":
        return "전주 동요일(D-7) 대비"
    if g == "raas:Week":
        return "전주 대비"
    if g == "raas:Month":
        return "전월 대비"
    if "Rolling" in g:
        return "7일 전(D-7) 대비"
    return "직전 기간 대비"


def _metric_def_lines(fields) -> list:
    """[P1] 필드별 지표 정의 — 변형·단위(롤링MAU·일간·월간)·비교근거(전주동요일/전주/전월)까지 명시.
       같은 '활성 사용자 수'라도 일/주/월/롤링을 LLM이 구분하도록 surfacing."""
    try:
        from raas_onto import get_adapter
        a = get_adapter()
        tmap = a.get_field_term_map() or {}
    except Exception:
        return []
    out, seen = [], set()
    for f in fields:
        if not f or f in seen:
            continue
        seen.add(f)
        try:
            info = a.get_field_info(f)
        except Exception:
            info = None
        m = (info or {}).get("metric")
        if not m or not m.get("label"):
            continue
        parts = [m["label"]]
        gran = (info.get("granularity") or {}).get("label")
        var = (info.get("variant") or {}).get("label")
        if gran:
            parts.append(gran)
        if var and var != "현재값":
            basis = _comparison_basis(info)
            parts.append(f"{var} · {basis}" if basis else var)
        alias = (tmap.get(f) or [None])[0]
        head = f + (f"({alias})" if alias else "")
        defn = (m.get("definition") or "").strip()
        out.append(f"- {head} = " + " · ".join(parts) + (f" — {defn}" if defn else ""))
        # 깊은 통합(Q2 v2): 이 필드의 승격된 기여 정의 보정을 정의 줄 옆에 병합(출처 명시)
        try:
            for cf in (a.get_contributed_for("field", f) or []):
                if cf.get("predicate") == "raas:contributedDefinition":
                    out[-1] += f"  ※기여 보정: {cf['content']}"
        except Exception:
            pass
    return out


def ontology_pack(ent, provider_names) -> str:
    lines = []
    # 사용 지표 정의(변형·단위까지 명시)
    fields = list((_p_program_kpi(ent) or {}).keys()) if ent.get("row") else []
    defs = _metric_def_lines(fields)
    if defs:
        lines += ["[지표 정의 (온톨로지)]"] + defs
    # 편성 메타 컬럼 의미(편성/편성표 provider 선택 시) — guestname·코너·생방·보라 해석
    if any(n in provider_names for n in ("programming", "schedule")):
        try:
            from raas_onto import get_adapter
            cols = get_adapter().get_lookup_columns() or {}
            wanted = ["guestname", "daily_corner", "weekly_corner", "view_radio_yn", "live_yn"]
            clines = [f"- {cols[c]['label']}({c}): {cols[c]['definition']}"
                      for c in wanted if c in cols and cols[c].get("definition")]
            if clines:
                lines += ["", "[편성 메타 정의 (온톨로지 컬럼 사전)]"] + clines
        except Exception as e:
            print(f"[grounding] column defs error: {e}")
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
    # 현재 6분류
    "episode_note": "회차별 방송 특이사항", "program_corner": "프로그램 및 코너 정보",
    "metric_term": "지표 및 용어 정의", "answer_style": "답변 스타일",
    "analysis_method": "분석 기법", "misc": "기타(미분류)",
    # 레거시 type 호환
    "metric_definition": "지표 정의 보정", "field_meaning": "필드 의미",
    "program_note": "프로그램 메모", "guest_policy": "게스트 정책",
    "corner_note": "코너 메모", "decomposition_hint": "분해 힌트", "fact": "사실",
}

def _fetch_contributed_struct(targets) -> str:
    """승격된(구조화) 기여 지식을 온톨로지에서 대상별로 surfacing. 텍스트 오버레이와 별도 블록."""
    try:
        from raas_onto import get_adapter
        a = get_adapter()
    except Exception:
        return ""
    seen, lines = set(), []
    full = list(targets or []) + [("global", None), ("unclassified", None)]
    for kind, tid in full:
        try:
            facts = a.get_contributed_for(kind, tid)
        except Exception:
            facts = []
        for f in facts:
            # contributedDefinition은 _metric_def_lines가 정의 줄에 병합(깊은 통합) → 블록 제외
            if f.get("predicate") == "raas:contributedDefinition":
                continue
            key = (f.get("predicate"), f.get("content"))
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"- [{f.get('label')}·{tid or '전역'}] {f.get('content')}")
    if not lines:
        return ""
    return ("## 승인 기여 지식 (온톨로지 구조화 · canonical)\n"
            "아래는 검토 승인되어 온톨로지에 졸업된 지식입니다. 사실로 신뢰해 답변에 반영하세요.\n"
            + "\n".join(lines))


def _fetch_overlay(targets, overlay_ctx) -> tuple:
    """주어진 (kind, id) 타깃들에 매칭되는 지식을 context 블록 + id 목록으로. 단일 오버레이 게이트.
       텍스트 오버레이(candidate·미승격 approved) + 구조화 기여(승격 approved, 온톨로지 경로).
       승격분은 텍스트에서 제외 → 구조화 경로로만 1회 주입(중복 방지)."""
    octx = overlay_ctx or {}
    items = []
    try:
        import raas_history_db as HDB
        items = HDB.get_knowledge_items(
            targets, contributor_id=octx.get("user_id"),
            include_candidate=(octx.get("mode") == "requery"))
    except Exception as e:
        print(f"[grounding] overlay error: {e}")
    # 승격된 approved(promoted_at 있음)는 텍스트에서 제외 — 구조화 블록이 대신 surfacing
    text_items = [it for it in items
                  if not (it.get("scope") == "approved" and it.get("promoted_at"))]
    blocks = []
    if text_items:
        lines = ["## 사용자·운영 도메인 지식 (오버레이)",
                 "아래는 사용자가 보강한 도메인 지식입니다. 답변에 적극 반영하세요."]
        for it in text_items:
            tag = "(본인 미승인)" if it.get("scope") == "candidate" else "(승인됨)"
            tk = _KTYPE_KO.get(it.get("type"), it.get("type"))
            tgt = it.get("target_id") or "전역"
            lines.append(f"- [{tk}·{tgt}] {it.get('content')} {tag}")
        blocks.append("\n".join(lines))
    struct = _fetch_contributed_struct(targets)
    if struct:
        blocks.append(struct)
    return "\n\n".join(blocks), [it["id"] for it in items]


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


# 시계열 직렬화 — CSV(키 1회) + 주간/월간 지표는 기간 내 매일 같은 값이므로 변동 시점만.
#   주간 지표(wau 등)는 월~일 동일, 월간 지표(mau 등)는 1일~말일 동일 → 첫날 값만 보내면 충분.
_TS_FIELDS = ["dau", "wau", "mau", "deep_rate", "real_rate", "new", "churn_rate"]
_WEEKLY_FIELDS = {"wau", "wau_1min", "wau_10min", "w1_ret", "new_w1_ret"}
_MONTHLY_FIELDS = {"mau", "mau_1min", "mau_10min", "m1_ret", "new_m1_ret"}

def _ts_csv(hist, fields=None) -> str:
    """시계열을 CSV로 직렬화(키 반복 제거). 일별 지표는 매일, 주간·월간 지표는 변동 시점(첫날)만 →
       기간 내 중복 값 제거로 토큰 대폭 절감. 행 구조 보존(날짜+값 한 줄)이라 정합성 안전."""
    hist = hist or []
    fields = fields or _TS_FIELDS
    def _d(h): return (h.get("DATE") or "").replace("/", "-")
    def _v(h, f):
        x = h.get(f)
        return "" if x in (None, "") else str(x)
    # 기간 내 값이 한 번도 없는 필드는 열에서 제외(빈 열 낭비 방지)
    fields = [f for f in fields if any(h.get(f) not in (None, "") for h in hist)]
    daily   = [f for f in fields if f not in _WEEKLY_FIELDS and f not in _MONTHLY_FIELDS]
    weekly  = [f for f in fields if f in _WEEKLY_FIELDS]
    monthly = [f for f in fields if f in _MONTHLY_FIELDS]
    out = []
    if daily:
        out.append("[일별] date," + ",".join(daily))
        out += [_d(h) + "," + ",".join(_v(h, f) for f in daily) for h in hist]
    def _step(label, flds):
        if not flds:
            return
        rows, prev = [], None
        for h in hist:
            key = tuple(h.get(f) for f in flds)
            if key != prev:
                prev = key
                if any(h.get(f) not in (None, "") for f in flds):  # 빈 값 변동점은 생략
                    rows.append(_d(h) + "," + ",".join(_v(h, f) for f in flds))
        if rows:
            out.append(f"[{label}] date,{','.join(flds)}  (기간 내 매일 동일 — 변동 시점만)")
            out.extend(rows)
    _step("주별", weekly)
    _step("월별", monthly)
    return "\n".join(out)


def _assemble_compare(question, ents, overlay_ctx=None) -> dict:
    lookback = _detect_lookback(question)
    # 명시 기간 존중 / 추세·장기 질의는 길게(365) / 그 외 미지정은 최근 90일
    win = lookback if lookback > 0 else (365 if _is_trend_query(question) else 90)
    period = _detect_period(question)
    metric = S.extract_kpi_metric(question)
    blocks, kpi_fields = [], []
    for e in ents:
        row = S._load_program_latest_row(e["code"])
        hist = S._load_program_history(e["code"], win)
        el = {"code": e["code"], "row": row, "history": hist, "scope_kind": e["kind"]}
        kpi = _p_program_kpi(el) or {}
        if not kpi_fields:
            kpi_fields = list(kpi.keys())
        blocks.append(f"### {e['name']}({e['code']}) — 최신 KPI 스냅샷\n"
                      + json.dumps(kpi, ensure_ascii=False, default=str))
        blocks.append(f"### {e['name']}({e['code']}) — 시계열(최근 {win}일, CSV)\n"
                      + _ts_csv(hist))
    head = ("비교 분석: " + " vs ".join(f"{e['name']}({e['code']})" for e in ents)
            + f" · 기간 힌트: {_PERIOD_KO.get(period, '일간')} · 비교지표: {metric or '핵심 지표'}")
    context = head + "\n\n" + "\n\n".join(blocks)
    defs = _metric_def_lines(kpi_fields) if kpi_fields else []
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
    defs = _metric_def_lines([fld])
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
    ent = resolve_entities(question, (overlay_ctx or {}).get("default_code"))
    if not ent.get("code"):
        return {"ok": False, "reason": "프로그램 미식별"}   # 비-프로그램 질의는 기존 엔진으로
    names = select_providers(question, ent)
    if ent.get("as_of_date") and "point_snapshot" not in names:
        names = ["point_snapshot"] + names   # 특정 날짜 지정이면 그 날짜 스냅샷 반드시 포함
    if (ent.get("scope_kind") == "channel" and "프로그램" in question
            and "channel_programs" not in names):
        names = ["channel_programs"] + names   # 채널 내 '프로그램' 질의는 소속 프로그램 행 반드시 포함
    if ((ent.get("is_trend") or ent.get("analytical")) and "period_events" not in names):
        names = names + ["period_events"]      # 추이·분석 질의엔 기간 내 이벤트·특일 주석 반드시 첨부
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
    if ent.get("as_of_date"):
        head += (f"\n※ 요청 특정일: {ent['as_of_date']} — 이 날짜의 값(point_snapshot)으로 답하세요"
                 f"(최신일 스냅샷 아님).")
    if ent.get("rolling_note"):
        head += ("\n※ 해석: 'MAU/WAU'를 하위 기간(주·일) 추이로 요청 → 캘린더 지표는 월/주 확정값이라 "
                 "같은 개념의 롤링 지표(롤링MAU=dau_r30 등)로 제시. 답변에 이 점을 짧게 안내할 것.")
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
    defs = _metric_def_lines(list(flds)) if flds else []
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
# 차트 출력 지침 — 그래프가 표보다 직관적이면 LLM이 차트 스펙을 emit (프론트가 ECharts로 렌더)
_CHART_HINT = (
    "- 시계열·비교처럼 **그래프가 표보다 직관적**이면 답변에 차트 블록을 포함하세요(긴 시계열은 표 대신 차트 권장):\n"
    "  ```chart\n"
    '  {"type":"line"|"bar","title":"제목","unit":"명|%|건","x":["06-01","06-02"],"series":[{"name":"롤링MAU","data":[621746,623411]}]}\n'
    "  ```\n"
    "  x와 각 series.data 길이는 동일하게, 값은 **근거의 실제 수치만**. 점이 매우 많으면 핵심 구간만. 단일 수치엔 차트 불필요.\n"
)

# ─── 답변 스타일 정책 (단일 큐레이션 블록 · 매 답변 주입) ──────────────────────
#  스타일은 사실·관계가 아니라 '생성 지침'이라 온톨로지/누적 오버레이가 아닌 시스템 프롬프트의
#  스타일 섹션이 제자리. DB(style_policy)에 거버넌스가 편집한 블록이 있으면 그걸, 없으면 아래 시드를 사용.
_DEFAULT_STYLE_POLICY = (
    "- 결론·핵심 수치를 첫 1–2줄에. 서론·일반론 금지, 결과만.\n"
    "- 수치엔 비교근거 병기(WoW·전주·전월 등), 천 단위 콤마.\n"
    "- 표는 꼭 필요할 때만 3–4열 이하. 긴 시계열은 표 대신 차트 권장.\n"
    "- 마크다운(굵게·목록) 사용 가능. 굵게는 핵심에만, 이모지 최소.\n"
    "- 불확실하면 단정 대신 '데이터 없음/추정'을 명시."
)

def style_policy_block() -> str:
    """현재 답변 스타일 정책을 시스템 프롬프트용 블록으로. DB 우선, 없으면 기본 시드."""
    pol = None
    try:
        import raas_history_db as HDB
        pol = HDB.get_style_policy()
    except Exception:
        pol = None
    return "[답변 스타일 정책]\n" + (pol or _DEFAULT_STYLE_POLICY).strip()

def system_with_style(base: str) -> str:
    """답변 생성용 시스템 프롬프트 = base + 현재 스타일 정책 블록(매 답변 1회 주입)."""
    return base + "\n" + style_policy_block()

DIGEST_SYSTEM = (
    "당신은 SBS 고릴라 라디오의 데이터 분석 어시스턴트입니다.\n"
    "아래 '근거 데이터'(전 프로그램 이상탐지 + 특이 프로그램 KPI + 일자 유형 + 온톨로지)만 사용해, "
    "**어제 방송에서 눈여겨볼 특이사항**을 간결한 브리핑으로 정리하세요.\n"
    "- 가장 중요한 신호부터. red(심각) → yellow(주의) 순.\n"
    "- 각 항목: 무엇이(프로그램·지표) 어떻게(증감·수치) 특이한지 1줄 + 가능한 원인 힌트(편성·요일·특일).\n"
    "- 근거에 없는 수치는 지어내지 말 것. 특이사항이 없으면 '특이사항 없음'으로 명시.\n"
    "- 끝에 '자세히 볼 프로그램'을 묻도록 유도하지 말 것(드릴다운은 UI가 제공).\n"
    + _CHART_HINT
    # 표현·형식 지침은 system_with_style()의 [답변 스타일 정책]이 단일 소스.
)


# ─── 개선하기 컨텍스트 (이력 → 개선 화면 재료) ─────────────────────────────
def _field_onto_item(f):
    """필드 → {field, label(변형 포함), meaning, formula, source(TTL·IRI)} 또는 None(비지표)."""
    try:
        from raas_onto import get_adapter
        info = get_adapter().get_field_info(f)
    except Exception:
        info = None
    m = (info or {}).get("metric")
    if not m or not m.get("label"):
        return None
    gran = (info.get("granularity") or {}).get("label")
    var = (info.get("variant") or {}).get("label")
    label = m["label"] + (f" · {gran}" if gran else "") + (f" · {var}" if var and var != "현재값" else "")
    src = "raas_ontology_fields.ttl" + (f" · {m['id']}" if m.get("id") else "")
    return {"field": f, "label": label, "meaning": (m.get("definition") or "").strip(),
            "formula": m.get("formula"), "source": src}


def improve_context(question: str, user_id=None) -> dict:
    """개선하기 화면용 — 이 질문이 실제 쓰는 데이터 필드(의미·출처)·온톨로지 항목·본인 기여.
       [P1] 온톨로지 항목 = 실제 사용한 것(필드 지표정의 + 분석형일 때만 분해 프레임워크), 출처 TTL 표기."""
    ent = resolve_entities(question)
    if not ent.get("code"):
        return {"ok": False, "reason": "프로그램 미식별"}
    kpi = _p_program_kpi(ent) or {}
    used_fields = [it for f in kpi.keys() if (it := _field_onto_item(f))]
    # 온톨로지 항목 = 실제 사용한 지표 정의(distinct) + 분석형이면 분해 프레임워크(cause.ttl)
    onto, seen = [], set()
    for it in used_fields:
        if it["label"] not in seen:
            seen.add(it["label"])
            onto.append({"label": it["label"], "purpose": it.get("meaning") or "",
                         "source": it["source"]})
    if ent.get("analytical"):
        try:
            for d in (S._query_decompositions() or []):
                onto.append({"label": d.get("label"), "purpose": d.get("purpose"),
                             "source": "raas_ontology_cause.ttl"})
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
        "scope_kind": ent.get("scope_kind"),    # program|channel → 개선 모달 target 자동추론
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
    + _CHART_HINT
    # 표현·형식(마크다운·간결성 등) 지침은 system_with_style()이 붙이는 [답변 스타일 정책]이 단일 소스.
)
