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
import datetime as _dt
from typing import Optional

_DOW_KO = ["월", "화", "수", "목", "금", "토", "일"]

def _dow_ko(date_str: str) -> str:
    """'YYYY-MM-DD'/'YYYY/MM/DD' → 한국 요일(월~일). 요일 계산은 코드가 하고 LLM엔 결과만 준다."""
    try:
        s = (date_str or "").replace("/", "-")[:10]
        return _DOW_KO[_dt.datetime.strptime(s, "%Y-%m-%d").weekday()]
    except Exception:
        return ""

import raas_metrics_engine as S
from raas_storyline_router import extract_program, PROGRAM_DIRECTORY

try:
    from raas_llm import call_claude, HAIKU_MODEL
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
        _ym = re.search(r"(20\d{2})\s*년", t)      # '2025년 12월 31일'의 연도 포착
        y, mo, d = (int(_ym.group(1)) if _ym else None), int(m.group(1)), int(m.group(2))
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


# '모든 프로그램/프로그램별/각 프로그램' — 전 프로그램 나열 의도(순위 아님). 관심 기본값보다 우선.
_ALL_PROG_SIGNAL = ("모든 프로그램", "전 프로그램", "전체 프로그램", "각 프로그램",
                    "프로그램별", "프로그램 별", "프로그램마다", "프로그램마", "전프로그램")

def _is_all_programs(q: str) -> bool:
    return any(s in (q or "") for s in _ALL_PROG_SIGNAL)


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
        _allp = _is_all_programs(question)
        if ch_code:
            ent["scope_kind"] = "channel"
            code = ch_code
            ent.update(code=code, name=ch_name, channel=ch_code)
            if _allp:
                ent["all_programs"] = True     # '파워FM 모든 프로그램' → 해당 채널 소속 전체
        elif _allp:
            # '모든 프로그램/프로그램별' 명시 → 전사(T00) + 소속 프로그램 전체 나열(관심 기본값보다 우선)
            ent["scope_kind"] = "channel"
            code = "T00"
            ent.update(code=code, name="전체", channel="T00")
            ent["all_programs"] = True
        elif (ent.get("focus_fields") or ent.get("metric")) and not _is_concept(question):
            # 엔티티 없는 지표 질의(추이·단순조회) → 사용자 관심 엔티티(default_code) 또는 전사(T00)
            #   단, 정의·전략·방법형(개념신호)은 데이터 스코프를 강제하지 않고 통과(과잉 캡처 방지).
            code, _sk, _nm, _chn = _default_entity(default_code)
            ent["scope_kind"] = _sk
            ent.update(code=code, name=_nm, channel=_chn)
        elif _requested_attr_fields(question) and not _is_concept(question):
            # 엔티티 없는 속성 질의('어제 게스트 누구야') → 전사(T00) + 소속 프로그램 나열
            ent["scope_kind"] = "channel"
            code = "T00"
            ent.update(code=code, name="전체", channel="T00")
            ent["all_programs"] = True
        elif _parse_abs_date(question) and not _is_concept(question):
            # 특정 날짜 + 지표 미지정('6월16일 주요 데이터 보여줘') → 전사(T00) 그날 스냅샷
            code, _sk, _nm, _chn = _default_entity(default_code)
            ent["scope_kind"] = _sk
            ent.update(code=code, name=_nm, channel=_chn)
    cand = _parse_abs_date(question)   # 특정 날짜 지정 여부
    if cand:
        ent["lookback"] = 0            # '7월1일'의 '1일'이 lookback=1로 오파싱돼 창을 쪼그라뜨리는 것 방지
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


# ─── 데이터 추출(extract) — 프로그램별×기간×지표 표 + 엑셀 다운로드 ──────────
#     분석 Q&A와 별개의 '결정적' 경로. LLM은 요청 파싱만, 숫자는 코드가 표로 만든다
#     (환각 0). 광고·마케팅의 반복 데이터 추출 작업용.
_EXTRACT_SIGNAL = ("뽑아", "추출", "내려받", "다운로드", "엑셀", "excel", "csv",
                   "표로", "데이터 받", "데이터를 받", "데이터 줘", "원본 데이터", "raw")
# 대량 덤프 의도 — '전부/싹' + '일자별/프로그램별' 조합. per-program×일자별×기간(수백~수천 칸)은
#   본질적으로 추출(표+엑셀 다운로드) 자리라 '뽑아' 없이 '보여줘'라도 추출 경로로 보낸다.
#   단건('어제 DAU 보여줘')·순위('프로그램별 순위')는 두 마커 AND라 안 걸림.
_BULK_MARK = ("전부", "싹", "몽땅", "다 보여", "전체 다")
_BREAKDOWN_MARK = ("일자별", "일별", "날짜별", "매일", "프로그램별")

def _wants_bulk_dump(question: str) -> bool:
    q = question or ""
    return any(b in q for b in _BULK_MARK) and any(d in q for d in _BREAKDOWN_MARK)

def detect_extract(question: str) -> bool:
    t = (question or "").lower()
    return any(s in t for s in _EXTRACT_SIGNAL) or _wants_bulk_dump(question)

# 추출 지원 지표(라벨→필드). 주간/월간은 _week/_mon, 장기 4종은 아카이브 병합 사용.
_EXTRACT_FIELDS = {
    "dau": "DAU", "wau": "WAU", "mau": "MAU", "dau_r7": "롤링WAU", "dau_r30": "롤링MAU",
    "new": "신규", "react": "복귀", "react_rate": "복귀율", "churn_rate": "이탈율",
    "real_rate": "실청취율", "deep_rate": "깊은청취율", "engage_rate": "참여율",
    "habit_rate": "습관형성률", "dau_1min": "1분이상청취", "dau_10min": "10분이상청취",
    "d1_ret": "D1유지율", "d7_ret": "D7유지율", "w1_ret": "W1유지율", "m1_ret": "M1유지율",
    "SMS": "문자참여수", "GG": "공감로그참여수", "TOTAL": "합계참여수",
    "SMS_WR": "문자참여자수", "GG_WR": "공감로그참여자수", "TOTAL_WR": "합계참여자수",
    "SMS_RATIO": "문자1인당참여", "GG_RATIO": "공감로그1인당참여",
}
_EXTRACT_SYS = (
    "너는 RAAS 데이터 추출 요청 파서다. 사용자의 자연어 추출 요청을 JSON으로만 응답한다.\n"
    "형식: {\"targets\":[대상들], \"per_program\":true/false, \"metrics\":[지표필드들], "
    "\"date_from\":\"YYYY-MM-DD\", \"date_to\":\"YYYY-MM-DD\"}\n"
    "- targets: 언급된 채널/프로그램(예: '파워FM','러브FM','컬투쇼'). 전체면 ['전체'].\n"
    "- per_program: '프로그램별'이면 true(채널을 소속 프로그램들로 펼침), 채널 단위면 false.\n"
    "- metrics: 아래 필드명만 사용(라벨 아닌 필드): "
    + ", ".join(f"{f}({l})" for f, l in _EXTRACT_FIELDS.items())
    + ". 주간은 _week, 월간은 _mon 접미사(예: react_rate_mon). 미지정이면 ['dau'].\n"
    "- 날짜: 연도 생략 시 2026년. 'N주간' 등 상대기간도 절대날짜로 환산."
)
_CH_PREFIX = {"F00": ("F",), "L00": ("L", "M"), "G00": ("G",), "P00": ("P",)}

def _extract_parse(question: str) -> dict:
    try:
        raw, _ = call_claude(_EXTRACT_SYS, question, max_tokens=400, model=HAIKU_MODEL)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1].lstrip("json").strip()
        return json.loads(raw)
    except Exception as e:
        print(f"[extract] parse 실패: {e}")
        return {}

def _build_extract_realtime(question: str) -> dict:
    """실시간(분단위) 동시사용자 추출 — 행=1분, 열=채널(전체·파워FM·러브FM·고릴라M·픽채널).
       날짜 지정(보관 범위 내)이면 그 날, 없으면 오늘. 원자료 다운로드라 다운샘플 없이 전 분."""
    import raas_datasource as DSRC
    import raas_rt_series as _RT
    kind = _rt_temporal(question)
    if kind and kind[0] == "unsupported":
        return {"ok": False, "reason": "분단위 다운로드는 특정일 또는 오늘만 지원 — 월/기간 범위는 아직 미지원"}
    if kind and kind[0] == "single":
        date = kind[1]
        av = _RT.rt_available(date)
        if not av["ok"]:
            return {"ok": False, "reason": f"분단위 데이터는 {av['earliest']}부터 — {date}는 보관 범위 밖"}
        when, label_date = date, date
    else:
        when, label_date = "today", _dt.date.today().strftime("%Y-%m-%d")
    tgt = _rt_resolve_target(question)                 # 공용 해석 — 프로그램/채널 지정 반영
    targets = None if tgt["kind"] == "all" else [tgt["ch_field"]]
    labels = None if tgt["kind"] == "all" else {tgt["ch_field"]: tgt["disp"]}
    tb = _RT.rt_table("concurrent", targets, when, 1, window=tgt["win"], labels=labels)  # 접근자 위임
    if not tb["rows"]:
        return {"ok": False, "reason": "분단위 데이터 없음"}
    win = tgt["win"]
    _scope = tgt["disp"] + (f" 편성창 {win[0]}~{win[1]}" if win else "")
    title = f"분단위 동시사용자(1분 간격) · {_scope} · {label_date}"
    sheet = {"field": "rt_concurrent", "label": f"분단위 동시사용자 — {tgt['disp']}",
             "header": tb["header"], "rows": tb["rows"]}
    return {"ok": True, "payload": {
        "title": title, "date_from": label_date, "date_to": label_date, "row_label": "분",
        "programs": [], "row_count": len(tb["rows"]), "col_count": len(tb["channels"]), "sheets": [sheet]}}


def build_extract(question: str, overlay_ctx=None) -> dict:
    """자연어 추출 요청 → {ok, payload}. payload에 지표별 시트(행=날짜, 열=프로그램).
       실시간(동시사용자) 의도면 1분 간격 채널 표로 분기."""
    if _detect_realtime(question):
        return _build_extract_realtime(question)
    spec = _extract_parse(question)
    try:
        rows = S._kpi_rows() or []
    except Exception:
        rows = []
    if not rows:
        return {"ok": False, "reason": "데이터 없음"}
    # 지표 필드 정리
    fields = [f for f in (spec.get("metrics") or ["dau"]) if isinstance(f, str)] or ["dau"]
    # 대상 → 코드 목록
    per_prog = bool(spec.get("per_program"))
    all_codes = sorted({r.get("PGM_CODE") for r in rows if r.get("PGM_CODE")})
    def _expand(target):
        t = (target or "").strip()
        if t in ("전체", "전사", "all", "T00"):
            return [c for c in all_codes if not c.endswith("00") and c != "L04"] if per_prog else ["T00"]
        ch = S._CHANNEL_CODE.get(t) or (t if t in _CH_PREFIX else None)
        if ch:
            if per_prog:
                pf = _CH_PREFIX.get(ch, ())
                return [c for c in all_codes if c.startswith(pf) and not c.endswith("00") and c != "L04"]
            return [ch]
        p = extract_program(t)                       # 프로그램명
        return [p["code"]] if p else []
    codes, seen = [], set()
    for tg in (spec.get("targets") or ["전체"]):
        for c in _expand(tg):
            if c not in seen:
                seen.add(c); codes.append(c)
    if not codes:
        return {"ok": False, "reason": "대상 프로그램/채널 미식별"}
    # 기간
    def _norm(d):
        d = (d or "").replace("-", "/").strip()
        return d if len(d) == 10 else None
    dfrom, dto = _norm(spec.get("date_from")), _norm(spec.get("date_to"))
    # 값 인덱스: (code,field) → {date: val}
    import raas_datasource as DSRC
    hist_metrics = set(_HIST_METRICS)
    engage_metrics = set(DSRC.ENGAGE_METRICS)
    def _series(code, field):
        if field in hist_metrics:                     # 장기 4종은 아카이브 병합
            return dict((d.replace("-", "/"), v) for d, v in DSRC.get_history_series_merged(code, field))
        if field in engage_metrics:                   # 참여(SMS·GG 등)는 참여 아카이브
            return dict((d.replace("-", "/"), v) for d, v in DSRC.get_engagement_series(code, field))
        out = {}
        for r in rows:
            if r.get("PGM_CODE") == code:
                v = _to_float(r.get(field))
                if v is not None:
                    out[(r.get("DATE") or "").strip()] = v
        return out
    # 전체 날짜 축(대상·지표 통틀어 존재하는 날짜, 기간 필터)
    date_set = set()
    cache = {}
    for f in fields:
        for c in codes:
            s = cache[(c, f)] = _series(c, f)
            date_set.update(s.keys())
    dates = sorted(d for d in date_set
                   if (not dfrom or d >= dfrom) and (not dto or d <= dto))
    if not dates:
        return {"ok": False, "reason": "해당 기간 데이터 없음"}
    prog_meta = [{"code": c, "name": _resolve_name(c)} for c in codes]
    sheets = []
    for f in fields:
        header = ["날짜"] + [f"{p['name']}({p['code']})" for p in prog_meta]
        srows = []
        for d in dates:
            row = [d.replace("/", "-")]
            for c in codes:
                v = cache[(c, f)].get(d)
                row.append(round(v) if isinstance(v, float) else (v if v is not None else ""))
            srows.append(row)
        sheets.append({"field": f, "label": _EXTRACT_FIELDS.get(f, f),
                       "header": header, "rows": srows})
    tgt_label = ", ".join(spec.get("targets") or []) + (" 프로그램별" if per_prog else "")
    title = f"{tgt_label} {'·'.join(s['label'] for s in sheets)} · {dates[0]}~{dates[-1]}"
    return {"ok": True, "payload": {
        "title": title.strip(), "date_from": dates[0], "date_to": dates[-1],
        "programs": prog_meta, "row_count": len(dates), "col_count": len(codes),
        "sheets": sheets}}


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
    # 지표 + 속성(게스트·제목·코너·생방송) 모두 — '그 날짜 스냅샷'이 편성/출연까지 답하도록
    keys = _KPI_BROAD + _ATTR_FIELDS + [f for f in focus if f not in _KPI_BROAD]
    vals = {k: row.get(k) for k in keys if row.get(k) not in (None, "")}
    vals["DATE"] = d
    return {"as_of": d, "요일": _dow_ko(d), "values": vals}


def _p_daily_lineup(ent):
    """[범용·a] 일자별 편성·출연 상세 — 게스트·회차제목·일일/주간 코너·생방송·보이는라디오를 날짜별로.
       '지난주 게스트 일자별로'처럼 속성/편성 필드를 기간만큼 조회하는 질의를 커버."""
    hist = _window_slice(ent.get("history") or [], ent)
    rows = []
    for h in hist:
        d = (h.get("DATE") or "").strip()
        rec = {"DATE": d, "요일": _dow_ko(d)}
        for f in _ATTR_FIELDS:
            v = h.get(f)
            if v not in (None, ""):
                rec[f] = v
        if len(rec) > 2:                       # DATE·요일 외 값이 하나라도 있을 때만(집계행·빈날 제외)
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
    latest = ent.get("as_of_date") or max((r.get("DATE") for r in rows if r.get("DATE")), default=None)
    if not latest:
        return None
    # 프로그램 행의 표준 일간 지표 전체(규모·흐름·품질·유지) — 특정 지표만 하드코딩해 빠지는 것 방지.
    #   빈 값은 아래 루프에서 자동 제외되므로 없는 지표는 알아서 안 실림.
    keys = ["dau", "dau_chg", "new", "new_chg", "react", "react_chg",
            "react_rate", "react_rate_diff", "churn_rate", "churn_rate_diff",
            "real_rate", "real_rate_diff", "deep_rate", "deep_rate_diff",
            "engage_rate", "engage_rate_diff", "habit_rate", "habit_rate_diff",
            "d1_ret", "d7_ret", "dau_1min", "dau_10min"]
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


# ── 편성 이력(B: 규칙은 온톨로지 공리, 조립은 LLM) — provider는 원자료+공리만 얇게 제공 ──
def _airing_raw(channel_code):
    """채널의 편성 이력 원자료 — 종영분(온톨로지 raas:ProgramAiring, 불변) + 현행(라이브) +
       해석 공리(24h 연속편성·시간단위·시간대 이동). 시간대 조립/스패닝/프랜차이즈 판정은
       LLM이 공리를 적용해 수행(코드에 규칙을 박지 않음 — 온톨로지 확장 시 자동 반영)."""
    prefixes = _CH_PREFIX.get(channel_code)
    if not prefixes:
        return None
    try:
        from raas_onto import get_adapter
        a = get_adapter()
    except Exception:
        return None
    # 종영분 — 원자료 그대로(자리 그룹핑·스패닝은 LLM 몫)
    ended = [{"name": r["name"], "air_time": r["slot_start"], "analysis_hour": r["analysis_slot"][:2],
              "start_date": r["start_date"] or None, "end_date": r["end_date"],
              "start_estimated": r["start_prov"] == "derived"}
             for r in a.get_program_airings(channel_code=channel_code)]
    # 현행 — 라이브 최신일 채널 프로그램(STIME 그대로). 같은 STIME에 평일·주말 편성 공존 가능.
    try:
        rows = S._kpi_rows() or []
        latest = max((r.get("DATE") for r in rows if r.get("DATE")), default=None)
    except Exception:
        rows, latest = [], None
    current, seen = [], set()
    for r in rows:
        if r.get("DATE") != latest:
            continue
        c = (r.get("PGM_CODE") or "").upper()
        st = str(r.get("STIME") or "")
        nm = (r.get("PGM_NAME") or "").strip()
        if (not c or c.endswith("00") or c in seen or not c.startswith(prefixes)
                or len(st) != 4 or not st.isdigit() or not nm):
            continue
        seen.add(c)
        current.append({"name": nm, "code": c, "start_time": st})
    if not ended and not current:
        return None
    current.sort(key=lambda p: p["start_time"])
    try:
        rules = get_adapter().get_domain_axioms(channel_code)
    except Exception:
        rules = []
    return {"ended_airings": ended, "current_programs": current,
            "rules": rules,
            "note": ("종영분=불변 이력(start_estimated=직전 편성 종료+1일 추정), 현행=라이브. "
                     "시간대별 편성은 rules(공리)를 적용해 조립하라: analysis_hour로 자리 묶기, "
                     "24h 연속편성이라 각 시각 방송분=시작≤그시각 중 최근(다시간 편성은 여러 시각 커버), "
                     "같은 쇼명이 다른 시간대에 있으면 시간대 이동.")}


def _p_channel_history(ent):
    """[채널 전용·얇음] 채널 편성 이력 원자료(종영+현행)+공리 → LLM이 시간대별로 조립.
       '러브FM 시간대별 편성 변화·연혁' 등 채널 단위 편성 질의 근거."""
    if ent.get("scope_kind") != "channel":
        return None
    code = ent.get("code") or ""
    raw = _airing_raw(code)
    if not raw:
        return None
    return {"channel": _resolve_name(code), **raw}


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


def _p_program_history(ent):
    """[편성 이력·얇음] 이 프로그램 자리의 편성 연혁 — 채널 편성 원자료(종영+현행)+공리를 주고
       LLM이 focus 프로그램의 시간대 자리 승계·시간대 이동(프랜차이즈)을 조립.
       '영스트리트 역대 DJ', '이 시간대 예전 프로그램', '전임 진행자' 등에 근거 제공."""
    code = ent.get("code")
    if not code or (len(code) == 3 and code.endswith("00")):  # 채널 집계코드 제외
        return None
    try:
        from raas_onto import get_adapter
        meta = get_adapter().get_program_meta(code) or {}
    except Exception:
        return None
    ch = ((meta.get("channel") or {}).get("code")) or ""
    raw = _airing_raw(ch)
    if not raw:
        return None
    slot = (meta.get("time_slot") or {}).get("start") or ""
    focus = {"name": S._pgm_name(code), "code": code,
             "analysis_hour": slot[:2] if len(slot) >= 2 and slot[:2].isdigit() else None,
             "time_slot": f"{slot}~{(meta.get('time_slot') or {}).get('end', '')}".strip("~")}
    # 이 프로그램의 자리(analysis_hour)에 집중하되, 같은 쇼명이 있으면 다른 시간대 이력도 참고.
    return {"channel": (meta.get("channel") or {}).get("label") or ch,
            "focus_program": focus, **raw}


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


_COV_META = {"DATE", "DATE_WEEK", "DATE_MON", "PGM_CODE", "PGM_NAME", "STIME",
             "program_title", "guestname", "daily_corner", "weekly_corner",
             "view_radio_yn", "live_yn"}
_COV_SKIP_SUFFIX = ("_prev", "_chg", "_diff", "_share", "_pw")   # 전주/증감/비율 등 파생 비교값 제외

def _p_data_coverage(ent):
    """이 scope의 '전체 지표' 데이터 보유 범위를 답할 때마다 라이브로 계산 + 온톨로지 정의 첨부.
       하드코딩 목록 없이 룩업에 실재하는 모든 지표 필드를 스캔(일간/주간/월간 변형 포함)하므로,
       'react_rate_mon(월간 복귀율)은 언제부터?' 같은 변형 지표도 최초일+정의로 답할 수 있다.
       과거 데이터를 백필하면 다음 질의부터 자동 반영."""
    code = ent.get("code")
    if not code:
        return None
    try:
        rows = S._kpi_rows() or []
    except Exception:
        return None
    first, all_dates = {}, set()
    for r in rows:
        if r.get("PGM_CODE") != code:
            continue
        d = (r.get("DATE") or "").strip()
        if not d:
            continue
        all_dates.add(d)
        for f, v in r.items():
            if f in _COV_META or f.endswith(_COV_SKIP_SUFFIX):
                continue
            vs = str(v or "").strip()
            if vs and vs not in ("0", "0.0", ".0"):
                if f not in first or d < first[f]:
                    first[f] = d
    if not all_dates:
        return None
    # 온톨로지 정의(지표·단위) 첨부 — 지식은 온톨로지에서
    try:
        from raas_onto import get_adapter
        a = get_adapter()
    except Exception:
        a = None
    def _defstr(f):
        if not a:
            return ""
        try:
            info = a.get_field_info(f) or {}
            m = info.get("metric") or {}
            parts = [x for x in (m.get("label"),
                                 (info.get("granularity") or {}).get("label"),
                                 (info.get("variant") or {}).get("label"))
                     if x and x != "현재값"]
            return " · ".join(parts)
        except Exception:
            return ""
    ad = sorted(all_dates)
    metrics = {f: {"최초일": first[f], "정의": _defstr(f)} for f in sorted(first)}
    out = {"보유 범위(상세 KPI)": f"{ad[0]} ~ {ad[-1]}", "총 일수": len(ad),
           "지표별 최초 수집일·정의": metrics,
           "안내": "현재 룩업 실측 스캔값(백필 시 자동 갱신). 정의는 온톨로지 기준. "
                   "지표별 시작일이 다를 수 있음(예: 월간 지표는 첫 월말/월초부터)."}
    # 장기 아카이브(별도 원천) 병합 — dau·롤링WAU·롤링MAU·1분청취는 더 이른 시작일 보유
    try:
        import raas_datasource as DSRC
        arch = {}
        for m in _HIST_METRICS:
            s = DSRC.get_history_series(code, m)
            if s:
                arch[m] = f"{s[0][0]} ~ {s[-1][0]}"
        if arch:
            out["장기 아카이브(별도 원천, 이 4개 지표만)"] = arch
            out["안내"] += " dau·dau_r7·dau_r30·dau_1min은 장기 아카이브가 더 이른 시작일을 " \
                          "보유 — '언제부터' 답변엔 아카이브 시작일을 우선 안내."
    except Exception:
        pass
    return out


_HIST_METRICS = ("dau", "dau_r7", "dau_r30", "dau_1min")   # 장기 아카이브 보유 4개 지표
_HIST_SIGNAL = ("작년", "재작년", "년 전", "년전", "장기", "역대", "수년", "몇 년", "연도별", "해 전")

def _history_years(q: str) -> list:
    """질문의 연도(2015~올해)를 4자리로 정규화해 추출. 4자리('2026')·2자리('26년') 모두.
       상단 경계는 올해(동적) — 상세 KPI가 연중 일부만 커버해 '올해 초'도 아카이브 영역이라
       현재 연도까지 인식해야 한다(2025 하드코딩 상단이 2026을 놓치던 회귀 방지)."""
    t = q or ""
    cur = _dt.date.today().year
    out = set()
    for m in re.findall(r"20\d{2}", t):
        if 2015 <= int(m) <= cur:
            out.add(m)
    for m in re.findall(r"(?<!\d)([12]\d)년", t):        # '26년' → 2026
        y = 2000 + int(m)
        if 2015 <= y <= cur:
            out.add(str(y))
    return sorted(out)


def _wants_history(q: str) -> bool:
    """연도(2015~올해)·장기 신호 → 장기 아카이브 provider 강제 포함."""
    t = q or ""
    if _history_years(t):
        return True
    return any(s in t for s in _HIST_SIGNAL)


_TODAY_SIGNAL = ("오늘", "금일", "오늘자", "today")
_LINEUP_SIGNAL = ("게스트", "초대손님", "초대 손님", "손님", "출연", "누가 나와", "누구 나와",
                  "누가 출연", "편성", "보이는라디오", "보이는 라디오", "보라")

def _wants_today_lineup(q: str) -> bool:
    """'오늘' + 게스트/편성 신호 → 오늘 편성 provider 강제 포함.
       상세 KPI 배치가 아직 못 담은 당일 게스트·보라 편성을 broadplan 라이브에서."""
    t = q or ""
    return any(a in t for a in _TODAY_SIGNAL) and any(b in t for b in _LINEUP_SIGNAL)


# 문자(SMS)·공감로그(GG) 참여 — '참여율'(engage_rate, 청취 몰입)과 구분되게 신호는 명시 어휘만
_ENGAGE_SIGNAL = ("문자", "sms", "공감로그", "공감 로그", "작성자", "1인당")

def _wants_engagement(q: str) -> bool:
    """문자·공감로그 참여 질의 → 참여 provider 강제 포함(과거 1년 프로그램별 SMS·GG)."""
    return any(s in (q or "").lower() for s in _ENGAGE_SIGNAL)


# 프로그램별 일자별 성별·연령·디바이스 분포(룩업) — 실시간 '지금'이 아닌 일자 단위
_DEMO_GENDER = {"F": "여", "M": "남"}
_DEMO_AGE = {"UNDER20": "10대이하", "20_24": "20-24", "25_29": "25-29", "30_34": "30-34",
             "35_39": "35-39", "40_44": "40-44", "45_49": "45-49", "50_54": "50-54",
             "55_59": "55-59", "OVER60": "60대이상"}
_DEMO_DEVICE = {"SP": "스마트폰", "PC": "PC", "PW": "웹", "PWR": "웹(기타)", "MWEB": "모바일웹",
                "CAR": "차량", "WATCH": "워치", "AI": "AI스피커"}

def _wants_program_demo(q: str) -> set:
    """프로그램 성별·연령·디바이스 분포 의도(일자 단위). 반환: {'gender','age','device'} 부분집합."""
    t = q or ""
    kinds = set()
    if any(k in t for k in ("성별", "남녀", "여성", "남성", "남자", "여자")):
        kinds.add("gender")
    if any(k in t for k in ("연령", "나이", "세대", "대별", "10대", "20대", "30대", "40대", "50대", "60대")):
        kinds.add("age")
    if any(k in t for k in ("디바이스", "기기", "스마트폰", "AI스피커", "차량", "워치")):
        kinds.add("device")
    return kinds


_EDITORIAL_SIGNAL = ("편성 변화", "편성변화", "편성 이력", "편성이력", "편성 연혁", "편성연혁",
                     "편성 개편", "편성개편", "편성 변천", "편성변천", "과거 편성", "예전 편성",
                     "예전 프로그램", "이전 프로그램", "과거 프로그램", "역대 프로그램", "역대 편성",
                     "무슨 프로그램이 있었", "어떤 프로그램이 있었", "전임 프로그램")


def _wants_editorial(q: str) -> bool:
    """'편성 변화·이력·연혁·역대·예전 프로그램' 등 편성 연혁 의도.
       채널 스코프에서 편성 이력 provider를 주경로로 세우고 KPI·아카이브 경쟁 제거."""
    return any(s in (q or "") for s in _EDITORIAL_SIGNAL)

def _p_long_history(ent):
    """장기 아카이브(최대 10년) 시계열 — dau·롤링WAU·롤링MAU·1분청취 4개 지표만.
       전체 기간은 월평균으로 다운샘플(토큰 절약), 질문에 특정 연도 언급 시 그 해 일별 첨부.
       지표 정의·아카이브 성격(원천 차이 등)은 온톨로지에서 함께 표면화."""
    code = ent.get("code")
    if not code:
        return None
    import raas_datasource as DSRC
    from collections import defaultdict
    # 상세 KPI 우선 병합 — 겹치는 기간은 raas_kpi_latest(메인) 값, 그 이전만 아카이브
    series = {m: DSRC.get_history_series_merged(code, m) for m in _HIST_METRICS}
    if not any(series.values()):
        return None
    cov = {m: f"{s[0][0]} ~ {s[-1][0]} ({len(s)}일)" for m, s in series.items() if s}
    # 월평균 CSV — 전체 아카이브를 월 단위로 (10년×4지표도 ~100행)
    monthly = defaultdict(dict)
    for m, s in series.items():
        agg = defaultdict(lambda: [0.0, 0])
        for d, v in s:
            a = agg[d[:7].replace("/", "-")]
            a[0] += v; a[1] += 1
        for ym, (sm, c) in agg.items():
            monthly[ym][m] = round(sm / c)
    lines = ["month," + ",".join(_HIST_METRICS)]
    for ym in sorted(monthly):
        lines.append(ym + "," + ",".join(str(monthly[ym].get(m, "")) for m in _HIST_METRICS))
    out = {"지표별 아카이브 범위": cov, "월평균(CSV)": "\n".join(lines)}
    # 특정 연도 언급 → 그 해 일별(포커스 지표 1개, 최대 2개 연도). '25년'(2자리)도 인식.
    q = ent.get("_question") or ""
    yrs = _history_years(q)[:2]
    if yrs:
        focus = next((f for f in (ent.get("focus_fields") or []) if f in _HIST_METRICS), "dau")
        for y in yrs:
            dly = [f"date,{focus}"] + [f"{d.replace('/', '-')},{int(v)}"
                                       for d, v in series.get(focus) or [] if d.startswith(y)]
            if len(dly) > 1:
                out[f"{y}년 일별({focus}, CSV)"] = "\n".join(dly)
    # 온톨로지 — 지표 정의 + 아카이브 공리(원천·범위·주의)
    onto = _metric_def_lines(list(_HIST_METRICS))
    try:
        from raas_onto import get_adapter
        ax = get_adapter()._onto
        t = ax.value_str(ax.get_one("raas:HistoricalArchiveAxiom", "rdfs:comment"))
        if t:
            onto.append(f"- [장기 아카이브 성격] {t}")
    except Exception:
        pass
    if onto:
        out["온톨로지 근거"] = "\n".join(onto)
    return out


def _p_today_lineup(ent):
    """[오늘 편성·게스트] 오늘(진행 중) 프로그램별 게스트·보라 편성 — 상세 KPI 배치가 아직
       못 담은 당일. broadplan 라이브(하루 1회 캐시). '오늘 게스트 누구·오늘 프로그램별 출연/보라'."""
    import raas_datasource as DSRC
    try:
        rows = DSRC.get_today_lineup() or []
    except Exception:
        return None
    if not rows:
        return None
    code = ent.get("code")
    allp = ent.get("all_programs") or code in (None, "T00", "")
    prefixes = _CH_PREFIX.get(code) if code else None
    out = []
    for r in rows:
        c = (r.get("PGM_CODE") or "").upper()
        if not c:
            continue
        if not allp:
            if prefixes:                       # 채널 scope → 소속 프로그램만
                if not c.startswith(prefixes):
                    continue
            elif c != code:                    # program scope → 그 프로그램만
                continue
        out.append({"code": c, "name": r.get("PGM_NAME") or _resolve_name(c),
                    "guest": (r.get("GUEST") or "").strip(),
                    "보이는라디오": r.get("view_radio")})
    if not out:
        return None
    return {"date": rows[0].get("DATE"),
            "programs": out,
            "note": ("오늘(진행 중) 편성·게스트. guest 빈값=초대손님 없음, 보이는라디오 Y/N. "
                     "이 질의는 오늘 편성/게스트 조회이므로 게스트·보라 편성만 간결히 답하고, "
                     "오늘은 아직 미집계인 측정 지표(DAU 등)의 '데이터 없음'은 언급하지 말 것.")}


_ENGAGE_FIELDS = ("SMS", "GG", "TOTAL", "SMS_WR", "GG_WR", "TOTAL_WR", "SMS_RATIO", "GG_RATIO")

# provider → 온톨로지 sourceCaveat를 끌어올 필드. 데이터 정확도 주의문을 답변에 전체 노출.
#   새 provider에 주의문이 필요하면 여기 한 줄 + 온톨로지 지표에 raas:sourceCaveat만 추가.
_PROVIDER_CAVEAT_FIELDS = {"engagement": _ENGAGE_FIELDS}


def caveats_for(providers_used) -> list:
    """사용된 provider들의 데이터 출처 주의문(온톨로지 raas:sourceCaveat) distinct 목록."""
    fields = []
    for p in providers_used or []:
        fields += list(_PROVIDER_CAVEAT_FIELDS.get(p, ()))
    if not fields:
        return []
    try:
        from raas_onto import get_adapter
        return get_adapter().get_field_caveats(fields)
    except Exception:
        return []


def _p_engagement(ent):
    """[참여] 프로그램별 문자(SMS)·공감로그(GG) 참여 — 과거 1년(평일). 정의는 온톨로지.
       단일 프로그램=최근 일자별 시계열, 전체/채널=최신일 프로그램별 스냅샷(순위·비교)."""
    import raas_datasource as DSRC
    idx = DSRC.get_engagement_index()
    if not idx:
        return None
    code = ent.get("code")
    allp = ent.get("all_programs") or code in (None, "T00", "")
    prefixes = _CH_PREFIX.get(code) if code else None
    if allp:
        codes = [c for c in idx if not c.endswith("00")]
    elif prefixes:
        codes = [c for c in idx if c.startswith(prefixes) and not c.endswith("00")]
    else:
        codes = [code] if code in idx else []
    if not codes:
        return None
    defs = _metric_def_lines(list(_ENGAGE_FIELDS))
    note = ("문자·공감로그 참여(평일만). _WR=참여자(작성자) 수, _RATIO=1인당 참여횟수(건수/참여자수), "
            "TOTAL=SMS+GG. 정의는 metric_defs 참조.")
    if len(codes) > 1:                                    # 전체/채널 → 최신일 프로그램별 스냅샷
        latest = max((d for c in codes for d in idx.get(c, {})), default=None)
        progs = []
        for c in codes:
            r = idx.get(c, {}).get(latest)
            if not r:
                continue
            rec = {"code": c, "name": _resolve_name(c)}
            for f in _ENGAGE_FIELDS:
                if r.get(f) not in (None, ""):
                    rec[f] = r[f]
            progs.append(rec)
        return {"as_of": latest, "programs": progs, "metric_defs": defs, "note": note}
    c = codes[0]                                          # 단일 프로그램 → 최근 일자별 시계열
    lb = ent.get("lookback") or 0
    dates = sorted(idx.get(c, {}).keys())
    win = dates[-lb:] if lb > 0 else dates[-20:]
    series = [dict({"date": d}, **{f: idx[c][d][f] for f in _ENGAGE_FIELDS
                                   if idx[c][d].get(f) not in (None, "")}) for d in win]
    return {"code": c, "name": _resolve_name(c), "series": series, "metric_defs": defs, "note": note}


def _p_program_demographics(ent):
    """[프로필 분포] 프로그램별 성별·연령·디바이스 분포(비율%). 룩업 3종.
       축: PERIOD=집계창(1D/1W/1M) · TYPE=청취깊이(ALL/1MIN/10MIN) · CATEGORY=인구 카테고리.
       '컬투쇼 어제 연령대/성별/디바이스 분포' 등. 창 선택은 현재 1D 고정(Phase 1에서 확장)."""
    code = ent.get("code")
    if not code or (len(code) == 3 and code.endswith("00") and not ent.get("all_programs")):
        pass  # 채널코드도 룩업에 존재(F00 등) — 그대로 조회 허용
    q = ent.get("_question") or ""
    kinds = _wants_program_demo(q)
    if not code or not kinds:
        return None
    import raas_series as SR
    # 집계창(PERIOD) 라우팅 — 통합 접근자가 소스 형태 무관하게 처리(1D 없으면 device는 자동 생략)
    if any(k in q for k in ("월간", "월별", "월 단위", "지난달", "전월", "이번달", "개월")):
        window, wlabel, unit = "1M", "월간(1M)", "개월"
    elif any(k in q for k in ("주간", "주별", "주 단위", "지난주", "전주", "이번주")):
        window, wlabel, unit = "1W", "주간(1W)", "주"
    else:
        window, wlabel, unit = "1D", "일간(1D)", "일"
    rowlabel = {"1D": "일별", "1W": "주별", "1M": "월별"}[window]
    depth = "10MIN" if ("10분" in q or "깊은" in q) else ("1MIN" if "1분" in q else "ALL")
    dims = {"DEPTH": depth}
    lb = ent.get("lookback") or 0
    as_of = (ent.get("as_of_date") or "").replace("-", "/")

    def _view(source, labels):
        dates = SR.available_dates(source, code, window=window, dims=dims)
        if not dates:
            return None
        if lb and lb > 1:                        # 기간 모드 — 기간평균 + 기간 내 추이(CSV)
            rows = [(d, SR.cell(source, code, d, window=window, dims=dims)) for d in dates[-lb:]]
            cats = list(rows[-1][1].keys())      # 최신 셀 기준 CATEGORY 순서
            acc = {c: [] for c in cats}
            for _, cell in rows:
                for c in cats:
                    fv = _rt_flt(cell.get(c))
                    if fv is not None:
                        acc[c].append(fv)
            avg = {labels.get(c, c): round(sum(acc[c]) / len(acc[c]), 1) for c in cats if acc[c]}
            csv = ["date," + ",".join(labels.get(c, c) for c in cats)]
            for d, cell in rows:
                csv.append(",".join([d[5:]] + [str(cell.get(c, "")) for c in cats]))
            return {"기간": f"{rows[0][0]}~{rows[-1][0]} ({len(rows)}{unit})",
                    "기간평균%": avg, f"{rowlabel}(CSV, 평균은 코드계산)": "\n".join(csv)}
        # 스냅샷 모드(단일 기간)
        date = as_of if (as_of and as_of in dates) else dates[-1]
        cell = SR.cell(source, code, date, window=window, dims=dims)
        out = {labels.get(k, k): v for k, v in cell.items() if v not in (None, "")}
        return {"as_of": date, "분포%": out} if out else None

    res = {"program": _resolve_name(code), "code": code,
           "청취깊이": {"ALL": "청취시작", "1MIN": "1분이상청취", "10MIN": "10분이상청취"}[depth],
           "집계창": wlabel,
           "기준": ("기간(추이+평균)" if lb and lb > 1 else "단일 기간 스냅샷")}
    for kind, source, labels, key in (
            ("gender", "pgm_gender", _DEMO_GENDER, "성별 분포"),
            ("age", "pgm_age", _DEMO_AGE, "연령대 분포"),
            ("device", "pgm_device", _DEMO_DEVICE, "디바이스 분포")):
        if kind in kinds:
            v = _view(source, labels)
            if v:
                res[key] = v
    return res if len(res) > 5 else None


_CORRELATE_SIGNAL = ("상관", "동반", "같이 움직", "함께 움직", "같이 갔", "같이 떨어", "같이 올랐",
                     "같이 하락", "같이 상승", "연관", "관련이", "관계가", "무엇과", "뭐랑", "뭐와",
                     "어떤 지표와", "영향을 미", "영향을 준", "좌우", "따라 움직", "동조", "함께 떨어")


def _wants_correlate(q: str) -> bool:
    """교차지표 동반움직임/상관 의도 — 'DAU가 여성비율·참여와 같이 갔나' 등.
       원인 흐름분해(flow_decomp, '왜 빠졌어')와 별개: 지표 간 관계를 물을 때."""
    return any(s in (q or "") for s in _CORRELATE_SIGNAL)


# 교차상관 후보 팩터(드라이버 공간) — (source, field, dims, label).
# 곧 Phase 3에서 온톨로지 raas:mayDrive로 이관 예정(지금은 명시 목록).
_CORRELATE_FACTORS = [
    ("kpi", "deep_rate", None, "깊은청취율"),
    ("kpi", "engage_rate", None, "참여율"),
    ("kpi", "real_rate", None, "실청취율"),
    ("kpi", "react_rate", None, "복귀율"),
    ("kpi", "churn_rate", None, "이탈률"),
    ("kpi", "habit_rate", None, "습관형성률"),
    ("kpi", "new", None, "신규사용자"),
    ("kpi", "react", None, "복귀사용자"),
    ("engagement", "SMS", None, "문자참여"),
    ("engagement", "GG", None, "공감로그참여"),
    ("pgm_gender", "F", {"DEPTH": "ALL"}, "여성비율"),
    ("pgm_gender", "M", {"DEPTH": "ALL"}, "남성비율"),
    ("pgm_age", "20_24", {"DEPTH": "ALL"}, "20-24비율"),
    ("pgm_age", "40_44", {"DEPTH": "ALL"}, "40-44비율"),
    ("pgm_age", "OVER60", {"DEPTH": "ALL"}, "60대이상비율"),
    ("pgm_device", "SP", {"DEPTH": "ALL"}, "스마트폰비율"),
    ("pgm_device", "AI", {"DEPTH": "ALL"}, "AI스피커비율"),
]

# 대상지표(target) 감지 — 기본 DAU. (source, field, dims, label)
_TARGET_HINTS = [
    (("참여율",), ("kpi", "engage_rate", None, "참여율")),
    (("깊은청취", "몰입"), ("kpi", "deep_rate", None, "깊은청취율")),
    (("실청취",), ("kpi", "real_rate", None, "실청취율")),
    (("이탈",), ("kpi", "churn_rate", None, "이탈률")),
    (("복귀율",), ("kpi", "react_rate", None, "복귀율")),
    (("습관",), ("kpi", "habit_rate", None, "습관형성률")),
    (("문자참여", "문자 참여"), ("engagement", "SMS", None, "문자참여")),
    (("여성비율", "여성 비율"), ("pgm_gender", "F", {"DEPTH": "ALL"}, "여성비율")),
]


def _target_metric(q: str):
    # DAU가 언급되면 대상(subject)일 확률이 큼 — 비교 팩터로 언급된 지표를 target으로 오인 방지
    if "dau" in (q or "").lower() or "활성 사용자" in q or "활성사용자" in q \
            or "청취자 수" in q or "청취자수" in q:
        return ("kpi", "dau", None, "DAU")
    for keys, tgt in _TARGET_HINTS:
        if any(k in q for k in keys):
            return tgt
    return ("kpi", "dau", None, "DAU")


def _p_metric_correlate(ent):
    """[교차지표] 대상지표(기본 DAU) 대비 후보 팩터들의 동반움직임·상관을 결정적으로 계산.
       소스를 가로질러(참여·성별·연령·디바이스·기타 KPI) 같은 창에서 정렬·상관 — 숫자는 코드,
       관계·인과 해석은 LLM(+온톨로지). '무엇과 같이 움직였나' 류."""
    q = ent.get("_question") or ""
    code = ent.get("code")
    if not code or ent.get("scope_kind") != "program" or not _wants_correlate(q):
        return None
    import raas_series as SR
    import raas_analytics as AN
    from raas_onto import get_adapter
    t_src, t_field, t_dims, t_label = _target_metric(q)
    lb = ent.get("lookback") or 45
    target = SR.series(t_src, code, t_field, dims=t_dims)[-lb:]
    if len(target) < 5:
        return None
    lo, hi = target[0][0], target[-1][0]
    # 팩터 목록은 온톨로지(raas:CorrelationFactor)에서 — 추가는 TTL만(코드 무관). 없으면 하드코딩 폴백.
    try:
        onto_f = get_adapter().get_correlation_factors()
        cat = [(f["source"], f["field"], f["dims"], f["label"]) for f in onto_f] or _CORRELATE_FACTORS
    except Exception:
        cat = _CORRELATE_FACTORS
    factors = {}
    for src, field, dims, label in cat:
        if (src, field) == (t_src, t_field):
            continue
        ser = [(d, v) for d, v in SR.series(src, code, field, dims=dims) if lo <= d <= hi]
        if len(ser) >= 5:
            factors[label] = ser
    if not factors:
        return None
    dec = AN.decompose(target, factors)
    top = [{"지표": r["factor"], "r": r["r"], "방향": r["direction"], "강도": r["strength"],
            "표본": r["n"], "변화%": (r["change"] or {}).get("pct")} for r in dec["factors"][:8]]
    if not top:
        return None
    res = {
        "program": _resolve_name(code), "code": code,
        "대상지표": t_label, "창": f"{lo}~{hi} ({len(target)}일)",
        "대상변화": dec["target_change"],
        "동반움직임(|r| 상위)": top,
    }
    try:                                      # 관계 지식(온톨로지) — 의미 있는 상관 vs 구성 효과 판별
        rel = get_adapter().get_metric_relations_block()
        if rel:
            res["관계지식(온톨로지)"] = rel
    except Exception:
        pass
    return res


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
    {"name": "today_lineup", "needs": "program",
     "desc": "오늘(진행 중) 프로그램별 게스트·보이는라디오 편성 — 상세 KPI 배치가 아직 못 담은 당일. '오늘 게스트 누구·오늘 프로그램별 출연/보라 편성' 등 오늘자 편성 조회",
     "fetch": _p_today_lineup},
    {"name": "engagement", "needs": "program",
     "desc": "프로그램별 문자(SMS)·공감로그(GG) 참여(과거 1년, 평일) — 참여 건수·참여자수(_WR)·1인당(_RATIO)·합계(TOTAL). '컬투쇼 문자 참여·공감로그 몇 건·프로그램별 참여 순위·1인당 참여' 등",
     "fetch": _p_engagement},
    {"name": "program_demographics", "needs": "program",
     "desc": "프로그램별 일자별 청취자 성별·연령대·디바이스 분포(비율%, 룩업). PERIOD 청취시작/1분이상/10분이상. '컬투쇼 어제 연령대·성별·디바이스 분포·주 시청층' 등",
     "fetch": _p_program_demographics},
    {"name": "field_projection", "needs": "program",
     "desc": "질문이 지목한 필드(지표+속성)를 일자별 표로 묶음 — 특정 필드 콕 집기/지표+속성 혼합('DAU와 게스트 일자별') 대응",
     "fetch": _p_field_projection},
    {"name": "channel_programs", "needs": "channel",
     "desc": "채널 소속 프로그램별 최신 KPI 나열 — '채널 내 프로그램 비교·순위·많이 하락한 프로그램' 질의(집계행만으론 답 불가)",
     "fetch": _p_channel_programs},
    {"name": "channel_history", "needs": "channel",
     "desc": "채널 전체의 시간대별 편성 이력 — 자리(정시)마다 역대 종영 프로그램 승계 + 현행. '채널 편성 변화·시간대별 편성 연혁·과거 편성표·언제부터 이 프로그램들' 등 채널 단위 편성 질의",
     "fetch": _p_channel_history},
    {"name": "metric_timeseries", "needs": "program",
     "desc": "주요 지표 시계열(의도 기반: 추이 질의는 전체, 원인·포인트 질의는 최근 4주)",
     "fetch": _p_metric_timeseries},
    {"name": "metric_correlate", "needs": "program",
     "desc": "대상지표(기본 DAU) 대비 여러 지표(참여·성별·연령·디바이스·기타 KPI)의 동반움직임·상관을 "
             "소스 가로질러 결정적으로 계산 — 'DAU가 여성비율·문자참여와 같이 움직였나·무엇과 연관·상관' 등 지표 간 관계 질의",
     "fetch": _p_metric_correlate},
    {"name": "data_coverage", "needs": "program",
     "desc": "데이터 보유 기간·범위 — '언제부터/언제까지 데이터 있나·며칠치·수집 기간' 등 데이터 커버리지 질의(실제 룩업 최초/최신일)",
     "fetch": _p_data_coverage},
    {"name": "long_history", "needs": "program",
     "desc": "장기 아카이브 시계열(최대 10년 — dau·롤링WAU·롤링MAU·1분청취 4개 지표만) — 과거 연도·작년·수년 장기 추이/비교 질의",
     "fetch": _p_long_history},
    {"name": "ontology", "needs": "program",
     "desc": "프로그램 도메인 사실(진행자·정규게스트·시간대·편성유형·광고가치·게스트명 해석정책 등 온톨로지)",
     "fetch": _p_ontology},
    {"name": "program_history", "needs": "program",
     "desc": "이 프로그램 자리(채널·시간대)의 역대 편성 이력 — 종영 프로그램 승계 체인(이전 진행자·프로그램)과 현행. '역대 DJ/진행자·예전 프로그램·언제부터 이 자리·전임' 등 편성 연혁 질의",
     "fetch": _p_program_history},
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

# '데이터 언제부터/언제까지/며칠치/보유·수집 기간' 등 커버리지 질의 신호
_COVERAGE_SIGNAL = ("언제부터", "언제까지", "언제 부터", "언제 까지", "며칠부터", "며칠치",
                    "몇일부터", "부터 있", "까지 있", "보유 기간", "수집 기간", "보유기간",
                    "얼마나 오래", "얼마나 됐", "데이터 기간", "기간이 언제", "최초", "언제까지의")


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


_CHANNEL_COMPARE_NAMES = ["파워FM", "러브FM", "고릴라M", "픽채널"]

def _detect_compare(question: str):
    """비교 구분자로 분할해 서로 다른 엔티티 ≥2개면 비교 scope. 아니면 None."""
    t = question or ""
    # '채널별' = 채널 4개(F00/L00/G00/P00) 나란히 비교 — 엔티티 미명시라도 채널 전체가 대상
    if "채널별" in t or "채널 별" in t:
        ch = [e for e in (_resolve_one(n) for n in _CHANNEL_COMPARE_NAMES) if e]
        if len(ch) >= 2:
            return ch
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
    def _d(h):
        raw = (h.get("DATE") or "").replace("/", "-")
        dw = _dow_ko(raw)
        return raw + (f"({dw})" if dw else "")   # 요일을 데이터에 명시 — LLM이 요일 계산해 틀리는 것 방지
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
    def _period(label, flds, date_key):
        # 주간/월간은 앵커 날짜(DATE_WEEK=월요일 / DATE_MON=1일)로 1행씩 — 일별 변동점(지연저장)이 아님.
        if not flds:
            return
        by = {}
        for h in hist:
            a = (h.get(date_key) or "").strip()
            if a and any(h.get(f) not in (None, "") for f in flds):
                by[a] = h   # 같은 주/월 내 값 동일 → 대표 1행
        if not by:
            return
        anchor_txt = "주 시작(월요일)" if label == "주별" else "월 시작(1일)"
        out.append(f"[{label}] date={anchor_txt},{','.join(flds)}")
        for a in sorted(by):
            araw = a.replace("/", "-")
            dw = _dow_ko(araw)
            out.append(araw + (f"({dw})" if dw else "") + "," + ",".join(_v(by[a], f) for f in flds))
    _period("주별", weekly, "DATE_WEEK")
    _period("월별", monthly, "DATE_MON")
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
# 인구 카테고리 순위(성별·연령·디바이스 비율%) — 통합 접근자(raas_series.ranking)로 전 프로그램 정렬.
# (keys, source, field, label). KPI 필드가 아니라 분포 소스라 별도 경로.
_RANK_DEMO_MAP = [
    (("남성", "남자"), "pgm_gender", "M", "남성 비율"),
    (("여성", "여자"), "pgm_gender", "F", "여성 비율"),
    (("스마트폰", "모바일"), "pgm_device", "SP", "스마트폰 비율"),
    (("ai스피커", "ai 스피커", "인공지능 스피커"), "pgm_device", "AI", "AI스피커 비율"),
    (("차량", "자동차", "카오디오"), "pgm_device", "CAR", "차량 비율"),
    (("60대", "60세 이상", "고령", "노년"), "pgm_age", "OVER60", "60대이상 비율"),
    (("10대", "청소년"), "pgm_age", "UNDER20", "10대이하 비율"),
]

def _demo_rank_period(q: str):
    """인구 카테고리 순위의 기간 의도 → 평균 낼 일수(None=최신 스냅샷)."""
    t = q or ""
    if any(k in t for k in ("월간", "월별", "지난달", "전월", "한달", "한 달", "1개월", "한 개월", "30일", "최근 한 달")):
        return 30
    if any(k in t for k in ("주간", "주별", "지난주", "전주", "한주", "한 주", "7일", "최근 한 주")):
        return 7
    return None

def _to_float(v):
    try:
        return float(str(v).replace(",", "").replace("%", ""))
    except Exception:
        return None

_RANK_CHANGE_UP   = ("증가", "늘어", "늘은", "는 ", "상승", "오른", "급증")
_RANK_CHANGE_DOWN = ("감소", "줄어", "줄은", "하락", "떨어", "급감")
_RANK_SUPERLATIVE = ("가장", "제일", "많이", "최다", "최대")

def _detect_ranking(question: str):
    t = question or ""
    tl = t.lower()
    _chg_up   = any(k in t for k in _RANK_CHANGE_UP)
    _chg_down = any(k in t for k in _RANK_CHANGE_DOWN)
    # 변화량 순위: '가장/제일/많이' + '증가/감소' + '프로그램' 조합만 인정(오탐 방지).
    #   예 "지난주 활성사용자가 가장 많이 증가한 프로그램은?" → dau_chg 내림차순.
    by_change = ((_chg_up or _chg_down)
                 and any(s in t for s in _RANK_SUPERLATIVE) and "프로그램" in t)
    if not any(s.lower() in tl for s in _RANK_SIGNAL) and not by_change:
        return None
    # 인구 카테고리 순위(성별·연령·디바이스 비율) — 변화량 순위가 아닐 때만(분포는 _chg 없음)
    if not by_change:
        for keys, src, field, lab in _RANK_DEMO_MAP:
            if any(k.lower() in tl for k in keys):
                asc = any(k in t for k in ("낮은", "최저", "하위", "worst", "적은", "least"))
                return {"demo": True, "source": src, "field": field, "label": lab,
                        "asc": asc, "by_change": False}
    fld, label = "dau", "DAU"
    for keys, f, lab in _RANK_FIELD_MAP:
        if any(k.lower() in tl for k in keys):
            fld, label = f, lab
            break
    if by_change:
        # 감소 질의(가장 많이 감소=가장 음수)면 오름차순 정렬
        return {"field": fld, "label": label, "asc": (_chg_down and not _chg_up),
                "by_change": True}
    asc = any(k in t for k in ("낮은", "최저", "하위", "worst", "적은", "least"))
    return {"field": fld, "label": label, "asc": asc, "by_change": False}


def _assemble_ranking_demo(question, spec, overlay_ctx=None) -> dict:
    """인구 카테고리 비율(성별·연령·디바이스) 전 프로그램 순위 — 통합 접근자 위임.
       비율은 구성비(%)라 규모(청취자 수)와 별개임을 명시(구성비 공리)."""
    import raas_series as SR
    days = _demo_rank_period(question)                    # 30/7 = 기간평균 순위, None = 최신 스냅샷
    rows = [(c, v) for c, v in SR.ranking(spec["source"], spec["field"], dims={"DEPTH": "ALL"}, days=days)
            if c not in _RANK_EXCLUDE and not (len(c) == 3 and c.endswith("00"))]
    if not rows:
        return {"ok": False, "reason": "분포 데이터 없음"}
    if spec["asc"]:
        rows = sorted(rows, key=lambda x: x[1])          # 낮은순(기본은 접근자가 내림차순)
    top = [{"code": c, "name": _resolve_name(c), "비율%": round(v, 1)} for c, v in rows[:15]]
    win_kr = {30: "지난 30일 평균", 7: "지난 7일 평균"}.get(days, "일간(최신)")
    payload = {"metric": spec["label"], "depth": "청취시작(ALL)", "window": win_kr,
               "order": "asc(낮은순)" if spec["asc"] else "desc(높은순)",
               "count": len(rows), "ranking": top}
    head = (f"순위 분석: 전 프로그램 {spec['label']} "
            f"{'하위' if spec['asc'] else '상위'} ({win_kr}, 대상 {len(rows)}개)")
    context = (head + f"\n\n### program_ranking(분포) — 전 프로그램 {spec['label']} 순위\n"
               + json.dumps(payload, ensure_ascii=False, default=str)
               + "\n\n## 해석 주의\n- 이 순위는 '구성비(%)' 기준 — 규모(청취자 수)와 별개다."
                 " 비율이 높아도 절대 인원은 적을 수 있으니, 규모가 궁금하면 DAU 순위와 함께 볼 것.")
    targets = [("program", it["code"]) for it in top[:8]] + [("global", None)]
    otext, overlay_ids = _fetch_overlay(targets, overlay_ctx)
    if otext:
        context += "\n\n" + otext
    return {"ok": True, "context": context,
            "providers_used": ["program_ranking_demo"], "entities_brief": head,
            "provenance": {"providers": ["program_ranking_demo"], "scope": "ranking",
                           "metric": spec["field"], "overlay_items": overlay_ids}}


def _assemble_ranking(question, spec, overlay_ctx=None) -> dict:
    if spec.get("demo"):
        return _assemble_ranking_demo(question, spec, overlay_ctx)
    try:
        rows = S._kpi_rows() or []
    except Exception:
        rows = []
    if not rows:
        return {"ok": False, "reason": "데이터 없음"}
    latest = max((r.get("DATE") for r in rows if r.get("DATE")), default=None)
    fld = spec["field"]
    by_change = spec.get("by_change")
    rank_fld = f"{fld}_chg" if by_change else fld   # 변화량 순위는 전주대비%(_chg)로 정렬
    items, seen = [], set()
    for r in rows:
        if r.get("DATE") != latest:
            continue
        code = r.get("PGM_CODE")
        if not code or code in _RANK_EXCLUDE or code.endswith("00") or code in seen:
            continue
        v = _to_float(r.get(rank_fld))
        if v is None:
            continue
        seen.add(code)
        it = {"code": code, "name": _resolve_name(code), fld: r.get(fld)}
        if by_change:
            it[rank_fld] = r.get(rank_fld)          # 순위 기준값(변화율)도 함께 노출
        it["_v"] = v
        items.append(it)
    if not items:
        return {"ok": False, "reason": "지표 데이터 없음"}
    items.sort(key=lambda x: x["_v"], reverse=not spec["asc"])
    top = [{k: v for k, v in it.items() if k != "_v"} for it in items[:15]]
    _metric_kr = f"{spec['label']} 변화량(전주대비%)" if by_change else spec["label"]
    payload = {"date": latest, "metric": _metric_kr, "field": rank_fld,
               "order": "asc(낮은순)" if spec["asc"] else "desc(높은순)",
               "count": len(items), "ranking": top}
    head = (f"순위 분석: 전 프로그램 {_metric_kr} "
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


# ─── Realtime scope — 실시간 동시사용자(summary_gorealra_1m, 1분 집계) ────────
#    "지금/실시간/동시청취" 질의. 데이터 획득은 raas_datasource(60초/일일 캐시),
#    여기서는 스냅샷·어제/지난주 동시각 비교·오늘 추이를 결정적으로 계산해 근거로 조립.
_RT_SIGNAL = ("동시사용자", "동시 사용자", "동시접속", "동시 접속", "동시청취", "동시 청취",
              "동시자", "동시 청취자", "동시청취자", "실시간")
_RT_NOW_HINT = ("청취", "듣는", "듣고", "접속자", "사용자", "몇 명", "몇명")

def _detect_realtime(question: str) -> bool:
    t = question or ""
    if any(s in t for s in _RT_SIGNAL):
        return True
    return ("지금" in t or "현재" in t) and any(s in t for s in _RT_NOW_HINT)

# 실시간 필드 ↔ 한글명 (tempsummary — 채널 필드가 곧 RAAS 코드라 오귀속 여지 없음)
_RT_CHANNELS = [("파워FM", "F00"), ("러브FM", "L00"), ("고릴라M", "G00"), ("픽채널", "P00")]
_RT_DEVICES = [("스마트폰(태블릿 포함)", "DV_SP"), ("PC클라이언트", "DV_PC"),
               ("웹브라우저(SBS홈페이지)", "DV_PW"), ("모바일웹", "DV_MWEB"),
               ("워치", "DV_WATCH"), ("자동차", "DV_CAR"), ("AI스피커(7사 합산)", "DV_AI")]
_RT_AGES = [("0-19", "AGE_T0_19"), ("20-24", "AGE_T20_24"), ("25-29", "AGE_T25_29"),
            ("30-34", "AGE_T30_34"), ("35-39", "AGE_T35_39"), ("40-44", "AGE_T40_44"),
            ("45-49", "AGE_T45_49"), ("50-54", "AGE_T50_54"), ("55-59", "AGE_T55_59"),
            ("60+", "AGE_T60")]

def _rt_hhmm(row) -> str:
    """행 _time(ISO) → 'HH:MM'."""
    t = str(row.get("_time") or "")
    return t[11:16] if len(t) >= 16 else ""

def _rt_int(v):
    try:
        return int(float(v)) if v not in (None, "", "None") else None
    except Exception:
        return None

def _rt_flt(v):
    try:
        return round(float(v), 1) if v not in (None, "", "None") else None
    except Exception:
        return None

def _rt_pct(cur, base):
    if cur is None or base in (None, 0):
        return None
    return round((cur - base) / base * 100, 1)

def _rt_stime_fmt(raw) -> str:
    """STIME 'HHMM' → 'HH:MM'. 형식 밖이면 원문."""
    s = str(raw or "").strip()
    return f"{s[:2]}:{s[2:]}" if len(s) == 4 and s.isdigit() else s

def _rt_ontology_block(ch_code: str) -> str:
    """채널 편성 성격 + 적용 도메인 공리를 온톨로지에서 읽어 '온톨로지 근거' 블록으로.
       (지식을 코드 문자열이 아니라 온톨로지 TTL에 두고 read-time 주입 — 채워질수록 답변 향상)"""
    if not ch_code or ch_code == "T00":
        return ""
    try:
        from raas_onto import get_adapter
        a = get_adapter()
    except Exception:
        return ""
    lines = []
    nature = a.get_channel_nature(ch_code)
    if nature:
        _kr = {"programmed": "편성형(시간대별 프로그램 편성)",
               "non-programmed": "비편성형(프로그램 단위 편성 없음)"}.get(nature, nature)
        lines.append(f"- {_CH_NAME_BY_CODE.get(ch_code, ch_code)} 채널 성격: {_kr}")
    for ax in a.get_domain_axioms(ch_code):
        lines.append(f"- [{ax['label']}] {ax['text']}")
    if not lines:
        return ""
    return "## 온톨로지 근거\n" + "\n".join(lines)

def _rt_current_program(ch_code: str):
    """현재 시각 기준 그 채널에서 '지금 방송 중'인 프로그램을 STIME으로 역산.
       오늘 요일에 방송하는 프로그램(KPI dau 존재) 중 STIME이 now 이하로 가장 큰 것.
       반환 {name, code, stime, etime} 또는 None."""
    pref = {"F00": ("F",), "L00": ("L", "M"), "G00": ("G",), "P00": ("P",)}.get(ch_code)
    if not pref:
        return None
    now = _dt.datetime.now()
    now_hm, today_wd = now.strftime("%H%M"), now.weekday()
    try:
        rows = S._kpi_rows() or []
    except Exception:
        rows = []
    by_code = {}
    for r in rows:
        c = r.get("PGM_CODE") or ""
        if c.endswith("00") or not c.startswith(pref):
            continue
        rec = by_code.setdefault(c, {"stime": "", "name": r.get("PGM_NAME") or _resolve_name(c), "wds": set()})
        st = (r.get("STIME") or "").strip()
        if len(st) == 4:
            rec["stime"] = st
        dau = str(r.get("dau") or "").strip()
        if dau and dau not in ("0", "0.0"):
            try:
                rec["wds"].add(_dt.datetime.strptime((r.get("DATE") or "").replace("/", "-"), "%Y-%m-%d").weekday())
            except Exception:
                pass
    # 오늘 방송(요일) + STIME 유효한 프로그램만, STIME 순
    airing = sorted((v["stime"], c, v["name"]) for c, v in by_code.items()
                    if len(v["stime"]) == 4 and (not v["wds"] or today_wd in v["wds"]))
    if not airing:
        return None
    cur = None
    for i, (st, c, nm) in enumerate(airing):
        if st <= now_hm:
            et = airing[i + 1][0] if i + 1 < len(airing) else "2400"
            cur = (st, c, nm, et)
    if not cur:
        return None
    st, c, nm, et = cur
    return {"name": nm, "code": c, "stime": _rt_stime_fmt(st),
            "etime": "24:00" if et == "2400" else _rt_stime_fmt(et)}

_RT_CH_PREF = {"F": "F00", "L": "L00", "M": "L00", "G": "G00", "P": "P00"}
_RT_CH_PREFIXES = {"F00": ("F",), "L00": ("L", "M"), "G00": ("G",), "P00": ("P",)}


def _rt_channel_from_q(question: str):
    """질문에서 채널 감지 → (필드, 표시명). 없으면 전체(T00)."""
    for nm, code in _RT_CHANNELS:
        if nm in (question or ""):
            return code, nm
    return "T00", "전체"


def _program_window(code: str):
    """프로그램 code → (channel_code, stime'HH:MM', etime'HH:MM', name).
       편성창=채널 내 STIME 정렬로 다음 프로그램 시작 직전까지. 편성형 채널 동시방송 공리의 근거."""
    ch = _RT_CH_PREF.get((code or "")[:1])
    if not ch:
        return None
    prefs = _RT_CH_PREFIXES[ch]
    try:
        rows = S._kpi_rows() or []
    except Exception:
        rows = []
    slots = {}
    for r in rows:
        c = r.get("PGM_CODE") or ""
        if c.endswith("00") or not c.startswith(prefs):
            continue
        st = (r.get("STIME") or "").strip()
        if len(st) == 4:
            slots[c] = st
    if code not in slots:
        return None
    order = sorted(slots.items(), key=lambda x: x[1])
    stime = slots[code]
    etime = "2400"
    for i, (c, st) in enumerate(order):
        if c == code:
            etime = order[i + 1][1] if i + 1 < len(order) else "2400"
            break
    _hm = lambda s: f"{s[:2]}:{s[2:]}"
    return ch, _hm(stime), ("24:00" if etime == "2400" else _hm(etime)), _resolve_name(code)


# ── 실시간 공용 해석 계층 — 대상·해상도를 한 곳에서 풀어 모든 경로(차트·추출·오늘추이)가 공유 ──
def _rt_wants_1min(question: str) -> bool:
    """사용자가 1분 해상도를 명시했는가(차트 기본 10분 오버라이드)."""
    return any(k in (question or "") for k in
               ("1분 단위", "1분단위", "1분 간격", "1분간격", "분단위로", "분 단위로", "1분해상도", "1분 해상도"))


def _rt_resolve_target(question: str) -> dict:
    """질의 → 실시간 조회 대상을 온톨로지로 해석(프로그램→채널+편성창). 모든 경로 공용.
       반환 {kind: program|channel|all, ch_field, disp, win:(start,end)'HH:MM'|None}."""
    try:
        from raas_storyline_router import extract_program
        p = extract_program(question)
    except Exception:
        p = None
    if p and p.get("code"):
        pw = _program_window(p["code"])
        if pw:
            return {"kind": "program", "ch_field": pw[0], "disp": pw[3], "win": (pw[1], pw[2])}
    ch_field, disp = _rt_channel_from_q(question)
    return {"kind": ("channel" if ch_field != "T00" else "all"),
            "ch_field": ch_field, "disp": disp, "win": None}


def _rt_temporal(question: str):
    """실시간 과거 질의의 시간 의도 분류 → ('single', 'YYYY-MM-DD') / ('unsupported', None) / None(오늘).
       월평균·기간범위(~)·월단위는 분단위 미지원으로 분리(오늘 경로로 새거나 오답 방지)."""
    q = question or ""
    if any(k in q for k in ("월평균", "월 평균", "~", "부터")) or \
            (re.search(r"\d{1,2}\s*월", q) and not re.search(r"\d{1,2}\s*월\s*\d{1,2}\s*일", q)):
        return ("unsupported", None)
    cand = _parse_abs_date(q)
    if not cand:
        return None
    today_str = _dt.date.today().strftime("%Y-%m-%d")
    y = cand[0]
    if not y:
        y = _dt.date.today().year
        if "%04d-%02d-%02d" % (y, cand[1], cand[2]) > today_str:
            y -= 1
    date = "%04d-%02d-%02d" % (y, cand[1], cand[2])
    return None if date >= today_str else ("single", date)


def _rt_history_branch(question: str, overlay_ctx=None):
    """과거 분단위 동시자 질의 → history 경로(프로그램/채널/전체 인식). 아니면 None.
       보관 범위 밖이면 '언제부터'를, 월/기간 집계는 '미지원'을 데이터 기반으로 안내."""
    kind = _rt_temporal(question)
    if kind is None:
        return None
    import raas_datasource as DSRC
    earliest = DSRC.get_rt_earliest()
    _prov = {"providers_used": ["realtime_history"],
             "provenance": {"providers": ["realtime_history"], "scope": "realtime"}, "ok": True}
    if kind[0] == "unsupported":                      # 월평균·기간범위·월단위 — 분단위 미지원
        _prov["entities_brief"] = "과거 분단위(기간/월 집계 미지원)"
        _prov["context"] = (
            "분석 대상: 과거 분단위 동시사용자\n### realtime_history — 기간/월 집계 미지원\n"
            f"분단위(1분 집계) 동시사용자는 **특정일 단위**로만 조회합니다(예: 2026-04-01). "
            f"월평균·기간범위(여러 날) 분단위 집계는 아직 미지원입니다"
            + (f" (분단위 보관 {earliest}부터)." if earliest else ".")
            + " 안내: 특정일 하나로 다시 물어주시도록 안내하고, 기간 단위 '규모'는 일간 지표(DAU 등, "
              "아카이브 최대 10년)로 답할 수 있음을 알려주세요.")
        return _prov
    date = kind[1]
    tgt = _rt_resolve_target(question)                 # 공용 해석(프로그램→채널+편성창)
    ch_field, disp, is_program = tgt["ch_field"], tgt["disp"], tgt["kind"] == "program"
    w_start, w_end = tgt["win"] if tgt["win"] else (None, None)
    _prov["entities_brief"] = f"과거 분단위 동시자({disp}, {date})"
    if earliest and date < earliest:                  # 보관 범위 밖 — 언제부터
        _prov["context"] = (
            f"분석 대상: 과거 분단위 동시사용자 — 요청일 {date} ({disp})\n"
            f"### realtime_history — 보관 범위 밖\n"
            f"분단위(1분 집계) 동시사용자 데이터는 **{earliest}부터** 보유합니다. "
            f"요청하신 {date}는 그 이전이라 데이터가 없습니다. {earliest} 이후 날짜면 조회 가능하며, "
            f"그 이전의 '일간' 규모는 아카이브로 조회 가능합니다(분단위는 아님).")
        return _prov
    import raas_rt_series as RT
    window = (w_start, w_end) if (is_program and w_start) else None
    vals = RT.rt_series("concurrent", ch_field, date, 1, window=window)   # 통합 접근자 위임
    if not vals:
        _prov["context"] = (
            f"분석 대상: 과거 분단위 동시사용자 — {date} ({disp})\n"
            f"### realtime_history — {date} 데이터 없음\n"
            f"해당일{'·해당 편성창' if is_program else ''} 분단위 집계가 없습니다"
            + (f" (분단위 보관 시작 {earliest})." if earliest else ".") + " 이 사실을 안내하세요.")
        return _prov
    peak = max(vals, key=lambda x: x[1])
    low = min(vals, key=lambda x: x[1])
    avg = round(sum(v for _, v in vals) / len(vals))
    step1 = _rt_wants_1min(question)                   # 사용자가 '1분 단위'면 오버라이드
    pts = vals if step1 else RT.rt_series("concurrent", ch_field, date, 10, window=window)
    res_kr = "1분" if step1 else "10분"
    csv = "time,concurrent\n" + "\n".join(f"{t},{v}" for t, v in pts)
    summary = {"대상": disp, "date": date, "해상도": f"원천 1분(추이 {res_kr} 간격)",
               "피크": {"시각": peak[0], "값": peak[1]}, "최저": {"시각": low[0], "값": low[1]},
               "평균": avg, "표본(분)": len(vals), "분단위 보관시작": earliest}
    if is_program:
        summary["편성창"] = f"{ch_field} {w_start}~{w_end}"
    head = (f"분석 대상: 과거 분단위 동시사용자(1분 집계) — {disp}, {date}"
            + (f" (편성창 {w_start}~{w_end})" if is_program else ""))
    guide = ("안내: 아래는 그 날짜의 분단위 동시자(보관 데이터). 추이·피크시간대를 짚어 답하고, 필요시 10분 "
             "CSV로 차트. 동시사용자=1분 버킷 내 활성 세션 고유 사용자 수(dc UUID). "
             "편성 시각·채널을 임의로 지어내지 말 것 — 근거에 있는 값만 사용.")
    if is_program:
        guide += (f" 참고: {disp}는 {ch_field} 채널 {w_start}~{w_end} 편성분이며, 편성형 채널은 동시간 "
                  "1개 프로그램만 방송하므로(도메인 공리) 이 편성창의 채널 동시청취 = 프로그램 동시청취.")
    context = (head + "\n" + guide + f"\n\n### realtime_history — {disp} {date} 1분 동시자\n"
               + json.dumps(summary, ensure_ascii=False) + f"\n\n[{res_kr} 간격 추이 CSV]\n{csv}")
    onto = _rt_ontology_block(ch_field if ch_field != "T00" else "")
    if onto:
        context += "\n\n" + onto
    _prov["context"] = context
    return _prov


def _assemble_realtime(question: str, overlay_ctx=None) -> dict:
    hist = _rt_history_branch(question, overlay_ctx)   # 과거 특정일 분단위 → history 경로
    if hist is not None:
        return hist
    import raas_datasource as DSRC
    today = DSRC.get_rt_concurrent()          # 확장 실시간(디바이스×채널·인증 비율 포함)
    if not today:
        context = ("분석 대상: 실시간 동시사용자(1분 집계)\n"
                   "### realtime_now — 조회 실패\n"
                   "실시간 데이터(summary_gorealra_1m)를 가져오지 못했습니다"
                   "(스플렁크 미접속 또는 집계 없음). 이 사실을 사용자에게 안내하세요.")
        return {"ok": True, "context": context, "providers_used": ["realtime_now"],
                "entities_brief": "실시간 동시사용자(조회 실패)",
                "provenance": {"providers": ["realtime_now"], "scope": "realtime"}}
    latest = today[-1]
    now_hhmm = _rt_hhmm(latest)
    total_now = _rt_int(latest.get("T00"))

    # 스냅샷 — 채널·디바이스×채널·보라×채널·인증 성별/연령 비율 (키를 한글명으로 결정적 변환)
    _RT_CHS = (("전체", ""), ("파워FM", "_F00"), ("러브FM", "_L00"), ("고릴라M", "_G00"), ("픽채널", "_P00"))
    device = {nm: {chn: _rt_int(latest.get(f + suf)) for chn, suf in _RT_CHS}
              for nm, f in _RT_DEVICES}

    def _prof(ch):                            # 채널별 인증 프로필(비율 %) — 분모=인증자합계
        return {"인증자수": _rt_int(latest.get(f"{ch}_REALINFO")),
                "성별비율%": {"여": _rt_flt(latest.get(f"R_{ch}_SEX_F")),
                            "남": _rt_flt(latest.get(f"R_{ch}_SEX_M"))},
                "연령비율%": {nm: _rt_flt(latest.get(f"R_{ch}_AGE_{f}")) for nm, f in _RT_AGES}}
    snap = {"기준시각": now_hhmm, "전체": total_now,
            "채널별": {nm: _rt_int(latest.get(f)) for nm, f in _RT_CHANNELS},
            "디바이스별(전체·채널)": device,
            "보는라디오": {"전체": _rt_int(latest.get("BA")),
                       "파워FM": _rt_int(latest.get("BA_F00")),
                       "러브FM": _rt_int(latest.get("BA_L00"))},
            "인증프로필(성별·연령 비율)": {"파워FM": _prof("F00"), "러브FM": _prof("L00")}}

    # 분당 문자·공감로그(infobank) + 분당 유입(tempsummary3) — 별도 Feed 최신값
    _msg = DSRC.get_rt_msg() or []
    _inf = DSRC.get_rt_inflow() or []
    _ml, _il = (_msg[-1] if _msg else {}), (_inf[-1] if _inf else {})
    rt_extra = {
        "분당 문자·공감로그": {
            "파워FM": {"SMS": _rt_int(_ml.get("F00_SMS")), "공감로그": _rt_int(_ml.get("F00_GG"))},
            "러브FM": {"SMS": _rt_int(_ml.get("L00_SMS")), "공감로그": _rt_int(_ml.get("L00_GG"))}},
        "분당 유입": {"파워FM": _rt_int(_il.get("F00_START")), "러브FM": _rt_int(_il.get("L00_START")),
                   "고릴라M": _rt_int(_il.get("G00_START")), "픽채널": _rt_int(_il.get("P00_START"))}}

    # 어제/지난주 같은 시각 비교 + 피크 (계산은 코드가, LLM은 읽기만)
    def _at(rows, hhmm):
        for r in rows:
            if _rt_hhmm(r) == hhmm:
                return _rt_int(r.get("T00"))
        return None
    def _peak(rows):
        best_v, best_t = None, None
        for r in rows:
            v = _rt_int(r.get("T00"))
            if v is not None and (best_v is None or v > best_v):
                best_v, best_t = v, _rt_hhmm(r)
        return {"값": best_v, "시각": best_t}
    yday, lastwk = DSRC.get_realtime_yesterday(), DSRC.get_realtime_lastweek()
    y_same, w_same = _at(yday, now_hhmm), _at(lastwk, now_hhmm)
    compare = {"어제 같은 시각": {"값": y_same, "증감%": _rt_pct(total_now, y_same)},
               "지난주 동요일 같은 시각": {"값": w_same, "증감%": _rt_pct(total_now, w_same)},
               "오늘 피크": _peak(today), "어제 피크": _peak(yday),
               "지난주 동요일 피크": _peak(lastwk)}

    # 편성표 조인 — 프로그램 언급 시('지금 컬투쇼 몇 명?') 소속 채널 + 방송시각 컨텍스트 첨부.
    #   실시간 집계는 채널 단위뿐이라, 방송 중 여부 판단 재료(시작시각·편성 요일)를 결정적으로 제공.
    prog = extract_program(question)
    prog_block = None
    ch_code, ch_name = _detect_channel(question)
    if prog:                                   # 프로그램이 있으면 그 소속 채널을 관심 채널로
        # channel이 한글명이므로 코드 변환(미등재는 접두사 유추: M*=러브FM 주말)
        ch_code = (S._CHANNEL_CODE.get(prog["channel"])
                   or {"F": "F00", "L": "L00", "M": "L00",
                       "G": "G00", "P": "P00"}.get(prog["code"][:1], "T00"))
        ch_name = prog["channel"]
        _prow = S._load_program_latest_row(prog["code"]) or {}
        prog_block = {
            "프로그램": prog["name"], "코드": prog["code"],
            "소속 채널": prog["channel"],      # PROGRAM_DIRECTORY.channel = 한글명
            "방송 시작시각": _rt_stime_fmt(_prow.get("STIME")) or "미상",
            "편성": "주말" if prog["code"].startswith("M") else "평일 중심",
        }
    elif ch_code and ch_code != "T00" and any(k in question for k in ("프로그램", "방송", "뭐", "무슨", "어떤")):
        # 프로그램명 없이 '지금 방송 중인 프로그램' 류 → 현재 시각 + STIME으로 역산
        _cur = _rt_current_program(ch_code)
        if _cur:
            prog_block = {
                "현재 방송 중": _cur["name"], "코드": _cur["code"],
                "소속 채널": ch_name,
                "방송 시간": f"{_cur['stime']}~{_cur['etime']}",
                "판정": f"현재 시각 {now_hhmm}이 편성 창에 속함(STIME 기준 역산)",
            }
    # 추이 — 프로그램/현재방송 스코프면 1분 해상도(편성창~현재), 아니면 10분 전체(24h 가독).
    #   성별·연령을 물으면 그 채널 인증자 비율도 같은 해상도로 시계열 제공(분당 데이터 존재).
    one_min = bool(prog_block) or _rt_wants_1min(question)   # 프로그램 스코프거나 사용자가 1분 명시
    win_start = None
    if prog_block:
        _ws = (prog_block.get("방송 시작시각") or prog_block.get("방송 시간", "").split("~")[0] or "").strip()
        if len(_ws) == 5 and _ws[2] == ":":
            win_start = _ws
    def _keep(hh):
        if not hh:
            return False
        if not one_min:
            return hh[4:5] == "0"               # 10분
        return (win_start is None) or (hh >= win_start)   # 1분(편성창 이후)
    import raas_rt_series as RT
    _res = 1 if one_min else 10
    _win = (win_start, "24:00") if (one_min and win_start) else None
    _ctb = RT.rt_table("concurrent", ["T00", "F00", "L00", "G00", "P00"], "today", _res, window=_win)
    ser = ["time,전체,파워FM,러브FM,고릴라M,픽채널"]
    for _row in _ctb["rows"]:
        ser.append(",".join([_row[0]] + [str(v or "") for v in _row[1:]]))
    # 성별/연령 추이(요청 시) — 파워FM(F00)/러브FM(L00)만 인증 비율 존재
    demo_ser = demo_ch = None
    want_sex = any(k in question for k in ("성별", "남녀", "여성", "남성", "여자", "남자"))
    want_age = any(k in question for k in ("연령", "나이", "세대", "대별", "20대", "30대", "40대", "50대", "60대"))
    if (want_sex or want_age) and ch_code in ("F00", "L00"):
        demo_ch = ch_code
        cols = []
        if want_sex:
            cols += [("여%", f"R_{demo_ch}_SEX_F"), ("남%", f"R_{demo_ch}_SEX_M")]
        if want_age:
            cols += [(f"{nm}%", f"R_{demo_ch}_AGE_{f}") for nm, f in _RT_AGES]
        lines = ["time," + ",".join(c[0] for c in cols)]
        for r in today:
            hh = _rt_hhmm(r)
            if _keep(hh):
                lines.append(",".join([hh] + [str(_rt_flt(r.get(c[1])) or "") for c in cols]))
        demo_ser = "\n".join(lines)

    head = (f"분석 대상: 실시간 동시사용자(1분 집계) · 기준시각 오늘 {now_hhmm}"
            + (f" · 관심 채널: {ch_name}" if ch_code and ch_code != "T00" else ""))
    defs = ("[용어 정의]\n"
            "- 동시사용자: 1분 버킷 내 활성 세션의 고유 사용자 수(dc UUID, 세션 겹침 기반)\n"
            "- 디바이스별은 전체·채널(파워FM/러브FM/고릴라M/픽채널)로 분해 제공. DV_AI는 AI스피커 7사 합산.\n"
            "- 인증프로필의 성별·연령은 **비율(%)**이며 분모는 그 채널의 '인증자수'(본인인증자, 전체보다 작음). "
            "전체 대비 비율로 말하지 말 것. 성별·연령은 파워FM/러브FM만 제공(고릴라M/픽채널 없음).\n"
            "- 보는라디오는 시청 모드(카테고리) — 디바이스 분해와 별개 축이라 합산 금지\n"
            "- 분당 문자·공감로그/유입은 그 1분의 값(누적 아님)\n"
            "- 비교값·증감%·피크는 코드가 계산한 값 — 그대로 사용하고 재계산하지 말 것")
    context = (head + "\n\n### realtime_now — 현재 동시사용자 스냅샷\n"
               + json.dumps(snap, ensure_ascii=False)
               + "\n\n### realtime_extra — 분당 문자·공감로그·유입\n"
               + json.dumps(rt_extra, ensure_ascii=False)
               + "\n\n### realtime_compare — 어제/지난주 동시각·피크 비교\n"
               + json.dumps(compare, ensure_ascii=False)
               + f"\n\n### realtime_series — 오늘 동시사용자 추이({'1분·편성창' if one_min else '10분 간격'})\n"
               + "\n".join(ser))
    providers = ["realtime_now", "realtime_extra", "realtime_compare", "realtime_series"]
    if demo_ser:
        context += (f"\n\n### realtime_series_demographic — {ch_name} 인증자 성별/연령 비율 추이"
                    f"({'1분·편성창' if one_min else '10분 간격'}, 분모=인증자수)\n" + demo_ser)
        providers.append("realtime_series_demographic")
    if prog_block:
        context += ("\n\n### realtime_program — 질의 프로그램 편성 컨텍스트\n"
                    + json.dumps(prog_block, ensure_ascii=False))
        providers.append("realtime_program")
    context += "\n\n" + defs
    # 온톨로지 도메인 근거(채널 편성 성격 + 동시방송 공리) — 지식을 코드가 아닌 온톨로지에서.
    onto = _rt_ontology_block(ch_code)
    if onto:
        context += "\n\n" + onto
        providers.append("channel_ontology")
    return {"ok": True, "context": context,
            "providers_used": providers,
            "entities_brief": head,
            "provenance": {"providers": providers,
                           "scope": "realtime", "channel": ch_code,
                           "program": prog["code"] if prog else None}}


# ─── Meta scope — 지표 카탈로그(시스템이 제공하는 지표 목록·정의) ─────────────
#    "어떤 지표/데이터 있나·볼 수 있나" 같은 시스템 능력 질의. 특정 데이터 값이 아니라
#    온톨로지 지표 체계를 근거로 답한다(엔티티 없음 → 폴백으로 새던 것을 흡수).
_META_OBJ = ("지표", "메트릭", "kpi", "지수", "데이터")
_META_ASK = ("어떤", "무슨", "무엇", "뭐", "뭘", "종류", "목록", "리스트", "볼 수 있", "제공")
_META_VALUE_EXCLUDE = ("높", "낮", "많", "적", "얼마", "몇", "순위", "랭킹", "추이", "그래프", "비교", "vs")

def _detect_meta(question: str) -> bool:
    """카탈로그(시스템 능력) 질의. 데이터 값 질의(높은/얼마/순위/추이)·특정날짜는 제외."""
    t = (question or "").lower()
    if any(x in t for x in _META_VALUE_EXCLUDE):
        return False
    if _parse_abs_date(question):        # 특정 날짜가 있으면 데이터 조회지 카탈로그가 아님
        return False
    return any(o in t for o in _META_OBJ) and any(a in t for a in _META_ASK)

def _meta_data_sources() -> str:
    """레지스트리 기반 '보유 데이터 소스' 요약 — 지표가 아닌 데이터(편성이력·아카이브·오늘편성)도
       메타 답변에 포함. 소스 추가 시 자동 반영(하드코딩 아님)."""
    try:
        import raas_metrics_registry as REG
        grain_ko = {"daily": "일간", "minute": "실시간(분)", "editorial": "편성/서술"}
        lines = ["[보유 데이터 소스 — 레지스트리 자동 집계]"]
        for s in REG.SOURCES:
            win = ("·".join(s.available_periods) if s.available_periods else "-")
            lines.append(f"- {s.label} ({grain_ko.get(s.grain, s.grain)}, 창 {win})")
        return "\n".join(lines)
    except Exception:
        return ""


def _assemble_meta(question, overlay_ctx=None) -> dict:
    try:
        from raas_onto import get_adapter
        catalog = get_adapter().get_metric_definitions_block()
    except Exception:
        catalog = ""
    if not catalog:
        return {"ok": False, "reason": "카탈로그 없음"}
    sources = _meta_data_sources()
    context = ("분석 대상: 지표 카탈로그(메타) — 시스템이 제공하는 지표 목록·정의\n"
               "안내: 어떤 지표/데이터를 볼 수 있는지 묻는 질의. 아래 카탈로그·소스 목록을 근거로 "
               "범주별로 소개하되 질문 범위에 맞게 간결히. 카탈로그에 없는 것은 없다고 답할 것.\n\n"
               "### metric_catalog — 제공 지표 체계·정의\n" + catalog
               + (("\n\n### data_sources — 보유 데이터 소스\n" + sources) if sources else ""))
    return {"ok": True, "context": context, "providers_used": ["metric_catalog"],
            "entities_brief": "지표 카탈로그(메타)",
            "provenance": {"providers": ["metric_catalog"], "scope": "meta"}}


# ─── 메인 — 맥락 조립 ───────────────────────────────────────────────────────
def assemble(question: str, overlay_ctx=None) -> dict:
    """질문 → 근거 context 조립. overlay_ctx={user_id, mode:'normal'|'requery'}.
       반환: {ok, context, providers_used, entities_brief, provenance}"""
    if _detect_realtime(question):     # 실시간은 비교·순위보다 먼저 (동시사용자 자체가 주제)
        return _assemble_realtime(question, overlay_ctx)
    cmp_ents = _detect_compare(question)
    if cmp_ents:
        return _assemble_compare(question, cmp_ents, overlay_ctx)
    if _detect_meta(question):
        _m = _assemble_meta(question, overlay_ctx)
        if _m.get("ok"):
            return _m
    # 참여(문자·공감로그) 순위는 KPI 랭킹(program_ranking, DAU 기준)이 아니라 참여 provider가 담당
    #   ('공감로그 참여 순위'가 DAU로 정렬되던 문제 방지). 전체 스냅샷을 주고 LLM이 정렬.
    rank_spec = None if _wants_engagement(question) else _detect_ranking(question)
    if rank_spec:
        _r = _assemble_ranking(question, rank_spec, overlay_ctx)
        if _r.get("ok"):
            return _r
    if _detect_guest_search(question):        # '임지연 어느 프로그램 출연?' 역검색(값→프로그램)
        _g = _assemble_guest_search(question)
        if _g:
            return _g
    ent = resolve_entities(question, (overlay_ctx or {}).get("default_code"))
    if not ent.get("code"):
        # 안전망(general) scope — 엔티티/지표/날짜/속성/메타/비교/순위 어디에도 안 걸린 잔여.
        #   전사(T00) 광역(현황·건강도·이상·인사이트·개념)으로 흡수해 표준 provider 경로로 답한다.
        #   데이터+온톨로지를 context에 넣고 LLM이 자유 답변(고정 템플릿 없음) → 폴백 은퇴 준비.
        ent["scope_kind"] = "channel"
        ent.update(code="T00", name="전체", channel="T00")
        ent["row"] = S._load_program_latest_row("T00")
        ent["history"] = S._load_program_history("T00", ent.get("lookback") or 0)
        ent["date"] = ((ent["row"] or {}).get("DATE") or "").replace("/", "-")
        if "프로그램" in (question or ""):
            ent["all_programs"] = True     # '위험한 프로그램' 등 → 소속 프로그램 나열 첨부
        ent["_general"] = True
    names = select_providers(question, ent)
    # 편성 연혁 의도(채널) — 편성 이력을 주경로로, KPI 순위·DAU 아카이브는 경쟁이라 제거
    edit = ent.get("scope_kind") == "channel" and _wants_editorial(question)
    if (ent.get("as_of_date") or ent.get("_general")) and "point_snapshot" not in names:
        names = ["point_snapshot"] + names   # 특정 날짜/안전망은 현황 스냅샷 반드시 포함
    if any(k in question for k in _COVERAGE_SIGNAL) and "data_coverage" not in names:
        names = ["data_coverage"] + names    # '언제부터/기간/보유' 질의는 실제 보유범위 반드시 포함
    if _wants_history(question) and not edit and "long_history" not in names:
        names = ["long_history"] + names     # 과거 연도·장기 질의는 아카이브 반드시 포함(편성 연혁은 제외)
    if _wants_today_lineup(question) and "today_lineup" not in names:
        names = ["today_lineup"] + names      # '오늘 게스트/편성' 질의는 당일 broadplan 반드시 포함
    if _wants_engagement(question) and "engagement" not in names:
        names = ["engagement"] + names        # '문자·공감로그 참여' 질의는 참여 데이터 반드시 포함
    if _wants_program_demo(question) and ent.get("scope_kind") == "program" and "program_demographics" not in names:
        names = ["program_demographics"] + names   # 프로그램 성별·연령·디바이스 분포 질의
    if _wants_correlate(question) and ent.get("scope_kind") == "program" and "metric_correlate" not in names:
        names = ["metric_correlate"] + names   # 지표 간 동반움직임·상관 질의(교차지표)
    if edit:
        # 편성 연혁 질의는 편성 전용 맥락으로 확정 — KPI·시계열 provider가 섞이면 LLM이 편성
        #   서술 대신 '데이터 없음' KPI 프레임으로 새는 걸 방지(Haiku 선택 비결정성 제거).
        _EDIT_KEEP = {"channel_history", "ontology", "calendar", "period_events"}
        names = ["channel_history"] + [n for n in names
                                       if n in _EDIT_KEEP and n != "channel_history"]
    elif (ent.get("scope_kind") == "channel" and ("프로그램" in question or ent.get("all_programs"))
            and "channel_programs" not in names):
        names = ["channel_programs"] + names   # 채널 내 '프로그램'/'모든 프로그램' 질의는 소속 프로그램 행 반드시 포함
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
# ─── 게스트/속성 → 프로그램 역검색 (값으로 엔티티 찾기) ─────────────────────
_GS_STOP = {"게스트", "출연", "출연자", "프로그램", "어느", "어떤", "무슨", "어디", "누구", "누가",
            "나온", "나왔", "나오는", "했어", "한", "하는", "알려줘", "보여줘", "누구야",
            "최근", "일주일간", "이번주", "지난주", "어제", "오늘", "출연했어", "출연한"}

def _detect_guest_search(q: str) -> bool:
    """게스트/출연 값으로 프로그램을 거꾸로 찾는 질의 — 질문에 프로그램이 없을 때만 이 경로로 온다."""
    t = q or ""
    return (("게스트" in t or "출연" in t)
            and any(k in t for k in ("어느", "어떤", "무슨", "어디", "프로그램에", "프로그램은", "프로그램 ")))

def _assemble_guest_search(question: str, overlay_ctx=None):
    try:
        rows = S._kpi_rows() or []
    except Exception:
        rows = []
    if not rows:
        return None
    words = [w for w in re.split(r"[\s,?!.]+", question or "")
             if len(w) >= 2 and w not in _GS_STOP]
    if not words:
        return None
    dates = sorted({(r.get("DATE") or "") for r in rows if r.get("DATE")})
    recent = set(dates[-30:])
    hits, seen = [], set()
    for r in rows:
        if (r.get("DATE") or "") not in recent:
            continue
        g = r.get("guestname") or ""
        if not g:
            continue
        if any(w in g for w in words):
            key = (r.get("PGM_CODE"), r.get("DATE"))
            if key in seen:
                continue
            seen.add(key)
            hits.append({"date": (r.get("DATE") or "").replace("/", "-"),
                         "요일": _dow_ko(r.get("DATE")),
                         "program": r.get("PGM_NAME") or _resolve_name(r.get("PGM_CODE")),
                         "code": r.get("PGM_CODE"), "guest": g, "live": r.get("live_yn")})
    if not hits:
        return None
    hits.sort(key=lambda h: h["date"], reverse=True)
    body = json.dumps({"검색어": words, "matches": hits[:40]}, ensure_ascii=False, default=str)
    context = (f"분석 대상: 게스트/출연 역검색 · 검색어: {', '.join(words)} · 최근 30일\n\n"
               f"### guest_search — 검색어가 게스트명에 포함된 프로그램·날짜(요일)\n{body}")
    return {"ok": True, "context": context, "providers_used": ["guest_search"],
            "entities_brief": f"분석 대상: 게스트 역검색({', '.join(words)}) · 최근 30일",
            "provenance": {"providers": ["guest_search"], "scope": "guest_search"}}


def _resolve_name(code) -> str:
    # 현재 프로그램명은 라이브(broadplan) 우선 — S._pgm_name에 위임(TTL 낡음 자동 교정)
    if not code:
        return ""
    try:
        return S._pgm_name(code)
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
    "- 날짜의 요일은 데이터에 표기된 것(예: 2026-06-08(월), '요일' 필드)을 그대로 쓰고 **직접 계산하지 말 것**. "
    "표기가 없으면 요일을 언급하지 말 것. 주별 데이터의 날짜는 '주 시작(월요일)' 앵커임.\n"
    "- 인과·원인 질문이면 흐름(신규/복귀/이탈)·편성·특일 등 제공된 근거로 구조적으로 설명.\n"
    + _CHART_HINT
    # 표현·형식(마크다운·간결성 등) 지침은 system_with_style()이 붙이는 [답변 스타일 정책]이 단일 소스.
)
