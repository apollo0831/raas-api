"""스토리라인 라우터 — 자유 질의 → 2_cause 슬롯 매핑.

목적: 1_anchor에서 TOP3가 보일 때 사용자가 "컬투쇼 왜 변했어?"처럼
TOP1이 아닌 프로그램이나 TOP3 밖 프로그램을 자유 질의로 지정해도,
"왜 변했어?" 칩을 누른 것과 동일한 답변 구조(2_cause)로 연결.

스코프 (이번 라운드):
  - high match만 처리 (cause_dau intent + 프로그램 식별 모두 성립)
  - low confidence 시 꼬리 칩 제안은 없음 → 기존 free-form 답변 유지
"""

from __future__ import annotations

import json
import re
from typing import Optional

from raas_query_engine import call_claude, HAIKU_MODEL


# ─────────────────────────────────────────────────────────────
# 프로그램 사전 (PGM_MAP에서 채널별 핵심 프로그램 추출)
# ─────────────────────────────────────────────────────────────

PROGRAM_DIRECTORY: dict[str, dict] = {
    "F01": {"name": "딘딘의 뮤직하이",            "channel": "파워FM"},
    "F02": {"name": "애프터 클럽",                  "channel": "파워FM"},
    "F03": {"name": "파워 스테이션",                "channel": "파워FM"},
    "F04": {"name": "이인권의 펀펀투데이",          "channel": "파워FM"},
    "F05": {"name": "김영철의 파워FM",              "channel": "파워FM"},
    "F06": {"name": "아름다운 이 아침 봉태규입니다", "channel": "파워FM"},
    "F07": {"name": "박하선의 씨네타운",            "channel": "파워FM"},
    "F08": {"name": "12시엔 주현영",                "channel": "파워FM"},
    "F09": {"name": "두시탈출 컬투쇼",              "channel": "파워FM"},
    "F10": {"name": "황제파워",                      "channel": "파워FM"},
    "F11": {"name": "박소현의 러브게임",            "channel": "파워FM"},
    "F12": {"name": "웬디의 영스트리트",            "channel": "파워FM"},
    "F13": {"name": "배성재의 텐",                   "channel": "파워FM"},
    "L05": {"name": "고현준의 뉴스 브리핑",          "channel": "러브FM"},
    "L06": {"name": "김태현의 정치쇼",              "channel": "러브FM"},
    "L07": {"name": "이숙영의 러브FM",              "channel": "러브FM"},
    "L08": {"name": "박연미의 목돈연구소",          "channel": "러브FM"},
    "L09": {"name": "유민상의 배고픈 라디오",        "channel": "러브FM"},
    "L10": {"name": "그대의 오후 정엽입니다",        "channel": "러브FM"},
    "L11": {"name": "어예진의 방과후 목돈연구소",    "channel": "러브FM"},
    "L12": {"name": "6시 저녁바람 김창완입니다",    "channel": "러브FM"},
    "L13": {"name": "주영진의 뉴스직격",            "channel": "러브FM"},
    "L14": {"name": "김윤상의 뮤직투나잇",          "channel": "러브FM"},
    "L15": {"name": "최백호의 낭만시대",            "channel": "러브FM"},
}

# 별칭 (긴 정식 이름 외에 자주 쓰는 약칭)
PROGRAM_ALIASES: dict[str, str] = {
    "컬투쇼": "F09",
    "두시탈출": "F09",
    "김영철": "F05",
    "파워FM 아침": "F05",
    "주현영": "F08",
    "박하선": "F07",
    "씨네타운": "F07",
    "봉태규": "F06",
    "딘딘": "F01",
    "배성재": "F13",
    "박소현": "F11",
    "러브게임": "F11",
    "웬디": "F12",
    "영스트리트": "F12",
    "주영진": "L13",
    "뉴스직격": "L13",
    "김창완": "L12",
    "최백호": "L15",
    "낭만시대": "L15",
}

_CAUSE_KEYWORDS = (
    "왜", "이유", "원인", "어째서",
    "떨어", "내려", "줄었", "감소", "하락",
    "올랐", "올라", "늘었", "증가", "상승",
    "변동", "변화", "차이",
)

_PROGRAM_NAME_RX = {
    code: re.compile(re.escape(info["name"])) for code, info in PROGRAM_DIRECTORY.items()
}
_PROGRAM_CODE_RX = re.compile(r"\b([FL]\d{2}|M0[5-9]|M1[01])\b")
_ALIAS_RX = {alias: re.compile(re.escape(alias)) for alias in PROGRAM_ALIASES}


# ─────────────────────────────────────────────────────────────
# 추출기 — 프로그램
# ─────────────────────────────────────────────────────────────

def extract_program(text: str) -> Optional[dict]:
    """질의에서 프로그램 식별. {code, name, channel} 반환 (없으면 None).

    우선순위: 코드 직접 매칭 → 정식 이름 (긴 것부터) → 별칭.
    """
    if not text:
        return None
    # 1) 코드 직접 매칭
    m = _PROGRAM_CODE_RX.search(text)
    if m:
        code = m.group(1)
        info = PROGRAM_DIRECTORY.get(code)
        if info:
            return {"code": code, "name": info["name"], "channel": info["channel"]}
    # 2) 정식 이름 매칭 (긴 이름 우선 — '러브FM'이 '러브게임'보다 짧으므로)
    for code in sorted(PROGRAM_DIRECTORY, key=lambda c: -len(PROGRAM_DIRECTORY[c]["name"])):
        if _PROGRAM_NAME_RX[code].search(text):
            info = PROGRAM_DIRECTORY[code]
            return {"code": code, "name": info["name"], "channel": info["channel"]}
    # 3) 별칭 매칭
    for alias, code in PROGRAM_ALIASES.items():
        if _ALIAS_RX[alias].search(text):
            info = PROGRAM_DIRECTORY.get(code)
            if info:
                return {"code": code, "name": info["name"], "channel": info["channel"]}
    return None


# ─────────────────────────────────────────────────────────────
# 의도 분류기 — Haiku
# ─────────────────────────────────────────────────────────────

_INTENT_SYSTEM = (
    "당신은 라디오 데이터 분석 시스템의 질의 분류기입니다. "
    "사용자의 질의가 특정 프로그램의 일간활성사용자(DAU) 변화 원인을 "
    "묻는 것인지 판단하세요.\n\n"
    "분류:\n"
    "- cause_dau: 특정 프로그램의 활성사용자 변동 원인·이유를 묻는 질문 "
    "(예: 'F05 왜 떨어졌어?', '컬투쇼 변화 원인', '주현영 활성사용자 감소 이유')\n"
    "- other: 단순 조회·게스트 정보·랭킹 등 그 외 모든 것\n\n"
    "JSON으로만 응답: {\"intent\":\"cause_dau\"|\"other\"}"
)


def _classify_intent(text: str) -> str:
    """'cause_dau' 또는 'other' 반환. 휴리스틱 사전 필터로 Haiku 호출 줄임."""
    if not text or not text.strip():
        return "other"
    if not any(kw in text for kw in _CAUSE_KEYWORDS):
        return "other"
    try:
        raw, _ = call_claude(_INTENT_SYSTEM, text, max_tokens=60, model=HAIKU_MODEL)
        m = re.search(r"\{[^{}]*\}", raw)
        if not m:
            return "other"
        data = json.loads(m.group(0))
        intent = data.get("intent", "other")
        return intent if intent in ("cause_dau", "other") else "other"
    except Exception as e:
        print(f"[router] _classify_intent 실패: {e}")
        return "other"


# ─────────────────────────────────────────────────────────────
# 라우터 본체 — high match만 반환
# ─────────────────────────────────────────────────────────────

def route(question: str, lenient: bool = False) -> Optional[dict]:
    """자유 질의 → 라우팅 결정.

    Args:
        question: 사용자 질의
        lenient: True면 의도 분류(cause_dau) 검사 생략 — 프로그램명만 매칭되면 라우팅.
                 1_anchor_more 등 '프로그램 선택' 흐름에서 사용.

    Returns:
      None — 라우팅 대상 아님 (기존 free-form 답변 유지)
      {slot_from, chip_intent, program: {code, name, channel}} — 2_cause 직진
    """
    if not question or not question.strip():
        return None
    program = extract_program(question)
    if not program:
        return None
    if not lenient and _classify_intent(question) != "cause_dau":
        return None
    return {
        "slot_from":   "1_anchor",
        "chip_intent": "explain_change",
        "program":     program,
    }


__all__ = ["PROGRAM_DIRECTORY", "extract_program", "route"]
