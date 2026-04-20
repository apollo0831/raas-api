"""
RAAS 자연어 질의 엔진 v2.0
- 기존: 단순 키워드 매칭 + 오늘 데이터만
- 신규: 질의 의도 분석 → 필요한 Splunk 쿼리 자동 생성 → Claude 분석
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import base64
import ssl
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

# ── 설정 (raas_morning_brief.py와 동일하게 유지) ──────────
SSPLUNK_HOST       = os.getenv("SPLUNK_HOST")
SPLUNK_USER       = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD   = os.getenv("SPLUNK_PASSWORD")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL")
SPLUNK_APP        = os.getenv("SPLUNK_APP")
# ─────────────────────────────────────────────────────────

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def auth():
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()

def splunk_search(spl: str, timeout: int = 60) -> list:
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", auth())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=timeout) as r:
            rows = []
            for line in r.read().decode("utf-8").strip().split("\n"):
                if not line: continue
                try:
                    obj = json.loads(line)
                    if obj.get("result"): rows.append(obj["result"])
                except: pass
            return rows
    except Exception as e:
        print(f"  [Splunk 오류] {e}", file=sys.stderr)
        return []

def call_claude(system: str, user: str, max_tokens: int = 1000) -> str:
    payload = json.dumps({
        "model": CLAUDE_MODEL, "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={"Content-Type": "application/json",
                 "anthropic-version": "2023-06-01",
                 "x-api-key": ANTHROPIC_API_KEY})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["content"][0]["text"]


# ── 질의 의도 분류 ─────────────────────────────────────────
INTENT_SYSTEM = """RAAS 데이터 분석 시스템의 질의 분류기입니다.
사용자 질문을 분석해서 JSON으로만 응답하세요. 다른 텍스트 없이 JSON만 출력하세요.

응답 형식:
{
  "intent": "daily|trend|compare|ranking|health|general",
  "date_type": "yesterday|today|specific|range|week|month",
  "specific_date": "YYYY-MM-DD 또는 null",
  "date_range_days": 7 또는 30 (범위 조회 시),
  "target": "platform|powerfm|lovefm|gorilam|specific_program",
  "program_keywords": ["프로그램명 키워드 배열"],
  "needs_comparison": true 또는 false,
  "summary": "질문 한줄 요약"
}

intent 분류:
- daily: 특정 날짜 단일 지표 질문 (어제 DAU는?)
- trend: 추세/변화 질문 (최근 일주일 트렌드, 오르고 있는 프로그램)
- compare: 비교 질문 (파워FM vs 러브FM, 전주 대비)
- ranking: 순위 질문 (가장 많이 들은, 상위 프로그램)
- health: 건강도/상태 질문 (잘 되고 있는, 위험한, 문제 있는)
- general: 기타 일반 질문"""

def classify_intent(question: str) -> dict:
    """질문 의도 분류"""
    try:
        result = call_claude(INTENT_SYSTEM, question, max_tokens=300)
        # JSON 추출
        result = result.strip()
        if result.startswith("```"):
            result = result.split("```")[1]
            if result.startswith("json"):
                result = result[4:]
        return json.loads(result.strip())
    except Exception as e:
        return {
            "intent": "general", "date_type": "yesterday",
            "specific_date": None, "date_range_days": 7,
            "target": "platform", "program_keywords": [],
            "needs_comparison": False,
            "summary": question
        }


# ── 의도별 데이터 조회 ────────────────────────────────────
def get_data_for_intent(intent_info: dict) -> dict:
    """의도에 맞는 Splunk 데이터 조회"""
    intent     = intent_info.get("intent", "daily")
    date_type  = intent_info.get("date_type", "yesterday")
    date_range = intent_info.get("date_range_days", 7)
    keywords   = intent_info.get("program_keywords", [])

    # 날짜 결정
    today = datetime.now()
    if date_type == "today":
        base_date = today.strftime("%Y-%m-%d")
    elif date_type == "specific" and intent_info.get("specific_date"):
        base_date = intent_info["specific_date"]
    elif date_type in ("week", "range"):
        base_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        base_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    data = {"intent": intent, "base_date": base_date}

    # ① 일간 단일 조회
    if intent in ("daily", "general"):
        rows = splunk_search(
            f'| inputlookup raas_briefing_latest.csv | head 1'
        )
        if rows:
            data["daily"] = rows[0]

        top_rows = splunk_search(
            "| inputlookup raas_top_programs_latest.csv | sort rank | head 10"
        )
        data["top_programs"] = top_rows

    # ② 트렌드 조회 (최근 N일)
    elif intent == "trend":
        days = min(date_range, 30)
        spl = f"""
| inputlookup raas_llm_context_day.csv
| eval _t = strptime(date_label, "%Y-%m-%d")
| where _t >= relative_time(now(), "-{days}d@d")
| sort date_label
| fields date_label total_dau deep_listen_rate dau_wow_pct
         powerfm_dau lovefm_dau gorilam_dau
"""
        rows = splunk_search(spl)
        data["trend"] = rows
        data["days"] = days

        # 프로그램별 트렌드
        if keywords:
            pgm_spl = f"""
| inputlookup raas_llm_context_day.csv
| eval _t = strptime(date_label, "%Y-%m-%d")
| where _t >= relative_time(now(), "-{days}d@d")
| sort date_label
| fields date_label F01_dau F02_dau F03_dau F04_dau F05_dau F06_dau
         F07_dau F08_dau F09_dau F10_dau F11_dau F12_dau F13_dau
         L01_dau L05_dau L06_dau L07_dau L08_dau L09_dau L10_dau
         L11_dau L12_dau L13_dau L14_dau L15_dau
"""
            pgm_rows = splunk_search(pgm_spl)
            data["program_trend"] = pgm_rows

    # ③ 순위/비교 조회
    elif intent in ("ranking", "compare"):
        rows = splunk_search(
            "| inputlookup raas_top_programs_latest.csv | sort rank"
        )
        data["ranking"] = rows

        brief_rows = splunk_search(
            "| inputlookup raas_briefing_latest.csv | head 1"
        )
        if brief_rows:
            data["daily"] = brief_rows[0]

        # 전주 데이터 비교용
        if intent == "compare":
            spl_week = f"""
| inputlookup raas_llm_context_day.csv
| sort -date_label
| head 14
| fields date_label total_dau deep_listen_rate powerfm_dau lovefm_dau gorilam_dau
"""
            data["weekly_data"] = splunk_search(spl_week)

    # ④ 건강도 조회
    elif intent == "health":
        rows = splunk_search(
            "| inputlookup raas_top_programs_latest.csv | sort rank"
        )
        data["ranking"] = rows

        trend_spl = f"""
| inputlookup raas_llm_context_day.csv
| sort -date_label
| head 7
| fields date_label total_dau deep_listen_rate dau_wow_pct
"""
        data["recent_trend"] = splunk_search(trend_spl)

    return data


# ── 최종 답변 생성 ────────────────────────────────────────
ANSWER_SYSTEM = """SBS 고릴라 라디오 앱 데이터 분석 어시스턴트입니다.
주어진 데이터를 바탕으로 질문에 명확하고 실용적으로 답하세요.

규칙:
- 한국어로 답하세요
- 수치는 천단위 쉼표 사용 (예: 189,021명)
- 비율은 소수점 1자리 (예: 74.5%)
- 증감은 방향 표시 (▲+5.2% / ▼-3.1%)
- 핵심 수치를 먼저, 해석과 인사이트를 뒤에
- 트렌드 질문은 변화 방향과 원인 추정 포함
- 최대 300자 이내로 간결하게

데이터 정의:
- DAU: 청취시간>0 고유 사용자 수
- 깊은청취율: 10분이상/1분이상 연속 청취 비율 (몰입도)
- WoW: 전주 동일 요일 대비 증감률"""

def format_data_for_claude(data: dict, question: str) -> str:
    """Splunk 데이터를 Claude 입력용 텍스트로 변환"""
    parts = [f"질문: {question}\n"]
    intent = data.get("intent", "general")

    # 일간 KPI
    if "daily" in data:
        d = data["daily"]
        total = float(d.get("total_dau", 0)) or 1
        wow = float(d.get("dau_wow_pct", 0))
        wow_str = f"▲+{wow:.1f}%" if wow > 0 else (f"▼{wow:.1f}%" if wow < 0 else "보합")
        parts.append(f"""[최신 일간 데이터] {d.get('report_date', '')}
- 전체 DAU: {int(float(d.get('total_dau',0))):,}명 ({wow_str})
- 깊은청취율: {d.get('deep_listen_rate',0)}%
- 파워FM: {int(float(d.get('powerfm_dau',0))):,}명 ({float(d.get('powerfm_dau',0))/total*100:.1f}%)
- 러브FM: {int(float(d.get('lovefm_dau',0))):,}명 ({float(d.get('lovefm_dau',0))/total*100:.1f}%)
- 고릴라M: {int(float(d.get('gorilam_dau',0))):,}명 ({float(d.get('gorilam_dau',0))/total*100:.1f}%)""")

    # 프로그램 순위
    if "top_programs" in data and data["top_programs"]:
        parts.append("\n[프로그램 순위]")
        for r in data["top_programs"][:10]:
            parts.append(f"  {r.get('rank')}위 {r.get('pgm_name')} ({r.get('channel')}): {int(float(r.get('dau',0))):,}명")

    # 순위 데이터 (ranking/compare용)
    if "ranking" in data and data["ranking"]:
        parts.append("\n[프로그램 순위]")
        for r in data["ranking"][:10]:
            parts.append(f"  {r.get('rank')}위 {r.get('pgm_name')} ({r.get('channel')}): {int(float(r.get('dau',0))):,}명")

    # 트렌드 데이터
    if "trend" in data and data["trend"]:
        parts.append(f"\n[최근 {data.get('days',7)}일 트렌드]")
        for r in data["trend"]:
            wow = float(r.get("dau_wow_pct", 0))
            wow_str = f"▲+{wow:.1f}%" if wow > 0 else (f"▼{wow:.1f}%" if wow < 0 else "보합")
            parts.append(
                f"  {r.get('date_label')}: DAU {int(float(r.get('total_dau',0))):,}명 "
                f"| 깊은청취율 {r.get('deep_listen_rate',0)}% | {wow_str}"
            )

    # 최근 트렌드 (health용)
    if "recent_trend" in data and data["recent_trend"]:
        parts.append("\n[최근 7일 추이]")
        for r in data["recent_trend"]:
            wow = float(r.get("dau_wow_pct", 0))
            wow_str = f"▲+{wow:.1f}%" if wow > 0 else (f"▼{wow:.1f}%" if wow < 0 else "보합")
            parts.append(
                f"  {r.get('date_label')}: DAU {int(float(r.get('total_dau',0))):,}명 "
                f"| 깊은청취율 {r.get('deep_listen_rate',0)}% | {wow_str}"
            )

    # 주간 비교 데이터
    if "weekly_data" in data and data["weekly_data"]:
        parts.append("\n[최근 14일 데이터 (비교용)]")
        for r in data["weekly_data"]:
            parts.append(
                f"  {r.get('date_label')}: DAU {int(float(r.get('total_dau',0))):,}명 "
                f"| 파워FM {int(float(r.get('powerfm_dau',0))):,} "
                f"| 러브FM {int(float(r.get('lovefm_dau',0))):,}"
            )

    return "\n".join(parts)


def query(question: str, target_date: str = None, verbose: bool = False) -> str:
    """
    자연어 질의 메인 함수
    verbose=True 이면 의도 분류 결과도 출력
    """
    # 1. 의도 분류
    if verbose:
        print("  [1/3] 질의 의도 분석 중...", flush=True)
    intent_info = classify_intent(question)
    if verbose:
        print(f"  → 의도: {intent_info.get('intent')} / "
              f"날짜: {intent_info.get('date_type')} / "
              f"대상: {intent_info.get('target')}")

    # target_date 명시 시 덮어쓰기
    if target_date:
        intent_info["specific_date"] = target_date
        intent_info["date_type"] = "specific"

    # 2. 데이터 조회
    if verbose:
        print("  [2/3] Splunk 데이터 조회 중...", flush=True)
    data = get_data_for_intent(intent_info)

    if not data:
        return "데이터를 찾을 수 없습니다. Splunk 연결을 확인하세요."

    # 3. Claude 답변 생성
    if verbose:
        print("  [3/3] Claude 답변 생성 중...", flush=True)
    context = format_data_for_claude(data, question)
    return call_claude(ANSWER_SYSTEM, context, max_tokens=600)


# ── CLI 실행 ──────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RAAS 자연어 질의 엔진 v2")
    parser.add_argument("question", nargs="?", help="질문")
    parser.add_argument("--date",    help="기준 날짜 YYYY-MM-DD")
    parser.add_argument("--verbose", action="store_true", help="상세 출력")
    parser.add_argument("--demo",    action="store_true", help="데모 질의 5개 실행")
    args = parser.parse_args()

    if args.demo:
        demo_questions = [
            "어제 가장 많이 들은 프로그램은?",
            "최근 일주일 DAU 트렌드는?",
            "파워FM이랑 러브FM 비교해줘",
            "깊은청취율이 높은 프로그램은?",
            "고릴라 앱 전반적으로 잘 되고 있어?",
        ]
        for q in demo_questions:
            print(f"\n{'='*55}")
            print(f"Q: {q}")
            print("="*55)
            print(query(q, verbose=True))
    elif args.question:
        print(query(args.question, args.date, args.verbose))
    else:
        parser.print_help()
