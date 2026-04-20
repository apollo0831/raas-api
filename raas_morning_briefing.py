"""
RAAS Morning Brief Generator v1.3
- Splunk 8.0.x Basic Auth
- 브리핑 결과를 Splunk 룩업에 자동 저장 (--save 옵션)
- 대시보드 표시용 구조화 데이터도 함께 저장

사용법:
  python raas_morning_brief.py               # 브리핑 출력
  python raas_morning_brief.py --save        # 브리핑 + Splunk 룩업에 저장
  python raas_morning_brief.py --date 2026-04-06 --save
  python raas_morning_brief.py --query "컬투쇼 DAU 알려줘"
  python raas_morning_brief.py --test
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import base64
import ssl
import sys
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

# ── 설정 ─────────────────────────────────────────────────
SPLUNK_HOST       = os.getenv("SPLUNK_HOST")
SPLUNK_USER       = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD   = os.getenv("SPLUNK_PASSWORD")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL")
SPLUNK_APP        = os.getenv("SPLUNK_APP")
# ─────────────────────────────────────────────────────────

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def auth_header():
    token = base64.b64encode(f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()
    return f"Basic {token}"

PGM_MAP = {
    "F01": ("딘딘의 뮤직하이",               "파워FM", "2300"),
    "F02": ("애프터 클럽",                   "파워FM", "0100"),
    "F03": ("파워 스테이션",                 "파워FM", "0300"),
    "F04": ("이인권의 펀펀투데이",           "파워FM", "0500"),
    "F05": ("김영철의 파워FM",               "파워FM", "0700"),
    "F06": ("아름다운 이 아침 봉태규입니다", "파워FM", "0900"),
    "F07": ("박하선의 씨네타운",             "파워FM", "1100"),
    "F08": ("12시엔 주현영",                 "파워FM", "1200"),
    "F09": ("두시탈출 컬투쇼",               "파워FM", "1400"),
    "F10": ("황제파워",                      "파워FM", "1600"),
    "F11": ("박소현의 러브게임",             "파워FM", "1800"),
    "F12": ("웬디의 영스트리트",             "파워FM", "2000"),
    "F13": ("배성재의 텐",                   "파워FM", "2200"),
    "L01": ("심야방송반",                    "러브FM", "0000"),
    "L02": ("Love20(재)",                    "러브FM", "0200"),
    "L03": ("OLDIES 20(재)",                 "러브FM", "0400"),
    "L05": ("고현준의 뉴스 브리핑",         "러브FM", "0600"),
    "L06": ("김태현의 정치쇼",               "러브FM", "0700"),
    "L07": ("이숙영의 러브FM",               "러브FM", "0900"),
    "L08": ("박연미의 목돈연구소",           "러브FM", "1100"),
    "L09": ("유민상의 배고픈 라디오",        "러브FM", "1200"),
    "L10": ("그대의 오후 정엽입니다",        "러브FM", "1400"),
    "L11": ("어예진의 방과후 목돈연구소",    "러브FM", "1600"),
    "L12": ("6시 저녁바람 김창완입니다",     "러브FM", "1800"),
    "L13": ("주영진의 뉴스직격",             "러브FM", "1700"),
    "L14": ("김윤상의 뮤직투나잇",           "러브FM", "2000"),
    "L15": ("최백호의 낭만시대",             "러브FM", "2200"),
}


def splunk_search(spl: str) -> list[dict]:
    url = f"{SPLUNK_HOST}/services/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", auth_header())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=60) as resp:
            rows = []
            for line in resp:
                line = line.decode("utf-8").strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("result"):
                        rows.append(obj["result"])
                except json.JSONDecodeError:
                    continue
            return rows
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"[Splunk HTTP {e.code}] {body[:200]}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[Splunk Error] {e}", file=sys.stderr)
        return []


def save_to_splunk(date_str: str, briefing: str, ctx: dict):
    """브리핑과 KPI 요약을 Splunk 룩업에 저장"""

    # 1. 브리핑 텍스트 저장 (raas_briefing_latest.csv)
    briefing_escaped = briefing.replace('"', '\\"').replace('\n', '\\n')
    spl_brief = f"""
| makeresults
| eval report_date="{date_str}"
| eval briefing="{briefing_escaped}"
| eval total_dau={ctx.get('total_dau', 0)}
| eval deep_listen_rate={ctx.get('deep_listen_rate', 0)}
| eval dau_wow_pct={ctx.get('dau_wow_pct', 0)}
| eval powerfm_dau={ctx.get('powerfm_dau', 0)}
| eval lovefm_dau={ctx.get('lovefm_dau', 0)}
| eval gorilam_dau={ctx.get('gorilam_dau', 0)}
| eval updated_at=strftime(now(), "%Y-%m-%d %H:%M:%S")
| fields report_date briefing total_dau deep_listen_rate dau_wow_pct powerfm_dau lovefm_dau gorilam_dau updated_at
| outputlookup raas_briefing_latest.csv
"""
    rows = splunk_search(spl_brief)
    print("  ✅ raas_briefing_latest.csv 저장 완료")

    # 2. 프로그램 순위 저장 (raas_top_programs_latest.csv)
    if ctx.get('top_programs'):
        makeresults_parts = []
        for i, p in enumerate(ctx['top_programs'][:10], 1):
            makeresults_parts.append(
                f'| makeresults | eval rank={i}, pgm_code="{p["code"]}", '
                f'pgm_name="{p["name"]}", channel="{p["channel"]}", '
                f'dau={p["dau"]}, report_date="{date_str}"'
            )
        spl_top = "\n| append [\n".join(makeresults_parts)
        spl_top += "\n]\n| fields rank pgm_code pgm_name channel dau report_date"
        spl_top += "\n| sort rank"
        spl_top += "\n| outputlookup raas_top_programs_latest.csv"

        splunk_search(spl_top)
        print("  ✅ raas_top_programs_latest.csv 저장 완료")


def test_connection():
    print(f"Splunk 연결 테스트: {SPLUNK_HOST}")
    rows = splunk_search('| makeresults | eval test="ok" | fields test')
    if rows and rows[0].get("test") == "ok":
        print("✅ Splunk 연결 성공!")
        for lk in ["program_user_funnel_day.csv", "raas_llm_context_day.csv",
                   "raas_briefing_latest.csv", "raas_top_programs_latest.csv"]:
            rows2 = splunk_search(f"| inputlookup {lk} | head 1 | stats count")
            exists = rows2 and int(rows2[0].get("count", 0)) > 0
            icon = "✅" if exists else "⚠️ (미생성)"
            print(f"  {icon} {lk}")
        rows3 = splunk_search(
            '| inputlookup program_user_funnel_day.csv | stats max(DATE) as d')
        if rows3:
            print(f"\n  최신 데이터: {rows3[0].get('d', '확인불가')}")
    else:
        print("❌ Splunk 연결 실패")


def get_daily_context(target_date: str) -> dict:
    date_splunk = target_date.replace("-", "/")
    rows = splunk_search(
        f'| inputlookup raas_llm_context_day.csv | search date_label="{target_date}" | head 1')
    if not rows:
        print("  → raas_llm_context_day.csv 없음, funnel 직접 조회")
        rows = splunk_search(f"""
| inputlookup program_user_funnel_day.csv
| search DATE="{date_splunk}" PERIOD=1D
| stats
    max(eval(if(TYPE="ALL",   floor(T00), null()))) as total_dau
    max(eval(if(TYPE="1MIN",  floor(T00), null()))) as total_1min
    max(eval(if(TYPE="10MIN", floor(T00), null()))) as total_10min
    max(eval(if(TYPE="ALL",   floor(F00), null()))) as powerfm_dau
    max(eval(if(TYPE="ALL",   floor(L00), null()))) as lovefm_dau
    max(eval(if(TYPE="ALL",   floor(G00), null()))) as gorilam_dau
    max(eval(if(TYPE="ALL",   floor(P00), null()))) as pick_dau
    {chr(10).join(f'    max(eval(if(TYPE="ALL", floor({c}), null()))) as {c}_dau' for c in list(PGM_MAP.keys()))}
""")
    if not rows:
        return {}
    row = rows[0]

    def iv(k):
        v = row.get(k)
        return int(float(v)) if v and str(v) not in ("", "null", "None") else 0

    def fv(k):
        v = row.get(k)
        return round(float(v), 1) if v and str(v) not in ("", "null", "None") else 0.0

    t1, t10 = iv("total_1min"), iv("total_10min")
    deep = fv("deep_listen_rate") or (round(t10/t1*100,1) if t1 > 0 else 0.0)

    programs = [{"code": c, "name": n, "channel": ch, "stime": s, "dau": iv(f"{c}_dau")}
                for c, (n, ch, s) in PGM_MAP.items() if iv(f"{c}_dau") > 0]
    programs.sort(key=lambda x: -x["dau"])

    return {
        "date": target_date,
        "total_dau": iv("total_dau"), "total_wau": iv("total_wau"),
        "total_mau": iv("total_mau"), "deep_listen_rate": deep,
        "dau_wow_pct": fv("dau_wow_pct"),
        "powerfm_dau": iv("powerfm_dau"), "lovefm_dau": iv("lovefm_dau"),
        "gorilam_dau": iv("gorilam_dau"), "pick_dau": iv("pick_dau"),
        "top_programs": programs[:12],
    }


def call_claude(system_prompt: str, user_message: str) -> str:
    payload = json.dumps({
        "model": CLAUDE_MODEL, "max_tokens": 1000,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
            "x-api-key": ANTHROPIC_API_KEY
        }
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["content"][0]["text"]


SYSTEM_PROMPT = """당신은 SBS 고릴라 라디오 앱의 데이터 분석 어시스턴트입니다.
청취 데이터를 기반으로 방송 관계자들에게 명확하고 실행 가능한 인사이트를 제공합니다.
- DAU: 청취시간>0 고유 사용자 수 | WAU: 7일 롤링 | MAU: 30일 롤링
- 깊은청취율: 10분이상/1분이상 비율 | WoW: 전주 동일 요일 대비
응답은 한국어로, 수치는 천단위 쉼표 사용."""


def morning_brief(target_date: str = None, save: bool = False) -> str:
    if not target_date:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[{target_date}] 데이터 조회 중...", flush=True)
    ctx = get_daily_context(target_date)

    if not ctx or not ctx.get("total_dau"):
        print("  최신 날짜 자동 탐색...", flush=True)
        rows = splunk_search(
            "| inputlookup program_user_funnel_day.csv | search TYPE=ALL PERIOD=1D"
            " | stats max(DATE) as latest")
        if rows:
            latest = rows[0].get("latest", "").replace("/", "-")
            print(f"  → {latest} 으로 재조회")
            ctx = get_daily_context(latest)
            target_date = ctx.get("date", target_date)

    if not ctx or not ctx.get("total_dau"):
        return "[오류] 데이터를 찾을 수 없습니다. --test 로 연결 확인 후 재시도하세요."

    total = ctx["total_dau"] or 1
    wow = ctx["dau_wow_pct"]
    top_str = "\n".join(
        f"  {i+1}. [{p['channel']}] {p['name']}: {p['dau']:,}명"
        for i, p in enumerate(ctx["top_programs"]))

    user_msg = f"""다음 데이터를 바탕으로 {ctx['date']} 고릴라 앱 아침 브리핑을 작성해주세요.

## 플랫폼 전체
- DAU: {ctx['total_dau']:,}명 (전주대비 {'▲+' if wow>=0 else '▼'}{abs(wow):.1f}%)
- 깊은청취율: {ctx['deep_listen_rate']:.1f}%

## 채널별 DAU
- 파워FM: {ctx['powerfm_dau']:,}명 ({ctx['powerfm_dau']/total*100:.1f}%)
- 러브FM: {ctx['lovefm_dau']:,}명 ({ctx['lovefm_dau']/total*100:.1f}%)
- 고릴라M: {ctx['gorilam_dau']:,}명 ({ctx['gorilam_dau']/total*100:.1f}%)
- 픽채널: {ctx['pick_dau']:,}명 ({ctx['pick_dau']/total*100:.1f}%)

## 상위 프로그램 (DAU 기준)
{top_str}

형식: 1.핵심지표요약 2.주목할점 3.상위프로그램하이라이트 4.오늘의한줄코멘트"""

    print("Claude API 호출 중...", flush=True)
    briefing = call_claude(SYSTEM_PROMPT, user_msg)

    if save:
        print("Splunk 룩업에 저장 중...", flush=True)
        save_to_splunk(ctx["date"], briefing, ctx)

    return briefing


def query_mode(question: str, target_date: str = None) -> str:
    if not target_date:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[{target_date}] 데이터 조회 중...", flush=True)
    ctx = get_daily_context(target_date)
    matched = [p for p in ctx.get("top_programs", [])
               if any(kw in question for kw in
                      [p["name"], p["code"], p["name"].split(" ")[-1]] if len(kw) > 1)]
    context_str = (f"날짜: {ctx.get('date', target_date)}\n"
                   f"플랫폼 전체 DAU: {ctx.get('total_dau',0):,}명\n"
                   f"깊은청취율: {ctx.get('deep_listen_rate',0):.1f}%\n\n")
    if matched:
        context_str += "관련 프로그램:\n" + "\n".join(
            f"  {p['name']}({p['channel']}): DAU {p['dau']:,}명" for p in matched)
    else:
        context_str += "전체 프로그램:\n" + "\n".join(
            f"  {p['name']}: {p['dau']:,}명" for p in ctx.get("top_programs", []))
    return call_claude(SYSTEM_PROMPT, f"데이터:\n{context_str}\n\n질문: {question}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RAAS Morning Brief v1.3")
    parser.add_argument("--date",  help="분석 날짜 YYYY-MM-DD")
    parser.add_argument("--query", help="자연어 질의")
    parser.add_argument("--save",  action="store_true", help="결과를 Splunk 룩업에 저장")
    parser.add_argument("--test",  action="store_true", help="연결 테스트")
    args = parser.parse_args()
    try:
        if args.test:
            test_connection()
        elif args.query:
            print(query_mode(args.query, args.date))
        else:
            print(morning_brief(args.date, args.save))
    except Exception as e:
        print(f"[오류] {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)