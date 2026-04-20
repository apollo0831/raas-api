"""
RAAS Morning Brief v1.4
수정사항:
  - top_programs 저장: append 중첩 → lookup REST API 업로드 방식
  - 대시보드: SplunkJS 제거 → Simple XML 순수 방식으로 변경
"""

import json, urllib.request, urllib.parse, urllib.error
import base64, ssl, sys, argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

# ── 설정 ────────────────────────────────────────────────
SPLUNK_HOST       = os.getenv("SPLUNK_HOST")
SPLUNK_USER       = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD   = os.getenv("SPLUNK_PASSWORD")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL")
SPLUNK_APP        = os.getenv("SPLUNK_APP")
# ────────────────────────────────────────────────────────

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def auth_header():
    t = base64.b64encode(f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()
    return f"Basic {t}"

PGM_MAP = {
    "F01":("딘딘의 뮤직하이","파워FM","2300"), "F02":("애프터 클럽","파워FM","0100"),
    "F03":("파워 스테이션","파워FM","0300"),    "F04":("이인권의 펀펀투데이","파워FM","0500"),
    "F05":("김영철의 파워FM","파워FM","0700"),  "F06":("아름다운 이 아침 봉태규입니다","파워FM","0900"),
    "F07":("박하선의 씨네타운","파워FM","1100"),"F08":("12시엔 주현영","파워FM","1200"),
    "F09":("두시탈출 컬투쇼","파워FM","1400"),  "F10":("황제파워","파워FM","1600"),
    "F11":("박소현의 러브게임","파워FM","1800"),"F12":("웬디의 영스트리트","파워FM","2000"),
    "F13":("배성재의 텐","파워FM","2200"),
    "L01":("심야방송반","러브FM","0000"),       "L02":("Love20(재)","러브FM","0200"),
    "L03":("OLDIES 20(재)","러브FM","0400"),   "L05":("고현준의 뉴스 브리핑","러브FM","0600"),
    "L06":("김태현의 정치쇼","러브FM","0700"),  "L07":("이숙영의 러브FM","러브FM","0900"),
    "L08":("박연미의 목돈연구소","러브FM","1100"),"L09":("유민상의 배고픈 라디오","러브FM","1200"),
    "L10":("그대의 오후 정엽입니다","러브FM","1400"),"L11":("어예진의 방과후 목돈연구소","러브FM","1600"),
    "L12":("6시 저녁바람 김창완입니다","러브FM","1800"),"L13":("주영진의 뉴스직격","러브FM","1700"),
    "L14":("김윤상의 뮤직투나잇","러브FM","2000"),"L15":("최백호의 낭만시대","러브FM","2200"),
}


def splunk_search(spl: str) -> list:
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/search/jobs/export"
    data = urllib.parse.urlencode({"search":spl,"output_mode":"json","count":0}).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", auth_header())
    req.add_header("Content-Type","application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=60) as resp:
            rows = []
            for line in resp:
                line = line.decode("utf-8").strip()
                if not line: continue
                try:
                    obj = json.loads(line)
                    if obj.get("result"): rows.append(obj["result"])
                except: pass
            return rows
    except urllib.error.HTTPError as e:
        print(f"[Splunk {e.code}] {e.read().decode()[:150]}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[Splunk Error] {e}", file=sys.stderr)
        return []


def upload_csv_to_lookup(lookup_name: str, csv_content: str) -> bool:
    """Splunk REST API로 CSV 파일 직접 업로드 (append 중첩 한계 우회)"""
    # 기존 파일 업데이트 시도 (POST)
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/data/lookup-table-files/{lookup_name}"
    boundary = "RaasUploadBoundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="eai:data"; filename="{lookup_name}"\r\n'
        f"Content-Type: text/csv\r\n\r\n"
        f"{csv_content}\r\n"
        f"--{boundary}--\r\n"
    ).encode("utf-8")
    
    for method in ["POST", "GET"]:  # POST=업데이트, 없으면 신규생성
        req = urllib.request.Request(url, data=body if method=="POST" else None,
                                     method=method)
        req.add_header("Authorization", auth_header())
        if method == "POST":
            req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
        try:
            with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=30) as resp:
                if method == "POST":
                    resp.read()
                    return True
        except urllib.error.HTTPError as e:
            if e.code == 404 and method == "POST":
                # 파일 없음 → 신규 생성
                create_url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/data/lookup-table-files"
                create_body = (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="eai:data"; filename="{lookup_name}"\r\n'
                    f"Content-Type: text/csv\r\n\r\n"
                    f"{csv_content}\r\n"
                    f"--{boundary}--\r\n"
                ).encode("utf-8")
                create_req = urllib.request.Request(create_url, data=create_body, method="POST")
                create_req.add_header("Authorization", auth_header())
                create_req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
                try:
                    with urllib.request.urlopen(create_req, context=SSL_CONTEXT, timeout=30) as r:
                        r.read()
                        return True
                except Exception as ce:
                    print(f"  신규 생성 실패: {ce}", file=sys.stderr)
            break
        except Exception as e:
            print(f"  업로드 오류: {e}", file=sys.stderr)
            break
    return False


def save_to_splunk(date_str: str, briefing: str, ctx: dict):
    """브리핑 + KPI + 프로그램 순위를 Splunk 룩업에 저장"""
    
    # ① 브리핑 + KPI 요약 (SPL makeresults 방식 — 단일 행이라 문제 없음)
    b_escaped = briefing.replace("\\","\\\\").replace('"','\\"').replace("\n","\\n")
    spl_brief = f"""
| makeresults
| eval report_date="{date_str}",
       briefing="{b_escaped}",
       total_dau={ctx.get('total_dau',0)},
       deep_listen_rate={ctx.get('deep_listen_rate',0)},
       dau_wow_pct={ctx.get('dau_wow_pct',0)},
       powerfm_dau={ctx.get('powerfm_dau',0)},
       lovefm_dau={ctx.get('lovefm_dau',0)},
       gorilam_dau={ctx.get('gorilam_dau',0)},
       pick_dau={ctx.get('pick_dau',0)},
       updated_at=strftime(now(),"%Y-%m-%d %H:%M:%S")
| fields report_date briefing total_dau deep_listen_rate dau_wow_pct
         powerfm_dau lovefm_dau gorilam_dau pick_dau updated_at
| outputlookup raas_briefing_latest.csv
"""
    splunk_search(spl_brief)
    print("  ✅ raas_briefing_latest.csv 저장 완료")

    # ② 프로그램 순위 — CSV 직접 업로드 (append 중첩 한계 우회)
    programs = ctx.get("top_programs", [])[:10]
    if programs:
        lines = ["rank,pgm_code,pgm_name,channel,dau,report_date"]
        for i, p in enumerate(programs, 1):
            name_safe = p["name"].replace('"', "")
            lines.append(f'{i},{p["code"]},"{name_safe}",{p["channel"]},{p["dau"]},{date_str}')
        csv_content = "\n".join(lines)
        
        ok = upload_csv_to_lookup("raas_top_programs_latest.csv", csv_content)
        if ok:
            print("  ✅ raas_top_programs_latest.csv 업로드 완료")
        else:
            # REST 업로드 실패 시 SPL 대안 (makeresults count + case)
            case_parts = "\n".join(
                f'    rank={i}, "{p["code"]}|{p["name"].replace(chr(34),"")}|{p["channel"]}|{p["dau"]}"'
                for i, p in enumerate(programs, 1)
            )
            spl_top = f"""
| makeresults count={len(programs)}
| streamstats count as rank
| eval raw=case(
{case_parts}
)
| rex field=raw "(?P<pgm_code>[^|]+)\\|(?P<pgm_name>[^|]+)\\|(?P<channel>[^|]+)\\|(?P<dau>\\d+)"
| eval dau=tonumber(dau), report_date="{date_str}"
| fields rank pgm_code pgm_name channel dau report_date
| outputlookup raas_top_programs_latest.csv
"""
            splunk_search(spl_top)
            print("  ✅ raas_top_programs_latest.csv SPL 방식으로 저장 완료")


def test_connection():
    print(f"Splunk 연결 테스트: {SPLUNK_HOST}")
    rows = splunk_search('| makeresults | eval t="ok" | fields t')
    if rows and rows[0].get("t") == "ok":
        print("✅ Splunk 연결 성공!")
        for lk in ["program_user_funnel_day.csv","raas_llm_context_day.csv",
                   "raas_briefing_latest.csv","raas_top_programs_latest.csv"]:
            r = splunk_search(f"| inputlookup {lk} | head 1 | stats count")
            ok = r and int(r[0].get("count",0)) > 0
            print(f"  {'✅' if ok else '⚠️ (미생성)'} {lk}")
        r2 = splunk_search("| inputlookup program_user_funnel_day.csv | stats max(DATE) as d")
        if r2: print(f"\n  최신 데이터: {r2[0].get('d','')}")
    else:
        print("❌ 연결 실패")


def get_daily_context(target_date: str) -> dict:
    date_splunk = target_date.replace("-", "/")
    rows = splunk_search(
        f'| inputlookup raas_llm_context_day.csv | search date_label="{target_date}" | head 1')
    if not rows:
        print("  → funnel 직접 조회")
        pgm_stats = "\n".join(
            f'    max(eval(if(TYPE="ALL", floor({c}), null()))) as {c}_dau'
            for c in PGM_MAP.keys()
        )
        rows = splunk_search(f"""
| inputlookup program_user_funnel_day.csv
| search DATE="{date_splunk}" PERIOD=1D
| stats
    max(eval(if(TYPE="ALL",   floor(T00),null()))) as total_dau
    max(eval(if(TYPE="1MIN",  floor(T00),null()))) as total_1min
    max(eval(if(TYPE="10MIN", floor(T00),null()))) as total_10min
    max(eval(if(TYPE="ALL",   floor(F00),null()))) as powerfm_dau
    max(eval(if(TYPE="ALL",   floor(L00),null()))) as lovefm_dau
    max(eval(if(TYPE="ALL",   floor(G00),null()))) as gorilam_dau
    max(eval(if(TYPE="ALL",   floor(P00),null()))) as pick_dau
{pgm_stats}
""")
    if not rows: return {}
    row = rows[0]
    def iv(k): v=row.get(k); return int(float(v)) if v and str(v) not in("","null","None") else 0
    def fv(k): v=row.get(k); return round(float(v),1) if v and str(v) not in("","null","None") else 0.0
    t1,t10 = iv("total_1min"),iv("total_10min")
    deep = fv("deep_listen_rate") or (round(t10/t1*100,1) if t1>0 else 0.0)
    progs = [{"code":c,"name":n,"channel":ch,"stime":s,"dau":iv(f"{c}_dau")}
             for c,(n,ch,s) in PGM_MAP.items() if iv(f"{c}_dau")>0]
    progs.sort(key=lambda x:-x["dau"])
    return {"date":target_date,"total_dau":iv("total_dau"),"total_wau":iv("total_wau"),
            "total_mau":iv("total_mau"),"deep_listen_rate":deep,"dau_wow_pct":fv("dau_wow_pct"),
            "powerfm_dau":iv("powerfm_dau"),"lovefm_dau":iv("lovefm_dau"),
            "gorilam_dau":iv("gorilam_dau"),"pick_dau":iv("pick_dau"),"top_programs":progs[:12]}


def call_claude(system_prompt, user_msg):
    payload = json.dumps({"model":CLAUDE_MODEL,"max_tokens":1000,"system":system_prompt,
                          "messages":[{"role":"user","content":user_msg}]}).encode()
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=payload,
        headers={"Content-Type":"application/json","anthropic-version":"2023-06-01",
                 "x-api-key":ANTHROPIC_API_KEY})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["content"][0]["text"]

SYSTEM = """SBS 고릴라 라디오 앱 데이터 분석 어시스턴트입니다.
DAU=청취시간>0 사용자|깊은청취율=10분이상/1분이상|WoW=전주대비
한국어로 간결하게, 수치는 천단위 쉼표 사용."""

def morning_brief(target_date=None, save=False):
    if not target_date:
        target_date = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[{target_date}] 데이터 조회 중...", flush=True)
    ctx = get_daily_context(target_date)
    if not ctx or not ctx.get("total_dau"):
        r = splunk_search("| inputlookup program_user_funnel_day.csv | search TYPE=ALL PERIOD=1D | stats max(DATE) as d")
        if r:
            latest = r[0].get("d","").replace("/","-")
            print(f"  → {latest} 재조회")
            ctx = get_daily_context(latest)
            target_date = ctx.get("date", target_date)
    if not ctx or not ctx.get("total_dau"):
        return "[오류] 데이터 없음"
    total = ctx["total_dau"] or 1
    wow = ctx["dau_wow_pct"]
    top_str = "\n".join(f"  {i+1}. [{p['channel']}] {p['name']}: {p['dau']:,}명"
                        for i,p in enumerate(ctx["top_programs"]))
    msg = f"""다음 데이터로 {ctx['date']} 고릴라 앱 아침 브리핑을 작성해주세요.

## 플랫폼 전체
- DAU: {ctx['total_dau']:,}명 (전주대비 {'▲+' if wow>=0 else '▼'}{abs(wow):.1f}%)
- 깊은청취율: {ctx['deep_listen_rate']:.1f}%

## 채널별 DAU
- 파워FM: {ctx['powerfm_dau']:,}명 ({ctx['powerfm_dau']/total*100:.1f}%)
- 러브FM: {ctx['lovefm_dau']:,}명 ({ctx['lovefm_dau']/total*100:.1f}%)
- 고릴라M: {ctx['gorilam_dau']:,}명 ({ctx['gorilam_dau']/total*100:.1f}%)
- 픽채널: {ctx['pick_dau']:,}명 ({ctx['pick_dau']/total*100:.1f}%)

## 상위 프로그램
{top_str}

형식: 1.핵심지표요약 2.주목할점 3.프로그램하이라이트 4.한줄코멘트"""
    print("Claude API 호출 중...", flush=True)
    briefing = call_claude(SYSTEM, msg)
    if save:
        print("Splunk 저장 중...", flush=True)
        save_to_splunk(ctx["date"], briefing, ctx)
    return briefing

def query_mode(question, target_date=None):
    if not target_date:
        target_date = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[{target_date}] 조회 중...", flush=True)
    ctx = get_daily_context(target_date)
    matched = [p for p in ctx.get("top_programs",[])
               if any(kw in question for kw in [p["name"],p["code"],p["name"].split(" ")[-1]] if len(kw)>1)]
    cstr = f"날짜:{ctx.get('date',target_date)}\nDAU:{ctx.get('total_dau',0):,}\n깊은청취율:{ctx.get('deep_listen_rate',0):.1f}%\n\n"
    if matched:
        cstr += "관련:\n" + "\n".join(f"  {p['name']}({p['channel']}): {p['dau']:,}명" for p in matched)
    else:
        cstr += "전체:\n" + "\n".join(f"  {p['name']}: {p['dau']:,}명" for p in ctx.get("top_programs",[]))
    return call_claude(SYSTEM, f"데이터:\n{cstr}\n\n질문:{question}")


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header

# ── 이메일 설정 ────────────────────────────────────
GMAIL_ADDRESS = "apollo0831@gmail.com"
GMAIL_APP_PW  = "fituhwtybxierrok"   # ← 공백 없는 16자리 앱 비밀번호
EMAIL_TO      = ["apollo0831@gmail.com"]  # ← 수신자 목록
# ──────────────────────────────────────────────────

def briefing_to_html(briefing_text: str, report_date: str, kpi: dict) -> str:
    raw = briefing_text.replace("\\n", "\n")
    lines = []
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            lines.append("<br>")
        elif line.startswith("## "):
            lines.append(f'<h3 style="color:#e8a838;margin:16px 0 6px;font-size:15px">{line[3:]}</h3>')
        elif line.startswith("# "):
            lines.append(f'<h2 style="color:#e8a838;margin:8px 0;font-size:17px">{line[2:]}</h2>')
        elif line.startswith(("- ", ". ")):
            c = line[2:].replace("**","<b>",1)
            while "**" in c: c = c.replace("**","</b>",1)
            lines.append(f'<li style="margin:4px 0">{c}</li>')
        else:
            c = line.replace("**","<b>",1)
            while "**" in c: c = c.replace("**","</b>",1)
            lines.append(f'<p style="margin:4px 0">{c}</p>')
    body_html = "\n".join(lines)

    total = float(kpi.get("total_dau", 0)) or 1
    def fmtk(v): n=float(v or 0); return f"{n/1000:.1f}k" if n>=10000 else f"{int(n):,}"
    def ppct(v): return f"{float(v or 0)/total*100:.1f}%"
    wow = float(kpi.get("dau_wow_pct", 0))
    wow_str = f"▲ +{wow:.1f}%" if wow>0 else (f"▼ {wow:.1f}%" if wow<0 else "→ 보합")
    wow_color = "#10b981" if wow>0 else ("#ef4444" if wow<0 else "#9ca3af")

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="background:#f8f8f8;font-family:'Malgun Gothic',sans-serif;margin:0;padding:20px">
<div style="max-width:680px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
  <div style="background:#0a0c10;padding:24px 32px">
    <div style="color:#e8a838;font-size:11px;letter-spacing:.1em;margin-bottom:6px">RAAS &middot; 고릴라 AI 브리핑</div>
    <div style="color:#fff;font-size:20px;font-weight:700">{report_date} 아침 브리핑</div>
    <div style="color:#6b7280;font-size:12px;margin-top:4px">SBS 고릴라 라디오 앱 청취 현황</div>
  </div>
  <div style="background:#11141a;padding:20px 32px;display:flex;gap:16px;flex-wrap:wrap">
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #e8a838">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">TOTAL DAU</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{fmtk(kpi.get("total_dau",0))}</div>
      <div style="color:{wow_color};font-size:11px;margin-top:2px">{wow_str}</div>
    </div>
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #3b82f6">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">깊은청취율</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{kpi.get("deep_listen_rate",0)}%</div>
      <div style="color:#6b7280;font-size:11px;margin-top:2px">10분이상/1분이상</div>
    </div>
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #3b82f6">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">파워FM</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{fmtk(kpi.get("powerfm_dau",0))}</div>
      <div style="color:#6b7280;font-size:11px;margin-top:2px">{ppct(kpi.get("powerfm_dau",0))} 점유</div>
    </div>
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #ec4899">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">러브FM</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{fmtk(kpi.get("lovefm_dau",0))}</div>
      <div style="color:#6b7280;font-size:11px;margin-top:2px">{ppct(kpi.get("lovefm_dau",0))} 점유</div>
    </div>
  </div>
  <div style="padding:24px 32px;color:#333;font-size:14px;line-height:1.8">{body_html}</div>
  <div style="background:#f1f1f1;padding:16px 32px;text-align:center">
    <div style="color:#999;font-size:11px">자동 생성: RAAS (Radio Analysis AI System) &middot; SBS 고릴라</div>
    <div style="color:#bbb;font-size:10px;margin-top:4px">Powered by Claude AI &middot; Splunk</div>
  </div>
</div></body></html>"""


def send_briefing_email(briefing: str, ctx: dict) -> bool:
    """브리핑을 HTML 이메일로 발송"""
    report_date = ctx.get("date", datetime.now().strftime("%Y-%m-%d"))
    subject = f"[고릴라 RAAS] AI 아침 브리핑 {report_date}"
    html = briefing_to_html(briefing, report_date, ctx)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = ", ".join(EMAIL_TO)
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PW)
            server.send_message(msg)
        print(f"  ✅ 이메일 발송 완료 → {', '.join(EMAIL_TO)}")
        return True
    except smtplib.SMTPAuthenticationError:
        print("  ❌ Gmail 인증 실패 — GMAIL_APP_PW 확인 (공백 없는 16자리)")
        return False
    except Exception as e:
        print(f"  ❌ 이메일 발송 오류: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date");  parser.add_argument("--query")
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--email", action="store_true", help="브리핑 이메일 발송")
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    try:
        if args.test:     test_connection()
        elif args.query:  print(query_mode(args.query, args.date))
        else:
            briefing = morning_brief(args.date, args.save)
            print(briefing)
            if args.email:
                ctx = get_daily_context(args.date or (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d"))
                send_briefing_email(briefing, ctx)
    except Exception as e:
        print(f"[오류] {type(e).__name__}: {e}", file=sys.stderr); sys.exit(1)


# ══════════════════════════════════════════════════
# 이메일 발송 기능 (v1.5 추가)
# ══════════════════════════════════════════════════