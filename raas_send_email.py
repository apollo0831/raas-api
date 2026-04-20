"""
RAAS 브리핑 이메일 발송 스크립트
Gmail MCP 대신 Python smtplib으로 직접 발송
또는 --save 후 결과를 이메일로 전송

사용법:
  python raas_send_email.py                    # 브리핑 생성 + 이메일 발송
  python raas_send_email.py --date 2026-04-08  # 특정 날짜 브리핑 발송
  python raas_send_email.py --test             # 테스트 이메일 발송
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import base64
import ssl
import sys
import smtplib
import argparse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

# ── 설정 ─────────────────────────────────────────────
SPLUNK_HOST       = os.getenv("SPLUNK_HOST")
SPLUNK_USER       = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD   = os.getenv("SPLUNK_PASSWORD")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL")
SPLUNK_APP        = os.getenv("SPLUNK_APP")

# 이메일 설정
GMAIL_ADDRESS  = os.getenv("GMAIL_ADDRESS")
GMAIL_APP_PW   = os.getenv("GMAIL_APP_PW")
EMAIL_TO       = [                         # 수신자 목록
    "apollo0831@gmail.com",               # 본인
    # "team@sbs.co.kr",                   # 추가 수신자
]
EMAIL_SUBJECT  = "[고릴라 RAAS] AI 아침 브리핑 {date}"
# ─────────────────────────────────────────────────────

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def auth():
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()

def splunk_search(spl):
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", auth())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=60) as r:
            rows = []
            for line in r.read().decode("utf-8").strip().split("\n"):
                if not line: continue
                try:
                    obj = json.loads(line)
                    if obj.get("result"): rows.append(obj["result"])
                except: pass
            return rows
    except Exception as e:
        print(f"[Splunk 오류] {e}", file=sys.stderr)
        return []

def briefing_to_html(briefing_text: str, report_date: str, kpi: dict) -> str:
    """브리핑 텍스트를 이메일용 HTML로 변환"""
    raw = briefing_text.replace("\\n", "\n")

    # 마크다운 → HTML 기본 변환
    lines = []
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            lines.append("<br>")
        elif line.startswith("## "):
            lines.append(f'<h3 style="color:#e8a838;margin:16px 0 6px;font-size:15px">{line[3:]}</h3>')
        elif line.startswith("# "):
            lines.append(f'<h2 style="color:#e8a838;margin:8px 0;font-size:17px">{line[2:]}</h2>')
        elif line.startswith("- ") or line.startswith("• "):
            content = line[2:].replace("**", "<strong>").replace("**", "</strong>")
            lines.append(f'<li style="margin:4px 0">{content}</li>')
        else:
            content = line.replace("**", "<strong>", 1)
            while "**" in content:
                content = content.replace("**", "</strong>", 1)
            lines.append(f'<p style="margin:4px 0">{content}</p>')

    body_html = "\n".join(lines)
    total = float(kpi.get("total_dau", 0)) or 1

    def fmt(v):
        n = float(v or 0)
        return f"{n/1000:.1f}k" if n >= 10000 else f"{int(n):,}"

    def pct(v):
        return f"{float(v or 0)/total*100:.1f}%"

    wow = float(kpi.get("dau_wow_pct", 0))
    wow_str = f"▲ +{wow:.1f}%" if wow > 0 else (f"▼ {wow:.1f}%" if wow < 0 else "→ 보합")
    wow_color = "#10b981" if wow > 0 else ("#ef4444" if wow < 0 else "#9ca3af")

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="background:#f8f8f8;font-family:'Malgun Gothic',sans-serif;margin:0;padding:20px">
<div style="max-width:680px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">

  <!-- 헤더 -->
  <div style="background:#0a0c10;padding:24px 32px">
    <div style="color:#e8a838;font-size:11px;letter-spacing:.1em;margin-bottom:6px">RAAS · 고릴라 AI 브리핑</div>
    <div style="color:#fff;font-size:20px;font-weight:700">{report_date} 아침 브리핑</div>
    <div style="color:#6b7280;font-size:12px;margin-top:4px">SBS 고릴라 라디오 앱 청취 현황</div>
  </div>

  <!-- KPI 카드 -->
  <div style="background:#11141a;padding:20px 32px;display:flex;gap:16px;flex-wrap:wrap">
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #e8a838">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">TOTAL DAU</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{fmt(kpi.get("total_dau",0))}</div>
      <div style="color:{wow_color};font-size:11px;margin-top:2px">{wow_str}</div>
    </div>
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #3b82f6">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">깊은청취율</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{kpi.get("deep_listen_rate",0)}%</div>
      <div style="color:#6b7280;font-size:11px;margin-top:2px">10분이상/1분이상</div>
    </div>
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #3b82f6">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">파워FM</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{fmt(kpi.get("powerfm_dau",0))}</div>
      <div style="color:#6b7280;font-size:11px;margin-top:2px">{pct(kpi.get("powerfm_dau",0))} 점유</div>
    </div>
    <div style="background:#181c24;border-radius:8px;padding:14px 18px;flex:1;min-width:120px;border-top:2px solid #ec4899">
      <div style="color:#6b7280;font-size:10px;margin-bottom:4px">러브FM</div>
      <div style="color:#e2e8f0;font-size:22px;font-weight:700">{fmt(kpi.get("lovefm_dau",0))}</div>
      <div style="color:#6b7280;font-size:11px;margin-top:2px">{pct(kpi.get("lovefm_dau",0))} 점유</div>
    </div>
  </div>

  <!-- 브리핑 본문 -->
  <div style="padding:24px 32px;color:#333;font-size:14px;line-height:1.8">
    {body_html}
  </div>

  <!-- 푸터 -->
  <div style="background:#f1f1f1;padding:16px 32px;text-align:center">
    <div style="color:#999;font-size:11px">자동 생성: RAAS (Radio Analysis AI System) · SBS 고릴라</div>
    <div style="color:#bbb;font-size:10px;margin-top:4px">Powered by Claude AI · Splunk</div>
  </div>
</div>
</body></html>"""


def send_email(subject: str, html_body: str, recipients: list) -> bool:
    """Gmail SMTP로 이메일 발송"""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"]    = Header(GMAIL_ADDRESS, "utf-8")
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PW)
            server.send_message(msg)  # UTF-8 자동 처리
        return True
    except smtplib.SMTPAuthenticationError:
        print("❌ Gmail 인증 실패 — 앱 비밀번호를 확인하세요")
        print("   발급 방법: https://myaccount.google.com/apppasswords")
        return False
    except Exception as e:
        import traceback; traceback.print_exc(); print(f"❌ 이메일 발송 오류: {e}")
        return False


def run(target_date: str = None, test_mode: bool = False):
    if not target_date:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    if test_mode:
        print("테스트 이메일 발송 중...")
        html = f"""<div style="font-family:sans-serif;padding:20px">
            <h2 style="color:#e8a838">RAAS 테스트 이메일</h2>
            <p>발송 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>이메일 설정이 정상입니다.</p>
        </div>"""
        ok = send_email("[RAAS 테스트] 이메일 발송 확인", html, EMAIL_TO)
        print("✅ 테스트 이메일 발송 완료" if ok else "❌ 발송 실패")
        return

    print(f"[{target_date}] 브리핑 데이터 조회 중...")
    rows = splunk_search(
        f'| inputlookup raas_briefing_latest.csv | search report_date="{target_date}" | head 1'
    )
    if not rows:
        # 최신 데이터로 시도
        rows = splunk_search(
            "| inputlookup raas_briefing_latest.csv | head 1"
        )

    if not rows:
        print("❌ 브리핑 데이터 없음 — raas_morning_brief.py --save 를 먼저 실행하세요")
        return

    kpi = rows[0]
    briefing = kpi.get("briefing", "")
    report_date = kpi.get("report_date", target_date)

    print(f"  데이터 확인: {report_date} / DAU {kpi.get('total_dau','?')}")

    html = briefing_to_html(briefing, report_date, kpi)
    subject = EMAIL_SUBJECT.format(date=report_date)

    print(f"이메일 발송 중 → {', '.join(EMAIL_TO)}")
    ok = send_email(subject, html, EMAIL_TO)
    if ok:
        print(f"✅ 이메일 발송 완료: {subject}")
    else:
        print("❌ 발송 실패")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RAAS 브리핑 이메일 발송")
    parser.add_argument("--date", help="날짜 YYYY-MM-DD")
    parser.add_argument("--test", action="store_true", help="테스트 이메일 발송")
    args = parser.parse_args()
    run(args.date, args.test)