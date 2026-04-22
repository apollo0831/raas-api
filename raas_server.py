"""
RAAS Local Proxy Server
브라우저 CORS 문제 해결 - Python이 Splunk/Claude API 중간 프록시 역할

사용법:
  python raas_server.py
  브라우저에서: http://localhost:5000

포트 변경: PORT = 5000 수정
"""

import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import raas_query_engine as QE
    QUERY_ENGINE_AVAILABLE = True
except ImportError:
    QUERY_ENGINE_AVAILABLE = False
import urllib.request
import urllib.parse
import urllib.error
import base64
import ssl
import sys
import os
import threading
from dotenv import load_dotenv
import os
load_dotenv()
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
import raas_briefing_engine as BE

# ── 설정 ─────────────────────────────────────────────
PORT            = 5000
SPLUNK_HOST       = os.getenv("SPLUNK_HOST")
SPLUNK_USER       = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD   = os.getenv("SPLUNK_PASSWORD")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL")
SPLUNK_APP        = os.getenv("SPLUNK_APP")        # ← Splunk 앱 내부 ID
# ─────────────────────────────────────────────────────

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def splunk_auth():
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()

def splunk_search(spl: str) -> list:
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", splunk_auth())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
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
        raise Exception(f"Splunk {e.code}: {e.read().decode()[:200]}")
    except Exception as e:
        raise Exception(f"Splunk 오류: {e}")

def call_claude(system: str, user: str) -> str:
    payload = json.dumps({
        "model": CLAUDE_MODEL, "max_tokens": 800,
        "system": system,
        "messages": [{"role": "user", "content": user}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={"Content-Type": "application/json",
                 "anthropic-version": "2023-06-01",
                 "x-api-key": ANTHROPIC_API_KEY})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["content"][0]["text"]

# HTML 파일 경로 (서버와 같은 폴더)
HTML_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raas_web.html")

class RAASHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {format % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, content):
        body = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            if os.path.exists(HTML_FILE):
                with open(HTML_FILE, "r", encoding="utf-8") as f:
                    self.send_html(f.read())
            else:
                self.send_html("<h2>raas_web.html 파일을 같은 폴더에 두세요</h2>")

        elif self.path == "/api/briefing":
            try:
                # 전체 데이터 수집
                data = BE.collect_all(splunk_search)

                # Claude 인사이트 생성
                context = data.get("claude_context", "")
                s7_alerts = data.get("s7_anomalies", {}).get("alerts", [])
                alert_text = "\\n".join(a["msg"] for a in s7_alerts)

                claude_prompt = f"""다음은 오늘 SBS 고릴라 라디오 앱의 핵심 지표입니다.

{context}

위 데이터를 바탕으로 아래 4가지를 간결하게 작성하세요 (전체 400자 이내):

01 / 핵심지표 요약 (3줄: DAU·깊은청취율·채널 현황)
02 / 주목할 점 (2줄: 오늘 특이사항, 전주 대비 변화)
03 / 프로그램 하이라이트 (2줄: 1위 프로그램과 주목 프로그램)
04 / 액션 추천 (1줄: 오늘 가장 중요한 조치 1가지)"""

                brief_text = call_claude(
                    "SBS 고릴라 라디오 앱 데이터 분석 어시스턴트. 한국어로 간결하게. 수치는 천단위 쉼표.",
                    claude_prompt
                )

                self.send_json({
                    "ok":   True,
                    "data": {**data, "brief": brief_text}
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/top_programs":
            try:
                rows = splunk_search(
                    "| inputlookup raas_top_programs_latest.csv | sort rank | head 10")
                self.send_json({"ok": True, "data": rows})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/status":
            self.send_json({"ok": True, "server": "RAAS",
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

        if self.path == "/api/query":
            try:
                question    = body.get("question", "")
                context     = body.get("context", "")
                target_date = body.get("date", None)

                if not question:
                    self.send_json({"ok": False, "error": "질문이 없습니다"}, 400)
                    return

                if QUERY_ENGINE_AVAILABLE:
                    answer = QE.query(question, target_date=target_date)
                else:
                    # 컨텍스트가 없으면 최신 브리핑 데이터 사용
                    if not context:
                        try:
                            bd = BE.collect_all(splunk_search)
                            context = bd.get("claude_context", "")
                        except:
                            pass

                    answer = call_claude(
                        "SBS 고릴라 라디오 앱 데이터 분석 어시스턴트. 한국어로 간결하게 답하세요. 수치는 천단위 쉼표.",
                        f"데이터:\\n{context}\\n\\n질문: {question}"
                    )
                self.send_json({"ok": True, "answer": answer})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)


if __name__ == "__main__":
    server = HTTPServer(("localhost", PORT), RAASHandler)
    print(f"""
╔══════════════════════════════════════╗
║   RAAS Local Server 시작             ║
╠══════════════════════════════════════╣
║  URL : http://localhost:{PORT}         ║
║  종료: Ctrl+C                        ║
╚══════════════════════════════════════╝
""")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버 종료")
        server.shutdown()
