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
import time
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
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from raas_history_db import (init_db, save_query, get_history, get_popular,
                             resolve_user_name, set_ip_user, delete_ip_user, get_all_ip_users)
from raas_prompts import QUERY_SYSTEM_PROMPT
from raas_briefing_context import build_query_context

# ── 설정 ─────────────────────────────────────────────
PORT            = 5000
SPLUNK_HOST       = os.getenv("SPLUNK_HOST")
SPLUNK_USER       = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD   = os.getenv("SPLUNK_PASSWORD")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL")
SPLUNK_APP        = os.getenv("SPLUNK_APP")        # ← Splunk 앱 내부 ID
SPLUNK_TIMEOUT    = int(os.getenv("SPLUNK_TIMEOUT", "10"))  # 초. 미도달 환경에서 빠른 CSV 폴백을 위해 짧게
# ─────────────────────────────────────────────────────

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

# ── 캐시 (Splunk 동시 검색 529 방지) ─────────────────────
CACHE_TTL = timedelta(minutes=5)
_cache: dict = {}
_cache_lock = threading.Lock()

def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and datetime.now() - entry["ts"] < CACHE_TTL:
            return entry["data"]
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {"data": data, "ts": datetime.now()}

def get_cached_timeline():
    cached = cache_get("timeline")
    if cached:
        return cached
    timeline, source = QE._load_timeline(splunk_search)
    cache_set("timeline", timeline)
    cache_set("timeline_source", source)
    return timeline

def get_timeline_source() -> str:
    return cache_get("timeline_source") or "unknown"
# ──────────────────────────────────────────────────────────

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
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=SPLUNK_TIMEOUT) as resp:
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
        "model": CLAUDE_MODEL, "max_tokens": 1500,
        "system": system,
        "messages": [{"role": "user", "content": user}]
    }).encode()
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages", data=payload,
                headers={"Content-Type": "application/json",
                         "anthropic-version": "2023-06-01",
                         "x-api-key": ANTHROPIC_API_KEY})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read())["content"][0]["text"]
        except urllib.error.HTTPError as e:
            if e.code == 529:
                print(f"  ⚠️ Claude API 529 (과부하) — 3초 후 재시도 ({attempt + 1}/3)")
                if attempt < 2:
                    time.sleep(3)
            else:
                raise Exception(f"Claude API {e.code}: {e.read().decode()[:200]}")
        except Exception as e:
            raise Exception(f"Claude API 오류: {e}")
    raise Exception("Claude API 일시 과부하. 잠시 후 다시 시도해 주세요.")

def call_claude_stream(system: str, user: str, max_tokens: int = 1000):
    """Claude API 스트리밍 — text 청크를 yield."""
    payload = json.dumps({
        "model": CLAUDE_MODEL, "max_tokens": max_tokens,
        "stream": True,
        "system": system,
        "messages": [{"role": "user", "content": user}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={"Content-Type": "application/json",
                 "anthropic-version": "2023-06-01",
                 "x-api-key": ANTHROPIC_API_KEY})
    with urllib.request.urlopen(req, timeout=90) as resp:
        for raw in resp:
            line = raw.decode("utf-8").rstrip("\n\r")
            if not line.startswith("data: "):
                continue
            chunk = line[6:]
            if chunk == "[DONE]":
                break
            try:
                ev = json.loads(chunk)
                if ev.get("type") == "content_block_delta":
                    text = ev.get("delta", {}).get("text", "")
                    if text:
                        yield text
            except json.JSONDecodeError:
                pass

# HTML 파일 경로 (서버와 같은 폴더)
HTML_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raas_web.html")

class RAASHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {format % args}")

    def _get_client_ip(self) -> str:
        """ngrok/프록시 환경은 X-Forwarded-For 우선, 직접 접속은 client_address."""
        forwarded = self.headers.get("X-Forwarded-For", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return self.client_address[0]

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
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-User-Id")
        self.end_headers()

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            if os.path.exists(HTML_FILE):
                with open(HTML_FILE, "r", encoding="utf-8") as f:
                    self.send_html(f.read())
            else:
                self.send_html("<h2>raas_web.html 파일을 같은 폴더에 두세요</h2>")

        elif self.path.startswith("/api/timeseries/program/"):
            try:
                parts = self.path.split("?", 1)
                code = parts[0].replace("/api/timeseries/program/", "").strip().upper()
                params = dict(urllib.parse.parse_qsl(parts[1])) if len(parts) > 1 else {}
                metric = params.get("metric", "dau_today")
                days   = int(params.get("days", 30))
                if not code:
                    self.send_json({"ok": False, "error": "프로그램 코드가 필요합니다"}, 400)
                    return
                timeline = get_cached_timeline()
                if code not in timeline:
                    self.send_json({"ok": False, "error": f"코드 '{code}' 데이터 없음"}, 404)
                    return
                trend = QE.get_metric_trend(timeline, code, metric, days=days)
                latest_row = timeline[code].get(max(timeline[code].keys()), {})
                self.send_json({
                    "ok": True, "code": code,
                    "name": latest_row.get("PGM_NAME", "") or QE._pgm_name(code),
                    "metric": metric, "days": days,
                    "data": [{"date": d, "value": v} for d, v in trend]
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/snapshot/"):
            try:
                target_date = self.path.replace("/api/snapshot/", "").strip().replace("-", "/")
                if not target_date:
                    self.send_json({"ok": False, "error": "날짜가 필요합니다 (YYYY/MM/DD)"}, 400)
                    return
                timeline = get_cached_timeline()
                snapshot = QE.get_snapshot_at(timeline, target_date)
                if not snapshot:
                    self.send_json({
                        "ok": False, "error": f"'{target_date}' 데이터 없음",
                        "available_dates": QE.get_available_dates(timeline)
                    }, 404)
                    return
                result = {}
                for code, row in snapshot.items():
                    result[code] = {
                        "code": code,
                        "name": row.get("PGM_NAME", "") or QE._pgm_name(code),
                        "dau": QE._i(row.get("dau_today")),
                        "dau_wow": QE._fn(row.get("dau_wow")),
                        "dau_week": QE._i(row.get("dau_week")),
                        "dau_mon": QE._i(row.get("dau_mon")),
                        "deep_rate": QE._fn(row.get("deep_rate")),
                        "engage_rate": QE._fn(row.get("engage_rate")),
                        "habit_rate": QE._fn(row.get("habit_rate")),
                        "new_user": QE._i(row.get("new_today")),
                        "react_user": QE._i(row.get("react_today")),
                        "churn_rate": QE._fn(row.get("churn_rate"))
                    }
                self.send_json({"ok": True, "date": target_date,
                                "codes_count": len(result), "data": result})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/trend"):
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                metric   = params.get("metric", "dau_today")
                scope    = params.get("scope", "T00").upper()
                days     = int(params.get("days", 30))
                date_key = params.get("date_key") or None
                timeline = get_cached_timeline()
                if scope not in timeline:
                    self.send_json({"ok": False, "error": f"스코프 '{scope}' 데이터 없음"}, 404)
                    return
                trend = QE.get_metric_trend(timeline, scope, metric, days=days, date_field=date_key)
                self.send_json({
                    "ok": True, "scope": scope,
                    "name": QE._pgm_name(scope),
                    "metric": metric, "days": days,
                    "data": [{"date": d, "value": v} for d, v in trend]
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/timeline/meta":
            try:
                timeline = get_cached_timeline()
                dates = QE.get_available_dates(timeline)
                source = get_timeline_source()
                resp = {
                    "ok": True,
                    "source": source,
                    "codes_count": len(timeline),
                    "days_count": len(dates),
                    "date_min": dates[0] if dates else None,
                    "date_max": dates[-1] if dates else None,
                    "available_dates": dates,
                    "codes": list(timeline.keys())
                }
                if source == "csv_fallback":
                    resp["warning"] = "Splunk unreachable, using CSV snapshot"
                self.send_json(resp)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/query/popular"):
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                limit = int(params.get("limit", 5))
                days  = int(params.get("days", 7))
                items = get_popular(limit=limit, days=days)
                self.send_json({
                    "ok": True,
                    "items": items,
                    "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/query/history"):
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                user_id = (self.headers.get("X-User-Id")
                           or params.get("user_id", "").strip())
                if not user_id:
                    self.send_json({"ok": False, "error": "user_id required"}, 400)
                    return
                limit = int(params.get("limit", 20))
                items = get_history(user_id=user_id, limit=limit)
                self.send_json({"ok": True, "user_id": user_id, "items": items})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/schema":
            # Phase 5 Step 5: 온톨로지 스키마 덤프 (web.html 동적 메타지식 로드용)
            try:
                from raas_onto import get_adapter
                schema = get_adapter().get_schema_dump()
                self.send_json({"ok": True, "data": schema})
            except Exception as e:
                self.send_json({"ok": False, "error": {
                    "code": "ONTOLOGY_LOAD_FAILED",
                    "message": str(e),
                    "fallback_available": True,
                }}, 503)

        elif self.path.startswith("/api/concept/search"):
            # Phase 5 Step 5: 키워드 자동완성/검색 (?q=...)
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                q = params.get("q", "").strip()
                if not q:
                    self.send_json({"ok": True, "query": "", "matches": []})
                    return
                from raas_onto import get_adapter
                matches = get_adapter().find_program_by_keyword(q)
                self.send_json({"ok": True, "query": q, "matches": matches})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/concept/"):
            # Phase 5 Step 5: 개념 정의 조회 (?) 모달 동적화
            try:
                name = self.path.replace("/api/concept/", "").strip()
                if not name:
                    self.send_json({"ok": False, "error": "concept name 필요"}, 400)
                    return
                from raas_onto import get_adapter
                concept = get_adapter().get_concept_definition(name)
                if not concept:
                    self.send_json({"ok": False, "error": f"개념 '{name}' 미존재"}, 404)
                    return
                self.send_json({"ok": True, "data": concept})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/briefing"):
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                timeline = get_cached_timeline()
                data = QE.collect_briefing_data(timeline)
                if 'error' in data:
                    self.send_json({"ok": False, "error": data['error']}, 503)
                    return
                self.send_json({"ok": True, "data": data})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/rawdata"):
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                code = params.get("code", "T00").upper()
                timeline = get_cached_timeline()
                date_rows = timeline.get(code)
                if date_rows is None:
                    self.send_json({"ok": False, "error": f"코드 '{code}' 없음"}, 404)
                    return
                sorted_rows = sorted(date_rows.values(),
                                     key=lambda r: r.get("DATE", ""), reverse=True)
                name = QE._pgm_name(code)
                self.send_json({"ok": True, "code": code, "name": name,
                                "count": len(sorted_rows), "rows": sorted_rows})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/ip-users":
            try:
                items = get_all_ip_users()
                self.send_json({"ok": True, "your_ip": self._get_client_ip(), "items": items})
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

        if self.path == "/api/ip-users":
            try:
                action = body.get("action", "set")
                ip     = body.get("ip", "").strip()
                if not ip:
                    self.send_json({"ok": False, "error": "ip 필요"}, 400)
                    return
                if action == "delete":
                    delete_ip_user(ip)
                    self.send_json({"ok": True, "deleted": ip})
                else:
                    name = body.get("name", "").strip()
                    if not name:
                        self.send_json({"ok": False, "error": "name 필요"}, 400)
                        return
                    set_ip_user(ip, name)
                    self.send_json({"ok": True, "ip": ip, "name": name})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/query/stream":
            try:
                question    = body.get("question", "")
                target_date = body.get("date", None)
                user_id     = (self.headers.get("X-User-Id")
                               or body.get("user_id", "anonymous") or "anonymous")
                ip          = self._get_client_ip()
                user_name   = resolve_user_name(ip)

                if not question:
                    self.send_json({"ok": False, "error": "질문이 없습니다"}, 400)
                    return
                if not QUERY_ENGINE_AVAILABLE:
                    self.send_json({"ok": False, "error": "Query engine unavailable"}, 500)
                    return

                timeline = get_cached_timeline()
                sys_prompt, ctx, max_tok, chart_data = QE.query_with_timeline_stream(
                    question, timeline, target_date=target_date
                )
                if sys_prompt is None:
                    self.send_json({"ok": False, "error": "데이터를 사용할 수 없습니다."}, 500)
                    return

                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()

                def sse(data: dict):
                    self.wfile.write(
                        ("data: " + json.dumps(data, ensure_ascii=False) + "\n\n").encode("utf-8")
                    )
                    self.wfile.flush()

                sse({"type": "meta", "chart_data": chart_data})
                full_text = []
                for chunk in call_claude_stream(sys_prompt, ctx, max_tokens=max_tok):
                    full_text.append(chunk)
                    sse({"type": "token", "text": chunk})
                answer = "".join(full_text)
                query_id = save_query(user_id, question, answer, chart_data=chart_data,
                                      ip=ip, user_name=user_name)
                sse({"type": "done", "query_id": query_id})
                self.close_connection = True  # SSE 완료 후 연결 명시적 종료
            except Exception as e:
                try:
                    self.wfile.write(
                        ("data: " + json.dumps({"type": "error", "message": str(e)}, ensure_ascii=False) + "\n\n").encode("utf-8")
                    )
                    self.wfile.flush()
                except Exception:
                    pass

        elif self.path == "/api/query":
            try:
                question    = body.get("question", "")
                context     = body.get("context", "")
                target_date = body.get("date", None)
                user_id     = (self.headers.get("X-User-Id")
                               or body.get("user_id", "anonymous") or "anonymous")
                ip          = self._get_client_ip()
                user_name   = resolve_user_name(ip)

                if not question:
                    self.send_json({"ok": False, "error": "질문이 없습니다"}, 400)
                    return

                if QUERY_ENGINE_AVAILABLE:
                    timeline = get_cached_timeline()
                    result = QE.query_with_timeline(
                        question, timeline,
                        target_date=target_date,
                    )
                    answer = result["answer"]
                    chart_data = result.get("chart_data")
                else:
                    # QE 로드 실패 시 간단 fallback
                    enriched_context = build_query_context(question, context)
                    answer = call_claude(
                        QUERY_SYSTEM_PROMPT,
                        f"데이터:\n{enriched_context}\n\n질문: {question}"
                    )
                    chart_data = None

                query_id = save_query(user_id, question, answer, chart_data=chart_data,
                                      ip=ip, user_name=user_name)
                self.send_json({"ok": True, "answer": answer,
                                "query_id": query_id, "chart_data": chart_data})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)


if __name__ == "__main__":
    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), RAASHandler)
    server.daemon_threads = True
    print(f"RAAS Local Server started: http://localhost:{PORT}  (Ctrl+C to quit)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버 종료")
        server.shutdown()
