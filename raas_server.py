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
                             set_ip_user, delete_ip_user, get_all_ip_users,
                             save_feedback, get_all_history)
from raas_auth import (register_user, authenticate, create_session,
                       resolve_session, destroy_session,
                       get_pending_users, list_users, approve_user, reject_user,
                       bootstrap_admins, ALLOWED_ROLES,
                       update_profile, change_password)
from raas_onboarding import list_active_profiles, build_suggestions
import raas_storyline_engine as STORY
import raas_report_engine as REPORT
from raas_querymap import (stats_overview, stats_by_role,
                           stats_by_user, stats_topics,
                           stats_role_metric_matrix,
                           build_graph, interest_map)
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
# PostHog (4주 PoC 측정) — 공개 키이므로 브라우저 노출 OK
POSTHOG_KEY       = os.getenv("POSTHOG_KEY", "")
POSTHOG_HOST      = os.getenv("POSTHOG_HOST", "https://eu.posthog.com")
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

def _supplement_timeline_from_csv(timeline: dict) -> None:
    """Splunk 룩업에 없는 필드를 로컬 CSV로 보완 (react_week, react_rate_week 등).
    날짜가 일치하는 행은 직접 보완하고, 로컬 CSV의 마지막 날짜 이후 Splunk 행은
    마지막 로컬 행 값을 근사치로 사용 (weekly 집계 등 느리게 변하는 지표 대상)."""
    _base = os.path.dirname(os.path.abspath(__file__))
    _candidates = [
        os.path.join(_base, 'data', 'raas_kpi_latest.csv'),
        os.path.join(_base, 'raas_kpi_latest.csv'),
    ]
    local_path = next((p for p in _candidates if os.path.exists(p)), None)
    if not local_path:
        return
    try:
        import pandas as pd
        df = pd.read_csv(local_path, dtype=str, keep_default_na=False)
        # code → {date → row} mapping
        local_by_code: dict = {}
        for r in df.to_dict(orient='records'):
            code = r.get('PGM_CODE')
            date = r.get('DATE')
            if code and date:
                local_by_code.setdefault(code, {})[date] = r
        # 1) 날짜 일치 행 직접 보완 ('0' 도 미계산으로 간주)
        def _missing(v): return not v or v in ('0', '0.0', '.0')
        for code, date_map in local_by_code.items():
            if code not in timeline:
                continue
            for date, local_row in date_map.items():
                if date not in timeline[code]:
                    continue
                splunk_row = timeline[code][date]
                for k, v in local_row.items():
                    if v and _missing(splunk_row.get(k, '')):
                        splunk_row[k] = v
        # 2) 로컬 CSV 마지막 날짜 이후 Splunk 행 → 마지막 로컬 행 값으로 보완
        # '0' 도 "미계산" 으로 간주하여 덮어쓴다 (weekly/monthly 집계는 0이면 미계산)
        _FORWARD_FIELDS = {
            'react_week', 'react_week_prev', 'react_week_chg', 'react_week_share',
            'react_rate_week', 'react_rate_week_prev', 'react_rate_week_diff',
            'react_mon', 'react_mon_prev', 'react_mon_chg', 'react_mon_share',
            'react_rate_mon', 'react_rate_mon_prev', 'react_rate_mon_diff',
        }
        for code, date_map in local_by_code.items():
            if code not in timeline:
                continue
            last_local_date = max(date_map.keys())
            last_local_row = date_map[last_local_date]
            for d, splunk_row in timeline[code].items():
                if d <= last_local_date:
                    continue
                for k in _FORWARD_FIELDS:
                    v = last_local_row.get(k, '')
                    if v and _missing(splunk_row.get(k, '')):
                        splunk_row[k] = v
    except Exception as e:
        print(f"  [supplement] local CSV merge failed: {e}")

def get_cached_timeline():
    cached = cache_get("timeline")
    if cached:
        return cached
    timeline, source = QE._load_timeline(splunk_search)
    if source == 'splunk':
        _supplement_timeline_from_csv(timeline)
    cache_set("timeline", timeline)
    cache_set("timeline_source", source)
    return timeline

def get_timeline_source() -> str:
    return cache_get("timeline_source") or "unknown"


def get_cached_anomalies() -> list:
    """s7_anomalies['alerts']만 별도 캐시 (5분 TTL).
    /api/briefing과 /api/suggestions가 같은 결과를 공유 — 중복 계산 방지."""
    cached = cache_get("anomalies")
    if cached is not None:
        return cached
    try:
        tl = get_cached_timeline()
        brief = QE.collect_briefing_data(tl)
        alerts = (brief.get('s7_anomalies') or {}).get('alerts', []) or []
    except Exception as e:
        print(f"[anomalies] 캐시 빌드 실패: {e}", flush=True)
        alerts = []
    cache_set("anomalies", alerts)
    return alerts
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

_CACHE_HEADERS = {
    "Content-Type": "application/json",
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "prompt-caching-2024-07-31",
    "x-api-key": ANTHROPIC_API_KEY,
}

def _system_block(system: str) -> list:
    """시스템 프롬프트를 캐시 가능한 content block 배열로 변환."""
    return [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]

def call_claude(system: str, user: str) -> tuple:
    """(answer_text, usage_dict) 반환."""
    payload = json.dumps({
        "model": CLAUDE_MODEL, "max_tokens": 1500,
        "system": _system_block(system),
        "messages": [{"role": "user", "content": user}]
    }).encode()
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages", data=payload,
                headers=_CACHE_HEADERS)
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read())
                return body["content"][0]["text"], body.get("usage", {})
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
    """Claude API 스트리밍 — text 청크를 yield, 마지막에 usage dict를 yield."""
    payload = json.dumps({
        "model": CLAUDE_MODEL, "max_tokens": max_tokens,
        "stream": True,
        "system": _system_block(system),
        "messages": [{"role": "user", "content": user}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers=_CACHE_HEADERS)
    input_tokens = output_tokens = cache_creation_tokens = cache_read_tokens = None
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
                etype = ev.get("type")
                if etype == "message_start":
                    u = ev.get("message", {}).get("usage", {})
                    input_tokens          = u.get("input_tokens")
                    cache_creation_tokens = u.get("cache_creation_input_tokens")
                    cache_read_tokens     = u.get("cache_read_input_tokens")
                elif etype == "content_block_delta":
                    text = ev.get("delta", {}).get("text", "")
                    if text:
                        yield text
                elif etype == "message_delta":
                    output_tokens = ev.get("usage", {}).get("output_tokens")
            except json.JSONDecodeError:
                pass
    yield {"_usage": {
        "input_tokens":          input_tokens,
        "output_tokens":         output_tokens,
        "cache_creation_tokens": cache_creation_tokens,
        "cache_read_tokens":     cache_read_tokens,
    }}

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

    def _get_session_token(self) -> str:
        """Authorization: Bearer <token> 헤더에서 토큰만 추출. 없으면 ''."""
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:].strip()
        return ""

    def _get_session_user(self):
        """현재 세션 사용자 dict. 토큰 무효/만료/미승인이면 None."""
        return resolve_session(self._get_session_token())

    def _require_admin(self):
        """관리자 검증. 관리자 아니면 401/403 응답 후 None 리턴.
        호출 측은 None이면 즉시 return."""
        user = self._get_session_user()
        if not user:
            self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
            return None
        if not user.get("is_admin"):
            self.send_json({"ok": False, "error": "관리자 권한이 필요합니다."}, 403)
            return None
        return user

    def _require_stats_viewer(self):
        """통계 열람 권한 — is_admin OR role in {'총괄관리','데이터'}."""
        user = self._get_session_user()
        if not user:
            self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
            return None
        if user.get("is_admin") or user.get("role") in ("총괄관리", "데이터"):
            return user
        self.send_json({"ok": False, "error": "통계 열람 권한이 없습니다."}, 403)
        return None

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
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-User-Id, Authorization")
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

        elif self.path.startswith("/api/query/history/all"):
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                limit  = int(params.get("limit", 50))
                offset = int(params.get("offset", 0))
                days   = int(params.get("days", 0))
                result = get_all_history(limit=limit, offset=offset, days=days)
                self.send_json({"ok": True, **result})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/query/history"):
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
                    return
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                limit = int(params.get("limit", 20))
                items = get_history(user_id=str(user["id"]), limit=limit)
                self.send_json({"ok": True, "user_id": user["id"], "items": items})
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
            if not self._require_admin():
                return
            try:
                items = get_all_ip_users()
                self.send_json({"ok": True, "your_ip": self._get_client_ip(), "items": items})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/me":
            # 세션 토큰 → 현재 사용자. 없으면 ok=False (401 아님 — 게이트는 클라이언트가 판단)
            user = self._get_session_user()
            if user:
                self.send_json({"ok": True, "user": user})
            else:
                self.send_json({"ok": False, "error": "세션 없음"}, 401)

        elif self.path.startswith("/api/my/interest-map"):
            # 본인 관심 맵 — 모든 로그인 사용자, 본인 데이터만
            user = self._get_session_user()
            if not user:
                self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
                return
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                days = int(params.get("days", 30))
                # user_id는 항상 세션에서 — 위조 불가능
                self.send_json({"ok": True,
                                "data": interest_map(user["id"], days=days)})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/admin/users":
            admin = self._require_admin()
            if not admin:
                return
            try:
                self.send_json({
                    "ok": True,
                    "pending": get_pending_users(),
                    "all":     list_users(),
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/admin/stats/"):
            # 통계 열람 — is_admin OR role in {'총괄관리','데이터'}
            viewer = self._require_stats_viewer()
            if not viewer:
                return
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                days  = int(params.get("days", 30))
                limit = int(params.get("limit", 50))
                sub = self.path.split("?", 1)[0].replace("/api/admin/stats/", "")
                if   sub == "overview":
                    self.send_json({"ok": True, "data": stats_overview(days=days)})
                elif sub == "by-role":
                    self.send_json({"ok": True, "data": stats_by_role(days=days)})
                elif sub == "by-user":
                    self.send_json({"ok": True,
                                    "data": stats_by_user(days=days, limit=limit),
                                    "me": viewer.get("id")})
                elif sub == "topics":
                    self.send_json({"ok": True, "data": stats_topics(days=days, limit=limit)})
                elif sub == "heatmap":
                    dim = params.get("dimension", "metric")
                    self.send_json({"ok": True,
                                    "data": stats_role_metric_matrix(days=days, dimension=dim, top_n=10)})
                elif sub == "graph":
                    self.send_json({"ok": True, "data": build_graph(days=days)})
                else:
                    self.send_json({"ok": False, "error": "지원하지 않는 통계 종류"}, 404)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/programs":
            # 관심 프로그램 멀티셀렉트용 — 채널별 그룹화
            try:
                from raas_onto import get_adapter
                progs = get_adapter().get_all_programs()
                # 채널별 그룹 (F00·L00·G00·P00). channel 없는 항목은 'misc'로.
                grouped = {}
                for p in progs:
                    ch       = (p.get('channel') or {})
                    ch_code  = ch.get('code') or 'misc'
                    ch_label = ch.get('label') or '기타'
                    g = grouped.setdefault(ch_code, {'code': ch_code, 'label': ch_label, 'programs': []})
                    g['programs'].append({
                        'code':  p.get('code'),
                        'label': p.get('label') or p.get('code'),
                        'time':  (p.get('time_slot') or {}).get('start') or '',
                    })
                # 채널 순서 보정 (F00, L00, G00, P00, misc)
                _order = ['F00', 'L00', 'G00', 'P00', 'misc']
                channels = [grouped[c] for c in _order if c in grouped]
                # 각 채널 내 프로그램은 방송 시작시간 순
                for g in channels:
                    g['programs'].sort(key=lambda x: (x['time'] or '99:99', x['code']))
                self.send_json({"ok": True, "channels": channels})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/profiles":
            # 가입/프로필 드롭다운용 — active=true만. 인증 불필요.
            try:
                self.send_json({"ok": True, "profiles": list_active_profiles()})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/suggestions":
            # 직무 기반 추천 칩 — 세션 토큰의 role/user_id로 build. 미인증이어도 graceful.
            # 개인화(gap·peer·similar)는 build_suggestions가 user_id로 recommend_for_user를 호출.
            try:
                user = self._get_session_user()
                role = (user or {}).get("role")
                uid  = (user or {}).get("id")
                anomalies = get_cached_anomalies()
                chips = build_suggestions(role, anomalies, N=6, user_id=uid)
                self.send_json({"ok": True, "role": role, "chips": chips})
            except Exception as e:
                # 어떤 오류든 빈 칩으로 graceful — 채팅 흐름 보호
                print(f"[suggestions] 실패: {e}", flush=True)
                self.send_json({"ok": True, "role": None, "chips": []})

        elif self.path == "/api/posthog-config":
            # 브라우저에 노출되는 공개 클라이언트 설정. 키 없으면 enabled=false → SDK init 스킵.
            self.send_json({
                "ok": True,
                "enabled": bool(POSTHOG_KEY),
                "key":     POSTHOG_KEY,
                "host":    POSTHOG_HOST,
            })

        elif self.path == "/api/storyline/role-detect":
            # 현재 로그인 사용자 → 스토리라인 직무 ID + 필요 셋업 안내
            try:
                user = self._get_session_user()
                result = STORY.role_detect(user)
                result["available_roles"] = STORY.list_available_roles()
                self.send_json({"ok": True, **result})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/storyline/entry"):
            # 첫 화면 — greeting + first_question + default chips
            try:
                params = {}
                if "?" in self.path:
                    params = dict(urllib.parse.parse_qsl(self.path.split("?", 1)[1]))
                user = self._get_session_user()
                # role: 쿼리 파라미터 우선, 없으면 사용자 role에서 자동 감지
                role = params.get("role")
                if not role:
                    detected = STORY.role_detect(user)
                    role = detected.get("role")
                if not role:
                    self.send_json({"ok": False,
                                    "error": "role이 지정되지 않았고 사용자에서도 감지 실패",
                                    "available_roles": STORY.list_available_roles()}, 400)
                    return
                # channel override (CP 한정)
                channel = params.get("channel")
                engine = STORY.StorylineEngine(role=role, user=user,
                                               channel_override=channel)
                self.send_json(engine.entry())
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path.startswith("/api/storyline/exports/"):
            # 일회용 토큰으로 산출물(PPT/텍스트) 다운로드 (D-014)
            try:
                token = self.path.split("/api/storyline/exports/", 1)[1].strip()
                token = token.split("?", 1)[0].split("#", 1)[0]
                result = REPORT.fetch_for_download(token)
                if not result:
                    self.send_response(404)
                    self.end_headers()
                    return
                body_bytes = result["bytes"]
                self.send_response(200)
                self.send_header("Content-Type", result.get("content_type", "application/octet-stream"))
                self.send_header("Content-Length", str(len(body_bytes)))
                self.send_header(
                    "Content-Disposition",
                    f"attachment; filename*=UTF-8''{urllib.parse.quote(result['filename'])}",
                )
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body_bytes)
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

        if self.path == "/api/register":
            try:
                result = register_user(
                    login_id=body.get("login_id", ""),
                    pw=body.get("password", ""),
                    name=body.get("name", ""),
                    role=body.get("role", ""),
                )
                if not result.get("ok"):
                    self.send_json(result, 400)
                    return
                # pending이면 로그인 안내. approved(부트스트랩)면 즉시 세션 발급.
                if result.get("status") == "approved":
                    token = create_session(result["user_id"])
                    self.send_json({"ok": True, "status": "approved",
                                    "token": token, "user_id": result["user_id"],
                                    "is_admin": result.get("is_admin", False)})
                else:
                    self.send_json({"ok": True, "status": "pending",
                                    "user_id": result["user_id"]})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        elif self.path == "/api/login":
            try:
                login_id = body.get("login_id", "").strip()
                pw       = body.get("password", "")
                if not login_id or not pw:
                    self.send_json({"ok": False, "error": "login_id·password 필수"}, 400)
                    return
                user = authenticate(login_id, pw)
                if not user:
                    self.send_json({"ok": False, "error": "아이디 또는 비밀번호가 올바르지 않습니다."}, 401)
                    return
                if user["status"] != "approved":
                    self.send_json({"ok": False, "error": "승인 대기 중",
                                    "status": user["status"]}, 403)
                    return
                token = create_session(user["id"])
                # user dict는 authenticate()가 my_programs 포함해서 반환
                self.send_json({"ok": True, "token": token, "user": user})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        elif self.path == "/api/logout":
            try:
                destroy_session(self._get_session_token())
                self.send_json({"ok": True})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        elif self.path == "/api/me/update":
            user = self._get_session_user()
            if not user:
                self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
                return
            try:
                result = update_profile(
                    user_id     = user['id'],
                    name        = body.get('name'),
                    role        = body.get('role'),
                    my_programs = body.get('my_programs'),
                )
                if not result.get('ok'):
                    self.send_json(result, 400)
                    return
                self.send_json(result)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        elif self.path == "/api/me/password":
            user = self._get_session_user()
            if not user:
                self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
                return
            try:
                result = change_password(
                    user_id = user['id'],
                    old_pw  = body.get('old_password', ''),
                    new_pw  = body.get('new_password', ''),
                )
                self.send_json(result, 200 if result.get('ok') else 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        elif self.path == "/api/admin/approve":
            admin = self._require_admin()
            if not admin:
                return
            try:
                action = body.get("action", "approve")
                uid    = int(body.get("user_id", 0))
                if not uid:
                    self.send_json({"ok": False, "error": "user_id 필수"}, 400)
                    return
                if action == "approve":
                    ok = approve_user(uid, admin["id"])
                elif action == "reject":
                    ok = reject_user(uid)
                else:
                    self.send_json({"ok": False, "error": "action은 approve|reject"}, 400)
                    return
                self.send_json({"ok": ok, "user_id": uid, "action": action})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        if self.path == "/api/ip-users":
            if not self._require_admin():
                return
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

        elif self.path == "/api/query/feedback":
            try:
                query_id = body.get("id")
                feedback = body.get("feedback")
                if query_id is None or feedback not in (1, -1):
                    self.send_json({"ok": False, "error": "id와 feedback(1/-1) 필요"}, 400)
                    return
                ok = save_feedback(int(query_id), int(feedback))
                self.send_json({"ok": ok})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/query/stream":
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
                    return
                question    = body.get("question", "")
                target_date = body.get("date", None)
                user_id     = str(user["id"])
                user_name   = user["name"]
                user_role   = user["role"]
                ip          = self._get_client_ip()

                if not question:
                    self.send_json({"ok": False, "error": "질문이 없습니다"}, 400)
                    return
                if not QUERY_ENGINE_AVAILABLE:
                    self.send_json({"ok": False, "error": "Query engine unavailable"}, 500)
                    return

                timeline = get_cached_timeline()
                sys_prompt, ctx, max_tok, chart_data, facts = QE.query_with_timeline_stream(
                    question, timeline, target_date=target_date
                )
                if sys_prompt is None:
                    self.send_json({"ok": False, "error": "데이터를 사용할 수 없습니다."}, 500)
                    return
                facts = facts or {}

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
                usage = {}
                for chunk in call_claude_stream(sys_prompt, ctx, max_tokens=max_tok):
                    if isinstance(chunk, dict) and "_usage" in chunk:
                        usage = chunk["_usage"]
                    else:
                        full_text.append(chunk)
                        sse({"type": "token", "text": chunk})
                answer = "".join(full_text)
                query_id = save_query(user_id, question, answer, chart_data=chart_data,
                                      ip=ip, user_name=user_name, user_role=user_role,
                                      input_tokens=usage.get("input_tokens"),
                                      output_tokens=usage.get("output_tokens"),
                                      cache_creation_tokens=usage.get("cache_creation_tokens"),
                                      cache_read_tokens=usage.get("cache_read_tokens"),
                                      intent=facts.get("intent"),
                                      scope=facts.get("scope"),
                                      scope_keyword=facts.get("scope_keyword"),
                                      metric=facts.get("metric"),
                                      metrics=facts.get("metrics"),
                                      topic_key=facts.get("topic_key"))
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
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401)
                    return
                question    = body.get("question", "")
                context     = body.get("context", "")
                target_date = body.get("date", None)
                user_id     = str(user["id"])
                user_name   = user["name"]
                user_role   = user["role"]
                ip          = self._get_client_ip()

                if not question:
                    self.send_json({"ok": False, "error": "질문이 없습니다"}, 400)
                    return

                in_tok = out_tok = None
                facts = {}
                if QUERY_ENGINE_AVAILABLE:
                    timeline = get_cached_timeline()
                    result = QE.query_with_timeline(
                        question, timeline,
                        target_date=target_date,
                    )
                    answer     = result["answer"]
                    chart_data = result.get("chart_data")
                    in_tok     = result.get("input_tokens")
                    out_tok    = result.get("output_tokens")
                    facts      = result.get("facts") or {}
                else:
                    # QE 로드 실패 시 간단 fallback — facts 없이 graceful 저장
                    enriched_context = build_query_context(question, context)
                    answer, usage = call_claude(
                        QUERY_SYSTEM_PROMPT,
                        f"데이터:\n{enriched_context}\n\n질문: {question}"
                    )
                    chart_data = None
                    in_tok  = usage.get("input_tokens")
                    out_tok = usage.get("output_tokens")

                query_id = save_query(user_id, question, answer, chart_data=chart_data,
                                      ip=ip, user_name=user_name, user_role=user_role,
                                      input_tokens=in_tok, output_tokens=out_tok,
                                      intent=facts.get("intent"),
                                      scope=facts.get("scope"),
                                      scope_keyword=facts.get("scope_keyword"),
                                      metric=facts.get("metric"),
                                      metrics=facts.get("metrics"),
                                      topic_key=facts.get("topic_key"))
                self.send_json({"ok": True, "answer": answer,
                                "query_id": query_id, "chart_data": chart_data})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/storyline/advance":
            # 칩 클릭 → 다음 슬롯 답변 렌더링
            try:
                user = self._get_session_user()
                role = body.get("role")
                if not role:
                    detected = STORY.role_detect(user)
                    role = detected.get("role")
                if not role:
                    self.send_json({"ok": False,
                                    "error": "role이 필요합니다.",
                                    "available_roles": STORY.list_available_roles()}, 400)
                    return
                slot_from = body.get("slot_from", "entry")
                chip_intent = body.get("chip_intent")
                if not chip_intent:
                    self.send_json({"ok": False,
                                    "error": "chip_intent가 필요합니다."}, 400)
                    return
                channel = body.get("channel")
                prev_context = body.get("prev_context") or {}
                engine = STORY.StorylineEngine(role=role, user=user,
                                               channel_override=channel)
                result = engine.advance(slot_from, chip_intent,
                                        prev_context=prev_context)
                status = 200 if result.get("ok") else 400
                self.send_json(result, status)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/storyline/export":
            # 산출물 생성 — matplotlib(차트 PNG) + python-pptx(PPT 1장) (D-014)
            try:
                role = body.get("role")
                if not role:
                    user = self._get_session_user()
                    detected = STORY.role_detect(user)
                    role = detected.get("role")
                if not role:
                    self.send_json({"ok": False, "error": "role이 필요합니다."}, 400)
                    return
                output_format_id = body.get("output_format_id", "")
                slots_visited = body.get("slots_visited") or []
                context = body.get("context") or {}
                result = REPORT.build_storyline_export(
                    role=role,
                    output_format_id=output_format_id,
                    slots_visited=slots_visited,
                    context=context,
                )
                if not result.get("ok"):
                    self.send_json(result, 400)
                    return
                token = REPORT.store_for_download(result)
                self.send_json({
                    "ok": True,
                    "filename": result["filename"],
                    "content_type": result["content_type"],
                    "kind": result.get("kind"),
                    "size": len(result["bytes"]),
                    "download_url": f"/api/storyline/exports/{token}",
                    "message": f"{result['filename']} 생성 완료",
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)


if __name__ == "__main__":
    init_db()
    bootstrap_admins()  # .env ADMIN_LOGIN_IDS 기반 관리자 자동 승격
    server = ThreadingHTTPServer(("0.0.0.0", PORT), RAASHandler)
    server.daemon_threads = True
    print(f"RAAS Local Server started: http://localhost:{PORT}  (Ctrl+C to quit)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버 종료")
        server.shutdown()
