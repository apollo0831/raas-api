"""
RAAS Local Proxy Server
브라우저 CORS 문제 해결 - Python이 Splunk/Claude API 중간 프록시 역할

사용법:
  python raas_server.py
  브라우저에서: http://localhost:5000

포트 변경: PORT = 5000 수정
"""

import json
import re
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
                             save_feedback, get_all_history,
                             add_knowledge_item, get_knowledge_items,
                             add_data_request, add_improvement, set_improvement_verdict,
                             list_improvements, list_data_requests,
                             get_knowledge_items_by_ids, review_improvement, update_data_request,
                             feedback_weakness, feedback_negative_open, knowledge_effect, loop_funnel,
                             list_approved_knowledge, retire_knowledge_item, reclassify_knowledge,
                             list_my_knowledge, retire_my_knowledge,
                             add_uploaded_data, list_my_uploads, retire_my_upload,
                             list_pending_uploads, approve_upload)
from raas_auth import (register_user, authenticate, create_session,
                       resolve_session, destroy_session,
                       get_pending_users, list_users, approve_user, reject_user,
                       bootstrap_admins, ALLOWED_ROLES,
                       update_profile, change_password)
from raas_onboarding import list_active_profiles, build_suggestions
import raas_storyline_engine as STORY
import raas_storyline_router as ROUTER
import raas_grounding as GROUND
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
# 답변 생성 max_tokens 상한(천장) — 짤림 방지. .env의 MAX_ANSWER_TOKENS로 조절(기본 8000).
MAX_ANSWER_TOKENS = int(os.getenv("MAX_ANSWER_TOKENS", "8000"))
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

def cache_clear(*keys):
    """지정 키 제거(없으면 전체 비움). Splunk 파생 캐시 강제 무효화용(수동 재적재)."""
    with _cache_lock:
        if keys:
            for k in keys:
                _cache.pop(k, None)
        else:
            _cache.clear()

# 수동 재적재 동시 실행 방지(더블클릭·중복 요청 → Splunk 이중 풀 차단)
_refresh_lock = threading.Lock()

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


# 스토리라인 엔진이 일반 채팅과 동일한 Splunk 파이프라인을 쓰도록 주입
# (CSV 직접 읽기는 더 이상 사용 X)
STORY.set_timeline_provider(get_cached_timeline)


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


def _verify_line(vr) -> str:
    """수치 검증 결과(GROUND.verify_numbers)를 관리자 푸터 한 줄로."""
    if not vr:
        return ""
    if vr.get("ok"):
        return "\n수치 검증: ✓ 근거 일치"
    ups = vr.get("unsupported") or []
    line = f"\n수치 검증: ⚠ 근거 미확인 {len(ups)}건"
    if ups:
        line += " — " + "; ".join(str(u) for u in ups[:3])
    return line


def _latest_data_date() -> str:
    """캐시된 타임라인의 최신 데이터 일자(어제 방송 기준). 'YYYY/MM/DD' 형식."""
    try:
        tl = get_cached_timeline()
        ds = sorted((tl.get("T00") or {}).keys())
        return ds[-1] if ds else ""
    except Exception as e:
        print(f"[latest_date] {e}", flush=True)
        return ""
# ──────────────────────────────────────────────────────────

def _build_data_check_text(report: dict, prefix_line: str = "") -> str:
    """데이터 점검 report → 답변 마크다운. 데이터 점검 표 + '정제 배치·스케줄 상태' 표(분리)."""
    s = report.get("summary", {})
    _emoji = {"red": "🔴", "yellow": "🟡", "green": "🟢"}
    order = {"red": 0, "yellow": 1, "green": 2}
    checks = report.get("checks", [])
    main = [c for c in checks if c.get("group") != "schedule"]
    sched = [c for c in checks if c.get("group") == "schedule"]

    def _table(rows):
        rows = sorted(rows, key=lambda c: order.get(c["severity"], 3))
        return ("| 상태 | 항목 | 상세 |\n|---|---|---|\n" + "".join(
            f"| {_emoji.get(c['severity'],'')} | {c['title']} | {(c.get('detail') or '').replace('|','/')} |\n"
            for c in rows))

    def _tally(rows):   # 표별 자체 카운트 — 종합 숫자와 표 내용이 어긋나지 않도록
        return (f"🔴 {sum(1 for c in rows if c['severity']=='red')} · "
                f"🟡 {sum(1 for c in rows if c['severity']=='yellow')} · "
                f"🟢 {sum(1 for c in rows if c['severity']=='green')}")

    head = (f"**📋 데이터 점검 · {report.get('data_date','?')}** "
            f"(스플렁크 · {report.get('code_count','?')}코드 · "
            f"{report.get('field_count','?')}필드)\n"
            f"{_tally(main)}\n\n")
    body = head + _table(main)
    if sched:
        body += (f"\n**🗓 정제 배치 · 스케줄 상태** (이상 필드별 관련 배치) · {_tally(sched)}\n\n"
                 + _table(sched))

    try:
        _fnd = "\n".join(f"[{c['severity']}] {c['title']} {c.get('detail','')}"
                         for c in checks if c["severity"] != "green")
        _sys = ("당신은 데이터 품질 점검 담당입니다. 아래 점검 결과를 1~2문장으로 총평하세요. "
                "심각(red)이 있으면 무엇이 문제인지 먼저 경고, 없으면 정상임을 간결히 안내. 군더더기 금지.")
        _u = (f"요약: red {s.get('red',0)} / yellow {s.get('yellow',0)} / green {s.get('green',0)}\n"
              f"주요 항목:\n{_fnd or '(이상 없음)'}")
        verdict, _ = QE.call_claude(_sys, _u, max_tokens=200, model=QE.HAIKU_MODEL)
    except Exception:
        verdict = (f"🔴 심각 이상 {s.get('red',0)}건 — 적재·무결성 점검 필요." if s.get("red")
                   else (f"🟡 주의 {s.get('yellow',0)}건 확인." if s.get("yellow") else "🟢 데이터 정상."))
    full = (verdict or "").strip() + "\n\n"
    if prefix_line:
        full += prefix_line + "\n\n"
    return full + body


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


def fetch_schedule_status(hours: int = 192):
    """스케줄 배치별 최신 상태 맵 조회(_internal sourcetype=scheduler, 5분 캐시).
    8일 윈도우(기본)로 주간/월간 배치까지 포착. 필드별 스케줄 상태 표시에 사용.
    반환: {savedsearch_name: {status, last, reason}} / 조회 실패 시 None(판정 보류)."""
    cached = cache_get("sched_status")
    if cached is not None:
        return cached
    spl = ('search index=_internal sourcetype=scheduler '
           f'earliest=-{hours}h '
           '| stats latest(status) as status latest(_time) as last_epoch latest(reason) as reason '
           'latest(run_time) as run_time latest(result_count) as result_count '
           'by savedsearch_name '
           '| eval last=strftime(last_epoch,"%m/%d %H:%M") '
           '| fields savedsearch_name status last reason run_time result_count')
    try:
        rows = splunk_search(spl)
    except Exception as e:
        print(f"[sched] scheduler 조회 실패: {e}", flush=True)
        return None
    status_map = {}
    for r in rows:
        nm = r.get("savedsearch_name") or ""
        if nm:
            status_map[nm] = {"status": r.get("status"), "last": r.get("last"),
                              "reason": r.get("reason"), "run_time": r.get("run_time"),
                              "result_count": r.get("result_count")}
    cache_set("sched_status", status_map)
    return status_map


def _run_data_check(timeline):
    """데이터 점검 + (이상이 있을 때만) 이상 필드별 스케줄 상태 교차확인. data_check·data_refresh 공용."""
    import raas_data_check as DC
    report = DC.run_data_check(timeline)
    s = report.get("summary", {})
    if s.get("red") or s.get("yellow"):        # 이상이 있을 때만 스케줄러 추가 조회(건강한 날은 스킵)
        DC.cross_check_schedules(report, fetch_schedule_status())
    return report

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

def call_claude_stream(system: str, user: str, max_tokens: int = 4000):
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
JS_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raas_web.js")


def _app_version() -> str:
    """프론트 버전 = raas_web.html/raas_web.js 수정시각(mtime) 중 최신. 파일이 바뀌면 값이 바뀐다.
    홈화면 바로가기의 자동 새로고침 감지 + /raas_web.js?v= 캐시버스터에 사용."""
    try:
        mt = max(os.path.getmtime(p) for p in (HTML_FILE, JS_FILE) if os.path.exists(p))
        return str(int(mt))
    except (OSError, ValueError):
        return "0"


# 토글의 범주/카테고리 id('scale' 등)를 실제 지표명으로 — 분석 여정 로깅용
_STORY_CAT_TO_METRIC = {
    "deep": "깊은청취율", "retention": "리텐션", "new_churn": "신규/이탈",
    "flow": "사용자흐름", "quality": "청취품질",
}
def _story_metric_name(toggle_state, focus=None) -> str:
    """로깅할 지표명 산출. focus(kpi_metric)가 있으면 우선.
    scale 범주는 기간에 따라 DAU/WAU/MAU로 환원(토글의 'scale'을 그대로 쓰지 않음)."""
    if focus:
        return focus
    ts = toggle_state or {}
    cat = ts.get("metric") or ts.get("category")
    period = ts.get("period", "day")
    if cat in (None, "", "scale"):
        return {"week": "WAU", "month": "MAU"}.get(period, "DAU")
    return _STORY_CAT_TO_METRIC.get(cat, cat)


_SMALL_BLOCK_RE = re.compile(r"\[small\][\s\S]*?\[/small\]")
def _strip_admin_meta(text):
    """관리자 참고 메타([small] 블록: 분석 대상·실데이터·추가 적재 필요)를 제거.
    비관리자 응답에서 호출 → 원시 데이터가 아예 전달되지 않음."""
    if not text:
        return text
    t = _SMALL_BLOCK_RE.sub("", text)
    return re.sub(r"\n{3,}", "\n\n", t).strip()

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
        # 캐시 방지 — 모바일 브라우저(특히 ngrok+Safari)의 휴리스틱 캐싱으로
        # 옛 HTML/JS가 남아 프론트 변경이 반영 안 되는 문제 방지
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
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
                    html = f.read()
                # 현재 버전(파일 mtime)을 HTML에 주입 — 자동 업데이트 감지용
                html = html.replace("%%APP_VER%%", _app_version())
                self.send_html(html)
            else:
                self.send_html("<h2>raas_web.html 파일을 같은 폴더에 두세요</h2>")

        elif self.path.startswith("/raas_web.js"):
            # 메인 프론트 JS (html에서 분리). ?v=버전 캐시버스터와 함께 서빙 — 디스크 서빙이라 재시작 불필요.
            if os.path.exists(JS_FILE):
                with open(JS_FILE, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_json({"ok": False, "error": "raas_web.js 없음"}, 404)

        elif self.path == "/api/version":
            self.send_json({"version": _app_version()})

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
                    channel     = body.get('channel'),
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
                reason = body.get("reason")   # 👎 아쉬움 사유(선택) — 있으면 함께 저장
                ok = save_feedback(int(query_id), int(feedback),
                                   reason=(reason if reason is not None else None))
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

                # 편성표 의도 — '코너 편성/편성표'는 원인 분석이 아니라 주간 편성표(룩업 데이터).
                #   프로그램명만 있으면 lenient로 탐지(라우터는 편성표 의도의 프로그램 탐지에만 사용).
                if STORY.is_schedule_query(question):
                    try:
                        _sr = ROUTER.route(question, lenient=True)
                    except Exception:
                        _sr = None
                    prog = (_sr or {}).get("program")
                    sched = None
                    if prog:
                        try:
                            sched = STORY.build_program_schedule(prog["code"])
                        except Exception as e:
                            print(f"[schedule] build error: {e}")
                            sched = None
                    if sched:
                        self.send_response(200)
                        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                        self.send_header("Cache-Control", "no-cache")
                        self.send_header("Connection", "keep-alive")
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.end_headers()
                        def sse_s(data: dict):
                            self.wfile.write(("data: " + json.dumps(data, ensure_ascii=False) + "\n\n").encode("utf-8"))
                            self.wfile.flush()
                        sse_s({"type": "token", "text": sched})
                        qid = save_query(
                            user_id, question, sched, ip=ip, user_name=user_name,
                            user_role=user_role, intent="schedule",
                            scope=prog["code"], scope_keyword=prog["channel"], source="general")
                        sse_s({"type": "done", "query_id": qid,
                               "routing_badge": "🗓 주간 편성표"})
                        self.close_connection = True
                        return
                # 검색·Grounding 레이어 — 프로그램 관련 자유 질의는 관련 데이터·온톨로지를 골라 LLM에 넣고
                #   LLM이 본연의 성능으로 답한다(고정 템플릿 없음). 비-프로그램 질의는 기존 엔진으로.
                _ground = None
                try:
                    _my = user.get("my_programs") or []
                    _dflt = _my[0] if _my else None   # 관심 프로그램/채널(최상단) → 엔티티 없는 지표질의 기본 대상
                    _ground = GROUND.assemble(question, overlay_ctx={"user_id": user_id, "mode": "normal",
                                                                     "default_code": _dflt})
                except Exception as _e:
                    print(f"[grounding] assemble error: {_e}")
                if _ground and _ground.get("ok"):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "keep-alive")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    def sse_g(d: dict):
                        self.wfile.write(("data: " + json.dumps(d, ensure_ascii=False) + "\n\n").encode("utf-8"))
                        self.wfile.flush()
                    _gu = f"근거 데이터:\n{_ground['context']}\n\n사용자 질문: {question}"
                    _gfull, _gusage = [], {}
                    for chunk in call_claude_stream(GROUND.system_with_style(GROUND.GROUNDING_SYSTEM), _gu, max_tokens=MAX_ANSWER_TOKENS):
                        if isinstance(chunk, dict) and "_usage" in chunk:
                            _gusage = chunk["_usage"]
                        else:
                            _gfull.append(chunk)
                            sse_g({"type": "token", "text": chunk})
                    _ganswer = "".join(_gfull)
                    _gqid = save_query(user_id, question, _ganswer, ip=ip, user_name=user_name,
                                       user_role=user_role, intent="grounded",
                                       scope=_ground["provenance"].get("program"), source="general",
                                       input_tokens=_gusage.get("input_tokens"),
                                       output_tokens=_gusage.get("output_tokens"))
                    if user.get("is_admin"):
                        _ov = _ground["provenance"].get("overlay_items") or []
                        _vr = None
                        try:
                            _vr = GROUND.verify_numbers(_ground["context"], _ganswer)
                        except Exception as _e:
                            print(f"[verify] {_e}")
                        # 적용 스코프·기준일 — 참고([small]) 토글 켤 때만 노출(엔티티 완화·특정일 답변의 기준 명시)
                        _eb_lines = (_ground.get("entities_brief") or "").splitlines()
                        _scope_parts = _eb_lines[:1] + [
                            l.strip().lstrip("※ ").split(" — ")[0]   # 'X 요청 특정일: 날짜'만(내부 지시문 제거)
                            for l in _eb_lines[1:] if "요청 특정일" in l]
                        _scope_line = " · ".join(p for p in _scope_parts if p)
                        _gp = ("[small]\n**검색 grounding**"
                               + (f" — {_scope_line}" if _scope_line else "")
                               + "\n사용 provider: "
                               + ", ".join(_ground["providers_used"])
                               + (f"\n적용된 지식 오버레이: {len(_ov)}건" if _ov else "")
                               + _verify_line(_vr)
                               + "\n[/small]")
                        sse_g({"type": "token", "text": "\n\n" + _gp})
                    sse_g({"type": "done", "query_id": _gqid, "routing_badge": "🔎 데이터 grounding"})
                    self.close_connection = True
                    return

                timeline = get_cached_timeline()
                sys_prompt, ctx, max_tok, chart_data, facts = QE.query_with_timeline_stream(
                    question, timeline, target_date=target_date
                )
                if sys_prompt is None:
                    self.send_json({"ok": False, "error": "데이터를 사용할 수 없습니다."}, 500)
                    return
                facts = facts or {}
                # 채팅 차트 단일화: QE 폴백의 구 dataZoom 차트(chart_data) 비활성 —
                #   채팅 답변 차트는 grounding ```chart 단일 경로만 사용(리모델링 정리).
                chart_data = None

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
                # 1단계-c: 관리자에게만 참고 푸터([small]) — 사용 데이터 + 지표 정의(온톨로지)
                if user.get("is_admin"):
                    try:
                        _prov = STORY.build_query_provenance(facts, target_date)
                        if _prov:
                            sse({"type": "token", "text": "\n\n" + _prov})
                    except Exception as _e:
                        print(f"[provenance] {_e}")
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

        elif self.path == "/api/improve/context":
            # 개선하기 화면 재료 — 사용 데이터 필드(의미)·온톨로지 항목·본인 기여
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                q = (body.get("question") or "").strip()
                ctx = GROUND.improve_context(q, user_id=str(user["id"]))
                self.send_json(ctx if ctx.get("ok") else {"ok": False, **ctx})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/improve/contribute":
            # 기여 입력 — 온톨로지 수정/추가(candidate) 또는 데이터 요청(요청형)
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                kind = body.get("kind")
                if kind == "knowledge":
                    kid = add_knowledge_item(
                        str(user["id"]), body.get("type") or "fact",
                        body.get("target_kind") or "program", body.get("target_id"),
                        (body.get("content") or "").strip(), op=body.get("op") or "add",
                        scope="candidate", status="draft")
                    self.send_json({"ok": kid > 0, "id": kid})
                elif kind == "data_request":
                    rid = add_data_request(
                        str(user["id"]), body.get("field_name") or "",
                        (body.get("description") or "").strip(),
                        target_id=body.get("target_id"), splunk_spl=body.get("splunk_spl"))
                    self.send_json({"ok": rid > 0, "id": rid})
                else:
                    self.send_json({"ok": False, "error": "kind=knowledge|data_request"}, 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/improve/requery":
            # 재질의 — 본인 candidate 오버레이 병합 grounding으로 재생성 + LLM judge(원본 vs 개선)
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                q = (body.get("question") or "").strip()
                ans_orig = body.get("answer_original") or ""
                g = GROUND.assemble(q, overlay_ctx={"user_id": str(user["id"]), "mode": "requery"})
                if not (g and g.get("ok")):
                    self.send_json({"ok": False, "error": "재질의 컨텍스트 구성 실패(프로그램 미식별)"}, 400); return
                _gu = f"근거 데이터:\n{g['context']}\n\n사용자 질문: {q}"
                parts = []
                for chunk in call_claude_stream(GROUND.system_with_style(GROUND.GROUNDING_SYSTEM), _gu, max_tokens=MAX_ANSWER_TOKENS):
                    if not (isinstance(chunk, dict) and "_usage" in chunk):
                        parts.append(chunk)
                ans_new = "".join(parts)
                verdict = GROUND.judge(q, ans_orig, ans_new) if ans_orig else None
                imp_id = add_improvement(
                    str(user["id"]), q, ans_orig, answer_improved=ans_new,
                    source_query_id=body.get("source_query_id"),
                    contributions_json=json.dumps(g["provenance"].get("overlay_items") or [], ensure_ascii=False),
                    judge_json=json.dumps(verdict, ensure_ascii=False) if verdict else None,
                    status="검토대기")
                self.send_json({"ok": True, "improvement_id": imp_id,
                                "answer_improved": ans_new, "judge": verdict,
                                "overlay_items": g["provenance"].get("overlay_items") or []})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/improve/verdict":
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                set_improvement_verdict(body.get("id"), body.get("verdict"))
                self.send_json({"ok": True})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/improve/mine":
            # ⑤ 본인 개선 시도 이력 + 본인 데이터 요청
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                uid = str(user["id"])
                imps = list_improvements(user_id=uid)
                for it in imps:
                    it["judge"] = json.loads(it["judge_json"]) if it.get("judge_json") else None
                self.send_json({"ok": True, "improvements": imps,
                                "data_requests": list_data_requests(contributor_id=uid),
                                "knowledge": list_my_knowledge(uid),
                                "uploads": list_my_uploads(uid)})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/upload/add":
            # 본인 데이터 업로드 — 소규모 표(columns + rows). candidate 저장.
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                cols = body.get("columns") or []
                rows = body.get("rows") or []
                if not cols or not rows:
                    self.send_json({"ok": False, "error": "표 데이터(헤더+행)가 필요합니다."}, 400); return
                rows = rows[:200]   # 소규모 상한
                kind = body.get("target_kind") or "program"
                uid_ = add_uploaded_data(
                    str(user["id"]), kind,
                    (None if kind == "global" else body.get("target_id")),
                    (body.get("name") or "업로드").strip(), cols, rows)
                self.send_json({"ok": uid_ > 0, "id": uid_})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/upload/mine/retire":
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                self.send_json({"ok": retire_my_upload(body.get("id"), str(user["id"]))})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/upload/review":
            # 거버넌스 — 업로드 데이터 승인(공유)
            user = self._require_stats_viewer()
            if not user:
                return
            try:
                self.send_json({"ok": approve_upload(body.get("id"), str(user["id"]))})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/knowledge/mine/retire":
            # 본인 기여 지식 삭제(능동 기여 관리)
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                ok = retire_my_knowledge(body.get("id"), str(user["id"]))
                if ok:
                    self.send_json({"ok": True})
                else:
                    # candidate가 아니면(=승인·공유됨) 본인 삭제 불가 — 관리자 회수만 가능
                    self.send_json({"ok": False, "error": "승인·공유된 지식은 본인이 삭제할 수 없습니다(관리자 회수만 가능)."}, 403)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/improve/queue":
            # ⑥ 거버넌스 검토 큐 — 관리자/데이터/총괄관리
            user = self._require_stats_viewer()
            if not user:
                return
            try:
                imps = list_improvements(status="검토대기")
                for it in imps:
                    it["judge"] = json.loads(it["judge_json"]) if it.get("judge_json") else None
                    ids = json.loads(it["contributions_json"]) if it.get("contributions_json") else []
                    it["contributions"] = get_knowledge_items_by_ids(ids)
                reqs = list_data_requests()
                reqs_open = [r for r in reqs if r.get("status") in ("요청됨", "처리중")]
                wk = feedback_weakness(days=30)
                for w in wk:
                    try:
                        w["name"] = GROUND._resolve_name(w["scope"]) or w["scope"]
                    except Exception:
                        w["name"] = w["scope"]
                self.send_json({"ok": True, "improvements": imps, "data_requests": reqs_open,
                                "weakness": wk,
                                "negative_open": feedback_negative_open(days=30),
                                "approved_knowledge": list_approved_knowledge(),
                                "pending_uploads": list_pending_uploads(),
                                "effect": knowledge_effect(days=30),
                                "funnel": loop_funnel(days=30)})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/improve/review":
            # ⑥ 개선 시도 승인/반려 — 승인 시 기여 지식 항목을 approved(공유)로 승격
            user = self._require_stats_viewer()
            if not user:
                return
            try:
                action = body.get("action")  # approve | reject
                ok = review_improvement(body.get("id"), action, str(user["id"]))
                self.send_json({"ok": ok})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/data_request/process":
            # ⑥ 데이터 요청 상태 갱신 — 요청됨→처리중→완료/반려
            user = self._require_stats_viewer()
            if not user:
                return
            try:
                ok = update_data_request(body.get("id"), body.get("status"), str(user["id"]))
                self.send_json({"ok": ok})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/knowledge/retire":
            # 본체(승인된 공유 지식) 회수 — 오버레이에서 제외(롤백)
            user = self._require_stats_viewer()
            if not user:
                return
            try:
                ok = retire_knowledge_item(body.get("id"), str(user["id"]))
                self.send_json({"ok": ok})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/knowledge/reclassify":
            # 관리자 재분류 — 미분류·global 지식 target 좁힘
            user = self._require_stats_viewer()
            if not user:
                return
            try:
                ok = reclassify_knowledge(body.get("id"), body.get("target_kind") or "global",
                                          body.get("target_id"), str(user["id"]))
                self.send_json({"ok": ok})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/knowledge/promote":
            # 승인 지식 → TTL canonical 승격 배치(contributed.ttl 재생성). 관리자(is_admin) 전용.
            user = self._require_admin()
            if not user:
                return
            try:
                import raas_ontology_promote as PROMOTE
                res = PROMOTE.promote_approved_to_ttl()
                self.send_json(res)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/knowledge/promote/preview":
            # 승격 전 미리보기(dry-run) — 병합 전후/대상/제외 항목. 관리자 전용.
            user = self._require_admin()
            if not user:
                return
            try:
                import raas_ontology_promote as PROMOTE
                self.send_json(PROMOTE.preview_promotion())
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/style/get":
            # 답변 스타일 정책 조회(편집 화면용). 관리자(is_admin) 전용.
            user = self._require_admin()
            if not user:
                return
            try:
                import raas_history_db as HDB
                cur = HDB.get_style_policy()
                self.send_json({"ok": True,
                                "content": cur if cur is not None else GROUND._DEFAULT_STYLE_POLICY,
                                "is_default": cur is None, "max": HDB.STYLE_POLICY_MAX})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

        elif self.path == "/api/style/set":
            # 답변 스타일 정책 저장(새 버전). 관리자(is_admin) 전용 + 문자 상한.
            user = self._require_admin()
            if not user:
                return
            try:
                import raas_history_db as HDB
                res = HDB.set_style_policy(body.get("content"), str(user["id"]))
                self.send_json(res, 200 if res.get("ok") else 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)

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
                    chart_data = None   # 채팅 차트 단일화: 구 chart_data 비활성(grounding ```chart만)
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

        elif self.path == "/api/storyline/today":
            # 스토리라인(단일 경로) — 어제 방송 특이사항. 엔진 일원화: grounding digest를
            #   GROUNDING_SYSTEM과 동일 엔진(provider·온톨로지·오버레이·LLM)으로 스트리밍.
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                user_id = str(user["id"]); user_name = user["name"]; user_role = user["role"]
                ip = self._get_client_ip()
                ref_date = _latest_data_date()
                anomalies = get_cached_anomalies()
                g = GROUND.assemble_digest(
                    ref_date, anomalies,
                    overlay_ctx={"user_id": user_id, "mode": "normal"},
                    question="어제 방송 특이사항")
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                def sse_t(d: dict):
                    self.wfile.write(("data: " + json.dumps(d, ensure_ascii=False) + "\n\n").encode("utf-8"))
                    self.wfile.flush()
                _tu = f"근거 데이터:\n{g['context']}\n\n사용자 질문: 어제 방송에서 특이사항을 알려줘"
                _full, _usage = [], {}
                for chunk in call_claude_stream(GROUND.system_with_style(GROUND.DIGEST_SYSTEM), _tu, max_tokens=MAX_ANSWER_TOKENS):
                    if isinstance(chunk, dict) and "_usage" in chunk:
                        _usage = chunk["_usage"]
                    else:
                        _full.append(chunk); sse_t({"type": "token", "text": chunk})
                _ans = "".join(_full)
                _qid = save_query(user_id, "어제 방송 특이사항", _ans, ip=ip, user_name=user_name,
                                  user_role=user_role, intent="today_digest", source="general",
                                  input_tokens=_usage.get("input_tokens"),
                                  output_tokens=_usage.get("output_tokens"))
                if user.get("is_admin"):
                    _ov = g["provenance"].get("overlay_items") or []
                    _vr = None
                    try:
                        _vr = GROUND.verify_numbers(g["context"], _ans)
                    except Exception as _e:
                        print(f"[verify] {_e}")
                    _gp = ("[small]\n**특이사항 digest** — 기준일 " + (ref_date or "?")
                           + " · provider: " + ", ".join(g["providers_used"])
                           + (f"\n적용된 지식 오버레이: {len(_ov)}건" if _ov else "")
                           + _verify_line(_vr) + "\n[/small]")
                    sse_t({"type": "token", "text": "\n\n" + _gp})
                sse_t({"type": "done", "query_id": _qid, "routing_badge": "🗓 어제 특이사항",
                       "drill": g.get("drill") or []})
                self.close_connection = True
                return
            except Exception as e:
                try:
                    self.send_json({"ok": False, "error": str(e)}, 500)
                except Exception:
                    pass
                return

        elif self.path == "/api/data_check":
            # 데이터 확인 — raas_kpi_latest.csv 규칙 점검(결정적) + Haiku 총평(하이브리드). 데이터 직무 전용.
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                if user.get("role") != "데이터" and not user.get("is_admin"):
                    self.send_json({"ok": False, "error": "데이터 직무 전용입니다."}, 403); return
                report = _run_data_check(get_cached_timeline())
                full = _build_data_check_text(report)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                def sse_d(d):
                    self.wfile.write(("data: " + json.dumps(d, ensure_ascii=False) + "\n\n").encode("utf-8"))
                    self.wfile.flush()
                sse_d({"type": "token", "text": full})
                _qid = save_query(str(user["id"]), "데이터 확인하기", full, ip=self._get_client_ip(),
                                  user_name=user["name"], user_role=user["role"],
                                  intent="data_check", source="general")
                sse_d({"type": "done", "query_id": _qid, "routing_badge": "🩺 데이터 점검"})
                self.close_connection = True
                return
            except Exception as e:
                try:
                    self.send_json({"ok": False, "error": str(e)}, 500)
                except Exception:
                    pass
                return

        elif self.path == "/api/data_refresh":
            # 수동 재적재 — Splunk 캐시 무효화 + 즉시 재풀 + 자동 재점검. 데이터 직무 관리자(apollo) 전용.
            try:
                user = self._get_session_user()
                if not user:
                    self.send_json({"ok": False, "error": "로그인이 필요합니다."}, 401); return
                if not (user.get("is_admin") and user.get("role") == "데이터"):
                    self.send_json({"ok": False, "error": "데이터 직무 관리자 전용입니다."}, 403); return
                # 동시 재적재 방지 — 진행 중이면 즉시 반려(Splunk 이중 풀 차단)
                if not _refresh_lock.acquire(blocking=False):
                    self.send_json({"ok": False, "error": "이미 데이터를 가져오는 중입니다. 잠시 후 다시 시도하세요."}, 429); return
                try:
                    import time as _time
                    cache_clear()                    # timeline·anomalies 등 Splunk 파생 캐시 전체 무효화
                    _t0 = _time.time()
                    tl = get_cached_timeline()       # 그 자리에서 Splunk 재적재(실패 시 빈 타임라인 → red로 표면화)
                    _elapsed = _time.time() - _t0
                    src = get_timeline_source()
                    latest = _latest_data_date()
                    report = _run_data_check(tl)
                finally:
                    _refresh_lock.release()
                _src_kr = {"splunk": "스플렁크", "csv": "로컬 CSV(폴백)"}.get(src, src or "unknown")
                prefix = (f"🔄 **최신 데이터 다시 가져옴** — 출처 {_src_kr} · "
                          f"최신일 {latest or '?'} · {_elapsed:.1f}초 소요")
                full = _build_data_check_text(report, prefix_line=prefix)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                def sse_r(d):
                    self.wfile.write(("data: " + json.dumps(d, ensure_ascii=False) + "\n\n").encode("utf-8"))
                    self.wfile.flush()
                sse_r({"type": "token", "text": full})
                _qid = save_query(str(user["id"]), "최신 데이터 다시 가져오기", full, ip=self._get_client_ip(),
                                  user_name=user["name"], user_role=user["role"],
                                  intent="data_refresh", source="general")
                sse_r({"type": "done", "query_id": _qid, "routing_badge": "🔄 데이터 재적재"})
                self.close_connection = True
                return
            except Exception as e:
                try:
                    self.send_json({"ok": False, "error": str(e)}, 500)
                except Exception:
                    pass
                return

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
