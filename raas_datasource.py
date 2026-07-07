# -*- coding: utf-8 -*-
"""RAAS 데이터 소스 레이어 — Splunk 수집 단일 소유.

이 모듈만 Splunk REST(search/jobs/export)를 호출한다. 나머지 모듈은
여기서 받은 구조화 데이터(타임라인 등)를 사용만 한다.

구성:
  splunk_search(spl)      범용 SPL 실행기 (REST export, JSON 라인 스트림)
  fetch_lookup(name)      "| inputlookup <name>" 헬퍼
  run_query(spl)          스플렁크 직접 쿼리 별칭 (= splunk_search)
  Feed                    캐시 정책 공용 클래스 (daily_at 경계 / ttl_sec)
  get_timeline()          KPI 타임라인 {PGM_CODE: {DATE: row}} — 일 1회 갱신

캐시 정책 (Feed):
  daily_at="HH:MM"  원천이 하루 1회 배치로 갱신되는 소스.
                    해당 시각 경계를 지나면 stale → 다음 요청 때 재적재.
  ttl_sec=N         N초 경과 시 stale — 실시간성 소스용 (예: 분 단위 지표).
  폴백 캐시         Splunk 실패로 폴백 소스(csv)로 채워진 캐시는
                    FALLBACK_RETRY_SEC 후 stale — Splunk 복구를 빨리 반영.

KPI 타임라인 갱신 시각:
  원천 raas_kpi_latest.csv 는 Splunk savedsearch 가 매일 06:50 에 생성.
  기본 경계는 07:00 (생성 여유 10분). .env RAAS_KPI_REFRESH_AT 로 변경 가능.

새 데이터 소스 추가법:
  1) loader 작성 — fetch_lookup()/run_query() 사용, (data, source) 반환
  2) Feed("이름", loader, daily_at="07:00")  또는  Feed(..., ttl_sec=60)
  3) 사용처에서 feed.get() 호출 (전 스레드 공유, 락으로 단일 적재 보장)
"""
import os
import ssl
import csv
import json
import base64
import threading
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# ── Splunk 접속 설정 ─────────────────────────────────────
SPLUNK_HOST     = os.getenv("SPLUNK_HOST")
SPLUNK_USER     = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD = os.getenv("SPLUNK_PASSWORD")
SPLUNK_APP      = os.getenv("SPLUNK_APP")
SPLUNK_TIMEOUT  = int(os.getenv("SPLUNK_TIMEOUT", "10"))  # 초. 미도달 환경에서 빠른 CSV 폴백을 위해 짧게

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

# ── Splunk REST 실행기 ───────────────────────────────────
def _splunk_auth():
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()

def splunk_search(spl: str) -> list:
    """SPL 실행 → 결과 행 list[dict]. (search/jobs/export, JSON 라인 스트림)"""
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", _splunk_auth())
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

def fetch_lookup(name: str, tail: str = "") -> list:
    """룩업 테이블 전체(또는 tail로 필터/가공)를 가져온다.
    예: fetch_lookup("raas_llm_context_day.csv", '| search date_label="2026/07/06"')"""
    spl = f"| inputlookup {name}"
    if tail:
        spl += f" {tail}"
    return splunk_search(spl)

# 스플렁크 직접 쿼리 (index 검색 등) — 이름만 다를 뿐 splunk_search와 동일
run_query = splunk_search


# ── Feed: 캐시되는 데이터 소스 하나 ──────────────────────
FALLBACK_RETRY_SEC = 600   # 폴백 소스로 채워진 캐시의 재시도 간격(초)

FEEDS: dict = {}           # {name: Feed} — 등록된 소스 목록 (점검·수동 무효화용)

class Feed:
    """캐시 정책이 붙은 데이터 소스. loader는 (data, source) 튜플을 반환한다.
    source가 'splunk'가 아니면(폴백) FALLBACK_RETRY_SEC 후 재시도한다."""

    def __init__(self, name: str, loader, daily_at: str = None, ttl_sec: int = None):
        self.name = name
        self.loader = loader
        self.daily_at = daily_at       # "HH:MM" — 일 1회 배치 갱신형
        self.ttl_sec = ttl_sec         # 초 — 실시간형
        self._lock = threading.Lock()
        self._data = None
        self._loaded_at = None
        self._source = None
        FEEDS[name] = self

    def _boundary(self, now: datetime) -> datetime:
        """가장 최근에 지난 daily_at 경계 시각."""
        hh, mm = (int(x) for x in self.daily_at.split(":"))
        b = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now < b:
            b -= timedelta(days=1)
        return b

    def _stale(self, now: datetime) -> bool:
        if self._loaded_at is None:
            return True
        # 폴백/실패로 채워진 캐시는 짧은 간격으로 재시도 (Splunk 복구 반영)
        #   실시간형(ttl_sec)은 자체 주기가 더 짧으므로 그 주기로 재시도.
        if self._source and self._source != "splunk":
            retry = self.ttl_sec if self.ttl_sec is not None else FALLBACK_RETRY_SEC
            return (now - self._loaded_at).total_seconds() >= retry
        if self.ttl_sec is not None:
            return (now - self._loaded_at).total_seconds() >= self.ttl_sec
        if self.daily_at:
            return self._loaded_at < self._boundary(now)
        return False

    def get(self, force: bool = False):
        with self._lock:
            if not force and not self._stale(datetime.now()):
                return self._data
            data, source = self.loader()
            self._data = data
            self._source = source
            self._loaded_at = datetime.now()
            return self._data

    def invalidate(self):
        """다음 get()에서 강제 재적재 (수동 재적재용)."""
        with self._lock:
            self._loaded_at = None

    def source(self) -> str:
        return self._source or "unknown"


# ── KPI 타임라인 (raas_kpi_latest.csv 룩업) ──────────────
KPI_LOOKUP = "raas_kpi_latest.csv"
# 원천 룩업은 매일 06:50 생성(Splunk savedsearch) → 생성 여유 10분을 두고 07:00 경계
KPI_REFRESH_AT = os.getenv("RAAS_KPI_REFRESH_AT", "07:00")

def _local_kpi_path():
    """로컬 폴백 CSV 경로 — data/ 우선, 같은 폴더 fallback (구 위치 호환)."""
    base = os.path.dirname(os.path.abspath(__file__))
    for p in (os.path.join(base, "data", KPI_LOOKUP),
              os.path.join(base, KPI_LOOKUP)):
        if os.path.exists(p):
            return p
    return None

def _supplement_timeline_from_csv(timeline: dict) -> None:
    """Splunk 룩업에 없는 필드를 로컬 CSV로 보완 (react_week, react_rate_week 등).
    날짜가 일치하는 행은 직접 보완하고, 로컬 CSV의 마지막 날짜 이후 Splunk 행은
    마지막 로컬 행 값을 근사치로 사용 (weekly 집계 등 느리게 변하는 지표 대상)."""
    local_path = _local_kpi_path()
    if not local_path:
        return
    try:
        with open(local_path, encoding="utf-8-sig") as f:
            local_rows = list(csv.DictReader(f))
        # code → {date → row} mapping
        local_by_code: dict = {}
        for r in local_rows:
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

def load_timeline(search_fn=None):
    """원샷 로드(캐시 없음) — (timeline, source) 반환.
    timeline = {PGM_CODE: {DATE: row}}. search_fn 주입은 CLI·테스트용
    (기본은 splunk_search, 실패 시 로컬 CSV 폴백)."""
    fn = search_fn or splunk_search
    source = 'splunk'
    try:
        rows = fn(f"| inputlookup {KPI_LOOKUP}")
    except Exception as e:
        local_path = _local_kpi_path()
        if not local_path:
            raise
        print(f"  [fallback] Splunk {type(e).__name__} → local CSV: {local_path}")
        with open(local_path, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        source = 'csv_fallback'
    timeline = {}
    for r in rows:
        code = r.get('PGM_CODE')
        date = r.get('DATE')
        if not code or not date:
            continue
        timeline.setdefault(code, {})[date] = r
    if source == 'splunk':
        _supplement_timeline_from_csv(timeline)
    if timeline:
        all_dates = set()
        for code_rows in timeline.values():
            all_dates.update(code_rows.keys())
        print(f"  [timeline] source={source}, {len(timeline)} codes x {len(all_dates)} dates "
              f"({min(all_dates)} ~ {max(all_dates)})")
    return timeline, source

_timeline_feed = Feed("kpi_timeline", load_timeline, daily_at=KPI_REFRESH_AT)

def get_timeline(force: bool = False) -> dict:
    """일일 캐시된 KPI 타임라인. 07:00 경계(또는 invalidate) 전까지 재사용."""
    return _timeline_feed.get(force=force)

def get_timeline_source() -> str:
    return _timeline_feed.source()

def invalidate_timeline() -> None:
    """수동 재적재 — 다음 get_timeline()이 Splunk를 다시 조회."""
    _timeline_feed.invalidate()


# ── 실시간 동시사용자 (summary_gorealra_1m, 1분 집계) ─────
# 필드 규칙(2026-07 확인): 채널별 *_COUNT는 값 없음 → *_SUM만 사용.
#   접미사: _BA=보는라디오 / _PW=웹브라우저(SBS홈페이지) / _PC=PC클라이언트 / _AI=AI스피커
#   스마트폰은 필드가 없어 파생: TOTAL_SUM_SP = TOTAL_SUM - TOTAL_SUM_PC - TOTAL_SUM_AI
#   채널 접두사 ↔ RAAS 코드: FM=파워FM(F00) / AM=러브FM(L00) / GM=고릴라M(G00) / PM=픽채널(P00)
RT_INDEX = "summary_gorealra_1m"
_RT_FIELDS = ("_time TOTAL_SUM AM_SUM FM_SUM GM_SUM PM_SUM "
              "TOTAL_SUM_SP TOTAL_SUM_PC TOTAL_SUM_PW TOTAL_SUM_AI TOTAL_SUM_BA")

def _rt_spl(earliest: str, latest: str) -> str:
    return (f"search index={RT_INDEX} earliest={earliest} latest={latest} "
            "| eval TOTAL_SUM_SP=TOTAL_SUM-TOTAL_SUM_PC-TOTAL_SUM_AI "
            f"| table {_RT_FIELDS} | sort 0 _time")

def _rt_loader(earliest: str, latest: str):
    """실시간 행 로더 — 실패 시 빈 리스트 + source='error' (Feed가 짧은 주기로 재시도)."""
    def load():
        try:
            return splunk_search(_rt_spl(earliest, latest)), "splunk"
        except Exception as e:
            print(f"  [realtime] 조회 실패({earliest}~{latest}): {e}")
            return [], "error"
    return load

# 오늘: 60초 캐시(사실상 1분 주기). 어제/지난주 동요일: 과거 불변 → 자정 경계 일일 캐시.
_rt_today_feed    = Feed("realtime_today",    _rt_loader("@d", "now"),      ttl_sec=60)
_rt_yday_feed     = Feed("realtime_yesterday", _rt_loader("-1d@d", "@d"),   daily_at="00:05")
_rt_lastweek_feed = Feed("realtime_lastweek",  _rt_loader("-7d@d", "-6d@d"), daily_at="00:05")

def get_realtime_today() -> list:
    """오늘 0시~현재 1분 단위 동시사용자 행 목록 (60초 캐시)."""
    return _rt_today_feed.get() or []

def get_realtime_yesterday() -> list:
    return _rt_yday_feed.get() or []

def get_realtime_lastweek() -> list:
    """지난주 동요일(7일 전) 하루치 — '지난주 이 시간' 비교용."""
    return _rt_lastweek_feed.get() or []


# ── 값 코어션 헬퍼 (타임라인 행 값 → int/float) ──────────
def _i(v, d=0):
    try: return int(float(v)) if v not in (None, '', 'None', 'null') else d
    except Exception: return d

def _fn(v):
    try: return float(v) if v not in (None, '', 'None', 'null') else None
    except Exception: return None


# ── 타임라인 조회 헬퍼 ───────────────────────────────────
def get_snapshot_at(timeline, target_date):
    """특정 날짜의 모든 코드 {code: row} dict."""
    norm = target_date.replace('-', '/')
    return {code: date_rows[norm]
            for code, date_rows in timeline.items() if norm in date_rows}

def get_metric_trend(timeline, code, metric_field, days=30, date_field=None):
    """코드+지표의 시계열 [(date, value)] 반환."""
    date_rows = timeline.get(code, {})
    sorted_dates = sorted(date_rows.keys(), reverse=True)[:days]
    result = []
    for d in reversed(sorted_dates):
        row = date_rows[d]
        raw = row.get(metric_field)
        try:
            val = float(raw) if raw not in (None, '', 'None', 'null') else None
        except (ValueError, TypeError):
            val = None
        result.append((d, val))
    return result

def get_available_dates(timeline):
    """timeline 내 모든 날짜 (오름차순)."""
    all_dates = set()
    for date_rows in timeline.values():
        all_dates.update(date_rows.keys())
    return sorted(all_dates)
