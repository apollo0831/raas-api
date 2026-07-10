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

def splunk_search(spl: str, timeout: int = None) -> list:
    """SPL 실행 → 결과 행 list[dict]. (search/jobs/export, JSON 라인 스트림)
    timeout: 장기 범위 쿼리(예: 10년 아카이브)는 기본 10초로 부족 → 개별 지정."""
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", _splunk_auth())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT,
                                    timeout=timeout or SPLUNK_TIMEOUT) as resp:
            rows = []
            for line in resp:
                line = line.decode("utf-8").strip()
                if not line: continue
                try:
                    obj = json.loads(line)
                    # export는 중간 미리보기(preview=true) + 최종(preview=false)을 함께 스트리밍 —
                    # 변환 검색(sort/stats)에서 중복 유발. 최종 결과만 취한다(preview 없으면 최종 취급).
                    if obj.get("preview") is True: continue
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


# ── 오늘 편성·게스트 (index=broadplan, 하루 1회) ─────────────
# 상세 KPI 배치(완료된 날)가 아직 못 담은 '오늘(진행 중)' 프로그램별 게스트·보라 편성.
# earliest=@d 라 오늘 하루치. 코드=PGM_CODE, 이름=PGM_NAME, GUEST, view_radio(Y/N).
# 저장소=Splunk(로컬 저장 없음). KPI 타임라인과 같은 편성 축이라 daily_at=KPI_REFRESH_AT 공유.
_TODAY_LINEUP_SPL = r"""search index=broadplan earliest=@d latest=now
| eval DATE = strftime(_time, "%Y/%m/%d")
| eval DAY = strftime(_time, "%w")
| eval date_mday=ltrim(strftime(_time,"%d"),"0"), date_month=lower(strftime(_time,"%B")), date_wday=lower(strftime(_time,"%A")), date_year=strftime(_time,"%Y")
| lookup HOLIDAY_0 HOLIDAY_DAY AS date_mday, HOLIDAY_WEEK AS date_wday, HOLIDAY_MONTH AS date_month, HOLIDAY_YEAR AS date_year output HOLIDAY_CHECK
| fillnull value="no" HOLIDAY_CHECK
| rename program.vod_id as vod_id, program.start_time as start_time, program.end_time as end_time, program.gonggam_ch_cd as ch, program.view_radio as view_radio
| join type=left vod_id
    [| inputlookup BROADPLAN.csv
    | search IS_END="N"
    | rename VOD_ID AS vod_id
    | fields SEQ, vod_id, PD_NAME]
| fillnull value="" PROGRAM_CODE, view_radio
| search SEQ=*
| rename program.title as TITLE
| table DATE, DAY, TITLE, start_time, end_time, program.guestlist.guest.name, view_radio, view_radio, ch, SEQ, PD_NAME
| stats first(program.guestlist.guest.name) as GUEST by SEQ, DATE, TITLE, view_radio
| rename SEQ as PGM_CODE, TITLE as PGM_NAME
| table DATE, PGM_CODE, PGM_NAME, GUEST, view_radio"""

def _load_today_lineup():
    try:
        return splunk_search(_TODAY_LINEUP_SPL, timeout=60), "splunk"
    except Exception as e:
        print(f"  [today_lineup] 조회 실패: {e}")
        return [], "error"

_today_lineup_feed = Feed("today_lineup", _load_today_lineup, daily_at=KPI_REFRESH_AT)

def get_today_lineup(force: bool = False) -> list:
    """오늘(진행 중) 프로그램별 게스트·보라 편성 행 목록
       [{DATE, PGM_CODE, PGM_NAME, GUEST, view_radio}]. 하루 1회 캐시(KPI_REFRESH_AT 경계)."""
    return _today_lineup_feed.get(force=force) or []

def get_today_lineup_source() -> str:
    return _today_lineup_feed.source()


# ── 실시간 동시사용자 (tempsummary, 1분 집계) ─────────────
# 원천: gorealra_app_log 세션(RA/BA/RS)을 1분 버킷 겹침 판정 → dc(UUID) → collect.
# 필드 규칙(2026-07 확정 — 구 summary_gorealra_1m 스키마는 폐기):
#   채널: T00/F00/L00/G00/P00 — RAAS 코드와 동일(별도 매핑 불필요)
#   디바이스: DV_SP(스마트폰·태블릿)/DV_PC/DV_PW(웹)/DV_MWEB/DV_WATCH/DV_CAR/DV_AI_*(7사)
#   BA = 보는라디오(CATEGORY 축 — 디바이스 분해와 별개, BA_F00/BA_L00 채널 분해 존재)
#   성별(SEX_M/F)·연령(AGE_T*)은 본인인증(AUTH=1) 사용자만 — 분모 SEX_TM/AGE_TM 제공
RT_INDEX = os.getenv("RAAS_RT_INDEX", "tempsummary")   # 인덱스명 변경 대비 env 오버라이드
_RT_AI_FIELDS = "DV_AI_SKT DV_AI_TMAP DV_AI_BTV DV_AI_BIXBY DV_AI_LGU DV_AI_KT DV_AI_KKO"
# 오늘(스냅샷·프로파일용) — 채널+디바이스+성별+연령. 과거일(비교용)은 채널만(narrow).
_RT_WIDE = ("_time T00 F00 L00 G00 P00 "
            "DV_SP DV_PC DV_PW DV_MWEB DV_WATCH DV_CAR DV_AI BA "
            "SEX_M SEX_F SEX_TM "
            "AGE_T0_19 AGE_T20_24 AGE_T25_29 AGE_T30_34 AGE_T35_39 "
            "AGE_T40_44 AGE_T45_49 AGE_T50_54 AGE_T55_59 AGE_T60 AGE_TM")
_RT_NARROW = "_time T00 F00 L00 G00 P00"

def _rt_spl(earliest: str, latest: str, fields: str) -> str:
    spl = f"search index={RT_INDEX} earliest={earliest} latest={latest} "
    if "DV_AI" in fields:   # AI스피커 합계 필드는 원천에 없어 7사 합산으로 파생
        spl += (f"| fillnull value=0 {_RT_AI_FIELDS} "
                "| eval DV_AI=DV_AI_SKT+DV_AI_TMAP+DV_AI_BTV+DV_AI_BIXBY+DV_AI_LGU+DV_AI_KT+DV_AI_KKO ")
    return spl + f"| table {fields} | sort 0 _time"

def _rt_loader(earliest: str, latest: str, fields: str):
    """실시간 행 로더 — 실패 시 빈 리스트 + source='error' (Feed가 짧은 주기로 재시도)."""
    def load():
        try:
            return splunk_search(_rt_spl(earliest, latest, fields)), "splunk"
        except Exception as e:
            print(f"  [realtime] 조회 실패({earliest}~{latest}): {e}")
            return [], "error"
    return load

# 오늘: 60초 캐시(사실상 1분 주기). 어제/지난주 동요일: 과거 불변 → 자정 경계 일일 캐시.
_rt_today_feed    = Feed("realtime_today",    _rt_loader("@d", "now", _RT_WIDE),        ttl_sec=60)
_rt_yday_feed     = Feed("realtime_yesterday", _rt_loader("-1d@d", "@d", _RT_NARROW),   daily_at="00:05")
_rt_lastweek_feed = Feed("realtime_lastweek",  _rt_loader("-7d@d", "-6d@d", _RT_NARROW), daily_at="00:05")

def get_realtime_today() -> list:
    """오늘 0시~현재 1분 단위 동시사용자 행 목록 (60초 캐시)."""
    return _rt_today_feed.get() or []

def get_realtime_yesterday() -> list:
    return _rt_yday_feed.get() or []

def get_realtime_lastweek() -> list:
    """지난주 동요일(7일 전) 하루치 — '지난주 이 시간' 비교용."""
    return _rt_lastweek_feed.get() or []


# ── 실시간 확장 3-Feed (Phase 1) — 소스별 분리, 각 60초 캐시 ──────────────
# 네이밍 통일: 원천의 FM/AM·_F/_G/_L/_P → RAAS 코드(F00/L00/G00/P00)로 정규화.
#   ① rt_concurrent(tempsummary): 채널 동시자 + 디바이스×채널 + 보라×채널 + 인증 성별·연령 비율
#   ② rt_msg(infobank): 분당 채널별 SMS·공감로그(GG)      ③ rt_inflow(tempsummary3): 분당 채널별 유입
# AI스피커: 원천 7사×채널을 RAAS가 DV_AI(집계)로 파생, 원시 벤더는 드롭(온톨로지엔 DV_AI만).
_RT_DEV_REAL = ("DV_SP", "DV_PC", "DV_PW", "DV_MWEB", "DV_WATCH", "DV_CAR")
_RT_AI_VENDORS = ("DV_AI_SKT", "DV_AI_TMAP", "DV_AI_BTV", "DV_AI_BIXBY", "DV_AI_LGU", "DV_AI_KT", "DV_AI_KKO")
_RT_CH_SUF = (("", ""), ("_F", "_F00"), ("_G", "_G00"), ("_L", "_L00"), ("_P", "_P00"))
_RT_AGES = ("0_19", "20_24", "25_29", "30_34", "35_39", "40_44", "45_49", "50_54", "55_59", "60")

def _build_rt_concurrent_spl():
    ev = []
    for a in _RT_AGES:
        ev.append(f'| eval R_FM_AGE_{a}=round(AGE_F{a}/AGE_FM*100,1)')
        ev.append(f'| eval R_AM_AGE_{a}=round(AGE_L{a}/AGE_LM*100,1)')
    ev += ['| eval R_FM_SEX_F=round(SEX_F_F/SEX_FM*100,1)', '| eval R_FM_SEX_M=round(SEX_M_F/SEX_FM*100,1)',
           '| eval R_AM_SEX_F=round(SEX_F_L/SEX_LM*100,1)', '| eval R_AM_SEX_M=round(SEX_M_L/SEX_LM*100,1)']
    dev = " ".join(d + s for d in _RT_DEV_REAL for s, _ in _RT_CH_SUF)
    ai = " ".join(v + s for v in _RT_AI_VENDORS for s, _ in _RT_CH_SUF)
    age_out = " ".join(f"R_F00_AGE_{a}" for a in _RT_AGES) + " " + " ".join(f"R_L00_AGE_{a}" for a in _RT_AGES)
    return (f"search index={RT_INDEX} earliest=@d latest=now | fillnull value=0 "
            + " ".join(ev)
            + " | rename R_FM_* as R_F00_*, R_AM_* as R_L00_*, AGE_FM as F00_REALINFO, AGE_LM as L00_REALINFO"
            + " | table _time T00 F00 L00 G00 P00 BA BA_F00 BA_L00 F00_REALINFO L00_REALINFO "
            + dev + " " + ai + " R_F00_SEX_F R_F00_SEX_M R_L00_SEX_F R_L00_SEX_M " + age_out
            + " | sort 0 _time")

_RT_CONCURRENT_SPL = _build_rt_concurrent_spl()

def _rt_concurrent_load():
    try:
        rows = splunk_search(_RT_CONCURRENT_SPL)
    except Exception as e:
        print(f"  [rt_concurrent] 조회 실패: {e}")
        return [], "error"
    out = []
    for r in rows:
        o = {}
        for src, dst in _RT_CH_SUF:            # AI 파생: 채널별 7사 합
            o["DV_AI" + dst] = sum(_i(r.get(v + src)) for v in _RT_AI_VENDORS)
        for k, v in r.items():
            if k.startswith(_RT_AI_VENDORS):   # 원시 AI 벤더 드롭
                continue
            if k.startswith(_RT_DEV_REAL) and k[-2:] in ("_F", "_G", "_L", "_P"):
                o[k + "00"] = v                # 실장비 채널접미 _F → _F00
            else:
                o[k] = v
        out.append(o)
    return out, "splunk"

_RT_MSG_SPL = r"""search index=infobank earliest=@d latest=now
| stats values(*) as * by BOARD_KEY
| eval _time = strptime(MSG_DATE, "%Y%m%d%H%M%S")
| join type=left PGM_KEY [| inputlookup BROADPLAN.csv | search IS_END="N" | rename INFO_PGM_CODE as PGM_KEY | fields PGM_KEY, SEQ]
| eval FMAM=case(SEQ like "F%","FM",SEQ like "L%","AM")
| eval apart=case(FMAM=="FM" AND (MSG_COM=="001" OR MSG_COM=="023"),"FM_SUM_1", FMAM=="FM" AND MSG_COM=="007","FM_SUM_7", FMAM=="FM" AND (MSG_COM=="009" OR MSG_COM=="024"),"FM_SUM_9", FMAM=="AM" AND (MSG_COM=="001" OR MSG_COM=="023"),"AM_SUM_1", FMAM=="AM" AND MSG_COM=="007","AM_SUM_7", FMAM=="AM" AND (MSG_COM=="009" OR MSG_COM=="024"),"AM_SUM_9")
| timechart span=1m count by apart
| fillnull value=0
| rename FM_SUM_1 as F00_SMS, FM_SUM_9 as F00_GG, AM_SUM_1 as L00_SMS, AM_SUM_9 as L00_GG
| table _time F00_SMS F00_GG L00_SMS L00_GG | sort 0 _time"""

def _rt_msg_load():
    try:
        return splunk_search(_RT_MSG_SPL), "splunk"
    except Exception as e:
        print(f"  [rt_msg] 조회 실패: {e}")
        return [], "error"

_RT_INFLOW_SPL = (f"search index=tempsummary3 earliest=@d latest=now | fillnull value=0 F00 G00 L00 P00 T00 "
                  "| rename F00 as F00_START, G00 as G00_START, L00 as L00_START, P00 as P00_START "
                  "| table _time F00_START G00_START L00_START P00_START | sort 0 _time")

def _rt_inflow_load():
    try:
        return splunk_search(_RT_INFLOW_SPL), "splunk"
    except Exception as e:
        print(f"  [rt_inflow] 조회 실패: {e}")
        return [], "error"

_rt_concurrent_feed = Feed("rt_concurrent", _rt_concurrent_load, ttl_sec=60)
_rt_msg_feed        = Feed("rt_msg",        _rt_msg_load,        ttl_sec=60)
_rt_inflow_feed     = Feed("rt_inflow",     _rt_inflow_load,     ttl_sec=60)

def get_rt_concurrent() -> list:
    """오늘 1분 단위 확장 실시간 — 채널 동시자·디바이스×채널·보라×채널·인증 성별/연령 비율."""
    return _rt_concurrent_feed.get() or []

def get_rt_msg() -> list:
    """오늘 1분 단위 채널별 SMS·공감로그(GG) — [{_time,F00_SMS,F00_GG,L00_SMS,L00_GG}]."""
    return _rt_msg_feed.get() or []

def get_rt_inflow() -> list:
    """오늘 1분 단위 채널별 유입 — [{_time,F00_START,G00_START,L00_START,P00_START}]."""
    return _rt_inflow_feed.get() or []

# 최근 14일(오늘 제외) 일자별 피크값·피크시각 — 과거 불변 → 일일 캐시. 스코프별 지연 생성.
#   argmax 관용구: sort로 (date, 값) 정렬 후 stats last()가 그날 최대값 행의 시각·값.
_RT_PEAK_DAYS = 14
_rt_peak_feeds: dict = {}

def _rt_peak_loader(field: str, window=None):
    def load():
        spl = (f"search index={RT_INDEX} earliest=-{_RT_PEAK_DAYS}d@d latest=@d "
               f'| eval date=strftime(_time,"%Y-%m-%d"), hm=strftime(_time,"%H:%M") ')
        if window:                       # 프로그램 편성창 [start, end) 시간대만
            spl += f'| where hm>="{window[0]}" AND hm<"{window[1]}" '
        spl += (f"| sort 0 date {field} "
                f"| stats last(hm) as peak_hm last({field}) as peak_val by date "
                f"| sort 0 date")
        try:
            return splunk_search(spl), "splunk"
        except Exception as e:
            print(f"  [realtime peak] 조회 실패({field},{window}): {e}")
            return [], "error"
    return load

def get_realtime_peak_trend(field: str = "T00", window=None) -> list:
    """최근 14일 일자별 [{date, peak_hm, peak_val}]. window=(start,end) 주면 그 시간대 내 피크.
       프로그램 스코프는 편성창을 넘겨 슬롯 내 피크타임 이동을 추적."""
    if field not in ("T00", "F00", "L00", "G00", "P00"):
        field = "T00"
    key = (field, window[0], window[1]) if window else (field,)
    feed = _rt_peak_feeds.get(key)
    if feed is None:
        nm = "realtime_peak_" + "_".join(str(x).replace(":", "") for x in key)
        feed = Feed(nm, _rt_peak_loader(field, window), daily_at="00:05")
        _rt_peak_feeds[key] = feed
    return feed.get() or []


# ── 장기 아카이브 (real_dau.csv + summary_uuid_stats, 최대 10년) ─────
# 상세 KPI(raas_kpi_latest, 2026-03~)와 별도의 장기 이력 — 4개 지표만: dau·dau_r7(롤링WAU)·
#   dau_r30(롤링MAU)·dau_1min(1분이상 청취). 원천 real_dau.csv는 TYPE=DAY/WEEK/MONTH.
# 출력 형태 = '키 long + 지표 wide'(DATE, PGM_CODE, dau, dau_r7, dau_r30, dau_1min) — KPI 타임라인과
#   동일 모양. 엔티티 열 하드코딩(F01…M13) 제거(정규식으로 코드형만) → 새 프로그램 자동 유입.
# 과거 불변 → 일일 캐시. 저장소는 Splunk(로컬 저장 없음). F01은 자정 넘김이라 방송일 -1d 보정.
_HISTORY_SPL = r"""| inputlookup real_dau.csv
| eval _time = strptime(DATE, "%Y/%m/%d")
| where _time >= relative_time(now(),"-10y@y") AND _time < relative_time(now(),"@d")
| rename TOTAL_COUNT as T00, FM_COUNT as F00, AM_COUNT as L00, GM_COUNT as G00, PM_COUNT as P00
| search TYPE="DAY"
| fields - DATE TYPE
| untable _time PGM_CODE value
| where match(PGM_CODE, "^[TFLGPM][0-9][0-9]$")
| eval _time = if(PGM_CODE=="F01", strptime(strftime(relative_time(_time,"-1d@d"), "%Y/%m/%d"), "%Y/%m/%d"), _time)
| eval METRIC="dau"
| append [
    | inputlookup real_dau.csv
    | eval _time = strptime(DATE, "%Y/%m/%d")
    | where _time >= relative_time(now(),"-10y@y") AND _time < relative_time(now(),"@d")
    | rename TOTAL_COUNT as T00, FM_COUNT as F00, AM_COUNT as L00, GM_COUNT as G00, PM_COUNT as P00
    | search TYPE="WEEK"
    | fields - DATE TYPE
    | untable _time PGM_CODE value
    | where match(PGM_CODE, "^[TFLGPM][0-9][0-9]$")
    | eval _time = if(PGM_CODE=="F01", strptime(strftime(relative_time(_time,"-1d@d"), "%Y/%m/%d"), "%Y/%m/%d"), _time)
    | eval METRIC="dau_r7" ]
| append [
    | inputlookup real_dau.csv
    | eval _time = strptime(DATE, "%Y/%m/%d")
    | where _time >= relative_time(now(),"-10y@y") AND _time < relative_time(now(),"@d")
    | rename TOTAL_COUNT as T00, FM_COUNT as F00, AM_COUNT as L00, GM_COUNT as G00, PM_COUNT as P00
    | search TYPE="MONTH"
    | fields - DATE TYPE
    | untable _time PGM_CODE value
    | where match(PGM_CODE, "^[TFLGPM][0-9][0-9]$")
    | eval _time = if(PGM_CODE=="F01", strptime(strftime(relative_time(_time,"-1d@d"), "%Y/%m/%d"), "%Y/%m/%d"), _time)
    | eval METRIC="dau_r30" ]
| append [
    search index=summary_uuid_stats sourcetype=stats_1d earliest=-10y@y latest=@d
    | rename TOTAL_COUNT as T00, FM_COUNT as F00, AM_COUNT as L00, GM_COUNT as G00, PM_COUNT as P00
    | untable _time PGM_CODE value
    | where match(PGM_CODE, "^[TFLGPM][0-9][0-9]$")
    | eval METRIC="dau_1min" ]
| eval DATE = strftime(_time, "%Y/%m/%d")
| stats first(eval(if(METRIC=="dau",      value, null()))) as dau
        first(eval(if(METRIC=="dau_r7",   value, null()))) as dau_r7
        first(eval(if(METRIC=="dau_r30",  value, null()))) as dau_r30
        first(eval(if(METRIC=="dau_1min", value, null()))) as dau_1min
        by DATE, PGM_CODE
| table DATE, PGM_CODE, dau, dau_r7, dau_r30, dau_1min"""

# 아카이브 보유 지표 4종(= 출력 열 이름 = grounding _HIST_METRICS와 동일)
HISTORY_METRICS = ("dau", "dau_r7", "dau_r30", "dau_1min")

def _load_history():
    """장기 아카이브 조회 후 로드 시 1회 인덱싱 → {PGM_CODE: {DATE: row}} (KPI 타임라인과 동일 구조).
       10만+ 행이라도 매 호출 선형스캔 대신 코드별 dict 조회가 되게(조회는 그 코드 날짜만 순회)."""
    try:
        rows = splunk_search(_HISTORY_SPL, timeout=300)   # 10년 범위 — 장기 timeout
    except Exception as e:
        print(f"  [history] 장기 아카이브 조회 실패: {e}")
        return {}, "error"
    idx: dict = {}
    for r in rows:
        code = (r.get("PGM_CODE") or "").strip().upper()
        date = (r.get("DATE") or "").strip()
        if code and date:
            idx.setdefault(code, {})[date] = r
    return idx, "splunk"

_history_feed = Feed("history_metrics", _load_history, daily_at="07:10")

def get_history_index() -> dict:
    """장기 아카이브 인덱스 {PGM_CODE: {DATE: {dau,dau_r7,dau_r30,dau_1min}}} (일일 캐시)."""
    return _history_feed.get() or {}

def get_history_series(code: str, metric: str) -> list:
    """코드+지표의 장기 '아카이브' 시계열 [(DATE, float)] 오름차순. metric ∈ HISTORY_METRICS.
       인덱스에서 그 코드의 날짜만 순회(로드 시 1회 인덱싱 덕에 빠름)."""
    out = []
    for d, r in (get_history_index().get((code or "").upper()) or {}).items():
        v = _fn(r.get(metric))
        if v is not None:
            out.append((d, v))
    return sorted(out)

def get_history_source() -> str:
    return _history_feed.source()


# ── 참여(문자 SMS · 공감로그 GG) 아카이브 (SUMMARY_INDEX_02, 과거 1년, 하루 1회) ──
# 프로그램별 일자별 참여 — 평일·비공휴일만(원천 쿼리 필터). 출력=키 long+지표 wide.
# 지표: SMS·GG(참여 건수), SMS_WR·GG_WR(참여자수, _WR=작성자), SMS_RATIO·GG_RATIO(1인당),
#   TOTAL(=GG+SMS)·TOTAL_WR(=GG_WR+SMS_WR). 필드 정의는 온톨로지(raas:*Participation).
ENGAGE_METRICS = ("SMS", "GG", "TOTAL", "SMS_WR", "GG_WR", "TOTAL_WR", "SMS_RATIO", "GG_RATIO")

_ENGAGE_SPL = r"""search index=SUMMARY_INDEX_02 earliest=-365d latest=@d
| lookup HOLIDAY_0 HOLIDAY_DAY AS date_mday, HOLIDAY_WEEK AS date_wday, HOLIDAY_MONTH AS date_month, HOLIDAY_YEAR AS date_year output HOLIDAY_CHECK
| fillnull value="no" HOLIDAY_CHECK
| search date_wday="monday" OR date_wday="tuesday" OR date_wday="wednesday" OR date_wday="thursday" OR date_wday="friday" HOLIDAY_CHECK = "no"
| eval DATE = strftime(_time, "%Y/%m/%d")
| eval SMS_RATIO=round(SMS/SMS_WR,1)
| eval GG_RATIO=round(GG/GG_WR,1)
| eval TOTAL = GG + SMS
| eval TOTAL_WR = GG_WR + SMS_WR
| rename SEQ as PGM_CODE
| table DATE, PGM_CODE, SMS, GG, SMS_WR, GG_WR, SMS_RATIO, GG_RATIO, TOTAL, TOTAL_WR"""

def _load_engagement():
    """참여 아카이브 조회 후 {PGM_CODE: {DATE: row}} 인덱싱(로드 시 1회, 아카이브와 동일 패턴)."""
    try:
        rows = splunk_search(_ENGAGE_SPL, timeout=120)
    except Exception as e:
        print(f"  [engagement] 참여 데이터 조회 실패: {e}")
        return {}, "error"
    idx: dict = {}
    for r in rows:
        code = (r.get("PGM_CODE") or "").strip().upper()
        date = (r.get("DATE") or "").strip()
        if code and date:
            idx.setdefault(code, {})[date] = r
    return idx, "splunk"

_engage_feed = Feed("engagement", _load_engagement, daily_at="07:15")

def get_engagement_index() -> dict:
    """참여 인덱스 {PGM_CODE: {DATE: {SMS,GG,...}}} (일일 캐시)."""
    return _engage_feed.get() or {}

def get_engagement_series(code: str, metric: str) -> list:
    """코드+지표의 참여 시계열 [(DATE, float)] 오름차순. metric ∈ ENGAGE_METRICS."""
    out = []
    for d, r in (get_engagement_index().get((code or "").upper()) or {}).items():
        v = _fn(r.get(metric))
        if v is not None:
            out.append((d, v))
    return sorted(out)


def get_history_series_merged(code: str, metric: str) -> list:
    """장기 아카이브 + 상세 KPI 병합 시계열 [(DATE, float)].
       메인 소스는 상세 KPI(raas_kpi_latest, 더 정교) — 겹치는 날짜는 상세 KPI 값이 우선.
       아카이브는 상세 KPI에 없는 과거 기간만 채운다."""
    merged = dict(get_history_series(code, metric))       # 아카이브(과거 폭넓음)
    for d, row in (get_timeline() or {}).get(code, {}).items():
        v = _fn(row.get(metric))
        if v is not None:
            merged[(d or "").strip()] = v                 # 상세 KPI 우선 덮어쓰기
    return sorted(merged.items())


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
