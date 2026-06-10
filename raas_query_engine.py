"""
RAAS 자연어 질의 엔진 v3.0
- timeline 기반 (raas_kpi_latest.csv 단일 소스)
- splunk_search / SPLUNK_HOST 의존성 제거
- 환각 방지 강화 (데이터 출처 명시, 범위 밖 질문 거부)
"""

import json
import os
import re
import urllib.request
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pandas as _pd
from pathlib import Path
from raas_prompts import QUERY_SYSTEM_PROMPT
from raas_briefing_context import build_query_context

_RULES_PATH = Path(__file__).parent / "data" / "raas_rules.md"

def _load_rules() -> str:
    """data/raas_rules.md 를 읽어 시스템 프롬프트 추가 섹션으로 반환. 파일 없으면 빈 문자열."""
    try:
        text = _RULES_PATH.read_text(encoding="utf-8")
        # HTML 주석 제거 (<!-- ... -->)
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL).strip()
        return f"\n\n[운영 규칙 — 반드시 준수]\n{text}" if text else ""
    except FileNotFoundError:
        return ""

# ── 설정 ──────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
HAIKU_MODEL       = os.getenv("HAIKU_MODEL",  "claude-haiku-4-5-20251001")

# 단순 조회 intent — Haiku로 처리 (ranking·trend 계열은 Claude가 1~2줄만 작성)
_HAIKU_INTENTS = {'ranking', 'trend', 'compare_trend', 'dual_trend', 'overview'}
# ──────────────────────────────────────────────────────────


# =============================================================================
# 데이터 유틸리티 (raas_briefing_engine 통합)
# =============================================================================

def _f(v, d=0.0):
    try: return float(v) if v not in (None, '', 'None', 'null') else d
    except: return d

def _i(v, d=0):
    try: return int(float(v)) if v not in (None, '', 'None', 'null') else d
    except: return d

def _fn(v):
    try: return float(v) if v not in (None, '', 'None', 'null') else None
    except: return None

def _yn(v):
    """'Y'/'N' 문자열 그대로 반환. 그 외 값·빈값 → None."""
    s = (v or '').strip().upper()
    return s if s in ('Y', 'N') else None

PGM_F = ['F01','F02','F03','F04','F05','F06','F07','F08','F09','F10','F11','F12','F13']
PGM_L = ['L01','L02','L03','L04','L05','L06','L07','L08','L09','L10','L11',
          'L12','L13','L14','L15','M05','M07','M10','M11']
CH  = ['F00', 'L00', 'G00', 'P00']
ALL = PGM_F + PGM_L


def _pgm_name(code, row=None, default=None):
    """프로그램/채널/플랫폼 코드 → 표시 이름.
    우선순위: TTL(어댑터) → row.pgm_name → code
    """
    try:
        from raas_onto import get_adapter
        label = get_adapter()._onto.label_ko(f"raas:{code}")
        if label and label != f"raas:{code}":
            return label
    except Exception:
        pass
    if row is not None:
        nm = (row.get('pgm_name') or '').strip()
        if nm:
            return nm
    return default if default is not None else code


def _load_timeline(search):
    """raas_kpi_latest.csv → timeline dict {PGM_CODE: {DATE: row}}."""
    source = 'splunk'
    try:
        rows = search("| inputlookup raas_kpi_latest.csv")
    except Exception as e:
        base = os.path.dirname(__file__)
        candidates = [
            os.path.join(base, 'data', 'raas_kpi_latest.csv'),
            os.path.join(base, 'raas_kpi_latest.csv'),
        ]
        local_path = next((p for p in candidates if os.path.exists(p)), None)
        if local_path:
            print(f"  [fallback] Splunk {type(e).__name__} → local CSV: {local_path}")
            df = _pd.read_csv(local_path, dtype=str, keep_default_na=False)
            rows = df.to_dict(orient='records')
            source = 'csv_fallback'
        else:
            raise
    timeline = {}
    for r in rows:
        code = r.get('PGM_CODE')
        date = r.get('DATE')
        if not code or not date:
            continue
        timeline.setdefault(code, {})[date] = r
    if timeline:
        all_dates = set()
        for code_rows in timeline.values():
            all_dates.update(code_rows.keys())
        print(f"  [timeline] source={source}, {len(timeline)} codes x {len(all_dates)} dates "
              f"({min(all_dates)} ~ {max(all_dates)})")
    return timeline, source


def _latest_snapshot(timeline):
    """각 PGM_CODE의 최신 행만 추출."""
    return {code: date_rows[max(date_rows.keys())]
            for code, date_rows in timeline.items() if date_rows}


def _load(search):
    """호환성 유지용: _load_timeline + _latest_snapshot."""
    timeline, _ = _load_timeline(search)
    return _latest_snapshot(timeline)


def get_timeseries(timeline, code, days=30):
    """특정 코드의 최근 N일 {date: row} dict."""
    date_rows = timeline.get(code, {})
    sorted_dates = sorted(date_rows.keys(), reverse=True)[:days]
    return {d: date_rows[d] for d in sorted_dates}


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


def get_codes_summary(timeline):
    """코드별 데이터 가용성 요약."""
    return {code: {'dates': len(dr), 'latest': max(dr.keys()) if dr else None}
            for code, dr in timeline.items()}


# =============================================================================

_WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]

def _weekday_ko(date_str: str) -> str:
    """'YYYY/MM/DD' 또는 'YYYY-MM-DD' → 한국 요일 약칭. Python datetime 기준으로 계산."""
    try:
        return _WEEKDAY_KO[datetime.strptime(date_str.replace("/", "-"), "%Y-%m-%d").weekday()]
    except Exception:
        return ""

# ── PGM_CODE 매핑 ──────────────────────────────────────────
def _build_scope_map() -> dict:
    """TTL에서 채널·플랫폼 키워드→코드 맵 빌드. 어댑터 실패 시 하드코딩 fallback."""
    try:
        from raas_onto import get_adapter
        adapter = get_adapter()
        idx = {}
        for cls in ("raas:Platform", "raas:Channel"):
            for subj in adapter._onto.instances_of(cls):
                code = adapter._onto.value_str(adapter._onto.get_one(subj, "raas:code"))
                if not code:
                    continue
                idx[code] = code
                idx[code.lower()] = code
                for label in adapter._all_labels(subj):
                    idx[label.lower()] = code
        return idx
    except Exception:
        return {
            'platform': 'T00', 'all': 'T00', 'T00': 'T00',
            '전체': 'T00', '고릴라': 'T00',
            'powerfm': 'F00', '파워fm': 'F00', '파워': 'F00', 'F00': 'F00',
            'lovefm': 'L00', '러브fm': 'L00', '러브': 'L00', 'L00': 'L00',
            'gorillam': 'G00', '고릴라m': 'G00', 'G00': 'G00',
            'pickch': 'P00', '픽채널': 'P00', 'P00': 'P00',
        }

def _build_keyword_index() -> dict:
    """TTL 어댑터에서 프로그램 키워드→코드 인덱스 빌드."""
    try:
        from raas_onto import get_adapter
        adapter = get_adapter()
        idx = {}
        for label, subjects in adapter._keyword_index.items():
            lbl = label.lower()
            for subj in subjects:
                code = adapter._onto.value_str(adapter._onto.get_one(subj, "raas:code"))
                if code:
                    idx[lbl] = code
        return idx
    except Exception:
        return {}


# ── 차트 빌더 ──────────────────────────────────────────────
_METRIC_LABELS = {
    # 볼륨
    'dau':              ('DAU',          '명'),
    'wau':              ('WAU',          '명'),
    'mau':              ('MAU',          '명'),
    'dau_r7':           ('롤링 7일 WAU', '명'),
    'dau_r30':          ('롤링 30일 MAU','명'),
    # 신규/복귀 사용자 수
    'new':              ('신규(일)',      '명'),
    'new_week':         ('신규(주)',      '명'),
    'new_mon':          ('신규(월)',      '명'),
    'react':            ('복귀(일)',      '명'),
    'react_week':       ('복귀(주)',      '명'),
    'react_mon':        ('복귀(월)',      '명'),
    # 비율 지표
    'react_rate':       ('복귀율',        '%'),
    'react_rate_week':  ('복귀율(주)',    '%'),
    'react_rate_mon':   ('복귀율(월)',    '%'),
    'churn_rate':       ('이탈률',        '%'),
    'churn_rate_week':  ('이탈률(주)',    '%'),
    'churn_rate_mon':   ('이탈률(월)',    '%'),
    'real_rate':        ('실청취율',      '%'),
    'real_rate_week':   ('실청취율(주)', '%'),
    'real_rate_mon':    ('실청취율(월)', '%'),
    'deep_rate':        ('깊은청취율',   '%'),
    'deep_rate_week':   ('깊은청취율(주)','%'),
    'deep_rate_mon':    ('깊은청취율(월)','%'),
    'engage_rate':      ('참여율',        '%'),
    'engage_rate_week': ('참여율(주)',    '%'),
    'engage_rate_mon':  ('참여율(월)',    '%'),
    'habit_rate':       ('습관형성률',   '%'),
    'habit_rate_week':  ('습관형성률(주)','%'),
    'habit_rate_mon':   ('습관형성률(월)','%'),
    # 유지율
    'd1_ret':           ('D1유지율(전체)','%'),
    'd7_ret':           ('D7유지율(전체)','%'),
    'w1_ret':           ('W1유지율(전체)','%'),
    'm1_ret':           ('M1유지율(전체)','%'),
    'new_d1_ret':       ('D1유지율(신규)','%'),
    'new_d7_ret':       ('D7유지율(신규)','%'),
    'new_w1_ret':       ('W1유지율(신규)','%'),
    'new_m1_ret':       ('M1유지율(신규)','%'),
}

def _metric_meta(metric_field: str):
    return _METRIC_LABELS.get(metric_field, (metric_field, ''))


# ── 지표 cadence 분류 ─────────────────────────────────────────────────────────
_WEEKLY_FIELDS = {
    'wau', 'wau_1min', 'wau_10min',
    'new_week', 'react_week',
    'churn_rate_week', 'react_rate_week', 'deep_rate_week',
    'engage_rate_week', 'habit_rate_week', 'real_rate_week',
    'w1_ret', 'new_w1_ret',
}
_MONTHLY_FIELDS = {
    'mau', 'mau_1min', 'mau_10min',
    'new_mon', 'react_mon',
    'churn_rate_mon', 'react_rate_mon', 'deep_rate_mon',
    'engage_rate_mon', 'habit_rate_mon', 'real_rate_mon',
    'm1_ret', 'new_m1_ret',
}

def _get_metric_cadence(field_name: str) -> str:
    if field_name in _WEEKLY_FIELDS or field_name.endswith('_week'):
        return 'weekly'
    if field_name in _MONTHLY_FIELDS or field_name.endswith('_mon'):
        return 'monthly'
    return 'daily'

def _downsample_weekly(points: list) -> list:
    """ISO 주 단위로 1포인트 (주 내 마지막 날). 현재 진행 중인 미완성 주는 제외."""
    if not points:
        return []
    buckets = {}
    for p in points:
        try:
            d = datetime.strptime(p['date'].replace('/', '-'), '%Y-%m-%d')
            key = d.isocalendar()[:2]  # (year, iso_week)
            if key not in buckets or p['date'] > buckets[key]['date']:
                buckets[key] = p
        except Exception:
            continue
    if not buckets:
        return []
    # 마지막 포인트의 요일이 일요일(ISO 7)이 아니면 해당 주는 미완성 → 제외
    try:
        last_d = datetime.strptime(points[-1]['date'].replace('/', '-'), '%Y-%m-%d')
        if last_d.isoweekday() != 7:
            incomplete_key = last_d.isocalendar()[:2]
            buckets.pop(incomplete_key, None)
    except Exception:
        pass
    return [buckets[k] for k in sorted(buckets.keys())]

def _downsample_monthly(points: list) -> list:
    """월 단위로 1포인트 (월 내 마지막 날). 현재 진행 중인 미완성 월은 제외."""
    import calendar as _cal
    if not points:
        return []
    buckets = {}
    for p in points:
        key = p['date'].replace('/', '-')[:7]  # YYYY-MM
        if key not in buckets or p['date'] > buckets[key]['date']:
            buckets[key] = p
    if not buckets:
        return []
    # 마지막 포인트가 월말이 아니면 해당 월은 미완성 → 제외
    try:
        last_d = datetime.strptime(points[-1]['date'].replace('/', '-'), '%Y-%m-%d')
        _, last_day = _cal.monthrange(last_d.year, last_d.month)
        if last_d.day != last_day:
            incomplete_key = points[-1]['date'].replace('/', '-')[:7]
            buckets.pop(incomplete_key, None)
    except Exception:
        pass
    return [buckets[k] for k in sorted(buckets.keys())]

def _apply_cadence_downsample(pts: list, cadence: str) -> list:
    if cadence == 'weekly':
        return _downsample_weekly(pts)
    if cadence == 'monthly':
        return _downsample_monthly(pts)
    return pts


def build_chart_timeseries(title, metric, unit, points, source, points_prev=None, cadence='daily', initial_days=None):
    """시계열 → sparkline dict. points < 2이면 None."""
    pts = _apply_cadence_downsample(
        [p for p in points if p.get('value') is not None], cadence
    )
    if len(pts) < 2:
        return None
    values = [p['value'] for p in pts]
    first, latest = values[0], values[-1]
    change_pct = round((latest - first) / first * 100, 1) if first else None
    # initial_days를 다운샘플 후 포인트 수 기준으로 변환 (프런트엔드 기간 버튼 선택용)
    if initial_days is not None:
        if cadence == 'weekly':
            initial_days = max(1, round(initial_days / 7))
        elif cadence == 'monthly':
            initial_days = max(1, round(initial_days / 30))
    result = {
        "type": "timeseries",
        "title": title,
        "metric": metric,
        "unit": unit,
        "cadence": cadence,
        "initial_days": initial_days,
        "points": pts,
        "summary": {
            "min": min(values),
            "max": max(values),
            "avg": round(sum(values) / len(values), 1),
            "latest": latest,
            "change_pct": change_pct,
        },
        "source": source,
    }
    if points_prev:
        prev_pts = _apply_cadence_downsample(
            [p for p in points_prev if p.get('value') is not None], cadence
        )
        # prev가 메인보다 길면 최신 데이터 기준으로 정렬; 메인은 항상 전체 유지
        if len(prev_pts) > len(pts):
            prev_pts = prev_pts[-len(pts):]
        if len(prev_pts) >= 2:
            result['points_prev'] = prev_pts
    return result


def build_chart_multi_timeseries(title, metric, unit, series, source, cadence='daily', initial_days=None):
    """멀티시리즈 시계열 → timeseries_multi dict. valid series < 2이면 None."""
    valid = []
    for s in series:
        pts = _apply_cadence_downsample(
            [p for p in s.get('points', []) if p.get('value') is not None], cadence
        )
        if len(pts) >= 2:
            valid.append({**s, 'points': pts})
    if len(valid) < 2:
        return None
    if initial_days is not None:
        if cadence == 'weekly':
            initial_days = max(1, round(initial_days / 7))
        elif cadence == 'monthly':
            initial_days = max(1, round(initial_days / 30))
    return {
        "type": "timeseries_multi",
        "title": title,
        "metric": metric,
        "unit": unit,
        "cadence": cadence,
        "initial_days": initial_days,
        "series": valid,
        "source": source,
    }


def build_chart_comparison(title, metric, unit, date, items, source):
    """비교 → bar dict. items < 2이면 None."""
    valid = [it for it in items if it.get('value') is not None]
    if len(valid) < 2:
        return None
    return {
        "type": "comparison",
        "title": title,
        "metric": metric,
        "unit": unit,
        "date": date,
        "items": valid,
        "source": source,
    }


def build_chart_data(data: dict, intent: dict, question: str):
    """extract_data 결과 + intent → chart_data dict 또는 None."""
    try:
        # 게스트 히스토리 질문 + 차트 요청 키워드 없음 → 차트 불필요
        _chart_kws = ('그래프', '차트', '추이', '시각화', '꺾은선')
        if data.get('guestname_history') and not any(k in question for k in _chart_kws):
            return None

        intent_type = intent.get('intent', 'general')
        scope       = data.get('scope', 'T00')
        scope_name  = data.get('scope_name', scope)
        date_max    = (data.get('date_max') or '').replace('/', '-')

        # trend → timeseries (+ 전주 비교선)
        if intent_type == 'trend' and 'trend' in data:
            t = data['trend']
            points = [{"date": d.replace('/', '-'), "value": v}
                      for d, v in t['data'] if v is not None]
            metric, unit = _metric_meta(t['metric_field'])
            cadence = _get_metric_cadence(t['metric_field'])
            initial_days = t.get('initial_days')
            points_prev = None
            if data.get('trend_prev') and data['trend_prev'].get('data'):
                points_prev = [{"date": d.replace('/', '-'), "value": v}
                               for d, v in data['trend_prev']['data'] if v is not None]
            return build_chart_timeseries(
                title=f"{scope_name} {metric}",
                metric=metric, unit=unit, points=points,
                source=f"timeline:{scope}/{t['metric_field']}",
                points_prev=points_prev, cadence=cadence, initial_days=initial_days
            )

        # dual_trend → 동일 단위면 timeseries_multi, 혼합 단위면 timeseries_dual
        if intent_type == 'dual_trend' and data.get('dual_trend'):
            dt = data['dual_trend']
            _DT_COLORS = ['#185fa5', '#1d9e75', '#d97706']
            series = []
            _dt_cadences = []
            for i, s in enumerate(dt['series']):
                pts = [{"date": d.replace('/', '-'), "value": v}
                       for d, v in s.get('data', []) if v is not None]
                if len(pts) >= 2:
                    series.append({
                        'label': s['label'], 'unit': s['unit'],
                        'color': _DT_COLORS[i % len(_DT_COLORS)], 'points': pts,
                    })
                    _dt_cadences.append(_get_metric_cadence(s.get('metric_field', '')))
            # 모든 시리즈가 동일 cadence면 사용, 혼합이면 daily
            _dt_cadence = _dt_cadences[0] if _dt_cadences and len(set(_dt_cadences)) == 1 else 'daily'
            _dt_initial = dt.get('initial_days')
            if len(series) >= 2:
                title = f"{scope_name} {' · '.join(s['label'] for s in series)} 추이"
                _units = [s['unit'] for s in series]
                if len(set(_units)) == 1:
                    # 동일 단위 → timeseries_multi (공유 Y스케일)
                    multi = build_chart_multi_timeseries(
                        title=title, metric=' · '.join(s['label'] for s in series),
                        unit=_units[0], series=series, source=f"timeline:{scope}",
                        cadence=_dt_cadence, initial_days=_dt_initial
                    )
                    if multi:
                        return multi
                else:
                    # 혼합 단위 → timeseries_dual (독립 Y스케일)
                    _ds_series = [
                        {**s, 'points': _apply_cadence_downsample(s['points'], _dt_cadence)}
                        for s in series
                    ]
                    return {
                        "type": "timeseries_dual",
                        "title": title,
                        "cadence": _dt_cadence,
                        "initial_days": _dt_initial,
                        "series": _ds_series,
                        "source": f"timeline:{scope}",
                    }
            # 1개 시리즈만 유효하면 단일 timeseries로 fallback
            if len(series) == 1:
                s0 = series[0]
                metric0, unit0 = s0['label'], s0['unit']
                return build_chart_timeseries(
                    title=f"{scope_name} {metric0} 추이",
                    metric=metric0, unit=unit0, points=s0['points'],
                    source=f"timeline:{scope}/dual_fallback",
                    cadence=_dt_cadence, initial_days=_dt_initial
                )

        # compare_trend → timeseries_multi (fallback: snapshot comparison bar)
        if intent_type == 'compare_trend' and data.get('compare_trend'):
            ct = data['compare_trend']
            metric, unit = _metric_meta(ct['metric_field'])
            _ct_cadence = _get_metric_cadence(ct['metric_field'])
            _is_pgm_grp = intent.get('pgm_group_trend', False)
            _grp_label = '프로그램별' if _is_pgm_grp else '채널별'
            _SERIES_COLORS = [
                '#185fa5', '#1d9e75', '#d97706', '#dc2626',
                '#7c3aed', '#db2777', '#0891b2', '#65a30d',
                '#92400e', '#1e40af', '#065f46', '#7f1d1d',
                '#4c1d95', '#064e3b', '#78350f', '#1e3a5f',
            ]
            series = [
                {**s, 'color': _SERIES_COLORS[i % len(_SERIES_COLORS)]}
                for i, s in enumerate(ct['series'])
            ]
            multi = build_chart_multi_timeseries(
                title=f"{_grp_label} {metric} 추이",
                metric=metric, unit=unit, series=series,
                source=f"timeline:multi/{ct['metric_field']}",
                cadence=_ct_cadence, initial_days=ct.get('initial_days')
            )
            if multi:
                return multi
            # 추이 데이터 부족 시 스냅샷 최신값으로 bar chart fallback
            snap_items = []
            for s in ct['series']:
                pts = s.get('points', [])
                if pts:
                    snap_items.append({'label': s['label'], 'value': pts[-1]['value']})
            if len(snap_items) >= 2:
                return build_chart_comparison(
                    title=f"{_grp_label} {metric} 비교 ({date_max})",
                    metric=metric, unit=unit,
                    date=date_max, items=snap_items,
                    source=f"snapshot:{date_max}"
                )

        # overview → 편성시간 순 전 지표 현황표
        if intent_type == 'overview' and data.get('overview'):
            ov_rows = data['overview']
            _ch = data.get('overview_channel')
            _wknd_sfx = ''
            if _ch == 'channels':
                _ch_name = '채널별'
            elif _ch:
                _ch_name = _pgm_name(_ch, default='')
                if _ch == 'L00':
                    _wknd_sfx = ' (주말 포함)' if data.get('overview_include_weekend') else ' (주말 제외)'
            else:
                _ch_name = '전체'
            # 질문에서 명시된 지표만 추출 (없으면 None → 전체 표시)
            _q_low = question.lower()
            _OV_GROUPS = [
                (['dau'],                           ['dau', 'dau_wow']),
                (['wau'],                           ['wau', 'dau_week_wow']),
                (['mau'],                           ['mau', 'dau_mon_wow']),
                (['7일롤링', 'r7', '롤링wau', '롤링7일wau', '7일wau', '최근7일 활성사용자', '7일mau'], ['dau_r7']),
                (['30일롤링', 'r30', '롤링mau', '롤링30일mau', '30일mau', '최근30일 활성사용자'],    ['dau_r30']),
                (['신규'],                           ['new', 'new_week', 'new_mon']),
                (['복귀율'],                         ['react_rate', 'react_rate_week', 'react_rate_mon']),
                (['복귀'],                           ['react', 'react_week', 'react_mon']),
                (['이탈'],                           ['churn_rate', 'churn_rate_week', 'churn_rate_mon']),
                (['실청취'],                         ['real_rate', 'real_rate_week', 'real_rate_mon']),
                (['깊은청취'],                       ['deep_rate', 'deep_rate_week', 'deep_rate_mon']),
                (['참여'],                           ['engage_rate', 'engage_rate_week', 'engage_rate_mon']),
                (['습관'],                           ['habit_rate', 'habit_rate_week', 'habit_rate_mon']),
                (['유지율', 'd1', 'd7', 'w1', 'm1', '리텐션', '코호트'],
                 ['d1_ret', 'd7_ret', 'w1_ret', 'm1_ret',
                  'new_d1_ret', 'new_d7_ret', 'new_w1_ret', 'new_m1_ret']),
            ]
            _matched = []
            for kws, fields in _OV_GROUPS:
                if any(kw in _q_low for kw in kws):
                    _matched.extend(fields)
            _ov_columns = _matched if _matched else None  # None → 전체
            _ov_date_label = (data.get('overview_date') or date_max).replace('/', '-')
            return {
                'type':    'table',
                'subtype': 'overview',
                'title':   f"{'채널별 현황' if _ch == 'channels' else f'{_ch_name} 프로그램 현황 (편성시간 순)'} ({_ov_date_label}){_wknd_sfx}",
                'rows':    ov_rows,
                'columns': _ov_columns,
                'source':  f"snapshot:{_ov_date_label}",
            }

        # compare → comparison (채널별 DAU)
        if intent_type == 'compare' and data.get('compare'):
            items = [{"label": c['name'], "value": c['dau']}
                     for c in data['compare'] if c.get('dau')]
            items.sort(key=lambda x: x['value'], reverse=True)
            return build_chart_comparison(
                title="채널별 DAU 비교",
                metric="DAU", unit="명",
                date=date_max, items=items,
                source=f"snapshot:{date_max}"
            )

        # ranking → table (TOP10, 전 지표 포함)
        if intent_type == 'ranking' and data.get('ranking'):
            _SKIP = {'code', 'name'}  # rank/dau는 항상 포함
            rows = []
            for i, r in enumerate(data['ranking']):
                if not r.get('dau') and not r.get('wau') and not r.get('mau'):
                    continue
                entry = {'rank': i + 1, 'name': r['name'], 'code': r['code']}
                for k, v in r.items():
                    if k not in _SKIP and v is not None:
                        entry[k] = v
                rows.append(entry)
            if rows:
                _rm2 = data.get('ranking_metric', 'dau')
                _rm2_label = {
                    'dau': 'DAU', 'wau': 'WAU', 'mau': 'MAU',
                    'dau_r7': '롤링WAU', 'dau_r30': '롤링MAU',
                    'dau_1min': '1분이상청취자', 'dau_10min': '10분이상청취자',
                    'wau_1min': '1분이상청취자(주)', 'wau_10min': '10분이상청취자(주)',
                    'mau_1min': '1분이상청취자(월)', 'mau_10min': '10분이상청취자(월)',
                    'new': '신규(일)', 'new_week': '신규(주)', 'new_mon': '신규(월)',
                    'react': '복귀(일)', 'react_week': '복귀(주)', 'react_mon': '복귀(월)',
                    'react_rate': '복귀율(일)', 'react_rate_week': '복귀율(주)', 'react_rate_mon': '복귀율(월)',
                    'churn_rate': '이탈율(일)', 'churn_rate_week': '이탈율(주)', 'churn_rate_mon': '이탈율(월)',
                    'real_rate': '실청취율(일)', 'real_rate_week': '실청취율(주)', 'real_rate_mon': '실청취율(월)',
                    'deep_rate': '깊은청취율(일)', 'deep_rate_week': '깊은청취율(주)', 'deep_rate_mon': '깊은청취율(월)',
                    'engage_rate': '참여율(일)', 'engage_rate_week': '참여율(주)', 'engage_rate_mon': '참여율(월)',
                    'habit_rate': '습관형성율(일)', 'habit_rate_week': '습관형성율(주)', 'habit_rate_mon': '습관형성율(월)',
                    'd1_ret': 'D1유지(전체)', 'd7_ret': 'D7유지(전체)',
                    'w1_ret': 'W1유지(전체)', 'm1_ret': 'M1유지(전체)',
                    'new_d1_ret': 'D1유지(신규)', 'new_d7_ret': 'D7유지(신규)',
                    'new_w1_ret': 'W1유지(신규)', 'new_m1_ret': 'M1유지(신규)',
                }.get(_rm2, _rm2.upper())
                _ch2 = data.get('ranking_channel')
                _ch2_name = _pgm_name(_ch2, default='') if _ch2 else ''
                _scope_prefix = f"{_ch2_name} " if _ch2_name else ""
                _show_all = data.get('ranking_show_all', False)
                _wknd_suffix = ''
                if _ch2 == 'L00':
                    _wknd_suffix = ' (주말 포함)' if data.get('ranking_include_weekend') else ' (주말 제외)'
                # 주/월/기간 지표 또는 집계 랭킹 → 생/녹·보라 배지 숨김
                _PERIOD_METRICS = {
                    'wau', 'mau', 'dau_r7', 'dau_r30',
                    'wau_1min', 'wau_10min', 'mau_1min', 'mau_10min',
                    'new_week', 'new_mon', 'react_week', 'react_mon',
                    'react_rate_week', 'react_rate_mon',
                    'churn_rate_week', 'churn_rate_mon',
                    'deep_rate_week', 'deep_rate_mon',
                    'engage_rate_week', 'engage_rate_mon',
                    'habit_rate_week', 'habit_rate_mon',
                    'real_rate_week', 'real_rate_mon',
                    'd1_ret', 'd7_ret', 'w1_ret', 'm1_ret',
                    'new_d1_ret', 'new_d7_ret', 'new_w1_ret', 'new_m1_ret',
                }
                _period_ranking = (
                    bool(data.get('ranking_agg_func')) or
                    bool(data.get('ranking_date_range')) or
                    _rm2 in _PERIOD_METRICS
                )
                _title_date = data.get('ranking_date_range') or data.get('ranking_ref_date') or date_max
                _title_date = _title_date.replace('/', '-')
                return {
                    'type':           'table',
                    'subtype':        'ranking',
                    'title':          f"{_scope_prefix}프로그램 {_rm2_label} TOP{len(rows)}{_wknd_suffix} ({_title_date})",
                    'sort_metric':    _rm2,
                    'show_all':       _show_all,
                    'period_ranking': _period_ranking,
                    'extra_fields':   data.get('ranking_extra_fields') or [],
                    'rows':           rows,
                    'source':         f"snapshot:{date_max}",
                }

        # health → trend 있으면 timeseries, 없으면 ranking comparison
        if intent_type == 'health':
            if 'trend' in data:
                t = data['trend']
                points = [{"date": d.replace('/', '-'), "value": v}
                          for d, v in t['data'] if v is not None]
                metric, unit = _metric_meta(t['metric_field'])
                _h_cadence = _get_metric_cadence(t['metric_field'])
                _h_initial = t.get('initial_days')
                _h_prev = None
                if data.get('trend_prev') and data['trend_prev'].get('data'):
                    _h_prev = [{"date": d.replace('/', '-'), "value": v}
                               for d, v in data['trend_prev']['data'] if v is not None]
                return build_chart_timeseries(
                    title=f"{scope_name} {metric} 추이",
                    metric=metric, unit=unit, points=points,
                    source=f"timeline:{scope}/{t['metric_field']}",
                    points_prev=_h_prev, cadence=_h_cadence, initial_days=_h_initial
                )
            if data.get('ranking'):
                _rm_h = data.get('ranking_metric', 'dau')
                items = [{"label": r['name'], "value": r.get(_rm_h) or r.get('dau')}
                         for r in data['ranking'][:5] if r.get(_rm_h) or r.get('dau')]
                _rm_h_label = {'dau': 'DAU', 'dau_1min': '1분이상청취자', 'dau_10min': '10분이상청취자'}.get(_rm_h, _rm_h.upper())
                return build_chart_comparison(
                    title=f"프로그램 {_rm_h_label} TOP5",
                    metric=_rm_h_label, unit="명",
                    date=date_max, items=items,
                    source=f"snapshot:{date_max}"
                )

        # funnel + 유지율 키워드 → D1/D7/W1/M1 멀티라인 timeseries 우선
        if intent_type == 'funnel' and data.get('retention_trend'):
            _RET_COLORS = ['#185fa5', '#1d9e75', '#d97706', '#dc2626']

            def _make_ret_chart(trend_list, cohort_label):
                series = []
                for i, s in enumerate(trend_list):
                    pts = [{"date": d.replace('/', '-'), "value": v}
                           for d, v in s['data'] if v is not None]
                    if len(pts) >= 2:
                        series.append({
                            'label': s['label'], 'unit': '%',
                            'color': _RET_COLORS[i % len(_RET_COLORS)], 'points': pts,
                        })
                if len(series) >= 2:
                    return build_chart_multi_timeseries(
                        title=f"{cohort_label} 코호트 유지율 추이 (D1·D7·W1·M1)",
                        metric="유지율", unit="%", series=series,
                        source=f"timeline:{data.get('scope', 'T00')}/retention",
                        initial_days=data.get('retention_trend_initial_days', 30),
                    )
                return None

            if data.get('retention_want_both') and data.get('retention_trend_new'):
                c1 = _make_ret_chart(data['retention_trend'], '전체')
                c2 = _make_ret_chart(data['retention_trend_new'], '신규')
                charts = [c for c in [c1, c2] if c]
                if len(charts) == 2:
                    return {'type': 'multi_chart', 'charts': charts}
                elif charts:
                    return charts[0]
            else:
                _cohort_label = data.get('retention_cohort_type', '전체')
                multi = _make_ret_chart(data['retention_trend'], _cohort_label)
                if multi:
                    return multi

        # funnel → 신규/복귀/이탈 비율 comparison
        if intent_type == 'funnel' and data.get('funnel'):
            f = data['funnel']
            items = []
            if f.get('new_share') is not None:
                items.append({'label': '신규', 'value': round(f['new_share'], 1)})
            if f.get('react_share') is not None:
                items.append({'label': '복귀', 'value': round(f['react_share'], 1)})
            if f.get('churn_rate') is not None:
                items.append({'label': '이탈', 'value': round(f['churn_rate'], 1)})
            if len(items) >= 2:
                return build_chart_comparison(
                    title="사용자 흐름 비율",
                    metric="비율", unit="%",
                    date=date_max, items=items,
                    source=f"funnel:{date_max}"
                )

        # engagement → 깊은청취율 timeseries
        if intent_type == 'engagement' and data.get('engagement_trend'):
            pts = [{"date": d.replace('/', '-'), "value": v}
                   for d, v in data['engagement_trend'] if v is not None]
            return build_chart_timeseries(
                title=f"{scope_name} 깊은청취율 추이",
                metric="깊은청취율", unit="%", points=pts,
                source=f"timeline:{scope}/deep_rate",
                initial_days=data.get('engagement_trend_initial_days'),
            )

        # growth → 습관형성률 TOP3 comparison
        if intent_type == 'growth' and data.get('growth', {}).get('top3_habit'):
            top3 = data['growth']['top3_habit']
            items = [{'label': p.get('name', '?'), 'value': p['rate']} for p in top3 if p.get('rate') is not None]
            if len(items) >= 2:
                return build_chart_comparison(
                    title="습관형성률 TOP3",
                    metric="습관형성률", unit="%",
                    date=date_max, items=items,
                    source=f"growth:{date_max}"
                )

        # compare_trend / pgm_group_trend 는 위 블록(line 542)에서 이미 처리됨.
        # 추이·비교 키워드가 질문에 있더라도 아래 일반 fallback이 DAU 차트를 덮어쓰지 않도록 차단.
        if intent_type == 'compare_trend':
            return None

        # general / snapshot: 질문 키워드 기반 fallback
        q = question.lower()
        trend_kw   = ["추세", "추이", "변화", "최근", "지난주", "이번 주", "트렌드", "흐름", "그래프", "차트"]
        compare_kw = ["vs", "비교", "차이", "1위", "top", "순위", "어디가"]

        if any(kw in q for kw in trend_kw) and 'trend' in data:
            t = data['trend']
            points = [{"date": d.replace('/', '-'), "value": v}
                      for d, v in t['data'] if v is not None]
            metric, unit = _metric_meta(t['metric_field'])
            _fb_cadence = _get_metric_cadence(t['metric_field'])
            _fb_initial = t.get('initial_days')
            _fb_prev = None
            if data.get('trend_prev') and data['trend_prev'].get('data'):
                _fb_prev = [{"date": d.replace('/', '-'), "value": v}
                            for d, v in data['trend_prev']['data'] if v is not None]
            return build_chart_timeseries(
                title=f"{scope_name} {metric}",
                metric=metric, unit=unit, points=points,
                source=f"timeline:{scope}/{t['metric_field']}",
                points_prev=_fb_prev, cadence=_fb_cadence, initial_days=_fb_initial
            )

        if any(kw in q for kw in compare_kw) and data.get('ranking'):
            items = [{"label": r['name'], "value": r['dau']}
                     for r in data['ranking'][:5] if r.get('dau')]
            return build_chart_comparison(
                title="프로그램 DAU TOP5",
                metric="DAU", unit="명",
                date=date_max, items=items,
                source=f"snapshot:{date_max}"
            )

        # report → 핵심 알림 기반 트렌드 차트 (DAU + 최우선 알림 지표)
        if intent_type == 'report' and data.get('report_trends'):
            _COLORS = ['#185fa5', '#dc2626']
            series_raw = data['report_trends']
            built = []
            for i, s in enumerate(series_raw[:2]):
                pts = [{"date": d.replace('/', '-'), "value": v} for d, v in s['data'] if v is not None]
                if len(pts) >= 2:
                    built.append({'label': s['label'], 'unit': s['unit'],
                                  'color': _COLORS[i], 'points': pts, 'field': s['field']})
            if len(built) == 2:
                if built[0]['unit'] == built[1]['unit']:
                    multi = build_chart_multi_timeseries(
                        title=f"핵심 지표 추이 — {built[0]['label']} · {built[1]['label']} (30일)",
                        metric=f"{built[0]['label']} · {built[1]['label']}",
                        unit=built[0]['unit'], series=built,
                        source="timeline:T00/report"
                    )
                    if multi:
                        return multi
                else:
                    return {
                        "type":   "timeseries_dual",
                        "title":  f"핵심 지표 추이 — {built[0]['label']} · {built[1]['label']} (30일)",
                        "series": built,
                        "source": "timeline:T00/report",
                    }
            elif len(built) == 1:
                s0 = built[0]
                metric0, unit0 = _metric_meta(s0['field'])
                return build_chart_timeseries(
                    title=f"전체 {s0['label']} 추이 (30일)",
                    metric=metric0, unit=unit0, points=s0['points'],
                    source=f"timeline:T00/{s0['field']}"
                )

        return None
    except Exception:
        return None


# ── Claude 호출 ────────────────────────────────────────────
_CACHE_HEADERS = {
    "Content-Type": "application/json",
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "prompt-caching-2024-07-31",
    "x-api-key": ANTHROPIC_API_KEY,
}

def call_claude(system, user: str, max_tokens: int = 1000, model: str = None) -> tuple:
    """(answer_text, usage_dict) 반환. system은 str 또는 (static_str, dynamic_str) 튜플."""
    if isinstance(system, tuple):
        static_part, dynamic_part = system
        system_payload = [{"type": "text", "text": static_part, "cache_control": {"type": "ephemeral"}}]
        if dynamic_part:
            system_payload.append({"type": "text", "text": dynamic_part})
    else:
        system_payload = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
    payload = json.dumps({
        "model": model or CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": system_payload,
        "messages": [{"role": "user", "content": user}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers=_CACHE_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.loads(r.read())
        return body["content"][0]["text"], body.get("usage", {})


# ── 의도 분류 ──────────────────────────────────────────────
INTENT_SYSTEM = """RAAS 데이터 분석 시스템의 질의 분류기입니다.
사용자 질문을 분석해서 JSON으로만 응답하세요. 다른 텍스트 없이 JSON만.

응답 형식:
{
  "intent": "snapshot|trend|compare|ranking|overview|health|funnel|engagement|growth|anomaly|report|general",
  "scope": "T00|F00|L00|G00|P00 또는 PGM_CODE 또는 null",
  "scope_keyword": "사용자가 언급한 채널/프로그램 키워드 또는 null",
  "metric": "dau|wau|mau|dau_r7|dau_r30|new|new_week|new_mon|react|react_week|react_mon|react_rate|react_rate_week|react_rate_mon|churn|churn_week|churn_mon|real|real_week|real_mon|deep|deep_week|deep_mon|engage|engage_week|engage_mon|habit|habit_week|habit_mon|d1|d7|w1|m1|new_d1|new_d7|new_w1|new_m1|1min|10min|all",
  "metrics": null,
  "date_type": "yesterday|today|specific|range",
  "specific_date": "YYYY/MM/DD 또는 null",
  "days": 7,
  "has_period": false,
  "summary": "질문 한 줄 요약"
}

metrics: trend/dual_trend 전용 — 추이를 볼 지표를 배열로 반환. 다른 intent는 null.
- trend (단일 지표): ["dau"]
- dual_trend (복수 지표, 최대 3개): ["new", "new_m1"]
- 지표 코드는 아래 metric 목록과 동일한 값 사용
- 1개면 단일 꺾은선, 2~3개면 단위가 같으면 다중 꺾은선(공유 Y축), 단위가 다르면 이중 Y축

has_period: 사용자가 기간을 명시했으면 true, 아니면 false
- true 예시: "이번주", "지난 4주간", "이번달", "최근 3개월", "지난달"
- false 예시: "WAU 추이", "WAU 그래프 보여줘", "DAU 어때" (기간 미명시)

intent 정의:
- snapshot: 특정 날짜 단일 시점 (어제 DAU, 오늘 현황)
- trend: 추세/흐름, 지표 1개 (최근 N일 변화, 오르고 있나)
- compare: 채널/기간 단일 시점 비교 (파워FM vs 러브FM DAU)
- compare_trend: 채널 간 추이 비교 (파워FM vs 러브FM DAU 추이, 두 채널 변화 비교)
- dual_trend: 동일 채널에서 지표 2~3개 동시 추이 (DAU와 깊은청취율, 신규사용자와 신규M1유지율)
- ranking: 순위 TOP N (가장 많이 들은 프로그램)
- overview: 여러 지표를 한 테이블에 (편성시간 순 현황표, "DAU·WAU·MAU 함께", "전 지표 보여줘")
- health: 건강도/위험 프로그램 (잘 되고 있어? 위험한 거 있어?)
- funnel: 사용자 흐름 (신규·유지·이탈·복귀 비율, D1/D7/W1/M1 유지율, 리텐션, 코호트)
- engagement: 청취 품질 (깊은청취율, 실청취율, 참여율, 몰입도)
- growth: 성장 지표 (습관형성률, 복귀율, 신규코호트 유지)
- anomaly: 이상 감지 (경보, 알림, 이상 있어?, 급락/급등)
- report: 종합 리포트 (전체 현황 요약, 브리핑, 모두 보여줘)
- general: 기타 (위 분류에 해당 없음)

scope 결정:
- 채널 전체 명시 시만 채널코드 사용: F00(파워FM) / L00(러브FM) / G00(고릴라M) / P00(픽채널)
  ※ 채널코드(F00 등)는 채널 전체를 명시할 때만 — 프로그램 이름이 채널명을 연상시켜도(예: 철파엠·영철파워·파타) scope_keyword 사용
- "전체" 또는 미언급 시: T00
- "고릴라"(단독) = T00 (플랫폼 전체) — "고릴라M" 또는 "고릴라엠"일 때만 G00
- 프로그램 코드 직접 언급 시 (F01, L03, M07 등): scope_keyword에 코드 그대로 입력
- 프로그램명/별명 언급 시: scope_keyword에 키워드 입력, scope는 null

metric: dau|wau|mau|dau_r7|dau_r30|new|new_week|new_mon|react|react_week|react_mon|react_rate|react_rate_week|react_rate_mon|churn|churn_week|churn_mon|real|real_week|real_mon|deep|deep_week|deep_mon|engage|engage_week|engage_mon|habit|habit_week|habit_mon|d1|d7|w1|m1|new_d1|new_d7|new_w1|new_m1|1min|10min|all
(react=복귀사용자수, react_rate=복귀율%; 1min=1분이상청취자수(절대값), 10min=10분이상청취자수(절대값), real=실청취율(비율); 명시 없으면 all, 기간이 명시되면 _week/_mon suffix 사용)
(dau_r7=롤링 7일 WAU — 별칭: 롤링WAU, 7일MAU, 최근7일 활성사용자; dau_r30=롤링 30일 MAU — 별칭: 롤링MAU, 30일MAU, 최근30일 활성사용자)
days: trend/dual_trend intent는 기간 명시 없으면 30, 나머지는 7

agg_func: ranking intent 전용 — 기간 집계 방식. 기간이 명시됐을 때만 사용, 없으면 null.
  latest(기본·기간없음)|mean(평균)|median(중간값)|max(최대·최고)|min(최소·최저)|sum(합계·총합)|variance(분산)|stdev(표준편차)

date_range: ranking intent에서 특정 기간이 명시됐을 때 "YYYY/MM/DD~YYYY/MM/DD" 형식으로 반환. 없으면 null.
  - "4월" → "2026/04/01~2026/04/30"
  - "지난달" → 전월 1일~말일
  - "최근 2주" → today 기준 14일 전~어제
  - 특정 날짜("4월 20일") → date_range 대신 specific_date 사용

extra_fields: ranking intent에서 추가로 표시 요청한 필드명 배열. 없으면 null.
  지원 필드: guestname|live_yn|dau|wau|mau|dau_1min|dau_10min|deep_rate|churn_rate|react_rate|new|d1_ret|new_m1_ret|habit_rate|engage_rate
  예: "게스트 필드와 함께" → ["guestname"], "WAU도 같이" → ["wau"]"""


def classify_intent(question: str, today: str = None) -> dict:
    prompt = question
    if today:
        try:
            yesterday = (datetime.strptime(today.replace('/', '-'), '%Y-%m-%d')
                         - timedelta(days=1)).strftime('%Y/%m/%d')
            prompt = f"[현재날짜: {today} | 최신데이터: {yesterday}]\n{question}"
        except Exception:
            pass
    try:
        result, _ = call_claude(INTENT_SYSTEM, prompt, max_tokens=300)
        result = result.strip()
        if result.startswith("```"):
            parts = result.split("```")
            result = parts[1]
            if result.startswith("json"):
                result = result[4:]
        intent = json.loads(result.strip())

        # scope_keyword → PGM_CODE 매핑 (어댑터 3단계 조회)
        if intent.get('scope_keyword') and intent.get('scope') in (None, '', 'T00'):
            kw_raw = intent['scope_keyword'].strip()
            kw = kw_raw.lower()
            # 0) 직접 PGM_CODE 입력 — F01·L03·M07 등 코드 형식 그대로 사용
            _CHANNEL_CODES = {'T00', 'F00', 'L00', 'G00', 'P00'}
            if re.match(r'^[A-Z]\d{2,3}$', kw_raw.upper()) and kw_raw.upper() not in _CHANNEL_CODES:
                intent['scope'] = kw_raw.upper()
            else:
                # 1) 채널·플랫폼 매핑 — TTL에서 매번 빌드 (hot-reload 반영)
                for key, code in _build_scope_map().items():
                    if key.lower() == kw:
                        intent['scope'] = code
                        break
                # 2) 어댑터 find_program_by_keyword (정식명/별칭/영문명 검색)
                if not intent.get('scope'):
                    try:
                        from raas_onto import get_adapter
                        matches = get_adapter().find_program_by_keyword(kw_raw)
                        if matches:
                            intent['scope'] = matches[0]['code']
                    except Exception:
                        pass
                # 3) 키워드 인덱스 (부분 일치 fallback)
                if not intent.get('scope'):
                    for kw_key, code in _build_keyword_index().items():
                        if kw_key in kw or kw in kw_key:
                            intent['scope'] = code
                            break

        intent.setdefault('intent', 'general')
        intent.setdefault('scope', 'T00')
        intent.setdefault('metric', 'all')
        intent.setdefault('days', 7)
        return intent
    except Exception as e:
        print(f"  [intent error] {e}", file=sys.stderr)
        return {
            'intent': 'general', 'scope': 'T00', 'metric': 'all',
            'date_type': 'yesterday', 'days': 7, 'summary': question
        }


# ── 데이터 추출 ────────────────────────────────────────────
def _fmt_dau(v):  return f"{int(v):,}명" if v else "—"
def _fmt_pct(v):  return f"{v:.1f}%" if v is not None else "—"
def _fmt_arrow(v):
    if v is None: return "—"
    return f"(+){v:.1f}%" if v > 0 else (f"(-){v:.1f}%" if v < 0 else "보합")


def _extract_full_snapshot(row: dict, date: str) -> dict:
    """CSV row에서 전체 필드 추출 — 유지율·전주비교·롤링·주간·월간 포함.
    CSV에 _prev/_diff 직접 필드가 있으면 우선 사용, 없으면 current - diff 역산.
    """
    i = _i; fn = _fn; yn = _yn

    return {
        'date': date,
        # ── 일간 규모 ─────────────────────────────────────────
        'dau':            i(row.get('dau')),
        'dau_wow':        fn(row.get('dau_chg')),
        'dau_prev':       i(row.get('dau_prev')),
        'dau_d2':         i(row.get('dau_d2')),
        'dau_1min':       i(row.get('dau_1min')),
        'dau_1min_wow':   fn(row.get('dau_1min_chg')),
        'dau_1min_prev':  i(row.get('dau_1min_prev')),
        'dau_10min':      i(row.get('dau_10min')),
        'dau_10min_wow':  fn(row.get('dau_10min_chg')),
        'dau_10min_prev': i(row.get('dau_10min_prev')),
        'dau_r7':         i(row.get('dau_r7')),
        'dau_r7_chg':     fn(row.get('dau_r7_chg')),
        'dau_r7_prev':    i(row.get('dau_r7_prev')),
        'dau_r30':        i(row.get('dau_r30')),
        'dau_r30_chg':    fn(row.get('dau_r30_chg')),
        'dau_r30_prev':   i(row.get('dau_r30_prev')),
        # ── 신규·복귀·이탈 (일) ───────────────────────────────
        'new_user':       i(row.get('new')),
        'new_share':      fn(row.get('new_share')),
        'new_wow':        fn(row.get('new_chg')),
        'new_prev':       i(row.get('new_prev')),
        'react_user':     i(row.get('react')),
        'react_share':    fn(row.get('react_share')),
        'react_wow':      fn(row.get('react_chg')),
        'react_prev':     i(row.get('react_prev')),
        'react_rate':     fn(row.get('react_rate')),
        'react_rate_diff':fn(row.get('react_rate_diff')),
        'react_rate_prev':fn(row.get('react_rate_prev')),
        'churn_rate':     fn(row.get('churn_rate')),
        'churn_diff':     fn(row.get('churn_rate_diff')),
        'churn_rate_prev':fn(row.get('churn_rate_prev')),
        # ── 청취 품질 (일) ────────────────────────────────────
        'deep_rate':      fn(row.get('deep_rate')),
        'deep_diff':      fn(row.get('deep_rate_diff')),
        'deep_rate_prev': fn(row.get('deep_rate_prev')),
        'real_rate':      fn(row.get('real_rate')),
        'real_diff':      fn(row.get('real_rate_diff')),
        'real_rate_prev': fn(row.get('real_rate_prev')),
        'engage_rate':    fn(row.get('engage_rate')),
        'engage_diff':    fn(row.get('engage_rate_diff')),
        'engage_rate_prev':fn(row.get('engage_rate_prev')),
        'habit_rate':     fn(row.get('habit_rate')),
        'habit_diff':     fn(row.get('habit_rate_diff')),
        'habit_rate_prev':fn(row.get('habit_rate_prev')),
        # ── 유지율 — 전체 코호트 ──────────────────────────────
        'd1_ret':         fn(row.get('d1_ret')),
        'd1_ret_diff':    fn(row.get('d1_ret_diff')),
        'd1_ret_prev':    fn(row.get('d1_ret_prev')),
        'd7_ret':         fn(row.get('d7_ret')),
        'd7_ret_diff':    fn(row.get('d7_ret_diff')),
        'd7_ret_prev':    fn(row.get('d7_ret_prev')),
        'w1_ret':         fn(row.get('w1_ret')),
        'w1_ret_diff':    fn(row.get('w1_ret_diff')),
        'w1_ret_prev':    fn(row.get('w1_ret_prev')),
        'm1_ret':         fn(row.get('m1_ret')),
        'm1_ret_diff':    fn(row.get('m1_ret_diff')),
        'm1_ret_prev':    fn(row.get('m1_ret_prev')),
        # ── 유지율 — 신규 코호트 ──────────────────────────────
        'new_d1_ret':      fn(row.get('new_d1_ret')),
        'new_d1_ret_prev': fn(row.get('new_d1_ret_prev')),
        'new_d1_ret_diff': fn(row.get('new_d1_ret_diff')),
        'new_d7_ret':      fn(row.get('new_d7_ret')),
        'new_d7_ret_prev': fn(row.get('new_d7_ret_prev')),
        'new_d7_ret_diff': fn(row.get('new_d7_ret_diff')),
        'new_w1_ret':      fn(row.get('new_w1_ret')),
        'new_w1_ret_prev': fn(row.get('new_w1_ret_prev')),
        'new_w1_ret_diff': fn(row.get('new_w1_ret_diff')),
        'new_m1_ret':      fn(row.get('new_m1_ret')),
        'new_m1_ret_prev': fn(row.get('new_m1_ret_prev')),
        'new_m1_ret_diff': fn(row.get('new_m1_ret_diff')),
        # ── 주간 (WAU) ────────────────────────────────────────
        'dau_week':              i(row.get('wau')),
        'dau_week_wow':          fn(row.get('wau_chg')),
        'dau_week_prev':         i(row.get('wau_prev')),
        'dau_week_avg':          fn(row.get('dau_week_avg')),
        'wau_1min':              i(row.get('wau_1min')),
        'wau_1min_wow':          fn(row.get('wau_1min_chg')),
        'wau_1min_prev':         i(row.get('wau_1min_prev')),
        'wau_10min':             i(row.get('wau_10min')),
        'wau_10min_wow':         fn(row.get('wau_10min_chg')),
        'wau_10min_prev':        i(row.get('wau_10min_prev')),
        'deep_week':             fn(row.get('deep_rate_week')),
        'deep_week_diff':        fn(row.get('deep_rate_week_diff')),
        'deep_week_prev':        fn(row.get('deep_rate_week_prev')),
        'real_week':             fn(row.get('real_rate_week')),
        'real_week_diff':        fn(row.get('real_rate_week_diff')),
        'real_week_prev':        fn(row.get('real_rate_week_prev')),
        'engage_week':           fn(row.get('engage_rate_week')),
        'engage_week_diff':      fn(row.get('engage_rate_week_diff')),
        'engage_week_prev':      fn(row.get('engage_rate_week_prev')),
        'habit_week':            fn(row.get('habit_rate_week')),
        'habit_week_diff':       fn(row.get('habit_rate_week_diff')),
        'habit_week_prev':       fn(row.get('habit_rate_week_prev')),
        'churn_week':            fn(row.get('churn_rate_week')),
        'churn_week_diff':       fn(row.get('churn_rate_week_diff')),
        'churn_week_prev':       fn(row.get('churn_rate_week_prev')),
        'react_rate_week':       fn(row.get('react_rate_week')),
        'react_rate_week_diff':  fn(row.get('react_rate_week_diff')),
        'react_rate_week_prev':  fn(row.get('react_rate_week_prev')),
        'new_week':              i(row.get('new_week')),
        'new_week_share':        fn(row.get('new_week_share')),
        'new_week_prev':         i(row.get('new_week_prev')),
        'new_week_wow':          fn(row.get('new_week_chg')),
        'react_week':            i(row.get('react_week')),
        'react_week_prev':       i(row.get('react_week_prev')),
        'react_week_wow':        fn(row.get('react_week_chg')),
        # ── 월간 (MAU) ────────────────────────────────────────
        'dau_mon':               i(row.get('mau')),
        'dau_mon_wow':           fn(row.get('mau_chg')),
        'dau_mon_prev':          i(row.get('mau_prev')),
        'dau_mon_avg':           fn(row.get('dau_mon_avg')),
        'wau_mon_avg':           fn(row.get('wau_mon_avg')),
        'mau_1min':              i(row.get('mau_1min')),
        'mau_1min_wow':          fn(row.get('mau_1min_chg')),
        'mau_1min_prev':         i(row.get('mau_1min_prev')),
        'mau_10min':             i(row.get('mau_10min')),
        'mau_10min_wow':         fn(row.get('mau_10min_chg')),
        'mau_10min_prev':        i(row.get('mau_10min_prev')),
        'deep_mon':              fn(row.get('deep_rate_mon')),
        'deep_mon_diff':         fn(row.get('deep_rate_mon_diff')),
        'deep_mon_prev':         fn(row.get('deep_rate_mon_prev')),
        'real_mon':              fn(row.get('real_rate_mon')),
        'real_mon_diff':         fn(row.get('real_rate_mon_diff')),
        'real_mon_prev':         fn(row.get('real_rate_mon_prev')),
        'engage_mon':            fn(row.get('engage_rate_mon')),
        'habit_mon':             fn(row.get('habit_rate_mon')),
        'habit_mon_diff':        fn(row.get('habit_rate_mon_diff')),
        'habit_mon_prev':        fn(row.get('habit_rate_mon_prev')),
        'churn_mon':             fn(row.get('churn_rate_mon')),
        'new_mon':               i(row.get('new_mon')),
        'new_mon_share':         fn(row.get('new_mon_share')),
        'new_mon_prev':          i(row.get('new_mon_prev')),
        'new_mon_wow':           fn(row.get('new_mon_chg')),
        'react_mon':             i(row.get('react_mon')),
        'react_mon_wow':         fn(row.get('react_mon_chg')),
        'react_rate_mon':        fn(row.get('react_rate_mon')),
        'react_rate_mon_diff':   fn(row.get('react_rate_mon_diff')),
        # ── 편성 메타 ─────────────────────────────────────────
        'airtime':       (row.get('STIME') or row.get('stime') or '').strip(),
        'guestname':     (row.get('guestname') or '').strip(),
        'daily_corner':  (row.get('daily_corner') or '').strip(),
        'weekly_corner': (row.get('weekly_corner') or '').strip(),
        'program_title': (row.get('program_title') or row.get('PGM_NAME') or '').strip(),
        'view_radio_yn': yn(row.get('view_radio_yn')),
        'live_yn':       (row.get('live_yn') or '').strip() or None,  # '생방송'/'녹음' 원문 보존
    }


def _build_funnel_dict(row: dict) -> dict:
    return {
        'dau':              _i(row.get('dau')),
        'dau_yday':         _i(row.get('dau_prev')),
        'new_user':         _i(row.get('new')),
        'new_pct':          _fn(row.get('new_share')),
        'new_share':        _fn(row.get('new_share')),
        'new_wow':          _fn(row.get('new_chg')),
        'react_user':       _i(row.get('react')),
        'react_pct':        _fn(row.get('react_share')),
        'react_share':      _fn(row.get('react_share')),
        'react_wow':        _fn(row.get('react_chg')),
        'react_rate':       _fn(row.get('react_rate')),
        'churn_rate':       _fn(row.get('churn_rate')),
        'churn_diff':       _fn(row.get('churn_rate_diff')),
        'churn_rate_pw':    _fn(row.get('churn_rate_prev')),
        'd1_ret':           _fn(row.get('d1_ret')),
        'd1_ret_diff':      _fn(row.get('d1_ret_diff')),
        'd7_ret':           _fn(row.get('d7_ret')),
        'new_d1_ret':       _fn(row.get('new_d1_ret')),
        'new_d7_ret':       _fn(row.get('new_d7_ret')),
        'new_w1_ret':       _fn(row.get('new_w1_ret')),
        'new_m1_ret':       _fn(row.get('new_m1_ret')),
        # 주간
        'dau_week':         _i(row.get('wau')),
        'dau_week_wow':     _fn(row.get('wau_chg')),
        'wau_prev':         _i(row.get('wau_prev')),
        'new_week':         _i(row.get('new_week')),
        'new_week_pct':     _fn(row.get('new_week_share')),
        'new_week_share':   _fn(row.get('new_week_share')),
        'new_week_wow':     _fn(row.get('new_week_chg')),
        'churn_rate_week':  _fn(row.get('churn_rate_week')),
        'churn_week':       _fn(row.get('churn_rate_week')),
        'churn_week_diff':  _fn(row.get('churn_rate_week_diff')),
        'churn_week_pw':    _fn(row.get('churn_rate_week_prev')),
        'w1_ret':           _fn(row.get('w1_ret')),
        'react_week':       _i(row.get('react_week')),
        'react_week_pct':   _fn(row.get('react_week_share')),
        'react_week_share': _fn(row.get('react_week_share')),
        'react_week_wow':   _fn(row.get('react_week_chg')),
        'react_rate_week':  _fn(row.get('react_rate_week')),
        # 월간
        'dau_mon':          _i(row.get('mau')),
        'dau_mon_wow':      _fn(row.get('mau_chg')),
        'mau_prev':         _i(row.get('mau_prev')),
        'new_mon':          _i(row.get('new_mon')),
        'new_mon_pct':      _fn(row.get('new_mon_share')),
        'new_mon_share':    _fn(row.get('new_mon_share')),
        'new_mon_wow':      _fn(row.get('new_mon_chg')),
        'churn_rate_mon':   _fn(row.get('churn_rate_mon')),
        'churn_mon':        _fn(row.get('churn_rate_mon')),
        'churn_mon_diff':   _fn(row.get('churn_rate_mon_diff')),
        'churn_mon_pw':     _fn(row.get('churn_rate_mon_prev')),
        'm1_ret':           _fn(row.get('m1_ret')),
        'react_mon':        _i(row.get('react_mon')),
        'react_mon_pct':    _fn(row.get('react_mon_share')),
        'react_mon_share':  _fn(row.get('react_mon_share')),
        'react_mon_wow':    _fn(row.get('react_mon_chg')),
        'react_rate_mon':   _fn(row.get('react_rate_mon')),
    }


def _compute_funnel_diffs(row: dict) -> dict:
    """퍼널 차트 pp diff 파생 필드 계산 (CSV에 없는 값, 기존 필드 조합으로 산출)."""
    def _pct(num, den):
        try:
            n, d = float(num), float(den)
            return round(n / d * 100, 1) if d != 0 else None
        except (TypeError, ValueError):
            return None

    def _pp(a, b):
        try:
            return round(float(a) - float(b), 1)
        except (TypeError, ValueError):
            return None

    # ── 일간 ──────────────────────────────────────────────────────
    dau        = _fn(row.get('dau'))
    dau_prev   = _fn(row.get('dau_prev'))
    dau_d2     = _fn(row.get('dau_d2'))
    new_share  = _fn(row.get('new_share'))
    react_share= _fn(row.get('react_share'))
    new_prev   = _fn(row.get('new_prev'))
    react_prev = _fn(row.get('react_prev'))
    d1_ret     = _fn(row.get('d1_ret'))
    d1_ret_prev= _fn(row.get('d1_ret_prev'))

    # 전일 신규·복귀 비율 (new_prev/dau_prev, react_prev/dau_prev)
    new_share_prev   = _pct(new_prev, dau_prev)
    react_share_prev = _pct(react_prev, dau_prev)

    # 유지 사용자 (D1 유지율 기반: maint = dau_prev × d1_ret/100)
    maint_today = (dau_prev * d1_ret / 100) if dau_prev and d1_ret else None
    maint_yest  = (dau_d2  * d1_ret_prev / 100) if dau_d2 and d1_ret_prev else None
    maint_p_today = _pct(maint_today, dau)
    maint_p_yest  = _pct(maint_yest, dau_prev)

    # ── 주간 ──────────────────────────────────────────────────────
    wau              = _fn(row.get('wau'))
    wau_prev         = _fn(row.get('wau_prev'))
    new_week         = _fn(row.get('new_week'))
    new_week_prev    = _fn(row.get('new_week_prev'))
    react_week       = _fn(row.get('react_week'))
    react_week_prev  = _fn(row.get('react_week_prev'))
    new_week_share   = _fn(row.get('new_week_share'))
    react_week_share = _fn(row.get('react_week_share'))

    nws_prev = _pct(new_week_prev, wau_prev)
    rws_prev = _pct(react_week_prev, wau_prev)

    def _maint(total, nw, rw):
        if any(v is None for v in [total, nw, rw]):
            return None
        return total - nw - rw

    maint_wk_today = _maint(wau, new_week, react_week)
    maint_wk_prev  = _maint(wau_prev, new_week_prev, react_week_prev)
    maint_p_wk_today = _pct(maint_wk_today, wau)
    maint_p_wk_prev  = _pct(maint_wk_prev, wau_prev)

    return {
        # 일간
        'funnel_new_p_day_diff':    _pp(new_share, new_share_prev),
        'funnel_react_p_day_diff':  _pp(react_share, react_share_prev),
        'funnel_maint_p_day_diff':  _pp(maint_p_today, maint_p_yest),
        'funnel_maint_p2_day_diff': _fn(row.get('d1_ret_diff')),    # maint/dau_prev = d1_ret
        'funnel_churn_p2_day_diff': _fn(row.get('churn_rate_diff')),
        # 주간
        'funnel_new_p_week_diff':    _pp(new_week_share, nws_prev),
        'funnel_react_p_week_diff':  _pp(react_week_share, rws_prev),
        'funnel_maint_p_week_diff':  _pp(maint_p_wk_today, maint_p_wk_prev),
        'funnel_maint_p2_week_diff': None,   # wau_d2 없어 계산 불가
        'funnel_churn_p2_week_diff': _fn(row.get('churn_rate_week_diff')),
        # 월간 — react_mon·churn_rate_mon null, 일부만 제공
        'funnel_new_p_mon_diff':    None,   # new_mon_share_prev 없음
        'funnel_react_p_mon_diff':  None,   # react_mon null
        'funnel_maint_p_mon_diff':  None,   # react_mon null로 유지 계산 불가
        'funnel_maint_p2_mon_diff': None,   # mau_d2 없음
        'funnel_churn_p2_mon_diff': _fn(row.get('churn_rate_mon_diff')),
    }


def _build_engagement_dict(row: dict) -> dict:
    return {
        'dau_1min':          _i(row.get('dau_1min')),
        'dau_10min':         _i(row.get('dau_10min')),
        'deep_rate':         _fn(row.get('deep_rate')),
        'deep_rate_diff':    _fn(row.get('deep_rate_diff')),
        'deep_rate_week':    _fn(row.get('deep_rate_week')),
        'deep_rate_mon':     _fn(row.get('deep_rate_mon')),
        'real_rate':         _fn(row.get('real_rate')),
        'real_rate_diff':    _fn(row.get('real_rate_diff')),
        'real_rate_week':    _fn(row.get('real_rate_week')),
        'real_rate_mon':     _fn(row.get('real_rate_mon')),
        'engage_rate':       _fn(row.get('engage_rate')),
        'engage_week':       _fn(row.get('engage_rate_week')),
        'engage_mon':        _fn(row.get('engage_rate_mon')),
        'wau_1min':          _i(row.get('wau_1min')),
        'wau_10min':         _i(row.get('wau_10min')),
        'mau_1min':          _i(row.get('mau_1min')),
        'mau_10min':         _i(row.get('mau_10min')),
    }


def _build_growth_dict(row: dict) -> dict:
    return {
        'habit_rate':       _fn(row.get('habit_rate')),
        'habit_diff':       _fn(row.get('habit_rate_diff')),
        'habit_week':       _fn(row.get('habit_rate_week')),
        'habit_mon':        _fn(row.get('habit_rate_mon')),
        'react_rate':       _fn(row.get('react_rate')),
        'react_rate_week':  _fn(row.get('react_rate_week')),
        'react_rate_mon':   _fn(row.get('react_rate_mon')),
        'top3_habit':       [],
    }


def _build_alert_kpi(row: dict) -> dict:
    return {
        'dau_chg':         _fn(row.get('dau_chg')),
        'deep_rate_diff':  _fn(row.get('deep_rate_diff')),
        'new_chg':         _fn(row.get('new_chg')),
        'churn_rate_diff': _fn(row.get('churn_rate_diff')),
        'react_rate':      _fn(row.get('react_rate')),
        'habit_rate':      _fn(row.get('habit_rate')),
    }


def _evaluate_alerts(row: dict, timeline_snap: dict, latest_dt: str) -> dict:
    """Z-score 기반 알림 평가 (우선) + 고정룰 fallback + 위험 프로그램 감지."""
    alerts = []
    try:
        from raas_onto import get_adapter
        adapter = get_adapter()

        alerts = adapter.evaluate_zscore_alerts(timeline_snap, latest_dt)

        zscore_fields = {a['field'] for a in alerts}
        if not alerts or len(alerts) < 2:
            kpi = _build_alert_kpi(row)
            fixed_alerts = adapter.evaluate_platform_alerts(kpi)
            for fa in fixed_alerts:
                field_hint = {
                    'DauPlunge': 'dau_chg', 'DauSurge': 'dau_chg',
                    'DeepRatePlunge': 'deep_rate_diff',
                    'NewUserPlunge': 'new_chg',
                    'ChurnRateRise': 'churn_rate_diff',
                    'HabitRateAchieved': 'habit_rate', 'HabitRateLow': 'habit_rate',
                }
                rid = fa.get('rule_id', '').replace('raas:Alert_', '')
                if field_hint.get(rid) not in zscore_fields:
                    fa['source'] = 'fixed_rule'
                    alerts.append(fa)

        exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
        prog_snap = {}
        for code, date_rows in timeline_snap.items():
            if code in exclude:
                continue
            r = date_rows.get(latest_dt, {})
            prog_snap[code] = {
                'dau':        _i(r.get('dau')),
                'churn_rate': _fn(r.get('churn_rate')),
                'dau_chg':    _fn(r.get('dau_chg')),
            }
        risk_progs = adapter.find_at_risk_programs(prog_snap)
        if risk_progs:
            names = [
                _pgm_name(r.get('code'),
                          row=(timeline_snap.get(r.get('code'), {}) or {}).get(latest_dt, {}))
                for r in risk_progs[:3]
            ]
            alerts.append({
                'level': 'yellow',
                'msg':   f"🟡 위험 프로그램 감지: {', '.join(names)}",
                'rule_id': 'AtRiskProgramDetected',
            })

    except Exception:
        pass

    if not alerts:
        alerts = [{'level': 'green', 'msg': '🟢 이상 없음 — 모든 지표 정상 범위', 'rule_id': 'NoAlert'}]
    return {'alerts': alerts}


def _channel_name(code: str) -> str:
    _MAP = {'F00': '파워FM', 'L00': '러브FM', 'G00': '고릴라M', 'P00': '픽채널', 'T00': '전체'}
    return _MAP.get(code, code)


def _program_channel(code: str) -> str:
    if code.startswith('F'):
        return '파워FM'
    if code.startswith('L') or code.startswith('M'):
        return '러브FM'
    if code.startswith('G'):
        return '고릴라M'
    if code.startswith('P'):
        return '픽채널'
    return ''


def collect_briefing_data(timeline: dict) -> dict:
    """timeline에서 s1~s7 브리핑 데이터 딕셔너리를 빌드."""
    available = get_available_dates(timeline)
    if not available:
        return {'error': '데이터 없음'}
    latest_dt = available[-1]
    row = timeline.get('T00', {}).get(latest_dt, {})

    # ── s1: 경영 요약 ──────────────────────────────────────────────
    s1 = {
        'date':           row.get('DATE') or latest_dt,
        'date_week':      (row.get('DATE_WEEK') or '').replace('/', '-'),
        'date_mon':       (row.get('DATE_MON') or '').replace('/', '-'),
        'dau':            _i(row.get('dau')),
        'dau_chg':        _fn(row.get('dau_chg')),
        'dau_wow':        _fn(row.get('dau_chg')),
        'dau_r7':         _i(row.get('dau_r7')),
        'dau_r7_chg':     _fn(row.get('dau_r7_chg')),
        'dau_r30':        _i(row.get('dau_r30')),
        'dau_r30_chg':    _fn(row.get('dau_r30_chg')),
        'wau':            _i(row.get('wau')),
        'wau_chg':        _fn(row.get('wau_chg')),
        'mau':            _i(row.get('mau')),
        'mau_chg':        _fn(row.get('mau_chg')),
        'dau_week_wow':   _fn(row.get('wau_chg')),
        'dau_mon_wow':    _fn(row.get('mau_chg')),
        'new_user':       _i(row.get('new')),
        'new_share':      _fn(row.get('new_share')),
        'react_user':     _i(row.get('react')),
        'react_share':    _fn(row.get('react_share')),
    }

    # ── s2: 퍼널 ────────────────────────────────────────────────────
    s2 = _build_funnel_dict(row)
    s2.update(_compute_funnel_diffs(row))

    # ── s3: 인게이지먼트 ─────────────────────────────────────────────
    s3 = _build_engagement_dict(row)
    # 채널별 깊은청취율
    channel_deep = []
    for ch_code in ['F00', 'L00', 'G00', 'P00']:
        ch_r = timeline.get(ch_code, {}).get(latest_dt, {})
        dr = _fn(ch_r.get('deep_rate'))
        if dr is not None:
            channel_deep.append({
                'code': ch_code, 'name': _channel_name(ch_code),
                'rate': dr,
                'rate_week': _fn(ch_r.get('deep_rate_week')),
                'rate_mon':  _fn(ch_r.get('deep_rate_mon')),
            })
    s3['channel_deep'] = channel_deep

    # ── s4: 성장 품질 ────────────────────────────────────────────────
    s4 = _build_growth_dict(row)

    # ── s5: 프로그램 랭킹 ────────────────────────────────────────────
    exclude_codes = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
    all_pgms = []
    for code, date_rows in timeline.items():
        if code in exclude_codes:
            continue
        r = date_rows.get(latest_dt, {})
        dau = _i(r.get('dau'))
        all_pgms.append({
            'code':          code,
            'name':          _pgm_name(code, row=r),
            'channel':       _program_channel(code),
            'guestname':     (r.get('guestname') or '').strip() or None,
            'dau':           dau,
            'dau_wow':       _fn(r.get('dau_chg')),
            'dau_week':      _i(r.get('wau')),
            'dau_week_wow':  _fn(r.get('wau_chg')),
            'dau_mon':       _i(r.get('mau')),
            'dau_mon_wow':   _fn(r.get('mau_chg')),
            'deep_rate':     _fn(r.get('deep_rate')),
            'deep_rate_diff':_fn(r.get('deep_rate_diff')),
            'deep_rate_week':_fn(r.get('deep_rate_week')),
            'deep_rate_week_diff': _fn(r.get('deep_rate_week_diff')),
            'deep_rate_mon': _fn(r.get('deep_rate_mon')),
            'deep_rate_mon_diff':  _fn(r.get('deep_rate_mon_diff')),
            'dau_10min':     _i(r.get('dau_10min')),
            'wau_10min':     _i(r.get('wau_10min')),
            'mau_10min':     _i(r.get('mau_10min')),
            'new_user':      _i(r.get('new')),
            'new_pct':       _fn(r.get('new_share')),
            'new_wow':       _fn(r.get('new_chg')),
            'new_week':      _i(r.get('new_week')),
            'new_week_pct':  _fn(r.get('new_week_share')),
            'new_week_wow':  _fn(r.get('new_week_chg')),
            'new_mon':       _i(r.get('new_mon')),
            'new_mon_pct':   _fn(r.get('new_mon_share')),
            'new_mon_wow':   _fn(r.get('new_mon_chg')),
            'habit_rate':    _fn(r.get('habit_rate')),
            'habit_week':    _fn(r.get('habit_rate_week')),
            'habit_mon':     _fn(r.get('habit_rate_mon')),
            'churn_rate':    _fn(r.get('churn_rate')),
            'd1_ret':        _fn(r.get('d1_ret')),
            'w1_ret':        _fn(r.get('w1_ret')),
            'm1_ret':        _fn(r.get('m1_ret')),
        })
    all_pgms.sort(key=lambda x: x.get('dau') or 0, reverse=True)
    s5 = {'all_programs': all_pgms, 'dau_top10': all_pgms[:10]}

    # ── s6: 채널별 ──────────────────────────────────────────────────
    ch_list = []
    total_dau_1min = _i(row.get('dau_1min')) or 1
    for ch_code in ['F00', 'L00', 'G00', 'P00']:
        ch_r = timeline.get(ch_code, {}).get(latest_dt, {})
        ch_dau_1min = _i(ch_r.get('dau_1min')) or 0
        share = round(ch_dau_1min / total_dau_1min * 100, 1) if total_dau_1min else None
        ch_list.append({
            'code':          ch_code,
            'name':          _channel_name(ch_code),
            'share':         share,
            'dau':           _i(ch_r.get('dau')),
            'dau_wow':       _fn(ch_r.get('dau_chg')),
            'dau_week':      _i(ch_r.get('wau')),
            'dau_week_wow':  _fn(ch_r.get('wau_chg')),
            'dau_mon':       _i(ch_r.get('mau')),
            'dau_mon_wow':   _fn(ch_r.get('mau_chg')),
            'deep_rate':     _fn(ch_r.get('deep_rate')),
            'deep_rate_week':_fn(ch_r.get('deep_rate_week')),
            'deep_rate_mon': _fn(ch_r.get('deep_rate_mon')),
            'real_rate':     _fn(ch_r.get('real_rate')),
            'real_rate_week':_fn(ch_r.get('real_rate_week')),
            'real_rate_mon': _fn(ch_r.get('real_rate_mon')),
            'engage_rate':   _fn(ch_r.get('engage_rate')),
            'engage_week':   _fn(ch_r.get('engage_rate_week')),
            'engage_mon':    _fn(ch_r.get('engage_rate_mon')),
            'churn_rate':    _fn(ch_r.get('churn_rate')),
            'churn_week':    _fn(ch_r.get('churn_rate_week')),
            'churn_mon':     _fn(ch_r.get('churn_rate_mon')),
            'react_rate':    _fn(ch_r.get('react_rate')),
            'react_rate_week':_fn(ch_r.get('react_rate_week')),
            'react_rate_mon':_fn(ch_r.get('react_rate_mon')),
            'new_pct':       _fn(ch_r.get('new_share')),
            'new_week_pct':  _fn(ch_r.get('new_week_share')),
            'new_mon_pct':   _fn(ch_r.get('new_mon_share')),
            'habit_rate':    _fn(ch_r.get('habit_rate')),
            'habit_week':    _fn(ch_r.get('habit_rate_week')),
            'habit_mon':     _fn(ch_r.get('habit_rate_mon')),
            'd1_ret':        _fn(ch_r.get('d1_ret')),
            'w1_ret':        _fn(ch_r.get('w1_ret')),
            'm1_ret':        _fn(ch_r.get('m1_ret')),
            'new_user':      _i(ch_r.get('new')),
            'new_week':      _i(ch_r.get('new_week')),
            'new_mon':       _i(ch_r.get('new_mon')),
        })
    s6 = {'channels': ch_list}

    # ── s7: 이상 알림 (ZScoreDetector) ──────────────────────────────
    s7 = _evaluate_alerts(row, timeline, latest_dt)

    return {
        'collected_at':  latest_dt,
        's1_executive':  s1,
        's2_funnel':     s2,
        's3_engagement': s3,
        's4_growth':     s4,
        's5_rankings':   s5,
        's6_channels':   s6,
        's7_anomalies':  s7,
    }


_DOW_ORDER = ['월', '화', '수', '목', '금', '토', '일']

def _build_corner_schedule(timeline: dict) -> dict:
    """
    timeline에서 프로그램별 요일별 코너 스케줄 재구성.
    Returns: {pgm_code: {'월': {'daily': str, 'weekly': str}, '화': {...}, ...}}
    - 같은 요일에 여러 날짜가 있으면 가장 최신 날짜 데이터를 사용.
    - daily_corner: 방송일 매일 동일한 코너.
    - weekly_corner: 해당 요일에만 편성되는 코너.
    """
    _CHANNEL_CODES = {'T00', 'F00', 'L00', 'G00', 'P00'}
    schedule = {}
    for code, date_rows in timeline.items():
        if code in _CHANNEL_CODES:
            continue
        pgm_sched = {}
        for date in sorted(date_rows.keys()):  # 오름차순 → 최신이 마지막에 덮어씀
            row = date_rows[date]
            dow = _weekday_ko(date)
            if not dow:
                continue
            daily  = (row.get('daily_corner')  or '').strip()
            weekly = (row.get('weekly_corner') or '').strip()
            if daily or weekly:
                pgm_sched[dow] = {'daily': daily, 'weekly': weekly}
        if pgm_sched:
            schedule[code] = pgm_sched
    return schedule


def _fmt_stime(raw: str) -> str:
    """'HHMM' → 'HH:MM'. 그 외 형식은 원문 반환."""
    s = (raw or '').strip()
    if len(s) == 4 and s.isdigit():
        return f"{s[:2]}:{s[2:]}"
    return s


def _calc_date_range(question: str, intent: dict, ref_date: str):
    """질문 + intent에서 (date_from, date_to) 튜플 계산. 날짜 형식: 'YYYY/MM/DD'."""
    import re as _re, calendar as _cal

    def _iso(d):
        return d.replace('/', '-')

    _spec = intent.get('specific_date')

    if _spec:
        try:
            _td = datetime.strptime(_iso(_spec), '%Y-%m-%d')
        except ValueError:
            return None, None
        _multi_kws = ('일자별', '날짜별', '요일별', '날짜 별')
        if any(k in question for k in _multi_kws):
            _mon = _td - timedelta(days=_td.weekday())
            _sun = _mon + timedelta(days=6)
            return _mon.strftime('%Y/%m/%d'), _sun.strftime('%Y/%m/%d')
        return _spec, _spec

    if not ref_date:
        return None, None

    try:
        _now = datetime.strptime(_iso(ref_date), '%Y-%m-%d')
    except ValueError:
        return None, None

    if any(k in question for k in ('지난주', '지난 주', '저번주')):
        # 주 경계는 실제 오늘 날짜 기준 — CSV 마지막 날짜(일요일 등)를 쓰면 월요일에 한 주씩 빠짐
        _today = datetime.now()
        _this_mon = _today - timedelta(days=_today.weekday())
        _last_mon = _this_mon - timedelta(days=7)
        return _last_mon.strftime('%Y/%m/%d'), (_last_mon + timedelta(days=6)).strftime('%Y/%m/%d')

    if any(k in question for k in ('이번주', '이번 주')):
        _today = datetime.now()
        _this_mon = _today - timedelta(days=_today.weekday())
        return _this_mon.strftime('%Y/%m/%d'), ref_date  # 상한은 마지막 데이터 날짜 유지

    if '지난달' in question:
        _first_this = _now.replace(day=1)
        _last_prev  = _first_this - timedelta(days=1)
        return _last_prev.replace(day=1).strftime('%Y/%m/%d'), _last_prev.strftime('%Y/%m/%d')

    _month_m = _re.search(r'(\d{1,2})월', question)
    if _month_m:
        _mn  = int(_month_m.group(1))
        _yr  = _now.year if _mn <= _now.month else _now.year - 1
        _ld  = _cal.monthrange(_yr, _mn)[1]
        return f"{_yr}/{_mn:02d}/01", f"{_yr}/{_mn:02d}/{_ld:02d}"

    # 최근/부터/이전 → intent.days 기간
    _n = intent.get('days', 7)
    return (_now - timedelta(days=_n - 1)).strftime('%Y/%m/%d'), ref_date


def extract_data(timeline, intent: dict, briefing_data: dict = None, question: str = '') -> dict:
    if not timeline:
        return {'error': 'timeline 없음'}
    available_dates = get_available_dates(timeline)
    if not available_dates:
        return {'error': '데이터 없음'}

    intent_type = intent.get('intent', 'general')
    scope = intent.get('scope', 'T00')
    metric = intent.get('metric', 'all')
    # trend/dual_trend/compare_trend는 30일 기본, 나머지는 7일 기본
    _default_days = 30 if intent_type in ('trend', 'dual_trend', 'compare_trend') else 7
    days = min(intent.get('days', _default_days), len(available_dates))

    if scope not in timeline:
        scope = 'T00' if 'T00' in timeline else list(timeline.keys())[0]

    data = {
        'scope': scope,
        'scope_name': _pgm_name(scope),
        'metric': metric,
        'date_min': available_dates[0],
        'date_max': available_dates[-1],
        'available_days': len(available_dates),
    }

    # snapshot — 모든 인텐트에서 항상 빌드 (full 필드 보장)
    latest_date = available_dates[-1]
    row = timeline.get(scope, {}).get(latest_date, {})
    data['snapshot'] = _extract_full_snapshot(row, latest_date)

    # guestname 히스토리 — 날짜 범위 게스트 질문 시 프로그램/채널/전체 스코프 모두 지원
    # guestname 비어있음 = 초대손님 없음 (데이터 누락 아님)
    _CHANNEL_CODES = {'T00', 'F00', 'L00', 'G00', 'P00'}
    _guest_kws = ('게스트', '출연', '일자별', '날짜별', '요일별', '날짜 별')
    _range_kws = ('지난주', '지난 주', '이번주', '이번 주', '최근', '부터', '지난달', '저번주', '이전',
                  '1월', '2월', '3월', '4월', '5월', '6월', '7월', '8월', '9월', '10월', '11월', '12월')
    _CH_PGM_MAP = {'F00': PGM_F, 'L00': PGM_L, 'T00': PGM_F + PGM_L}
    # ranking intent + extra_fields에 guestname 포함 시 → 히스토리 불필요 (랭킹 행에 이미 표시됨)
    _skip_guest_hist = (intent_type == 'ranking' and 'guestname' in (intent.get('extra_fields') or []))
    # 게스트 질문 + (범위 키워드 OR intent에 specific_date가 설정됨) 시 히스토리 조회
    if (not _skip_guest_hist
            and any(k in question for k in _guest_kws)
            and (any(k in question for k in _range_kws) or intent.get('specific_date'))):

        # ── 날짜 범위 계산 ────────────────────────────────────────────────────
        _ref_date  = available_dates[-1] if available_dates else None
        _date_from, _date_to = _calc_date_range(question, intent, _ref_date)

        # ── 날짜 범위로 timeline 필터 ──────────────────────────────────────────
        def _filter_dates(all_dates):
            if _date_from and _date_to:
                return [d for d in sorted(all_dates) if _date_from <= d <= _date_to]
            return sorted(all_dates)  # 범위 미확정: 전체

        if scope not in _CHANNEL_CODES:
            # 프로그램 단일 스코프
            _src_dates = set(timeline.get(scope, {}).keys())
            _gh_dates = _filter_dates(_src_dates)
            _guest_hist = []
            for d in _gh_dates:
                _gn = (timeline.get(scope, {}).get(d, {}).get('guestname') or '').strip()
                _guest_hist.append({'date': d, 'guestname': _gn if _gn else None})
            _gh_range = (_gh_dates[0], _gh_dates[-1]) if _gh_dates else None
        elif scope in _CH_PGM_MAP:
            # 채널(F00/L00) 또는 전체(T00)
            _pgm_list = _CH_PGM_MAP[scope]
            _src_dates = {d for code in _pgm_list for d in timeline.get(code, {}).keys()}
            _gh_dates = _filter_dates(_src_dates)
            _guest_hist = []
            for d in _gh_dates:
                _day_entries = []
                for code in _pgm_list:
                    _gn = (timeline.get(code, {}).get(d, {}).get('guestname') or '').strip()
                    if _gn:
                        _day_entries.append(f"{_pgm_name(code)}: {_gn}")
                _guest_hist.append({'date': d, 'guestname': ' / '.join(_day_entries) if _day_entries else None})
            _gh_range = (_gh_dates[0], _gh_dates[-1]) if _gh_dates else None
        else:
            _guest_hist = []
            _gh_range = None
        # _gh_dates가 있을 때만 저장 — 요청 날짜에 데이터 없으면 빈 섹션 생성하지 않음
        # (날짜 내 초대손님 없음과 날짜 자체 데이터 없음을 구분)
        if _gh_dates:
            data['guestname_history'] = _guest_hist
            data['guestname_history_range'] = _gh_range

    # live_schedule — 생방송/녹음방송 편성 질문
    _live_kws = ('생방송', '녹음방송', '녹음 방송')
    if any(k in question for k in _live_kws):
        _ref_date  = available_dates[-1] if available_dates else None
        _ldate_from, _ldate_to = _calc_date_range(question, intent, _ref_date)
        _CHANNEL_CODES = {'T00', 'F00', 'L00', 'G00', 'P00'}
        _CH_PGM_MAP    = {'F00': PGM_F, 'L00': PGM_L, 'T00': PGM_F + PGM_L}

        def _lfilter(all_dates):
            if _ldate_from and _ldate_to:
                return [d for d in sorted(all_dates) if _ldate_from <= d <= _ldate_to]
            # 날짜 범위 미확정 → 최신 날짜 하루만 (어제/오늘 맥락)
            return [_ref_date] if _ref_date else []

        if scope not in _CHANNEL_CODES:
            # 개별 프로그램: 날짜별 live_yn 수집
            _src_dates = set(timeline.get(scope, {}).keys())
            _ldates = _lfilter(_src_dates)
            _lsched = []
            for d in _ldates:
                _ly = (timeline.get(scope, {}).get(d, {}).get('live_yn') or '').strip()
                _lsched.append({'date': d, 'programs': [
                    {'code': scope, 'name': _pgm_name(scope), 'live_yn': _ly}
                ] if _ly else []})
            data['live_schedule'] = _lsched
            data['live_schedule_range'] = (_ldates[0], _ldates[-1]) if _ldates else None
        else:
            _pgm_list = _CH_PGM_MAP.get(scope, PGM_F + PGM_L)
            _src_dates = {d for code in _pgm_list for d in timeline.get(code, {}).keys()}
            _ldates = _lfilter(_src_dates)
            _lsched = []
            for d in _ldates:
                _pgms = []
                for code in _pgm_list:
                    _ly = (timeline.get(code, {}).get(d, {}).get('live_yn') or '').strip()
                    if _ly:
                        _pgms.append({'code': code, 'name': _pgm_name(code), 'live_yn': _ly})
                _lsched.append({'date': d, 'programs': _pgms})
            data['live_schedule'] = _lsched
            data['live_schedule_range'] = (_ldates[0], _ldates[-1]) if _ldates else None

    # view_radio_schedule — 보는라디오(보라) 편성 질문
    _vr_kws = ('보라', '보는라디오', '영상라디오', '영상 라디오')
    if any(k in question for k in _vr_kws):
        _ref_date  = available_dates[-1] if available_dates else None
        _vrdate_from, _vrdate_to = _calc_date_range(question, intent, _ref_date)
        _CHANNEL_CODES = {'T00', 'F00', 'L00', 'G00', 'P00'}
        _CH_PGM_MAP    = {'F00': PGM_F, 'L00': PGM_L, 'T00': PGM_F + PGM_L}

        def _vrfilter(all_dates):
            if _vrdate_from and _vrdate_to:
                return [d for d in sorted(all_dates) if _vrdate_from <= d <= _vrdate_to]
            return [_ref_date] if _ref_date else []

        if scope not in _CHANNEL_CODES:
            _src_dates = set(timeline.get(scope, {}).keys())
            _vrdates   = _vrfilter(_src_dates)
            _vrsched   = []
            for d in _vrdates:
                _vry = (timeline.get(scope, {}).get(d, {}).get('view_radio_yn') or '').strip().upper()
                _vrsched.append({'date': d, 'programs': [
                    {'code': scope, 'name': _pgm_name(scope), 'view_radio_yn': _vry}
                ] if _vry in ('Y', 'N') else []})
            data['view_radio_schedule'] = _vrsched
            data['view_radio_schedule_range'] = (_vrdates[0], _vrdates[-1]) if _vrdates else None
        else:
            _pgm_list  = _CH_PGM_MAP.get(scope, PGM_F + PGM_L)
            _src_dates = {d for code in _pgm_list for d in timeline.get(code, {}).keys()}
            _vrdates   = _vrfilter(_src_dates)
            _vrsched   = []
            for d in _vrdates:
                _pgms = []
                for code in _pgm_list:
                    _vry = (timeline.get(code, {}).get(d, {}).get('view_radio_yn') or '').strip().upper()
                    if _vry in ('Y', 'N'):
                        _pgms.append({'code': code, 'name': _pgm_name(code), 'view_radio_yn': _vry})
                _vrsched.append({'date': d, 'programs': _pgms})
            data['view_radio_schedule'] = _vrsched
            data['view_radio_schedule_range'] = (_vrdates[0], _vrdates[-1]) if _vrdates else None

    # corner_schedule — 요일별 코너 편성 현황
    _CHANNEL_CODES_CS = {'T00', 'F00', 'L00', 'G00', 'P00'}
    _CH_PGM_MAP_CS    = {'F00': PGM_F, 'L00': PGM_L, 'T00': PGM_F + PGM_L}
    if '코너' in question:
        _full_sched = _build_corner_schedule(timeline)
        if scope not in _CHANNEL_CODES_CS:
            # 개별 프로그램
            sched = _full_sched.get(scope, {})
            if sched:
                data['corner_schedule'] = {
                    'type': 'program',
                    'scope': scope,
                    'scope_name': _pgm_name(scope),
                    'schedule': sched,  # {요일: {daily, weekly}}
                }
        else:
            # 채널/전체: 소속 프로그램 코너 목록
            _pgm_list = _CH_PGM_MAP_CS.get(scope, PGM_F + PGM_L)
            _ch_sched = {code: _full_sched[code]
                         for code in _pgm_list if code in _full_sched}
            if _ch_sched:
                data['corner_schedule'] = {
                    'type': 'channel',
                    'scope': scope,
                    'scope_name': _pgm_name(scope),
                    'programs': {code: {
                        'name': _pgm_name(code),
                        'schedule': sched,
                    } for code, sched in _ch_sched.items()},
                }

    # trend
    if intent_type in ('trend', 'general', 'health', 'engagement'):
        field_map = {
            # 볼륨
            'dau': 'dau', 'wau': 'wau', 'mau': 'mau',
            'dau_r7': 'dau_r7', 'dau_r30': 'dau_r30',
            # 신규/복귀
            'new': 'new', 'new_week': 'new_week', 'new_mon': 'new_mon',
            'react': 'react', 'react_week': 'react_week', 'react_mon': 'react_mon',
            'react_rate': 'react_rate', 'react_rate_week': 'react_rate_week', 'react_rate_mon': 'react_rate_mon',
            # 이탈/품질
            'churn': 'churn_rate', 'churn_week': 'churn_rate_week', 'churn_mon': 'churn_rate_mon',
            'real': 'real_rate', 'real_week': 'real_rate_week', 'real_mon': 'real_rate_mon',
            'deep': 'deep_rate', 'deep_week': 'deep_rate_week', 'deep_mon': 'deep_rate_mon',
            'engage': 'engage_rate', 'engage_week': 'engage_rate_week', 'engage_mon': 'engage_rate_mon',
            'habit': 'habit_rate', 'habit_week': 'habit_rate_week', 'habit_mon': 'habit_rate_mon',
            # 유지율
            'd1': 'd1_ret', 'd7': 'd7_ret', 'w1': 'w1_ret', 'm1': 'm1_ret',
            'new_d1': 'new_d1_ret', 'new_d7': 'new_d7_ret', 'new_w1': 'new_w1_ret', 'new_m1': 'new_m1_ret',
            'all': 'dau',
        }
        mf = field_map.get(metric, 'dau')
        # points: 전체 가용 데이터 (기간 버튼 활성화 최대화)
        _full_days = len(available_dates)
        _curr_trend = get_metric_trend(timeline, scope, mf, days=_full_days)
        # points_prev: 주간/월간 지표는 기간 명시 추이 쿼리일 때만 비교선 생성
        _cadence = _get_metric_cadence(mf)
        _has_period = intent.get('has_period', False)
        # prev는 클라이언트(chart.points 전체 기반)가 기간 버튼별로 동적 계산하므로
        # 서버는 chart.points에 전체 히스토리를 내려주기만 하면 됨 — trend_prev 불필요
        data['trend'] = {'metric_field': mf, 'initial_days': days,
                         'days': len(_curr_trend), 'data': _curr_trend}

    # ranking
    if intent_type in ('ranking', 'health', 'general'):
        latest_date = available_dates[-1]
        exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
        # 채널 범위 필터
        _CH_PROGRAMS = {
            'F00': set(PGM_F),
            'L00': set(PGM_L),
        }
        _rank_channel = scope if scope in ('F00', 'L00', 'G00', 'P00') else None
        _allowed = _CH_PROGRAMS.get(_rank_channel) if _rank_channel else None
        # 러브FM 주말 전용 프로그램 (온톨로지 WeekendOnly: M05/M07/M10/M11)
        _WEEKEND_PGMS = {'M05', 'M07', 'M10', 'M11'}
        _include_weekend = intent.get('include_weekend', False)

        # ── 날짜 범위 / 특정일 / 집계 설정 ────────────────────────────────
        _rank_date_range    = intent.get('date_range')        # "YYYY/MM/DD~YYYY/MM/DD"
        _rank_agg_func      = (intent.get('agg_func') or '').lower()
        _rank_specific_date = intent.get('specific_date')     # "YYYY/MM/DD"
        _rank_extra_fields  = [f for f in (intent.get('extra_fields') or []) if f]

        if _rank_date_range and '~' in str(_rank_date_range):
            _dr_from, _dr_to = str(_rank_date_range).split('~', 1)
            _rank_dates = [d for d in available_dates if _dr_from <= d <= _dr_to]
        elif _rank_specific_date:
            _sp = str(_rank_specific_date).replace('-', '/')
            _rank_dates = [d for d in available_dates if d == _sp] or [latest_date]
        else:
            _rank_dates = [latest_date]

        _rank_ref_date = _rank_dates[-1] if _rank_dates else latest_date
        _AGG_SET = {'mean', 'median', 'max', 'min', 'sum', 'variance', 'stdev'}
        _use_agg = _rank_agg_func in _AGG_SET and len(_rank_dates) > 1

        # rank_sort_field 미리 계산 (집계 로직에서 활용)
        _rank_field_map = {
            'wau': 'wau', 'mau': 'mau', 'dau_r7': 'dau_r7', 'dau_r30': 'dau_r30',
            '1min': 'dau_1min', '10min': 'dau_10min',
            '1min_week': 'wau_1min', '10min_week': 'wau_10min',
            '1min_mon': 'mau_1min', '10min_mon': 'mau_10min',
            'new': 'new', 'new_week': 'new_week', 'new_mon': 'new_mon',
            'react': 'react', 'react_week': 'react_week', 'react_mon': 'react_mon',
            'react_rate': 'react_rate', 'react_rate_week': 'react_rate_week', 'react_rate_mon': 'react_rate_mon',
            'deep': 'deep_rate', 'churn': 'churn_rate',
            'engage': 'engage_rate', 'habit': 'habit_rate', 'real': 'real_rate',
            'deep_week': 'deep_rate_week', 'churn_week': 'churn_rate_week',
            'engage_week': 'engage_rate_week', 'habit_week': 'habit_rate_week', 'real_week': 'real_rate_week',
            'deep_mon': 'deep_rate_mon', 'churn_mon': 'churn_rate_mon',
            'engage_mon': 'engage_rate_mon', 'habit_mon': 'habit_rate_mon', 'real_mon': 'real_rate_mon',
            'd1': 'd1_ret', 'd7': 'd7_ret', 'w1': 'w1_ret', 'm1': 'm1_ret',
            'new_d1': 'new_d1_ret', 'new_d7': 'new_d7_ret',
            'new_w1': 'new_w1_ret', 'new_m1': 'new_m1_ret',
        }
        rank_sort_field = _rank_field_map.get(metric, 'dau')

        rows = []
        for code, date_rows in timeline.items():
            if code in exclude:
                continue
            # 채널 지정 시: 해당 채널 소속 프로그램만 포함
            if _allowed is not None and code not in _allowed:
                continue
            # G00/P00은 코드 prefix로 필터 (명시적 목록 없음)
            if _rank_channel == 'G00' and not code.startswith('G'):
                continue
            if _rank_channel == 'P00' and not code.startswith('P'):
                continue
            # 러브FM 순위 기본 동작: 주말 프로그램 제외 (명시적 요청 시 포함)
            if _rank_channel == 'L00' and not _include_weekend and code in _WEEKEND_PGMS:
                continue
            row = date_rows.get(_rank_ref_date)
            if not row:
                continue
            dau = _i(row.get('dau'))
            # 주말 프로그램(include_weekend=True)은 주중엔 dau=0이므로 WAU로 활성 판단
            if _include_weekend and code in _WEEKEND_PGMS:
                if not (_i(row.get('wau')) or _i(row.get('mau')) or dau):
                    continue
            elif not dau or dau <= 0:
                continue
            fn = _fn
            rows.append({
                'code': code, 'name': _pgm_name(code, row=row),
                'live_yn': (row.get('live_yn') or '').strip() or None,
                'view_radio_yn': (row.get('view_radio_yn') or '').strip().upper() or None,
                # DAU / 롤링
                'dau': dau,
                'dau_r7': _i(row.get('dau_r7')) or None,
                'dau_r30': _i(row.get('dau_r30')) or None,
                'wau': _i(row.get('wau')) or None,
                'mau': _i(row.get('mau')) or None,
                'dau_wow': fn(row.get('dau_chg')),
                'dau_week_wow': fn(row.get('wau_chg')),
                'dau_mon_wow': fn(row.get('mau_chg')),
                # 신규 / 복귀 / 이탈 — 일간
                'new': _i(row.get('new')) or None,
                'react': _i(row.get('react')) or None,
                'churn_rate': fn(row.get('churn_rate')),
                # 신규 / 복귀 / 이탈 — 주간
                'new_week': _i(row.get('new_week')) or None,
                'react_week': _i(row.get('react_week')) or None,
                'churn_rate_week': fn(row.get('churn_rate_week')),
                # 신규 / 복귀 / 이탈 — 월간
                'new_mon': _i(row.get('new_mon')) or None,
                'react_mon': _i(row.get('react_mon')) or None,
                'churn_rate_mon': fn(row.get('churn_rate_mon')),
                # 청취 품질 — 일간
                'dau_1min': _i(row.get('dau_1min')) or None,
                'dau_10min': _i(row.get('dau_10min')) or None,
                'real_rate': fn(row.get('real_rate')),
                'deep_rate': fn(row.get('deep_rate')),
                'engage_rate': fn(row.get('engage_rate')),
                'habit_rate': fn(row.get('habit_rate')),
                # 청취 품질 — 주간
                'wau_1min': _i(row.get('wau_1min')) or None,
                'wau_10min': _i(row.get('wau_10min')) or None,
                'real_rate_week': fn(row.get('real_rate_week')),
                'deep_rate_week': fn(row.get('deep_rate_week')),
                'engage_rate_week': fn(row.get('engage_rate_week')),
                'habit_rate_week': fn(row.get('habit_rate_week')),
                # 청취 품질 — 월간
                'mau_1min': _i(row.get('mau_1min')) or None,
                'mau_10min': _i(row.get('mau_10min')) or None,
                'real_rate_mon': fn(row.get('real_rate_mon')),
                'deep_rate_mon': fn(row.get('deep_rate_mon')),
                'engage_rate_mon': fn(row.get('engage_rate_mon')),
                'habit_rate_mon': fn(row.get('habit_rate_mon')),
                # 유지율 — 전체 코호트
                'd1_ret': fn(row.get('d1_ret')),
                'd7_ret': fn(row.get('d7_ret')),
                'w1_ret': fn(row.get('w1_ret')),
                'm1_ret': fn(row.get('m1_ret')),
                # 유지율 — 신규 코호트
                'new_d1_ret': fn(row.get('new_d1_ret')),
                'new_d7_ret': fn(row.get('new_d7_ret')),
                'new_w1_ret': fn(row.get('new_w1_ret')),
                'new_m1_ret': fn(row.get('new_m1_ret')),
                # 신규 WoW / 구성비율
                'new_wow': fn(row.get('new_chg')),
                'new_pct': fn(row.get('new_share')),
                'new_week_wow': fn(row.get('new_week_chg')),
                'new_week_pct': fn(row.get('new_week_share')),
                'new_mon_wow': fn(row.get('new_mon_chg')),
                # 복귀 WoW / 구성비율
                'react_wow': fn(row.get('react_chg')),
                'react_pct': fn(row.get('react_share')),
                'react_week_wow': fn(row.get('react_week_chg')),
                'react_week_pct': fn(row.get('react_week_share')),
                # 복귀율 (비율) — 일간/주간/월간 + diff
                'react_rate': fn(row.get('react_rate')),
                'react_rate_week': fn(row.get('react_rate_week')),
                'react_rate_mon': fn(row.get('react_rate_mon')),
                'react_rate_diff': fn(row.get('react_rate_diff')),
                'react_rate_week_diff': fn(row.get('react_rate_week_diff')),
                'react_rate_mon_diff': fn(row.get('react_rate_mon_diff')),
                # 이탈율 diff
                'churn_rate_diff': fn(row.get('churn_rate_diff')),
                'churn_rate_week_diff': fn(row.get('churn_rate_week_diff')),
                'churn_rate_mon_diff': fn(row.get('churn_rate_mon_diff')),
                # 깊은청취율 diff
                'deep_rate_diff': fn(row.get('deep_rate_diff')),
                'deep_rate_week_diff': fn(row.get('deep_rate_week_diff')),
                'deep_rate_mon_diff': fn(row.get('deep_rate_mon_diff')),
                # 참여율 diff
                'engage_rate_diff': fn(row.get('engage_rate_diff')),
                'engage_rate_week_diff': fn(row.get('engage_rate_week_diff')),
                'engage_rate_mon_diff': fn(row.get('engage_rate_mon_diff')),
                # 습관형성율 diff
                'habit_rate_diff': fn(row.get('habit_rate_diff')),
                'habit_rate_week_diff': fn(row.get('habit_rate_week_diff')),
                'habit_rate_mon_diff': fn(row.get('habit_rate_mon_diff')),
                # 실청취율 diff
                'real_rate_diff': fn(row.get('real_rate_diff')),
                'real_rate_week_diff': fn(row.get('real_rate_week_diff')),
                'real_rate_mon_diff': fn(row.get('real_rate_mon_diff')),
                # 유지율 diff — 전체 코호트
                'd1_ret_diff': fn(row.get('d1_ret_diff')),
                'd7_ret_diff': fn(row.get('d7_ret_diff')),
                'w1_ret_diff': fn(row.get('w1_ret_diff')),
                'm1_ret_diff': fn(row.get('m1_ret_diff')),
                # 유지율 diff — 신규 코호트
                'new_d1_ret_diff': fn(row.get('new_d1_ret_diff')),
                'new_d7_ret_diff': fn(row.get('new_d7_ret_diff')),
                'new_w1_ret_diff': fn(row.get('new_w1_ret_diff')),
                'new_m1_ret_diff': fn(row.get('new_m1_ret_diff')),
                # 편성 메타 (extra_fields 요청 시 출력)
                'guestname': (row.get('guestname') or '').strip() or None,
                'live_yn_v': (row.get('live_yn') or '').strip() or None,
            })
        # ── 기간 집계: rank_sort_field 값을 기간 평균/합계 등으로 재계산 ──
        if _use_agg:
            import statistics as _stats_mod
            _agg_dispatch = {
                'mean':     lambda v: round(_stats_mod.mean(v), 2),
                'median':   lambda v: round(_stats_mod.median(v), 2),
                'max':      lambda v: max(v),
                'min':      lambda v: min(v),
                'sum':      lambda v: sum(v),
                'variance': lambda v: round(_stats_mod.variance(v), 4) if len(v) >= 2 else v[0],
                'stdev':    lambda v: round(_stats_mod.stdev(v), 4) if len(v) >= 2 else 0.0,
            }
            _agg_fn = _agg_dispatch.get(_rank_agg_func)
            if _agg_fn:
                for r in rows:
                    _raw_vals = [_fn(timeline.get(r['code'], {}).get(d, {}).get(rank_sort_field))
                                 for d in _rank_dates]
                    _raw_vals = [v for v in _raw_vals if v is not None]
                    if _raw_vals:
                        r[rank_sort_field] = _agg_fn(_raw_vals)

        rows.sort(key=lambda x: (x.get(rank_sort_field) or 0), reverse=True)
        data['ranking'] = rows[:40]
        data['ranking_metric'] = rank_sort_field
        data['ranking_channel'] = _rank_channel
        data['ranking_include_weekend'] = _include_weekend
        data['ranking_agg_func'] = _rank_agg_func if _use_agg else None
        data['ranking_date_range'] = _rank_date_range if _use_agg else None  # 집계 시에만 표시
        data['ranking_ref_date'] = _rank_ref_date   # 실제 사용된 날짜 (헤더 표시용)
        data['ranking_extra_fields'] = _rank_extra_fields

    # overview — 편성시간 순 전 지표 현황표
    if intent_type == 'overview':
        # 프로그램 코드들이 실제로 데이터를 갖는 최신 날짜 계산
        _PGM_CODES_OV = [c for c in PGM_F + PGM_L if c in timeline]
        _pgm_dates = sorted(set(
            d for c in _PGM_CODES_OV for d in timeline[c].keys()
        ))
        _pgm_latest = _pgm_dates[-1] if _pgm_dates else latest_date

        # date_type에 따라 실제 조회 날짜 결정
        if intent.get('specific_date'):
            _sp = intent['specific_date'].replace('-', '/')
            _ov_latest = _sp if _sp in _pgm_dates else _pgm_latest
        else:
            _ov_latest = _pgm_latest
        # 해당 날짜에 데이터가 없으면 최신 날짜로 fallback
        _has_data = any(
            _ov_latest in timeline.get(c, {}) for c in _PGM_CODES_OV
        )
        if not _has_data:
            _ov_latest = _pgm_latest
        data['overview_date'] = _ov_latest
        _q_ov = question.lower()
        _is_channel_ov = (
            any(kw in _q_ov for kw in ['전 채널', '채널별', '채널 현황', '채널 비교'])
            and scope in ('T00', '', None)
        )
        if _is_channel_ov:
            fn = _fn
            ch_ov_rows = []
            for ch_code in ('F00', 'L00', 'G00', 'P00'):
                date_rows = timeline.get(ch_code)
                if not date_rows:
                    continue
                row = date_rows.get(_ov_latest)
                if not row:
                    continue
                ch_ov_rows.append({
                    'code': ch_code,
                    'name': _pgm_name(ch_code, row=row),
                    'channel': _pgm_name(ch_code, row=row),
                    'is_weekend': False,
                    'dau':    _i(row.get('dau')) or None,
                    'wau':    _i(row.get('wau')) or None,
                    'mau':    _i(row.get('mau')) or None,
                    'dau_r7': _i(row.get('dau_r7')) or None,
                    'dau_r30':_i(row.get('dau_r30')) or None,
                    'dau_wow':      fn(row.get('dau_chg')),
                    'dau_week_wow': fn(row.get('wau_chg')),
                    'dau_mon_wow':  fn(row.get('mau_chg')),
                    'new':      _i(row.get('new')) or None,
                    'new_week': _i(row.get('new_week')) or None,
                    'new_mon':  _i(row.get('new_mon')) or None,
                    'react_rate':      fn(row.get('react_rate')),
                    'react_rate_week': fn(row.get('react_rate_week')),
                    'react_rate_mon':  fn(row.get('react_rate_mon')),
                    'churn_rate':      fn(row.get('churn_rate')),
                    'churn_rate_week': fn(row.get('churn_rate_week')),
                    'churn_rate_mon':  fn(row.get('churn_rate_mon')),
                    'real_rate':      fn(row.get('real_rate')),
                    'real_rate_week': fn(row.get('real_rate_week')),
                    'real_rate_mon':  fn(row.get('real_rate_mon')),
                    'deep_rate':      fn(row.get('deep_rate')),
                    'deep_rate_week': fn(row.get('deep_rate_week')),
                    'deep_rate_mon':  fn(row.get('deep_rate_mon')),
                    'engage_rate':      fn(row.get('engage_rate')),
                    'habit_rate':      fn(row.get('habit_rate')),
                    'habit_rate_week': fn(row.get('habit_rate_week')),
                    'habit_rate_mon':  fn(row.get('habit_rate_mon')),
                    'd1_ret': fn(row.get('d1_ret')),
                    'd7_ret': fn(row.get('d7_ret')),
                    'w1_ret': fn(row.get('w1_ret')),
                    'm1_ret': fn(row.get('m1_ret')),
                })
            data['overview'] = ch_ov_rows
            data['overview_channel'] = 'channels'
            data['overview_include_weekend'] = False
        else:
            # 편성시간 순 프로그램 순서 (raas_time_schema.ttl startTime 기준)
            _SCHEDULE_ORDER = [
                # 파워FM (00:00 기준 순환)
                'F02','F03','F04','F05','F06','F07','F08','F09','F10','F11','F12','F13','F01',
                # 러브FM 평일
                'L01','L02','L03','L05','L06','L07','L08','L09','L10','L11','L13','L12','L14','L15',
                # 러브FM 주말
                'M05','M10','M07','M11',
            ]
            _CH_PROGRAMS = {'F00': set(PGM_F), 'L00': set(PGM_L)}
            _ov_channel = scope if scope in ('F00', 'L00') else None
            _allowed_ov = _CH_PROGRAMS.get(_ov_channel) if _ov_channel else None
            _include_wknd = intent.get('include_weekend', False)
            _WEEKEND_PGMS = {'M05', 'M07', 'M10', 'M11'}
            exclude_ov = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
            ov_rows = []
            fn = _fn
            for code in _SCHEDULE_ORDER:
                if code in exclude_ov:
                    continue
                if _allowed_ov is not None and code not in _allowed_ov:
                    continue
                if _ov_channel == 'L00' and not _include_wknd and code in _WEEKEND_PGMS:
                    continue
                date_rows = timeline.get(code)
                if not date_rows:
                    continue
                row = date_rows.get(_ov_latest)
                if not row:
                    continue
                # 채널 결정
                ch = 'F00' if code in set(PGM_F) else 'L00' if code in set(PGM_L) else None
                ch_name = _pgm_name(ch, default='') if ch else ''
                is_wknd = code in _WEEKEND_PGMS
                ov_rows.append({
                    'code': code,
                    'name': _pgm_name(code, row=row),
                    'channel': ch_name,
                    'airtime': _fmt_stime(row.get('STIME') or row.get('stime') or row.get('airtime') or ''),
                    'is_weekend': is_wknd,
                    'live_yn': (row.get('live_yn') or '').strip() or None,
                    'view_radio_yn': (row.get('view_radio_yn') or '').strip().upper() or None,
                    # 볼륨
                    'dau':    _i(row.get('dau')) or None,
                    'wau':    _i(row.get('wau')) or None,
                    'mau':    _i(row.get('mau')) or None,
                    'dau_r7': _i(row.get('dau_r7')) or None,
                    'dau_r30':_i(row.get('dau_r30')) or None,
                    # WoW
                    'dau_wow':      fn(row.get('dau_chg')),
                    'dau_week_wow': fn(row.get('wau_chg')),
                    'dau_mon_wow':  fn(row.get('mau_chg')),
                    # 신규
                    'new':      _i(row.get('new')) or None,
                    'new_week': _i(row.get('new_week')) or None,
                    'new_mon':  _i(row.get('new_mon')) or None,
                    'new_pct':  fn(row.get('new_pct')),
                    'new_week_pct': fn(row.get('new_week_pct')),
                    # 복귀사용자
                    'react':      _i(row.get('react')) or None,
                    'react_week': _i(row.get('react_week')) or None,
                    'react_mon':  _i(row.get('react_mon')) or None,
                    # 복귀율
                    'react_rate':      fn(row.get('react_rate')),
                    'react_rate_week': fn(row.get('react_rate_week')),
                    'react_rate_mon':  fn(row.get('react_rate_mon')),
                    # 이탈율
                    'churn_rate':      fn(row.get('churn_rate')),
                    'churn_rate_week': fn(row.get('churn_rate_week')),
                    'churn_rate_mon':  fn(row.get('churn_rate_mon')),
                    # 실청취율
                    'real_rate':      fn(row.get('real_rate')),
                    'real_rate_week': fn(row.get('real_rate_week')),
                    'real_rate_mon':  fn(row.get('real_rate_mon')),
                    # 깊은청취율
                    'deep_rate':      fn(row.get('deep_rate')),
                    'deep_rate_week': fn(row.get('deep_rate_week')),
                    'deep_rate_mon':  fn(row.get('deep_rate_mon')),
                    # 참여율
                    'engage_rate':      fn(row.get('engage_rate')),
                    'engage_rate_week': fn(row.get('engage_rate_week')),
                    'engage_rate_mon':  fn(row.get('engage_rate_mon')),
                    # 습관형성율
                    'habit_rate':      fn(row.get('habit_rate')),
                    'habit_rate_week': fn(row.get('habit_rate_week')),
                    'habit_rate_mon':  fn(row.get('habit_rate_mon')),
                    # 전체 코호트 유지율
                    'd1_ret': fn(row.get('d1_ret')),
                    'd7_ret': fn(row.get('d7_ret')),
                    'w1_ret': fn(row.get('w1_ret')),
                    'm1_ret': fn(row.get('m1_ret')),
                    # 신규 코호트 유지율
                    'new_d1_ret': fn(row.get('new_d1_ret')),
                    'new_d7_ret': fn(row.get('new_d7_ret')),
                    'new_w1_ret': fn(row.get('new_w1_ret')),
                    'new_m1_ret': fn(row.get('new_m1_ret')),
                })
            data['overview'] = ov_rows
            data['overview_channel'] = _ov_channel
            data['overview_include_weekend'] = _include_wknd

    # compare
    if intent_type == 'compare':
        _channel_scopes = {'T00', 'F00', 'L00', 'G00', 'P00'}
        if scope in _channel_scopes:
            # 채널 간 비교
            latest_date = available_dates[-1]
            ch_rows = []
            for ch in ('F00', 'L00', 'G00', 'P00'):
                row = timeline.get(ch, {}).get(latest_date, {})
                if not row:
                    continue
                ch_rows.append({
                    'code': ch, 'name': _pgm_name(ch, row=row),
                    'dau': _i(row.get('dau')),
                    'dau_wow': _fn(row.get('dau_chg')),
                    'real_rate': _fn(row.get('real_rate')),
                    'deep_rate': _fn(row.get('deep_rate')),
                    'churn_rate': _fn(row.get('churn_rate')),
                })
            data['compare'] = ch_rows
        else:
            # 프로그램 스코프: 주간 비교 (지난주 vs 지지난주)
            compare_days = max(days * 2, 14)
            raw_trend = get_metric_trend(timeline, scope, 'dau', days=compare_days)
            valid_pts = [(d, v) for d, v in raw_trend if v is not None]
            if valid_pts:
                mid = len(valid_pts) // 2
                w2_pts = valid_pts[:mid]   # 지지난주 (더 오래된 절반)
                w1_pts = valid_pts[mid:]   # 지난주  (최근 절반)
                def _wk(pts):
                    vals = [v for _, v in pts]
                    return {
                        'date_range': f"{pts[0][0]}~{pts[-1][0]}",
                        'avg': round(sum(vals) / len(vals)) if vals else None,
                        'max': max(vals) if vals else None,
                        'min': min(vals) if vals else None,
                        'days': pts,
                    }
                data['weekly_compare'] = {
                    'scope': scope,
                    'scope_name': _pgm_name(scope),
                    'week1': _wk(w1_pts),
                    'week2': _wk(w2_pts),
                }
                # trend도 채워서 차트 렌더링 활용
                data['trend'] = {'metric_field': 'dau', 'days': len(valid_pts), 'data': valid_pts}

    # dual_trend — 지표 2~3개 동시 추이 (Claude가 metrics 배열로 반환)
    if intent_type == 'dual_trend':
        # metric 코드(classifier 출력) → (csv_field, 레이블, 단위)
        _dual_field_map = {
            # 볼륨 (명)
            'dau':      ('dau',      'DAU',        '명'),
            'wau':      ('wau',      'WAU',        '명'),
            'mau':      ('mau',      'MAU',        '명'),
            'dau_r7':   ('dau_r7',   '롤링WAU',    '명'),
            'dau_r30':  ('dau_r30',  '롤링MAU',    '명'),
            '1min':     ('dau_1min', '1분↑청취',   '명'),
            '10min':    ('dau_10min','10분↑청취',  '명'),
            # 신규/복귀 수 (명)
            'new':          ('new',          '신규(일)', '명'),
            'new_week':     ('new_week',     '신규(주)', '명'),
            'new_mon':      ('new_mon',      '신규(월)', '명'),
            'react':        ('react',        '복귀자(일)','명'),
            'react_week':   ('react_week',   '복귀자(주)','명'),
            'react_mon':    ('react_mon',    '복귀자(월)','명'),
            # 비율 (%)
            'react_rate':       ('react_rate',       '복귀율',     '%'),
            'react_rate_week':  ('react_rate_week',  '복귀율(주)', '%'),
            'react_rate_mon':   ('react_rate_mon',   '복귀율(월)', '%'),
            'churn':            ('churn_rate',        '이탈률',     '%'),
            'churn_week':       ('churn_rate_week',   '이탈률(주)', '%'),
            'churn_mon':        ('churn_rate_mon',    '이탈률(월)', '%'),
            'deep':             ('deep_rate',         '깊은청취율', '%'),
            'deep_week':        ('deep_rate_week',    '깊은청취(주)','%'),
            'deep_mon':         ('deep_rate_mon',     '깊은청취(월)','%'),
            'real':             ('real_rate',         '실청취율',   '%'),
            'real_week':        ('real_rate_week',    '실청취(주)', '%'),
            'real_mon':         ('real_rate_mon',     '실청취(월)', '%'),
            'engage':           ('engage_rate',       '참여율',     '%'),
            'engage_week':      ('engage_rate_week',  '참여율(주)', '%'),
            'engage_mon':       ('engage_rate_mon',   '참여율(월)', '%'),
            'habit':            ('habit_rate',        '습관형성률', '%'),
            'habit_week':       ('habit_rate_week',   '습관형성(주)','%'),
            'habit_mon':        ('habit_rate_mon',    '습관형성(월)','%'),
            # 유지율 — 전체 코호트 (%)
            'd1': ('d1_ret', 'D1유지율', '%'),
            'd7': ('d7_ret', 'D7유지율', '%'),
            'w1': ('w1_ret', 'W1유지율', '%'),
            'm1': ('m1_ret', 'M1유지율', '%'),
            # 유지율 — 신규 코호트 (%)
            'new_d1': ('new_d1_ret', '신규D1유지율', '%'),
            'new_d7': ('new_d7_ret', '신규D7유지율', '%'),
            'new_w1': ('new_w1_ret', '신규W1유지율', '%'),
            'new_m1': ('new_m1_ret', '신규M1유지율', '%'),
        }
        _dt_series = []
        _dt_fetch_days = len(available_dates)  # 차트 기간버튼용 전체 데이터
        for m in (intent.get('metrics') or ['dau', 'deep']):
            mf, label, unit = _dual_field_map.get(m, (m, m.upper(), ''))
            trend_data = get_metric_trend(timeline, scope, mf, days=_dt_fetch_days)
            _dt_series.append({
                'metric': m, 'metric_field': mf, 'label': label,
                'unit': unit, 'data': trend_data,
            })
        data['dual_trend'] = {'series': _dt_series, 'initial_days': days}

    # compare_trend — 채널 간 추이 멀티시리즈
    if intent_type == 'compare_trend':
        _selected = intent.get('compare_channels') or ['F00', 'L00']
        ct_field_map = {
            # 사용자 수 — 기간별
            'dau': 'dau', 'wau': 'wau', 'mau': 'mau',
            'dau_r7': 'wau', 'dau_r30': 'mau',
            'new': 'new', 'new_week': 'new_week', 'new_mon': 'new_mon',
            'react': 'react', 'react_week': 'react_week', 'react_mon': 'react_mon',
            '1min': 'dau_1min', '10min': 'dau_10min',
            # 비율 지표 — 일간
            'churn': 'churn_rate', 'deep': 'deep_rate', 'real': 'real_rate',
            'engage': 'engage_rate', 'habit': 'habit_rate', 'react_rate': 'react_rate',
            # 비율 지표 — 주간
            'churn_week': 'churn_rate_week', 'deep_week': 'deep_rate_week',
            'real_week': 'real_rate_week', 'engage_week': 'engage_rate_week',
            'habit_week': 'habit_rate_week', 'react_rate_week': 'react_rate_week',
            # 비율 지표 — 월간
            'churn_mon': 'churn_rate_mon', 'deep_mon': 'deep_rate_mon',
            'real_mon': 'real_rate_mon', 'engage_mon': 'engage_rate_mon',
            'habit_mon': 'habit_rate_mon', 'react_rate_mon': 'react_rate_mon',
            # 유지율 — 전체 코호트
            'd1': 'd1_ret', 'd7': 'd7_ret', 'w1': 'w1_ret', 'm1': 'm1_ret',
            # 유지율 — 신규 코호트
            'new_d1': 'new_d1_ret', 'new_d7': 'new_d7_ret',
            'new_w1': 'new_w1_ret', 'new_m1': 'new_m1_ret',
            'all': 'dau',
        }
        mf_ct = ct_field_map.get(metric, 'dau')
        _ct_fetch_days = len(available_dates)  # 가용 전체 데이터 반환
        _ct_series = []
        for ch_code in _selected:
            if ch_code not in timeline:
                continue
            ch_trend = get_metric_trend(timeline, ch_code, mf_ct, days=_ct_fetch_days)
            _ct_series.append({
                'code': ch_code,
                'label': _pgm_name(ch_code),
                'points': [{'date': d.replace('/', '-'), 'value': v}
                           for d, v in ch_trend if v is not None],
            })
        data['compare_trend'] = {'metric_field': mf_ct, 'series': _ct_series, 'initial_days': days}

    # health — 위험 프로그램
    # Phase 5 Step 4: 어댑터의 find_at_risk_programs 우선 + 인라인 룰 fallback
    if intent_type == 'health':
        latest_date = available_dates[-1]
        exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
        risks = None
        try:
            from raas_onto import get_adapter
            # 어댑터는 latest snapshot의 dict를 받음 (code → row)
            snapshot = {}
            for code, date_rows in timeline.items():
                if code in exclude:
                    continue
                row = date_rows.get(latest_date)
                if not row:
                    continue
                # 어댑터 룰은 dau/churn_rate/dau_chg 사용 (alias된 row가 들어와야 함)
                snapshot[code] = {
                    'dau':         _i(row.get('dau')),
                    'churn_rate':  _fn(row.get('churn_rate')),
                    'dau_chg':     _fn(row.get('dau_chg')),
                }
            adapter_risks = get_adapter().find_at_risk_programs(snapshot)
            # 어댑터 결과를 quey_engine 포맷으로 변환 (name/dau_wow 키 호환)
            if adapter_risks:
                risks = []
                for r in adapter_risks:
                    code = r.get('code')
                    row = (timeline.get(code, {}) or {}).get(latest_date, {})
                    risks.append({
                        'code': code,
                        'name': _pgm_name(code, row=row),
                        'dau':        _i(row.get('dau')),
                        'churn_rate': _fn(row.get('churn_rate')),
                        'dau_wow':    _fn(row.get('dau_chg')),
                    })
        except Exception:
            risks = None
        if risks is None:
            # fallback: 인라인 룰
            risks = []
            for code, date_rows in timeline.items():
                if code in exclude:
                    continue
                row = date_rows.get(latest_date, {})
                churn = _fn(row.get('churn_rate'))
                wow   = _fn(row.get('dau_chg'))
                dau   = _i(row.get('dau'))
                if churn and wow and dau and dau >= 1000 and churn >= 30 and wow <= -5:
                    risks.append({
                        'code': code, 'name': _pgm_name(code, row=row),
                        'dau': dau, 'churn_rate': churn, 'dau_wow': wow,
                    })
        risks.sort(key=lambda x: x.get('dau_wow') or 0)
        data['risks'] = risks[:5]

    # ── 신규 intent: timeline에서 직접 빌드 ──────────────────────────
    _t00_row = timeline.get('T00', {}).get(available_dates[-1] if available_dates else '', {})

    # funnel
    if intent_type == 'funnel':
        data['funnel'] = _build_funnel_dict(_t00_row)
        _ret_kws = ['유지율', 'd1', 'd7', 'w1', 'm1', '코호트', '리텐션', 'retention']
        if any(kw in question.lower() for kw in _ret_kws):
            _q_lower = question.lower()
            _want_new  = '신규' in _q_lower and '전체' not in _q_lower
            _want_both = (not _want_new and '전체' not in _q_lower
                          and not any(m in _q_lower for m in ['new_d1', 'new_d7', 'new_w1', 'new_m1']))

            _ALL_FIELDS = [('D1','d1_ret'),('D7','d7_ret'),('W1','w1_ret'),('M1','m1_ret')]
            _NEW_FIELDS = [('신규D1','new_d1_ret'),('신규D7','new_d7_ret'),
                           ('신규W1','new_w1_ret'),('신규M1','new_m1_ret')]

            def _build_ret_series(fields):
                series = []
                for label, field in fields:
                    trend = get_metric_trend(timeline, scope, field, days=len(available_dates))
                    valid = [(d, v) for d, v in trend if v is not None]
                    if len(valid) >= 3:
                        series.append({'label': label, 'field': field, 'data': valid, 'unit': '%'})
                return series

            if _want_both:
                _all_series = _build_ret_series(_ALL_FIELDS)
                _new_series = _build_ret_series(_NEW_FIELDS)
                if _all_series:
                    data['retention_trend']     = _all_series
                    data['retention_trend_new'] = _new_series if _new_series else None
                    data['retention_want_both'] = bool(_new_series)
                    data['retention_trend_initial_days'] = 30
                    data['retention_cohort_type'] = '전체'
            else:
                _ret_fields   = _NEW_FIELDS if _want_new else _ALL_FIELDS
                _cohort_type  = '신규' if _want_new else '전체'
                _ret_series   = _build_ret_series(_ret_fields)
                if _ret_series:
                    data['retention_trend'] = _ret_series
                    data['retention_trend_initial_days'] = 30
                    data['retention_cohort_type'] = _cohort_type

    # engagement — 깊은청취율 추세 차트도 함께 수집
    if intent_type == 'engagement':
        data['engagement'] = _build_engagement_dict(_t00_row)
        data['engagement_trend'] = get_metric_trend(timeline, scope, 'deep_rate', days=len(available_dates))
        data['engagement_trend_initial_days'] = days

    # growth
    if intent_type == 'growth':
        data['growth'] = _build_growth_dict(_t00_row)

    # anomaly — 어댑터 룰 기반 이상 감지
    if intent_type == 'anomaly':
        _latest_dt = available_dates[-1] if available_dates else ''
        data['anomalies'] = _evaluate_alerts(_t00_row, timeline, _latest_dt)

    # report — top-level 키로 직접 빌드 (s1~s7 포장 레이어 제거)
    if intent_type == 'report':
        _latest_dt = available_dates[-1] if available_dates else ''
        row = _t00_row
        # channels
        ch_data = {}
        for ch_code in ['F00', 'L00', 'G00', 'P00']:
            ch_row = timeline.get(ch_code, {}).get(_latest_dt, {})
            ch_data[ch_code] = {
                'name':       _pgm_name(ch_code),
                'dau':        _i(ch_row.get('dau')),
                'dau_wow':    _fn(ch_row.get('dau_chg')),
                'deep_rate':  _fn(ch_row.get('deep_rate')),
                'react_rate': _fn(ch_row.get('react_rate')),
            }
        data['channels'] = ch_data
        data['funnel'] = _build_funnel_dict(row)
        data['engagement'] = _build_engagement_dict(row)
        data['growth'] = _build_growth_dict(row)
        data['anomalies'] = _evaluate_alerts(row, timeline, _latest_dt)
        if not data.get('ranking'):
            exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
            rpt_rows = []
            for code, date_rows in timeline.items():
                if code in exclude:
                    continue
                r_row = date_rows.get(_latest_dt)
                if not r_row:
                    continue
                dau = _i(r_row.get('dau'))
                if not dau or dau <= 0:
                    continue
                rpt_rows.append({
                    'code': code, 'name': _pgm_name(code, row=r_row), 'dau': dau,
                    'deep_rate':  _fn(r_row.get('deep_rate')),
                    'churn_rate': _fn(r_row.get('churn_rate')),
                    'dau_wow':    _fn(r_row.get('dau_chg')),
                })
            rpt_rows.sort(key=lambda x: x['dau'], reverse=True)
            data['ranking'] = rpt_rows[:5]

        # 핵심 알림 기반 차트 트렌드 수집 (DAU + 최우선 알림 지표)
        _ALERT_TO_TREND = {
            'dau_chg':         ('dau',        'DAU',        '명'),
            'new_chg':         ('new',        '신규사용자',  '명'),
            'deep_rate_diff':  ('deep_rate',  '깊은청취율',  '%'),
            'churn_rate_diff': ('churn_rate', '이탈률',      '%'),
            'react_rate_diff': ('react_rate', '복귀율',      '%'),
            'habit_rate':      ('habit_rate', '습관형성률',  '%'),
            'new_d1_ret':      ('new_d1_ret', '신규D1유지율','%'),
            'new_d7_ret':      ('new_d7_ret', '신규D7유지율','%'),
        }
        alerts_list = data.get('anomalies', {}).get('alerts', [])
        top_alert = (next((a for a in alerts_list if a.get('level') == 'red'), None)
                     or next((a for a in alerts_list if a.get('level') == 'yellow'), None))

        report_series = []
        dau_pts = [(d, v) for d, v in get_metric_trend(timeline, 'T00', 'dau', days=30) if v is not None]
        if len(dau_pts) >= 3:
            report_series.append({'label': 'DAU', 'field': 'dau', 'data': dau_pts, 'unit': '명'})

        if top_alert:
            af = top_alert.get('field', '')
            if af in _ALERT_TO_TREND:
                t_field, t_label, t_unit = _ALERT_TO_TREND[af]
                if t_field != 'dau':
                    alert_pts = [(d, v) for d, v in get_metric_trend(timeline, 'T00', t_field, days=30) if v is not None]
                    if len(alert_pts) >= 3:
                        report_series.append({'label': t_label, 'field': t_field, 'data': alert_pts, 'unit': t_unit})

        if report_series:
            data['report_trends'] = report_series

    # 모든 intent에 플랫폼 이상 알림 첨부 (anomaly/report 제외, 알림 있을 때만)
    if 'anomalies' not in data and _t00_row:
        _latest_dt = available_dates[-1] if available_dates else ''
        _ambient_alerts = _evaluate_alerts(_t00_row, timeline, _latest_dt)
        if any(a.get('level') != 'green' for a in _ambient_alerts.get('alerts', [])):
            data['anomalies'] = _ambient_alerts

    return data


def format_for_claude(data: dict, intent: dict, question: str, timeline: dict = None) -> str:
    if 'error' in data:
        return f"질문: {question}\n[오류] {data['error']}"

    _date_max = data.get('date_max', '')
    _today = (datetime.strptime(_date_max.replace('/', '-'), '%Y-%m-%d') + timedelta(days=1)).strftime('%Y/%m/%d') if _date_max else ''
    # "어제" 질문은 항상 date_max — Claude의 날짜 계산을 신뢰하지 않음
    _spec = _date_max if '어제' in question else (intent.get('specific_date') or _date_max)

    # 질문의 '어제'에 실제 날짜 병기 → Claude가 데이터 날짜와 직접 매칭
    _disp_q = question
    if _spec and '어제' in question:
        _disp_q = question.replace('어제', f'어제({_spec})', 1)

    _date_note = f"[날짜기준] 오늘={_today} | 어제(최신데이터)={_date_max}" if _today else ""

    # 질문에서 프로그램/채널 코드 감지 → 이름 참조 블록 주입
    _CH_CODE_NAMES = {'F00': ('파워FM', '채널'), 'L00': ('러브FM', '채널'),
                      'G00': ('고릴라M', '채널'), 'P00': ('픽채널', '채널'), 'T00': ('전체', '채널')}
    _detected_codes = list(dict.fromkeys(re.findall(r'[A-Z]\d{2}', question)))
    _code_ref_parts = []
    for _c in _detected_codes:
        if _c in _CH_CODE_NAMES:
            _nm, _kind = _CH_CODE_NAMES[_c]
            _code_ref_parts.append(f"{_c}={_nm}({_kind})")
        else:
            _nm = _pgm_name(_c)
            if _nm == _c and timeline and _c in timeline:
                _dates = sorted(timeline[_c].keys())
                if _dates:
                    _row = timeline[_c][_dates[-1]]
                    _nm = (_row.get('pgm_name') or '').strip() or _c
            _code_ref_parts.append(f"{_c}={_nm}(프로그램)")

    lines = [
        *([f"[코드 참조] {' | '.join(_code_ref_parts)}"] if _code_ref_parts else []),
        *([_date_note] if _date_note else []),
        f"질문: {_disp_q}\n",
        f"--- 데이터 출처: raas_kpi_latest.csv ({data['date_min']} ~ {data['date_max']}, {data['available_days']}일치) ---",
        f"--- 분석 대상: {data['scope_name']} ({data['scope']}) ---\n",
    ]

    if 'snapshot' in data:
        s = data['snapshot']
        lines.append(f"[어제({s['date']}) 기준 핵심 지표]")

        lines.append(f"  [일간 규모]")
        lines.append(f"    DAU: {_fmt_dau(s.get('dau'))} (WoW {_fmt_arrow(s.get('dau_wow'))} | 전주동일요일 {_fmt_dau(s.get('dau_prev'))})")
        if s.get('dau_d2'):
            lines.append(f"    그저께(D-2) DAU: {_fmt_dau(s.get('dau_d2'))}")
        if s.get('dau_r7'):
            lines.append(f"    롤링 7일 WAU: {_fmt_dau(s.get('dau_r7'))} (WoW {_fmt_arrow(s.get('dau_r7_chg'))})")
        if s.get('dau_r30'):
            lines.append(f"    롤링 30일 MAU: {_fmt_dau(s.get('dau_r30'))} (MoM {_fmt_arrow(s.get('dau_r30_chg'))})")
        if s.get('dau_1min') or s.get('dau_10min'):
            lines.append(f"    1분↑청취자: {_fmt_dau(s.get('dau_1min'))} (WoW {_fmt_arrow(s.get('dau_1min_wow'))} | 전주 {_fmt_dau(s.get('dau_1min_prev'))})")
            lines.append(f"    10분↑청취자: {_fmt_dau(s.get('dau_10min'))} (WoW {_fmt_arrow(s.get('dau_10min_wow'))} | 전주 {_fmt_dau(s.get('dau_10min_prev'))})")

        lines.append(f"  [신규·복귀·이탈]")
        lines.append(f"    신규: {_fmt_dau(s.get('new_user'))} ({_fmt_pct(s.get('new_share'))}) WoW {_fmt_arrow(s.get('new_wow'))} | 전주 {_fmt_dau(s.get('new_prev'))}")
        lines.append(f"    복귀: {_fmt_dau(s.get('react_user'))} ({_fmt_pct(s.get('react_share'))}) WoW {_fmt_arrow(s.get('react_wow'))} | 전주 {_fmt_dau(s.get('react_prev'))}")
        lines.append(f"    복귀율: {_fmt_pct(s.get('react_rate'))} (Δ {_fmt_arrow(s.get('react_rate_diff'))} pp | 전주 {_fmt_pct(s.get('react_rate_prev'))})")
        if s.get('churn_rate') is not None:
            lines.append(f"    이탈율: {_fmt_pct(s.get('churn_rate'))} (Δ {_fmt_arrow(s.get('churn_diff'))} pp | 전주 {_fmt_pct(s.get('churn_rate_prev'))})")

        lines.append(f"  [청취 품질 (일)]")
        if s.get('deep_rate') is not None:
            lines.append(f"    깊은청취율: {_fmt_pct(s.get('deep_rate'))} (Δ {_fmt_arrow(s.get('deep_diff'))} pp | 전주 {_fmt_pct(s.get('deep_rate_prev'))})")
        if s.get('real_rate') is not None:
            lines.append(f"    실청취율: {_fmt_pct(s.get('real_rate'))} (Δ {_fmt_arrow(s.get('real_diff'))} pp | 전주 {_fmt_pct(s.get('real_rate_prev'))})")
        if s.get('engage_rate') is not None:
            lines.append(f"    참여율: {_fmt_pct(s.get('engage_rate'))} (Δ {_fmt_arrow(s.get('engage_diff'))} pp | 전주 {_fmt_pct(s.get('engage_rate_prev'))})")
        if s.get('habit_rate') is not None:
            lines.append(f"    습관형성률: {_fmt_pct(s.get('habit_rate'))} (Δ {_fmt_arrow(s.get('habit_diff'))} pp | 전주 {_fmt_pct(s.get('habit_rate_prev'))})")

        if any(s.get(k) is not None for k in ('d1_ret', 'd7_ret', 'w1_ret', 'm1_ret')):
            lines.append(f"  [유지율 — 전체 코호트]")
            if s.get('d1_ret') is not None:
                lines.append(f"    D1: {_fmt_pct(s.get('d1_ret'))} (Δ {_fmt_arrow(s.get('d1_ret_diff'))} pp | 전주 {_fmt_pct(s.get('d1_ret_prev'))})")
            if s.get('d7_ret') is not None:
                lines.append(f"    D7: {_fmt_pct(s.get('d7_ret'))} (Δ {_fmt_arrow(s.get('d7_ret_diff'))} pp | 전주 {_fmt_pct(s.get('d7_ret_prev'))})")
            if s.get('w1_ret') is not None:
                lines.append(f"    W1: {_fmt_pct(s.get('w1_ret'))} (Δ {_fmt_arrow(s.get('w1_ret_diff'))} pp | 전주 {_fmt_pct(s.get('w1_ret_prev'))})")
            if s.get('m1_ret') is not None:
                lines.append(f"    M1: {_fmt_pct(s.get('m1_ret'))} (Δ {_fmt_arrow(s.get('m1_ret_diff'))} pp | 전월 {_fmt_pct(s.get('m1_ret_prev'))})")

        if any(s.get(k) is not None for k in ('new_d1_ret', 'new_d7_ret', 'new_w1_ret', 'new_m1_ret')):
            lines.append(f"  [유지율 — 신규 코호트]")
            if s.get('new_d1_ret') is not None:
                lines.append(f"    D1: {_fmt_pct(s.get('new_d1_ret'))} (전주 {_fmt_pct(s.get('new_d1_ret_prev'))}, Δ {_fmt_arrow(s.get('new_d1_ret_diff'))} pp)")
            if s.get('new_d7_ret') is not None:
                lines.append(f"    D7: {_fmt_pct(s.get('new_d7_ret'))} (전주 {_fmt_pct(s.get('new_d7_ret_prev'))}, Δ {_fmt_arrow(s.get('new_d7_ret_diff'))} pp)")
            if s.get('new_w1_ret') is not None:
                lines.append(f"    W1: {_fmt_pct(s.get('new_w1_ret'))} (전주 {_fmt_pct(s.get('new_w1_ret_prev'))}, Δ {_fmt_arrow(s.get('new_w1_ret_diff'))} pp)")
            if s.get('new_m1_ret') is not None:
                lines.append(f"    M1: {_fmt_pct(s.get('new_m1_ret'))} (전월 {_fmt_pct(s.get('new_m1_ret_prev'))}, Δ {_fmt_arrow(s.get('new_m1_ret_diff'))} pp)")

        if s.get('dau_week'):
            lines.append(f"  [주간 (WAU)]")
            lines.append(f"    WAU: {_fmt_dau(s.get('dau_week'))} (WoW {_fmt_arrow(s.get('dau_week_wow'))} | 전전주 {_fmt_dau(s.get('dau_week_prev'))})")
            if s.get('wau_1min') or s.get('wau_10min'):
                lines.append(f"    1분↑(주간): {_fmt_dau(s.get('wau_1min'))} (WoW {_fmt_arrow(s.get('wau_1min_wow'))} | 전주 {_fmt_dau(s.get('wau_1min_prev'))})")
                lines.append(f"    10분↑(주간): {_fmt_dau(s.get('wau_10min'))} (WoW {_fmt_arrow(s.get('wau_10min_wow'))} | 전주 {_fmt_dau(s.get('wau_10min_prev'))})")
            if s.get('deep_week') is not None:
                lines.append(f"    깊은청취율: {_fmt_pct(s.get('deep_week'))} (Δ {_fmt_arrow(s.get('deep_week_diff'))} pp | 전주 {_fmt_pct(s.get('deep_week_prev'))})")
            if s.get('real_week') is not None:
                lines.append(f"    실청취율: {_fmt_pct(s.get('real_week'))} (Δ {_fmt_arrow(s.get('real_week_diff'))} pp | 전주 {_fmt_pct(s.get('real_week_prev'))})")
            if s.get('engage_week') is not None:
                lines.append(f"    참여율: {_fmt_pct(s.get('engage_week'))} (Δ {_fmt_arrow(s.get('engage_week_diff'))} pp | 전주 {_fmt_pct(s.get('engage_week_prev'))})")
            if s.get('habit_week') is not None:
                lines.append(f"    습관형성률: {_fmt_pct(s.get('habit_week'))} (Δ {_fmt_arrow(s.get('habit_week_diff'))} pp | 전주 {_fmt_pct(s.get('habit_week_prev'))})")
            if s.get('churn_week') is not None:
                lines.append(f"    이탈율: {_fmt_pct(s.get('churn_week'))} (Δ {_fmt_arrow(s.get('churn_week_diff'))} pp | 전주 {_fmt_pct(s.get('churn_week_prev'))})")
            if s.get('react_rate_week') is not None:
                lines.append(f"    복귀율: {_fmt_pct(s.get('react_rate_week'))} (Δ {_fmt_arrow(s.get('react_rate_week_diff'))} pp | 전주 {_fmt_pct(s.get('react_rate_week_prev'))})")
            if s.get('new_week') or s.get('react_week'):
                lines.append(f"    신규: {_fmt_dau(s.get('new_week'))} ({_fmt_pct(s.get('new_week_share'))}) WoW {_fmt_arrow(s.get('new_week_wow'))} | 전주 {_fmt_dau(s.get('new_week_prev'))}")
                lines.append(f"    복귀: {_fmt_dau(s.get('react_week'))} WoW {_fmt_arrow(s.get('react_week_wow'))} | 전주 {_fmt_dau(s.get('react_week_prev'))}")

        if s.get('dau_mon'):
            lines.append(f"  [월간 (MAU)]")
            lines.append(f"    MAU: {_fmt_dau(s.get('dau_mon'))} (MoM {_fmt_arrow(s.get('dau_mon_wow'))} | 전전월 {_fmt_dau(s.get('dau_mon_prev'))})")
            if s.get('mau_1min') or s.get('mau_10min'):
                lines.append(f"    1분↑(월간): {_fmt_dau(s.get('mau_1min'))} (MoM {_fmt_arrow(s.get('mau_1min_wow'))} | 전월 {_fmt_dau(s.get('mau_1min_prev'))})")
                lines.append(f"    10분↑(월간): {_fmt_dau(s.get('mau_10min'))} (MoM {_fmt_arrow(s.get('mau_10min_wow'))} | 전월 {_fmt_dau(s.get('mau_10min_prev'))})")
            if s.get('deep_mon') is not None:
                lines.append(f"    깊은청취율: {_fmt_pct(s.get('deep_mon'))} (Δ {_fmt_arrow(s.get('deep_mon_diff'))} pp | 전월 {_fmt_pct(s.get('deep_mon_prev'))})")
            if s.get('real_mon') is not None:
                lines.append(f"    실청취율: {_fmt_pct(s.get('real_mon'))} (Δ {_fmt_arrow(s.get('real_mon_diff'))} pp | 전월 {_fmt_pct(s.get('real_mon_prev'))})")
            lines.append(f"    참여율: {_fmt_pct(s.get('engage_mon'))}")
            if s.get('habit_mon') is not None:
                lines.append(f"    습관형성률: {_fmt_pct(s.get('habit_mon'))} (Δ {_fmt_arrow(s.get('habit_mon_diff'))} pp | 전월 {_fmt_pct(s.get('habit_mon_prev'))})")
            if s.get('churn_mon') is not None:
                lines.append(f"    이탈율: {_fmt_pct(s.get('churn_mon'))}")
            if s.get('react_rate_mon') is not None:
                lines.append(f"    복귀율: {_fmt_pct(s.get('react_rate_mon'))} (Δ {_fmt_arrow(s.get('react_rate_mon_diff'))} pp)")
            if s.get('new_mon') or s.get('react_mon'):
                lines.append(f"    신규: {_fmt_dau(s.get('new_mon'))} ({_fmt_pct(s.get('new_mon_share'))}) MoM {_fmt_arrow(s.get('new_mon_wow'))} | 전월 {_fmt_dau(s.get('new_mon_prev'))}")
                lines.append(f"    복귀: {_fmt_dau(s.get('react_mon'))} MoM {_fmt_arrow(s.get('react_mon_wow'))}")

        if any(s.get(k) for k in ('airtime', 'guestname', 'daily_corner', 'weekly_corner', 'program_title', 'live_yn', 'view_radio_yn')):
            lines.append(f"  [편성 메타]")
            if s.get('airtime'):
                lines.append(f"    방송시간: {s['airtime']}")
            if s.get('live_yn'):
                lines.append(f"    생방송여부: {s['live_yn']}")
            if s.get('view_radio_yn') in ('Y', 'N'):
                lines.append(f"    보라편성: {'있음(Y)' if s['view_radio_yn'] == 'Y' else '없음(N)'}")
            if not data.get('guestname_history') and s.get('guestname'):
                # 날짜 범위 히스토리가 없을 때만 단일 어제 게스트 표시
                lines.append(f"    어제({s['date']}) 게스트: {s['guestname']}")
            if s.get('daily_corner'):
                lines.append(f"    일간 코너: {s['daily_corner']}")
            if s.get('weekly_corner'):
                lines.append(f"    주간 코너: {s['weekly_corner']}")
            if s.get('program_title'):
                lines.append(f"    프로그램명: {s['program_title']}")

        lines.append('')

    # guestname_history — 스냅샷 루프 외부에서 독립 섹션으로 출력 (채널/전체 스코프 포함)
    # guestname=None인 날짜 = 초대손님 없음 (데이터 누락 아님)
    if 'guestname_history' in data:
        _gh = data['guestname_history']
        _ghr = data.get('guestname_history_range')
        _rng_str = f"{_ghr[0]}~{_ghr[1]}" if _ghr else ''
        lines.append(f"[게스트 출연 현황{' (' + _rng_str + ')' if _rng_str else ''}]")
        lines.append("※ guestname 비어있음 = 해당일 초대손님 없음 (데이터 누락 아님)")
        for _ge in _gh:
            _dow = _weekday_ko(_ge['date'])
            _dlabel = f"{_ge['date']}({_dow})" if _dow else _ge['date']
            if _ge['guestname']:
                lines.append(f"  {_dlabel}: {_ge['guestname']}")
            else:
                lines.append(f"  {_dlabel}: 초대손님 없음")
        lines.append('')

    # live_schedule — 날짜별 생방송/녹음방송 편성 현황
    if data.get('live_schedule'):
        _ls = data['live_schedule']
        _lsr = data.get('live_schedule_range')
        _lrng = f"{_lsr[0]}~{_lsr[1]}" if _lsr and _lsr[0] != _lsr[1] else (_lsr[0] if _lsr else '')
        lines.append(f"[생방송 편성 현황{' (' + _lrng + ')' if _lrng else ''}]")
        lines.append("※ live_yn 비어있음 = 집계 데이터 없음 (집계 주기 미도래 등)")
        for _le in _ls:
            _dow = _weekday_ko(_le['date'])
            _dlabel = f"{_le['date']}({_dow})" if _dow else _le['date']
            if _le['programs']:
                _live_pgms  = [p['name'] for p in _le['programs'] if p['live_yn'] == '생방송']
                _rec_pgms   = [p['name'] for p in _le['programs'] if p['live_yn'] == '녹음']
                _parts = []
                if _live_pgms:
                    _parts.append(f"생방송({len(_live_pgms)}): {', '.join(_live_pgms)}")
                if _rec_pgms:
                    _parts.append(f"녹음({len(_rec_pgms)}): {', '.join(_rec_pgms)}")
                lines.append(f"  {_dlabel}: {' | '.join(_parts)}")
            else:
                lines.append(f"  {_dlabel}: 집계 데이터 없음")
        lines.append('')

    # view_radio_schedule — 보라(보는라디오) 편성 현황
    if data.get('view_radio_schedule'):
        _vs  = data['view_radio_schedule']
        _vsr = data.get('view_radio_schedule_range')
        _vrng = f"{_vsr[0]}~{_vsr[1]}" if _vsr and _vsr[0] != _vsr[1] else (_vsr[0] if _vsr else '')
        lines.append(f"[보라(보는라디오) 편성 현황{' (' + _vrng + ')' if _vrng else ''}]")
        lines.append("※ view_radio_yn: Y=보라편성있음, N=보라편성없음, 비어있음=집계데이터없음")
        for _ve in _vs:
            _dow = _weekday_ko(_ve['date'])
            _dlabel = f"{_ve['date']}({_dow})" if _dow else _ve['date']
            if _ve['programs']:
                _y_pgms = [p['name'] for p in _ve['programs'] if p['view_radio_yn'] == 'Y']
                _n_pgms = [p['name'] for p in _ve['programs'] if p['view_radio_yn'] == 'N']
                _parts  = []
                if _y_pgms:
                    _parts.append(f"편성({len(_y_pgms)}): {', '.join(_y_pgms)}")
                if _n_pgms:
                    _parts.append(f"미편성({len(_n_pgms)}): {', '.join(_n_pgms)}")
                lines.append(f"  {_dlabel}: {' | '.join(_parts)}")
            else:
                lines.append(f"  {_dlabel}: 집계 데이터 없음")
        lines.append('')

    # corner_schedule — 요일별 코너 편성 현황
    if data.get('corner_schedule'):
        _cs = data['corner_schedule']
        if _cs['type'] == 'program':
            _cs_name  = _cs['scope_name']
            _cs_sched = _cs['schedule']  # {요일: {daily, weekly}}
            lines.append(f"[{_cs_name} 코너 편성 현황 (최근 방송 기준)]")
            lines.append("※ 매일코너=방송일 공통, 주간코너=해당 요일 특정 코너")
            for dow in _DOW_ORDER:
                if dow not in _cs_sched:
                    continue
                _d = _cs_sched[dow]
                lines.append(f"  {dow}요일:")
                if _d.get('daily'):
                    lines.append(f"    매일코너: {_d['daily']}")
                if _d.get('weekly'):
                    lines.append(f"    주간코너: {_d['weekly']}")
            lines.append('')
        else:
            # channel scope
            _cs_name = _cs['scope_name']
            lines.append(f"[{_cs_name} 프로그램 코너 편성 현황 (최근 방송 기준)]")
            lines.append("※ 매일코너=방송일 공통, 주간코너=해당 요일 특정 코너")
            for code, pgm_info in _cs['programs'].items():
                lines.append(f"  [{pgm_info['name']}]")
                for dow in _DOW_ORDER:
                    if dow not in pgm_info['schedule']:
                        continue
                    _d = pgm_info['schedule'][dow]
                    _parts_str = []
                    if _d.get('daily'):
                        _parts_str.append(f"매일: {_d['daily']}")
                    if _d.get('weekly'):
                        _parts_str.append(f"주간: {_d['weekly']}")
                    if _parts_str:
                        lines.append(f"    {dow}: {' / '.join(_parts_str)}")
            lines.append('')

    if 'trend' in data:
        t = data['trend']
        _ctx_days = intent.get('days', 30)
        _ctx_data = t['data'][-_ctx_days:]  # Claude 컨텍스트는 요청 기간만 (차트는 전체 유지)
        lines.append(f"[최근 {len(_ctx_data)}일 시계열 — {t['metric_field']}]")
        for date, value in _ctx_data:
            dow = _weekday_ko(date)
            prefix = f"  {date}({dow})" if dow else f"  {date}"
            if value is not None:
                val_str = _fmt_pct(value) if 'rate' in t['metric_field'] else _fmt_dau(value)
                lines.append(f"{prefix}: {val_str}")
            else:
                lines.append(f"{prefix}: 데이터 없음")
        lines.append('')

    if data.get('ranking'):
        _rm = data.get('ranking_metric', 'dau')
        _rm_label_map = {
            'dau': 'DAU', 'wau': 'WAU', 'mau': 'MAU',
            'dau_r7': '롤링WAU', 'dau_r30': '롤링MAU',
            'dau_1min': '1분↑청취', 'dau_10min': '10분↑청취',
            'wau_1min': '1분↑청취(주)', 'wau_10min': '10분↑청취(주)',
            'mau_1min': '1분↑청취(월)', 'mau_10min': '10분↑청취(월)',
            'new': '신규(일)', 'new_week': '신규(주)', 'new_mon': '신규(월)',
            'react': '복귀(일)', 'react_week': '복귀(주)', 'react_mon': '복귀(월)',
            'react_rate': '복귀율(일)', 'react_rate_week': '복귀율(주)', 'react_rate_mon': '복귀율(월)',
            'churn_rate': '이탈율(일)', 'churn_rate_week': '이탈율(주)', 'churn_rate_mon': '이탈율(월)',
            'real_rate': '실청취율(일)', 'real_rate_week': '실청취율(주)', 'real_rate_mon': '실청취율(월)',
            'deep_rate': '깊은청취율(일)', 'deep_rate_week': '깊은청취율(주)', 'deep_rate_mon': '깊은청취율(월)',
            'engage_rate': '참여율(일)', 'engage_rate_week': '참여율(주)', 'engage_rate_mon': '참여율(월)',
            'habit_rate': '습관형성율(일)', 'habit_rate_week': '습관형성율(주)', 'habit_rate_mon': '습관형성율(월)',
            'd1_ret': 'D1유지(전체)', 'd7_ret': 'D7유지(전체)',
            'w1_ret': 'W1유지(전체)', 'm1_ret': 'M1유지(전체)',
            'new_d1_ret': 'D1유지(신규)', 'new_d7_ret': 'D7유지(신규)',
            'new_w1_ret': 'W1유지(신규)', 'new_m1_ret': 'M1유지(신규)',
        }
        _rm_label = _rm_label_map.get(_rm, _rm.upper())
        _is_rate = _rm.endswith('_rate') or _rm.endswith('_ret')
        _rch = data.get('ranking_channel')
        _rch_name = _pgm_name(_rch, default='') if _rch else ''
        _rch_prefix = f"{_rch_name} " if _rch_name else ""
        _ragg     = data.get('ranking_agg_func') or ''
        _rdr      = data.get('ranking_date_range') or ''   # 집계 시에만 설정됨
        _rref     = data.get('ranking_ref_date') or data.get('date_max') or ''
        _rexf     = data.get('ranking_extra_fields') or []
        _agg_ko   = {'mean': '평균', 'median': '중간값', 'max': '최대', 'min': '최소',
                     'sum': '합계', 'variance': '분산', 'stdev': '표준편차'}.get(_ragg, '')
        if _ragg and _rdr:
            # 집계 기간 랭킹: "(2026/04/01~2026/04/30) [평균]"
            _period_label = f" ({_rdr}) [{_agg_ko}]"
        elif _rref and _rref != data.get('date_max'):
            # 특정일 지정이지만 실제 사용 날짜가 다를 때: 실제 날짜만 표시
            _period_label = f" ({_rref})"
        elif _rref:
            _period_label = f" ({_rref})"
        else:
            _period_label = " (최신일)"
        lines.append(f"[{_rch_prefix}프로그램 순위 TOP 10{_period_label} — {_rm_label} 기준]")

        # 정렬 기준 카테고리별 보조 필드
        _rm_is_vol  = _rm in ('dau', 'wau', 'mau', 'dau_r7', 'dau_r30',
                               'dau_1min', 'dau_10min', 'wau_1min', 'wau_10min', 'mau_1min', 'mau_10min')
        _rm_is_new  = _rm.startswith('new') and not _rm.endswith('_ret')
        _rm_is_react_cnt = _rm in ('react', 'react_week', 'react_mon')
        _wow_field_map = {
            'new': 'new_wow', 'new_week': 'new_week_wow', 'new_mon': 'new_mon_wow',
            'react': 'react_wow', 'react_week': 'react_week_wow', 'react_mon': 'react_mon_wow',
        }
        for i, r in enumerate(data['ranking'], 1):
            _sort_val = r.get(_rm)
            _sort_str = _fmt_pct(_sort_val) if _is_rate else _fmt_dau(_sort_val)
            _aux = []
            if _rm != 'dau':
                _aux.append(f"DAU {_fmt_dau(r.get('dau'))}")
            if _rm_is_vol:
                _aux.append(f"깊은청취 {_fmt_pct(r.get('deep_rate'))}")
            elif _rm_is_new:
                _aux.append(f"신규WoW {_fmt_arrow(r.get(_wow_field_map.get(_rm, 'new_wow')))}")
            elif _rm_is_react_cnt:
                _aux.append(f"복귀WoW {_fmt_arrow(r.get(_wow_field_map.get(_rm, 'react_wow')))}")
            _aux.append(f"DAU WoW {_fmt_arrow(r.get('dau_wow'))}")
            # extra_fields
            _ef_parts = []
            for ef in _rexf:
                _ef_val = r.get(ef)
                if ef == 'guestname' and r.get('guestname'):
                    _ef_parts.append(f"게스트: {r['guestname']}")
                elif ef == 'live_yn' and r.get('live_yn_v'):
                    _ef_parts.append(f"편성: {'생방' if r['live_yn_v'].upper() == 'Y' else '녹음'}")
                elif _ef_val is not None and _ef_val != '' and ef not in ('guestname', 'live_yn'):
                    _ef_lbl = _rm_label_map.get(ef, ef.upper())
                    _ef_str_v = _fmt_pct(_ef_val) if (ef.endswith('_rate') or ef.endswith('_ret')) else _fmt_dau(_ef_val)
                    if ef != _rm:
                        _ef_parts.append(f"{_ef_lbl} {_ef_str_v}")
            _line = f"  {i}위 {r['name']}: {_rm_label} {_sort_str} | {' | '.join(_aux)}"
            if _ef_parts:
                _line += f" || {' | '.join(_ef_parts)}"
            lines.append(_line)
        lines.append('')

    if data.get('overview'):
        _ov_ch = data.get('overview_channel')
        _ov_ch_name = _pgm_name(_ov_ch, default='') if _ov_ch else '전체'
        lines.append(f"[{_ov_ch_name} 프로그램 편성시간 순 현황]")
        for r in data['overview']:
            wknd = ' [주말]' if r.get('is_weekend') else ''
            lines.append(
                f"  {r['name']}{wknd}: DAU {_fmt_dau(r.get('dau'))} | WAU {_fmt_dau(r.get('wau'))} | MAU {_fmt_dau(r.get('mau'))} | "
                f"깊은청취 {_fmt_pct(r.get('deep_rate'))} | 복귀율 {_fmt_pct(r.get('react_rate'))} | 이탈율 {_fmt_pct(r.get('churn_rate'))}"
            )
        lines.append('')

    if data.get('compare'):
        lines.append("[채널별 비교]")
        for c in data['compare']:
            lines.append(
                f"  {c['name']}: DAU {_fmt_dau(c['dau'])} ({_fmt_arrow(c['dau_wow'])}) | "
                f"실청취율 {_fmt_pct(c.get('real_rate'))} | "
                f"깊은청취 {_fmt_pct(c.get('deep_rate'))} | "
                f"이탈률 {_fmt_pct(c.get('churn_rate'))}"
            )
        lines.append('')

    if data.get('dual_trend'):
        dt = data['dual_trend']
        lines.append(f"[듀얼 지표 추이 — {data['date_max']}]")
        for s in dt['series']:
            pts = [(d, v) for d, v in s.get('data', []) if v is not None]
            if pts:
                v0, v1 = pts[0][1], pts[-1][1]
                chg = round((v1 - v0) / v0 * 100, 1) if v0 else None
                val_fmt = _fmt_pct if s['unit'] == '%' else _fmt_dau
                chg_str = f"({chg:+.1f}%)" if chg is not None else ""
                lines.append(f"  {s['label']}: {val_fmt(v0)} → {val_fmt(v1)} {chg_str}")
        lines.append('')

    if data.get('compare_trend'):
        ct = data['compare_trend']
        mf_label, mf_unit = _metric_meta(ct['metric_field'])
        _grp_lbl = '프로그램별' if intent.get('pgm_group_trend') else '채널별'
        lines.append(f"[{_grp_lbl} {mf_label} 추이 비교]")
        for s in ct['series']:
            pts = [p for p in s.get('points', []) if p.get('value') is not None]
            if pts:
                v0, v1 = pts[0]['value'], pts[-1]['value']
                chg = round((v1 - v0) / v0 * 100, 1) if v0 else None
                chg_str = f"({chg:+.1f}%)" if chg is not None else ""
                val_fmt = _fmt_pct if mf_unit == '%' else _fmt_dau
                lines.append(f"  {s['label']}({s['code']}): {val_fmt(v0)} → {val_fmt(v1)} {chg_str}")
        lines.append('')

    if data.get('weekly_compare'):
        wc = data['weekly_compare']
        w1, w2 = wc['week1'], wc['week2']
        lines.append(f"[{wc['scope_name']} 주간 비교]")
        if w1['avg'] and w2['avg']:
            chg = (w1['avg'] - w2['avg']) / w2['avg'] * 100
            lines.append(f"  지난주 평균:   {_fmt_dau(w1['avg'])} ({chg:+.1f}%)")
        else:
            lines.append(f"  지난주 평균:   {_fmt_dau(w1['avg'])}")
        lines.append(f"  지난주 ({w1['date_range']}): 최고 {_fmt_dau(w1['max'])} | 최저 {_fmt_dau(w1['min'])}")
        for d, v in w1['days']:
            lines.append(f"    {d}({_weekday_ko(d)}): {_fmt_dau(v)}")
        lines.append(f"  지지난주 평균: {_fmt_dau(w2['avg'])}")
        lines.append(f"  지지난주 ({w2['date_range']}): 최고 {_fmt_dau(w2['max'])} | 최저 {_fmt_dau(w2['min'])}")
        for d, v in w2['days']:
            lines.append(f"    {d}({_weekday_ko(d)}): {_fmt_dau(v)}")
        lines.append('')

    if data.get('risks'):
        lines.append("[이탈 위험 프로그램]")
        for r in data['risks']:
            lines.append(
                f"  {r['name']}: 이탈률 {_fmt_pct(r['churn_rate'])} | "
                f"DAU {_fmt_arrow(r['dau_wow'])} | DAU {_fmt_dau(r['dau'])}"
            )
        lines.append('')

    # ── 신규 intent 섹션 ─────────────────────────────────────
    if data.get('funnel'):
        f = data['funnel']
        lines.append(f"[사용자 흐름 (Funnel) — {data['date_max']}]")
        lines.append(f"  [일간] 신규: {_fmt_dau(f.get('new_user'))} ({_fmt_pct(f.get('new_share'))}) WoW {_fmt_arrow(f.get('new_wow'))}")
        lines.append(f"         복귀: {_fmt_dau(f.get('react_user'))} ({_fmt_pct(f.get('react_share'))}) | 복귀율 {_fmt_pct(f.get('react_rate'))}")
        lines.append(f"         이탈률: {_fmt_pct(f.get('churn_rate'))} ({_fmt_arrow(f.get('churn_diff'))} pp)")
        if f.get('d1_ret') is not None:
            lines.append(f"         D1유지율: {_fmt_pct(f.get('d1_ret'))} ({_fmt_arrow(f.get('d1_ret_diff'))} pp) | D7유지율: {_fmt_pct(f.get('d7_ret'))}")
        if f.get('new_week') is not None:
            lines.append(f"  [주간] 신규: {_fmt_dau(f.get('new_week'))} ({_fmt_pct(f.get('new_week_share'))}) | 이탈률: {_fmt_pct(f.get('churn_rate_week'))} | W1유지율: {_fmt_pct(f.get('w1_ret'))}")
            lines.append(f"         복귀율: {_fmt_pct(f.get('react_rate_week'))}")
        if f.get('new_mon') is not None:
            lines.append(f"  [월간] 신규: {_fmt_dau(f.get('new_mon'))} ({_fmt_pct(f.get('new_mon_share'))}) | 이탈률: {_fmt_pct(f.get('churn_rate_mon'))} | M1유지율: {_fmt_pct(f.get('m1_ret'))}")
        if f.get('new_d1_ret') is not None:
            lines.append(f"  [신규코호트] D1 {_fmt_pct(f.get('new_d1_ret'))} | D7 {_fmt_pct(f.get('new_d7_ret'))} | W1 {_fmt_pct(f.get('new_w1_ret'))} | M1 {_fmt_pct(f.get('new_m1_ret'))}")
        lines.append('')

    if data.get('engagement'):
        e = data['engagement']
        lines.append(f"[청취 품질 (Engagement) — {data['date_max']}]")
        if e.get('deep_rate') is not None:
            lines.append(f"  깊은청취율 (10분/1분): 일 {_fmt_pct(e.get('deep_rate'))} ({_fmt_arrow(e.get('deep_rate_diff'))} pp) | 주 {_fmt_pct(e.get('deep_rate_week'))} | 월 {_fmt_pct(e.get('deep_rate_mon'))}")
        if e.get('real_rate') is not None:
            lines.append(f"  실청취율  (1분/DAU):   일 {_fmt_pct(e.get('real_rate'))} ({_fmt_arrow(e.get('real_rate_diff'))} pp) | 주 {_fmt_pct(e.get('real_rate_week'))} | 월 {_fmt_pct(e.get('real_rate_mon'))}")
        if e.get('engage_rate') is not None:
            lines.append(f"  참여율:                일 {_fmt_pct(e.get('engage_rate'))} | 주 {_fmt_pct(e.get('engage_week'))} | 월 {_fmt_pct(e.get('engage_mon'))}")
        if e.get('dau_1min') is not None:
            lines.append(f"  청취자 수 (1분 이상): {_fmt_dau(e.get('dau_1min'))} | 10분 이상: {_fmt_dau(e.get('dau_10min'))}")
        ch_deep = e.get('channel_deep', {})
        if ch_deep:
            parts = [f"{v.get('name','?')} {_fmt_pct(v.get('rate'))}" for v in ch_deep.values() if v.get('rate') is not None]
            if parts: lines.append(f"  채널별 깊은청취율: {' | '.join(parts)}")
        lines.append('')

    if data.get('growth'):
        g = data['growth']
        lines.append(f"[성장 품질 (Growth) — {data['date_max']}]")
        if g.get('habit_rate') is not None:
            lines.append(f"  습관형성률: 일 {_fmt_pct(g.get('habit_rate'))} ({_fmt_arrow(g.get('habit_diff'))} pp) | 주 {_fmt_pct(g.get('habit_week'))} | 월 {_fmt_pct(g.get('habit_mon'))}")
        if g.get('react_rate') is not None:
            lines.append(f"  복귀율:     일 {_fmt_pct(g.get('react_rate'))} | 주 {_fmt_pct(g.get('react_rate_week'))} | 월 {_fmt_pct(g.get('react_rate_mon'))}")
        for i, p in enumerate(g.get('top3_habit', [])[:3], 1):
            lines.append(f"  습관형성 {i}위: {p.get('name','?')} {_fmt_pct(p.get('rate'))} ({_fmt_dau(p.get('count'))} 신규)")
        lines.append('')

    if data.get('anomalies'):
        alts = data['anomalies'].get('alerts', [])
        intent_type = intent.get('intent', 'general')
        if intent_type == 'anomaly':
            lines.append("[이상 감지 알림 — 전체]")
            for a in alts:
                lines.append(f"  [{a.get('level','').upper()}] {a.get('msg','')}")
            lines.append('')
        else:
            # 다른 intent는 RED/YELLOW만 간략 첨부
            urgent = [a for a in alts if a.get('level') in ('red', 'yellow')]
            if urgent:
                lines.append("[현재 이상 알림]")
                for a in urgent[:3]:
                    lines.append(f"  [{a.get('level','').upper()}] {a.get('msg','')}")
                lines.append('')

    if data.get('channels'):
        ch = data['channels']
        lines.append(f"[채널별 지표 — {data['date_max']}]")
        for ch_code in ['F00', 'L00', 'G00', 'P00']:
            c = ch.get(ch_code)
            if not c:
                continue
            lines.append(
                f"  {c.get('name', ch_code)}: DAU {_fmt_dau(c.get('dau'))} (WoW {_fmt_arrow(c.get('dau_wow'))}) "
                f"| 깊은청취 {_fmt_pct(c.get('deep_rate'))} | 복귀율 {_fmt_pct(c.get('react_rate'))}"
            )
        lines.append('')

    return '\n'.join(lines)




# ── 메인 질의 함수 ─────────────────────────────────────────
def query(question: str, target_date: str = None, verbose: bool = False) -> dict:
    """자연어 질의 — timeline 자체 로드. {"answer": str, "chart_data": dict|None} 리턴."""
    if verbose:
        print("  [1/3] timeline 로드 중...", flush=True)

    # data/ 우선, 같은 폴더 fallback (구 위치 호환)
    _base = os.path.dirname(__file__)
    _candidates = [
        os.path.join(_base, 'data', 'raas_kpi_latest.csv'),
        os.path.join(_base, 'raas_kpi_latest.csv'),
    ]
    local_path = next((p for p in _candidates if os.path.exists(p)), None)
    if not local_path:
        return {"answer": "데이터 파일(raas_kpi_latest.csv)을 찾을 수 없습니다. data/ 폴더에 두세요.", "chart_data": None}
    try:
        import pandas as pd
        df = pd.read_csv(local_path, dtype=str, keep_default_na=False)
        rows = df.to_dict(orient='records')
        timeline, _ = _load_timeline(lambda _: rows)
    except Exception as e:
        return {"answer": f"데이터 로드 실패: {e}", "chart_data": None}

    return _answer(question, timeline, target_date, verbose)


def query_with_timeline(question: str, timeline: dict,
                        target_date: str = None,
                        briefing_data: dict = None) -> dict:
    """서버 캐시 timeline + briefing_data를 받아 처리."""
    if not timeline:
        return {"answer": "데이터를 사용할 수 없습니다.", "chart_data": None}
    return _answer(question, timeline, target_date, verbose=False, briefing_data=briefing_data)


def query_with_timeline_stream(question: str, timeline: dict,
                               target_date: str = None,
                               briefing_data: dict = None):
    """스트리밍용 — (system, context, max_tokens, chart_data) 반환. Claude 호출 없음."""
    if not timeline:
        return None, None, None, None
    return _answer(question, timeline, target_date, verbose=False,
                   briefing_data=briefing_data, _return_context=True)


# intent별 max_tokens 상한
_INTENT_TOKENS = {
    'snapshot': 700, 'trend': 800, 'compare': 800, 'compare_trend': 800, 'dual_trend': 800,
    'ranking': 600, 'overview': 700, 'health': 900,
    'funnel': 1000, 'engagement': 1000, 'growth': 900,
    'anomaly': 800, 'report': 1500, 'general': 1500,
}


def _answer(question, timeline, target_date, verbose, briefing_data=None, _return_context=False):
    if verbose:
        print("  [2/3] 의도 분류 중...", flush=True)
    available_dates = get_available_dates(timeline)
    today_str = None
    if available_dates:
        try:
            today_str = (datetime.strptime(
                available_dates[-1].replace('/', '-'), '%Y-%m-%d'
            ) + timedelta(days=1)).strftime('%Y/%m/%d')
        except Exception:
            pass
    intent = classify_intent(question, today=today_str)
    if target_date:
        intent['specific_date'] = target_date
        intent['date_type'] = 'specific'
    elif available_dates and (
        '어제' in question or intent.get('date_type') == 'yesterday'
    ):
        # "어제"는 항상 타임라인 최신 날짜(= 어제 데이터)로 고정
        intent['specific_date'] = available_dates[-1]
        intent['date_type'] = 'specific'
    # 주말 프로그램 포함 여부 (러브FM 한정)
    intent['include_weekend'] = any(kw in question for kw in ('주말 프로그램 포함', '주말포함', '주말 포함'))
    # overview 키워드 강제 감지 — classifier가 ranking으로 오분류하는 경우 보정
    _OVERVIEW_KW = ('편성시간 순', '편성순', '함께 보여', '전 지표', '전체 지표', '모든 지표',
                    '지표 한눈', '한눈에 보', 'dau와 wau', 'wau와 mau', 'dau·wau', 'wau·mau')
    if any(kw in question for kw in _OVERVIEW_KW):
        intent['intent'] = 'overview'
    # compare_trend 감지: 채널 2개 이상 언급 + 추이/추세 키워드 (또는 vs)
    _HAS_TREND_KW = any(kw in question for kw in ('추이', '추세', '트렌드', '변화'))
    # 질문에 명시된 지표 키워드 → metric 코드 매핑 (channel compare_trend·pgm_group_trend 공용)
    _PGM_METRIC_KW = {
        'dau':        ('DAU', 'dau'),
        'wau':        ('WAU', 'wau'),
        'mau':        ('MAU', 'mau'),
        'new':        ('신규 사용자', '신규자', '신규유입'),
        'churn':      ('이탈률', '이탈율'),
        'react_rate': ('복귀율',),
        'react':      ('복귀 사용자', '복귀자', '복귀유입'),
        'deep':       ('깊은청취율', '깊은청취'),
        'real':       ('실청취율', '실청취'),
        'engage':     ('참여율',),
        'habit':      ('습관형성률', '습관형성'),
        'd1':         ('D1유지율', 'D1유지'),
        'd7':         ('D7유지율', 'D7유지'),
        'w1':         ('W1유지율', 'W1유지'),
        'm1':         ('M1유지율', 'M1유지'),
    }
    # 기간 한정어(주간/월간)가 있으면 베이스 metric을 _week/_mon으로 업그레이드하는 헬퍼
    _PERIOD_UPGRADABLE = {'dau', 'new', 'react', 'churn', 'real', 'deep', 'engage', 'habit', 'react_rate'}

    def _apply_period_suffix(intent_dict, q):
        """질문에 주간/월간 한정어가 있으면 metric suffix를 자동 적용한다."""
        base = intent_dict.get('metric', '')
        if base not in _PERIOD_UPGRADABLE:
            return
        if any(k in q for k in ('주간', '주별', '이번주', '지난주', '이 주')):
            intent_dict['metric'] = f"{base}_week"
        elif any(k in q for k in ('월간', '월별', '이번달', '지난달', '이 달')):
            intent_dict['metric'] = f"{base}_mon"
    _CH_DETECT = {
        'F00': ('파워FM', '파워fm', '파워 fm', 'powerfm'),
        'L00': ('러브FM', '러브fm', '러브 fm', 'lovefm'),
        'G00': ('고릴라M', '고릴라m', 'gorillam'),
        'P00': ('픽채널', 'pickch'),
    }
    _ct_channels = [code for code, kws in _CH_DETECT.items()
                    if any(kw in question for kw in kws)]
    if (_HAS_TREND_KW or 'vs' in question.lower()) and len(_ct_channels) >= 2:
        intent['intent'] = 'compare_trend'
        intent['compare_channels'] = _ct_channels
        intent['days'] = 30  # 추이 쿼리는 항상 30일 기준
        for _mk, _mks in _PGM_METRIC_KW.items():
            if any(k in question for k in _mks):
                intent['metric'] = _mk
                break
        _apply_period_suffix(intent, question)
    # pgm_group_trend 감지: 특정 채널 내 전체 프로그램 추이 요청
    # "파워FM 전체 프로그램 이탈율 추세" → compare_trend with F01~F13
    _PGM_GROUP_MAP = {
        'F': (('파워FM', '파워fm', '파워 fm'),
              PGM_F),
        'L': (('러브FM', '러브fm', '러브 fm'),
              PGM_L),
    }
    _PGM_GROUP_SCOPE_KW = ('전체 프로그램', '프로그램 전체', '모든 프로그램', '속한 전체',
                           '에 속한', '소속 프로그램', '프로그램들의', '프로그램 각각',
                           '프로그램별', '개별 프로그램')
    if (intent.get('intent') not in ('compare_trend',)
            and _HAS_TREND_KW
            and any(kw in question for kw in _PGM_GROUP_SCOPE_KW)):
        for _pfx, (ch_kws, pgm_list) in _PGM_GROUP_MAP.items():
            if any(kw in question for kw in ch_kws):
                intent['intent'] = 'compare_trend'
                intent['compare_channels'] = pgm_list
                intent['days'] = 30
                intent['pgm_group_trend'] = True
                # 질문에 명시된 지표가 있으면 metric 강제 설정
                for _mk, _mks in _PGM_METRIC_KW.items():
                    if any(k in question for k in _mks):
                        intent['metric'] = _mk
                        break
                _apply_period_suffix(intent, question)
                break
    # dual_trend: Claude가 intent='dual_trend'와 metrics 배열을 직접 반환
    # (키워드 후처리 제거 — Claude가 자연어에서 지표를 추출)
    if verbose:
        print(f"  -> {intent.get('summary')}: intent={intent.get('intent')}, "
              f"scope={intent.get('scope')}, days={intent.get('days')}", flush=True)

    data = extract_data(timeline, intent, briefing_data=briefing_data, question=question)
    if 'error' in data:
        return {"answer": f"데이터 추출 실패: {data['error']}", "chart_data": None}

    # 전체 목록 요청 감지 ("전체", "전부", "모두", "다 보여줘" 등)
    _SHOW_ALL_KW = ('전체', '전부', '모두', '다 보여', '전 프로그램', '리스트 전체')
    if data.get('ranking') and any(kw in question for kw in _SHOW_ALL_KW):
        data['ranking_show_all'] = True

    if verbose:
        print("  [3/3] 답변 생성 중...", flush=True)
    context = format_for_claude(data, intent, question, timeline=timeline)
    context = build_query_context(question, context, intent=intent, data=data)
    max_tokens = _INTENT_TOKENS.get(intent.get('intent', 'general'), 600)
    date_max = available_dates[-1] if available_dates else ''
    date_system = (
        f"\n\n[날짜 기준 — 필수 준수]\n"
        f"- 현재날짜(오늘): {today_str}\n"
        f"- 최신데이터: {date_max} (= 어제)\n"
        f"- 사용자가 '어제'라고 하면 {date_max} 날짜 데이터를 사용할 것\n"
        f"- 사용자가 '오늘'이라고 하면 아직 데이터 미수집임을 안내할 것\n"
        f"- '{date_max} 기준 핵심 지표'는 어제 데이터임"
    ) if today_str and date_max else ''
    # 차트 먼저 빌드 — intent_note를 chart 가용 여부에 따라 조정하기 위해
    chart_data = build_chart_data(data, intent, question)
    _has_chart = chart_data is not None
    _chart_type = chart_data.get('type', '') if _has_chart else ''

    # intent별 추가 지시 (차트 가용 여부 반영)
    intent_note = ''
    _it = intent.get('intent', '')

    # ── 차트 시각화 intent: 텍스트에 중복 수치 금지 ──────────────
    if _it == 'ranking':
        intent_note = (
            "\n\n[랭킹 응답 규칙] 순위 테이블은 UI에서 자동 시각화됩니다."
            " 답변 텍스트에 마크다운 테이블·순위 목록을 절대 포함하지 마세요."
            " 핵심 인사이트(1위 프로그램, 주목할 변화 등) 1~2문장으로만 답하세요."
        )
    elif _it == 'overview':
        intent_note = (
            "\n\n[현황표 응답 규칙] 전 지표 현황 테이블은 UI에서 자동 시각화됩니다."
            " 답변 텍스트에 테이블·목록·수치를 절대 포함하지 마세요."
            " 표를 안내하는 자연스러운 한 문장(예: '편성시간 순으로 전체 지표를 정리했습니다.')만 작성하세요."
        )
    elif _it == 'trend':
        intent_note = (
            "\n\n[추이 응답 규칙] 시계열 차트는 UI에서 자동 시각화됩니다."
            " 날짜별 수치 나열·기간 요약 테이블은 답변 텍스트에 절대 포함하지 마세요."
            " 추세의 방향·특징·주목할 변화에 대한 설명을 1~2문장으로 작성하세요."
        )
    elif _it == 'compare_trend':
        _is_pgm_grp = intent.get('pgm_group_trend', False)
        if _chart_type == 'timeseries_multi':
            if _is_pgm_grp:
                intent_note = (
                    "\n\n[프로그램 비교 추이 응답 규칙] 각 프로그램의 멀티시리즈 차트는 UI에서 자동 시각화됩니다."
                    " 날짜별 수치 나열은 절대 포함하지 마세요."
                    " 전체 추세의 공통 패턴을 먼저 한 문장으로 설명하고,"
                    " 가장 눈에 띄게 높거나 낮은 프로그램 1~2개를 이름(코드)과 함께 하이라이트하세요."
                )
            else:
                intent_note = (
                    "\n\n[채널 비교 추이 응답 규칙] 멀티시리즈 차트는 UI에서 자동 시각화됩니다."
                    " 날짜별 수치 나열은 절대 포함하지 마세요."
                    " 각 채널의 추세 방향과 채널 간 차이를 1~2문장으로 비교 설명하세요."
                )
        elif _has_chart:
            intent_note = (
                "\n\n[비교 응답 규칙] 비교 차트가 UI에 표시됩니다."
                " 마크다운 테이블 없이 현재 수치 차이와 특이점을 1~2문장으로 설명하세요."
            )
        else:
            intent_note = (
                "\n\n[비교 응답 규칙] 추이 차트 데이터가 부족합니다."
                " 마크다운 테이블 없이 2~3문장으로 간결하게 비교 설명하세요."
            )
    elif _it == 'dual_trend':
        if _chart_type in ('timeseries_multi', 'timeseries_dual'):
            intent_note = (
                "\n\n[듀얼 지표 응답 규칙] 두 지표의 차트는 UI에서 자동 시각화됩니다."
                " 날짜별 수치 나열은 절대 포함하지 마세요."
                " 두 지표의 상관관계 또는 추세 특징을 1~2문장으로 설명하세요."
            )
        elif _chart_type == 'timeseries':
            intent_note = (
                "\n\n[지표 추이 응답 규칙] 일부 지표만 차트로 표시됩니다."
                " 마크다운 테이블 없이 확인 가능한 데이터 기준으로 1~2문장 설명하세요."
            )
        else:
            intent_note = (
                "\n\n[듀얼 지표 응답 규칙] 추이 데이터가 없어 스냅샷 기준으로 답합니다."
                " 마크다운 테이블·목록 없이 2~3문장으로 간결하게 설명하세요."
            )

    # ── 분석 intent: 분석 방향 + 판단 기준 지시 ──────────────────
    elif _it == 'snapshot':
        intent_note = (
            "\n\n[스냅샷 분석 규칙] 어제 핵심 지표를 요약할 때 단순 수치 나열이 아니라"
            " WoW 방향과 '양호/주의/위험' 판단을 포함하세요."
            " 가장 주목할 변화 1가지를 마지막에 명시하세요."
        )
    elif _it == 'funnel':
        intent_note = (
            "\n\n[퍼널 분석 규칙] 신규→유지→이탈 흐름에서 가장 큰 누수 단계(D1/D7/W1/M1 중)"
            " 를 먼저 특정해 제시하세요. 전주 대비 가장 크게 악화된 단계를 하이라이트하고,"
            " 편성·콘텐츠팀이 즉시 취할 수 있는 액션 1가지로 마무리하세요."
        )
    elif _it == 'engagement':
        intent_note = (
            "\n\n[인게이지먼트 분석 규칙] 깊은청취율·실청취율·참여율은 절대 수준과 추세 방향을"
            " 함께 판단하세요. 기준: 깊은청취율 90%↑ 양호 / 80%↓ 주의."
            " 일/주/월 단위 중 방향이 다른 구간이 있으면 그 의미를 해석하세요."
        )
    elif _it == 'growth':
        intent_note = (
            "\n\n[성장 분석 규칙] 습관형성률은 장기 사용자 기반의 핵심 지표입니다."
            " 기준: 15% 이하 부진(YELLOW) / 30% 이상 우수. 현재 수준을 이 기준으로 판단하고,"
            " 일/주/월 추세에서 반전 또는 지속 여부를 명확히 하세요."
        )
    elif _it == 'anomaly':
        intent_note = (
            "\n\n[이상 감지 분석 규칙] RED → YELLOW → GREEN 순으로 심각도 우선 서술하세요."
            " 각 알림에 대해 현재 수치·임계값 차이·권장 액션을 포함하세요."
            " GREEN(이상 없음)이면 안심 메시지와 함께 지속 모니터링 권고 지표 1개를 제시하세요."
        )
    elif _it == 'report':
        intent_note = (
            "\n\n[종합 리포트 분석 규칙] 모든 섹션 데이터를 통합 분석해"
            " '오늘의 핵심 메시지' 1가지를 첫 줄에 제시하세요."
            " 이후 규모→품질→성장→채널 순으로 간결히 요약하고,"
            " 이번 주 가장 주의할 지표 1가지와 구체적 액션으로 마무리하세요."
        )
    elif _it == 'health':
        intent_note = (
            "\n\n[위험 프로그램 분석 규칙] DAU 규모·이탈률·WoW를 종합해 가장 긴급한"
            " 1~2개 프로그램을 선별하세요. 프로그램명·DAU·이탈률·WoW 수치를 포함하고"
            " 위험 판단 근거를 2~3문장으로 설명하세요."
        )
    elif _it == 'compare':
        intent_note = (
            "\n\n[비교 분석 규칙] 채널 비교 시 DAU와 함께 실청취율을 반드시 언급하세요"
            " (실청취율 = 실제 1분 이상 청취한 비율, 채널 콘텐츠 품질의 핵심 지표)."
            " 비교 대상 간 가장 주목할 차이 1가지를 먼저 제시하고,"
            " 실청취율·깊은청취율·이탈률로 근거를 보완하세요."
            " 마크다운 테이블 없이 서술형으로 작성하세요."
        )
    _static_system  = QUERY_SYSTEM_PROMPT + _load_rules()
    _chart_note = (
        "\n\n[차트·테이블 중복 금지] UI에 차트 또는 테이블이 자동 표시됩니다."
        " 동일한 수치를 텍스트 표·목록·수치 나열로 중복 제시하지 마세요."
    ) if _has_chart else ''
    _dynamic_system = date_system + intent_note + _chart_note
    full_system = (_static_system, _dynamic_system)
    if _return_context:
        return _static_system + _dynamic_system, context, max_tokens, chart_data
    _model = HAIKU_MODEL if _it in _HAIKU_INTENTS else CLAUDE_MODEL
    answer_text, usage = call_claude(full_system, context, max_tokens=max_tokens, model=_model)
    return {
        "answer": answer_text,
        "chart_data": chart_data,
        "input_tokens":  usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
    }


# ── CLI ───────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RAAS 자연어 질의 v3")
    parser.add_argument("question", nargs="?", help="질문")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()

    if args.demo:
        demos = [
            "어제 가장 많이 들은 프로그램은?",
            "최근 일주일 DAU 추세는?",
            "파워FM이랑 러브FM 비교해줘",
            "김영철 프로그램 어때?",
            "고릴라 앱 전반적으로 잘 되고 있어?",
        ]
        for q in demos:
            print(f"\n{'='*55}\nQ: {q}\n{'='*55}")
            result = query(q, verbose=True)
            print(result["answer"])
            cd = result.get("chart_data")
            if cd:
                print(f"  [chart] type={cd['type']} title={cd['title']}")
    elif args.question:
        result = query(args.question, verbose=args.verbose)
        print(result["answer"])
    else:
        parser.print_help()
