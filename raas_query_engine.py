"""
RAAS 자연어 질의 엔진 v3.0
- timeline 기반 (raas_kpi_latest.csv 단일 소스)
- splunk_search / SPLUNK_HOST 의존성 제거
- 환각 방지 강화 (데이터 출처 명시, 범위 밖 질문 거부)
"""

import json
import os
import urllib.request
import sys
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import raas_briefing_engine as BE
from raas_prompts import QUERY_SYSTEM_PROMPT
from raas_briefing_context import build_query_context

# ── 설정 ──────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
# ──────────────────────────────────────────────────────────

_WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]

def _weekday_ko(date_str: str) -> str:
    """'YYYY/MM/DD' 또는 'YYYY-MM-DD' → 한국 요일 약칭. Python datetime 기준으로 계산."""
    try:
        return _WEEKDAY_KO[datetime.strptime(date_str.replace("/", "-"), "%Y-%m-%d").weekday()]
    except Exception:
        return ""

# ── PGM_CODE 매핑 ──────────────────────────────────────────
SCOPE_MAP = {
    'platform': 'T00', 'all': 'T00', 'T00': 'T00',
    '전체': 'T00', '고릴라': 'T00',
    'powerfm': 'F00', '파워fm': 'F00', '파워': 'F00', 'F00': 'F00',
    'lovefm': 'L00', '러브fm': 'L00', '러브': 'L00', 'L00': 'L00',
    'gorillam': 'G00', '고릴라m': 'G00', 'G00': 'G00',
    'pickch': 'P00', '픽채널': 'P00', 'P00': 'P00',
}

def _build_keyword_index():
    idx = {}
    for code, name in BE.PGM_NAMES.items():
        if not name:
            continue
        idx[name.lower()] = code
        if '김영철' in name: idx['김영철'] = code
        if '컬투' in name:   idx['컬투쇼'] = code; idx['컬투'] = code
        if '봉태규' in name: idx['봉태규'] = code
        if '주현영' in name: idx['주현영'] = code
        if '황제' in name:   idx['황제파워'] = code
        if '정치쇼' in name: idx['정치쇼'] = code
        if '이숙영' in name: idx['이숙영'] = code
    return idx

KEYWORD_TO_CODE = _build_keyword_index()


# ── 차트 빌더 ──────────────────────────────────────────────
_METRIC_LABELS = {
    'dau':         ('DAU',        '명'),
    'new':         ('신규 유입',  '명'),
    'react':       ('복귀 사용자','명'),
    'deep_rate':   ('깊은청취율', '%'),
    'real_rate':   ('실청취율',   '%'),
    'engage_rate': ('참여율',     '%'),
    'habit_rate':  ('습관형성률', '%'),
    'churn_rate':  ('이탈률',     '%'),
    'react_rate':  ('복귀율',     '%'),
}

def _metric_meta(metric_field: str):
    return _METRIC_LABELS.get(metric_field, (metric_field, ''))


def build_chart_timeseries(title, metric, unit, points, source):
    """시계열 → sparkline dict. points < 2이면 None."""
    pts = [p for p in points if p.get('value') is not None]
    if len(pts) < 2:
        return None
    values = [p['value'] for p in pts]
    first, latest = values[0], values[-1]
    change_pct = round((latest - first) / first * 100, 1) if first else None
    return {
        "type": "timeseries",
        "title": title,
        "metric": metric,
        "unit": unit,
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
        intent_type = intent.get('intent', 'general')
        scope       = data.get('scope', 'T00')
        scope_name  = data.get('scope_name', scope)
        date_max    = (data.get('date_max') or '').replace('/', '-')

        # trend → timeseries
        if intent_type == 'trend' and 'trend' in data:
            t = data['trend']
            points = [{"date": d.replace('/', '-'), "value": v}
                      for d, v in t['data'] if v is not None]
            metric, unit = _metric_meta(t['metric_field'])
            return build_chart_timeseries(
                title=f"{scope_name} {metric}",
                metric=metric, unit=unit, points=points,
                source=f"timeline:{scope}/{t['metric_field']}"
            )

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

        # ranking → comparison (TOP5)
        if intent_type == 'ranking' and data.get('ranking'):
            items = [{"label": r['name'], "value": r['dau']}
                     for r in data['ranking'][:5] if r.get('dau')]
            return build_chart_comparison(
                title="프로그램 DAU TOP5",
                metric="DAU", unit="명",
                date=date_max, items=items,
                source=f"snapshot:{date_max}"
            )

        # health → trend 있으면 timeseries, 없으면 ranking comparison
        if intent_type == 'health':
            if 'trend' in data:
                t = data['trend']
                points = [{"date": d.replace('/', '-'), "value": v}
                          for d, v in t['data'] if v is not None]
                metric, unit = _metric_meta(t['metric_field'])
                return build_chart_timeseries(
                    title=f"{scope_name} {metric} 추이",
                    metric=metric, unit=unit, points=points,
                    source=f"timeline:{scope}/{t['metric_field']}"
                )
            if data.get('ranking'):
                items = [{"label": r['name'], "value": r['dau']}
                         for r in data['ranking'][:5] if r.get('dau')]
                return build_chart_comparison(
                    title="프로그램 DAU TOP5",
                    metric="DAU", unit="명",
                    date=date_max, items=items,
                    source=f"snapshot:{date_max}"
                )

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
                source=f"timeline:{scope}/deep_rate"
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

        # general / snapshot: 질문 키워드 기반 fallback
        q = question.lower()
        trend_kw   = ["추세", "추이", "변화", "최근", "지난주", "이번 주", "트렌드", "흐름"]
        compare_kw = ["vs", "비교", "차이", "1위", "top", "순위", "어디가"]

        if any(kw in q for kw in trend_kw) and 'trend' in data:
            t = data['trend']
            points = [{"date": d.replace('/', '-'), "value": v}
                      for d, v in t['data'] if v is not None]
            metric, unit = _metric_meta(t['metric_field'])
            return build_chart_timeseries(
                title=f"{scope_name} {metric}",
                metric=metric, unit=unit, points=points,
                source=f"timeline:{scope}/{t['metric_field']}"
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

        return None
    except Exception:
        return None


# ── Claude 호출 ────────────────────────────────────────────
def call_claude(system: str, user: str, max_tokens: int = 1000) -> str:
    payload = json.dumps({
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
            "x-api-key": ANTHROPIC_API_KEY
        })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["content"][0]["text"]


# ── 의도 분류 ──────────────────────────────────────────────
INTENT_SYSTEM = """RAAS 데이터 분석 시스템의 질의 분류기입니다.
사용자 질문을 분석해서 JSON으로만 응답하세요. 다른 텍스트 없이 JSON만.

응답 형식:
{
  "intent": "snapshot|trend|compare|ranking|health|funnel|engagement|growth|anomaly|report|general",
  "scope": "T00|F00|L00|G00|P00 또는 PGM_CODE 또는 null",
  "scope_keyword": "사용자가 언급한 채널/프로그램 키워드 또는 null",
  "metric": "dau|deep|new|react|churn|engage|habit|real|all",
  "date_type": "yesterday|today|specific|range",
  "specific_date": "YYYY/MM/DD 또는 null",
  "days": 7,
  "summary": "질문 한 줄 요약"
}

intent 정의:
- snapshot: 특정 날짜 단일 시점 (어제 DAU, 오늘 현황)
- trend: 추세/흐름 (최근 N일 변화, 오르고 있나)
- compare: 채널/기간 비교 (파워FM vs 러브FM)
- ranking: 순위 TOP N (가장 많이 들은 프로그램)
- health: 건강도/위험 프로그램 (잘 되고 있어? 위험한 거 있어?)
- funnel: 사용자 흐름 (신규·유지·이탈·복귀 비율, D1/D7/W1/M1 유지율, 리텐션, 코호트)
- engagement: 청취 품질 (깊은청취율, 실청취율, 참여율, 몰입도)
- growth: 성장 지표 (습관형성률, 복귀율, 신규코호트 유지)
- anomaly: 이상 감지 (경보, 알림, 이상 있어?, 급락/급등)
- report: 종합 리포트 (전체 현황 요약, 브리핑, 모두 보여줘)
- general: 기타 (위 분류에 해당 없음)

scope 결정:
- 채널 언급 시: F00(파워FM) / L00(러브FM) / G00(고릴라M) / P00(픽채널)
- "전체" 또는 미언급 시: T00
- 프로그램 언급 시: scope_keyword에 키워드 입력

metric: dau/deep/new/react/churn/engage/habit/real/d1/d7/w1/m1/all (명시 없으면 all)
days: 기간 명시 없으면 7"""


def classify_intent(question: str) -> dict:
    try:
        result = call_claude(INTENT_SYSTEM, question, max_tokens=300)
        result = result.strip()
        if result.startswith("```"):
            parts = result.split("```")
            result = parts[1]
            if result.startswith("json"):
                result = result[4:]
        intent = json.loads(result.strip())

        # scope_keyword → PGM_CODE 매핑
        # Phase 5 Step 4: 어댑터 우선 → SCOPE_MAP/KEYWORD_TO_CODE fallback
        if intent.get('scope_keyword') and not intent.get('scope'):
            kw_raw = intent['scope_keyword'].strip()
            kw = kw_raw.lower()
            # 1) SCOPE_MAP 우선 (채널/플랫폼 영문/한글 매핑은 어댑터에 없음)
            for key, code in SCOPE_MAP.items():
                if key.lower() in kw:
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
            # 3) KEYWORD_TO_CODE fallback (기존 하드코딩 별칭)
            if not intent.get('scope'):
                for kw_key, code in KEYWORD_TO_CODE.items():
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
    전주 절대값이 CSV에 없는 경우 current - diff 로 계산.
    """
    i = BE._i; fn = BE._fn

    def _prev(cur, diff):
        if cur is not None and diff is not None:
            return round(cur - diff, 1)
        return None

    # 계산에 재사용할 핵심 일간 값
    _deep   = fn(row.get('deep_rate'));   _deep_d  = fn(row.get('deep_rate_diff'))
    _real   = fn(row.get('real_rate'));   _real_d  = fn(row.get('real_rate_diff'))
    _eng    = fn(row.get('engage_rate')); _eng_d   = fn(row.get('engage_rate_diff'))
    _hab    = fn(row.get('habit_rate'));  _hab_d   = fn(row.get('habit_rate_diff'))
    _churn  = fn(row.get('churn_rate'));  _churn_d = fn(row.get('churn_rate_diff'))
    _rr     = fn(row.get('react_rate'));  _rr_d    = fn(row.get('react_rate_diff'))

    return {
        'date': date,
        # ── 일간 규모 ─────────────────────────────────────────
        'dau':            i(row.get('dau')),
        'dau_wow':        fn(row.get('dau_chg')),
        'dau_prev':       i(row.get('dau_prev')),       # 전주 동일 요일 DAU
        'dau_d2':         i(row.get('dau_d2')),         # D-2(어제) DAU
        'dau_1min':       i(row.get('dau_1min')),
        'dau_10min':      i(row.get('dau_10min')),
        'dau_r7':         i(row.get('dau_r7')),         # 7일 롤링 DAU
        'dau_r7_chg':     fn(row.get('dau_r7_chg')),   # 7일 롤링 WoW
        'dau_r7_prev':    i(row.get('dau_r7_prev')),
        'dau_r30':        i(row.get('dau_r30')),        # 30일 롤링 DAU
        'dau_r30_chg':    fn(row.get('dau_r30_chg')),  # 30일 롤링 MoM
        'dau_r30_prev':   i(row.get('dau_r30_prev')),
        # ── 신규·복귀·이탈 ────────────────────────────────────
        'new_user':       i(row.get('new')),
        'new_share':      fn(row.get('new_share')),
        'new_wow':        fn(row.get('new_chg')),
        'new_prev':       i(row.get('new_prev')),       # 전주 신규 절대값
        'react_user':     i(row.get('react')),
        'react_share':    fn(row.get('react_share')),
        'react_wow':      fn(row.get('react_chg')),     # 복귀수 WoW
        'react_prev':     i(row.get('react_prev')),     # 전주 복귀 절대값
        'react_rate':     _rr,
        'react_rate_diff':_rr_d,
        'react_rate_prev':_prev(_rr, _rr_d),
        'churn_rate':     _churn,
        'churn_diff':     _churn_d,
        'churn_rate_prev':_prev(_churn, _churn_d),      # 전주 이탈율 (계산)
        # ── 청취 품질 (일) ────────────────────────────────────
        'deep_rate':      _deep,
        'deep_diff':      _deep_d,
        'deep_rate_prev': _prev(_deep, _deep_d),        # 전주 깊은청취율 (계산)
        'real_rate':      _real,
        'real_diff':      _real_d,
        'real_rate_prev': fn(row.get('real_rate_prev')) if fn(row.get('real_rate_prev')) is not None else _prev(_real, _real_d),
        'engage_rate':    _eng,
        'engage_diff':    _eng_d,
        'engage_rate_prev':_prev(_eng, _eng_d),
        'habit_rate':     _hab,
        'habit_diff':     _hab_d,
        'habit_rate_prev':_prev(_hab, _hab_d),
        # ── 유지율 — 전체 코호트 ──────────────────────────────
        'd1_ret':         fn(row.get('d1_ret')),
        'd1_ret_diff':    fn(row.get('d1_ret_diff')),
        'd7_ret':         fn(row.get('d7_ret')),
        'd7_ret_diff':    fn(row.get('d7_ret_diff')),
        'w1_ret':         fn(row.get('w1_ret')),
        'w1_ret_diff':    fn(row.get('w1_ret_diff')),
        'm1_ret':         fn(row.get('m1_ret')),
        'm1_ret_diff':    fn(row.get('m1_ret_diff')),
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
        # ── 주간 (WAU) — CSV 필드명: wau, wau_chg ─────────────
        'dau_week':         i(row.get('wau')),
        'dau_week_wow':     fn(row.get('wau_chg')),
        'dau_week_prev':    i(row.get('wau_prev')),     # 전전주 WAU
        'dau_week_avg':     fn(row.get('dau_week_avg')),
        'deep_week':        fn(row.get('deep_rate_week')),
        'real_week':        fn(row.get('real_rate_week')),
        'real_week_prev':   fn(row.get('real_rate_week_prev')),
        'engage_week':      fn(row.get('engage_rate_week')),
        'habit_week':       fn(row.get('habit_rate_week')),
        'churn_week':       fn(row.get('churn_rate_week')),
        'new_week':         i(row.get('new_week')),
        'new_week_share':   fn(row.get('new_week_share')),
        'new_week_prev':    i(row.get('new_week_prev')),    # 전주 신규
        'new_week_wow':     fn(row.get('new_week_chg')),
        'react_week':       i(row.get('react_week')),
        'react_week_prev':  i(row.get('react_week_prev')),  # 전주 복귀
        'react_week_wow':   fn(row.get('react_week_chg')),
        'react_rate_week':  fn(row.get('react_rate_week')),
        'react_rate_week_diff':fn(row.get('react_rate_week_diff')),
        # ── 월간 (MAU) — CSV 필드명: mau, mau_chg ─────────────
        'dau_mon':          i(row.get('mau')),
        'dau_mon_wow':      fn(row.get('mau_chg')),
        'dau_mon_prev':     i(row.get('mau_prev')),     # 전전월 MAU
        'dau_mon_avg':      fn(row.get('dau_mon_avg')),
        'wau_mon_avg':      fn(row.get('wau_mon_avg')),
        'deep_mon':         fn(row.get('deep_rate_mon')),
        'real_mon':         fn(row.get('real_rate_mon')),
        'real_mon_prev':    fn(row.get('real_rate_mon_prev')),
        'engage_mon':       fn(row.get('engage_rate_mon')),
        'habit_mon':        fn(row.get('habit_rate_mon')),
        'churn_mon':        fn(row.get('churn_rate_mon')),
        'new_mon':          i(row.get('new_mon')),
        'new_mon_share':    fn(row.get('new_mon_share')),
        'new_mon_prev':     i(row.get('new_mon_prev')),     # 전월 신규
        'new_mon_wow':      fn(row.get('new_mon_chg')),
        'react_mon':        i(row.get('react_mon')),
        'react_mon_wow':    fn(row.get('react_mon_chg')),
        'react_rate_mon':   fn(row.get('react_rate_mon')),
        'react_rate_mon_diff':fn(row.get('react_rate_mon_diff')),
        # ── 편성 메타 ─────────────────────────────────────────
        'guestname':        (row.get('guestname') or '').strip(),
    }


def extract_data(timeline, intent: dict, briefing_data: dict = None) -> dict:
    if not timeline:
        return {'error': 'timeline 없음'}
    available_dates = BE.get_available_dates(timeline)
    if not available_dates:
        return {'error': '데이터 없음'}

    intent_type = intent.get('intent', 'general')
    scope = intent.get('scope', 'T00')
    metric = intent.get('metric', 'all')
    days = min(intent.get('days', 7), len(available_dates))

    if scope not in timeline:
        scope = 'T00' if 'T00' in timeline else list(timeline.keys())[0]

    data = {
        'scope': scope,
        'scope_name': BE._pgm_name(scope),
        'metric': metric,
        'date_min': available_dates[0],
        'date_max': available_dates[-1],
        'available_days': len(available_dates),
    }

    # snapshot
    if intent_type in ('snapshot', 'general'):
        latest_date = available_dates[-1]
        row = timeline.get(scope, {}).get(latest_date, {})
        data['snapshot'] = _extract_full_snapshot(row, latest_date)

    # trend
    if intent_type in ('trend', 'general', 'health', 'engagement'):
        field_map = {
            'dau': 'dau', 'deep': 'deep_rate', 'new': 'new',
            'react': 'react', 'churn': 'churn_rate',
            'engage': 'engage_rate', 'habit': 'habit_rate',
            'real': 'real_rate', 'all': 'dau',
            'd1': 'd1_ret', 'd7': 'd7_ret', 'w1': 'w1_ret', 'm1': 'm1_ret',
        }
        mf = field_map.get(metric, 'dau')
        trend_data = BE.get_metric_trend(timeline, scope, mf, days=days)
        data['trend'] = {'metric_field': mf, 'days': len(trend_data), 'data': trend_data}

    # ranking
    if intent_type in ('ranking', 'health', 'general'):
        latest_date = available_dates[-1]
        exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
        rows = []
        for code, date_rows in timeline.items():
            if code in exclude:
                continue
            row = date_rows.get(latest_date)
            if not row:
                continue
            dau = BE._i(row.get('dau'))
            if not dau or dau <= 0:
                continue
            rows.append({
                'code': code, 'name': BE._pgm_name(code, row=row), 'dau': dau,
                'deep_rate': BE._fn(row.get('deep_rate')),
                'churn_rate': BE._fn(row.get('churn_rate')),
                'dau_wow': BE._fn(row.get('dau_chg')),
            })
        rows.sort(key=lambda x: x['dau'], reverse=True)
        data['ranking'] = rows[:10]

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
                    'code': ch, 'name': BE._pgm_name(ch, row=row),
                    'dau': BE._i(row.get('dau')),
                    'dau_wow': BE._fn(row.get('dau_chg')),
                    'deep_rate': BE._fn(row.get('deep_rate')),
                    'churn_rate': BE._fn(row.get('churn_rate')),
                })
            data['compare'] = ch_rows
        else:
            # 프로그램 스코프: 주간 비교 (지난주 vs 지지난주)
            compare_days = max(days * 2, 14)
            raw_trend = BE.get_metric_trend(timeline, scope, 'dau', days=compare_days)
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
                    'scope_name': BE._pgm_name(scope),
                    'week1': _wk(w1_pts),
                    'week2': _wk(w2_pts),
                }
                # trend도 채워서 차트 렌더링 활용
                data['trend'] = {'metric_field': 'dau', 'days': len(valid_pts), 'data': valid_pts}

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
                    'dau':         BE._i(row.get('dau')),
                    'churn_rate':  BE._fn(row.get('churn_rate')),
                    'dau_chg':     BE._fn(row.get('dau_chg')),
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
                        'name': BE._pgm_name(code, row=row),
                        'dau':        BE._i(row.get('dau')),
                        'churn_rate': BE._fn(row.get('churn_rate')),
                        'dau_wow':    BE._fn(row.get('dau_chg')),
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
                churn = BE._fn(row.get('churn_rate'))
                wow   = BE._fn(row.get('dau_chg'))
                dau   = BE._i(row.get('dau'))
                if churn and wow and dau and dau >= 1000 and churn >= 30 and wow <= -5:
                    risks.append({
                        'code': code, 'name': BE._pgm_name(code, row=row),
                        'dau': dau, 'churn_rate': churn, 'dau_wow': wow,
                    })
        risks.sort(key=lambda x: x.get('dau_wow') or 0)
        data['risks'] = risks[:5]

    # ── 신규 intent: briefing_data 우선, timeline fallback ──────────
    bd = briefing_data or {}

    # funnel — s2_funnel 우선
    if intent_type == 'funnel':
        if bd.get('s2_funnel'):
            data['funnel'] = bd['s2_funnel']
        else:
            latest_date = available_dates[-1]
            row = timeline.get('T00', {}).get(latest_date, {})
            data['funnel'] = {
                'dau':              BE._i(row.get('dau')),
                'new_user':         BE._i(row.get('new')),
                'new_share':        BE._fn(row.get('new_share')),
                'new_wow':          BE._fn(row.get('new_chg')),
                'react_user':       BE._i(row.get('react')),
                'react_share':      BE._fn(row.get('react_share')),
                'react_rate':       BE._fn(row.get('react_rate')),
                'churn_rate':       BE._fn(row.get('churn_rate')),
                'churn_diff':       BE._fn(row.get('churn_rate_diff')),
                'd1_ret':           BE._fn(row.get('d1_ret')),
                'd1_ret_diff':      BE._fn(row.get('d1_ret_diff')),
                'd7_ret':           BE._fn(row.get('d7_ret')),
                'new_d1_ret':       BE._fn(row.get('new_d1_ret')),
                'new_d7_ret':       BE._fn(row.get('new_d7_ret')),
                'new_w1_ret':       BE._fn(row.get('new_w1_ret')),
                'new_m1_ret':       BE._fn(row.get('new_m1_ret')),
                'new_week':         BE._i(row.get('new_week')),
                'new_week_share':   BE._fn(row.get('new_week_share')),
                'churn_rate_week':  BE._fn(row.get('churn_rate_week')),
                'w1_ret':           BE._fn(row.get('w1_ret')),
                'react_week':       BE._i(row.get('react_week')),
                'react_rate_week':  BE._fn(row.get('react_rate_week')),
                'new_mon':          BE._i(row.get('new_mon')),
                'new_mon_share':    BE._fn(row.get('new_mon_share')),
                'churn_rate_mon':   BE._fn(row.get('churn_rate_mon')),
                'm1_ret':           BE._fn(row.get('m1_ret')),
                'react_mon':        BE._i(row.get('react_mon')),
                'react_rate_mon':   BE._fn(row.get('react_rate_mon')),
            }

    # engagement — s3_engagement 우선; 추세 차트용 trend도 함께 수집
    if intent_type == 'engagement':
        if bd.get('s3_engagement'):
            data['engagement'] = bd['s3_engagement']
        else:
            latest_date = available_dates[-1]
            row = timeline.get('T00', {}).get(latest_date, {})
            data['engagement'] = {
                'dau_1min':          BE._i(row.get('dau_1min')),
                'dau_10min':         BE._i(row.get('dau_10min')),
                'deep_rate':         BE._fn(row.get('deep_rate')),
                'deep_rate_diff':    BE._fn(row.get('deep_rate_diff')),
                'deep_rate_week':    BE._fn(row.get('deep_rate_week')),
                'deep_rate_mon':     BE._fn(row.get('deep_rate_mon')),
                'real_rate':         BE._fn(row.get('real_rate')),
                'real_rate_diff':    BE._fn(row.get('real_rate_diff')),
                'real_rate_week':    BE._fn(row.get('real_rate_week')),
                'real_rate_mon':     BE._fn(row.get('real_rate_mon')),
                'engage_rate':       BE._fn(row.get('engage_rate')),
                'engage_week':       BE._fn(row.get('engage_rate_week')),
                'engage_mon':        BE._fn(row.get('engage_rate_mon')),
            }
        # 깊은청취율 trend (차트용)
        dr_trend = BE.get_metric_trend(timeline, scope, 'deep_rate', days=days)
        data['engagement_trend'] = dr_trend

    # growth — s4_growth 우선
    if intent_type == 'growth':
        if bd.get('s4_growth'):
            data['growth'] = bd['s4_growth']
        else:
            latest_date = available_dates[-1]
            row = timeline.get('T00', {}).get(latest_date, {})
            data['growth'] = {
                'habit_rate':       BE._fn(row.get('habit_rate')),
                'habit_diff':       BE._fn(row.get('habit_rate_diff')),
                'habit_week':       BE._fn(row.get('habit_rate_week')),
                'habit_mon':        BE._fn(row.get('habit_rate_mon')),
                'react_rate':       BE._fn(row.get('react_rate')),
                'react_rate_week':  BE._fn(row.get('react_rate_week')),
                'react_rate_mon':   BE._fn(row.get('react_rate_mon')),
                'top3_habit':       [],
            }

    # anomaly — s7_anomalies 우선
    if intent_type == 'anomaly':
        if bd.get('s7_anomalies'):
            data['anomalies'] = bd['s7_anomalies']
        else:
            data['anomalies'] = {'alerts': [{'level': 'green', 'msg': '브리핑 데이터 없음 — timeline 기준 이상 감지 불가'}]}

    # report — 전 섹션 묶음 (briefing_data 우선, 없으면 timeline snapshot 보강)
    if intent_type == 'report':
        data['report'] = {
            's1': bd.get('s1_executive', {}),
            's2': bd.get('s2_funnel', {}),
            's3': bd.get('s3_engagement', {}),
            's4': bd.get('s4_growth', {}),
            's5': bd.get('s5_rankings', {}),
            's6': bd.get('s6_channels', {}),
            's7': bd.get('s7_anomalies', {}),
        }
        # briefing_data 없으면 timeline 기반 snapshot·ranking도 함께 추출
        if not bd:
            latest_date = available_dates[-1]
            row = timeline.get('T00', {}).get(latest_date, {})
            if not data.get('snapshot'):
                data['snapshot'] = _extract_full_snapshot(row, latest_date)
            # ranking도 추출
            if not data.get('ranking'):
                exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
                rows = []
                for code, date_rows in timeline.items():
                    if code in exclude: continue
                    r_row = date_rows.get(latest_date)
                    if not r_row: continue
                    dau = BE._i(r_row.get('dau'))
                    if not dau or dau <= 0: continue
                    rows.append({
                        'code': code, 'name': BE._pgm_name(code, row=r_row), 'dau': dau,
                        'deep_rate': BE._fn(r_row.get('deep_rate')),
                        'churn_rate': BE._fn(r_row.get('churn_rate')),
                        'dau_wow': BE._fn(r_row.get('dau_chg')),
                    })
                rows.sort(key=lambda x: x['dau'], reverse=True)
                data['ranking'] = rows[:5]

    # 모든 intent에 s7 이상 알림 첨부 (존재 시)
    if bd.get('s7_anomalies') and 'anomalies' not in data:
        data['anomalies'] = bd['s7_anomalies']

    return data


def format_for_claude(data: dict, intent: dict, question: str) -> str:
    if 'error' in data:
        return f"질문: {question}\n[오류] {data['error']}"

    lines = [
        f"질문: {question}\n",
        f"--- 데이터 출처: raas_kpi_latest.csv ({data['date_min']} ~ {data['date_max']}, {data['available_days']}일치) ---",
        f"--- 분석 대상: {data['scope_name']} ({data['scope']}) ---\n",
    ]

    if 'snapshot' in data:
        s = data['snapshot']
        lines.append(f"[{s['date']} 기준 핵심 지표]")

        lines.append(f"  [일간 규모]")
        lines.append(f"    DAU: {_fmt_dau(s.get('dau'))} (WoW {_fmt_arrow(s.get('dau_wow'))} | 전주동일요일 {_fmt_dau(s.get('dau_prev'))})")
        if s.get('dau_d2'):
            lines.append(f"    D-2(어제) DAU: {_fmt_dau(s.get('dau_d2'))}")
        if s.get('dau_r7'):
            lines.append(f"    7일롤링: {_fmt_dau(s.get('dau_r7'))} (WoW {_fmt_arrow(s.get('dau_r7_chg'))})")
        if s.get('dau_r30'):
            lines.append(f"    30일롤링: {_fmt_dau(s.get('dau_r30'))} (MoM {_fmt_arrow(s.get('dau_r30_chg'))})")
        if s.get('dau_1min') or s.get('dau_10min'):
            lines.append(f"    1분↑청취자: {_fmt_dau(s.get('dau_1min'))} | 10분↑청취자: {_fmt_dau(s.get('dau_10min'))}")

        lines.append(f"  [신규·복귀·이탈]")
        lines.append(f"    신규: {_fmt_dau(s.get('new_user'))} ({_fmt_pct(s.get('new_share'))}) WoW {_fmt_arrow(s.get('new_wow'))} | 전주 {_fmt_dau(s.get('new_prev'))}")
        lines.append(f"    복귀: {_fmt_dau(s.get('react_user'))} ({_fmt_pct(s.get('react_share'))}) WoW {_fmt_arrow(s.get('react_wow'))} | 전주 {_fmt_dau(s.get('react_prev'))}")
        lines.append(f"    복귀율: {_fmt_pct(s.get('react_rate'))} (Δ {_fmt_arrow(s.get('react_rate_diff'))} pp | 전주 {_fmt_pct(s.get('react_rate_prev'))})")
        if s.get('churn_rate') is not None:
            lines.append(f"    이탈율: {_fmt_pct(s.get('churn_rate'))} (Δ {_fmt_arrow(s.get('churn_diff'))} pp | 전주 {_fmt_pct(s.get('churn_rate_prev'))})")

        lines.append(f"  [청취 품질]")
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
                lines.append(f"    D1: {_fmt_pct(s.get('d1_ret'))} (Δ {_fmt_arrow(s.get('d1_ret_diff'))} pp)")
            if s.get('d7_ret') is not None:
                lines.append(f"    D7: {_fmt_pct(s.get('d7_ret'))} (Δ {_fmt_arrow(s.get('d7_ret_diff'))} pp)")
            if s.get('w1_ret') is not None:
                lines.append(f"    W1: {_fmt_pct(s.get('w1_ret'))} (Δ {_fmt_arrow(s.get('w1_ret_diff'))} pp)")
            if s.get('m1_ret') is not None:
                lines.append(f"    M1: {_fmt_pct(s.get('m1_ret'))} (Δ {_fmt_arrow(s.get('m1_ret_diff'))} pp)")

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
            if s.get('deep_week') is not None:
                lines.append(f"    깊은청취율: {_fmt_pct(s.get('deep_week'))} | 실청취율: {_fmt_pct(s.get('real_week'))} (전주 {_fmt_pct(s.get('real_week_prev'))}) | 참여율: {_fmt_pct(s.get('engage_week'))}")
            if s.get('churn_week') is not None:
                lines.append(f"    이탈율: {_fmt_pct(s.get('churn_week'))} | 복귀율: {_fmt_pct(s.get('react_rate_week'))} (Δ {_fmt_arrow(s.get('react_rate_week_diff'))} pp) | 습관: {_fmt_pct(s.get('habit_week'))}")
            if s.get('new_week') or s.get('react_week'):
                lines.append(f"    신규: {_fmt_dau(s.get('new_week'))} ({_fmt_pct(s.get('new_week_share'))}) WoW {_fmt_arrow(s.get('new_week_wow'))} | 전주 {_fmt_dau(s.get('new_week_prev'))}")
                lines.append(f"    복귀: {_fmt_dau(s.get('react_week'))} WoW {_fmt_arrow(s.get('react_week_wow'))} | 전주 {_fmt_dau(s.get('react_week_prev'))}")

        if s.get('dau_mon'):
            lines.append(f"  [월간 (MAU)]")
            lines.append(f"    MAU: {_fmt_dau(s.get('dau_mon'))} (MoM {_fmt_arrow(s.get('dau_mon_wow'))} | 전전월 {_fmt_dau(s.get('dau_mon_prev'))})")
            if s.get('deep_mon') is not None:
                lines.append(f"    깊은청취율: {_fmt_pct(s.get('deep_mon'))} | 실청취율: {_fmt_pct(s.get('real_mon'))} (전월 {_fmt_pct(s.get('real_mon_prev'))}) | 참여율: {_fmt_pct(s.get('engage_mon'))}")
            if s.get('churn_mon') is not None:
                lines.append(f"    이탈율: {_fmt_pct(s.get('churn_mon'))} | 복귀율: {_fmt_pct(s.get('react_rate_mon'))} (Δ {_fmt_arrow(s.get('react_rate_mon_diff'))} pp) | 습관: {_fmt_pct(s.get('habit_mon'))}")
            if s.get('new_mon') or s.get('react_mon'):
                lines.append(f"    신규: {_fmt_dau(s.get('new_mon'))} ({_fmt_pct(s.get('new_mon_share'))}) WoW {_fmt_arrow(s.get('new_mon_wow'))} | 전월 {_fmt_dau(s.get('new_mon_prev'))}")
                lines.append(f"    복귀: {_fmt_dau(s.get('react_mon'))} WoW {_fmt_arrow(s.get('react_mon_wow'))}")

        if s.get('guestname'):
            lines.append(f"  [편성 메타]")
            lines.append(f"    오늘 게스트: {s['guestname']}")

        lines.append('')

    if 'trend' in data:
        t = data['trend']
        lines.append(f"[최근 {t['days']}일 시계열 — {t['metric_field']}]")
        for date, value in t['data']:
            dow = _weekday_ko(date)
            prefix = f"  {date}({dow})" if dow else f"  {date}"
            if value is not None:
                val_str = _fmt_pct(value) if 'rate' in t['metric_field'] else _fmt_dau(value)
                lines.append(f"{prefix}: {val_str}")
            else:
                lines.append(f"{prefix}: 데이터 없음")
        lines.append('')

    if data.get('ranking'):
        lines.append("[최신일 프로그램 순위 TOP 10 (DAU 기준)]")
        for i, r in enumerate(data['ranking'], 1):
            lines.append(
                f"  {i}위 {r['name']}: {_fmt_dau(r['dau'])} | "
                f"깊은청취 {_fmt_pct(r['deep_rate'])} | WoW {_fmt_arrow(r['dau_wow'])}"
            )
        lines.append('')

    if data.get('compare'):
        lines.append("[채널별 비교]")
        for c in data['compare']:
            lines.append(
                f"  {c['name']}: DAU {_fmt_dau(c['dau'])} ({_fmt_arrow(c['dau_wow'])}) | "
                f"깊은청취 {_fmt_pct(c['deep_rate'])}"
            )
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

    if data.get('report'):
        r = data['report']
        s1 = r.get('s1', {}); s2 = r.get('s2', {}); s3 = r.get('s3', {})
        s4 = r.get('s4', {}); s5 = r.get('s5', {}); s6 = r.get('s6', {}); s7 = r.get('s7', {})
        lines.append(f"[종합 리포트 — {data['date_max']}]")
        if s1:
            lines.append(f"  규모:   DAU {s1.get('dau',0):,}명 (WoW {s1.get('dau_wow') or 0:+.1f}%) | WAU {s1.get('wau') or 0:,} | MAU {s1.get('mau') or 0:,}")
            lines.append(f"          신규 {s1.get('new_user',0):,}명 ({s1.get('new_pct') or 0:.1f}%) | 복귀 {s1.get('react_user',0):,}명")
        if s2:
            lines.append(f"  퍼널:   D1유지율 {_fmt_pct(s2.get('d1_ret'))} | D7 {_fmt_pct(s2.get('d7_ret'))} | 이탈률 {_fmt_pct(s2.get('churn_rate'))} ({_fmt_arrow(s2.get('churn_diff'))} pp)")
        if s3:
            lines.append(f"  품질:   깊은청취 {_fmt_pct(s3.get('deep_rate'))} | 실청취율 {_fmt_pct(s3.get('real_rate'))} | 참여율 {_fmt_pct(s3.get('engage_rate'))}")
        if s4:
            lines.append(f"  성장:   습관형성률 {_fmt_pct(s4.get('habit_rate'))} | 복귀율 {_fmt_pct(s4.get('react_rate'))}")
        if s6 and s6.get('channels'):
            ch_str = ' | '.join(f"{c['name']} {c.get('dau',0):,}" for c in s6['channels'] if c.get('code') in ('F00','L00','G00','P00'))
            if ch_str: lines.append(f"  채널:   {ch_str}")
        if s5 and s5.get('dau_top10'):
            top5 = ' / '.join(f"{p['name']} {p.get('dau',0):,}" for p in s5['dau_top10'][:5])
            lines.append(f"  TOP5:   {top5}")
        if s7 and s7.get('alerts'):
            alert_str = ' | '.join(a.get('msg','') for a in s7['alerts'][:3])
            lines.append(f"  알림:   {alert_str}")
        if s5 and s5.get('risk_list'):
            risk_str = ' | '.join(f"{r.get('name','?')} WoW{r.get('dau_wow',0):+.1f}%" for r in s5['risk_list'][:3])
            lines.append(f"  위험:   {risk_str}")
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
        timeline = BE._load_timeline(lambda spl: rows)
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


# intent별 max_tokens 상한
_INTENT_TOKENS = {
    'snapshot': 500, 'trend': 600, 'compare': 600,
    'ranking': 500, 'health': 600,
    'funnel': 800, 'engagement': 700, 'growth': 700,
    'anomaly': 500, 'report': 1400, 'general': 600,
}


def _answer(question, timeline, target_date, verbose, briefing_data=None):
    if verbose:
        print("  [2/3] 의도 분류 중...", flush=True)
    intent = classify_intent(question)
    if target_date:
        intent['specific_date'] = target_date
        intent['date_type'] = 'specific'
    if verbose:
        print(f"  -> {intent.get('summary')}: intent={intent.get('intent')}, "
              f"scope={intent.get('scope')}, days={intent.get('days')}", flush=True)

    data = extract_data(timeline, intent, briefing_data=briefing_data)
    if 'error' in data:
        return {"answer": f"데이터 추출 실패: {data['error']}", "chart_data": None}

    if verbose:
        print("  [3/3] 답변 생성 중...", flush=True)
    context = format_for_claude(data, intent, question)
    context = build_query_context(question, context, intent=intent, data=data)
    max_tokens = _INTENT_TOKENS.get(intent.get('intent', 'general'), 600)
    answer_text = call_claude(QUERY_SYSTEM_PROMPT, context, max_tokens=max_tokens)
    chart_data = build_chart_data(data, intent, question)
    return {"answer": answer_text, "chart_data": chart_data}


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
