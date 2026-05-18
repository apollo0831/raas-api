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
    'dau_today':   ('DAU',       '명'),
    'new_today':   ('신규 유입',  '명'),
    'react_today': ('복귀 사용자','명'),
    'deep_rate':   ('깊은청취율', '%'),
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
  "intent": "snapshot|trend|compare|ranking|health|general",
  "scope": "T00|F00|L00|G00|P00 또는 PGM_CODE 또는 null",
  "scope_keyword": "사용자가 언급한 채널/프로그램 키워드 또는 null",
  "metric": "dau|deep|new|react|churn|engage|habit|all",
  "date_type": "yesterday|today|specific|range",
  "specific_date": "YYYY/MM/DD 또는 null",
  "days": 7,
  "summary": "질문 한 줄 요약"
}

intent 정의:
- snapshot: 특정 날짜 단일 시점 (어제 DAU, 어제 가장 많이 들은 프로그램)
- trend: 추세 (최근 N일 흐름, 오르고 있는 프로그램)
- compare: 비교 (파워FM vs 러브FM, 어제 vs 그저께)
- ranking: 순위 (TOP N)
- health: 전체 건강도/상태 (잘 되고 있어?, 위험한 프로그램?)
- general: 기타

scope 결정:
- 채널 언급 시: F00/L00/G00/P00
- "전체" 또는 미언급 시: T00
- 프로그램 언급 시: scope_keyword에 키워드 입력

metric: dau/deep/new/react/churn/engage/habit/all (명시 없으면 all)
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


def extract_data(timeline, intent: dict) -> dict:
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
        data['snapshot'] = {
            'date': latest_date,
            'dau':         BE._i(row.get('dau_today')),
            'dau_wow':     BE._fn(row.get('dau_wow')),
            'deep_rate':   BE._fn(row.get('deep_rate')),
            'engage_rate': BE._fn(row.get('engage_rate')),
            'habit_rate':  BE._fn(row.get('habit_rate')),
            'churn_rate':  BE._fn(row.get('churn_rate')),
            'new_user':    BE._i(row.get('new_today')),
            'react_user':  BE._i(row.get('react_today')),
            'react_rate':  BE._fn(row.get('react_rate')),
        }

    # trend
    if intent_type in ('trend', 'general', 'health'):
        field_map = {
            'dau': 'dau_today', 'deep': 'deep_rate', 'new': 'new_today',
            'react': 'react_today', 'churn': 'churn_rate',
            'engage': 'engage_rate', 'habit': 'habit_rate', 'all': 'dau_today'
        }
        mf = field_map.get(metric, 'dau_today')
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
            dau = BE._i(row.get('dau_today'))
            if not dau or dau <= 0:
                continue
            rows.append({
                'code': code, 'name': BE._pgm_name(code, row=row), 'dau': dau,
                'deep_rate': BE._fn(row.get('deep_rate')),
                'churn_rate': BE._fn(row.get('churn_rate')),
                'dau_wow': BE._fn(row.get('dau_wow')),
            })
        rows.sort(key=lambda x: x['dau'], reverse=True)
        data['ranking'] = rows[:10]

    # compare
    if intent_type == 'compare':
        latest_date = available_dates[-1]
        ch_rows = []
        for ch in ('F00', 'L00', 'G00', 'P00'):
            row = timeline.get(ch, {}).get(latest_date, {})
            if not row:
                continue
            ch_rows.append({
                'code': ch, 'name': BE._pgm_name(ch, row=row),
                'dau': BE._i(row.get('dau_today')),
                'dau_wow': BE._fn(row.get('dau_wow')),
                'deep_rate': BE._fn(row.get('deep_rate')),
                'churn_rate': BE._fn(row.get('churn_rate')),
            })
        data['compare'] = ch_rows

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
                    'dau':         BE._i(row.get('dau_today')),
                    'churn_rate':  BE._fn(row.get('churn_rate')),
                    'dau_chg':     BE._fn(row.get('dau_wow')),
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
                        'dau':        BE._i(row.get('dau_today')),
                        'churn_rate': BE._fn(row.get('churn_rate')),
                        'dau_wow':    BE._fn(row.get('dau_wow')),
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
                wow   = BE._fn(row.get('dau_wow'))
                dau   = BE._i(row.get('dau_today'))
                if churn and wow and dau and dau >= 1000 and churn >= 30 and wow <= -5:
                    risks.append({
                        'code': code, 'name': BE._pgm_name(code, row=row),
                        'dau': dau, 'churn_rate': churn, 'dau_wow': wow,
                    })
        risks.sort(key=lambda x: x.get('dau_wow') or 0)
        data['risks'] = risks[:5]

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
        lines.append(f"  DAU: {_fmt_dau(s['dau'])} ({_fmt_arrow(s['dau_wow'])} WoW)")
        if s['deep_rate']   is not None: lines.append(f"  깊은청취율: {_fmt_pct(s['deep_rate'])}")
        if s['engage_rate'] is not None: lines.append(f"  참여율: {_fmt_pct(s['engage_rate'])}")
        if s['habit_rate']  is not None: lines.append(f"  습관형성률: {_fmt_pct(s['habit_rate'])}")
        if s['churn_rate']  is not None: lines.append(f"  이탈률: {_fmt_pct(s['churn_rate'])}")
        if s['new_user']:  lines.append(f"  신규 유입: {_fmt_dau(s['new_user'])}")
        if s['react_user']: lines.append(f"  복귀: {_fmt_dau(s['react_user'])} (복귀율 {_fmt_pct(s['react_rate'])})")
        lines.append('')

    if 'trend' in data:
        t = data['trend']
        lines.append(f"[최근 {t['days']}일 시계열 — {t['metric_field']}]")
        for date, value in t['data']:
            if value is not None:
                val_str = _fmt_pct(value) if 'rate' in t['metric_field'] else _fmt_dau(value)
                lines.append(f"  {date}: {val_str}")
            else:
                lines.append(f"  {date}: 데이터 없음")
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

    if data.get('risks'):
        lines.append("[이탈 위험 프로그램]")
        for r in data['risks']:
            lines.append(
                f"  {r['name']}: 이탈률 {_fmt_pct(r['churn_rate'])} | "
                f"DAU {_fmt_arrow(r['dau_wow'])} | DAU {_fmt_dau(r['dau'])}"
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
        timeline = BE._load_timeline(lambda spl: rows)
    except Exception as e:
        return {"answer": f"데이터 로드 실패: {e}", "chart_data": None}

    return _answer(question, timeline, target_date, verbose)


def query_with_timeline(question: str, timeline: dict, target_date: str = None) -> dict:
    """서버 캐시 timeline을 받아 처리. {"answer": str, "chart_data": dict|None} 리턴."""
    if not timeline:
        return {"answer": "데이터를 사용할 수 없습니다.", "chart_data": None}
    return _answer(question, timeline, target_date, verbose=False)


def _answer(question, timeline, target_date, verbose):
    if verbose:
        print("  [2/3] 의도 분류 중...", flush=True)
    intent = classify_intent(question)
    if target_date:
        intent['specific_date'] = target_date
        intent['date_type'] = 'specific'
    if verbose:
        print(f"  -> {intent.get('summary')}: intent={intent.get('intent')}, "
              f"scope={intent.get('scope')}, days={intent.get('days')}", flush=True)

    data = extract_data(timeline, intent)
    if 'error' in data:
        return {"answer": f"데이터 추출 실패: {data['error']}", "chart_data": None}

    if verbose:
        print("  [3/3] 답변 생성 중...", flush=True)
    context = format_for_claude(data, intent, question)
    context = build_query_context(question, context)
    answer_text = call_claude(QUERY_SYSTEM_PROMPT, context, max_tokens=600)
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
