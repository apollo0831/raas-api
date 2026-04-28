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
        if intent.get('scope_keyword') and not intent.get('scope'):
            kw = intent['scope_keyword'].lower().strip()
            for key, code in SCOPE_MAP.items():
                if key.lower() in kw:
                    intent['scope'] = code
                    break
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
        'scope_name': BE.PGM_NAMES.get(scope, scope),
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
                'code': code, 'name': BE.PGM_NAMES.get(code, code), 'dau': dau,
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
                'code': ch, 'name': BE.PGM_NAMES.get(ch, ch),
                'dau': BE._i(row.get('dau_today')),
                'dau_wow': BE._fn(row.get('dau_wow')),
                'deep_rate': BE._fn(row.get('deep_rate')),
                'churn_rate': BE._fn(row.get('churn_rate')),
            })
        data['compare'] = ch_rows

    # health — 위험 프로그램
    if intent_type == 'health':
        latest_date = available_dates[-1]
        exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
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
                    'code': code, 'name': BE.PGM_NAMES.get(code, code),
                    'dau': dau, 'churn_rate': churn, 'dau_wow': wow,
                })
        risks.sort(key=lambda x: x['dau_wow'])
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


# ── 답변 생성 system prompt ────────────────────────────────
ANSWER_SYSTEM = """SBS 고릴라 라디오 앱 데이터 분석 어시스턴트입니다.
주어진 데이터만을 근거로 정확하게 답변하세요.

규칙:
- 한국어로 답변
- 수치는 천단위 쉼표 (예: 189,021명)
- 비율은 소수점 1자리 (예: 74.5%)
- 증감은 화살표 (▲+5.2% / ▼-3.1%)
- 300자 이내 간결하게
- 데이터에 없는 내용은 절대 추측 금지
- "추정", "예상", "아마도" 사용 금지
- 데이터 범위 밖 질문: "해당 데이터가 없습니다" 명시

데이터 정의:
- DAU: 청취시간>0 고유 사용자 수
- 깊은청취율: 10분이상/1분이상 비율
- WoW: 전주 동일 요일 대비 증감률
- 참여율: 메뉴 클릭/공유 등 적극 행동 비율
- 습관형성률: 일정 빈도 이상 청취 비율
- 복귀율: 휴면 사용자 중 복귀 비율
- 이탈률: 일정 기간 미접속 비율"""


# ── 메인 질의 함수 ─────────────────────────────────────────
def query(question: str, target_date: str = None, verbose: bool = False) -> str:
    """자연어 질의 — timeline 자체 로드"""
    if verbose:
        print("  [1/3] timeline 로드 중...", flush=True)

    local_path = os.path.join(os.path.dirname(__file__), 'raas_kpi_latest.csv')
    if not os.path.exists(local_path):
        return "데이터 파일(raas_kpi_latest.csv)을 찾을 수 없습니다."
    try:
        import pandas as pd
        df = pd.read_csv(local_path, dtype=str, keep_default_na=False)
        rows = df.to_dict(orient='records')
        timeline = BE._load_timeline(lambda spl: rows)
    except Exception as e:
        return f"데이터 로드 실패: {e}"

    return _answer(question, timeline, target_date, verbose)


def query_with_timeline(question: str, timeline: dict, target_date: str = None) -> str:
    """서버 캐시 timeline을 받아 처리 (성능 최적화)."""
    if not timeline:
        return "데이터를 사용할 수 없습니다."
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
        return f"데이터 추출 실패: {data['error']}"

    if verbose:
        print("  [3/3] 답변 생성 중...", flush=True)
    context = format_for_claude(data, intent, question)
    return call_claude(ANSWER_SYSTEM, context, max_tokens=600)


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
            print(query(q, verbose=True))
    elif args.question:
        print(query(args.question, verbose=args.verbose))
    else:
        parser.print_help()
