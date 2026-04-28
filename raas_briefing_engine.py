"""
RAAS Briefing Engine v2.2
raas_kpi_latest.csv 단일 룩업 — 일간 + 주간 + 월간 통합
"""
import os
import pandas as pd
from datetime import datetime

def _f(v,d=0.0):
    try: return float(v) if v not in (None,'','None','null') else d
    except: return d
def _i(v,d=0):
    try: return int(float(v)) if v not in (None,'','None','null') else d
    except: return d
def _fn(v):
    try: return float(v) if v not in (None,'','None','null') else None
    except: return None

PGM_NAMES={
    'T00':'전체','F00':'파워FM','L00':'러브FM','G00':'고릴라M','P00':'픽채널',
    'F01':'뮤직하이','F02':'애프터클럽','F03':'팝스테이션','F04':'펀펀투데이',
    'F05':'김영철의파워FM','F06':'아름다운이아침봉태규','F07':'씨네타운',
    'F08':'12시엔주현영','F09':'컬투쇼','F10':'황제파워','F11':'러브게임',
    'F12':'영스트리트','F13':'배텐',
    'L01':'YESTERDAY','L02':'LOVE20','L03':'OLDIES20',
    'L05':'뉴스브리핑','L06':'정치쇼','L07':'이숙영의러브FM',
    'L08':'목돈연구소','L09':'배고픈라디오','L10':'정엽입니다',
    'L11':'인생은오디션','L12':'저녁바람','L13':'뉴스직격',
    'L14':'뮤직투나잇','L15':'음악이흐르는밤',
    'M05':'책하고놀자','M07':'드라이브뮤직오전','M10':'최영주의러브FM','M11':'드라이브뮤직오후',
}
PGM_F=['F01','F02','F03','F04','F05','F06','F07','F08','F09','F10','F11','F12','F13']
PGM_L=['L01','L02','L03','L04','L05','L06','L07','L08','L09','L10','L11',
        'L12','L13','L14','L15','M05','M07','M10','M11']
CH=['F00','L00','G00','P00']
ALL=PGM_F+PGM_L

def _load_timeline(search):
    """raas_kpi_latest.csv를 timeline 구조로 로드.
    반환: {PGM_CODE: {DATE: row, ...}, ...}
    같은 PGM_CODE의 여러 날짜 행을 모두 보존.
    """
    try:
        rows = search("| inputlookup raas_kpi_latest.csv")
    except Exception as e:
        local_path = os.path.join(os.path.dirname(__file__), 'raas_kpi_latest.csv')
        if os.path.exists(local_path):
            print(f"  [fallback] Splunk {type(e).__name__} - local CSV: {local_path}")
            df = pd.read_csv(local_path, dtype=str, keep_default_na=False)
            rows = df.to_dict(orient='records')
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
        print(f"  [timeline] {len(timeline)} codes x {len(all_dates)} dates ({min(all_dates)} ~ {max(all_dates)})")
    return timeline

def _latest_snapshot(timeline):
    """timeline에서 각 PGM_CODE의 최신 날짜 행만 추출.
    반환: {PGM_CODE: row} (기존 _load 결과와 동일 구조)
    """
    snapshot = {}
    for code, date_rows in timeline.items():
        if not date_rows:
            continue
        snapshot[code] = date_rows[max(date_rows.keys())]
    return snapshot

def _load(search):
    """[호환성 유지용] _load_timeline + _latest_snapshot 조합."""
    return _latest_snapshot(_load_timeline(search))


def build_s1(kpi):
    t=kpi.get('T00',{})
    if not t: return {}
    return {
        'dau':        _i(t.get('dau_today')),
        'dau_wow':    _fn(t.get('dau_wow')),
        'wau':        _i(t.get('wau_today')) or None,
        'mau':        _i(t.get('mau_today')) or None,
        'new_user':   _i(t.get('new_today')),
        'react_user': _i(t.get('react_today')),
        'new_pct':    _fn(t.get('new_pct')),
        'react_pct':  _fn(t.get('react_pct')),
        # 주간
        'dau_week':     _i(t.get('dau_week')) or None,
        'dau_week_wow': _fn(t.get('dau_week_wow')),
        # 월간
        'dau_mon':      _i(t.get('dau_mon')) or None,
        'dau_mon_wow':  _fn(t.get('dau_mon_wow')),
    }

def build_s2(kpi):
    t=kpi.get('T00',{})
    if not t: return {}
    return {
        # 일간
        'dau':             _i(t.get('dau_today')),
        'dau_yday':        _i(t.get('dau_yday')),
        'new_user':        _i(t.get('new_today')),
        'new_wow':         _fn(t.get('new_wow')),
        'react_user':      _i(t.get('react_today')),
        'react_wow':       _fn(t.get('react_wow')),
        'churn_rate':      _fn(t.get('churn_rate')),
        'churn_diff':      _fn(t.get('churn_diff')),
        'react_rate':      _fn(t.get('react_rate')),
        'react_rate_diff': _fn(t.get('react_rate_diff')),
        'd1_ret':          _fn(t.get('d1_ret')),
        'd7_ret':          _fn(t.get('d7_ret')),
        'new_d1_ret':      _fn(t.get('new_d1_ret')),
        'new_d1_ret_pw':   _fn(t.get('new_d1_ret_pw')),
        'new_d1_diff':     _fn(t.get('new_d1_diff')),
        'new_d7_ret':      _fn(t.get('new_d7_ret')),
        'new_d7_ret_pw':   _fn(t.get('new_d7_ret_pw')),
        'new_d7_diff':     _fn(t.get('new_d7_diff')),
        'new_w1_ret':      _fn(t.get('new_w1_ret')),
        'new_w1_ret_pw':   _fn(t.get('new_w1_ret_pw')),
        'new_w1_diff':     _fn(t.get('new_w1_diff')),
        'new_m1_ret':      _fn(t.get('new_m1_ret')),
        'new_m1_ret_pw':   _fn(t.get('new_m1_ret_pw')),
        'new_m1_diff':     _fn(t.get('new_m1_diff')),
        'd1_diff':         _fn(t.get('d1_diff')),
        'd7_diff':         _fn(t.get('d7_diff')),
        'new_pct':         _fn(t.get('new_pct')),
        'react_pct':       _fn(t.get('react_pct')),
        # 주간
        'dau_week':          _i(t.get('dau_week')) or None,
        'new_week':          _i(t.get('new_week')) or None,
        'new_week_wow':      _fn(t.get('new_week_wow')),
        'react_week':        _i(t.get('react_week')) or None,
        'churn_week':        _fn(t.get('churn_week')),
        'churn_week_diff':   _fn(t.get('churn_week_diff')),
        'react_rate_week':   _fn(t.get('react_rate_week')),
        'w1_ret':            _fn(t.get('w1_ret')),
        'w1_diff':           _fn(t.get('w1_diff')),
        # 월간
        'dau_mon':           _i(t.get('dau_mon')) or None,
        'new_mon':           _i(t.get('new_mon')) or None,
        'new_mon_wow':       _fn(t.get('new_mon_wow')),
        'react_mon':         _i(t.get('react_mon')) or None,
        'churn_mon':         _fn(t.get('churn_mon')),
        'churn_mon_diff':    _fn(t.get('churn_mon_diff')),
        'react_rate_mon':    _fn(t.get('react_rate_mon')),
        'm1_ret':            _fn(t.get('m1_ret')),
        'm1_diff':           _fn(t.get('m1_diff')),
        'dau_week_avg':      _fn(t.get('dau_week_avg')),
        'dau_mon_avg':       _fn(t.get('dau_mon_avg')),
        'wau_mon_avg':       _fn(t.get('wau_mon_avg')),
    }

def build_s3(kpi):
    t=kpi.get('T00',{})
    if not t: return {}
    ch_deep={}
    for c in CH:
        row=kpi.get(c,{})
        v1w,v10w=_fn(row.get('wau_1min')),_fn(row.get('wau_10min'))
        v1m,v10m=_fn(row.get('mau_1min')),_fn(row.get('mau_10min'))
        ch_deep[c]={
            'name':      PGM_NAMES.get(c,c),
            'rate':      _fn(row.get('deep_rate')),
            'rate_week': round(v10w/v1w*100,2) if v1w and v10w and v1w>0 else None,
            'rate_mon':  round(v10m/v1m*100,2) if v1m and v10m and v1m>0 else None,
        }
    return {
        'dau':              _i(t.get('dau_today')),
        'dau_1min':         _i(t.get('dau_1min')),
        'dau_10min':        _i(t.get('dau_10min')),
        'deep_rate':        _fn(t.get('deep_rate')),
        'deep_rate_diff':   _fn(t.get('deep_rate_diff')),
        'engage_rate':      _fn(t.get('engage_rate')),
        'engage_diff':      _fn(t.get('engage_diff')),
        'channel_deep':     ch_deep,
        # 주간
        'wau_1min':             _fn(t.get('wau_1min')),
        'wau_10min':            _fn(t.get('wau_10min')),
        'deep_rate_week':       _fn(t.get('deep_rate_week')),
        'deep_rate_week_diff':  _fn(t.get('deep_rate_week_diff')),
        'engage_week':          _fn(t.get('engage_week')),
        'engage_week_diff':     _fn(t.get('engage_week_diff')),
        # 월간
        'mau_1min':             _fn(t.get('mau_1min')),
        'mau_10min':            _fn(t.get('mau_10min')),
        'deep_rate_mon':        _fn(t.get('deep_rate_mon')),
        'deep_rate_mon_diff':   _fn(t.get('deep_rate_mon_diff')),
        'engage_mon':           _fn(t.get('engage_mon')),
        'engage_mon_diff':      _fn(t.get('engage_mon_diff')),
    }

def build_s4(kpi):
    t=kpi.get('T00',{})
    if not t: return {}
    # 프로그램별 습관형성률 TOP3 (신규 500명 이상)
    hl=[]
    for c in ALL:
        row=kpi.get(c,{}); h=_fn(row.get('habit_rate')); n=_f(row.get('new_today'),0)
        if h is not None and n>=500: hl.append({'code':c,'name':PGM_NAMES.get(c,c),'rate':h,'count':int(n)})
    top3=sorted(hl,key=lambda x:x['rate'],reverse=True)[:3]
    return {
        'new_today':   _i(t.get('new_today')),
        'new_pct':     _fn(t.get('new_pct')),
        'habit_rate':  _fn(t.get('habit_rate')),
        'habit_diff':  _fn(t.get('habit_diff')),
        'd1_ret':      _fn(t.get('d1_ret')),
        'd7_ret':      _fn(t.get('d7_ret')),
        'top3_habit':  top3,
        # 주간
        'habit_week':      _fn(t.get('habit_week')),
        'habit_week_diff': _fn(t.get('habit_week_diff')),
        # 월간
        'habit_mon':       _fn(t.get('habit_mon')),
        'habit_mon_diff':  _fn(t.get('habit_mon_diff')),
        # 복귀율
        'react_rate':          _fn(t.get('react_rate')),
        'react_rate_week':     _fn(t.get('react_rate_week')),
        'react_rate_mon':      _fn(t.get('react_rate_mon')),
        'react_rate_diff':     _fn(t.get('react_rate_diff')),
        'react_rate_week_diff':_fn(t.get('react_rate_week_diff')),
        'react_rate_mon_diff': _fn(t.get('react_rate_mon_diff')),
    }

def _pgm_dict(code, row, rank=None, name=None, channel=None, dau=None):
    ch_name = channel or ('파워FM' if code.startswith('F') else '러브FM')
    return {
        'rank':    rank,
        'code':    code,
        'name':    name or PGM_NAMES.get(code, row.get('pgm_name', code)),
        'channel': ch_name,
        'dau':     dau if dau is not None else _i(row.get('dau_today')),
        # 기간별 규모
        'dau_week':       _i(row.get('dau_week')),
        'dau_mon':        _i(row.get('dau_mon')),
        'dau_1min':       _i(row.get('dau_1min')),
        'dau_10min':      _i(row.get('dau_10min')),
        'wau_1min':       _i(row.get('wau_1min')),
        'wau_10min':      _i(row.get('wau_10min')),
        'mau_1min':       _i(row.get('mau_1min')),
        'mau_10min':      _i(row.get('mau_10min')),
        # 품질
        'deep_rate':      _fn(row.get('deep_rate')),
        'deep_rate_week': _fn(row.get('deep_rate_week')),
        'deep_rate_mon':  _fn(row.get('deep_rate_mon')),
        'engage_rate':    _fn(row.get('engage_rate')),
        'engage_week':    _fn(row.get('engage_week')),
        'engage_mon':     _fn(row.get('engage_mon')),
        'habit_rate':     _fn(row.get('habit_rate')),
        'habit_week':     _fn(row.get('habit_week')),
        'habit_mon':      _fn(row.get('habit_mon')),
        # 유지율
        'd1_ret':         _fn(row.get('d1_ret')),
        'd7_ret':         _fn(row.get('d7_ret')),
        'w1_ret':         _fn(row.get('w1_ret')),
        # 신규/이탈/복귀
        'new_user':       _i(row.get('new_today')),
        'new_pct':        _fn(row.get('new_pct')),
        'new_week':       _i(row.get('new_week')),
        'new_mon':        _i(row.get('new_mon')),
        'new_week_pct':   _fn(row.get('new_week_pct')),
        'new_mon_pct':    _fn(row.get('new_mon_pct')),
        'churn_rate':     _fn(row.get('churn_rate')),
        'churn_week':     _fn(row.get('churn_week')),
        'churn_mon':      _fn(row.get('churn_mon')),
        'react_user':     _i(row.get('react_today')),
        'react_week':     _i(row.get('react_week')),
        'react_mon':      _i(row.get('react_mon')),
        'react_rate':     _fn(row.get('react_rate')),
        'react_rate_week':_fn(row.get('react_rate_week')),
        'react_rate_mon': _fn(row.get('react_rate_mon')),
        'pgm_name':       row.get('pgm_name',''),
        # 전주 대비 변동
        'dau_pw':              _fn(row.get('dau_pw')),
        'dau_wow':             _fn(row.get('dau_wow')),
        'dau_week_pw':         _fn(row.get('dau_week_pw')),
        'dau_week_wow':        _fn(row.get('dau_week_wow')),
        'dau_mon_pw':          _fn(row.get('dau_mon_pw')),
        'dau_mon_wow':         _fn(row.get('dau_mon_wow')),
        'new_pw':              _fn(row.get('new_pw')),
        'new_wow':             _fn(row.get('new_wow')),
        'new_week_pw':         _fn(row.get('new_week_pw')),
        'new_week_wow':        _fn(row.get('new_week_wow')),
        'new_mon_pw':          _fn(row.get('new_mon_pw')),
        'new_mon_wow':         _fn(row.get('new_mon_wow')),
        'react_pw':            _fn(row.get('react_pw')),
        'react_wow':           _fn(row.get('react_wow')),
        'react_week_pw':       _fn(row.get('react_week_pw')),
        'react_week_wow':      _fn(row.get('react_week_wow')),
        'churn_diff':          _fn(row.get('churn_diff')),
        'churn_week_diff':     _fn(row.get('churn_week_diff')),
        'churn_mon_diff':      _fn(row.get('churn_mon_diff')),
        'deep_rate_diff':      _fn(row.get('deep_rate_diff')),
        'deep_rate_week_diff': _fn(row.get('deep_rate_week_diff')),
        'deep_rate_mon_diff':  _fn(row.get('deep_rate_mon_diff')),
        'engage_diff':         _fn(row.get('engage_diff')),
        'engage_week_diff':    _fn(row.get('engage_week_diff')),
        'engage_mon_diff':     _fn(row.get('engage_mon_diff')),
        'habit_diff':          _fn(row.get('habit_diff')),
        'habit_week_diff':     _fn(row.get('habit_week_diff')),
        'habit_mon_diff':      _fn(row.get('habit_mon_diff')),
        'd1_diff':             _fn(row.get('d1_diff')),
        'd7_diff':             _fn(row.get('d7_diff')),
        'w1_diff':             _fn(row.get('w1_diff')),
        'm1_diff':             _fn(row.get('m1_diff')),
        'guestname':           (row.get('guestname') or '').strip(),
    }

def build_s5(kpi):
    all_pgm_codes = sorted(
        [k for k in kpi.keys()
         if (k.startswith('F') or k.startswith('L') or k.startswith('M'))
         and k not in ('F00','L00') and len(k)==3
         and k != 'L04'],
        key=lambda c: _f(kpi[c].get('dau_today'),0), reverse=True
    )
    dau_top10    = [_pgm_dict(c, kpi[c], rank=i+1) for i,c in enumerate(all_pgm_codes[:10])]
    all_programs = [_pgm_dict(c, kpi[c]) for c in all_pgm_codes]
    dl,nl,rl=[],[],[]
    for c in ALL:
        row=kpi.get(c,{}); nm=PGM_NAMES.get(c,c)
        dr=_fn(row.get('deep_rate')); u1=_f(row.get('dau_1min'),0)
        n=_f(row.get('new_today'),0); rc=_f(row.get('react_today'),0)
        if dr is not None and u1>=500: dl.append({'code':c,'name':nm,'rate':dr,'u1min':int(u1)})
        if n>0: nl.append({'code':c,'name':nm,'count':int(n)})
        if rc>0: rl.append({'code':c,'name':nm,'count':int(rc)})
    risk=[]
    for c in ALL:
        row=kpi.get(c,{}); ch=_fn(row.get('churn_rate')); w=_fn(row.get('dau_wow')); d=_f(row.get('dau_today'),0)
        if ch and w and d>=1000 and ch>=30 and w<=-5:
            risk.append({'code':c,'name':PGM_NAMES.get(c,c),'dau':int(d),'churn_rate':ch,'dau_wow':w})
    return {
        'dau_top10':    dau_top10,
        'all_programs': all_programs,
        'deep_top5':    sorted(dl,key=lambda x:x['rate'],reverse=True)[:5],
        'new_top5':     sorted(nl,key=lambda x:x['count'],reverse=True)[:5],
        'react_top5':   sorted(rl,key=lambda x:x['count'],reverse=True)[:5],
        'risk_list':    sorted(risk,key=lambda x:x['dau_wow'])[:3],
    }

def build_s6(kpi):
    t00=_f(kpi.get('T00',{}).get('dau_today'),1) or 1
    chs=[]
    for c in CH:
        row=kpi.get(c,{}); d=_f(row.get('dau_today'),0)
        # deep_rate 폴백 계산 (1min/10min 직접 비율 우선, 없으면 CSV 값)
        dau_1min  = _i(row.get('dau_1min'))
        dau_10min = _i(row.get('dau_10min'))
        deep_rate = round(dau_10min/dau_1min*100, 1) if dau_1min and dau_1min > 0 else _fn(row.get('deep_rate'))
        wau_1min  = _i(row.get('wau_1min'))
        wau_10min = _i(row.get('wau_10min'))
        deep_rate_week = round(wau_10min/wau_1min*100, 1) if wau_1min and wau_1min > 0 else _fn(row.get('deep_rate_week'))
        mau_1min  = _i(row.get('mau_1min'))
        mau_10min = _i(row.get('mau_10min'))
        deep_rate_mon = round(mau_10min/mau_1min*100, 1) if mau_1min and mau_1min > 0 else _fn(row.get('deep_rate_mon'))
        chs.append({
            'code':c,'name':PGM_NAMES[c],
            'share':round(d/t00*100,1),
            # 일간
            'dau':         int(d),
            'deep_rate':   deep_rate,
            'churn_rate':  _fn(row.get('churn_rate')),
            'react_rate':  _fn(row.get('react_rate')),
            'new_user':    _i(row.get('new_today')),
            'new_pct':     _fn(row.get('new_pct')),
            # 주간
            'dau_week':        _i(row.get('dau_week')),
            'deep_rate_week':  deep_rate_week,
            'churn_week':      _fn(row.get('churn_week')),
            'react_rate_week': _fn(row.get('react_rate_week')),
            'new_week':        _i(row.get('new_week')),
            'new_week_pct':    _fn(row.get('new_week_pct')),
            'wau_1min':        wau_1min,
            'wau_10min':       wau_10min,
            # 월간
            'dau_mon':        _i(row.get('dau_mon')),
            'deep_rate_mon':  deep_rate_mon,
            'churn_mon':      _fn(row.get('churn_mon')),
            'react_rate_mon': _fn(row.get('react_rate_mon')),
            'new_mon':        _i(row.get('new_mon')),
            'new_mon_pct':    _fn(row.get('new_mon_pct')),
            'mau_1min':       mau_1min,
            'mau_10min':      mau_10min,
            # 추가 지표
            'dau_1min':    _i(row.get('dau_1min')),
            'dau_10min':   _i(row.get('dau_10min')),
            'engage_rate': _fn(row.get('engage_rate')),
            'engage_week': _fn(row.get('engage_week')),
            'engage_mon':  _fn(row.get('engage_mon')),
            'habit_rate':  _fn(row.get('habit_rate')),
            'habit_week':  _fn(row.get('habit_week')),
            'habit_mon':   _fn(row.get('habit_mon')),
            'd1_ret':      _fn(row.get('d1_ret')),
            'd7_ret':      _fn(row.get('d7_ret')),
            'w1_ret':      _fn(row.get('w1_ret')),
            'm1_ret':      _fn(row.get('m1_ret')),
            'new_d1_ret':  _fn(row.get('new_d1_ret')),
            'new_d1_diff': _fn(row.get('new_d1_diff')),
            'new_d7_ret':  _fn(row.get('new_d7_ret')),
            'new_d7_diff': _fn(row.get('new_d7_diff')),
            'new_w1_ret':  _fn(row.get('new_w1_ret')),
            'new_w1_diff': _fn(row.get('new_w1_diff')),
            'new_m1_ret':  _fn(row.get('new_m1_ret')),
            'new_m1_diff': _fn(row.get('new_m1_diff')),
            'react_user':  _i(row.get('react_today')),
            'react_week':  _i(row.get('react_week')),
            'react_mon':   _i(row.get('react_mon')),
            # 전주 대비 변동
            'dau_wow':             _fn(row.get('dau_wow')),
            'dau_week_wow':        _fn(row.get('dau_week_wow')),
            'dau_mon_wow':         _fn(row.get('dau_mon_wow')),
            'new_wow':             _fn(row.get('new_wow')),
            'new_week_wow':        _fn(row.get('new_week_wow')),
            'new_mon_wow':         _fn(row.get('new_mon_wow')),
            'react_wow':           _fn(row.get('react_wow')),
            'react_week_wow':      _fn(row.get('react_week_wow')),
            'churn_diff':          _fn(row.get('churn_diff')),
            'churn_week_diff':     _fn(row.get('churn_week_diff')),
            'churn_mon_diff':      _fn(row.get('churn_mon_diff')),
            'deep_rate_diff':      _fn(row.get('deep_rate_diff')),
            'deep_rate_week_diff': _fn(row.get('deep_rate_week_diff')),
            'deep_rate_mon_diff':  _fn(row.get('deep_rate_mon_diff')),
            'engage_diff':         _fn(row.get('engage_diff')),
            'engage_week_diff':    _fn(row.get('engage_week_diff')),
            'engage_mon_diff':     _fn(row.get('engage_mon_diff')),
            'habit_diff':          _fn(row.get('habit_diff')),
            'habit_week_diff':     _fn(row.get('habit_week_diff')),
            'habit_mon_diff':      _fn(row.get('habit_mon_diff')),
            'd1_diff':             _fn(row.get('d1_diff')),
            'd7_diff':             _fn(row.get('d7_diff')),
            'w1_diff':             _fn(row.get('w1_diff')),
            'm1_diff':             _fn(row.get('m1_diff')),
        })
    return {'channels':chs}

def build_s7(s1,s2,s3,s4,s5):
    a=[]
    w=s1.get('dau_wow')
    if w is not None:
        if w<=-10: a.append({'level':'red',   'msg':f"DAU {w:+.1f}% 급락 🔴"})
        elif w>=10: a.append({'level':'green', 'msg':f"DAU {w:+.1f}% 급증 🟢"})
    dd=s3.get('deep_rate_diff')
    if dd is not None and dd<=-3: a.append({'level':'red','msg':f"깊은청취율 {dd:+.1f}pp 급락 🔴"})
    nw=s2.get('new_wow')
    if nw is not None and nw<=-20: a.append({'level':'red','msg':f"신규 {nw:+.1f}% 급감 🔴"})
    cd=s2.get('churn_diff')
    if cd is not None and cd>=3: a.append({'level':'yellow','msg':f"이탈율 {cd:+.1f}pp 상승 🟡"})
    # 주간 추가 알림
    cwd=s2.get('churn_week_diff')
    if cwd is not None and cwd>=3: a.append({'level':'yellow','msg':f"주간 이탈율 {cwd:+.1f}pp 상승 🟡"})
    rr=s2.get('react_rate')
    if rr is not None and rr>=5: a.append({'level':'green','msg':f"복귀율 {rr:.1f}% 달성 🟢"})
    hr=s4.get('habit_rate')
    if hr is not None:
        if hr>=30: a.append({'level':'green', 'msg':f"습관형성률 {hr:.1f}% 목표달성 🟢"})
        elif hr<=15: a.append({'level':'yellow','msg':f"습관형성률 {hr:.1f}% 부진 🟡"})
    for r in s5.get('risk_list',[])[:2]:
        a.append({'level':'yellow','msg':f"{r['name']} 이탈{r['churn_rate']:.1f}%·WoW{r['dau_wow']:+.1f}% 🟡"})
    if not a: a.append({'level':'green','msg':'전 지표 정상 범위 🟢'})
    return {'alerts':a}

def build_context(s1,s2,s3,s4,s5,s6,s7,timeline=None):
    L=["=== RAAS 고릴라 앱 브리핑 ===\n"]
    if s1:
        L.append(f"[일간] DAU {s1.get('dau',0):,} (WoW{s1.get('dau_wow') or 0:+.1f}%)")
        if s1.get('wau_today'): L.append(f"  WAU롤링 {s1['wau_today']:,}")
        if s1.get('dau_week'):  L.append(f"[주간] WAU {s1['dau_week']:,} (WoW{s1.get('dau_week_wow') or 0:+.1f}%)")
        if s1.get('dau_mon'):   L.append(f"[월간] MAU {s1['dau_mon']:,} (MoM{s1.get('dau_mon_wow') or 0:+.1f}%)")
        L.append(f"  신규 {s1.get('new_user',0):,}({s1.get('new_pct') or 0:.1f}%) 복귀 {s1.get('react_user',0):,}({s1.get('react_pct') or 0:.1f}%)")
    if s2:
        L.append(f"\n[퍼널-일] D1 {s2.get('d1_ret') or '—'}% D7 {s2.get('d7_ret') or '—'}% 이탈 {s2.get('churn_rate') or '—'}% 복귀율 {s2.get('react_rate') or '—'}%")
        if s2.get('w1_ret'):  L.append(f"[퍼널-주] W1유지율 {s2['w1_ret']}% 이탈 {s2.get('churn_week') or '—'}%")
        if s2.get('m1_ret'):  L.append(f"[퍼널-월] M1유지율 {s2['m1_ret']}% 이탈 {s2.get('churn_mon') or '—'}%")
    if s3:
        L.append(f"\n[품질] 깊은청취 일{s3.get('deep_rate') or '—'}% 주{s3.get('deep_rate_week') or '—'}% 월{s3.get('deep_rate_mon') or '—'}%")
        L.append(f"  참여율 일{s3.get('engage_rate') or '—'}% 주{s3.get('engage_week') or '—'}% 월{s3.get('engage_mon') or '—'}%")
    if s4:
        L.append(f"\n[성장] 습관형성률 일{s4.get('habit_rate') or '—'}% 주{s4.get('habit_week') or '—'}% 월{s4.get('habit_mon') or '—'}%")
        for p in s4.get('top3_habit',[]): L.append(f"  ↳{p['name']} {p['rate']:.1f}%")
    if s5:
        L.append("\n[TOP5]")
        for p in s5.get('dau_top10',[])[:5]: L.append(f"  {p['rank']}위 {p['name']} {p['dau']:,}")
        for r in s5.get('risk_list',[]): L.append(f"  ⚠️{r['name']} 이탈{r['churn_rate']}% WoW{r['dau_wow']:+.1f}%")
    if s6:
        L.append("\n[채널]")
        for c in s6.get('channels',[]): L.append(f"  {c['name']} {c['dau']:,} 점유{c['share'] or 0:.1f}% 깊은청취일{c['deep_rate'] or '—'}%/주{c['deep_rate_week'] or '—'}%")
    if s7:
        L.append("\n[이상징후]")
        for a in s7.get('alerts',[]): L.append(f"  {a['msg']}")
    if timeline:
        try:
            ts_text = _build_timeseries_insights(timeline, days=7)
            if ts_text:
                L.append(ts_text)
        except Exception as e:
            print(f"  [warn] timeseries insights error: {e}")
    return '\n'.join(L)

def collect_all(search_fn, return_timeline=False):
    """KPI 데이터 수집 및 섹션 빌드.

    Args:
        search_fn: Splunk 쿼리 실행 함수
        return_timeline: True면 결과에 '_timeline' 추가 (시계열 API용)
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] KPI 수집 시작")
    def safe(fn, *args, fb=None):
        try: return fn(*args)
        except Exception as e:
            print(f"  ⚠️ {fn.__name__}: {e}")
            return fb if fb is not None else {}
    timeline = safe(_load_timeline, search_fn, fb={})
    kpi = _latest_snapshot(timeline)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {len(kpi)}코드 로드, 섹션 계산중...")
    s1=build_s1(kpi); s2=build_s2(kpi); s3=build_s3(kpi); s4=build_s4(kpi)
    s5=build_s5(kpi); s6=build_s6(kpi); s7=build_s7(s1,s2,s3,s4,s5)
    ctx=build_context(s1,s2,s3,s4,s5,s6,s7,timeline=timeline)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 완료")
    result = {
        's1_executive':s1,'s2_funnel':s2,'s3_engagement':s3,'s4_growth':s4,
        's5_rankings':s5,'s6_channels':s6,'s7_anomalies':s7,
        'claude_context':ctx,'collected_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    if return_timeline:
        result['_timeline'] = timeline
        all_dates = set()
        for code_rows in timeline.values():
            all_dates.update(code_rows.keys())
        if all_dates:
            result['_timeline_meta'] = {
                'date_min': min(all_dates), 'date_max': max(all_dates),
                'days_count': len(all_dates), 'codes_count': len(timeline)
            }
    return result


# ── 시계열 조회 헬퍼 (Step 3 이후 활용) ─────────────────────────────────

def get_timeseries(timeline, code, days=30):
    """특정 PGM_CODE의 최근 N일치 시계열. 날짜 오름차순 반환."""
    if code not in timeline:
        return []
    date_rows = timeline[code]
    sorted_dates = sorted(date_rows.keys(), reverse=True)[:days]
    sorted_dates.reverse()
    return [date_rows[d] for d in sorted_dates]

def get_snapshot_at(timeline, target_date):
    """특정 날짜의 모든 PGM_CODE 스냅샷. build_s1~s7 호환 형태."""
    return {code: date_rows[target_date]
            for code, date_rows in timeline.items()
            if target_date in date_rows}

def get_metric_trend(timeline, code, metric_field, days=30):
    """특정 PGM_CODE + 지표의 시계열. [(date, value_or_None), ...] 오름차순."""
    return [(r.get('DATE'), _fn(r.get(metric_field)))
            for r in get_timeseries(timeline, code, days)]

def get_available_dates(timeline):
    """timeline에 존재하는 모든 날짜 목록 (오름차순)."""
    all_dates = set()
    for date_rows in timeline.values():
        all_dates.update(date_rows.keys())
    return sorted(all_dates)

def get_codes_summary(timeline):
    """코드별 데이터 가용성 요약. {PGM_CODE: {days, date_min, date_max}}"""
    summary = {}
    for code, date_rows in timeline.items():
        if not date_rows:
            continue
        dates = sorted(date_rows.keys())
        summary[code] = {'days': len(dates), 'date_min': dates[0], 'date_max': dates[-1]}
    return summary


def _build_timeseries_insights(timeline, days=7):
    """timeline에서 시계열 인사이트 텍스트 생성. claude_context에 추가용."""
    if not timeline:
        return ''
    available_dates = get_available_dates(timeline)
    if len(available_dates) < 2:
        return ''

    actual_days = min(days, len(available_dates))
    lines = ['', f'[최근 {actual_days}일 시계열 추세]']

    # 1. 전체(T00) DAU 추세
    t00_trend = get_metric_trend(timeline, 'T00', 'dau_today', days=actual_days)
    valid = [(d, v) for d, v in t00_trend if v is not None]
    if len(valid) >= 2:
        avg = sum(v for _, v in valid) / len(valid)
        first_v, latest_v = valid[0][1], valid[-1][1]
        chg = (latest_v - first_v) / first_v * 100 if first_v else 0
        direction = 'up' if chg > 2 else ('down' if chg < -2 else 'flat')
        dir_str = {'up': '(+) 상승', 'down': '(-) 하락', 'flat': '(=) 안정'}[direction]
        lines.append(f'  전체 DAU {actual_days}일 평균: {int(avg):,}명')
        lines.append(f'  추세: {dir_str} ({chg:+.1f}%, {int(first_v):,} -> {int(latest_v):,})')

    # 2. 채널별 변화
    lines.append(f'  [채널별 {actual_days}일 변화]')
    for ch in ('F00', 'L00', 'G00', 'P00'):
        ch_trend = get_metric_trend(timeline, ch, 'dau_today', days=actual_days)
        vl = [(d, v) for d, v in ch_trend if v is not None]
        if len(vl) >= 2:
            fv, lv = vl[0][1], vl[-1][1]
            chg = (lv - fv) / fv * 100 if fv else 0
            avg = sum(v for _, v in vl) / len(vl)
            lines.append(f'    {PGM_NAMES.get(ch, ch)}: avg {int(avg):,} / {chg:+.1f}% ({int(fv):,}->{int(lv):,})')

    # 3. 깊은청취율 추세
    dr_trend = get_metric_trend(timeline, 'T00', 'deep_rate', days=actual_days)
    vl_dr = [(d, v) for d, v in dr_trend if v is not None]
    if len(vl_dr) >= 2:
        fv, lv = vl_dr[0][1], vl_dr[-1][1]
        avg_dr = sum(v for _, v in vl_dr) / len(vl_dr)
        lines.append(f'  [깊은청취율 추세] avg {avg_dr:.1f}%, {fv:.1f}% -> {lv:.1f}% ({lv-fv:+.1f}pp)')

    # 4. 급변 감지 (전일 대비 ±10%, DAU 1000명 이상)
    if len(available_dates) >= 2:
        latest_d, prev_d = available_dates[-1], available_dates[-2]
        spikes, drops = [], []
        exclude = {'T00', 'F00', 'L00', 'G00', 'P00', 'L04'}
        for code, date_rows in timeline.items():
            if code in exclude:
                continue
            lr = date_rows.get(latest_d)
            pr = date_rows.get(prev_d)
            if not lr or not pr:
                continue
            lv = _f(lr.get('dau_today'), 0)
            pv = _f(pr.get('dau_today'), 0)
            if pv < 1000:
                continue
            chg = (lv - pv) / pv * 100
            nm = PGM_NAMES.get(code, code)
            if chg >= 10:
                spikes.append((nm, chg, int(lv)))
            elif chg <= -10:
                drops.append((nm, chg, int(lv)))
        if spikes or drops:
            lines.append('  [전일 대비 급변]')
            for nm, chg, dau in sorted(spikes, key=lambda x: -x[1])[:3]:
                lines.append(f'    [+] {nm} {chg:+.1f}% (DAU {dau:,})')
            for nm, chg, dau in sorted(drops, key=lambda x: x[1])[:3]:
                lines.append(f'    [-] {nm} {chg:+.1f}% (DAU {dau:,})')

    # 5. 어제 vs 최근 평균 비교
    if len(available_dates) >= 4:
        hist_dates = available_dates[-actual_days:-1]
        latest_d = available_dates[-1]
        hist_dau = [_f(timeline.get('T00', {}).get(d, {}).get('dau_today'), 0)
                    for d in hist_dates]
        hist_dau = [v for v in hist_dau if v]
        latest_dau = _f(timeline.get('T00', {}).get(latest_d, {}).get('dau_today'), 0)
        if hist_dau and latest_dau:
            avg = sum(hist_dau) / len(hist_dau)
            diff_pct = (latest_dau - avg) / avg * 100 if avg else 0
            lines.append(f'  [어제 vs 최근 평균] DAU {int(latest_dau):,} vs {len(hist_dau)}일 avg {int(avg):,} ({diff_pct:+.1f}%)')

    return '\n'.join(lines)
