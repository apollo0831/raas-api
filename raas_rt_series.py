"""실시간 통합 접근자 (RT-1) — raas_rt_series.

분단위 실시간을 '필드명에 인코딩된 형태'와 무관하게 하나의 API로 조회한다.
필드 템플릿 레지스트리(RT_METRICS) + 시간 fetch(datasource.rt_fetch) + 균일 반환.
일간 raas_series의 실시간 짝. 소비자(차트·추출·스냅샷)는 이 위 얇은 포매터가 된다.

원칙: 여기 있는 건 **구조 어댑터 + 필드 템플릿**뿐 — 도메인 규칙(동시방송 공리·인증 분모=
인증자합계·보라 별도축)은 온톨로지, 질의 해석은 상위 플래너(P-1). target(프로그램→채널+편성창)
해석은 grounding._rt_resolve_target이 담당하고 여기엔 (채널, window)만 넘어온다.
"""
import raas_datasource as DSRC

_ALL5 = ["T00", "F00", "L00", "G00", "P00"]
_AUTH = ["F00", "L00"]                    # 인증(성별·연령·문자·공감) 채널
_CH_LABEL = {"T00": "전체", "F00": "파워FM", "L00": "러브FM", "G00": "고릴라M", "P00": "픽채널"}

# 필드 템플릿 레지스트리 — 새 실시간 지표 = 여기 한 줄(코드 로직 X). 온톨로지 RTMetric과 1:1.
#   tmpl 슬롯: {ch}=채널코드 · {ch_suf}=''(T00)|'_F00'… · {device}/{sex}/{age}=dims 값.
RT_METRICS = {
    "concurrent": {"source": "rt_concurrent", "tmpl": "{ch}",              "chans": _ALL5, "dims": []},
    "device":     {"source": "rt_concurrent", "tmpl": "DV_{device}{ch_suf}", "chans": _ALL5, "dims": ["device"]},
    "viewradio":  {"source": "rt_concurrent", "tmpl": "BA{ch_suf}",        "chans": ["T00", "F00", "L00"], "dims": []},
    "authusers":  {"source": "rt_concurrent", "tmpl": "{ch}_REALINFO",     "chans": _AUTH, "dims": []},
    "sex_ratio":  {"source": "rt_concurrent", "tmpl": "R_{ch}_SEX_{sex}",  "chans": _AUTH, "dims": ["sex"]},
    "age_ratio":  {"source": "rt_concurrent", "tmpl": "R_{ch}_AGE_{age}",  "chans": _AUTH, "dims": ["age"]},
    "msg_sms":    {"source": "rt_msg",        "tmpl": "{ch}_SMS",          "chans": _AUTH, "dims": []},
    "msg_gg":     {"source": "rt_msg",        "tmpl": "{ch}_GG",           "chans": _AUTH, "dims": []},
    "inflow":     {"source": "rt_inflow",     "tmpl": "{ch}_START",        "chans": ["F00", "G00", "L00", "P00"], "dims": []},
}


def _field(m: dict, ch: str, dims: dict) -> str:
    dims = dims or {}
    ch_suf = "" if ch == "T00" else "_" + ch
    return m["tmpl"].format(ch=ch, ch_suf=ch_suf,
                            device=dims.get("device", ""), sex=dims.get("sex", ""), age=dims.get("age", ""))


def _hhmm(r) -> str:
    t = str(r.get("_time") or "")
    return t[11:16] if len(t) >= 16 else ""


def _num(v):
    try:
        return None if v in (None, "", "None") else (int(float(v)) if float(v) == int(float(v)) else float(v))
    except Exception:
        return None


def _on_grid(t: str, res) -> bool:
    if not res or res <= 1:
        return True
    try:
        return int(t[3:5]) % res == 0
    except Exception:
        return True


# ── 공개 API ────────────────────────────────────────────────────────────────
def metric_channels(metric: str) -> list:
    m = RT_METRICS.get(metric)
    return list(m["chans"]) if m else []


def rt_available(when: str = "today") -> dict:
    """보관 범위 확인 → {ok, earliest, reason?}. 과거일이 보관 시작 이전이면 ok=False."""
    earliest = DSRC.get_rt_earliest()
    if when in ("today", "now", "", None):
        return {"ok": True, "earliest": earliest}
    date = (when or "").replace("/", "-")[:10]
    if earliest and date < earliest:
        return {"ok": False, "earliest": earliest, "reason": f"분단위는 {earliest}부터 보유 — {date}는 범위 밖"}
    return {"ok": True, "earliest": earliest}


def rt_series(metric: str, target: str = "T00", when: str = "today",
              resolution: int = 1, dims: dict = None, window=None) -> list:
    """[(hhmm, value)] — 단일 실시간 지표·대상의 분단위 시계열(형태 무관 균일).
       target=채널코드(T00/F00/…). window=(start,end)'HH:MM'면 그 창만(프로그램 편성창).
       resolution>1이면 그 분 그리드만(차트 다운샘플)."""
    m = RT_METRICS.get(metric)
    if not m:
        return []
    ch = (target or "T00").upper()
    if ch not in m["chans"]:
        return []
    field = _field(m, ch, dims)
    out = []
    for r in DSRC.rt_fetch(m["source"], when):
        t = _hhmm(r)
        if not t or (window and not (window[0] <= t < window[1])) or not _on_grid(t, resolution):
            continue
        v = _num(r.get(field))
        if v is not None:
            out.append((t, v))
    return out


def rt_table(metric: str, targets: list = None, when: str = "today",
             resolution: int = 1, dims: dict = None, window=None, labels: dict = None) -> dict:
    """다열 표 {header, rows} — targets=채널코드 리스트(None→그 지표 전 채널). 추출/스냅샷용."""
    m = RT_METRICS.get(metric)
    if not m:
        return {"header": [], "rows": []}
    chs = [c for c in (targets or m["chans"]) if c in m["chans"]]
    lab = labels or _CH_LABEL
    per_ch = {c: dict(rt_series(metric, c, when, resolution, dims, window)) for c in chs}
    times = sorted(set().union(*[set(d) for d in per_ch.values()])) if per_ch else []
    header = ["시각"] + [lab.get(c, c) for c in chs]
    rows = [[t] + [per_ch[c].get(t, "") for c in chs] for t in times]
    return {"header": header, "rows": rows, "channels": chs}


def rt_snapshot(metric: str, target: str = "T00", when: str = "today", dims: dict = None):
    """최신(또는 그날 마지막) 단일 값."""
    s = rt_series(metric, target, when, 1, dims)
    return s[-1][1] if s else None
