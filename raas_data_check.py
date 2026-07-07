# -*- coding: utf-8 -*-
"""데이터 확인 — 매일 아침 스플렁크에서 가져오는 집계 타임라인 점검(데이터 직무용).

run_data_check(timeline) → {ok, source, data_date, code_count, field_count, summary, checks[]}.
timeline = get_cached_timeline() 형태 {PGM_CODE: {DATE: row}} (스플렁크). 폴백 CSV 파일이 아니라
실제 서비스가 쓰는 스플렁크 데이터를 검사. 스플렁크 집계는 최신 DATE = 어제(D-1)가 정상.
목적: '스플렁크가 집계를 잘 만들었나 + RAAS가 잘 가져왔나'만 검사(데이터 품질).
사용자 행동 변화 이상탐지(z-score)는 여기서 하지 않음. 검사는 결정적(규칙),
총평은 서버가 LLM으로 짧게 덧붙임(하이브리드).
"""
from __future__ import annotations
import datetime
import json
import os
import re
from collections import defaultdict

# ── 필드 계보(lineage) — data/field_lineage.json (build_field_lineage.py로 생성) ──
# 결측/이상 필드가 어느 룩업·정제작업(서머리인덱스)에서 오는지 안내하는 데 사용.
_LINEAGE = None
_LINEAGE_SUFFIXES = ("_prev", "_chg", "_diff", "_share")


def _load_lineage() -> dict:
    """field_lineage.json 로드(1회 캐시). 없으면 빈 dict — 계보 안내만 생략(점검은 정상 동작)."""
    global _LINEAGE
    if _LINEAGE is not None:
        return _LINEAGE
    here = os.path.dirname(os.path.abspath(__file__))
    for p in (os.path.join(here, "data", "field_lineage.json"),
              os.path.join(here, "field_lineage.json")):
        try:
            with open(p, encoding="utf-8") as f:
                _LINEAGE = json.load(f).get("fields", {})
                return _LINEAGE
        except Exception:
            continue
    _LINEAGE = {}
    return _LINEAGE


def lineage_for(field: str):
    """CSV 컬럼명 → 계보 엔트리. _prev/_chg/_diff/_share 변형은 base로 해석."""
    fields = _load_lineage()
    if not field:
        return None
    if field in fields:
        return fields[field]
    for suf in _LINEAGE_SUFFIXES:
        if field.endswith(suf) and field[: -len(suf)] in fields:
            return fields[field[: -len(suf)]]
    return None


def _fmt_source(s: dict) -> str:
    """소스 dict → '룩업 · 정제작업(실행시간)' 짧은 문자열."""
    out = s.get("lookup", "?")
    job = s.get("job")
    if job:
        exec_ = s.get("exec")
        out += " · " + job + (f"({exec_})" if exec_ else "")
    part = s.get("part")
    if part:
        out += f" [{part}]"
    return out


def source_lines(fields, limit: int = 8) -> list:
    """문제 필드 목록 → 'field → 소스1; 소스2' 라인(중복 제거)."""
    seen, lines = set(), []
    for f in fields:
        e = lineage_for(f)
        if not e:
            continue
        srcs = "; ".join(_fmt_source(s) for s in e.get("sources", []))
        key = (f, srcs)
        if key in seen:
            continue
        seen.add(key)
        lines.append(f"{f} → {srcs}")
        if len(lines) >= limit:
            break
    return lines


def distinct_jobs(fields) -> list:
    """문제 필드들이 의존하는 정제작업(중복 제거) — 스케줄러 실패 대조용 struct 리스트."""
    seen, jobs = set(), []
    for f in fields:
        e = lineage_for(f)
        if not e:
            continue
        for s in e.get("sources", []):
            key = (s.get("lookup"), s.get("job"))
            if key in seen:
                continue
            seen.add(key)
            jobs.append({"lookup": s.get("lookup"), "job": s.get("job"),
                         "exec": s.get("exec"), "summary_index": s.get("summary_index")})
    return jobs


# ── 스케줄러 실패 교차확인 — 필드 이상 ↔ 정제작업이 실제로 skip/에러났는지 ────────
def _norm_name(s: str) -> str:
    """정제작업명/스케줄러 savedsearch_name 비교용 정규화(공백 제거·소문자)."""
    return re.sub(r"\s+", "", (s or "")).strip().lower()


def _recount(report: dict) -> None:
    checks = report.get("checks", [])
    report["summary"] = {
        "red": sum(1 for c in checks if c["severity"] == "red"),
        "yellow": sum(1 for c in checks if c["severity"] == "yellow"),
        "green": sum(1 for c in checks if c["severity"] == "green"),
    }


_SEV_ORDER = {"green": 0, "yellow": 1, "red": 2}
_RUN_TIME_MIN = 1.0   # 성공이라도 수행시간이 이 값(초) 이하면 '무작업 의심'으로 표시


def _status_badge(entry: dict):
    """스케줄러 상태 엔트리 → (표시배지, severity).
    성공이라도 run_time<=_RUN_TIME_MIN(초)이면 사실상 무작업(빈 결과) 의심 → yellow."""
    st = str(entry.get("status") or "").lower()
    if "error" in st or "fail" in st:
        return "❌ 실패", "red"
    if st == "skipped":
        return "⏭ 스킵", "red"
    if st == "deferred":
        return "🕒 지연", "yellow"
    if st in ("success", "completed", "delegated_remote_completion", "delegated_remote"):
        try:
            rt = float(entry.get("run_time"))
        except (TypeError, ValueError):
            rt = None
        if rt is not None and rt <= _RUN_TIME_MIN:
            rc = entry.get("result_count")
            rc_note = f"·결과 {rc}행" if str(rc or "").strip() not in ("", "None") else ""
            return f"⚠ 정상이나 수행 {rt:g}s{rc_note}(무작업 의심)", "yellow"
        return "✅ 정상", "green"
    return (st or "?"), "yellow"


def cross_check_schedules(report: dict, status_map) -> dict:
    """이상 필드마다 관련 정제작업의 실제 스케줄 상태를 필드별로 붙인다(in-place, summary 재계산).

    status_map 의미:
      None → 조회 불가(권한/접속) → '확인 불가'(yellow)만 남기고 보류.
      {savedsearch_name: {status,last,reason,...}} → 필드별 상태 라인 생성.

    필드 → 계보 정제작업(job) → status_map 에서 정규화+부분일치로 상태 조회.
    라인 severity = 그 필드 배치들 중 가장 나쁜 스케줄 상태(실패/스킵=red, 로그없음=yellow, 정상=green).
    """
    checks = report.setdefault("checks", [])

    def add(sev, title, detail):
        # group=schedule → 렌더러가 '정제 배치·스케줄 상태' 별도 표로 분리
        checks.append({"severity": sev, "title": title, "detail": detail, "group": "schedule"})

    flagged = report.get("flagged_fields") or []
    if not flagged:
        _recount(report); return report   # 필드 단위 이상 없음 → 스케줄 표 생략

    unknown = status_map is None           # 조회 실패 → 배치명만 표시(상태=확인불가)
    norm = {} if unknown else {_norm_name(k): v for k, v in status_map.items() if k}

    def _lookup(job):
        jn = _norm_name(job)
        if jn in norm:
            return norm[jn]
        for k, v in norm.items():
            if k and (k in jn or jn in k):
                return v
        return None

    def _base_of(f):
        for suf in _LINEAGE_SUFFIXES:
            if f.endswith(suf) and f[: -len(suf)]:
                return f[: -len(suf)]
        return f

    shown = 0
    n_bad = 0
    seen_bases = set()
    for f in flagged:
        base = _base_of(f)                 # prev/chg/diff/share 변형은 base로 묶어 중복 라인 방지
        if base in seen_bases:
            continue
        seen_bases.add(base)
        e = lineage_for(base)
        if not e:
            continue
        parts, worst = [], "green"
        for s in e.get("sources", []):
            job = s.get("job")
            if not job or job.startswith("("):   # 정적 룩업(BROADPLAN 등)은 스케줄 없음
                continue
            if unknown:
                parts.append(f"{job}: ⚠ 상태확인불가")
                if _SEV_ORDER["yellow"] > _SEV_ORDER[worst]:
                    worst = "yellow"
                continue
            st = _lookup(job)
            if st is None:
                parts.append(f"{job}: ⚪ 로그없음")
                if _SEV_ORDER["yellow"] > _SEV_ORDER[worst]:
                    worst = "yellow"
            else:
                badge, sev = _status_badge(st)
                extra = ""
                if sev == "red" and st.get("reason"):
                    extra = f" ({str(st['reason'])[:40]})"
                parts.append(f"{job}: {badge}({st.get('last','?')}){extra}")
                if _SEV_ORDER[sev] > _SEV_ORDER[worst]:
                    worst = sev
        if not parts:
            continue
        if worst != "green":
            n_bad += 1
        add(worst, f"{base} — {e.get('label', base)}", " · ".join(parts))
        shown += 1
        if shown >= 16:
            add("yellow", "…", f"이상 필드(기준) {len(seen_bases)}개 중 16개만 표시")
            break

    if unknown and shown:
        add("yellow", "스케줄 상태 확인 불가",
            "_internal(sourcetype=scheduler) 조회 실패 — 배치명만 표시(권한/접속 확인)")
    elif shown and n_bad == 0:
        add("green", "관련 정제배치 모두 정상 실행",
            "이상 필드의 정제작업이 스케줄상 정상 — 이상 원인은 계산 로직/소스 데이터 의심")

    _recount(report); return report

# 메타(비수치) 컬럼 — 수치 파싱·빈값 검사에서 제외
_META_COLS = {
    "DATE", "DATE_WEEK", "DATE_MON", "PGM_CODE", "PGM_NAME", "STIME", "program_title",
    "guestname", "daily_corner", "weekly_corner", "view_radio_yn", "live_yn",
}
_EXPECTED_CODES = 37
_STALE_RED_FRAC = 0.5      # 정체 코드가 절반 초과면 red

# 매핑된 프로그램이 없는 미사용 코드 — 코드 수 카운트엔 존재하나 값 기반 점검에선 제외.
_EXCLUDED_CODES = {"L04"}


def _is_weekend_program(code: str) -> bool:
    """주말 편성 프로그램(M* 코드). 평일엔 방송이 없어 값이 비는 게 정상(결측 아님)."""
    return (code or "").startswith("M")


def _is_weekly(f: str) -> bool:
    return ("week" in f) or f.startswith("wau") or f.startswith("w1_")


def _is_monthly(f: str) -> bool:
    return ("mon" in f) or f.startswith("mau") or f.startswith("m1_")


def _num(v):
    """수치 파싱. 빈 값/None → None(빈값), 파싱 실패 → False(비수치), 성공 → float."""
    if v is None:
        return None
    s = str(v).strip().replace(",", "").replace("%", "")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return False


def _parse_date(s):
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime((s or "").strip(), fmt).date()
        except Exception:
            pass
    return None


def _flatten(timeline) -> list:
    """{PGM_CODE:{DATE:row}} (스플렁크 타임라인) → flat row list."""
    rows = []
    for code, date_rows in (timeline or {}).items():
        if not isinstance(date_rows, dict):
            continue
        for date, row in date_rows.items():
            if not isinstance(row, dict):
                continue
            r = dict(row)
            r["PGM_CODE"] = row.get("PGM_CODE") or code
            r["DATE"] = row.get("DATE") or date
            rows.append(r)
    return rows


def run_data_check(timeline) -> dict:
    checks = []
    def add(sev, title, detail="", sources=None):
        c = {"severity": sev, "title": title, "detail": detail}
        if sources:
            c["sources"] = sources   # 스케줄러 실패 대조용 struct(룩업·정제작업·서머리인덱스)
        checks.append(c)

    rows = _flatten(timeline)
    if not rows:
        add("red", "스플렁크 데이터 없음", "타임라인이 비어 있음 — 아침 적재 실패 의심")
        return {"ok": False, "source": "splunk", "summary": {"red": 1, "yellow": 0, "green": 0}, "checks": checks}

    # 컬럼 집합(행마다 키가 다를 수 있어 union)
    keyset = set()
    for r in rows[:100]:
        keyset.update(r.keys())
    numeric_cols = [c for c in keyset if c not in _META_COLS]
    daily_cols = [c for c in numeric_cols if not _is_weekly(c) and not _is_monthly(c)]

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    dates = sorted({(r.get("DATE") or "").strip() for r in rows if r.get("DATE")})
    latest = dates[-1] if dates else None
    prior = dates[-2] if len(dates) >= 2 else None
    latest_d = _parse_date(latest)

    latest_rows = [r for r in rows if (r.get("DATE") or "").strip() == latest]
    prior_rows = {r.get("PGM_CODE"): r for r in rows if (r.get("DATE") or "").strip() == prior}
    # 값 기반 점검용 — 미사용 코드(L04 등) 제외. 코드 수 카운트는 latest_rows 그대로 사용.
    checkable_rows = [r for r in latest_rows if r.get("PGM_CODE") not in _EXCLUDED_CODES]
    # 최신 데이터 일자가 평일이면 주말 프로그램(M*) 결측은 정상 → 결측 판정에서 제외.
    latest_is_weekday = bool(latest_d and latest_d.weekday() < 5)

    # ── A. 최신성 (스플렁크 집계 최신 DATE = 어제 D-1이 정상) ─────────
    if latest_d and latest_d < yesterday:
        gap = (yesterday - latest_d).days
        add("red", "데이터가 최신이 아님",
            f"최신 데이터 {latest} · 기대 {yesterday:%Y/%m/%d}(어제) · {gap}일 지연 — 아침 적재 실패/지연 의심")
    elif latest_d == yesterday:
        add("green", "최신 데이터 확인(어제)", f"최신 {latest}")
    else:
        add("yellow", "최신 데이터 날짜 확인 필요", f"최신 {latest} · 기대 어제 {yesterday:%Y/%m/%d}")

    if len(latest_rows) < _EXPECTED_CODES:
        add("yellow", "코드 수 부족", f"최신일 {len(latest_rows)} / 기대 {_EXPECTED_CODES}개")
    else:
        add("green", "코드 수 확인", f"{len(latest_rows)}개")

    # ── B. 무결성 ──────────────────────────────────────────────────
    bad_type = []
    for r in checkable_rows:
        for c in numeric_cols:
            if _num(r.get(c)) is False:
                bad_type.append((r.get("PGM_CODE"), c, str(r.get(c)).strip()))
    if bad_type:
        _detail = "; ".join(f"{code}·{c}={val!r}" for code, c, val in bad_type[:6])
        add("red", f"비수치 값 {len(bad_type)}건", _detail)   # 관련 배치는 하단 '정제 배치·스케줄' 표 참고
    else:
        add("green", "수치 필드 형식 정상", "이상문자 없음")

    dropped = defaultdict(list)   # 전일 있었는데 최신 결측
    empty_total = 0
    for r in checkable_rows:
        code = r.get("PGM_CODE")
        pr = prior_rows.get(code)
        # 평일의 주말 프로그램(M*) 빈 값은 정상(방송 없음) → 결측 판정 제외
        weekend_prog_on_weekday = _is_weekend_program(code) and latest_is_weekday
        for c in numeric_cols:
            if str(r.get(c) or "").strip() == "":
                empty_total += 1
                if weekend_prog_on_weekday:
                    continue
                if pr and str(pr.get(c) or "").strip() != "":
                    dropped[c].append(code)
    if dropped:
        flds = sorted(dropped.items(), key=lambda kv: -len(kv[1]))
        add("yellow", f"전일 대비 결측 발생 · {len(dropped)}개 필드",
            ", ".join(f"{k}({len(v)})" for k, v in flds))   # 관련 배치는 하단 '정제 배치·스케줄' 표 참고
    if empty_total:
        add("green", f"빈 값 총 {empty_total}건", "(대부분 정상 결측 — 전일 대비 신규 결측은 위 항목 참고)")

    # ── C. 신규값 생성(어제/지난주/지난달과 다른 값인지) ──────────────
    stale = []
    for r in checkable_rows:
        pr = prior_rows.get(r.get("PGM_CODE"))
        if not pr:
            continue
        if daily_cols and all(str(r.get(c) or "") == str(pr.get(c) or "") for c in daily_cols):
            stale.append(r.get("PGM_CODE"))
    if stale:
        sev = "red" if len(stale) > len(latest_rows) * _STALE_RED_FRAC else "yellow"
        add(sev, f"정체값(전일과 동일) {len(stale)}개 코드",
            "일간 지표 전체가 전일과 동일 — 신규값 미생성 의심: " + ", ".join(stale[:10])
            + "  ▸ 일간 정제배치 전반(program_user/newuser_funnel_day.csv 등 01:20~02:50)"
              " 또는 07:10 통합 저장 배치 미실행 확인")
    else:
        add("green", "일간 신규값 생성 확인", "전일과 다른 값이 생성됨")

    for label, key_col, val_col in [("주간", "DATE_WEEK", "wau"), ("월간", "DATE_MON", "mau")]:
        if key_col not in numeric_cols and key_col not in _META_COLS:
            # key_col은 메타라 numeric_cols에 없음 — 존재만 확인
            if key_col not in keyset:
                continue
        if val_col not in keyset:
            continue
        within_change, boundary_stale = 0, 0
        for r in checkable_rows:
            pr = prior_rows.get(r.get("PGM_CODE"))
            if not pr:
                continue
            same_period = str(r.get(key_col) or "") == str(pr.get(key_col) or "")
            v_now = str(r.get(val_col) or "").strip()
            same_val = v_now == str(pr.get(val_col) or "").strip()
            # 0/빈값이 경계를 넘어 그대로인 건 '미사용·무활동 코드'(예: L04)라 미갱신이 아님 → 제외
            meaningful = v_now not in ("", "0", "0.0", ".0", "None")
            if same_period and not same_val:
                within_change += 1
            if (not same_period) and same_val and meaningful:
                boundary_stale += 1
        if within_change:
            add("yellow", f"{label} 지표 기간 내 변동 {within_change}건",
                f"같은 {label}({key_col}) 내인데 {val_col} 값이 변함")
        if boundary_stale:
            add("yellow", f"{label} 경계 미갱신 {boundary_stale}건",
                f"새 {label}({key_col})인데 {val_col} 값이 그대로")

    # ── D. 범위·이상치 ──────────────────────────────────────────────
    bad_range = []
    for r in checkable_rows:
        for c in numeric_cols:
            v = _num(r.get(c))
            if v is None or v is False:
                continue
            if v < 0 and not any(k in c for k in ("chg", "diff", "prev")):
                bad_range.append((r.get("PGM_CODE"), c, v, "음수"))
            elif ("rate" in c or "_ret" in c) and v > 100 and not any(k in c for k in ("chg", "diff")):
                bad_range.append((r.get("PGM_CODE"), c, v, ">100%"))
    if bad_range:
        _detail = "; ".join(f"{code}·{c}={v}({why})" for code, c, v, why in bad_range[:6])
        add("yellow", f"범위 이상 {len(bad_range)}건", _detail)   # 관련 배치는 하단 '정제 배치·스케줄' 표 참고

    # ── E. 전면 결측(조용한 파이프라인 중단) ───────────────────────────
    # 최신일에 '전 코드' 비어있는데(0/N) 과거엔 값이 있었고, 그 마지막 값이 전일보다 이전.
    # → 전일 대비 델타로는 못 잡는 오래된 지속 결측(예: 주간 배치가 몇 주째 값 미생성).
    def _has(v):
        return str(v or "").strip() not in ("", "None")
    zero_cols = [c for c in numeric_cols if not any(_has(r.get(c)) for r in checkable_rows)]
    full_missing = []
    for c in zero_cols:
        last_had = None
        for r in rows:
            if _has(r.get(c)):
                d = (r.get("DATE") or "").strip()
                if last_had is None or d > last_had:
                    last_had = d
        if last_had and prior and last_had < prior:   # 전일 이전에 멈춤 → 델타 규칙 사각지대
            full_missing.append((c, last_had))
    if full_missing:
        by_base = {}   # 변형(_prev/_chg/_diff/_share)은 base로 묶어 표시
        for c, d in full_missing:
            b = c
            for suf in ("_prev", "_chg", "_diff", "_share"):
                if b.endswith(suf):
                    b = b[: -len(suf)]; break
            by_base[b] = min(by_base.get(b, d), d)   # 가장 오래된 마지막값
        listed = ", ".join(f"{b}(~{d})" for b, d in sorted(by_base.items()))
        add("red", f"전면 결측 · {len(by_base)}개 지표 (최신일 전 코드 결측)",
            f"과거엔 값이 있었으나 지금 전 코드 비어있음 — 파이프라인 중단 의심: {listed}")

    # 필드 단위 이상 목록 — 스케줄 상태를 필드별로 붙일 때 사용(결측·비수치·범위·전면결측)
    flagged = set(dropped.keys())
    flagged.update(c for _, c, _ in bad_type)
    flagged.update(c for _, c, _, _ in bad_range)
    flagged.update(c for c, _ in full_missing)   # 전면 결측도 스케줄 표에 배치 상태 노출

    summary = {
        "red": sum(1 for c in checks if c["severity"] == "red"),
        "yellow": sum(1 for c in checks if c["severity"] == "yellow"),
        "green": sum(1 for c in checks if c["severity"] == "green"),
    }
    return {"ok": True, "source": "splunk", "data_date": latest, "prior_date": prior,
            "code_count": len(latest_rows), "field_count": len(numeric_cols),
            "flagged_fields": sorted(flagged),
            "summary": summary, "checks": checks}


if __name__ == "__main__":
    import json
    import raas_server as SRV
    print(json.dumps(run_data_check(SRV.get_cached_timeline()), ensure_ascii=False, indent=2))

