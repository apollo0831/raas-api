# -*- coding: utf-8 -*-
"""데이터 확인 — raas_kpi_latest.csv 최신성·무결성 규칙 점검(데이터 직무용).

run_data_check(anomalies=None) → {ok, data_date, mtime, code_count, summary, checks[]}.
검사는 결정적(규칙). 서술(총평)은 서버가 LLM으로 짧게 덧붙임(하이브리드).
"""
from __future__ import annotations
import csv
import os
import datetime
from collections import defaultdict

_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raas_kpi_latest.csv")

# 메타(비수치) 컬럼 — 수치 파싱·빈값 검사에서 제외
_META_COLS = {
    "DATE", "DATE_WEEK", "DATE_MON", "PGM_CODE", "PGM_NAME", "STIME", "program_title",
    "guestname", "daily_corner", "weekly_corner", "view_radio_yn", "live_yn",
}
_EXPECTED_CODES = 37
_REFRESH_HOUR = 6          # 매일 06:50 갱신 → 오늘 06시 이후 mtime 기대
_STALE_RED_FRAC = 0.5      # 정체 코드가 절반 초과면 red


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


def run_data_check(anomalies=None) -> dict:
    checks = []
    def add(sev, title, detail=""):
        checks.append({"severity": sev, "title": title, "detail": detail})

    if not os.path.exists(_CSV):
        add("red", "CSV 파일 없음", _CSV)
        return {"ok": False, "summary": {"red": 1, "yellow": 0, "green": 0}, "checks": checks}

    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(_CSV))
    try:
        with open(_CSV, encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as e:
        add("red", "CSV 로드 실패", str(e))
        return {"ok": False, "summary": {"red": 1, "yellow": 0, "green": 0}, "checks": checks}

    if not rows:
        add("red", "데이터 없음(0행)", _CSV)
        return {"ok": False, "summary": {"red": 1, "yellow": 0, "green": 0}, "checks": checks}

    cols = list(rows[0].keys())
    numeric_cols = [c for c in cols if c not in _META_COLS]
    daily_cols = [c for c in numeric_cols if not _is_weekly(c) and not _is_monthly(c)]

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    dates = sorted({(r.get("DATE") or "").strip() for r in rows if r.get("DATE")})
    latest = dates[-1] if dates else None
    prior = dates[-2] if len(dates) >= 2 else None
    latest_d = _parse_date(latest)

    latest_rows = [r for r in rows if (r.get("DATE") or "").strip() == latest]
    prior_rows = {r.get("PGM_CODE"): r for r in rows if (r.get("DATE") or "").strip() == prior}

    # ── A. 최신성·적재 ──────────────────────────────────────────────
    if mtime.date() < today or (mtime.date() == today and mtime.hour < _REFRESH_HOUR):
        add("red", "파일이 오늘 갱신되지 않음",
            f"CSV mtime = {mtime:%Y-%m-%d %H:%M} (오늘 06:50 이후 갱신 기대)")
    else:
        add("green", "파일 갱신 확인", f"mtime = {mtime:%Y-%m-%d %H:%M}")

    if latest_d and latest_d < yesterday:
        gap = (yesterday - latest_d).days
        add("red", "데이터가 최신이 아님",
            f"최신 데이터 {latest} · 기대 {yesterday:%Y/%m/%d}(어제) · {gap}일 지연")
    elif latest_d == yesterday:
        add("green", "최신 데이터 확인", f"최신 {latest} (어제)")
    else:
        add("yellow", "최신 데이터 날짜 확인 필요", f"최신 {latest}")

    if len(latest_rows) < _EXPECTED_CODES:
        add("yellow", "코드 수 부족", f"최신일 {len(latest_rows)} / 기대 {_EXPECTED_CODES}개")
    else:
        add("green", "코드 수 확인", f"{len(latest_rows)}개")

    # ── B. 무결성 ──────────────────────────────────────────────────
    # 비수치(이상문자) — 수치 컬럼에 숫자 아닌 값
    bad_type = []
    for r in latest_rows:
        for c in numeric_cols:
            if _num(r.get(c)) is False:
                bad_type.append((r.get("PGM_CODE"), c, (r.get(c) or "").strip()))
    if bad_type:
        add("red", f"비수치 값 {len(bad_type)}건",
            "; ".join(f"{code}·{c}={val!r}" for code, c, val in bad_type[:6]))
    else:
        add("green", "수치 필드 형식 정상", "이상문자 없음")

    # 빈 값 — 최신일에 결측인데 전일엔 값이 있던 필드(=필드가 끊긴 신호) 우선
    dropped = defaultdict(list)   # 전일 있었는데 최신 결측
    empty_total = 0
    for r in latest_rows:
        code = r.get("PGM_CODE")
        pr = prior_rows.get(code)
        for c in numeric_cols:
            if (r.get(c) or "").strip() == "":
                empty_total += 1
                if pr and (pr.get(c) or "").strip() != "":
                    dropped[c].append(code)
    if dropped:
        add("yellow", f"전일 대비 결측 발생 {sum(len(v) for v in dropped.values())}건",
            "필드: " + ", ".join(f"{k}({len(v)})" for k, v in list(dropped.items())[:8]))
    if empty_total:
        add("green", f"빈 값 총 {empty_total}건", "(대부분 정상 결측 — 전일 대비 신규 결측은 위 항목 참고)")

    # ── C. 신규값 생성(어제/지난주/지난달과 다른 값인지) ──────────────
    # 정체값: 최신행의 '일간 지표 전체'가 전일과 완전 동일 → 신규값 미생성 의심
    stale = []
    for r in latest_rows:
        pr = prior_rows.get(r.get("PGM_CODE"))
        if not pr:
            continue
        if daily_cols and all((r.get(c) or "") == (pr.get(c) or "") for c in daily_cols):
            stale.append(r.get("PGM_CODE"))
    if stale:
        sev = "red" if len(stale) > len(latest_rows) * _STALE_RED_FRAC else "yellow"
        add(sev, f"정체값(전일과 동일) {len(stale)}개 코드",
            "일간 지표 전체가 전일과 동일 — 신규값 미생성 의심: " + ", ".join(stale[:10]))
    else:
        add("green", "일간 신규값 생성 확인", "전일과 다른 값이 생성됨")

    # 주간/월간 cadence: 같은 주/월인데 값 변경 / 새 주·월인데 값 그대로
    for label, key_col, val_col in [("주간", "DATE_WEEK", "wau"), ("월간", "DATE_MON", "mau")]:
        if key_col not in cols or val_col not in cols:
            continue
        within_change, boundary_stale = 0, 0
        for r in latest_rows:
            pr = prior_rows.get(r.get("PGM_CODE"))
            if not pr:
                continue
            same_period = (r.get(key_col) or "") == (pr.get(key_col) or "")
            same_val = (r.get(val_col) or "") == (pr.get(val_col) or "")
            if same_period and not same_val:
                within_change += 1
            if (not same_period) and same_val:
                boundary_stale += 1
        if within_change:
            add("yellow", f"{label} 지표 기간 내 변동 {within_change}건",
                f"같은 {label}({key_col}) 내인데 {val_col} 값이 변함")
        if boundary_stale:
            add("yellow", f"{label} 경계 미갱신 {boundary_stale}건",
                f"새 {label}({key_col})인데 {val_col} 값이 그대로")

    # ── D. 범위·이상치 ──────────────────────────────────────────────
    bad_range = []
    for r in latest_rows:
        for c in numeric_cols:
            v = _num(r.get(c))
            if v is None or v is False:
                continue
            if v < 0 and not any(k in c for k in ("chg", "diff", "prev")):
                bad_range.append((r.get("PGM_CODE"), c, v, "음수"))
            elif ("rate" in c or "_ret" in c) and v > 100 and not any(k in c for k in ("chg", "diff")):
                bad_range.append((r.get("PGM_CODE"), c, v, ">100%"))
    if bad_range:
        add("yellow", f"범위 이상 {len(bad_range)}건",
            "; ".join(f"{code}·{c}={v}({why})" for code, c, v, why in bad_range[:6]))

    if anomalies:
        sig = [a for a in anomalies if (a.get("level") or a.get("severity")) in ("red", "yellow")]
        if sig:
            add("yellow", f"이상탐지(z-score) {len(sig)}건",
                "; ".join(str(a.get("program") or a.get("name") or a.get("scope") or "?") for a in sig[:6]))

    summary = {
        "red": sum(1 for c in checks if c["severity"] == "red"),
        "yellow": sum(1 for c in checks if c["severity"] == "yellow"),
        "green": sum(1 for c in checks if c["severity"] == "green"),
    }
    return {"ok": True, "data_date": latest, "prior_date": prior,
            "mtime": mtime.strftime("%Y-%m-%d %H:%M"), "code_count": len(latest_rows),
            "field_count": len(numeric_cols), "summary": summary, "checks": checks}


if __name__ == "__main__":
    import json
    print(json.dumps(run_data_check(), ensure_ascii=False, indent=2))
