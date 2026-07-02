# -*- coding: utf-8 -*-
"""라우트 스모크 — 전 엔드포인트에 무인증 요청을 쏘고 응답코드를 기록/비교.

용도: 서버 라우팅 리팩토링 전후 회귀 확인 (기능 검증이 아니라 '라우팅+가드' 검증).
  python tests/smoke_routes.py capture before.json   # 현재 응답코드 스냅샷
  python tests/smoke_routes.py capture after.json
  python tests/smoke_routes.py diff before.json after.json

무인증이므로 대부분 401/400이 정상값 — 코드가 '같게 유지되는지'만 본다.
서버가 localhost:5000 에 떠 있어야 함.
"""
import json
import sys
import urllib.request
import urllib.error

BASE = "http://localhost:5000"

GET_ROUTES = [
    "/", "/raas_web.js?v=1", "/api/version", "/api/status", "/api/schema",
    "/api/timeline/meta", "/api/programs", "/api/profiles", "/api/posthog-config",
    "/api/me", "/api/ip-users", "/api/admin/users", "/api/suggestions",
    "/api/timeseries/program/F09?metric=dau&days=7", "/api/snapshot/2026-06-30",
    "/api/trend?metric=dau", "/api/query/popular", "/api/query/history/all",
    "/api/query/history", "/api/concept/search?q=dau", "/api/concept/dau",
    "/api/briefing", "/api/rawdata", "/api/my/interest-map",
    "/api/admin/stats/overview", "/api/storyline/exports/tok", "/api/nonexistent",
]

POST_ROUTES = [
    "/api/register", "/api/login", "/api/logout", "/api/me/update", "/api/me/password",
    "/api/admin/approve", "/api/query/feedback", "/api/query/stream",
    "/api/improve/context", "/api/improve/contribute", "/api/improve/requery",
    "/api/improve/verdict", "/api/improve/mine", "/api/upload/add",
    "/api/upload/mine/retire", "/api/upload/review", "/api/knowledge/mine/retire",
    "/api/improve/queue", "/api/improve/review", "/api/data_request/process",
    "/api/knowledge/retire", "/api/knowledge/reclassify", "/api/knowledge/promote",
    "/api/knowledge/promote/preview", "/api/style/get", "/api/style/set",
    "/api/query", "/api/storyline/today", "/api/data_check", "/api/data_refresh",
    "/api/nonexistent-post",
]


# 무인증 통과 + Splunk/브리핑 연산으로 콜드캐시에서 오래 걸리는 라우트
_HEAVY = ("/api/briefing", "/api/rawdata", "/api/trend", "/api/timeseries", "/api/snapshot")


def _code(method: str, path: str) -> int:
    req = urllib.request.Request(BASE + path, method=method)
    if method == "POST":
        req.data = b"{}"
        req.add_header("Content-Type", "application/json")
    tmo = 180 if any(path.startswith(h) for h in _HEAVY) else 20
    try:
        with urllib.request.urlopen(req, timeout=tmo) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return -1   # 무응답/연결오류


def capture(outfile: str):
    result = {}
    for p in GET_ROUTES:
        result["GET " + p] = _code("GET", p)
    for p in POST_ROUTES:
        result["POST " + p] = _code("POST", p)
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    print(f"captured {len(result)} routes -> {outfile}")


def diff(a: str, b: str):
    da = json.load(open(a, encoding="utf-8"))
    db = json.load(open(b, encoding="utf-8"))
    bad = []
    for k in sorted(set(da) | set(db)):
        va, vb = da.get(k), db.get(k)
        if va != vb:
            bad.append(f"  {k}: {va} -> {vb}")
    if bad:
        print("MISMATCH", len(bad))
        print("\n".join(bad))
        sys.exit(1)
    print(f"OK — {len(da)} routes identical")


if __name__ == "__main__":
    if sys.argv[1] == "capture":
        capture(sys.argv[2])
    else:
        diff(sys.argv[2], sys.argv[3])
