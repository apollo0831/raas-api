"""
RAAS 질의맵 — 읽기 전용 SQL 집계 (Phase 3 Step 1).

목적:
  query_history에 쌓인 질의/fact를 권한 있는 직무(시스템관리자/총괄관리/데이터)가
  한눈에 보게 한다. 새 Claude API 호출 없음 — 순수 SQL.

원칙:
  - fact(intent 등)가 null인 레거시 행은 graceful 제외(쿼리에서 WHERE intent IS NOT NULL).
  - 데이터가 적거나 0건이어도 항상 list/dict 반환 (호출자는 빈 상태 UI 처리).
  - raas_history_db.get_conn 컨텍스트 매니저 재사용.
"""

from datetime import datetime, timedelta
from typing import Optional, List

from raas_history_db import get_conn
from raas_onboarding import list_active_profiles


def _since(days: int) -> Optional[str]:
    """days>0이면 '최근 N일' 경계 timestamp 문자열. days<=0이면 None(전체)."""
    if not days or days <= 0:
        return None
    return (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def _date_filter(since: Optional[str], prefix: str = "WHERE") -> tuple:
    """(sql_fragment, params)"""
    if since:
        return f" {prefix} created_at >= ? ", [since]
    return "", []


# ── 1) 전체 요약 ─────────────────────────────────────────────
def stats_overview(days: int = 30) -> dict:
    """전체 KPI + 일별 추이 + 피드백 카운트."""
    since = _since(days)
    df, params = _date_filter(since)
    with get_conn() as conn:
        # KPI
        total = conn.execute(f"SELECT COUNT(*) FROM query_history {df}", params).fetchone()[0]
        fact_total = conn.execute(
            f"SELECT COUNT(*) FROM query_history {df}{'AND' if df else 'WHERE'} intent IS NOT NULL",
            params
        ).fetchone()[0]
        active_users = conn.execute(
            f"SELECT COUNT(DISTINCT user_id) FROM query_history {df}", params
        ).fetchone()[0]
        tok_row = conn.execute(
            f"SELECT COALESCE(SUM(input_tokens),0)  AS sin, "
            f"       COALESCE(SUM(output_tokens),0) AS sout, "
            f"       COALESCE(AVG(input_tokens),0)  AS ain, "
            f"       COALESCE(AVG(output_tokens),0) AS aout "
            f"FROM query_history {df}", params
        ).fetchone()
        fb_pos = conn.execute(
            f"SELECT COUNT(*) FROM query_history {df}{'AND' if df else 'WHERE'} feedback = 1",
            params
        ).fetchone()[0]
        fb_neg = conn.execute(
            f"SELECT COUNT(*) FROM query_history {df}{'AND' if df else 'WHERE'} feedback = -1",
            params
        ).fetchone()[0]
        # 일별 추이 (SQLite: date() 함수 활용)
        daily = conn.execute(
            f"SELECT date(created_at) AS d, COUNT(*) AS c "
            f"FROM query_history {df}"
            f"GROUP BY date(created_at) ORDER BY d ASC", params
        ).fetchall()

    return {
        "days":           days,
        "total":          total,
        "fact_total":     fact_total,
        "active_users":   active_users,
        "tokens": {
            "input_sum":   int(tok_row["sin"]  or 0),
            "output_sum":  int(tok_row["sout"] or 0),
            "input_avg":   round(float(tok_row["ain"]  or 0), 1),
            "output_avg":  round(float(tok_row["aout"] or 0), 1),
        },
        "feedback":       {"positive": fb_pos, "negative": fb_neg},
        "daily_series":   [{"date": r["d"], "count": r["c"]} for r in daily],
    }


# ── 2) 직무별 ────────────────────────────────────────────────
def _mode(rows: list, key: str) -> Optional[str]:
    """list[Row]에서 가장 흔한 값 (None 제외)."""
    counts = {}
    for r in rows:
        v = r[key]
        if v is None or v == "":
            continue
        counts[v] = counts.get(v, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def stats_by_role(days: int = 30) -> list:
    """직무별 질문수·사용자수·평균 토큰 + 가장 많은 intent/metric/scope.
    user_role IS NULL 행은 제외."""
    since = _since(days)
    df, params = _date_filter(since)
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT user_role, intent, scope, metric, "
            f"       input_tokens, output_tokens, user_id "
            f"FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} user_role IS NOT NULL",
            params
        ).fetchall()
    # 그룹핑 in-memory (행 수가 적으므로 안전)
    by_role: dict = {}
    for r in rows:
        role = r["user_role"]
        by_role.setdefault(role, []).append(r)
    out = []
    for role, grp in by_role.items():
        users = {r["user_id"] for r in grp if r["user_id"] is not None}
        tok_in  = [r["input_tokens"]  for r in grp if r["input_tokens"]  is not None]
        tok_out = [r["output_tokens"] for r in grp if r["output_tokens"] is not None]
        avg_in  = round(sum(tok_in)/len(tok_in),  1) if tok_in  else 0
        avg_out = round(sum(tok_out)/len(tok_out),1) if tok_out else 0
        out.append({
            "role":       role,
            "queries":    len(grp),
            "users":      len(users),
            "top_intent": _mode(grp, "intent"),
            "top_metric": _mode(grp, "metric"),
            "top_scope":  _mode(grp, "scope"),
            "avg_input_tokens":  avg_in,
            "avg_output_tokens": avg_out,
        })
    out.sort(key=lambda x: x["queries"], reverse=True)
    return out


# ── 3) 사용자별 ──────────────────────────────────────────────
def stats_by_user(days: int = 30, limit: int = 50) -> list:
    """사용자 × 질문수/토큰/피드백/마지막활동/대표 주제. user_role IS NOT NULL 우선."""
    since = _since(days)
    with get_conn() as conn:
        # 사용자 기본 집계 — 직무는 '현재 직무'(users.role) 우선.
        # MAX(user_role)은 과거 질의 role들의 유니코드 최대값이라 사실상 무작위였음(예: '제작' 오표기).
        # 계정 미연결(레거시/IP) 행만 질의 당시 role로 폴백.
        _where = "WHERE q.created_at >= ? " if since else ""
        _params = [since] if since else []
        rows = conn.execute(
            f"SELECT q.user_id, "
            f"       COUNT(*)                                AS queries, "
            f"       MAX(q.user_name)                        AS user_name, "
            f"       COALESCE(MAX(u.role), MAX(q.user_role)) AS role, "
            f"       COALESCE(SUM(q.input_tokens),0)         AS tok_in, "
            f"       COALESCE(SUM(q.output_tokens),0)        AS tok_out, "
            f"       SUM(CASE WHEN q.feedback=1  THEN 1 ELSE 0 END) AS fb_pos, "
            f"       SUM(CASE WHEN q.feedback=-1 THEN 1 ELSE 0 END) AS fb_neg, "
            f"       MAX(q.created_at)                       AS last_active "
            f"FROM query_history q "
            f"LEFT JOIN users u ON q.user_id GLOB '[0-9]*' AND u.id = CAST(q.user_id AS INTEGER) "
            f"{_where}"
            f"GROUP BY q.user_id "
            f"ORDER BY (CASE WHEN MAX(q.user_role) IS NULL THEN 1 ELSE 0 END), "
            f"         COUNT(*) DESC "
            f"LIMIT ?",
            _params + [limit]
        ).fetchall()
        # 사용자별 top_topic_key 별도 조회
        top_by_user: dict = {}
        for r in rows:
            uid = r["user_id"]
            tr = conn.execute(
                f"SELECT topic_key, COUNT(*) AS c "
                f"FROM query_history "
                f"WHERE user_id = ? AND topic_key IS NOT NULL"
                f"{(' AND created_at >= ?' if since else '')}"
                f" GROUP BY topic_key ORDER BY c DESC LIMIT 1",
                [uid] + ([since] if since else [])
            ).fetchone()
            if tr:
                top_by_user[uid] = tr["topic_key"]
    return [
        {
            "user_id":      r["user_id"],
            "user_name":    r["user_name"] or "—",
            "role":         r["role"],
            "queries":      r["queries"],
            "tokens_in":    int(r["tok_in"]  or 0),
            "tokens_out":   int(r["tok_out"] or 0),
            "feedback_pos": int(r["fb_pos"]  or 0),
            "feedback_neg": int(r["fb_neg"]  or 0),
            "last_active":  r["last_active"],
            "top_topic":    top_by_user.get(r["user_id"]),
        }
        for r in rows
    ]


# ── 4) 인기 주제 ────────────────────────────────────────────
def stats_topics(days: int = 30, limit: int = 20) -> dict:
    """topic_key 빈도 TOP + question 텍스트 빈도 TOP."""
    since = _since(days)
    df, params = _date_filter(since)
    with get_conn() as conn:
        # by topic_key
        topic_rows = conn.execute(
            f"SELECT topic_key, COUNT(*) AS c, "
            f"       MAX(intent) AS intent, MAX(scope) AS scope, MAX(metric) AS metric, "
            f"       MAX(created_at) AS last_asked "
            f"FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} topic_key IS NOT NULL "
            f"GROUP BY topic_key ORDER BY c DESC LIMIT ?",
            params + [limit]
        ).fetchall()
        # by question 텍스트
        q_rows = conn.execute(
            f"SELECT question, COUNT(*) AS c, MAX(created_at) AS last_asked "
            f"FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} intent IS NOT NULL "
            f"GROUP BY question ORDER BY c DESC LIMIT ?",
            params + [limit]
        ).fetchall()
    return {
        "by_topic_key": [
            {"topic_key": r["topic_key"], "count": r["c"],
             "intent": r["intent"], "scope": r["scope"], "metric": r["metric"],
             "last_asked": r["last_asked"]}
            for r in topic_rows
        ],
        "by_question": [
            {"question": r["question"], "count": r["c"], "last_asked": r["last_asked"]}
            for r in q_rows
        ],
    }


# ── 5) 직무 × 지표/scope 매트릭스 (히트맵 입력) ───────────
def stats_role_metric_matrix(days: int = 30, dimension: str = "metric",
                             top_n: int = 10) -> dict:
    """직무(행) × 지표 또는 scope(열) 빈도 매트릭스.
    행은 role_profiles.json active 8종 전체 (빈 행도 insight).
    열은 빈도 상위 top_n.

    Returns:
        {
          'roles':       list[str],     # 8개 active role
          'cols':        list[str],     # top_n 지표/scope
          'cells':       list[list[int]], # roles × cols
          'row_totals':  list[int],
          'col_totals':  list[int],
          'grand_total': int,
          'dimension':   'metric' | 'scope',
        }
    """
    if dimension not in ("metric", "scope"):
        dimension = "metric"
    since = _since(days)
    df, params = _date_filter(since)

    # active role 목록 (role_profiles.json 기반, 안전 fallback)
    try:
        roles = [p["role"] for p in list_active_profiles()]
    except Exception:
        roles = []
    if not roles:
        roles = ['제작','편성','서비스운영','CP','플랫폼전략','데이터','총괄관리','마케팅(광고·협찬)']

    with get_conn() as conn:
        # 1) 상위 N개 col 선정 (모든 직무 합산 빈도)
        col_rows = conn.execute(
            f"SELECT {dimension} AS col, COUNT(*) AS c FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} {dimension} IS NOT NULL "
            f"AND user_role IS NOT NULL AND intent IS NOT NULL "
            f"GROUP BY {dimension} ORDER BY c DESC LIMIT ?",
            params + [top_n]
        ).fetchall()
        cols = [r["col"] for r in col_rows]
        # 2) role × col 셀
        cell_rows = conn.execute(
            f"SELECT user_role, {dimension} AS col, COUNT(*) AS c FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} {dimension} IS NOT NULL "
            f"AND user_role IS NOT NULL AND intent IS NOT NULL "
            f"GROUP BY user_role, {dimension}",
            params
        ).fetchall()

    # 매트릭스 구성
    role_idx = {r: i for i, r in enumerate(roles)}
    col_idx  = {c: i for i, c in enumerate(cols)}
    cells = [[0] * len(cols) for _ in roles]
    for r in cell_rows:
        ri = role_idx.get(r["user_role"])
        ci = col_idx.get(r["col"])
        if ri is None or ci is None:
            continue
        cells[ri][ci] = r["c"]
    row_totals = [sum(row) for row in cells]
    col_totals = [sum(cells[ri][ci] for ri in range(len(roles))) for ci in range(len(cols))]
    return {
        "dimension":   dimension,
        "days":        days,
        "roles":       roles,
        "cols":        cols,
        "cells":       cells,
        "row_totals":  row_totals,
        "col_totals":  col_totals,
        "grand_total": sum(row_totals),
    }


# ── 6) 같은 직무 동료 인기 질문 (온보딩 source-3 재사용) ─────
def get_popular_by_role(role: str, limit: int = 5, days: int = 30,
                        exclude_user_id: Optional[str] = None) -> List[str]:
    """같은 직무 동료가 많이 본 질문 텍스트 리스트.
    exclude_user_id: 호출자 본인 user_id 제외 (보통 추천 받는 사람)."""
    if not role:
        return []
    since = _since(days)
    sql = ("SELECT question, COUNT(*) AS c FROM query_history "
           "WHERE user_role = ? AND intent IS NOT NULL ")
    params: list = [role]
    if since:
        sql += "AND created_at >= ? "
        params.append(since)
    if exclude_user_id:
        sql += "AND user_id <> ? "
        params.append(str(exclude_user_id))
    sql += "GROUP BY question ORDER BY c DESC LIMIT ?"
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [r["question"] for r in rows]


# ── 7) co-query 페어 (Phase 5) ─────────────────────────────
def co_query_pairs(days: int = 30, dimension: str = "metric",
                   min_support: int = 2, top_n: int = 30) -> list:
    """같은 사용자 + 같은 date에 함께 등장한 (a, b) 쌍 카운트.

    Args:
        dimension:   'metric' | 'scope' | 'mixed' (m:dau ↔ s:T00 혼합)
        min_support: 최소 동반 빈도 — 이하 쌍은 노이즈로 간주
        top_n:       반환 상위 쌍 수

    Returns:
        [{a, b, weight}]  — a < b 정렬, weight 내림차순
    """
    if dimension not in ("metric", "scope", "mixed"):
        dimension = "metric"
    since = _since(days)
    df, params = _date_filter(since)

    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT user_id, date(created_at) AS d, metric, scope FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} intent IS NOT NULL",
            params
        ).fetchall()

    # (user_id, date) → 그날 함께 등장한 dimension value set
    groups: dict = {}
    for r in rows:
        key = (r["user_id"], r["d"])
        if dimension == "metric":
            v = r["metric"]
            if v: groups.setdefault(key, set()).add(v)
        elif dimension == "scope":
            v = r["scope"]
            if v: groups.setdefault(key, set()).add(v)
        else:  # mixed — 두 종류 노드 혼합 (prefix 부여)
            g = groups.setdefault(key, set())
            if r["metric"]: g.add(f"m:{r['metric']}")
            if r["scope"]:  g.add(f"s:{r['scope']}")

    # 모든 (a, b) 페어 카운트
    pair_counts: dict = {}
    for values in groups.values():
        items = sorted(values)
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                k = (items[i], items[j])
                pair_counts[k] = pair_counts.get(k, 0) + 1

    pairs = [{"a": a, "b": b, "weight": w}
             for (a, b), w in pair_counts.items() if w >= min_support]
    pairs.sort(key=lambda p: -p["weight"])
    return pairs[:top_n]


# ── 8) 사용자-질의 관계 그래프 빌더 ────────────────────────
def build_graph(days: int = 30, max_users: int = 30,
                max_metrics: int = 12, max_scopes: int = 12) -> dict:
    """관리자용 관계 그래프 데이터.

    노드 종류: user | role | metric | scope
    엣지 종류:
      - membership : user → role
      - interest   : user → metric/scope (weight=count)
      - co_query   : metric — metric, scope — scope (weight=동반 빈도)
      - similar    : user — user (weight=cosine, 같은 role 사용자 ≥3명일 때만)

    Returns:
      {nodes: [...], edges: [...], meta: {...}}
    """
    since = _since(days)
    df, params = _date_filter(since)

    with get_conn() as conn:
        # ── 사용자: fact 보유 distinct user_id 중 상위 max_users (질의수 기준) ──
        # 직무는 '현재 직무'(users.role) 우선 — 사용자별 통계와 동일 기준(라벨은 현재 소속).
        _uw = "WHERE qh.created_at >= ? AND " if since else "WHERE "
        user_rows = conn.execute(
            f"SELECT qh.user_id, MAX(qh.user_name) AS name, "
            f"       COALESCE(MAX(u.role), MAX(qh.user_role)) AS role, "
            f"       COUNT(*) AS q "
            f"FROM query_history qh "
            f"LEFT JOIN users u ON qh.user_id GLOB '[0-9]*' AND u.id = CAST(qh.user_id AS INTEGER) "
            f"{_uw}qh.intent IS NOT NULL AND qh.user_role IS NOT NULL "
            f"AND qh.user_id IS NOT NULL "
            f"GROUP BY qh.user_id ORDER BY q DESC LIMIT ?",
            ([since] if since else []) + [max_users]
        ).fetchall()
        users = [{"user_id": str(r["user_id"]), "name": r["name"] or str(r["user_id"]),
                  "role": r["role"], "q": r["q"]} for r in user_rows]

        # ── 상위 지표·대상 ──
        metric_rows = conn.execute(
            f"SELECT metric, COUNT(*) AS c FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} intent IS NOT NULL AND metric IS NOT NULL "
            f"GROUP BY metric ORDER BY c DESC LIMIT ?",
            params + [max_metrics]
        ).fetchall()
        metrics = [(r["metric"], r["c"]) for r in metric_rows]

        scope_rows = conn.execute(
            f"SELECT scope, COUNT(*) AS c FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} intent IS NOT NULL AND scope IS NOT NULL "
            f"GROUP BY scope ORDER BY c DESC LIMIT ?",
            params + [max_scopes]
        ).fetchall()
        scopes = [(r["scope"], r["c"]) for r in scope_rows]

        # ── user × metric / user × scope 빈도 ──
        uid_set     = {u["user_id"] for u in users}
        metric_set  = {m for m, _ in metrics}
        scope_set   = {s for s, _ in scopes}
        edge_rows = conn.execute(
            f"SELECT user_id, metric, scope FROM query_history "
            f"{df}{'AND' if df else 'WHERE'} intent IS NOT NULL "
            f"AND user_id IS NOT NULL AND user_role IS NOT NULL",
            params
        ).fetchall()

    # 노드 빌드 (prefix로 id 충돌 회피)
    nodes: list = []
    # role 노드 (active 8종)
    try:
        active_roles = [p["role"] for p in list_active_profiles()]
    except Exception:
        active_roles = ['제작','편성','서비스운영','CP','플랫폼전략','데이터','총괄관리','마케팅(광고·협찬)']
    # 실제 그래프에 등장하는 role만 노드로 포함 (사용자가 있는 role)
    role_used = {u["role"] for u in users if u["role"]}
    for role in active_roles:
        if role in role_used:
            nodes.append({"id": f"r:{role}", "type": "role", "label": role,
                          "weight": sum(u["q"] for u in users if u["role"] == role)})
    for u in users:
        nodes.append({"id": f"u:{u['user_id']}", "type": "user",
                      "label": u["name"], "role": u["role"], "weight": u["q"]})
    for m, c in metrics:
        nodes.append({"id": f"m:{m}", "type": "metric", "label": m, "weight": c})
    for s, c in scopes:
        nodes.append({"id": f"s:{s}", "type": "scope", "label": s, "weight": c})

    # 엣지 빌드
    edges: list = []
    # membership: user → role
    for u in users:
        if u["role"] in role_used:
            edges.append({"source": f"u:{u['user_id']}", "target": f"r:{u['role']}",
                          "type": "membership", "weight": 1})

    # interest: user → metric/scope (집계)
    um_count: dict = {}
    us_count: dict = {}
    for r in edge_rows:
        uid = str(r["user_id"])
        if uid not in uid_set:
            continue
        if r["metric"] and r["metric"] in metric_set:
            um_count[(uid, r["metric"])] = um_count.get((uid, r["metric"]), 0) + 1
        if r["scope"] and r["scope"] in scope_set:
            us_count[(uid, r["scope"])] = us_count.get((uid, r["scope"]), 0) + 1
    for (uid, m), w in um_count.items():
        edges.append({"source": f"u:{uid}", "target": f"m:{m}",
                      "type": "interest", "weight": w})
    for (uid, s), w in us_count.items():
        edges.append({"source": f"u:{uid}", "target": f"s:{s}",
                      "type": "interest", "weight": w})

    # co_query: metric—metric, scope—scope (별도 분리)
    for p in co_query_pairs(days=days, dimension="metric", min_support=1, top_n=30):
        if p["a"] in metric_set and p["b"] in metric_set and p["weight"] >= 1:
            edges.append({"source": f"m:{p['a']}", "target": f"m:{p['b']}",
                          "type": "co_query", "weight": p["weight"]})
    for p in co_query_pairs(days=days, dimension="scope", min_support=1, top_n=30):
        if p["a"] in scope_set and p["b"] in scope_set and p["weight"] >= 1:
            edges.append({"source": f"s:{p['a']}", "target": f"s:{p['b']}",
                          "type": "co_query", "weight": p["weight"]})

    # similar: user—user (같은 role 사용자 ≥3명일 때만, cosine ≥ 0.2)
    try:
        from raas_recommend import _cosine, _user_vectors_by_role
        # role별로 그룹화해서 그 안에서 페어 비교
        role_users: dict = {}
        for u in users:
            role_users.setdefault(u["role"], []).append(u["user_id"])
        for role, uids in role_users.items():
            if not role or len(uids) < 3:
                continue
            vecs = _user_vectors_by_role(role)  # 같은 role 모든 user 벡터
            for i in range(len(uids)):
                for j in range(i + 1, len(uids)):
                    a, b = uids[i], uids[j]
                    if a not in vecs or b not in vecs:
                        continue
                    s = _cosine(vecs[a], vecs[b])
                    if s >= 0.2:
                        edges.append({"source": f"u:{a}", "target": f"u:{b}",
                                      "type": "similar", "weight": round(s, 3)})
    except Exception:
        pass

    return {
        "nodes": nodes,
        "edges": edges,
        "meta": {
            "days": days,
            "n_users":   len(users),
            "n_metrics": len(metrics),
            "n_scopes":  len(scopes),
            "n_roles":   sum(1 for n in nodes if n["type"] == "role"),
            "n_edges":   len(edges),
        },
    }


# ── 9) 개인 관심 맵 ────────────────────────────────────────
def interest_map(user_id, days: int = 30) -> dict:
    """본인 관심 분포 + 사각지대 + (선택) 동료 평균 대비.

    Returns:
      {
        user_id, role, days,
        total_queries,
        by_metric: [{metric, count, pct}],
        by_scope:  [{scope, count, pct}],
        top_topics: [{topic_key, count}],
        blind_spots: [{question, source, reason}],
        peer_compare: {...} | null,
      }
    """
    if user_id is None:
        return {"user_id": None, "total_queries": 0,
                "by_metric": [], "by_scope": [], "top_topics": [],
                "blind_spots": [], "peer_compare": None}
    uid = str(user_id)
    since = _since(days)

    with get_conn() as conn:
        # 본인 role
        role_row = conn.execute(
            "SELECT user_role FROM query_history "
            "WHERE user_id = ? AND user_role IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 1", (uid,)
        ).fetchone()
        if role_row and role_row["user_role"]:
            role = role_row["user_role"]
        else:
            row = conn.execute(
                "SELECT u.role FROM users u "
                "JOIN sessions s ON s.user_id = u.id "
                "WHERE u.id = ? LIMIT 1",
                (int(uid),) if str(uid).isdigit() else (uid,)
            ).fetchone()
            role = row["role"] if row else None

        # 본인 fact 집계
        df_clause = "AND created_at >= ?" if since else ""
        date_params = [since] if since else []
        total = conn.execute(
            f"SELECT COUNT(*) FROM query_history "
            f"WHERE user_id = ? AND intent IS NOT NULL {df_clause}",
            [uid] + date_params
        ).fetchone()[0]

        metric_rows = conn.execute(
            f"SELECT metric, COUNT(*) AS c FROM query_history "
            f"WHERE user_id = ? AND intent IS NOT NULL AND metric IS NOT NULL {df_clause} "
            f"GROUP BY metric ORDER BY c DESC LIMIT 10",
            [uid] + date_params
        ).fetchall()
        scope_rows = conn.execute(
            f"SELECT scope, COUNT(*) AS c FROM query_history "
            f"WHERE user_id = ? AND intent IS NOT NULL AND scope IS NOT NULL {df_clause} "
            f"GROUP BY scope ORDER BY c DESC LIMIT 10",
            [uid] + date_params
        ).fetchall()
        topic_rows = conn.execute(
            f"SELECT topic_key, COUNT(*) AS c FROM query_history "
            f"WHERE user_id = ? AND topic_key IS NOT NULL {df_clause} "
            f"GROUP BY topic_key ORDER BY c DESC LIMIT 5",
            [uid] + date_params
        ).fetchall()

    by_metric = [{"metric": r["metric"], "count": r["c"],
                  "pct": round(r["c"] / total * 100, 1) if total else 0}
                 for r in metric_rows]
    by_scope  = [{"scope": r["scope"], "count": r["c"],
                  "pct": round(r["c"] / total * 100, 1) if total else 0}
                 for r in scope_rows]
    top_topics = [{"topic_key": r["topic_key"], "count": r["c"]} for r in topic_rows]

    # 사각지대 — recommend_for_user 재사용 (gap-template + gap-metric)
    blind_spots: list = []
    try:
        from raas_recommend import recommend_for_user
        recs = recommend_for_user(uid, role, limit=6)
        # gap·gap-metric만 (peer/similar는 사각지대 아님)
        blind_spots = [r for r in recs if r.get("source") in ("gap", "gap-metric")]
    except Exception:
        pass

    # 동료 평균 대비 — 같은 role 사용자 ≥3명 있을 때만
    peer_compare = None
    try:
        if role:
            with get_conn() as conn:
                peer_count = conn.execute(
                    "SELECT COUNT(DISTINCT user_id) FROM query_history "
                    "WHERE user_role = ? AND intent IS NOT NULL AND user_id <> ?",
                    (role, uid)
                ).fetchone()[0] or 0
                if peer_count >= 3:
                    peer_total = conn.execute(
                        f"SELECT COUNT(*) FROM query_history "
                        f"WHERE user_role = ? AND intent IS NOT NULL AND user_id <> ? {df_clause}",
                        [role, uid] + date_params
                    ).fetchone()[0] or 0
                    peer_top = conn.execute(
                        f"SELECT metric, COUNT(*) AS c FROM query_history "
                        f"WHERE user_role = ? AND intent IS NOT NULL AND metric IS NOT NULL "
                        f"AND user_id <> ? {df_clause} "
                        f"GROUP BY metric ORDER BY c DESC LIMIT 5",
                        [role, uid] + date_params
                    ).fetchall()
                    peer_compare = {
                        "same_role_users": peer_count,
                        "peer_avg_queries": round(peer_total / peer_count, 1) if peer_count else 0,
                        "my_queries":       total,
                        "peer_top_metrics": [{"metric": r["metric"], "count": r["c"]} for r in peer_top],
                    }
    except Exception:
        pass

    return {
        "user_id":      user_id,
        "role":         role,
        "days":         days,
        "total_queries": total,
        "by_metric":    by_metric,
        "by_scope":     by_scope,
        "top_topics":   top_topics,
        "blind_spots":  blind_spots,
        "peer_compare": peer_compare,
    }
