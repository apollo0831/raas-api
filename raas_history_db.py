"""
RAAS Query History DB
SQLite 기반 질의 이력 저장 및 인기 질의 집계
IP → 사용자 이름 매핑 포함
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager
from typing import Optional

DB_PATH  = Path(__file__).parent / "raas_history.db"
LOG_PATH = Path(__file__).parent / "data" / "raas_query_log.jsonl"


def init_db():
    """앱 시작 시 1회 호출. 테이블 생성 및 마이그레이션 (idempotent)."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    TEXT      NOT NULL,
                question   TEXT      NOT NULL,
                answer     TEXT      NOT NULL,
                chart_data TEXT,
                ip         TEXT,
                user_name  TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 기존 DB 마이그레이션: 컬럼이 없으면 추가
        for col, typedef in [
            ("ip", "TEXT"), ("user_name", "TEXT"), ("feedback", "INTEGER"),
            ("input_tokens", "INTEGER"), ("output_tokens", "INTEGER"),
            ("cache_creation_tokens", "INTEGER"), ("cache_read_tokens", "INTEGER"),
            # 질의 노드화 ETL (Phase 3-1) — classify_intent + 후처리 결과 적재
            ("intent",        "TEXT"),
            ("scope",         "TEXT"),
            ("scope_keyword", "TEXT"),
            ("metric",        "TEXT"),
            ("metrics_json",  "TEXT"),   # list → JSON
            ("topic_key",     "TEXT"),   # f"{intent}:{scope}:{metric}" 그룹핑/인기 집계용
        ]:
            try:
                conn.execute(f"ALTER TABLE query_history ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # 이미 존재하면 무시

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ip_users (
                ip         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # ── 계정 / 세션 (Part 2: 사용자 관리) ───────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                login_id     TEXT    UNIQUE NOT NULL,
                pw_hash      TEXT    NOT NULL,
                pw_salt      TEXT    NOT NULL,
                name         TEXT    NOT NULL,
                role         TEXT    NOT NULL,
                title        TEXT,
                status       TEXT    DEFAULT 'pending',
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_at  TIMESTAMP,
                approved_by  INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token       TEXT PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at  TIMESTAMP
            )
        """)
        # query_history에 user_role 추가 (직무별 인기 질문 집계용)
        try:
            conn.execute("ALTER TABLE query_history ADD COLUMN user_role TEXT")
        except Exception:
            pass
        # users에 관심 프로그램 추가 (JSON 배열로 저장: ["F06","L03",...])
        try:
            conn.execute("ALTER TABLE users ADD COLUMN my_programs TEXT")
        except Exception:
            pass
        # users에 시스템 관리자 권한 플래그 (role과 무관, ADMIN_LOGIN_IDS로만 부여)
        try:
            conn.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0")
        except Exception:
            pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_created "
            "ON query_history (user_id, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_created "
            "ON query_history (created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_user "
            "ON sessions (user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_status "
            "ON users (status)"
        )
        # 질의 노드화 ETL용 — 인기 집계·직무별 패턴 조회 가속
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_topic_key "
            "ON query_history (topic_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_intent "
            "ON query_history (intent)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_role_topic "
            "ON query_history (user_role, topic_key)"
        )


@contextmanager
def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ── IP 사용자 매핑 ─────────────────────────────────────

def resolve_user_name(ip: str) -> str:
    """IP → 등록된 이름 반환. 미등록 시 IP 그대로."""
    if not ip:
        return "unknown"
    with get_conn() as conn:
        row = conn.execute("SELECT name FROM ip_users WHERE ip = ?", (ip,)).fetchone()
    return row["name"] if row else ip


def set_ip_user(ip: str, name: str):
    """IP → 이름 매핑 등록/수정."""
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO ip_users (ip, name, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(ip) DO UPDATE SET name=excluded.name, updated_at=CURRENT_TIMESTAMP",
            (ip, name)
        )


def delete_ip_user(ip: str):
    """IP 매핑 삭제."""
    with get_conn() as conn:
        conn.execute("DELETE FROM ip_users WHERE ip = ?", (ip,))


def get_all_ip_users() -> list:
    """등록된 IP → 이름 매핑 전체 반환."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ip, name, updated_at FROM ip_users ORDER BY updated_at DESC"
        ).fetchall()
    return [{"ip": r["ip"], "name": r["name"], "updated_at": r["updated_at"]} for r in rows]


# ── 질의 저장 / 조회 ──────────────────────────────────

def save_query(user_id: str, question: str, answer: str,
               chart_data: Optional[dict] = None,
               ip: str = None, user_name: str = None,
               user_role: str = None,
               input_tokens: int = None, output_tokens: int = None,
               cache_creation_tokens: int = None, cache_read_tokens: int = None,
               # 질의 노드화 ETL (Phase 3-1) — classify_intent + 후처리 결과
               intent: str = None, scope: str = None, scope_keyword: str = None,
               metric: str = None, metrics: Optional[list] = None,
               topic_key: str = None) -> int:
    """질의 1건 DB 저장. JSONL 로그도 함께 기록.
    intent/scope/metric 등 fact 인자는 모두 optional — None이어도 정상 저장."""
    chart_json   = json.dumps(chart_data, ensure_ascii=False) if chart_data is not None else None
    metrics_json = json.dumps(metrics,    ensure_ascii=False) if metrics    else None
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO query_history "
            "(user_id, question, answer, chart_data, ip, user_name, user_role, "
            " input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, "
            " intent, scope, scope_keyword, metric, metrics_json, topic_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, question, answer, chart_json, ip, user_name, user_role,
             input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens,
             intent, scope, scope_keyword, metric, metrics_json, topic_key)
        )
        row_id = cur.lastrowid

    _append_log(row_id, user_id, question, answer, ip, user_name,
                input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens,
                intent=intent, scope=scope, scope_keyword=scope_keyword,
                metric=metric, metrics=metrics, topic_key=topic_key, user_role=user_role)
    return row_id


def _append_log(row_id, user_id, question, answer, ip, user_name,
                input_tokens=None, output_tokens=None,
                cache_creation_tokens=None, cache_read_tokens=None,
                intent=None, scope=None, scope_keyword=None,
                metric=None, metrics=None, topic_key=None, user_role=None):
    """data/raas_query_log.jsonl 에 1줄 append. 실패해도 무시."""
    try:
        LOG_PATH.parent.mkdir(exist_ok=True)
        entry = {
            "ts":                   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "id":                   row_id,
            "user_id":              user_id,
            "user_name":            user_name or ip or "unknown",
            "user_role":            user_role,
            "ip":                   ip,
            "question":             question,
            "answer_len":           len(answer),
            "input_tokens":         input_tokens,
            "output_tokens":        output_tokens,
            "cache_creation_tokens": cache_creation_tokens,
            "cache_read_tokens":    cache_read_tokens,
            "intent":               intent,
            "scope":                scope,
            "scope_keyword":        scope_keyword,
            "metric":               metric,
            "metrics":              metrics,
            "topic_key":            topic_key,
        }
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def save_feedback(query_id: int, feedback: int) -> bool:
    """질의 1건에 피드백 저장. feedback: 1(좋음) / -1(나쁨)."""
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE query_history SET feedback = ? WHERE id = ?",
            (feedback, query_id)
        )
    return cur.rowcount > 0


def get_history(user_id: str, limit: int = 20) -> list:
    """user_id의 최근 질의 N건. 최신순."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, question, answer, chart_data, feedback,
                      input_tokens, output_tokens, created_at
               FROM query_history
               WHERE user_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (user_id, limit)
        ).fetchall()
    return [
        {
            "id":            r["id"],
            "question":      r["question"],
            "answer":        r["answer"],
            "chart_data":    json.loads(r["chart_data"]) if r["chart_data"] else None,
            "feedback":      r["feedback"],
            "input_tokens":  r["input_tokens"],
            "output_tokens": r["output_tokens"],
            "created_at":    r["created_at"],
        }
        for r in rows
    ]


def get_all_history(limit: int = 50, offset: int = 0, days: int = 0) -> dict:
    """전체 사용자 질의 이력. 최신순, 페이지네이션."""
    where = ""
    params_count: list = []
    params_rows:  list = []
    if days > 0:
        since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        where = "WHERE created_at >= ?"
        params_count = [since]
        params_rows  = [since, limit, offset]
    else:
        params_rows = [limit, offset]

    with get_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM query_history {where}", params_count).fetchone()[0]
        rows  = conn.execute(
            f"""SELECT id, user_id, user_name, ip, question, answer, chart_data,
                       feedback, input_tokens, output_tokens, created_at
                FROM query_history {where}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?""",
            params_rows
        ).fetchall()

    return {
        "total": total,
        "items": [
            {
                "id":            r["id"],
                "user_id":       r["user_id"],
                "user_name":     r["user_name"] or r["ip"] or "unknown",
                "question":      r["question"],
                "answer":        r["answer"],
                "chart_data":    json.loads(r["chart_data"]) if r["chart_data"] else None,
                "feedback":      r["feedback"],
                "input_tokens":  r["input_tokens"],
                "output_tokens": r["output_tokens"],
                "created_at":    r["created_at"],
            }
            for r in rows
        ],
    }


def get_popular(limit: int = 5, days: int = 7) -> list:
    """최근 N일 내 question 그룹화 후 빈도순 상위 limit개."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT question, COUNT(*) AS count, MAX(created_at) AS last_asked
               FROM query_history
               WHERE created_at >= ?
               GROUP BY question
               ORDER BY count DESC
               LIMIT ?""",
            (since, limit)
        ).fetchall()
    return [
        {"question": r["question"], "count": r["count"], "last_asked": r["last_asked"]}
        for r in rows
    ]
