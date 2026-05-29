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
        for col, typedef in [("ip", "TEXT"), ("user_name", "TEXT")]:
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_created "
            "ON query_history (user_id, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_created "
            "ON query_history (created_at DESC)"
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
               ip: str = None, user_name: str = None) -> int:
    """질의 1건 DB 저장. JSONL 로그도 함께 기록."""
    chart_json = json.dumps(chart_data, ensure_ascii=False) if chart_data is not None else None
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO query_history (user_id, question, answer, chart_data, ip, user_name) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, question, answer, chart_json, ip, user_name)
        )
        row_id = cur.lastrowid

    _append_log(row_id, user_id, question, answer, ip, user_name)
    return row_id


def _append_log(row_id, user_id, question, answer, ip, user_name):
    """data/raas_query_log.jsonl 에 1줄 append. 실패해도 무시."""
    try:
        LOG_PATH.parent.mkdir(exist_ok=True)
        entry = {
            "ts":        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "id":        row_id,
            "user_id":   user_id,
            "user_name": user_name or ip or "unknown",
            "ip":        ip,
            "question":  question,
            "answer_len": len(answer),
        }
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def get_history(user_id: str, limit: int = 20) -> list:
    """user_id의 최근 질의 N건. 최신순."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, question, answer, chart_data, created_at
               FROM query_history
               WHERE user_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (user_id, limit)
        ).fetchall()
    return [
        {
            "id":         r["id"],
            "question":   r["question"],
            "answer":     r["answer"],
            "chart_data": json.loads(r["chart_data"]) if r["chart_data"] else None,
            "created_at": r["created_at"],
        }
        for r in rows
    ]


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
