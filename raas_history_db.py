"""
RAAS Query History DB
SQLite 기반 질의 이력 저장 및 인기 질의 집계
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager
from typing import Optional

DB_PATH = Path(__file__).parent / "raas_history.db"


def init_db():
    """앱 시작 시 1회 호출. 테이블이 없으면 생성 (idempotent)."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    TEXT      NOT NULL,
                question   TEXT      NOT NULL,
                answer     TEXT      NOT NULL,
                chart_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    """with 컨텍스트 매니저로 connection 관리, 자동 commit/close."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def save_query(user_id: str, question: str, answer: str,
               chart_data: Optional[dict] = None) -> int:
    """질의 1건 저장, id 리턴."""
    chart_json = json.dumps(chart_data, ensure_ascii=False) if chart_data is not None else None
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO query_history (user_id, question, answer, chart_data) "
            "VALUES (?, ?, ?, ?)",
            (user_id, question, answer, chart_json)
        )
        return cur.lastrowid


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
            "id": r["id"],
            "question": r["question"],
            "answer": r["answer"],
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
