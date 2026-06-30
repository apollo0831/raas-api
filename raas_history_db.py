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
            ("source",        "TEXT"),   # 'general'(일반 질의) | 'storyline'(스토리라인 칩)
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
        # users에 담당 채널 (CP 전용, D-015 옵션 A) — '파워FM' / '러브FM' / NULL
        try:
            conn.execute("ALTER TABLE users ADD COLUMN channel TEXT")
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

        # ── 스토리라인 내용 캐시 (이력 DB 재설계 §B) ───────────────
        # analysis_key = hash(slot_type, channel, program, metric, period, window, data_date)
        # data_date를 키에 포함 → 다음날 06:50 데이터 갱신 시 키 미적중으로 자동 만료.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_cache (
                analysis_key TEXT PRIMARY KEY,
                data_date    TEXT NOT NULL,
                slot_type    TEXT,
                program_code TEXT,
                metric       TEXT,
                period       TEXT,
                window       TEXT,
                payload      TEXT NOT NULL,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hits         INTEGER DEFAULT 0,
                gen_tokens   INTEGER
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ac_date ON analysis_cache (data_date)"
        )

        # ── 스토리라인 경로 로그 (이력 DB 재설계 §A — 2단계) ───────
        # 전이(edge) 1건 = 1행. 세션(session_id) 단위로 분석 이동경로 복원.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS storyline_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT NOT NULL,
                seq         INTEGER NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                user_id     TEXT,
                user_role   TEXT,
                event_type  TEXT NOT NULL,        -- enter|chip|toggle|freetext|exit|export
                slot_from   TEXT,
                slot_to     TEXT,
                chip_intent TEXT,
                chip_label  TEXT,
                program_code TEXT,
                program_name TEXT,
                channel_code TEXT,
                metric      TEXT,
                period      TEXT,
                window      TEXT,
                end_reason  TEXT,                 -- export|free_query|timeout
                analysis_key TEXT,
                cache_hit   INTEGER,
                latency_ms  INTEGER,
                output_tokens INTEGER
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_se_session ON storyline_events (session_id, seq)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_se_user_time ON storyline_events (user_id, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_se_role_slot ON storyline_events (user_role, slot_from, slot_to)"
        )
        # 기존 테이블 마이그레이션 — chip_label 컬럼(첫 진입 칩 이름 표시용)
        try:
            conn.execute("ALTER TABLE storyline_events ADD COLUMN chip_label TEXT")
        except Exception:
            pass

        # ── 지식 개선 루프 (docs/knowledge_loop_design.md §4) ─────────
        # 지식 오버레이 — 온톨로지를 직접 수정하지 않고 '엔티티별 구조화 지식 항목'으로 적재.
        #   grounding이 대상 엔티티의 항목을 읽어 LLM context에 주입(읽기시 병합).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                scope        TEXT,    -- candidate(본인) | approved(공유)
                contributor_id TEXT,
                type         TEXT,    -- metric_definition|field_meaning|program_note|guest_policy|corner_note|decomposition_hint|fact
                target_kind  TEXT,    -- metric|field|program|channel|global
                target_id    TEXT,
                content      TEXT,
                op           TEXT,    -- add|edit
                status       TEXT,    -- draft|submitted|approved|rejected
                improvement_id INTEGER,
                reviewed_by  TEXT, reviewed_at TIMESTAMP,
                promoted_at  TIMESTAMP    -- TTL canonical 승격 시각(NULL=미승격)
            )
        """)
        # 기존 DB 마이그레이션 — promoted_at 컬럼(이미 있으면 무시)
        try:
            conn.execute("ALTER TABLE knowledge_items ADD COLUMN promoted_at TIMESTAMP")
        except Exception:
            pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ki_target ON knowledge_items (target_kind, target_id, scope)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ki_contrib ON knowledge_items (contributor_id, scope)")
        # 데이터 요청(요청형 — 스플렁크 필드 추가)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                contributor_id TEXT, kind TEXT, target_id TEXT,
                field_name TEXT, description TEXT, splunk_spl TEXT,
                status TEXT, processed_by TEXT, processed_at TIMESTAMP,
                improvement_id INTEGER
            )
        """)
        # 개선 시도(원답변↔개선답변 + 평가 + 상태)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS improvements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                contributor_id TEXT, source_query_id INTEGER,
                question TEXT, answer_original TEXT, answer_improved TEXT,
                contributions_json TEXT, user_verdict TEXT, judge_json TEXT,
                status TEXT, reviewed_by TEXT, reviewed_at TIMESTAMP
            )
        """)
        # 본인 데이터 업로드(소규모 표) — grounding candidate provider
        conn.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                contributor_id TEXT,
                target_kind TEXT, target_id TEXT,
                name TEXT, columns_json TEXT, rows_json TEXT,
                scope TEXT, status TEXT,
                reviewed_by TEXT, reviewed_at TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ud_target ON uploaded_data (target_kind, target_id, scope)")


@contextmanager
def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ── 스토리라인 내용 캐시 (이력 DB 재설계 §B) ─────────────────────
def cache_get(analysis_key: str):
    """캐시 적중 시 payload(JSON 문자열) 반환 + hits 증가. 미스면 None."""
    if not analysis_key:
        return None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT payload FROM analysis_cache WHERE analysis_key = ?",
            (analysis_key,)
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE analysis_cache SET hits = hits + 1 WHERE analysis_key = ?",
            (analysis_key,)
        )
        return row["payload"]


def cache_put(analysis_key: str, data_date: str, payload: str,
              slot_type: str = None, program_code: str = None,
              metric: str = None, period: str = None, window: str = None,
              gen_tokens: int = None) -> None:
    """계산 결과 payload(JSON 문자열) 적재. 같은 키면 갱신."""
    if not (analysis_key and data_date and payload):
        return
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO analysis_cache "
            "(analysis_key, data_date, slot_type, program_code, metric, period, window, "
            " payload, hits, gen_tokens) "
            "VALUES (?,?,?,?,?,?,?,?, "
            "  COALESCE((SELECT hits FROM analysis_cache WHERE analysis_key = ?), 0), ?)",
            (analysis_key, data_date, slot_type, program_code, metric, period, window,
             payload, analysis_key, gen_tokens)
        )


# ── 스토리라인 경로 로그 (이력 DB 재설계 §A) ─────────────────────
def log_event(session_id: str, event_type: str, *, user_id=None, user_role=None,
              slot_from=None, slot_to=None, chip_intent=None, chip_label=None,
              program_code=None, program_name=None, channel_code=None,
              metric=None, period=None, window=None, end_reason=None,
              analysis_key=None, cache_hit=None, latency_ms=None,
              output_tokens=None) -> int:
    """전이 1건 적재. seq는 세션 내 자동 증가. 실패해도 본 흐름 방해 안 함."""
    if not session_id or not event_type:
        return -1
    try:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), -1) + 1 AS nxt FROM storyline_events WHERE session_id = ?",
                (session_id,)
            ).fetchone()
            seq = row["nxt"] if row else 0
            cur = conn.execute(
                "INSERT INTO storyline_events "
                "(session_id, seq, user_id, user_role, event_type, slot_from, slot_to, "
                " chip_intent, chip_label, program_code, program_name, channel_code, metric, period, "
                " window, end_reason, analysis_key, cache_hit, latency_ms, output_tokens) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (session_id, seq, user_id, user_role, event_type, slot_from, slot_to,
                 chip_intent, chip_label, program_code, program_name, channel_code, metric, period,
                 window, end_reason, analysis_key,
                 (1 if cache_hit else 0) if cache_hit is not None else None,
                 latency_ms, output_tokens)
            )
            return cur.lastrowid
    except Exception as e:
        print(f"[storyline] log_event 실패(무시): {e}", flush=True)
        return -1


def get_session_events(session_id: str) -> list:
    """세션의 전이 목록(순서대로) — 이력 보기/여정 복원용."""
    if not session_id:
        return []
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM storyline_events WHERE session_id = ? ORDER BY seq",
            (session_id,)
        ).fetchall()
        return [dict(r) for r in rows]


# ── 지식 개선 루프 (docs/knowledge_loop_design.md) ───────────────────────────
def add_knowledge_item(contributor_id, type, target_kind, target_id, content,
                       op="add", scope="candidate", status="draft",
                       improvement_id=None) -> int:
    """지식 오버레이 항목 추가. 기본 scope=candidate(본인 재질의에만 반영)."""
    if not (type and content):
        return -1
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO knowledge_items "
            "(scope, contributor_id, type, target_kind, target_id, content, op, status, improvement_id) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (scope, str(contributor_id) if contributor_id else None, type,
             target_kind, target_id, content, op, status, improvement_id))
        return cur.lastrowid


def add_data_request(contributor_id, field_name, description, target_id=None,
                     splunk_spl=None, improvement_id=None) -> int:
    """요청형 — 스플렁크 필드 추가 요청. 상태=요청됨."""
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO data_requests "
            "(contributor_id, kind, target_id, field_name, description, splunk_spl, status, improvement_id) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (str(contributor_id) if contributor_id else None, "splunk_field", target_id,
             field_name, description, splunk_spl, "요청됨", improvement_id))
        return cur.lastrowid


def add_improvement(contributor_id, question, answer_original, answer_improved=None,
                    source_query_id=None, contributions_json=None, user_verdict=None,
                    judge_json=None, status="검토대기") -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO improvements "
            "(contributor_id, source_query_id, question, answer_original, answer_improved, "
            " contributions_json, user_verdict, judge_json, status) VALUES (?,?,?,?,?,?,?,?,?)",
            (str(contributor_id) if contributor_id else None, source_query_id, question,
             answer_original, answer_improved, contributions_json, user_verdict, judge_json, status))
        return cur.lastrowid


def set_improvement_verdict(improvement_id, verdict) -> bool:
    with get_conn() as conn:
        conn.execute("UPDATE improvements SET user_verdict = ? WHERE id = ?",
                     (verdict, improvement_id))
    return True


def list_improvements(user_id=None, status=None, limit=80) -> list:
    """개선 시도 목록 — 본인(user_id) 또는 전체(검토용). +기여자명."""
    where, params = [], []
    if user_id:
        where.append("i.contributor_id = ?"); params.append(str(user_id))
    if status:
        where.append("i.status = ?"); params.append(status)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(
            f"""SELECT i.*, (SELECT u.name FROM users u WHERE u.id = CAST(i.contributor_id AS INTEGER)) AS user_name
                FROM improvements i {w} ORDER BY i.created_at DESC LIMIT ?""",
            tuple(params)).fetchall()
        return [dict(r) for r in rows]


def list_data_requests(status=None, contributor_id=None, limit=100) -> list:
    where, params = [], []
    if status:
        where.append("status = ?"); params.append(status)
    if contributor_id:
        where.append("contributor_id = ?"); params.append(str(contributor_id))
    w = ("WHERE " + " AND ".join(where)) if where else ""
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM data_requests {w} ORDER BY created_at DESC LIMIT ?", tuple(params)).fetchall()
        return [dict(r) for r in rows]


def get_knowledge_items_by_ids(ids) -> list:
    if not ids:
        return []
    qs = ",".join("?" * len(ids))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM knowledge_items WHERE id IN ({qs})", tuple(ids)).fetchall()
        return [dict(r) for r in rows]


def review_improvement(improvement_id, action, reviewer) -> bool:
    """승인 시 그 개선이 적용한 candidate 지식 항목을 approved(공유)로 승격 → 전체 적용."""
    with get_conn() as conn:
        row = conn.execute("SELECT contributions_json FROM improvements WHERE id = ?",
                           (improvement_id,)).fetchone()
        if not row:
            return False
        if action == "approve":
            try:
                ids = json.loads(row["contributions_json"] or "[]")
            except Exception:
                ids = []
            for kid in ids:
                conn.execute(
                    "UPDATE knowledge_items SET scope='approved', status='approved', "
                    "reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?", (str(reviewer), kid))
            conn.execute("UPDATE improvements SET status='승인', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
                         (str(reviewer), improvement_id))
        else:
            conn.execute("UPDATE improvements SET status='반려', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
                         (str(reviewer), improvement_id))
    return True


def add_uploaded_data(contributor_id, target_kind, target_id, name, columns, rows,
                      scope="candidate") -> int:
    """소규모 표 데이터 저장(candidate). columns: [str], rows: [[...]]."""
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO uploaded_data "
            "(contributor_id, target_kind, target_id, name, columns_json, rows_json, scope, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (str(contributor_id), target_kind, target_id, name,
             json.dumps(columns, ensure_ascii=False), json.dumps(rows, ensure_ascii=False),
             scope, "draft"))
        return cur.lastrowid


def list_my_uploads(contributor_id, limit=50) -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, target_kind, target_id, name, columns_json, rows_json, scope, status, created_at "
            "FROM uploaded_data WHERE contributor_id=? AND status!='rejected' "
            "ORDER BY created_at DESC LIMIT ?", (str(contributor_id), limit)).fetchall()
        return [dict(r) for r in rows]


def retire_my_upload(upload_id, contributor_id) -> bool:
    with get_conn() as conn:
        conn.execute("UPDATE uploaded_data SET status='rejected' WHERE id=? AND contributor_id=?",
                     (upload_id, str(contributor_id)))
    return True


def get_uploaded_data(targets, contributor_id=None, include_candidate=False) -> list:
    """grounding 주입용 — targets [(kind,id)]에 매칭되는 업로드. approved(+본인 candidate)."""
    if not targets:
        return []
    conds, params = [], []
    for kind, tid in targets:
        if tid is None:
            conds.append("(target_kind=? AND target_id IS NULL)"); params.append(kind)
        else:
            conds.append("(target_kind=? AND target_id=?)"); params += [kind, tid]
    where_t = "(" + " OR ".join(conds) + ")"
    scope_or = ["scope='approved'"]
    if include_candidate and contributor_id is not None:
        scope_or.append("(scope='candidate' AND contributor_id=?)"); params_tail = [str(contributor_id)]
    else:
        params_tail = []
    sql = (f"SELECT * FROM uploaded_data WHERE {where_t} AND status!='rejected' "
           f"AND ({' OR '.join(scope_or)})")
    with get_conn() as conn:
        rows = conn.execute(sql, tuple(params + params_tail)).fetchall()
        return [dict(r) for r in rows]


def list_pending_uploads(limit=100) -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM uploaded_data WHERE scope='candidate' AND status!='rejected' "
            "ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


def approve_upload(upload_id, reviewer) -> bool:
    with get_conn() as conn:
        conn.execute("UPDATE uploaded_data SET scope='approved', status='approved', "
                     "reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
                     (str(reviewer), upload_id))
    return True


def list_my_knowledge(contributor_id, limit=100) -> list:
    """본인이 기여한 지식 항목(candidate+approved, rejected 제외) — 능동 기여 관리용."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, type, target_kind, target_id, content, scope, status, created_at
                 FROM knowledge_items
                WHERE contributor_id = ? AND status != 'rejected'
                ORDER BY created_at DESC LIMIT ?""",
            (str(contributor_id), limit)).fetchall()
        return [dict(r) for r in rows]


def retire_my_knowledge(item_id, contributor_id) -> bool:
    """본인 기여 지식 삭제(rejected). 소유자 확인."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE knowledge_items SET status='rejected' "
            "WHERE id=? AND contributor_id=?", (item_id, str(contributor_id)))
    return True


def list_approved_knowledge(limit=200) -> list:
    """승인된 공유 지식(본체) — grounding이 모든 답변에 주입하는 canonical 도메인 지식."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, type, target_kind, target_id, content, contributor_id,
                      reviewed_by, reviewed_at, created_at
                 FROM knowledge_items
                WHERE scope='approved' AND status='approved'
                ORDER BY target_kind, target_id, reviewed_at DESC
                LIMIT ?""", (limit,)).fetchall()
        return [dict(r) for r in rows]


def list_approved_for_promotion(limit=2000) -> list:
    """TTL 승격용 — 승인된 공유 지식 전체(canonical 미러 재생성 입력)."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, type, target_kind, target_id, content, contributor_id,
                      improvement_id, reviewed_at, promoted_at
                 FROM knowledge_items
                WHERE scope='approved' AND status='approved'
                ORDER BY id LIMIT ?""", (limit,)).fetchall()
        return [dict(r) for r in rows]


def mark_promoted(ids) -> int:
    """승격 완료 표시(promoted_at)."""
    if not ids:
        return 0
    qs = ",".join("?" * len(ids))
    with get_conn() as conn:
        conn.execute(
            f"UPDATE knowledge_items SET promoted_at=CURRENT_TIMESTAMP WHERE id IN ({qs})",
            tuple(ids))
    return len(ids)


def retire_knowledge_item(item_id, reviewer) -> bool:
    """승인된 지식 회수 — status='rejected'로 내려 오버레이에서 제외(롤백)."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE knowledge_items SET status='rejected', reviewed_by=?, "
            "reviewed_at=CURRENT_TIMESTAMP WHERE id=?", (str(reviewer), item_id))
    return True


def knowledge_effect(days=30, limit=30) -> list:
    """승인된 공유 지식의 반영 효과 — 대상 scope의 승인 전/후 👎 비교(롤백 판단 보조)."""
    with get_conn() as conn:
        items = conn.execute(
            """SELECT id, type, target_id, content, reviewed_at
                 FROM knowledge_items
                WHERE scope='approved' AND status='approved' AND reviewed_at IS NOT NULL
                  AND target_kind IN ('program','channel') AND target_id IS NOT NULL
                ORDER BY reviewed_at DESC LIMIT ?""", (limit,)).fetchall()

        def win(scope, lo, hi):
            r = conn.execute(
                "SELECT SUM(CASE WHEN feedback=-1 THEN 1 ELSE 0 END) AS neg, COUNT(*) AS tot "
                "FROM query_history WHERE scope=? AND created_at>=? AND created_at<?",
                (scope, lo, hi)).fetchone()
            return {"neg": r["neg"] or 0, "tot": r["tot"] or 0}

        out = []
        for it in items:
            ra, sc = it["reviewed_at"], it["target_id"]
            lo_b = conn.execute("SELECT datetime(?, ?)", (ra, f'-{days} days')).fetchone()[0]
            hi_a = conn.execute("SELECT datetime(?, ?)", (ra, f'+{days} days')).fetchone()[0]
            out.append({"id": it["id"], "type": it["type"], "target_id": sc,
                        "content": it["content"], "reviewed_at": ra,
                        "before": win(sc, lo_b, ra), "after": win(sc, ra, hi_a)})
        return out


def feedback_weakness(days=30, limit=15) -> list:
    """scope(프로그램/채널)별 👎 집계 — 약점 후보 랭킹. 👎 많은/비율 높은 순."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT scope,
                      SUM(CASE WHEN feedback=-1 THEN 1 ELSE 0 END) AS neg,
                      SUM(CASE WHEN feedback=1  THEN 1 ELSE 0 END) AS pos,
                      COUNT(*) AS total
                 FROM query_history
                WHERE created_at >= datetime('now', ?)
                  AND scope IS NOT NULL AND scope != ''
                GROUP BY scope
               HAVING neg > 0
                ORDER BY neg DESC, (neg*1.0/total) DESC
                LIMIT ?""",
            (f'-{int(days)} days', limit)).fetchall()
        out = []
        for r in rows:
            tot = r["total"] or 1
            out.append({"scope": r["scope"], "neg": r["neg"], "pos": r["pos"],
                        "total": r["total"], "neg_rate": round(r["neg"] / tot * 100, 1)})
        return out


def feedback_negative_open(days=30, limit=25) -> list:
    """개선 시도가 아직 없는 '아쉬움' 질의 — 검토자가 바로 착수할 수 있는 약점 신호."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT q.id, q.question, q.answer, q.scope, q.user_name, q.created_at
                 FROM query_history q
                WHERE q.feedback = -1
                  AND q.created_at >= datetime('now', ?)
                  AND NOT EXISTS (SELECT 1 FROM improvements i WHERE i.source_query_id = q.id)
                ORDER BY q.created_at DESC
                LIMIT ?""",
            (f'-{int(days)} days', limit)).fetchall()
        return [dict(r) for r in rows]


def update_data_request(req_id, status, reviewer) -> bool:
    with get_conn() as conn:
        conn.execute("UPDATE data_requests SET status=?, processed_by=?, processed_at=CURRENT_TIMESTAMP WHERE id=?",
                     (status, str(reviewer), req_id))
    return True


def get_knowledge_items(targets, contributor_id=None, include_candidate=False) -> list:
    """grounding 읽기시 병합용. targets=[(target_kind, target_id), ...] (+ ('global', None)).
    approved(공유) 전체 + (include_candidate면 본인 candidate) 중 대상 매칭 항목 반환."""
    targets = list(targets or []) + [("global", None)]
    conds, params = [], []
    for kind, tid in targets:
        if tid in (None, ""):
            conds.append("target_kind = ?"); params.append(kind)
        else:
            conds.append("(target_kind = ? AND target_id = ?)"); params += [kind, str(tid)]
    target_where = "(" + " OR ".join(conds) + ")"
    scope_where, sp = "scope = 'approved'", []
    if include_candidate and contributor_id:
        scope_where = "(scope = 'approved' OR (scope = 'candidate' AND contributor_id = ?))"
        sp.append(str(contributor_id))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM knowledge_items WHERE {target_where} AND {scope_where} "
            f"AND status != 'rejected' ORDER BY created_at DESC LIMIT 50",
            tuple(params + sp)).fetchall()
        return [dict(r) for r in rows]


def get_frequent_next(user_role: str, slot_from: str, limit: int = 4) -> list:
    """이 시점(role, slot_from)에서 사용자들이 자주 간 다음 단계 — 빈출 다음행동 추천."""
    if not (user_role and slot_from):
        return []
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT slot_to, chip_intent, COUNT(*) AS c
            FROM storyline_events
            WHERE user_role = ? AND slot_from = ?
              AND event_type IN ('chip', 'freetext') AND slot_to IS NOT NULL
            GROUP BY slot_to, chip_intent
            ORDER BY c DESC
            LIMIT ?
            """,
            (user_role, slot_from, limit)
        ).fetchall()
        return [dict(r) for r in rows]


def get_recent_sessions(user_id: str = None, days: int = 7, limit: int = 30,
                        offset: int = 0, all_users: bool = False) -> list:
    """세션 요약 — 세션별 첫 분석대상/첫 진입 칩 + 전이 수 + 토큰 합계 (+사용자명).
    all_users=True면 전체 사용자(이력 보기용), 아니면 user_id 본인(사이드바용).
    days=0이면 전체 기간."""
    if not all_users and not user_id:
        return []
    user_clause = "" if all_users else "AND e.user_id = ?"
    date_clause = "" if int(days) <= 0 else "AND e.created_at >= datetime('now', ?, 'localtime')"
    params: list = []
    if not all_users:
        params.append(str(user_id))
    if int(days) > 0:
        params.append(f"-{int(days)} days")
    params += [limit, offset]
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT e.session_id,
                   MIN(e.created_at) AS started_at,
                   MAX(e.created_at) AS ended_at,
                   COUNT(*)          AS steps,
                   -- 세션 사용자명 (전체 사용자 모드 표시용)
                   (SELECT u.name FROM users u WHERE u.id = CAST(e.user_id AS INTEGER) LIMIT 1) AS user_name,
                   -- 첫(앵커) 분석 대상 프로그램 (seq 최소의 non-null)
                   (SELECT e2.program_name FROM storyline_events e2
                    WHERE e2.session_id = e.session_id AND e2.program_name IS NOT NULL
                    ORDER BY e2.seq LIMIT 1) AS program_name,
                   -- 첫 진입 칩 이름 (seq 최소의 non-null chip_label) — '이력 보기' 제목용
                   (SELECT e3.chip_label FROM storyline_events e3
                    WHERE e3.session_id = e.session_id AND e3.chip_label IS NOT NULL
                    ORDER BY e3.seq LIMIT 1) AS first_chip_label,
                   COUNT(DISTINCT e.program_name) AS program_count,
                   MAX(e.channel_code) AS channel_code,
                   COALESCE(SUM(e.output_tokens), 0) AS total_tokens,
                   MAX(CASE WHEN e.end_reason IS NOT NULL THEN e.end_reason END) AS end_reason
            FROM storyline_events e
            WHERE 1=1
              {user_clause}
              {date_clause}
            GROUP BY e.session_id
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params)
        ).fetchall()
        return [dict(r) for r in rows]


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
               topic_key: str = None, source: str = "general") -> int:
    """질의 1건 DB 저장. JSONL 로그도 함께 기록.
    intent/scope/metric 등 fact 인자는 모두 optional — None이어도 정상 저장.
    source: 'general'(일반 질의) | 'storyline'(스토리라인 칩 질의)."""
    chart_json   = json.dumps(chart_data, ensure_ascii=False) if chart_data is not None else None
    metrics_json = json.dumps(metrics,    ensure_ascii=False) if metrics    else None
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO query_history "
            "(user_id, question, answer, chart_data, ip, user_name, user_role, "
            " input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, "
            " intent, scope, scope_keyword, metric, metrics_json, topic_key, source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, question, answer, chart_json, ip, user_name, user_role,
             input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens,
             intent, scope, scope_keyword, metric, metrics_json, topic_key, source)
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


def get_history(user_id: str, limit: int = 20, days: int = 7) -> list:
    """user_id의 최근 질의 N건. 최신순. 최근 `days`일(기본 7일)만.
    스토리라인 분석 여정(칩·라우팅 질의)은 '최근 분석 여정'에서 별도 표시하므로 제외."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id, question, answer, chart_data, feedback,
                      input_tokens, output_tokens, created_at, source
               FROM query_history
               WHERE user_id = ?
                 AND COALESCE(source, '') != 'storyline'
                 AND COALESCE(intent, '') != 'storyline_routed'
                 AND created_at >= datetime('now', ?)
               ORDER BY created_at DESC
               LIMIT ?""",
            (user_id, f"-{int(days)} days", limit)
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
            "source":        r["source"] or "general",
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
                       feedback, input_tokens, output_tokens, created_at, source
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
                "source":        r["source"] or "general",
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
