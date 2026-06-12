"""
RAAS 사용자 인증 — 표준 라이브러리만 사용 (hashlib, secrets, sqlite3).
raas_history.db의 users·sessions 테이블 위에서 동작.

- 비밀번호: pbkdf2_hmac('sha256', salt 16바이트, 200,000 iter)
- 세션 토큰: secrets.token_urlsafe(32), 만료 30일
- 부트스트랩: .env의 ADMIN_LOGIN_IDS(콤마 구분)에 있는 login_id는
              가입 시 자동 승인·role='관리자' 강제. 기존 계정도 init 시 승격.
"""

import hashlib
import secrets
import os
import json
from datetime import datetime, timedelta
from typing import Optional, List

from raas_history_db import get_conn

# ── 상수 ─────────────────────────────────────────────────────
PBKDF2_ITER     = 200_000
SESSION_DAYS    = 30
# 직무 — 사용자 선택 가능한 8종. 시스템 관리자 권한은 별개 (users.is_admin).
# 단일 진실 소스는 role_profiles.json — 추후 동적 로드로 전환 가능.
ALLOWED_ROLES   = {'제작', '편성', '서비스운영', '플랫폼전략',
                   '데이터', 'CP', '총괄관리', '마케팅(광고·협찬)'}


# ── 비밀번호 해시 ────────────────────────────────────────────
def _hash_pw(pw: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac(
        'sha256', pw.encode('utf-8'), bytes.fromhex(salt), PBKDF2_ITER
    ).hex()


def _verify_pw(pw: str, pw_hash: str, salt: str) -> bool:
    return secrets.compare_digest(_hash_pw(pw, salt), pw_hash)


def _parse_my_programs(raw) -> list:
    if not raw:
        return []
    try:
        v = json.loads(raw) if isinstance(raw, str) else raw
        return [str(x) for x in v if x]
    except Exception:
        return []


def _user_dict(row) -> dict:
    """sqlite3.Row → 응답용 user dict. my_programs 자동 파싱."""
    keys = row.keys()
    return {
        'id':           row['id'],
        'login_id':     row['login_id'],
        'name':         row['name'],
        'role':         row['role'],
        'status':       row['status'] if 'status' in keys else 'approved',
        'is_admin':     bool(row['is_admin']) if 'is_admin' in keys else False,
        'my_programs':  _parse_my_programs(row['my_programs']) if 'my_programs' in keys else [],
    }


# ── 가입 / 로그인 ────────────────────────────────────────────
def register_user(login_id: str, pw: str, name: str,
                  role: str,
                  my_programs: Optional[List[str]] = None) -> dict:
    """신규 사용자 등록. status='pending'.
    ADMIN_LOGIN_IDS에 포함된 login_id면 자동 승인 + is_admin=1.
    리턴: {ok, user_id, status} 또는 {ok=False, error}.
    """
    login_id = (login_id or '').strip()
    name     = (name or '').strip()
    role     = (role or '').strip()
    my_json  = json.dumps(my_programs or [], ensure_ascii=False)

    if not login_id or not pw or not name or not role:
        return {'ok': False, 'error': 'login_id·password·name·role 필수'}
    if role not in ALLOWED_ROLES:
        return {'ok': False, 'error': f"role은 {sorted(ALLOWED_ROLES)} 중 하나"}
    if len(pw) < 4:
        return {'ok': False, 'error': '비밀번호는 4자 이상'}

    salt    = secrets.token_hex(16)
    pw_hash = _hash_pw(pw, salt)

    # 부트스트랩 대상이면 자동 승인·관리자 권한 부여 (role은 사용자 선택 그대로 유지)
    is_bootstrap = login_id in _bootstrap_ids()
    eff_status   = 'approved' if is_bootstrap else 'pending'
    is_admin     = 1 if is_bootstrap else 0
    approved_at  = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') if is_bootstrap else None

    try:
        with get_conn() as conn:
            cur = conn.execute(
                "INSERT INTO users "
                "(login_id, pw_hash, pw_salt, name, role, status, approved_at, my_programs, is_admin) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (login_id, pw_hash, salt, name, role, eff_status, approved_at, my_json, is_admin)
            )
            uid = cur.lastrowid
    except Exception as e:
        if 'UNIQUE' in str(e):
            return {'ok': False, 'error': '이미 사용 중인 login_id'}
        return {'ok': False, 'error': str(e)}

    return {'ok': True, 'user_id': uid, 'status': eff_status, 'is_admin': bool(is_admin)}


def authenticate(login_id: str, pw: str) -> Optional[dict]:
    """ID/PW 검증. status='approved'만 통과. 통과 시 user dict 반환."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, login_id, pw_hash, pw_salt, name, role, status, my_programs, is_admin "
            "FROM users WHERE login_id = ?",
            (login_id,)
        ).fetchone()
    if not row:
        return None
    if not _verify_pw(pw, row['pw_hash'], row['pw_salt']):
        return None
    # status에 따라 분기 — 호출자가 pending UI로 안내
    return _user_dict(row)


# ── 세션 관리 ────────────────────────────────────────────────
def create_session(user_id: int) -> str:
    """세션 토큰 생성. 만료 SESSION_DAYS일."""
    token   = secrets.token_urlsafe(32)
    expires = (datetime.utcnow() + timedelta(days=SESSION_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
            (token, user_id, expires)
        )
    return token


def resolve_session(token: str) -> Optional[dict]:
    """토큰 → user dict. 만료/미존재/거절상태면 None."""
    if not token:
        return None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT u.id, u.login_id, u.name, u.role, u.status, u.my_programs, u.is_admin, s.expires_at "
            "FROM sessions s JOIN users u ON s.user_id = u.id "
            "WHERE s.token = ?",
            (token,)
        ).fetchone()
    if not row:
        return None
    # 만료 확인
    try:
        if row['expires_at'] and datetime.strptime(
                row['expires_at'], '%Y-%m-%d %H:%M:%S'
        ) < datetime.utcnow():
            destroy_session(token)
            return None
    except Exception:
        pass
    if row['status'] != 'approved':
        return None
    return _user_dict(row)


def destroy_session(token: str) -> None:
    if not token:
        return
    with get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))


# ── 프로필 수정 / 비밀번호 변경 ───────────────────────────────
def update_profile(user_id: int, name: Optional[str] = None,
                   role: Optional[str] = None,
                   my_programs: Optional[List[str]] = None) -> dict:
    """현재 사용자의 프로필 수정. None인 필드는 변경하지 않음.
    login_id·status·is_admin은 이 경로로 바꾸지 않음."""
    sets, vals = [], []
    if name is not None:
        n = name.strip()
        if not n:
            return {'ok': False, 'error': '이름은 비울 수 없습니다.'}
        sets.append("name = ?"); vals.append(n)
    if role is not None:
        r = role.strip()
        if r not in ALLOWED_ROLES:
            return {'ok': False, 'error': f"role은 {sorted(ALLOWED_ROLES)} 중 하나"}
        sets.append("role = ?"); vals.append(r)
    if my_programs is not None:
        if not isinstance(my_programs, list):
            return {'ok': False, 'error': 'my_programs는 배열'}
        sets.append("my_programs = ?")
        vals.append(json.dumps([str(x) for x in my_programs if x], ensure_ascii=False))
    if not sets:
        return {'ok': False, 'error': '변경할 필드 없음'}
    vals.append(user_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", vals)
        row = conn.execute(
            "SELECT id, login_id, name, role, status, my_programs, is_admin "
            "FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    if not row:
        return {'ok': False, 'error': '사용자 없음'}
    return {'ok': True, 'user': _user_dict(row)}


def change_password(user_id: int, old_pw: str, new_pw: str) -> dict:
    if not old_pw or not new_pw:
        return {'ok': False, 'error': '현재/새 비밀번호 필수'}
    if len(new_pw) < 4:
        return {'ok': False, 'error': '새 비밀번호는 4자 이상'}
    with get_conn() as conn:
        row = conn.execute(
            "SELECT pw_hash, pw_salt FROM users WHERE id = ?", (user_id,)
        ).fetchone()
        if not row:
            return {'ok': False, 'error': '사용자 없음'}
        if not _verify_pw(old_pw, row['pw_hash'], row['pw_salt']):
            return {'ok': False, 'error': '현재 비밀번호가 올바르지 않습니다.'}
        new_salt = secrets.token_hex(16)
        new_hash = _hash_pw(new_pw, new_salt)
        conn.execute(
            "UPDATE users SET pw_hash = ?, pw_salt = ? WHERE id = ?",
            (new_hash, new_salt, user_id)
        )
    return {'ok': True}


# ── 관리자 기능 ──────────────────────────────────────────────
def get_pending_users() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, login_id, name, role, title, created_at "
            "FROM users WHERE status = 'pending' "
            "ORDER BY created_at ASC"
        ).fetchall()
    return [dict(r) for r in rows]


def list_users() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, login_id, name, role, title, status, "
            "       created_at, approved_at, approved_by "
            "FROM users ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def approve_user(uid: int, admin_id: int) -> bool:
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE users SET status='approved', approved_at=?, approved_by=? "
            "WHERE id = ? AND status != 'approved'",
            (now, admin_id, uid)
        )
    return cur.rowcount > 0


def reject_user(uid: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE users SET status='rejected' WHERE id = ? AND status='pending'",
            (uid,)
        )
        # 거절 시 활성 세션 모두 무효화
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (uid,))
    return cur.rowcount > 0


# ── 부트스트랩 (.env ADMIN_LOGIN_IDS) ────────────────────────
def _bootstrap_ids() -> set:
    raw = os.getenv('ADMIN_LOGIN_IDS', '') or ''
    return {s.strip() for s in raw.split(',') if s.strip()}


def bootstrap_admins() -> None:
    """기존 계정 중 ADMIN_LOGIN_IDS에 있는 login_id를 자동 승인·is_admin=1로 설정.
    init_db() 직후 1회 호출. role은 사용자가 선택한 값을 유지하되,
    구버전 잔재인 role='관리자'는 신 ALLOWED_ROLES에 없으므로 '총괄관리'로 보정.
    신규 가입은 register_user에서 처리."""
    ids = _bootstrap_ids()
    if not ids:
        return
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    with get_conn() as conn:
        for login_id in ids:
            conn.execute(
                "UPDATE users SET "
                "  is_admin=1, status='approved', "
                "  approved_at=COALESCE(approved_at, ?), "
                "  role=CASE WHEN role='관리자' THEN '총괄관리' ELSE role END "
                "WHERE login_id = ?",
                (now, login_id)
            )
