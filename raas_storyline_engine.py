"""raas_storyline_engine.py — 직무별 대화형 스토리라인 백엔드 엔진.

`data/storylines/{role}.json` 5슬롯 정의를 읽고 실제 KPI 데이터로 답변 렌더링.

엔드포인트 4종(raas_server.py)에서 호출:
    GET  /api/storyline/role-detect    → role_detect(user)
    GET  /api/storyline/entry          → StorylineEngine.entry()
    POST /api/storyline/advance        → StorylineEngine.advance(...)
    POST /api/storyline/export         → export_stub(...)  (Phase 3 ⑥에서 본격 구현)

설계 원칙:
    - JSON이 단일 출처. 코드는 데이터 채우기 + 렌더링만.
    - 슬롯별 데이터 계산은 _compute_slot_data() 디스패치 — 데이터 적재되면 함수만 채우면 됨.
    - 데이터 부족 시 자동 fallback_answer_template 사용.
    - 시뮬레이터(raas_storyline_simulator.py)의 render() 로직과 동일.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent
STORYLINES_DIR = ROOT / "data" / "storylines"
KPI_CSV = ROOT / "data" / "raas_kpi_latest.csv"

# ─── 직무 매핑 ──────────────────────────────────────────────────────────
# (1) role 식별자 → JSON 파일명
ROLE_TO_FILE = {
    "cp": "cp.json",
    "pd_program": "pd_program.json",
    "제작pd": "pd_program.json",
    "pd_schedule": "pd_schedule.json",
    "편성pd": "pd_schedule.json",
}

# (2) 한국어 role(raas_auth.ALLOWED_ROLES) → 영문 role ID
KOREAN_ROLE_TO_ID = {
    "cp": "cp",
    "CP": "cp",
    "제작": "pd_program",       # raas_auth ALLOWED_ROLES와 일치
    "제작pd": "pd_program",     # 호환
    "제작PD": "pd_program",     # 호환
    "편성": "pd_schedule",       # raas_auth ALLOWED_ROLES와 일치
    "편성pd": "pd_schedule",     # 호환
    "편성PD": "pd_schedule",     # 호환
}

# (3) CP 채널 매핑 — 사용자 프로파일에 channel 필드 없으면 my_programs 첫 글자로 추론
PROGRAM_PREFIX_TO_CHANNEL = {
    "F": "파워FM",
    "L": "러브FM",
    "M": "러브FM",  # M05~M11도 러브FM 소속
    "G": "고릴라M",
    "P": "픽채널",
    "T": "전체",
}

CHANNEL_TO_PROGRAM_PREFIXES = {
    "파워FM": ["F"],          # F01~F13 (F00 제외)
    "러브FM": ["L", "M"],     # L01~L15, M05~M11 (L00 제외)
    "고릴라M": ["G"],
    "픽채널": ["P"],
}

# ─── 템플릿 렌더링 ──────────────────────────────────────────────────────
def render(template: str, data: dict) -> str:
    """{변수} 또는 {변수:포맷} 치환. 누락 변수는 [{변수명}?] 표시.

    raas_storyline_simulator.py와 동일 로직.
    """
    if not template:
        return ""

    def repl(m: re.Match) -> str:
        var_full = m.group(1)
        var_name = var_full.split(":")[0] if ":" in var_full else var_full
        if var_name not in data:
            return f"[{var_name}?]"
        try:
            return ("{" + var_full + "}").format_map(data)
        except (ValueError, KeyError, TypeError):
            return f"[{var_name}?]"

    return re.sub(r"\{([^{}]+)\}", repl, template)


# ─── JSON 설정 로더 (캐시) ──────────────────────────────────────────────
_CONFIG_CACHE: dict[str, dict] = {}


def load_config(role: str) -> dict:
    """role ID → 스토리라인 JSON dict. 캐시."""
    key = (role or "").lower().strip()
    if key in _CONFIG_CACHE:
        return _CONFIG_CACHE[key]

    fname = ROLE_TO_FILE.get(key)
    if not fname:
        raise ValueError(f"Unknown role: '{role}'. 사용 가능: {list_available_roles()}")

    cfg_path = STORYLINES_DIR / fname
    if not cfg_path.exists():
        raise FileNotFoundError(str(cfg_path))

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    _CONFIG_CACHE[key] = cfg
    return cfg


def list_available_roles() -> list[str]:
    """사용 가능한 직무 ID 목록 (JSON 파일이 있는 것만)."""
    return sorted({k for k, v in ROLE_TO_FILE.items() if (STORYLINES_DIR / v).exists()})


def reload_configs() -> None:
    """캐시 초기화 — JSON 편집 후 호출."""
    _CONFIG_CACHE.clear()


# ─── 사용자 → 스토리라인 직무 매핑 ──────────────────────────────────────
def role_detect(user: Optional[dict]) -> dict:
    """로그인한 사용자의 user dict에서 스토리라인 role ID 결정.

    Returns:
        {
            "role": "cp" | "pd_program" | "pd_schedule" | None,
            "available": bool,                  # JSON + 필요 매핑이 있는지
            "missing_setup": [str],             # 부족한 셋업 사항
            "user_context": {...}               # entry 호출 시 사용할 컨텍스트
        }
    """
    if not user:
        return {
            "role": None,
            "available": False,
            "missing_setup": ["로그인이 필요합니다."],
            "user_context": {},
        }

    korean_role = (user.get("role") or "").strip()
    role_id = KOREAN_ROLE_TO_ID.get(korean_role) or KOREAN_ROLE_TO_ID.get(korean_role.lower())

    missing: list[str] = []
    if not role_id:
        missing.append(
            f"사용자의 role '{korean_role}'에 대응하는 스토리라인이 없습니다. "
            f"현재 지원: CP, 제작PD, 편성PD"
        )
        return {
            "role": None,
            "available": False,
            "missing_setup": missing,
            "user_context": {"user_name": user.get("name", "")},
        }

    # CP는 channel 매핑이 필요
    user_context = {"user_name": user.get("name", "") or user.get("login_id", "사용자")}
    if role_id == "cp":
        channel = _derive_cp_channel(user)
        if not channel:
            missing.append(
                "CP→채널 매핑이 등록되지 않았습니다. "
                "프로필에서 담당 채널(파워FM 또는 러브FM)을 선택해 주세요."
            )
        else:
            user_context["channel_name"] = channel

    return {
        "role": role_id,
        "available": not missing,
        "missing_setup": missing,
        "user_context": user_context,
    }


def _derive_cp_channel(user: dict) -> Optional[str]:
    """CP 사용자의 채널 결정.

    우선순위:
    1. user["channel"] 명시 필드
    2. user["my_programs"] 첫 프로그램 코드의 prefix → 채널
    3. None (사용자 본인이 등록 필요)
    """
    if user.get("channel"):
        return user["channel"]

    for code in _parse_my_programs(user):
        ch = PROGRAM_PREFIX_TO_CHANNEL.get(code[0].upper())
        if ch in ("파워FM", "러브FM"):
            return ch
    return None


def _parse_my_programs(user: dict) -> list[str]:
    """user.my_programs를 list[str]로 표준화. SQLite는 JSON 문자열로 저장."""
    my = user.get("my_programs") or []
    if isinstance(my, str):
        try:
            my = json.loads(my)
        except (ValueError, TypeError):
            my = []
    return [c for c in (my or []) if isinstance(c, str) and c]


def _derive_pd_primary_program(user: dict) -> Optional[str]:
    """제작PD가 가장 우선시하는 프로그램 코드 (my_programs[0])."""
    codes = _parse_my_programs(user)
    return codes[0].upper() if codes else None


# KPI CSV에서 특정 프로그램 최신 회차 조회 (캐시)
_KPI_CACHE: list[dict] | None = None
_KPI_CACHE_MTIME: float | None = None


def _kpi_rows() -> list[dict]:
    """raas_kpi_latest.csv 전체 행 캐시. mtime 변경 시 자동 리로드."""
    global _KPI_CACHE, _KPI_CACHE_MTIME
    if not KPI_CSV.exists():
        return []
    mtime = KPI_CSV.stat().st_mtime
    if _KPI_CACHE is not None and _KPI_CACHE_MTIME == mtime:
        return _KPI_CACHE
    with open(KPI_CSV, encoding="utf-8-sig") as f:
        _KPI_CACHE = list(csv.DictReader(f))
    _KPI_CACHE_MTIME = mtime
    return _KPI_CACHE


def _load_program_latest_row(code: str) -> Optional[dict]:
    """프로그램 코드의 가장 최근일 KPI row."""
    rows = [r for r in _kpi_rows() if r.get("PGM_CODE", "").upper() == code.upper()]
    if not rows:
        return None
    rows.sort(key=lambda r: r.get("DATE", ""), reverse=True)
    return rows[0]


def _load_program_history(code: str, lookback_days: int = 60) -> list[dict]:
    """프로그램 코드의 최근 N일 KPI rows (오름차순)."""
    rows = [r for r in _kpi_rows() if r.get("PGM_CODE", "").upper() == code.upper()]
    rows.sort(key=lambda r: r.get("DATE", ""))
    return rows[-lookback_days:] if lookback_days else rows


# 요일 한국어
_WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]


def _weekday_ko(date_str: str) -> str:
    """'YYYY/MM/DD' → '월'~'일'."""
    try:
        from datetime import datetime
        d = datetime.strptime(date_str.replace("-", "/"), "%Y/%m/%d")
        return _WEEKDAY_KO[d.weekday()]
    except (ValueError, TypeError):
        return ""


# ─── KPI 데이터 로더 (Phase 1 ② 즉시 동작 가능) ────────────────────────
def _load_latest_program_kpis(channel: str) -> list[dict]:
    """채널의 최신일 프로그램별 KPI.

    Returns:
        [{"code", "name", "dau", "dau_prev", "dau_chg"}, ...]
    """
    prefixes = CHANNEL_TO_PROGRAM_PREFIXES.get(channel, [])
    if not prefixes:
        return []
    if not KPI_CSV.exists():
        return []

    with open(KPI_CSV, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return []

    latest_date = max(r.get("DATE", "") for r in rows if r.get("DATE"))
    programs: list[dict] = []
    for r in rows:
        if r.get("DATE") != latest_date:
            continue
        code = r.get("PGM_CODE", "")
        if not code or len(code) < 3 or code.endswith("00"):
            continue
        if not any(code.startswith(p) for p in prefixes):
            continue

        try:
            dau = int(float(r.get("dau") or 0))
            dau_prev = int(float(r.get("dau_prev") or 0))
            chg_raw = r.get("dau_chg") or ""
            chg = float(chg_raw) if chg_raw else None
        except (ValueError, TypeError):
            continue

        if dau <= 0:
            continue

        programs.append(
            {
                "code": code,
                "name": r.get("PGM_NAME") or code,
                "dau": dau,
                "dau_prev": dau_prev,
                "dau_chg": chg,
            }
        )
    return programs


def _top_changes(programs: list[dict], n: int = 3) -> list[dict]:
    """변화율 절대값 기준 TOP N (None인 항목은 제외)."""
    with_chg = [p for p in programs if p.get("dau_chg") is not None]
    with_chg.sort(key=lambda p: abs(p["dau_chg"]), reverse=True)
    return with_chg[:n]


# ─── 슬롯 데이터 컴퓨터 (디스패치) ───────────────────────────────────────
def _compute_pd_program_anchor(user_context: dict) -> tuple[dict, bool]:
    """제작PD 1_anchor: 본인 프로그램의 직전 회차 + 동일 요일 4주 평균 대비.

    KPI CSV만으로 동작 (공감로그/문자는 적재 후 추가될 변수).
    """
    code = user_context.get("program_code")
    if not code:
        return {}, True

    rows = _load_program_history(code, lookback_days=90)
    if not rows:
        return {}, True

    latest = rows[-1]
    last_date = latest.get("DATE", "")
    try:
        curr_dau = int(float(latest.get("dau") or 0))
    except (ValueError, TypeError):
        return {}, True
    if curr_dau <= 0:
        return {}, True

    # 동일 요일 4주 평균 (최신 회차 제외)
    from datetime import datetime
    try:
        latest_dt = datetime.strptime(last_date.replace("-", "/"), "%Y/%m/%d")
        latest_weekday = latest_dt.weekday()
    except (ValueError, TypeError):
        latest_weekday = None

    weekday_daus: list[int] = []
    if latest_weekday is not None:
        for r in rows[:-1]:
            d = r.get("DATE", "")
            try:
                dt = datetime.strptime(d.replace("-", "/"), "%Y/%m/%d")
                if dt.weekday() == latest_weekday:
                    weekday_daus.append(int(float(r.get("dau") or 0)))
            except (ValueError, TypeError):
                continue
    # 가장 가까운 4개만
    recent_weekday_avg = (
        round(sum(weekday_daus[-4:]) / min(4, len(weekday_daus)))
        if weekday_daus else None
    )

    vs_weekday_pct = (
        round((curr_dau - recent_weekday_avg) / recent_weekday_avg * 100, 1)
        if recent_weekday_avg else None
    )

    # 신규 사용자 변화율 (fallback 텍스트의 단서)
    try:
        new_chg = latest.get("new_chg")
        new_user_change_pct = float(new_chg) if new_chg else 0.0
    except (ValueError, TypeError):
        new_user_change_pct = 0.0

    return {
        "program_name": latest.get("PGM_NAME") or code,
        "last_episode_date": last_date.replace("/", "-"),
        "weekday": _weekday_ko(last_date),
        "curr_dau": curr_dau,
        "weekday_avg_dau": recent_weekday_avg or 0,
        "vs_weekday_pct": vs_weekday_pct if vs_weekday_pct is not None else 0.0,
        "new_user_change_pct": new_user_change_pct,
    }, True  # 공감로그·인접 매핑 미적재 → fallback 템플릿이 더 깔끔


def _compute_cp_anchor(user_context: dict) -> tuple[dict, bool]:
    """CP 1_anchor: 채널 프로그램 변화 TOP 3."""
    channel = user_context.get("channel_name")
    if not channel:
        return {}, True

    programs = _load_latest_program_kpis(channel)
    if not programs:
        return {}, True

    top3 = _top_changes(programs, 3)
    if not top3:
        return {}, True

    top = top3[0]
    data = {
        "channel_name": channel,
        "program_count": len(programs),
        "top_change_program": top["name"],
        "change_pct": top["dau_chg"],
        "prev_dau": top["dau_prev"],
        "curr_dau": top["dau"],
    }
    if len(top3) > 1:
        data["top_2_program"] = top3[1]["name"]
        data["top_2_change_pct"] = top3[1]["dau_chg"]
    if len(top3) > 2:
        data["top_3_program"] = top3[2]["name"]
        data["top_3_change_pct"] = top3[2]["dau_chg"]
    return data, False


# 슬롯 디스패치 테이블 (role, slot) → computer fn
_SLOT_COMPUTERS = {
    ("CP", "1_anchor"): _compute_cp_anchor,
    ("PD_PROGRAM", "1_anchor"): _compute_pd_program_anchor,
    # 나머지 슬롯은 Phase 1 ① 적재 후 추가 — 현재는 fallback
}


# ─── 스토리라인 엔진 ──────────────────────────────────────────────────────
class StorylineEngine:
    def __init__(self, role: str, user: Optional[dict] = None,
                 channel_override: Optional[str] = None):
        self.role = role
        self.user = user or {}
        self.config = load_config(role)
        self._channel_override = channel_override

    def _user_context(self) -> dict:
        ctx: dict = {
            "user_name": self.user.get("name") or self.user.get("login_id") or "사용자",
        }
        role = self.config.get("role")

        # CP — 담당 채널
        if role == "CP":
            if self._channel_override:
                ctx["channel_name"] = self._channel_override
            else:
                ch = _derive_cp_channel(self.user) if self.user else None
                ctx["channel_name"] = ch or "파워FM"  # 미설정 시 시연용 기본값

        # 제작PD — 담당 프로그램(my_programs[0])과 최근 회차 자동 채움
        elif role == "PD_PROGRAM":
            code = _derive_pd_primary_program(self.user) if self.user else None
            if code:
                row = _load_program_latest_row(code)
                if row:
                    ctx["program_code"] = code
                    ctx["program_name"] = row.get("PGM_NAME") or code
                    last_date = row.get("DATE", "")
                    ctx["last_episode_date"] = last_date.replace("/", "-")
                    ctx["weekday"] = _weekday_ko(last_date)
        return ctx

    def entry(self) -> dict:
        """첫 화면 — greeting + first_question + default chips."""
        ctx = self._user_context()
        entry_cfg = self.config["entry"]
        return {
            "ok": True,
            "role": self.config["role"],
            "role_name_ko": self.config["role_name_ko"],
            "greeting": render(entry_cfg["greeting_template"], ctx),
            "first_question": render(entry_cfg["first_question"], ctx),
            "chips": entry_cfg["default_chips"],
            "user_context": ctx,
        }

    def advance(self, slot_from: str, chip_intent: str,
                prev_context: Optional[dict] = None) -> dict:
        """칩 클릭 → 다음 슬롯 답변 렌더링.

        Args:
            slot_from: "entry" 또는 슬롯 ID ("1_anchor" 등)
            chip_intent: 클릭한 칩의 intent 값
            prev_context: 직전 advance가 돌려준 context_out

        Returns:
            {ok, slot, slot_name, answer, fallback_used, chips_next, context_out}
        """
        prev_context = prev_context or {}

        # slot_from에서 칩 목록 찾기
        if slot_from == "entry":
            chips = self.config["entry"]["default_chips"]
        else:
            slot_cfg_from = self.config["slots"].get(slot_from)
            if not slot_cfg_from:
                return {"ok": False, "error": f"slot '{slot_from}' not found"}
            chips = slot_cfg_from.get("chips_next", [])

        clicked_chip = next(
            (c for c in chips if c.get("intent") == chip_intent), None
        )
        if not clicked_chip:
            return {
                "ok": False,
                "error": f"intent '{chip_intent}' not found in slot '{slot_from}'",
                "available_intents": [c.get("intent") for c in chips],
            }

        next_slot = clicked_chip.get("next_slot")

        # _end: 산출물 종료 (Phase 3 ⑥에서 본격 구현)
        if next_slot == "_end":
            output_id = clicked_chip.get("output_format_id", "")
            outputs = {o["id"]: o for o in self.config["ontology"]["outputsTo"]}
            o = outputs.get(output_id, {"name": output_id, "format": "?"})
            return {
                "ok": True,
                "slot": "_end",
                "ended": True,
                "output_format_id": output_id,
                "output_name": o.get("name", output_id),
                "output_format": o.get("format", ""),
                "message": (
                    f"{o.get('name', output_id)} 산출물 생성 — "
                    "Phase 3 ⑥에서 실제 변환 구현 예정"
                ),
            }

        # 다음 슬롯 렌더링
        slot_cfg = self.config["slots"].get(next_slot)
        if not slot_cfg:
            return {"ok": False, "error": f"next_slot '{next_slot}' not found"}

        slot_data, fallback_used = self._compute_slot_data(next_slot, prev_context)
        ctx = {**self._user_context(), **prev_context, **slot_data}

        template_key = (
            "fallback_answer_template" if fallback_used else "answer_template"
        )
        template = (
            slot_cfg.get(template_key) or slot_cfg.get("answer_template") or ""
        )
        answer = render(template, ctx)

        return {
            "ok": True,
            "slot": next_slot,
            "slot_name": slot_cfg.get("name"),
            "slot_purpose": slot_cfg.get("purpose"),
            "answer": answer,
            "fallback_used": fallback_used,
            "chips_next": slot_cfg.get("chips_next", []),
            "context_out": ctx,
        }

    def _compute_slot_data(self, slot_id: str,
                           prev_context: dict) -> tuple[dict, bool]:
        """슬롯별 데이터 계산. _SLOT_COMPUTERS에 등록된 함수만 본격 동작."""
        computer = _SLOT_COMPUTERS.get((self.config["role"], slot_id))
        if computer:
            ctx = self._user_context()
            return computer(ctx)
        # 등록되지 않은 슬롯 — fallback (prev_context는 그대로 통과)
        return {}, True


# ─── Export stub (Phase 3 ⑥ 예정) ───────────────────────────────────────
def export_stub(role: str, output_format_id: str,
                slots_visited: list) -> dict:
    """산출물 생성 — 현재 메타데이터만 반환. PPT/카톡 변환은 Phase 3 ⑥."""
    cfg = load_config(role)
    outputs = {o["id"]: o for o in cfg["ontology"]["outputsTo"]}
    o = outputs.get(output_format_id)
    if not o:
        return {
            "ok": False,
            "error": f"unknown output_format_id '{output_format_id}'",
            "available": list(outputs.keys()),
        }
    return {
        "ok": True,
        "stub": True,
        "role": role,
        "output_format_id": output_format_id,
        "output_name": o.get("name"),
        "output_format": o.get("format"),
        "slots_visited": [s.get("slot") for s in slots_visited],
        "message": (
            f"{o.get('name')} 산출물 생성 요청 접수 — "
            "실제 변환(PPT/카톡 등)은 Phase 3 ⑥에서 구현됩니다."
        ),
    }


# ─── CLI 자가 테스트 ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    print("=== 사용 가능한 직무 ===")
    print(list_available_roles())
    print()

    # 시연용 가상 user
    fake_user = {
        "name": "박CP",
        "login_id": "cp_powerfm",
        "role": "CP",
        "channel": "파워FM",
        "my_programs": [],
    }

    print("=== role_detect ===")
    rd = role_detect(fake_user)
    print(json.dumps(rd, ensure_ascii=False, indent=2))
    print()

    print("=== entry (CP) ===")
    engine = StorylineEngine(role="cp", user=fake_user)
    e = engine.entry()
    print(json.dumps(e, ensure_ascii=False, indent=2))
    print()

    print("=== advance: entry → 1_anchor (intent=show_biggest_change) ===")
    adv1 = engine.advance("entry", "show_biggest_change")
    print(json.dumps({k: v for k, v in adv1.items() if k != "context_out"},
                     ensure_ascii=False, indent=2))
    print()

    print("=== advance: 1_anchor → 2_cause (intent=explain_change) ===")
    adv2 = engine.advance("1_anchor", "explain_change",
                          prev_context=adv1.get("context_out", {}))
    print(json.dumps({k: v for k, v in adv2.items() if k != "context_out"},
                     ensure_ascii=False, indent=2))
