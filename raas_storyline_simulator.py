"""raas_storyline_simulator.py — 직무별 스토리라인 터미널 시뮬레이터.

JSON 파일만으로 5슬롯 대화 흐름을 검증하는 도구. 백엔드 없이 동작.

사용법:
    python raas_storyline_simulator.py cp
    python raas_storyline_simulator.py pd_program --mode fallback
    python raas_storyline_simulator.py pd_schedule --mode happy
    python raas_storyline_simulator.py cp --auto    # 자동 진행 (모든 칩 1번 선택)

mode:
    happy    — 모든 데이터 적재 완료 가정, answer_template 사용
    fallback — 데이터 부족 가정, fallback_answer_template 사용
    auto     — slot의 data_requirements 기반 자동 판단
"""
import argparse
import json
import re
import sys
from pathlib import Path

# Windows 콘솔 UTF-8 출력
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).parent
STORYLINES_DIR = ROOT / "data" / "storylines"


# ─── 더미 데이터 (template 변수 치환용) ──────────────────────────────────
# 실제 시연 시엔 raas_kpi_latest.csv + 사용자 컨텍스트에서 채워짐
DUMMY = {
    # 사용자/채널/프로그램 메타
    "user_name": "김프로",
    "channel_name": "파워FM",       # CP는 채널 단위 — 파워FM CP / 러브FM CP
    "program_name": "두 시 탈출 컬투쇼",
    "program_count": 13,             # 파워FM 프로그램 수 (F01~F13)

    # CP 1_anchor
    "top_change_program": "두 시 탈출 컬투쇼",
    "change_pct": -7.3,
    "prev_dau": 152300,
    "curr_dau": 141200,
    "top_2_program": "골든디스크",
    "top_2_change_pct": 4.1,
    "top_3_program": "지금은 라디오시대",
    "top_3_change_pct": 2.8,

    # CP 2_cause
    "cause_1_name": "주중 특별 게스트 효과 감소",
    "cause_1_evidence": "지난주 게스트 출연 회차 +18%, 이번주 게스트 없음",
    "cause_2_name": "인접 시간대 흡수",
    "cause_2_evidence": "앞 시간대 +5%, 동일 청취층 이동 가능성",
    "peak_date": "2026-06-15",
    "peak_change_pct": -12.4,
    "peak_event_summary": "주말 회차 — 특이 이벤트 없는데도 큰 폭 감소",
    "new_user_change_pct": -3.1,
    "adjacent_change_pct": 5.2,
    "channel_specific_probability": "높음",

    # CP 3_context
    "pd_name": "박PD",
    "next_episode_date": "2026-06-18",
    "watchpoint_1": "게스트 없는 평일 회차 청취자 흐름",
    "watchpoint_2": "오프닝 토픽 반응",
    "cause_summary": "주중 게스트 효과 감소 + 인접 흡수",
    "adjacent_program": "골든디스크",

    # CP 4_loop
    "window_weeks": 4,
    "n": 2,
    "change_table_rows": (
        "| 두 시 탈출 컬투쇼 | 코너 개편(06-01) | 148,500 | 142,100 | -4.3% |\n"
        "| 골든디스크 | 진행자 교체(05-20) | 98,200 | 112,400 | +14.5% |"
    ),
    "notable_program": "골든디스크",
    "before_dau": 98200,
    "after_dau": 112400,
    "interpretation": "진행자 교체가 명확한 양의 효과",

    # CP 5_closing
    "dispatch_summary": "박PD에게 게스트·인접 비교 요약 카톡 발송",

    # 제작PD 1_anchor
    "last_episode_date": "2026-06-16",
    "weekday": "월",
    "weekday_avg_dau": 145200,
    "vs_weekday_pct": -2.8,
    "emotion_log_count": 312,
    "emotion_log_avg": 215,
    "vs_emotion_pct": 45.1,
    "interpretation_one_line": "활성사용자는 평소와 비슷하나 공감로그·문자가 평소의 1.5배 — 화제성 있는 회차",

    # 제작PD 2_cause
    "corner_breakdown_rows": (
        "| 오프닝 토크 | 142,000 | +1.2% |\n"
        "| 게스트 인터뷰 | 168,000 | +18.7% |\n"
        "| 청취자 사연 | 152,000 | +3.4% |\n"
        "| 마지막 코너 | 128,000 | -8.1% |"
    ),
    "peak_corner": "게스트 인터뷰",
    "peak_cause_reason": "특별 게스트 출연으로 청취자 +18.7%, 공감로그 +120건 집중",

    # 제작PD 3_context
    "dau_week": 980000,
    "dau_week_wow_pct": -1.2,
    "weekly_pattern_summary": "월·수·금 강세, 화·목 약세",
    "our_dau": 141200,
    "prev_program": "12시 뉴스",
    "next_program": "골든디스크",
    "next_dau": 112400,
    "our_wow": -2.8,
    "prev_wow": 1.4,
    "next_wow": 4.1,
    "relative_position": "중간 (앞 시간대 강세, 뒤 시간대 상승 추세)",

    # 제작PD 4_loop
    "change_date": "2026-06-01",
    "change_summary": "마지막 코너를 청취자 참여형으로 변경",
    "churn_change_pp": -1.2,

    # 제작PD 5_closing
    "peak_cause_summary": "특별 게스트로 화제성 ↑",

    # 편성PD 1_anchor
    "quarter": "26Q2",
    "channel_grid_rows": (
        "| 출근 (07-09) | 245K -2.1% | 198K +1.2% | 120K +4.5% | 88K +8.3% |\n"
        "| 오전 (09-12) | 188K +0.3% | 215K -1.8% | 95K +2.1% | 72K +5.4% |\n"
        "| 점심 (12-14) | 312K -4.5% | 245K +2.1% | 142K +1.8% | 102K +3.2% |\n"
        "| 오후 (14-18) | 285K -7.8% | 198K -1.2% | 118K +0.5% | 89K +6.7% |\n"
        "| 저녁 (18-22) | 198K +1.4% | 312K +2.8% | 145K +3.1% | 124K +5.2% |"
    ),
    "anomaly_1": "파워FM 오후 (14-18시)",
    "anomaly_1_change_pct": -7.8,
    "anomaly_1_reason_hint": "두 시 탈출 컬투쇼 게스트 효과 감소",
    "anomaly_2": "파워FM 점심 (12-14시)",
    "anomaly_2_change_pct": -4.5,
    "anomaly_3": "픽채널 출근 (07-09시)",
    "anomaly_3_change_pct": 8.3,
    "daily_kpi_rows": "| 파워FM | 1.2M -2.1% | ... | | 러브FM | 980K +1.2% | ... |",

    # 편성PD 2_cause
    "target_timeslot": "파워FM 오후 (14-18시)",
    "demographics_rows": (
        "| 20대 여성 | 12% | 9% | -3pp |\n"
        "| 30대 여성 | 18% | 21% | +3pp |\n"
        "| 40대 남성 | 22% | 25% | +3pp |"
    ),
    "new_user": 8500,
    "prev_new_user": 11200,
    "top_new_user_timeslot": "픽채널 출근 (07-09시)",
    "adjacent_channel_summary": "러브FM 오후 -1.2% / 고릴라M 오후 +0.5% / 픽채널 오후 +6.7% → 픽채널 이동 가능성",

    # 편성PD 3_context
    "curr_q": 285000,
    "prev_q": 308000,
    "prev_year_q": 312000,
    "qoq_change_pct": -7.5,
    "yoy_change_pct": -8.7,
    "dau_definition": "1일 1회 이상 1분 이상 청취한 고유 사용자 (PERIOD=1D)",
    "new_user_definition": "기준일 이전 30일간 첫 청취 또는 90일 이상 미접속 후 복귀",
    "trend_interpretation": "3분기 연속 감소 — 구조적 추세 가능성",
    "wow_pct": -2.8,
    "mom_pct": -4.1,

    # 편성PD 4_loop
    "decision_outcome_rows": (
        "| 2026-04-01 | 14시 슬롯 진행자 교체 | 308,000 | 285,000 | -7.5% |\n"
        "| 2026-03-15 | 픽채널 출근 신규 프로그램 | 78,000 | 88,000 | +12.8% |"
    ),
    "notable_decision": "14시 슬롯 진행자 교체",
    "user_hypothesis": "이 시간대에 음악 중심 프로그램이 가능한가",
    "hypothesis_evaluation": "타 채널 동시간대 음악 비중 프로그램 분석 결과: 30대 여성 +5pp, 평균 청취 +2분",
    "evidence_summary": "동시간대 음악 프로그램 평균 청취자 18만, 잠재 청취층 인구통계 일치도 72%",

    # 편성PD 5_closing
    "trend_summary": "3분기 연속 감소 — 구조 변화 가능성",
    "demographics_summary": "20대 여성 -3pp, 40대 남성 +3pp (청취층 노령화)",
}


# ─── 렌더링 헬퍼 ──────────────────────────────────────────────────────────
def render(template: str, data: dict) -> str:
    """{변수} 또는 {변수:포맷} 치환. 누락 변수는 [{변수명}?] 로 표시."""
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


def show_box(title: str, body: str) -> None:
    """답변을 박스로 감쌈."""
    width = 78
    print(f"\n┌─ {title} " + "─" * max(0, width - len(title) - 4))
    for ln in body.split("\n"):
        print(f"│ {ln}")
    print("└" + "─" * (width - 1))


def show_chips(chips: list, prefix: str = "  ") -> None:
    print()
    for i, c in enumerate(chips, 1):
        next_s = c.get("next_slot", "?")
        out_id = c.get("output_format_id")
        suffix = f"  →  {next_s}"
        if out_id:
            suffix += f"  (출력: {out_id})"
        print(f"{prefix}[{i}] {c['label']}{suffix}")


def get_choice(n: int, auto: bool = False) -> int | None:
    if auto:
        print("선택 (자동): 1")
        return 0
    while True:
        s = input("\n선택 (1~{}, q=종료): ".format(n)).strip()
        if s.lower() in ("q", "quit", "exit"):
            return None
        try:
            i = int(s)
            if 1 <= i <= n:
                return i - 1
        except ValueError:
            pass
        print(f"  ⚠ 1~{n} 또는 q 입력")


def is_slot_blocked(slot: dict, mode: str) -> bool:
    """슬롯의 data_requirements 기반 차단 여부 판단."""
    if mode == "fallback":
        return True
    if mode == "happy":
        return False
    reqs = slot.get("data_requirements", [])
    high_blocked = [
        r for r in reqs
        if r.get("available") is False and r.get("blocks_quality") == "high"
    ]
    return len(high_blocked) > 0


# ─── 메인 시뮬레이션 ─────────────────────────────────────────────────────
def run(role: str, mode: str = "happy", auto: bool = False) -> None:
    cfg_path = STORYLINES_DIR / f"{role}.json"
    if not cfg_path.exists():
        available = sorted(p.stem for p in STORYLINES_DIR.glob("*.json"))
        print(f"⚠ {cfg_path} 없음.")
        print(f"   사용 가능: {', '.join(available)}")
        return
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    print("\n" + "═" * 78)
    print(f"  RAAS 스토리라인 시뮬레이터")
    print(f"  직무: {cfg['role_name_ko']} ({cfg['role']})")
    print(f"  모드: {mode}  (happy=모든 데이터 / fallback=데이터 부족 / auto=요건 기반)")
    print(f"  소스: {cfg_path.name}")
    print("═" * 78)

    # Entry
    entry = cfg["entry"]
    greeting = render(entry["greeting_template"], DUMMY)
    first_q = render(entry["first_question"], DUMMY)
    show_box("🤖 RAAS — 인사 + 첫 질문 (entry)", f"{greeting}\n\n{first_q}")
    show_chips(entry["default_chips"])

    current_chips = entry["default_chips"]
    visited = []

    while True:
        choice = get_choice(len(current_chips), auto=auto)
        if choice is None:
            print("\n👋 종료.")
            break

        chip = current_chips[choice]
        next_slot = chip.get("next_slot")
        print(f"\n👤 You: {chip['label']}")

        # 산출물 출력으로 종료
        if next_slot == "_end":
            output_id = chip.get("output_format_id", "?")
            outputs = {o["id"]: o for o in cfg["ontology"]["outputsTo"]}
            o = outputs.get(output_id, {"name": output_id, "format": "?"})
            show_box(
                "🤖 RAAS — 산출물 생성 (5_closing → _end)",
                f"✅ {o['name']} 생성 완료\n"
                f"   형식: {o.get('format', '?')}\n"
                f"   intent: {chip['intent']}\n"
                f"   (실제 변환은 Phase 3 ⑥에서 구현 예정)",
            )
            print(f"\n📊 방문 슬롯 ({len(visited)}개): {' → '.join(visited)}")
            print(f"📤 산출물: {o['name']}")
            break

        if next_slot not in cfg["slots"]:
            print(f"  ⚠ 알 수 없는 next_slot: {next_slot}")
            break

        slot = cfg["slots"][next_slot]
        visited.append(next_slot)
        blocked = is_slot_blocked(slot, mode)
        template = (
            slot.get("fallback_answer_template") if blocked else slot.get("answer_template")
        ) or slot.get("answer_template", "(답변 템플릿 없음)")

        answer = render(template, DUMMY)
        title = f"🤖 RAAS — {next_slot}: {slot['name']}"
        if blocked:
            title += " [FALLBACK]"
        show_box(title, answer)

        current_chips = slot.get("chips_next", [])
        if not current_chips:
            print("\n📊 더 이상 칩 없음 — 슬롯 종료")
            break
        show_chips(current_chips)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="RAAS 직무별 스토리라인 터미널 시뮬레이터"
    )
    ap.add_argument("role", help="cp | pd_program | pd_schedule")
    ap.add_argument(
        "--mode",
        choices=["happy", "fallback", "auto"],
        default="happy",
        help="happy=모든 데이터 가정 / fallback=강제 차단 / auto=요건 기반",
    )
    ap.add_argument("--auto", action="store_true", help="모든 선택을 1번으로 자동 진행")
    args = ap.parse_args()
    run(args.role, mode=args.mode, auto=args.auto)


if __name__ == "__main__":
    main()
