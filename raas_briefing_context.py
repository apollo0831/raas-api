"""
RAAS 브리핑/질의 컨텍스트 빌더 (Phase 6-A)
raas_server.py에서 call_claude() 호출 전에 온톨로지 정보를 기존 claude_context에 주입.
"""

import logging
import sys
import os

logger = logging.getLogger(__name__)

# 어댑터는 raas_onto/ 서브디렉토리에 있음
_ONTO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raas_onto")
if _ONTO_DIR not in sys.path:
    sys.path.insert(0, _ONTO_DIR)


def _get_adapter():
    try:
        from raas_ontology_adapter import get_adapter
        return get_adapter()
    except Exception as e:
        logger.warning(f"어댑터 로드 실패: {e}")
        return None


def build_briefing_context(claude_context: str, kpi_data: dict, target_date: str) -> str:
    """기존 claude_context에 온톨로지 정보 추가.

    Args:
        claude_context: briefing_engine.collect_all()["claude_context"]
        kpi_data:       briefing_engine.collect_all() 전체 반환값
        target_date:    "YYYY-MM-DD" 형식 날짜
    Returns:
        enriched_context: claude_context + DayType 정보 + 주목 프로그램 + 추세
    """
    adapter = _get_adapter()
    if adapter is None:
        return claude_context

    sections = []

    # 1. DayType 정보 + 패턴 지침
    try:
        daytype_info = adapter.get_daytype_comparison(target_date)
        if daytype_info:
            dt = daytype_info.get("day_type", "")
            weekday = daytype_info.get("day_of_week", "")
            holiday = daytype_info.get("holiday_name", "")
            comparison_text = daytype_info.get("comparison_text", "")

            lines = ["[DayType 정보]", comparison_text]

            if dt == "주말":
                lines.append(
                    f"⚠️ 주말 패턴 적용: {weekday}은 평일 대비 DAU 30~50% 낮음이 정상."
                )
                lines.append("→ '급락·긴급 점검' 표현 금지. WoW는 전주 동일 요일(주말) 기준만 사용.")
                lines.append("→ 05 액션 추천은 '주말 정상 패턴 — 다음 평일 추이 모니터링' 수준으로 처리.")
            elif dt == "공휴일":
                name = holiday if holiday else "공휴일"
                lines.append(
                    f"⚠️ 공휴일 패턴 적용({name}): 청취 감소는 자연 현상."
                )
                lines.append("→ '긴급 점검·이탈 우려' 표현 금지.")
                lines.append("→ 05 액션 추천은 '공휴일 패턴 — 다음 평일 회복 모니터링' 수준으로 처리.")
            else:
                lines.append(f"→ 평일({weekday}) 기준 비교. 전주 동일 요일 대비 WoW 사용.")

            sections.append("\n".join(lines))
    except Exception as e:
        logger.warning(f"DayType 조회 실패: {e}")

    # 2. 주목할 프로그램 선정 + 게스트 효과
    try:
        all_kpi = kpi_data.get("s5_rankings", {}).get("all_program_kpi", {})
        if all_kpi:
            notable = adapter.select_notable_programs(all_kpi, max_count=2)
            if notable:
                notable_lines = ["[주목할 프로그램 후보]"]
                for prog in notable:
                    line = (
                        f"- {prog['code']} {prog['label']}: "
                        f"DAU {prog['dau']:,}명, WoW {prog['wow_pct']:+.1f}%"
                    )
                    notable_lines.append(line)
                    if prog.get("host"):
                        notable_lines.append(f"  진행자: {prog['host']}")

                    # WoW ±10% 이상이면 게스트 효과도 조회
                    if abs(prog["wow_pct"]) >= 10:
                        try:
                            top_guests = adapter.find_top_guests(
                                prog["code"], days=30, top_n=3
                            )
                            if top_guests:
                                notable_lines.append("  최근 게스트 효과:")
                                for g in top_guests:
                                    notable_lines.append(
                                        f"    {g['name']} ({g['appearances']}회): "
                                        f"{g['effect_pct']:+.1f}%"
                                    )
                        except Exception as e:
                            logger.debug(f"게스트 효과 조회 실패 ({prog['code']}): {e}")

                sections.append("\n".join(notable_lines))
    except Exception as e:
        logger.warning(f"주목 프로그램 선정 실패: {e}")

    # 3. 연속 추세 감지 (T00 DAU 기준)
    try:
        timeline = kpi_data.get("_timeline", {})
        t00_data = timeline.get("T00", {})
        t00_dates = sorted(t00_data.keys())[-7:]

        if len(t00_dates) >= 3:
            dau_values = [
                t00_data[d].get("dau_today") for d in t00_dates
            ]
            dau_values = [v for v in dau_values if v is not None]

            if len(dau_values) >= 3:
                trend = adapter.detect_consecutive_trend(dau_values)
                if trend.get("consecutive_days", 0) >= 3:
                    arrow = "▲" if trend["direction"] == "up" else "▼"
                    sections.append(
                        f"[연속 추세 감지]\n"
                        f"- 전체 DAU: {trend['consecutive_days']}일 연속 {arrow} "
                        f"({trend['total_change_pct']:+.1f}%)"
                    )
    except Exception as e:
        logger.warning(f"연속 추세 감지 실패: {e}")

    if not sections:
        return claude_context

    onto_block = "\n\n".join(sections)
    return f"{claude_context}\n\n{onto_block}"


def build_query_context(question: str, claude_context: str) -> str:
    """질의용 컨텍스트 빌드 — 질문에 맞춰 동적 추출.

    Args:
        question:       사용자 질문 원문
        claude_context: 기존 KPI 컨텍스트
    Returns:
        enriched_context: claude_context + 온톨로지 메타정보
    """
    adapter = _get_adapter()
    if adapter is None:
        return claude_context

    matched_programs = []
    matched_persons = []

    # 1. 질문에서 프로그램 키워드 매칭
    try:
        for prog in adapter.get_all_programs():
            labels = [prog.get("label", "")] + prog.get("alt_labels", [])
            for lbl in labels:
                if lbl and len(lbl) >= 2 and lbl in question:
                    matched_programs.append(prog)
                    break
    except Exception as e:
        logger.warning(f"프로그램 키워드 매칭 실패: {e}")

    # 2. 진행자·고정 게스트 이름 매칭
    try:
        for prog in matched_programs:
            host = prog.get("main_host")
            if host and host.get("label"):
                matched_persons.append(host)

        # 알려진 RegularGuest 직접 검색
        for person_kw in ["정상근", "이재익", "김도형"]:
            if person_kw in question:
                matches = adapter.find_person_by_name(person_kw)
                for m in matches:
                    matched_persons.append(m["person"])
    except Exception as e:
        logger.warning(f"인물 매칭 실패: {e}")

    if not matched_programs and not matched_persons:
        return claude_context

    onto_lines = ["[온톨로지 컨텍스트]"]

    if matched_programs:
        onto_lines.append("관련 프로그램:")
        for prog in matched_programs:
            onto_lines.append(f"- {prog['code']} {prog.get('label', prog['code'])}")
            host = prog.get("main_host")
            ptype = prog.get("program_type", {})
            if host and host.get("label"):
                onto_lines.append(f"  진행자: {host['label']}")
            elif ptype.get("id") == "raas:AutomatedProgram":
                onto_lines.append(f"  유형: {ptype.get('label', '')} (DJ 없음)")
            regulars = prog.get("regular_guests", [])
            if regulars:
                names = [g.get("label", "") for g in regulars if g.get("label")]
                onto_lines.append(f"  고정 게스트: {', '.join(names)}")

    if matched_persons:
        onto_lines.append("\n관련 인물:")
        seen = set()
        for p in matched_persons:
            pid = p.get("id")
            if pid in seen:
                continue
            seen.add(pid)
            line = f"- {p.get('label', '')}"
            if p.get("occupation"):
                line += f" ({p['occupation']})"
            onto_lines.append(line)

    onto_block = "\n".join(onto_lines)
    return f"{claude_context}\n\n{onto_block}"
