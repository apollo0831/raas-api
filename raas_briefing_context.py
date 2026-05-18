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


def _build_period_focus(period: str, kpi_data: dict) -> str:
    """기간(일/주/월)별 핵심 지표 포커스 텍스트 생성."""
    s1 = kpi_data.get("s1_executive", {}) or {}
    s2 = kpi_data.get("s2_funnel",    {}) or {}
    s3 = kpi_data.get("s3_engagement",{}) or {}

    def _f(v, d="—"):
        return v if v is not None else d

    if period == "week":
        lines = [
            "[기간 포커스: 주간(WAU)]",
            f"분석 기준: 7일 롤링 활성 사용자(WAU) 중심으로 작성",
            f"WAU {_f(s1.get('dau_week'),0):,}명 (WoW {_f(s1.get('dau_week_wow'),0):+.1f}%)"
            if isinstance(s1.get('dau_week'), (int, float)) else
            f"WAU {_f(s1.get('dau_week'))} (WoW {_f(s1.get('dau_week_wow'))}%)",
            f"주간 신규 {_f(s2.get('new_user_week'),'—')} | 이탈률 {_f(s2.get('churn_week'),'—')}% | 복귀율 {_f(s2.get('react_week'),'—')}%",
            f"W1 유지율 {_f(s2.get('w1_ret'),'—')}% (전주 대비 {_f(s2.get('new_w1_ret_diff'),'—')}pp)",
            f"주간 깊은청취율 {_f(s3.get('deep_rate_week'),'—')}% | 참여율 {_f(s3.get('engage_week'),'—')}%",
            "→ 01 핵심지표는 WAU 기준으로 작성. 일간 DAU 수치 사용 금지.",
        ]
    elif period == "mon":
        lines = [
            "[기간 포커스: 월간(MAU)]",
            f"분석 기준: 30일 롤링 활성 사용자(MAU) 중심으로 작성",
            f"MAU {_f(s1.get('dau_mon'),0):,}명 (MoM {_f(s1.get('dau_mon_wow'),0):+.1f}%)"
            if isinstance(s1.get('dau_mon'), (int, float)) else
            f"MAU {_f(s1.get('dau_mon'))} (MoM {_f(s1.get('dau_mon_wow'))}%)",
            f"월간 신규 {_f(s2.get('new_user_mon'),'—')} | 이탈률 {_f(s2.get('churn_mon'),'—')}% | 복귀율 {_f(s2.get('react_mon'),'—')}%",
            f"M1 유지율 {_f(s2.get('m1_ret'),'—')}% (전월 대비 {_f(s2.get('new_m1_ret_diff'),'—')}pp)",
            f"월간 깊은청취율 {_f(s3.get('deep_rate_mon'),'—')}% | 참여율 {_f(s3.get('engage_mon'),'—')}%",
            "→ 01 핵심지표는 MAU 기준으로 작성. 일간/주간 수치 사용 금지.",
        ]
    else:  # day
        lines = [
            "[기간 포커스: 일간(DAU)]",
            f"분석 기준: 일간 활성 사용자(DAU) 중심으로 작성",
            f"DAU {_f(s1.get('dau'),0):,}명 (WoW {_f(s1.get('dau_wow'),0):+.1f}%)"
            if isinstance(s1.get('dau'), (int, float)) else
            f"DAU {_f(s1.get('dau'))} (WoW {_f(s1.get('dau_wow'))}%)",
            f"D1 유지율 {_f(s2.get('d1_ret'),'—')}% | D7 유지율 {_f(s2.get('d7_ret'),'—')}%",
            f"일간 이탈률 {_f(s2.get('churn_rate'),'—')}% | 복귀율 {_f(s2.get('react_rate'),'—')}%",
            f"깊은청취율 {_f(s3.get('deep_rate'),'—')}%",
            "→ 01 핵심지표는 DAU 기준으로 작성.",
        ]

    return "\n".join(lines)


def build_briefing_context(claude_context: str, kpi_data: dict, target_date: str,
                           period: str = "day") -> str:
    """기존 claude_context에 온톨로지 정보 추가.

    Args:
        claude_context: briefing_engine.collect_all()["claude_context"]
        kpi_data:       briefing_engine.collect_all() 전체 반환값
        target_date:    "YYYY-MM-DD" 형식 날짜
        period:         "day" | "week" | "mon"
    Returns:
        enriched_context: claude_context + 기간 포커스 + DayType 정보 + 주목 프로그램 + 추세
    """
    adapter = _get_adapter()
    if adapter is None:
        return claude_context

    sections = []

    # 0. 기간 포커스 (일/주/월)
    try:
        sections.append(_build_period_focus(period, kpi_data))
    except Exception as e:
        logger.warning(f"기간 포커스 생성 실패: {e}")

    # 1. DayType 정보 + 패턴 지침 (오늘 + 전일 + 주중 공휴일)
    try:
        from datetime import datetime as _dt, timedelta as _td

        daytype_info = adapter.get_daytype_comparison(target_date)

        # 전일 DayType (일간 전일 대비 급변 해석용)
        try:
            prev_date = (_dt.strptime(target_date, "%Y-%m-%d") - _td(days=1)).strftime("%Y-%m-%d")
            prev_info = adapter.get_daytype_comparison(prev_date)
        except Exception:
            prev_info = {}

        # 최근 7일 공휴일 목록 (주간 WAU 해석용)
        week_holidays = []
        try:
            base = _dt.strptime(target_date, "%Y-%m-%d")
            for i in range(1, 8):
                d = (base - _td(days=i)).strftime("%Y-%m-%d")
                di = adapter.get_daytype_comparison(d)
                if di and di.get("day_type") == "공휴일" and di.get("holiday_name"):
                    week_holidays.append(f"{d[5:]} {di['holiday_name']}")  # MM-DD 이름
        except Exception:
            pass

        if daytype_info:
            dt      = daytype_info.get("day_type", "")
            weekday = daytype_info.get("day_of_week", "")
            holiday = daytype_info.get("holiday_name", "")

            lines = ["[DayType 정보]", daytype_info.get("comparison_text", "")]

            if dt == "주말":
                lines.append(f"⚠️ 오늘({weekday}) 주말 패턴: 평일 대비 DAU 30~50% 낮음이 정상.")
                lines.append(f"→ 급락 언급 시 '오늘({weekday}, 주말) 패턴으로 정상 감소' 문구로 명시할 것.")
                lines.append("→ WoW는 전주 동일 요일(주말) 기준만 사용.")
                lines.append("→ 05 액션: '주말 정상 패턴 — 다음 평일 추이 모니터링'")

            elif dt == "공휴일":
                name = holiday or "공휴일"
                lines.append(f"⚠️ 오늘({weekday}) 공휴일({name}) 패턴: 청취 감소는 자연 현상.")
                lines.append(f"→ 급락 언급 시 반드시 '공휴일({name}) 영향'으로 공휴일명까지 명시할 것.")
                lines.append(f"→ 05 액션: '공휴일({name}) 패턴 — 다음 평일 회복 모니터링'")

            else:
                lines.append(f"→ 오늘은 {weekday}(평일). 전주 동일 요일 대비 WoW 사용.")

            # 전일 DayType — 오늘 일간 급증 원인 판단용
            if prev_info:
                prev_dt      = prev_info.get("day_type", "")
                prev_weekday = prev_info.get("day_of_week", "")
                prev_holiday = prev_info.get("holiday_name", "")
                if prev_dt == "주말":
                    lines.append(
                        f"→ 전일({prev_date[5:]}, {prev_weekday})은 주말 —"
                        f" 일간 DAU 급증이 있다면 주말→평일 반등 정상 패턴으로 명시할 것."
                    )
                elif prev_dt == "공휴일":
                    pname = prev_holiday or "공휴일"
                    lines.append(
                        f"→ 전일({prev_date[5:]}, {prev_weekday})은 공휴일({pname}) —"
                        f" 일간 DAU 급증이 있다면 '공휴일({pname}) 다음 날 반등 패턴'으로 명시할 것."
                    )

            # 주중 공휴일 — 주간(WAU) 감소 원인 판단용
            if week_holidays:
                joined = ", ".join(week_holidays)
                lines.append(
                    f"→ 최근 7일 내 법정 공휴일: {joined} —"
                    f" 주간(WAU) 급락 언급 시 해당 공휴일명까지 명시할 것."
                )

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


def build_query_context(question: str, claude_context: str,
                        intent: dict = None, data: dict = None) -> str:
    """질의용 컨텍스트 빌드 — 질문·의도·데이터에 맞춰 온톨로지 정보를 동적으로 추가.

    Args:
        question:       사용자 질문 원문
        claude_context: format_for_claude() 결과 (KPI 수치 텍스트)
        intent:         classify_intent() 결과 (optional)
        data:           extract_data() 결과 (optional) — date_max, trend, risks 등 포함
    Returns:
        enriched_context: claude_context + 온톨로지 블록들
    """
    adapter = _get_adapter()
    if adapter is None:
        return claude_context

    intent = intent or {}
    data   = data   or {}
    intent_type = intent.get("intent", "general")
    sections = []

    # ── A. 프로그램·인물 메타 + 게스트 효과 ─────────────────────
    matched_programs = []
    matched_persons  = []

    try:
        for prog in adapter.get_all_programs():
            labels = [prog.get("label", "")] + prog.get("alt_labels", [])
            for lbl in labels:
                if lbl and len(lbl) >= 2 and lbl in question:
                    matched_programs.append(prog)
                    break
    except Exception as e:
        logger.warning(f"프로그램 키워드 매칭 실패: {e}")

    try:
        for prog in matched_programs:
            host = prog.get("main_host")
            if host and host.get("label"):
                matched_persons.append(host)
        for kw in ["정상근", "이재익", "김도형"]:
            if kw in question:
                for m in adapter.find_person_by_name(kw):
                    matched_persons.append(m["person"])
    except Exception as e:
        logger.warning(f"인물 매칭 실패: {e}")

    if matched_programs or matched_persons:
        onto_lines = ["[온톨로지 컨텍스트]"]
        if matched_programs:
            onto_lines.append("관련 프로그램:")
            for prog in matched_programs:
                onto_lines.append(f"- {prog['code']} {prog.get('label', prog['code'])}")
                host  = prog.get("main_host")
                ptype = prog.get("program_type", {})
                if host and host.get("label"):
                    onto_lines.append(f"  진행자: {host['label']}")
                elif ptype.get("id") == "raas:AutomatedProgram":
                    onto_lines.append(f"  유형: {ptype.get('label', '')} (DJ 없음)")
                regulars = prog.get("regular_guests", [])
                if regulars:
                    names = [g.get("label", "") for g in regulars if g.get("label")]
                    onto_lines.append(f"  고정 게스트: {', '.join(names)}")
                # 게스트 출연 효과 (브리핑에서만 하던 기능)
                try:
                    guests = adapter.find_top_guests(prog["code"], days=30, top_n=3)
                    if guests:
                        onto_lines.append("  최근 게스트 DAU 효과:")
                        for g in guests:
                            onto_lines.append(
                                f"    {g['name']} ({g['appearances']}회 출연): "
                                f"평균 {g['effect_pct']:+.1f}%"
                            )
                except Exception:
                    pass

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

        sections.append("\n".join(onto_lines))

    # ── B. DayType 컨텍스트 (공휴일·주말 패턴 해석 오류 방지) ────
    raw_date = data.get("date_max") or data.get("date_min")
    if raw_date:
        try:
            from datetime import datetime as _dt, timedelta as _td
            td = raw_date.replace("/", "-")   # "YYYY/MM/DD" → "YYYY-MM-DD"
            daytype_info = adapter.get_daytype_comparison(td)
            if daytype_info:
                dt      = daytype_info.get("day_type", "")
                weekday = daytype_info.get("day_of_week", "")
                holiday = daytype_info.get("holiday_name", "")
                lines = [f"[날짜·요일 컨텍스트] {daytype_info.get('comparison_text', '')}"]
                if dt == "주말":
                    lines.append(f"→ {weekday}(주말): 평일 대비 DAU 30~50% 낮음이 정상 패턴.")
                    lines.append("→ WoW는 전주 동일 요일(주말) 기준으로만 비교할 것.")
                elif dt == "공휴일":
                    lines.append(f"→ 공휴일({holiday or '공휴일'}): 청취 감소는 정상 패턴, 공휴일명 명시.")
                else:
                    lines.append(f"→ {weekday}(평일): 전주 동일 요일 WoW 기준 비교.")
                # 전일 DayType (급증 해석용)
                try:
                    prev = (_dt.strptime(td, "%Y-%m-%d") - _td(days=1)).strftime("%Y-%m-%d")
                    prev_info = adapter.get_daytype_comparison(prev)
                    if prev_info:
                        prev_dt = prev_info.get("day_type", "")
                        prev_hn = prev_info.get("holiday_name", "")
                        if prev_dt == "주말":
                            lines.append(f"→ 전일({prev[5:]})은 주말 → 오늘 DAU 급증이면 주말→평일 반등 정상 패턴.")
                        elif prev_dt == "공휴일":
                            lines.append(f"→ 전일({prev[5:]})은 공휴일({prev_hn or '공휴일'}) → 오늘 DAU 급증이면 공휴일 다음날 반등 패턴.")
                except Exception:
                    pass
                sections.append("\n".join(lines))
        except Exception as e:
            logger.debug(f"DayType 조회 실패: {e}")

    # ── C. 연속 추세 감지 (trend 데이터가 있을 때) ───────────────
    if data.get("trend"):
        try:
            trend_pairs = data["trend"].get("data", [])
            values = [v for _, v in trend_pairs if v is not None]
            if len(values) >= 3:
                trend = adapter.detect_consecutive_trend(values)
                days  = trend.get("consecutive_days", 0)
                if days >= 3:
                    metric = data["trend"].get("metric_field", "지표")
                    arrow  = "▲" if trend["direction"] == "up" else "▼"
                    chg    = trend.get("total_change_pct", 0) or 0
                    sections.append(
                        f"[연속 추세 감지]\n"
                        f"- {metric}: 최근 {days}일 연속 {arrow} ({chg:+.1f}%)\n"
                        f"  → 3일 이상 연속 변화 시 단발 노이즈가 아닌 추세 신호로 해석할 것."
                    )
        except Exception as e:
            logger.debug(f"연속 추세 감지 실패: {e}")

    # ── D. 위험 프로그램 목록 (risk·health intent 또는 위험 키워드) ─
    risk_kw = ["위험", "이탈", "급락", "급감", "감소", "문제", "리스크", "위기"]
    if (intent_type in ("risk", "health") or any(kw in question for kw in risk_kw)):
        risks = data.get("risks") or []
        if risks:
            try:
                risk_lines = ["[위험 프로그램 (비즈니스 룰 기준: DAU≥1000 & 이탈율≥30% & WoW≤-5%)]"]
                for r in risks[:3]:
                    risk_lines.append(
                        f"- {r.get('code','')} {r.get('name','')}: "
                        f"DAU {r.get('dau') or 0:,}명, "
                        f"이탈율 {r.get('churn_rate') or 0:.1f}%, "
                        f"WoW {r.get('dau_wow') or 0:+.1f}%"
                    )
                sections.append("\n".join(risk_lines))
            except Exception as e:
                logger.debug(f"위험 프로그램 블록 생성 실패: {e}")

    if not sections:
        return claude_context

    onto_block = "\n\n".join(sections)
    return f"{claude_context}\n\n{onto_block}"
