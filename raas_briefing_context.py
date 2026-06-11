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

    # ── 0. 지표 정의 — 분석·인사이트 intent에만 주입 ──────────────
    # ranking·trend 계열은 지표 정의 없이도 충분하므로 생략해 토큰 절감
    _SKIP_METRIC_DEF = {'ranking', 'trend', 'compare_trend', 'dual_trend'}
    if intent_type not in _SKIP_METRIC_DEF:
        try:
            metric_def_block = adapter.get_metric_definitions_block()
            if metric_def_block:
                sections.append(metric_def_block)
        except Exception as e:
            logger.debug(f"지표 정의 블록 생성 실패: {e}")

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

    # ── A-2. 게스트 데이터 해석 규칙 (게스트 관련 질의에만 주입) ────
    _guest_kws = ['게스트', '초대손님', '손님', 'guestname', '게스트명', '출연']
    if any(kw in question for kw in _guest_kws):
        sections.append(
            "[게스트 데이터 해석]\n"
            "- guestname 필드가 비어있으면 해당 날짜에 초대손님이 없었던 것임"
            " — '데이터가 없다', '정보가 제공되지 않았다' 등의 표현 금지\n"
            "- 채널(파워FM 등)이나 전체 단위의 guestname은 항상 비어있음"
            " — 게스트 질문 시 컨텍스트에 프로그램별로 합산된 [게스트 출연 현황] 섹션이 제공됨"
        )

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

    onto_block = "\n\n".join(sections)
    if not onto_block.strip():
        return claude_context
    return f"{claude_context}\n\n{onto_block}"
