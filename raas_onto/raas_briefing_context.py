"""
RAAS 브리핑/질의 컨텍스트 빌더 (Phase 6-A)

기존 claude_context에 온톨로지 정보를 추가해 LLM에 전달.
환각 방지와 답변 품질 향상이 목적.

설계 원칙:
- 기존 claude_context는 변형 없이 보존
- 온톨로지 정보를 별도 섹션으로 추가
- 어댑터 메서드 활용 (find_top_guests, detect_consecutive_trend 등)
- 빈 정보는 섹션 자체를 생략
"""
from __future__ import annotations
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def build_briefing_context(
    claude_context: str,
    kpi_data: dict,
    target_date: str = None,
) -> str:
    """기존 claude_context에 온톨로지 정보 추가 (브리핑용).
    
    Args:
        claude_context: briefing_engine.collect_all() 결과의 claude_context
        kpi_data: collect_all() 전체 결과 dict
        target_date: 브리핑 기준 날짜 (YYYY-MM-DD), 미지정 시 오늘
    
    Returns:
        온톨로지 정보가 추가된 풍부한 컨텍스트
    """
    try:
        from raas_ontology_adapter import get_adapter
        adapter = get_adapter()
    except Exception as e:
        logger.warning(f"어댑터 로드 실패: {e}")
        return claude_context  # 폴백
    
    if not target_date:
        target_date = datetime.now().strftime("%Y-%m-%d")
    
    sections = []
    
    # ─── 섹션 1: DayType 정보 ──────────────────────────────────────────
    try:
        daytype_info = adapter.get_daytype_comparison(target_date)
        if daytype_info and daytype_info.get("comparison_text"):
            sections.append(
                "[DayType 정보]\n"
                f"{daytype_info['comparison_text']}\n"
                f"오늘은 {daytype_info.get('day_of_week', '?')} ({daytype_info.get('day_type', '?')})입니다.\n"
            )
    except Exception as e:
        logger.warning(f"DayType 정보 조회 실패: {e}")
    
    # ─── 섹션 2: 주목할 프로그램 + 게스트 효과 ───────────────────────
    try:
        # all_program_kpi가 어디에 있는지 확인하여 추출
        all_kpi = {}
        s5 = kpi_data.get("s5_rankings", {}) if isinstance(kpi_data, dict) else {}
        
        # s5_rankings의 구조에 따라 추출 (운영 환경 적응)
        if isinstance(s5, dict):
            # 1) all_program_kpi 직접
            all_kpi = s5.get("all_program_kpi", {})
            # 2) 없으면 risk_list/top_programs에서 합치기
            if not all_kpi:
                for key in ["top_programs", "risk_list", "growing_programs"]:
                    items = s5.get(key, [])
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and item.get("code"):
                                all_kpi[item["code"]] = item
        
        if all_kpi:
            notable = adapter.select_notable_programs(all_kpi, max_count=2)
            if notable:
                lines = ["[주목할 프로그램 후보]"]
                for prog in notable:
                    code = prog.get("code", "?")
                    label = prog.get("label", code)
                    dau = prog.get("dau", 0)
                    wow = prog.get("wow_pct", 0)
                    arrow = "▲" if wow > 0 else "▼"
                    lines.append(
                        f"- {code} {label}: DAU {dau:,}명, WoW {wow:+.1f}% {arrow}"
                    )
                    if prog.get("host"):
                        lines.append(f"  진행자: {prog['host']}")
                    
                    # 변화 큰 경우 게스트 효과 분석 추가
                    if abs(wow) >= 10:
                        try:
                            top_guests = adapter.find_top_guests(code, days=30, top_n=3)
                            if top_guests:
                                lines.append(f"  최근 비고정 게스트 효과 (RegularGuest 제외):")
                                for g in top_guests[:3]:
                                    lines.append(
                                        f"    {g['name']} ({g['appearances']}회 출연): "
                                        f"{g['effect_pct']:+.1f}% [{g['baseline_daytype']}]"
                                    )
                        except Exception as e:
                            logger.warning(f"게스트 분석 실패 ({code}): {e}")
                
                sections.append("\n".join(lines))
    except Exception as e:
        logger.warning(f"주목할 프로그램 분석 실패: {e}")
    
    # ─── 섹션 3: 연속 추세 감지 ──────────────────────────────────────
    try:
        timeline = kpi_data.get("_timeline", {}) if isinstance(kpi_data, dict) else {}
        if timeline:
            trend_lines = ["[연속 추세 감지]"]
            
            # T00 (전체) DAU 추세
            t00_data = timeline.get("T00", {})
            if t00_data:
                sorted_dates = sorted(t00_data.keys())[-7:]
                if len(sorted_dates) >= 3:
                    dau_values = [t00_data[d].get("dau_today") for d in sorted_dates]
                    trend = adapter.detect_consecutive_trend(dau_values)
                    if trend.get("consecutive_days", 0) >= 3:
                        arrow = "▲" if trend["direction"] == "up" else "▼"
                        trend_lines.append(
                            f"- 전체 DAU: {trend['consecutive_days']}일 연속 {arrow} "
                            f"({trend['total_change_pct']:+.1f}%)"
                        )
            
            # 깊은청취율 추세
            if t00_data:
                sorted_dates = sorted(t00_data.keys())[-7:]
                if len(sorted_dates) >= 3:
                    deep_values = [t00_data[d].get("deep_rate") for d in sorted_dates]
                    trend = adapter.detect_consecutive_trend(deep_values)
                    if trend.get("consecutive_days", 0) >= 3:
                        arrow = "▲" if trend["direction"] == "up" else "▼"
                        trend_lines.append(
                            f"- 깊은청취율: {trend['consecutive_days']}일 연속 {arrow} "
                            f"({trend['total_change_pct']:+.1f}pp)"
                        )
            
            # 추세가 감지된 경우만 추가
            if len(trend_lines) > 1:
                sections.append("\n".join(trend_lines))
    except Exception as e:
        logger.warning(f"연속 추세 감지 실패: {e}")
    
    # ─── 통합 ─────────────────────────────────────────────────────────
    if sections:
        ontology_part = "\n\n=== 온톨로지 컨텍스트 ===\n" + "\n\n".join(sections) + "\n=== 온톨로지 끝 ===\n"
        return claude_context + ontology_part
    
    return claude_context


def build_query_context(question: str, claude_context: str) -> str:
    """질문에 맞춰 온톨로지 정보를 동적 추출하여 컨텍스트 빌드.
    
    Args:
        question: 사용자 자연어 질문
        claude_context: 기존 데이터 컨텍스트
    
    Returns:
        온톨로지 컨텍스트가 추가된 결과
    """
    try:
        from raas_ontology_adapter import get_adapter
        adapter = get_adapter()
    except Exception as e:
        logger.warning(f"어댑터 로드 실패: {e}")
        return claude_context
    
    sections = []
    
    # ─── 1. 질문에서 프로그램 키워드 추출 ──────────────────────────
    matched_programs = []
    seen_codes = set()
    
    try:
        for prog in adapter.get_all_programs():
            code = prog.get("code")
            if code in seen_codes:
                continue
            
            labels = [prog.get("label", "")] + prog.get("alt_labels", [])
            for label in labels:
                if label and len(label) >= 2 and label in question:
                    matched_programs.append(prog)
                    seen_codes.add(code)
                    break
    except Exception as e:
        logger.warning(f"프로그램 매칭 실패: {e}")
    
    # ─── 2. 질문에서 사람 이름 추출 ────────────────────────────────
    matched_persons = []
    seen_persons = set()
    
    try:
        # 일단 모든 RegularGuest 이름 후보로 검색
        regular_names = adapter._get_regular_guest_names()
        for name in regular_names:
            if name in question:
                matches = adapter.find_person_by_name(name)
                for m in matches:
                    pid = m["person"]["id"]
                    if pid not in seen_persons:
                        matched_persons.append(m)
                        seen_persons.add(pid)
        
        # 매칭된 프로그램의 진행자도 추가
        for prog in matched_programs:
            host = prog.get("main_host")
            if host and host.get("id") not in seen_persons:
                matched_persons.append({"person": host, "as_main_host": [prog["code"]], "as_regular_guest": []})
                seen_persons.add(host["id"])
    except Exception as e:
        logger.warning(f"인물 매칭 실패: {e}")
    
    # ─── 3. 컨텍스트 조립 ──────────────────────────────────────────
    if matched_programs:
        lines = ["[관련 프로그램]"]
        for prog in matched_programs:
            line = f"- {prog['code']} {prog.get('label', '?')}"
            lines.append(line)
            
            # 진행자
            host = prog.get("main_host")
            if host:
                host_line = f"  진행자: {host.get('label', '?')}"
                if host.get("occupation"):
                    host_line += f" ({host['occupation']})"
                lines.append(host_line)
            
            # ProgramType (자동 음악 채널)
            ptype = prog.get("program_type", {})
            if ptype.get("id") == "raas:AutomatedProgram":
                lines.append(f"  유형: {ptype.get('label', '자동 음악')} (DJ 없음)")
            
            # 고정 게스트
            regulars = prog.get("regular_guests", [])
            if regulars:
                names = [g.get("label", "?") for g in regulars]
                lines.append(f"  고정 게스트: {', '.join(names)}")
            
            # 시간대
            ts = prog.get("time_slot", {})
            if ts.get("start"):
                lines.append(f"  방송 시간: {ts['start']}-{ts.get('end', '?')}")
        
        sections.append("\n".join(lines))
    
    if matched_persons:
        lines = ["[관련 인물]"]
        for m in matched_persons:
            person = m["person"]
            line = f"- {person.get('label', '?')}"
            if person.get("occupation"):
                line += f" ({person['occupation']})"
            lines.append(line)
            
            if m.get("as_main_host"):
                progs = ", ".join(m["as_main_host"])
                lines.append(f"  진행: {progs}")
            if m.get("as_regular_guest"):
                progs = ", ".join(m["as_regular_guest"])
                lines.append(f"  고정 출연: {progs}")
        
        sections.append("\n".join(lines))
    
    # ─── 통합 ─────────────────────────────────────────────────────
    if sections:
        ontology_part = "\n\n=== 온톨로지 컨텍스트 ===\n" + "\n\n".join(sections) + "\n=== 온톨로지 끝 ===\n"
        return claude_context + ontology_part
    
    return claude_context


# ─── 자체 테스트 ────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("브리핑/질의 컨텍스트 빌더 자체 테스트")
    print("=" * 70)
    
    # 테스트 1: 질의 컨텍스트
    test_questions = [
        "F03 파워 스테이션 진행자 누구야?",
        "정상근이 출연하는 프로그램?",
        "이재익이 어디 나와?",
        "5/1 김영철의 파워FM 어땠어?",
    ]
    
    base_context = "날짜: 2026-05-03\nDAU: 91,235명\n"
    
    for q in test_questions:
        print(f"\n질문: {q}")
        result = build_query_context(q, base_context)
        # 추가된 부분만 출력
        if "온톨로지 컨텍스트" in result:
            onto = result.split("=== 온톨로지 컨텍스트 ===")[1].split("=== 온톨로지 끝 ===")[0]
            print("추가된 온톨로지 컨텍스트:")
            print(onto)
        else:
            print("(온톨로지 컨텍스트 추가 없음)")
    
    print("\n" + "=" * 70)
    print("브리핑 컨텍스트 테스트 (kpi_data 가짜)")
    print("=" * 70)
    
    fake_kpi = {
        "s5_rankings": {
            "all_program_kpi": {
                "F05": {"dau": 14765, "dau_chg": -16.2, "churn_rate": 49.4},
                "F09": {"dau": 18699, "dau_chg": -1.7, "churn_rate": 52.4},
            }
        },
        "_timeline": {}
    }
    
    result = build_briefing_context(base_context, fake_kpi, target_date="2026-05-03")
    print(result)
