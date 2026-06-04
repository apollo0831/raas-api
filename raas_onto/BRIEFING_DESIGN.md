# RAAS AI 브리핑 품질 향상 — Phase 6-A 통합 설계

> Phase 6-B (Claude Code 통합 작업)의 작업 지시서.
> 이 문서를 입력으로 Claude Code 세션에서 실제 raas_server.py에 적용한다.

**문서 구조**:
1. 목표와 비목표
2. 현재 상태 진단 (실제 브리핑 결과 분석)
3. 데이터 흐름 변경
4. 새 브리핑 형식 명세 (5섹션)
5. 새 시스템 프롬프트 전문
6. 어댑터 메서드 활용 가이드
7. raas_server.py 변경 매트릭스
8. 검증 시나리오
9. Phase 6-B 체크리스트

---

# 챕터 1: 목표와 비목표

## 1.1 Phase 6-A의 목표

**최종 목표**: AI 브리핑이 매일 다른 의미 있는 인사이트를 제공하도록 한다. 환각을 제거하고 DayType·게스트·연속추세 같은 신규 정보를 활용해 답변 품질을 비약적으로 향상시킨다.

**구체 목표**:
1. 어댑터를 LLM 호출 경로에 연결 (현재 미연결)
2. DayType 인식으로 평일/주말/공휴일 정확히 비교
3. 게스트 효과 분석 자동 통합 (RegularGuest 제외)
4. 연속 추세(3일 이상) 자동 감지
5. 주목할 프로그램 1~2개 자동 선정 (단순 1위가 아닌 진짜 변화)
6. 환각 방지 가드레일 명시
7. 시스템 프롬프트 통합 및 강화

## 1.2 비목표 (이번 단계에서 하지 않는 것)

- ❌ Splunk SPL 쿼리 수정
- ❌ briefing_engine.py 대규모 변경 (claude_context 보강만)
- ❌ Phase 2-C-4 Episode 인스턴스화 (옵션 B 캐시로 충분)
- ❌ 새 KPI 추가
- ❌ 웹 UI 변경

## 1.3 성공 기준

다음을 모두 만족하면 Phase 6-A 성공:

1. **DayType 정확 인식**: 일요일 브리핑이 "주말 대비"로 비교됨 (현재는 평일과 비교)
2. **게스트 효과 활용**: 사용자 폭증 시 게스트 정보 자동 언급
3. **연속 추세 감지**: 3일 이상 같은 방향 변화 자동 표시
4. **환각 감소**: 평균값 추측, 출처 불명 수치 인용 사라짐
5. **회귀 테스트**: 57개 기존 테스트 + 신규 12개 모두 통과

---

# 챕터 2: 현재 상태 진단

## 2.1 실제 브리핑 결과 분석 (5/3 일요일)

```
01 / 핵심지표 요약
DAU 91,235명 (WoW -7.6%, 6일 평균 대비 -45.3% 급락)   ← 일요일이라 정상 감소
깊은청취율 일 88.1% (7일 추세 -3.3pp 하락)
파워FM 64,848명(점유 68.4%)이 견인, 전 채널 일제히 감소

02 / 주목할 점
전일 대비 DAU -51.8% 급락하며 7일 평균(155,930) 크게 하회   ← 토→일 정상 감소
김영철의파워FM(-36.7%), 최영주의러브FM(-32.5%) 등 동반 급감   ← 주말 효과 미반영

03 / 프로그램 하이라이트
1위 아름다운이아침봉태규 19,053명, 습관형성률 22.5%로 충성도 최상위
김영철의파워FM 이탈률 49.4%·WoW -16.2%로 이탈 방어 시급

04 / 액션 추천
DAU 급락 원인(외부이슈/지표오류) 긴급 점검 후 김영철·펀펀투데이 이탈자 푸시   ← 잘못된 권고
```

## 2.2 발견된 문제 5가지

### 문제 1 — DayType 인식 실패 (가장 심각)

5/3은 **일요일**인데 평일 평균(155,930)과 비교하여 "급락"으로 표현. 실제로는 정상 주말 패턴.

**잘못된 결과**:
- "긴급 점검" 권고 → 매일 같은 잘못된 경고 → 신뢰 저하
- "외부이슈/지표오류" 추측 → 실제로는 그냥 일요일

### 문제 2 — 게스트 효과 정보 전무

김영철의 파워FM -36.7% 감소했는데 원인 분석 없음:
- 주말 정상 감소인지
- 정상근(고정 게스트) 부재인지
- 다른 이유인지

### 문제 3 — 연속 추세 부분만 활용

깊은청취율은 "7일 추세 -3.3pp 하락" 잡았으나, DAU·이탈률·신규의 연속 변화는 미언급.

### 문제 4 — 주목할 프로그램이 매일 비슷

1위(봉태규) + 위험(김영철)이 거의 매일 동일. 사용자가 "오늘 이거 봐야 한다"는 정보를 못 얻음.

### 문제 5 — 환각 위험

"6일 평균", "7일 평균(155,930)" 같은 평균값의 출처와 산출 방식 불명. LLM이 임의 계산했을 가능성.

## 2.3 근본 원인

**어댑터가 LLM 호출 경로에 연결되지 않음**.

```python
# 현재 raas_server.py 라인 203~206
brief_text = call_claude(
    "SBS 고릴라 라디오 앱 데이터 분석 어시스턴트. 한국어로 간결하게. 수치는 천단위 쉼표.",  # 30자
    claude_prompt
)
```

- 어댑터의 `get_llm_context_for_query()`, `find_top_guests()` 등이 호출 안 됨
- 시스템 프롬프트 30자로 매우 짧음
- briefing_engine의 `claude_context`에 KPI 수치만 있고 온톨로지 정보 없음

---

# 챕터 3: 데이터 흐름 변경

## 3.1 현재 흐름

```
Splunk → briefing_engine.collect_all() → claude_context (KPI 수치만)
                                            ↓
                                       call_claude() (30자 시스템 프롬프트)
                                            ↓
                                       brief_text
```

## 3.2 신규 흐름

```
Splunk → briefing_engine.collect_all() → claude_context (KPI 수치)
                                            ↓
                          [신규] adapter.build_briefing_context()
                          - DayType 정보 추가
                          - 비교 컨텍스트 (같은 DayType 평균)
                          - 연속 추세 감지
                          - 주목할 프로그램 선정
                          - 게스트 효과 분석
                                            ↓
                          [신규] 풍부한 컨텍스트 = claude_context + ontology_context
                                            ↓
                          call_claude(NEW_BRIEFING_SYSTEM_PROMPT, enriched_context)
                                            ↓
                                       brief_text
```

핵심: **briefing_engine은 거의 안 건드리고**, raas_server.py에서 어댑터 호출을 추가.

---

# 챕터 4: 새 브리핑 형식 명세

## 4.1 5섹션 구조

```
01 / 핵심지표 [3줄, 각 50자 내외]
02 / 주목할 프로그램 [1~2개, 각 2~3줄]
03 / 추세 시그널 [2~3줄]
04 / 비교 컨텍스트 [1~2줄]
05 / 액션 추천 [1줄]
```

총 분량: 11~15줄. 기존 8줄 대비 약 50% 증가하지만 정보 밀도 훨씬 높음.

## 4.2 섹션별 상세 명세

### 01 / 핵심지표

```
DAU XX,XXX명 (전주 [요일] 대비 ▼Y.Y%, 같은 [평일/주말/공휴일] 평균 대비 ±Z.Z%)
깊은청취율 XX.X% (N일 연속 [상승/하락])
파워FM XX,XXX명(점유 XX.X%) 강세, 러브FM XX,XXX명 보합
```

**필수 요소**:
- 같은 요일 비교 명시 (전주 일요일 대비)
- 같은 DayType 평균 비교 (평일·주말·공휴일 별도)
- 연속 추세 표시 (3일 이상만)
- 채널 강세/보합 분류

### 02 / 주목할 프로그램

```
⭐ [프로그램명] (코드): DAU XX,XXX명 (▲X.X% WoW)
  → 변화 원인 (게스트, 추세, DayType 등)
⚠️ [프로그램명] (코드): N일 연속 하락 ▼X.X%
  → 게스트/편성 변화 또는 우려사항
```

**선정 기준** (priority 순):
1. WoW ±10% 이상 (큰 변화)
2. 3일 이상 연속 추세 (지속적 변화)
3. WoW ±5~10% (중간 변화)

DAU 1,000 미만 프로그램은 제외.

### 03 / 추세 시그널

```
신규 사용자: 4일 연속 감소 (-X.X% 누적)
이탈률: 3일 연속 상승 (XX.X% → XX.X%)
시사·정치 시간대(L05, L06, L13): 평일 평균 대비 +XX% 견조
```

**감지 대상 지표**:
- DAU (T00, 채널별)
- 신규 사용자
- 이탈률
- 깊은청취율
- 습관형성률

3일 미만 변화는 추세로 표시 안 함.

### 04 / 비교 컨텍스트

```
오늘은 일요일(주말). 같은 주말 평균과 비교 시 정상 범위.
```

또는 공휴일:
```
오늘은 5/1 근로자의 날(공휴일). 출근 시간대 청취 -50% 영향이 큼.
```

또는 평일:
```
오늘은 화요일(평일). 같은 화요일 평균 대비 ±X% 변동.
```

**필수**: 평일·주말·공휴일 명시 + 같은 DayType 비교 명시.

### 05 / 액션 추천

```
김영철의 파워FM 5일 연속 하락 → 콘텐츠 점검 또는 고정 게스트 출연 일정 확인
```

**원칙**:
- 1가지만 (가장 시급한 것)
- 근거 부족 시 "추세 모니터링" 보수적 답변
- 추측성 권고 금지 ("외부이슈 점검" 같은 막연한 표현)

---

# 챕터 5: 새 시스템 프롬프트 전문

## 5.1 브리핑용 시스템 프롬프트

```python
BRIEFING_SYSTEM_PROMPT = """당신은 SBS 고릴라 라디오 앱의 매일 운영 브리핑을 작성하는 데이터 분석 어시스턴트입니다.
방송 관계자(편성·콘텐츠·마케팅 담당자)가 아침에 5분 안에 읽고 핵심을 파악할 수 있어야 합니다.

[지표 정의]
- DAU: 청취시간 > 0인 고유 사용자 수 (일간)
- WAU: 7일 롤링 활성 사용자
- MAU: 30일 롤링 활성 사용자
- 깊은청취율: 10분 이상 청취 / 1분 이상 청취 비율
- WoW: 전주 동일 요일 대비 변화율
- 이탈률: 7일 무청취 사용자 비율
- 습관형성률: 신규 사용자 첫 7일 내 3일 이상 청취 비율

[필수 답변 형식 — 정확히 5개 섹션, 라벨 그대로 사용]

01 / 핵심지표 [3줄, 각 줄 50자 내외]
- DAU와 WoW. 같은 요일 비교 명시 (예: "전주 일요일 대비")
- 깊은청취율과 N일 연속 추세 (3일 이상만 추세로 표시)
- 채널 분포 또는 강세/보합 현황

02 / 주목할 프로그램 [1~2개, 각 2~3줄]
- 변화가 가장 큰 프로그램 1~2개 선정
- 코드, 이름, DAU, 변화율 명시
- 변화 원인 추정 가능하면 명시 (게스트 효과, 연속 추세, DayType 영향)
- 단순 1위가 아닌 "오늘 봐야 하는" 프로그램

03 / 추세 시그널 [2~3줄]
- 3일 이상 연속 변화한 지표만 (DAU, 이탈률, 신규, 깊은청취율)
- 단발 변화는 추세 아님

04 / 비교 컨텍스트 [1~2줄]
- 오늘이 평일/주말/공휴일 중 무엇인지 명시
- 같은 DayType 평균과 비교 (평일·주말 섞어 비교 절대 금지)
- 공휴일이면 공휴일명 명시

05 / 액션 추천 [1줄, 1가지만]
- 가장 시급한 1가지 조치
- 근거 부족 시 "추세 모니터링" 보수적 답변

[환각 방지 — 절대 규칙]
1. 제공된 데이터와 온톨로지 컨텍스트에 있는 정보만 인용
2. 평균값을 임의로 계산하지 말 것 — 컨텍스트 명시 평균만 사용
3. 정보 없으면 "해당 정보 없음" 명시 — 추측 금지
4. 진행자 이름은 온톨로지 컨텍스트에 명시된 경우만 언급
5. "외부 이슈" 같은 추측성 원인은 데이터 근거 있을 때만

[수치 표기]
- 천단위 쉼표
- 소수점 1자리 (XX.X%)
- 변화율은 부호 명시 (+5.0%, -3.2%)
- 양의 변화는 ▲, 음은 ▼"""
```

## 5.2 질의용 시스템 프롬프트

```python
QUERY_SYSTEM_PROMPT = """당신은 SBS 고릴라 라디오 앱의 데이터 분석 어시스턴트입니다.
사용자 질문에 정확하고 간결하게 답합니다.

[답변 원칙]
- 단답형 질문(누구, 언제, 무엇)은 1~2문장
- 분석형 질문(왜, 어떻게)은 4~6문장
- 비교 질문은 표나 구조화된 형태
- 항상 데이터 출처 또는 근거 명시

[지표 정의 — 위 브리핑 프롬프트와 동일]

[온톨로지 컨텍스트 활용]
- 진행자 정보가 컨텍스트에 있으면 답변에 포함
- ProgramType (HostedProgram/AutomatedProgram) 명시
  · 자동 음악 프로그램은 "DJ 없는 자동 편성 채널"로 답변
- DayType 정보가 있으면 비교 컨텍스트로 활용
- RegularGuest와 일회성 게스트 구분
  · 정상근, 이재익 등은 RegularGuest

[환각 방지 — 절대 규칙]
1. 제공된 데이터/온톨로지에 없는 정보는 "정보 없음" 명시
2. 비율 계산은 직접 하지 말 것 — 컨텍스트 값만 사용
3. 진행자 이름은 온톨로지에 명시된 경우만 언급
4. "추정컨대" 같은 추측 표현 사용 금지

[수치 표기]
- 천단위 쉼표
- 소수점 1자리
- 변화율은 부호 명시"""
```

---

# 챕터 6: 어댑터 메서드 활용 가이드

## 6.1 새로 추가된 어댑터 메서드 (Phase 6-A에서 추가됨)

| 메서드 | 용도 | 호출 시점 |
|---|---|---|
| `find_top_guests(code, days)` | 비고정 게스트 효과 분석 | 주목할 프로그램의 변화 원인 분석 |
| `detect_consecutive_trend(values)` | 연속 추세 감지 | 03 추세 시그널 섹션 |
| `select_notable_programs(all_kpi)` | 주목할 프로그램 선정 | 02 주목할 프로그램 섹션 |
| `get_daytype_comparison(date)` | DayType 비교 텍스트 | 04 비교 컨텍스트 섹션 |
| `get_program_meta(code)` | 프로그램 메타정보 (Host 포함) | 진행자 인용 시 |
| `_get_regular_guest_names()` | RegularGuest 분석 제외 | find_top_guests 내부 |

## 6.2 브리핑 컨텍스트 빌드 패턴

raas_server.py의 브리핑 호출부에서 사용할 패턴:

```python
def build_briefing_context(claude_context: str, kpi_data: dict, target_date: str) -> str:
    """기존 claude_context에 온톨로지 정보 추가."""
    from raas_ontology_adapter import get_adapter
    adapter = get_adapter()
    
    # 1. DayType 정보
    daytype_info = adapter.get_daytype_comparison(target_date)
    daytype_section = f"\n[DayType 정보]\n{daytype_info.get('comparison_text', '')}\n"
    
    # 2. 주목할 프로그램 선정
    all_kpi = kpi_data.get("s5_rankings", {}).get("all_program_kpi", {})
    notable = adapter.select_notable_programs(all_kpi, max_count=2)
    
    notable_section = "\n[주목할 프로그램 후보]\n"
    for prog in notable:
        notable_section += f"- {prog['code']} {prog['label']}: DAU {prog['dau']:,}명, WoW {prog['wow_pct']:+.1f}%\n"
        if prog.get("host"):
            notable_section += f"  진행자: {prog['host']}\n"
        
        # 게스트 효과 분석 (변화 큰 프로그램만)
        if abs(prog["wow_pct"]) >= 10:
            top_guests = adapter.find_top_guests(prog["code"], days=30, top_n=3)
            if top_guests:
                notable_section += f"  최근 게스트 효과:\n"
                for g in top_guests[:3]:
                    notable_section += f"    {g['name']} ({g['appearances']}회): {g['effect_pct']:+.1f}%\n"
    
    # 3. 연속 추세 감지
    timeline = kpi_data.get("_timeline", {})
    trend_section = "\n[연속 추세 감지]\n"
    
    # T00 DAU 추세
    t00_dates = sorted(timeline.get("T00", {}).keys())[-7:]  # 최근 7일
    if len(t00_dates) >= 3:
        dau_values = [timeline["T00"][d].get("dau_today") for d in t00_dates]
        trend = adapter.detect_consecutive_trend(dau_values)
        if trend["consecutive_days"] >= 3:
            arrow = "▲" if trend["direction"] == "up" else "▼"
            trend_section += f"- 전체 DAU: {trend['consecutive_days']}일 연속 {arrow} ({trend['total_change_pct']:+.1f}%)\n"
    
    # 통합
    enriched = claude_context + daytype_section + notable_section + trend_section
    return enriched
```

## 6.3 질의 컨텍스트 빌드 패턴

```python
def build_query_context(question: str, claude_context: str) -> str:
    """질의용 컨텍스트 빌드 — 질문에 맞춰 동적 추출."""
    from raas_ontology_adapter import get_adapter
    adapter = get_adapter()
    
    # 1. 질문에서 프로그램 키워드 추출
    matched_programs = []
    for prog in adapter.get_all_programs():
        for label in [prog["label"]] + prog.get("alt_labels", []):
            if label and len(label) >= 2 and label in question:
                matched_programs.append(prog)
                break
    
    # 2. 질문에서 사람 이름 추출
    matched_persons = []
    for prog in matched_programs:
        if prog.get("main_host"):
            matched_persons.append(prog["main_host"])
    
    # 별도로 사람 이름 검색 (정상근 같은 RegularGuest)
    for person_kw in ["정상근", "이재익", "김도형"]:  # 향후 확장
        if person_kw in question:
            matches = adapter.find_person_by_name(person_kw)
            for m in matches:
                matched_persons.append(m["person"])
    
    # 3. 컨텍스트 조립
    onto_section = "\n[온톨로지 컨텍스트]\n"
    
    if matched_programs:
        onto_section += "관련 프로그램:\n"
        for prog in matched_programs:
            onto_section += f"- {prog['code']} {prog['label']}\n"
            if prog.get("main_host"):
                onto_section += f"  진행자: {prog['main_host']['label']}\n"
            elif prog.get("program_type", {}).get("id") == "raas:AutomatedProgram":
                onto_section += f"  유형: {prog['program_type']['label']} (DJ 없음)\n"
            if prog.get("regular_guests"):
                names = [g["label"] for g in prog["regular_guests"]]
                onto_section += f"  고정 게스트: {', '.join(names)}\n"
    
    if matched_persons:
        onto_section += "\n관련 인물:\n"
        seen = set()
        for p in matched_persons:
            if p["id"] in seen:
                continue
            seen.add(p["id"])
            onto_section += f"- {p['label']}"
            if p.get("occupation"):
                onto_section += f" ({p['occupation']})"
            onto_section += "\n"
    
    return claude_context + onto_section
```

---

# 챕터 7: raas_server.py 변경 매트릭스

## 7.1 변경할 위치 3곳

### 위치 1 — 시스템 프롬프트 모듈 import (파일 상단)

**추가 위치**: import 섹션 끝

```python
from raas_prompts import BRIEFING_SYSTEM_PROMPT, QUERY_SYSTEM_PROMPT
from raas_ontology_adapter import get_adapter
from raas_briefing_context import build_briefing_context, build_query_context
```

새 모듈 3개 만들거나, 기존 위치에 직접 정의도 가능. 모듈 분리가 유지보수 유리.

### 위치 2 — 브리핑 LLM 호출 (라인 198~206)

**변경 전**:
```python
brief_text = call_claude(
    "SBS 고릴라 라디오 앱 데이터 분석 어시스턴트. 한국어로 간결하게. 수치는 천단위 쉼표.",
    claude_prompt
)
```

**변경 후**:
```python
# 어댑터로 컨텍스트 보강
target_date = data.get("date") or datetime.now().strftime("%Y-%m-%d")
enriched_context = build_briefing_context(
    claude_prompt,  # 기존 prompt
    data,           # collect_all 결과
    target_date
)

brief_text = call_claude(
    BRIEFING_SYSTEM_PROMPT,
    enriched_context
)
```

### 위치 3 — 질의 LLM 호출 (라인 425~432)

**변경 전**:
```python
try:
    bd = BE.collect_all(splunk_search)
    context = bd.get("claude_context", " ")
except:
    pass
answer = call_claude(
    "SBS 고릴라 라디오 앱 데이터 분석 어시스턴트. 한국어로 간결하게 답하세요. 수치는 천단위 쉼표.",
    f"데이터:\n{context}\n\n질문: {question}"
)
```

**변경 후**:
```python
try:
    bd = BE.collect_all(splunk_search)
    context = bd.get("claude_context", " ")
except Exception as e:
    logger.warning(f"collect_all 실패: {e}")
    context = ""

# 어댑터로 컨텍스트 보강
enriched_context = build_query_context(question, context)

answer = call_claude(
    QUERY_SYSTEM_PROMPT,
    f"데이터:\n{enriched_context}\n\n질문: {question}"
)
```

## 7.2 신규 파일 (선택)

깔끔한 분리를 위해 다음 2개 파일 신규 작성 권장:

### `raas_prompts.py` — 시스템 프롬프트 모음

챕터 5의 `BRIEFING_SYSTEM_PROMPT`, `QUERY_SYSTEM_PROMPT` 내용 그대로.

### `raas_briefing_context.py` — 컨텍스트 빌더

챕터 6.2, 6.3의 `build_briefing_context()`, `build_query_context()` 내용.

이렇게 분리하면 raas_server.py는 호출만 담당하고, 프롬프트와 컨텍스트 로직이 명확히 나눠집니다.

## 7.3 변경 라인 요약

| 파일 | 변경 유형 | 라인 |
|---|---|---|
| raas_server.py | 수정 | 라인 198~206 (브리핑) |
| raas_server.py | 수정 | 라인 425~432 (질의) |
| raas_server.py | 추가 | import 섹션 (3줄) |
| raas_prompts.py | **신규** | 약 80줄 |
| raas_briefing_context.py | **신규** | 약 100줄 |
| raas_ontology_adapter.py | 이미 갱신됨 | 메서드 6개 추가 (Phase 6-A에서) |

---

# 챕터 8: 검증 시나리오

## 8.1 회귀 시나리오 — 기존 동작 보존

**검증 1**: 기존 회귀 테스트 57개 통과
```
python phase5_regression_test.py
→ 57 PASS / 0 FAIL
```

**검증 2**: 브리핑 응답 형식 5섹션 유지
```
응답에 "01 /", "02 /", "03 /", "04 /", "05 /" 라벨 모두 포함
```

## 8.2 신규 능력 시나리오

### 시나리오 A — DayType 인식 (5/3 일요일)

**Before** (현재):
```
"DAU 91,235명 (WoW -7.6%, 6일 평균 대비 -45.3% 급락)"
"DAU 급락 원인 긴급 점검"
```

**After** (목표):
```
"DAU 91,235명 (전주 일요일 대비 -7.6%, 같은 주말 평균 대비 +1.2%)"
"오늘은 일요일. 정상 주말 패턴."
```

### 시나리오 B — 게스트 효과 인식

**질문**: "L07 이숙영의 러브FM 어제 청취자 어땠어?"

**Before**: 단순 DAU 답변
**After**: 
```
"L07 이숙영의 러브FM 5/2 DAU XX,XXX명 (+33% WoW).
4월 마지막 주 비고정 게스트 효과 평균 +20% — 4/29~5/1 기간 게스트 출연일이 많았던 영향."
```

### 시나리오 C — 연속 추세 감지

**Before**: 단발 변화만 표시
**After**: 
```
"03 / 추세 시그널
- 신규 사용자: 4일 연속 감소 (-8.2% 누적)
- 이탈률: 3일 연속 상승"
```

### 시나리오 D — 환각 제거

**Before**: 
```
"6일 평균 대비 -45.3%"  ← 출처 불명
"외부이슈/지표오류 긴급 점검"  ← 추측
```

**After**: 
```
"같은 주말 평균(컨텍스트 명시) 대비 +1.2%"
"추세 모니터링 권장"
```

## 8.3 A/B 비교 검증

**방법**: 변경 적용 후 같은 날짜 브리핑을 변경 전·후 비교.

```
[변경 전 브리핑]
... (이전 결과 보관)

[변경 후 브리핑]
... (신규 결과)
```

**평가 지표**:
- DayType 명시 여부 (Y/N)
- 같은 DayType 비교 사용 여부 (Y/N)
- 게스트 효과 언급 (Y/N, 변화 큰 경우)
- 연속 추세 감지 (Y/N, 3일 이상 변화 있는 경우)
- 추측성 표현 빈도 (감소해야 함)

---

# 챕터 9: Phase 6-B 체크리스트

Claude Code 세션에서 단계별로 적용:

## Step 1: 신규 파일 작성 (위험 없음)

- [ ] `raas_prompts.py` 작성 (챕터 5)
- [ ] `raas_briefing_context.py` 작성 (챕터 6)
- [ ] `python -c "from raas_prompts import *"` 임포트 확인
- [ ] Git 커밋: `feat: add briefing prompts and context builders`

## Step 2: 어댑터 갱신 (이미 완료)

- [ ] 새 ZIP의 `raas_ontology_adapter.py`로 교체
- [ ] `python phase5_regression_test.py` → 57 PASS 확인
- [ ] Git 커밋: `feat(adapter): add Phase 6-A briefing methods`

## Step 3: 브리핑 호출부 변경 (낮은 위험)

- [ ] `raas_server.py` 라인 198~206 변경 (챕터 7.2 위치 2)
- [ ] import 섹션에 신규 모듈 추가
- [ ] 로컬 테스트: `python raas_server.py` → 서버 정상 기동
- [ ] 웹앱에서 브리핑 1회 실행 → 5섹션 형식 확인
- [ ] Git 커밋: `feat: integrate ontology adapter to AI briefing`

## Step 4: 질의 호출부 변경 (중간 위험)

- [ ] `raas_server.py` 라인 425~432 변경 (챕터 7.2 위치 3)
- [ ] 로컬 테스트: 질의 5개 실행
- [ ] 검증 시나리오 A~D 수행
- [ ] Git 커밋: `feat: integrate ontology adapter to AI query`

## Step 5: A/B 비교 검증 (사용자)

- [ ] 변경 전 브리핑 결과 (이미 보관)
- [ ] 변경 후 브리핑 결과 캡처
- [ ] 차이점 정리
- [ ] 만족스러우면 운영 배포

---

# 부록 A: 향후 확장 가능성

## A.1 추가 RegularGuest 후보

분석 결과 다음이 RegularGuest 후보로 추정됨. 도메인 검증 필요:

| 프로그램 | 후보 | 출연 횟수 (5/2까지) |
|---|---|---|
| L06 김태현의 정치쇼 | **김도형** | 4/29, 4/30, 5/01 (3일 연속) |
| L07 이숙영의 러브FM | 이재익 | 확정됨 ✓ |
| F05 김영철의 파워FM | 정상근 | 확정됨 ✓ |

## A.2 향후 Phase 6-B 이후

- **Phase 6-C**: AI 질의 답변 품질 측정 자동화 (정량 지표)
- **Phase 7**: Episode 인스턴스화 (1,443개 회차) — Phase 2-C-4
- **Phase 8**: 광고 시장 인사이트 (Daypart × DayType 매트릭스)

## A.3 박사과정 활용

이 작업의 학술적 가치:

- **온톨로지 기반 LLM 컨텍스트 주입 설계 패턴**
- **환각 방지의 구조적 접근 (프롬프트 + 온톨로지 + 데이터 가용성 명시)**
- **DayType 같은 시간 컨텍스트의 자동 인식과 분석 보정**
- **게스트 효과 분석에서 RegularGuest 자동 제외 방법론**
