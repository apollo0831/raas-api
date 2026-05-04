# RAAS 온톨로지 PoC v0.8 (Phase 6-A: AI 브리핑 품질 향상 설계)

> SBS 고릴라 라디오 앱 분석 시스템(RAAS)의 온톨로지 + AI 브리핑 강화 설계

## 진행 단계 (전체)

| Phase | 내용 | 상태 |
|---|---|---|
| 2-A~B | 스키마 + KPI Metric (51) | ✅ |
| 2-C-1 | Platform/Channel/Program (36) | ✅ |
| 2-C-2 | TimeSlot/Daypart/ScheduleType (42) | ✅ |
| 4 | 비즈니스 룰 (15) | ✅ |
| 4.5 | 캘린더/공휴일 (34) | ✅ |
| 5-A | 운영 통합 설계 + 어댑터 + 회귀 테스트 | ✅ |
| 5-B | RAAS 시스템 통합 (어댑터 ↔ 코드 연결) | ✅ 완료 |
| 2-C-3 | Person/Host (Person 27 + Role 7 + Policy 5 + ProgramType 2) | ✅ |
| **6-A** | **AI 브리핑 품질 향상 설계 (이번 단계)** | **✅** |
| 6-B | 시스템 프롬프트 + 컨텍스트 빌더 통합 (Claude Code) | ⏳ 진행 예정 |
| 2-C-4 | Episode 인스턴스화 | ⏳ |

## 검증 결과 (Phase 6-A까지)

- **트리플 1,885개**
- **24/24 클래스 카운트 PASS**
- **57/57 회귀 테스트 PASS**
- 콜드 스타트 18.9ms

## Phase 6-A의 핵심 — AI 브리핑 품질 향상

### 진단된 문제 (실제 5/3 일요일 브리핑 분석)

1. **DayType 인식 실패** — 일요일을 평일과 비교, "급락" 잘못 표현
2. **게스트 효과 정보 전무** — 변동 원인 분석 없음
3. **연속 추세 부분 활용** — 깊은청취율만 잡고 다른 지표는 누락
4. **주목할 프로그램이 매일 비슷** — 1위 + 위험 패턴 고정
5. **환각 위험** — 평균값 출처 불명, 추측성 권고

### 근본 원인

**어댑터가 LLM 호출 경로에 미연결**. Phase 5-B에서 어댑터를 만들고 회귀 테스트는 통과시켰지만, 실제 `call_claude()` 호출에는 어댑터의 풍부한 정보가 들어가지 않음.

### Phase 6-A 산출물

#### 신규 파일 3개

| 파일 | 역할 |
|---|---|
| **`BRIEFING_DESIGN.md`** | **9챕터 통합 설계 문서 — Claude Code 입력용** |
| **`raas_prompts.py`** | **새 시스템 프롬프트 (브리핑용 + 질의용)** |
| **`raas_briefing_context.py`** | **컨텍스트 빌더 (build_briefing_context, build_query_context)** |

#### 어댑터 신규 메서드 7개

| 메서드 | 기능 |
|---|---|
| `find_top_guests(code, days)` | 비고정 게스트 효과 분석 (RegularGuest 자동 제외) |
| `detect_consecutive_trend(values)` | 3일 이상 연속 추세 감지 |
| `select_notable_programs(all_kpi)` | 주목할 프로그램 자동 선정 |
| `get_daytype_comparison(date)` | DayType 비교 텍스트 생성 |
| `_get_regular_guest_names()` | RegularGuest 이름 추출 (분석 제외용) |
| `_parse_guestname_for_analysis()` | guestname 파싱 + 직업 접미어 제거 |
| `_build_guest_cache_from_csv()` | CSV에서 게스트 효과 캐시 (옵션 B) |
| `reset_guest_cache()` | 캐시 무효화 |

### 새 브리핑 형식 — 5섹션 구조

```
01 / 핵심지표 (3줄)
- DAU + WoW (전주 동일 요일 대비)
- 깊은청취율 + N일 연속 추세
- 채널 강세/보합

02 / 주목할 프로그램 (1~2개)
- 변화 큰 프로그램 자동 선정
- 게스트 효과, 연속 추세, DayType 영향 분석

03 / 추세 시그널 (2~3줄)
- 3일 이상 연속 변화 (DAU, 이탈률, 신규 등)

04 / 비교 컨텍스트 (1~2줄)
- 평일/주말/공휴일 명시
- 같은 DayType 평균과 비교

05 / 액션 추천 (1줄)
- 가장 시급한 1가지
```

## 검증 결과 — 컨텍스트 빌더 동작 확인

`raas_briefing_context.py` 자체 테스트로 검증:

### 시나리오 1: F03 파워 스테이션 인식
```
질문: F03 파워 스테이션 진행자 누구야?
추가된 컨텍스트:
[관련 프로그램]
- F03 파워 스테이션
  유형: 자동 음악 프로그램 (DJ 없음)
  방송 시간: 03:00-05:00
```
→ LLM이 "DJ 없는 자동 음악 채널" 답변 가능 ✓

### 시나리오 2: 정상근 매칭
```
질문: 정상근이 출연하는 프로그램?
추가된 컨텍스트:
[관련 인물]
- 정상근
  고정 출연: F05
```
→ "F05 김영철의 파워FM 고정 게스트" 답변 가능 ✓

### 시나리오 3: 5/3 일요일 브리핑
```
[DayType 정보]
오늘은 일(주말)입니다. 평일 대비 DAU 감소는 정상 범위입니다.

[주목할 프로그램 후보]
- F05 김영철의 파워FM: DAU 14,765명, WoW -16.2% ▼
  진행자: 김영철
  최근 비고정 게스트 효과 (RegularGuest 제외):
    이석무 (1회 출연): +7.8% [weekend]
```
→ DayType 인식 ✓ 주목 프로그램 자동 선정 ✓ 게스트 효과 분석 ✓

## Person 인스턴스 27명 (Phase 2-C-3 + 보강)

**MainHost 25명** (정규 주 진행자):
- 파워FM: 딘딘, 이인권, 김영철, 봉태규, 박하선, 주현영, 김태균, 황제성, 박소현, 웬디, 배성재
- 러브FM 평일: 고현준, 김태현, 이숙영, 박연미, 유민상, 정엽, 이상미, 김창완, 편상욱, 김윤상, 박은경
- 러브FM 주말: 김선재, 최영주, DJ래피 (M10/M11 공유)

**RegularGuest 2명**:
- **정상근** (F05 김영철의 파워FM)
- **이재익** (L07 이숙영의 러브FM, 사용자 도메인 정보로 추가)

## 파일 구성 (26개)

### 온톨로지 (7개 .ttl)
- raas_kpi_ontology.ttl
- raas_domain_entities.ttl
- raas_time_schema.ttl
- raas_business_rules.ttl
- raas_calendar.ttl
- raas_person_schema.ttl (Person 클래스, Role 7, Policy 5, ProgramType 2)
- raas_person_instances.ttl (Person 27명)

### Phase 5-A 산출물 (3개)
- PHASE5_INTEGRATION_PLAN.md
- raas_ontology_adapter.py (Phase 6-A 메서드 7개 추가)
- phase5_regression_test.py (57개 항목)

### Phase 6-A 산출물 (3개) ⭐ 신규
- **BRIEFING_DESIGN.md** (9챕터 통합 설계 문서)
- **raas_prompts.py** (시스템 프롬프트)
- **raas_briefing_context.py** (컨텍스트 빌더)

### 검증/유틸 (4개)
- validate.py / validate_full.py / extract_timeslots.py / raas_paths.py

### 데모 (6개)
- example_usage.py ~ v6.py

### 문서 (2개)
- README.md / LOCAL_GUIDE.md

## Phase 6-B 진행 가이드 (Claude Code)

새 Claude Code 세션을 열고 다음 프롬프트로 시작:

```
/Users/apollo/projects/raas 디렉토리에서 작업합니다.

raas_onto/BRIEFING_DESIGN.md 의 설계에 따라 raas_server.py에
온톨로지 컨텍스트 주입을 통합해주세요.

진행 방식:
- BRIEFING_DESIGN.md 챕터 9의 5단계 체크리스트 순서대로
- 각 Step 완료 후 회귀 테스트 (raas_onto/phase5_regression_test.py)
- Step 3, 4 완료 후에는 webapp에서 실제 응답 확인
- Git 커밋은 Step별로 분리

먼저 BRIEFING_DESIGN.md를 읽고 작업 계획을 정리해주세요.
```

Claude Code가 자동으로:
1. 신규 파일 2개 (raas_prompts.py, raas_briefing_context.py)를 RAAS 저장소에 복사
2. raas_ontology_adapter.py 갱신 (Phase 6-A 메서드 추가된 버전)
3. raas_server.py 라인 198~206, 425~432 수정
4. 회귀 테스트 + 실제 동작 확인

## 박사과정 + 광고 시장 연구 가치

**박사과정**:
- LLM 환각 방지의 구조적 접근 (프롬프트 + 온톨로지 + 컨텍스트 가용성 명시)
- DayType 같은 시간 컨텍스트의 자동 보정 (평일/주말 섞어 비교 방지)
- 게스트 효과 분석에서 RegularGuest 자동 제외 방법론
- "메모리 캐시 옵션 B" — 온톨로지를 비대화하지 않으면서 효과 분석

**광고 시장**:
- 게스트 효과 정량화 → 광고 단가 차등화 근거
- AutomatedProgram 분류 → 광고 형식 차별화 (라이브 광고 불가)
- DayType × Daypart 매트릭스 → 광고 인벤토리 정밀 산정

## 실행 방법

```bash
cd raas_onto/

# 통합 검증 (24/24 PASS)
python3 validate_full.py

# 회귀 테스트 (57/57 PASS)
python3 phase5_regression_test.py

# 컨텍스트 빌더 자체 테스트
python3 raas_briefing_context.py
```

## 다음 단계

**Phase 6-B (Claude Code)**: BRIEFING_DESIGN.md 따라 raas_server.py 통합

**Phase 2-C-4**: Episode 인스턴스화 (1,443개 회차)

## 라이선스 / 출처

내부 PoC. 외부 배포 전 보안 검토 필요.
