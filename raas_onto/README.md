# RAAS 온톨로지 PoC v0.6 (Phase 5-A 완료)

> SBS 고릴라 라디오 앱 분석 시스템(RAAS)의 KPI + 도메인 + 시간 + 비즈니스 룰 + 캘린더 온톨로지 + **운영 통합 설계**

## 진행 단계

| Phase | 내용 | 상태 |
|---|---|---|
| 2-A | 스키마 설계 | ✅ |
| 2-B | KPI Metric (11 + 변형 40) | ✅ |
| 2-C-1 | Platform/Channel/Program (36) | ✅ |
| 2-C-2 | TimeSlot/Daypart/ScheduleType (42) | ✅ |
| 4 | 비즈니스 룰 (PlatformAlert 9 + Concept 4) | ✅ |
| 4.5 | 캘린더/일자유형/공휴일 (34) | ✅ |
| **5-A** | **운영 통합 설계 문서 (어댑터 + 회귀 테스트)** | **✅** |
| 5-B | RAAS 시스템 실제 통합 | ⏳ Claude Code |
| 2-C-3 | Person 파싱 | ⏳ |

## 검증 결과 (Phase 5-A까지)

- **트리플 1,522개**
- **20/20 클래스 카운트 PASS**
- **회귀 테스트 45/45 PASS**
- **성능**: 콜드 스타트 10.7ms, 캐시 히트 0.017ms

## 파일 구성 (18개)

### 온톨로지 (5개 .ttl)
| 파일 | 역할 |
|---|---|
| `raas_kpi_ontology.ttl` | KPI 온톨로지 (11 metric + 40 variant) |
| `raas_domain_entities.ttl` | Platform/Channel/Program (36) |
| `raas_time_schema.ttl` | TimeSlot/Daypart/ScheduleType (42) |
| `raas_business_rules.ttl` | 비즈니스 룰 (15) |
| `raas_calendar.ttl` | 캘린더/공휴일 (34) |

### Phase 5-A 산출물 (3개) ⭐ 신규
| 파일 | 역할 |
|---|---|
| **`PHASE5_INTEGRATION_PLAN.md`** | **통합 설계 문서 — Claude Code 입력용** |
| **`raas_ontology_adapter.py`** | **어댑터 참조 구현 (운영 통합용)** |
| **`phase5_regression_test.py`** | **회귀 테스트 45개** |

### 검증/유틸리티 (4개)
| 파일 | 역할 |
|---|---|
| `validate.py` | Turtle 파서 + KPI 검증 |
| `validate_full.py` | 5개 .ttl 통합 검증 |
| `extract_timeslots.py` | CSV → TimeSlot 자동 추출 |
| `raas_paths.py` | 경로 자동 탐색 헬퍼 |

### 데모 (5개)
| 파일 | 역할 |
|---|---|
| `example_usage.py` | KPI 활용 (5가지) |
| `example_usage_v2.py` | 도메인 엔티티 (4가지) |
| `example_usage_v3.py` | 시간 정보 (5가지) |
| `example_usage_v4.py` | 비즈니스 룰 (5가지) |
| `example_usage_v5.py` | 캘린더/DayType (5가지) |

### 문서 (2개)
| 파일 | 역할 |
|---|---|
| `README.md` | 이 파일 |
| `LOCAL_GUIDE.md` | 로컬 실행 가이드 |

## Phase 5-A의 핵심 산출물

### 1. PHASE5_INTEGRATION_PLAN.md (메인)

Claude Code(Phase 5-B)에서 RAAS 시스템에 온톨로지를 통합할 때 따라야 할 **완전한 작업 지시서**.

7개 챕터 + 2개 부록:
- 챕터 1: 목표/비목표 (성공 기준 명시)
- 챕터 2: 아키텍처 (현재 vs 목표 구조도)
- 챕터 3: 모듈 명세 (14개 메서드)
- 챕터 4: API 명세 (`/api/schema`, `/api/concept`)
- 챕터 5: 코드 변경 매트릭스 (파일별 수정 라인 명시)
- 챕터 6: 마이그레이션 (5단계 점진적 교체)
- 챕터 7: 검증 계획 (12+6+4+3 = 25개 테스트)
- 부록 A: 어댑터 참조 구현
- 부록 B: 회귀 테스트
- **체크리스트**: Phase 5-B 진행 시 단계별 체크

### 2. raas_ontology_adapter.py (참조 구현)

운영 환경에 그대로 도입 가능한 600줄 Python 모듈.

| 메서드 | 대체 대상 (RAAS 코드) |
|---|---|
| `get_field_info()` | FIELD_ALIASES (87줄), field_map (8줄), getMetricField (24분기) |
| `find_program_by_keyword()` | KEYWORD_TO_CODE (15줄) |
| `get_program_meta()` | PGM_NAMES (33줄), PGM_MAP (33줄) |
| `evaluate_platform_alerts()` | build_s7() (25줄) |
| `find_at_risk_programs()` | risks 룰 (10줄) |
| `classify_program()` | (신규) 4개 ProgramConcept 동시 분류 |
| `get_concept_definition()` | (신규) LLM 컨텍스트 주입용 |
| `get_schema_dump()` | (신규) /api/schema 응답 본체 |
| `get_day_type()` | (신규) DayType 인식 |

### 3. phase5_regression_test.py (검증 자동화)

Phase 5-B 진행 시 매 Step마다 실행하는 회귀 테스트.

- RT-01~12: KPI/프로그램/룰 회귀 (12개)
- NT-01~06: 신규 능력 (6개)
- PT-01~04: 성능 (4개)

## 직접 실행

```bash
cd raas_onto/

# 통합 검증 (20/20 PASS)
python3 validate_full.py

# 어댑터 자체 테스트
python3 raas_ontology_adapter.py

# 회귀 테스트 (45/45 PASS)
python3 phase5_regression_test.py

# 데모 (5종)
python3 example_usage_v5.py  # 캘린더 — 가장 흥미로움
python3 example_usage_v4.py  # 비즈니스 룰
python3 example_usage_v3.py  # 시간 정보
python3 example_usage_v2.py  # 도메인 엔티티
python3 example_usage.py     # KPI
```

## 운영 통합 시 효과 요약

| 영역 | 현재 코드 | 줄 수 | Phase 5 후 |
|---|---|---|---|
| 메타지식 (briefing) | FIELD_ALIASES, PGM_NAMES | 120 | 어댑터 호출 |
| 비즈니스 룰 | build_s7() | 25 | adapter.evaluate_platform_alerts() |
| 자연어 매칭 | KEYWORD_TO_CODE | 15 | adapter.find_program_by_keyword() |
| KPI 매핑 | field_map, PGM_MAP | 41 | 어댑터 메서드 |
| 룰 중복 | risks (양쪽) | 20 | adapter.find_at_risk_programs() |
| 클라이언트 | getMetricField + 분기 | 50 | /api/schema |
| **합계** | | **~270줄 제거** | **+ 1곳에서 관리** |

## Phase 5-B 진행 가이드

Claude Code 세션을 새로 열고 다음을 입력으로 사용:

1. **이 ZIP 파일** 압축 해제 후 RAAS 저장소와 함께 작업 디렉토리에 둠
2. `PHASE5_INTEGRATION_PLAN.md`를 Claude Code에 첨부
3. "이 설계 문서에 따라 RAAS 시스템에 온톨로지를 통합해주세요. Step 1부터 시작."

Claude Code는 다음을 수행:
- `raas_ontology_adapter.py`를 RAAS 저장소에 배치
- 회귀 테스트 검증 (45 PASS 확인)
- Step 2~5를 차례로 적용 (각 Step마다 회귀 테스트)
- Git 커밋 5개 단계로 분리

## 박사과정 + 광고 시장 연구 종합 가치

이 PoC가 만들어낸 학술적/실무적 가치:

**박사과정**:
- 온톨로지 4단계(추론 규칙) 도달
- 산업 시스템에서 도메인 지식 단일 출처 확보 사례
- LLM 환각 방지를 구조적으로 접근 (프롬프트 → 온톨로지)
- 점진적 통합 방법론 (5단계 점진 교체)

**광고 시장**:
- Daypart × DayType 매트릭스 자동 생성 (광고 인벤토리 산정)
- 공휴일 효과 정량화 (출근 시간대 -52%)
- 채널/프로그램 자동 분류 (이탈위험/성장 등)
- /api/schema 활용한 광고 영업 자동화 가능성

## 라이선스 / 출처

내부 PoC. 외부 배포 전 보안 검토 필요.
