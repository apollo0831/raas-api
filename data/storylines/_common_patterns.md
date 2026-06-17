# 3개 스토리라인 공통 패턴 — Phase 2 백엔드 설계 입력

> 분석 대상: [cp.json](cp.json) · [pd_program.json](pd_program.json) · [pd_schedule.json](pd_schedule.json)
> 작성: 2026-06-17
> 목적: Phase 2 ③ 백엔드 엔드포인트 설계 + Phase 1 ① 데이터 적재 통합 우선순위

---

## 1. 공통 데이터 적재 우선순위 (Phase 1 ① 가이드)

3직무 스토리라인의 `ingest_blocking_high_priority`를 교차 분석한 결과 — **공유 적재 1건 = 3직무 동시 잠금해제**.

### 🔴 1순위 적재 (3직무 모두 또는 2직무 공유)

| 필드 | 차단 직무·슬롯 | 적재 위치 |
|---|---|---|
| **공감로그/문자 건수 (프로그램별 일간)** | 제작PD(`1_anchor`,`2_cause`) + CP(`2_cause`) | Splunk → RAAS |
| **특별 게스트/이벤트 캘린더** | 제작PD(`2_cause`) + CP(`2_cause`,`4_loop`) | 별도 메타데이터 |
| **시간대별 활성사용자 (채널×시간대)** | 편성PD(`1_anchor`,`2_cause`,`3_context`) | Splunk → RAAS |
| **인구통계 (성별·연령대, 시간대별)** | 편성PD(`2_cause`) + 마케팅(미작성) | Splunk → RAAS |

→ 이 4건만 적재하면 **3직무 모두의 1·2·3 슬롯이 본격 동작**.

### 🟡 2순위 적재 (전제·맥락·루프 잠금해제)

| 필드 | 차단 직무·슬롯 |
|---|---|
| **CP→담당 프로그램 매핑** | CP(`1_anchor`,`3_context`) — 진입 전제 |
| **프로그램→담당 PD 매핑** | CP(`3_context` PD 디스패치) |
| **프로그램→인접 시간대 매핑** | 제작PD(`3_context`) + 편성PD(`2_cause`) + CP(`2_cause`,`3_context`) |
| **프로그램→작가·진행자 매핑** | 제작PD(`5_closing` 카톡 공유) |
| **분기 단위 집계 뷰** | 편성PD(`1_anchor`,`3_context`) — RAAS 분기 뷰 부재 |
| **26년 4월 이전 과거 데이터** | 편성PD(`3_context` YoY) |
| **코너 단위 활성사용자** | 제작PD(`1_anchor`,`2_cause`) — RAAS·Splunk 모두 미보유 |
| **개편 이벤트 메타데이터** | 제작PD·CP·편성PD 모두 `4_loop` |

### 🟢 즉시 동작 가능한 경로 (PoC 가치 증명 최단)

3직무 모두 부분 적재 상태(KPI CSV + 매핑만)로 다음 흐름 가능:

| 직무 | 가능한 경로 | 소요 |
|---|---|---|
| CP | entry → 1_anchor → 2_cause(fallback) → 5_closing | 2분 |
| 제작PD | entry → 1_anchor → 3_context(WoW만) → 5_closing | 2분 |
| 편성PD | entry → 1_anchor(일/주/월만) → 3_context(WoW/MoM만) → 5_closing | 3분 |

→ **3직무 동시 시연이 매핑 메타데이터만으로 가능**.

---

## 2. 5슬롯 공통 구조 패턴

### 슬롯별 의도 일관성

| 슬롯 | 모든 직무 공통 의도 | 직무별 변주 |
|---|---|---|
| `1_anchor` | "오늘/이번 주 무슨 일이 있었나" (1순위 KPI 즉답) | CP=N개 변화, 제작PD=직전 회차, 편성PD=시간대 grid |
| `2_cause` | "왜 그랬나" (원인 후보 + 근거) | CP=게스트/이벤트, 제작PD=코너/선곡, 편성PD=인구통계 |
| `3_context` | "맥락은 어떤가" (비교축 적용) | CP=WoW/MoM, 제작PD=WoW+인접, 편성PD=QoQ/YoY |
| `4_loop` | "과거 결정은 어땠나" (폐쇄루프) | CP=개편 효과, 제작PD=코너 개편, 편성PD=과거 편성 결정 |
| `5_closing` | "오늘 핵심 3가지 + 어디로 보낼까" | 직무별 outputsTo 분기 |

### chip_intent 공통 패턴

다음 intent는 3직무 모두 공유 → 공통 처리 가능:

| intent | 사용 슬롯 | 의미 |
|---|---|---|
| `back_to_cause` | 4_loop → 2_cause | 폐쇄루프에서 원인으로 복귀 |
| `goto_closing` | 1·2·3·4 → 5 | 빠른 마무리 |
| `compare_adjacent` | 1·2 → 3 | 인접 비교 (Phase 2 인접 매핑 적재 후) |

### outputsTo 형식별 그룹화

총 13개 산출물 → 형식별 통합 가능:

| 형식 | 산출물 수 | 비고 |
|---|---|---|
| PPT (보고서) | 5 | CP 국장1장, CP 주간회의, CP MAU별도, 편성회의, 국장요약 |
| PPT/PDF (한 장) | 1 | 제작PD→CP 보고용 한 장 |
| 카톡/메일 텍스트 | 2 | CP→PD 디스패치, 제작PD→작가/진행자 |
| XLSX (엑셀) | 2 | 제작PD 그래프 엑셀, 편성PD 비교 그래프 |
| PNG | 1 | 제작PD 모바일 공유 |
| HTML (Web) | 1 | 편성PD 청취율조사 나란히 |
| Mixed | 1 | 편성PD 비교 그래프(PNG|XLSX) |

→ **Phase 3 ⑥의 PPT 생성기 = 5개 보고서 양식의 공통 백엔드**.

---

## 3. Phase 2 ③ 백엔드 엔드포인트 스펙

3개 JSON을 분석한 결과, 백엔드 엔드포인트는 다음 4종이 필요:

### 3.1 `GET /api/storyline/entry`

```
Query: ?role=CP|PD_PROGRAM|PD_SCHEDULE

Response:
{
  "ok": true,
  "role": "CP",
  "greeting": "...",          // greeting_template 변수 치환 후
  "first_question": "...",
  "chips": [
    { "label": "...", "intent": "...", "next_slot": "1_anchor" },
    ...
  ],
  "user_context": {
    "user_name": "...",
    "programs": [...]         // CP 담당 프로그램 (1_anchor용)
  }
}
```

### 3.2 `POST /api/storyline/advance`

```
Body:
{
  "role": "CP",
  "slot_from": "entry|1_anchor|...",
  "chip_intent": "explain_change",
  "context": {                // 직전 슬롯에서 결정된 데이터
    "top_change_program": "...",
    "change_pct": -7.3
  }
}

Response:
{
  "ok": true,
  "slot": "2_cause",
  "slot_name": "원인 설명 (CP의 가장 큰 갈증)",
  "answer": "...",            // answer_template 또는 fallback 변수 치환
  "fallback_used": false,
  "chips_next": [...]
}
```

### 3.3 `POST /api/storyline/export`

```
Body:
{
  "role": "CP",
  "output_format_id": "director_one_pager_ppt",
  "slots_visited": [
    { "slot": "1_anchor", "context": {...} },
    { "slot": "2_cause", "context": {...} }
  ]
}

Response:
{
  "ok": true,
  "format": "PPT",
  "download_url": "/api/storyline/exports/abc123.pptx",
  "filename": "CP_보고서_2026-06-17.pptx"
}
```

### 3.4 `GET /api/storyline/role-detect`

로그인 후 직무 매핑 (현재 RAAS_USER.role 활용 + 신규 매핑 테이블):

```
Response:
{
  "ok": true,
  "role": "CP|PD_PROGRAM|PD_SCHEDULE|...",
  "config_path": "data/storylines/cp.json",
  "available_now": true,      // 매핑·기본 데이터 충족 여부
  "missing_setup": [          // 부족하면 표시
    "담당 프로그램 매핑이 필요합니다"
  ]
}
```

---

## 4. 슬롯 답변 채우기 — 데이터 → 변수 매핑

`answer_template`의 변수를 채우려면 다음 모듈이 필요:

### `raas_storyline_engine.py` (신규)

```
class StorylineEngine:
    def __init__(self, role: str):
        self.config = load(f"data/storylines/{role}.json")

    def entry(self, user) -> dict
    def advance(self, slot_from, intent, context) -> dict
    def export(self, format_id, slots_visited) -> Path

    def _compute_slot_data(self, slot_id, context) -> dict:
        """data_requirements를 확인하고 가능한 데이터를 모음"""
        # available=true 필드만 모음
        # available=false면 fallback 경로

    def _render_answer(self, template, data, fallback=None) -> str:
        """{변수} 치환. 누락 변수 있으면 fallback 사용"""
```

### 데이터 컴퓨터 (직무·슬롯별)

| 컴퓨터 함수 | 사용 슬롯 | 의존 데이터 |
|---|---|---|
| `compute_program_dau_change_pct()` | CP 1_anchor | raas_kpi_latest.csv |
| `compute_weekday_avg_dau()` | 제작PD 1_anchor | raas_kpi_latest.csv (4주 평균) |
| `compute_timeslot_grid()` | 편성PD 1_anchor | Splunk (적재 후) |
| `compute_qoq_yoy()` | 편성PD 3_context | 분기 집계 (구축 후) |
| `compute_corner_breakdown()` | 제작PD 2_cause | 코너 데이터 (구축 후) |

→ Phase 2 ③ 작업 시 우선순위: 즉시 동작 가능한 컴퓨터 → fallback 동작 → 적재 완료 후 본격 컴퓨터.

---

## 5. PostHog 이벤트 통합 스키마

3개 JSON의 `instrumentation` 합집합:

| event | 공통/직무별 | properties |
|---|---|---|
| `storyline_entry` | 공통 | role, first_question_id, default_chips |
| `storyline_chip_click` | 공통 | role, slot_from, slot_to, chip_label, chip_intent |
| `storyline_slot_view` | 공통 | role, slot_id, slot_name, fallback_used |
| `storyline_export` | 공통 | role, output_format_id, slots_visited |
| `storyline_complete` | 공통 | role, slots_visited, duration_s, exported |
| `storyline_hypothesis_input` | 편성PD 전용 | role, hypothesis_text, evaluated |

→ Phase 4 ⑦ 환류 학습에서 `storyline_chip_click`의 (role, slot_from, chip_intent) 3종 키가 `isExplainedBy` 가중치 환류의 핵심.

---

## 6. 직무별 미작성 스토리라인 (잔여)

Phase 2 이후 작성 대상 (8직무 중 5개 잔여):

| 우선순위 | 직무 | 비고 |
|---|---|---|
| 4 | 총괄관리 | CP 다음 1순위 (정형 보고 라인) |
| 5 | 플랫폼전략 | 매주 MAU 보고가 큰 가치 |
| 6 | 서비스 운영 | 실시간 동시사용자 연동 작업 동반 |
| 7 | 마케팅 | 사일로 — 별도 진입 |
| 8 | 데이터(apollo) | 자유 탐색 — 큐레이션 없음 |

→ 3개 만든 후 공통 패턴이 검증됐으므로 **다음 5개는 개당 30분~1시간**으로 빠르게 가능.

---

## 7. 다음 작업 의사결정 포인트

다음 단계 진행 시 결정 필요한 3가지:

1. **Phase 1 ①(데이터 적재) 작업 시작 시기** — 위 §1 1·2순위 표를 직접 가이드로 사용
2. **Phase 2 ③(백엔드) 구현 우선순위** — `entry` 먼저 vs `advance` 먼저
3. **사용자 프로필 확장 시점** — CP→담당 프로그램 매핑 등록 UI는 어디(프로필 모달? 첫 로그인 위저드?)
