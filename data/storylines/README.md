# data/storylines/ — 직무별 대화형 스토리라인 데이터

> 8직무 인터뷰 → 5슬롯 대화 흐름 → 시스템이 읽고 동작하는 JSON
> 근거 문서: [docs/interviews/RAAS_직무별_대화형_스토리라인.md](../../docs/interviews/RAAS_직무별_대화형_스토리라인.md)
> 결정 로그: [docs/decisions.md D-010](../../docs/decisions.md)

---

## 1. 폴더 구조

```
data/storylines/
  README.md             ← 이 파일 (스키마 명세)
  _common_patterns.md   ← 3직무 공통 패턴 + 데이터 적재 통합 우선순위 ✅
  cp.json               ← CP (책임 프로듀서) ✅
  pd_program.json       ← 제작PD ✅
  pd_schedule.json      ← 편성PD ✅
  general_mgmt.json     ← 총괄관리 — Phase 2 이후
  platform_strategy.json
  marketing.json
  service_ops.json
  data_analyst.json     ← 자유 탐색 모드 (큐레이션 X)
```

---

## 2. JSON 스키마 v1

각 직무 파일은 다음 6개 블록으로 구성:

### 2.1 메타 (필수)

```
{
  "schema_version": "v1",
  "role": "CP",
  "role_name_ko": "CP (책임 프로듀서)",
  "updated": "YYYY-MM-DD",
  "source_documents": [...]      // 추적성 확보용
}
```

### 2.2 persona_summary (필수)

```
"persona_summary": {
  "viewpoint": "...",              // 시점 (담당 프로그램군 등)
  "cycle": ["일", "주", "분기"],    // 주기
  "biggest_pain": "...",           // 가장 큰 페인
  "biggest_value_unlock": "...",   // 가장 큰 가치 잠금해제 포인트
  "role_in_information_flow": "..."// 정보 흐름 내 위치
}
```

### 2.3 ontology (4관계 — 필수)

[RAAS_직무별_대화형_스토리라인.md §4](../../docs/interviews/RAAS_직무별_대화형_스토리라인.md) 온톨로지 직접 반영:

| 관계 | JSON 키 | 의미 |
|---|---|---|
| `role hasPrimaryKPI metric` | `hasPrimaryKPI` | 직무의 1순위 지표 |
| `metric isExplainedBy metric` | `isExplainedBy` | 결과를 설명하는 하위/연관 지표 |
| `role prefersComparison axis` | `prefersComparison` | WoW/QoQ/YoY/인접 등 비교축 선호 |
| `role outputsTo format` | `outputsTo` | 산출물 형식 (PPT/PDF/카톡 등) |

각 항목은 `id`, `name`, `priority`, `_blocked` 또는 `_note` 옵션 가짐.

### 2.4 entry (필수)

첫 화면 — 사용자가 빈 대시보드 대신 받는 첫 질문.

```
"entry": {
  "greeting_template": "{user_name}님 안녕하세요. ...",
  "first_question": "**...**부터 보시겠어요?",
  "default_chips": [
    {
      "label": "...",
      "intent": "...",
      "next_slot": "1_anchor" | "2_cause" | ...
    }
  ]
}
```

### 2.5 slots (필수 — 5슬롯)

5슬롯 ID 고정:

| 슬롯 ID | 이름 | 의도 |
|---|---|---|
| `1_anchor` | 앵커 | 직무의 1순위 지표 = 결과 |
| `2_cause` | 원인 | 결과를 설명하는 하위/연관 지표 |
| `3_context` | 맥락 | 비교축 (WoW/QoQ/YoY/인접/벤치마크) |
| `4_loop` | 루프 | 과거 결정·개편·캠페인 사후 효과 (폐쇄루프) |
| `5_closing` | 마무리 | 오늘의 핵심 3가지 + 공유/보고/내보내기 |

각 슬롯 구조:

```
"1_anchor": {
  "name": "...",
  "purpose": "...",
  "data_requirements": [
    {
      "field": "...",
      "source": "...",                  // raas_kpi_latest.csv / Splunk / 별도 등
      "available": true | false,
      "_note": "..."
    }
  ],
  "answer_template": "...",             // {변수}로 데이터 슬롯
  "fallback_answer_template": "...",   // 데이터 부족 시 대체
  "chips_next": [
    {
      "label": "...",
      "intent": "...",
      "next_slot": "...",
      "output_format_id": "..."        // 5_closing에만 — outputsTo의 id
    }
  ]
}
```

### 2.6 data_dependencies (필수)

데이터 적재 작업 우선순위 가이드. Phase 1 ① 진행 시 직접 참조:

```
"data_dependencies": {
  "available_now_can_run": [...],          // 즉시 동작 가능
  "partial_with_fallback": [...],          // fallback으로 일부 동작
  "ingest_blocking_high_priority": [        // 적재 1순위
    {
      "field": "...",
      "source": "Splunk",
      "blocks_slots": ["2_cause"],
      "story_impact": "...",
      "ingest_priority": 1
    }
  ],
  "ingest_blocking_medium_priority": [...]
}
```

### 2.7 expected_user_flows (선택)

테스트 시나리오 — 어떤 경로가 happy path인지 명시.

### 2.8 instrumentation (선택)

PostHog 이벤트 정의. Phase 4 ⑦ 온톨로지 환류 학습의 원천.

```
"instrumentation": {
  "posthog_events": [
    { "event": "storyline_entry", "properties": [...] },
    { "event": "storyline_chip_click", "properties": [...] },
    ...
  ]
}
```

---

## 3. 운영 원칙

### 3.1 한국어 vs 영어

| 분류 | 언어 |
|---|---|
| 스키마 키 (`role`, `slots`, `chips_next`) | 영어 |
| 슬롯 ID (`1_anchor`, `2_cause`) | 영어 |
| Intent ID (`explain_change`, `send_pd_dispatch_kakao`) | 영어 |
| 사용자 노출 텍스트 (`label`, `first_question`, `answer_template`) | 한국어 |
| `_note` 주석 필드 | 한국어 (인터뷰 인용) |

### 3.2 차단(blocked) 표기

JSON에는 주석이 없으므로 다음 컨벤션:

- `"available": false` — 현재 데이터 미적재
- `"_blocked": true` — 영구 차단 (외부 시스템 변경 필요)
- `"_note": "..."` — 한 줄 설명 (인터뷰 인용·이유)
- `"blocks_quality": "high|medium|low"` — 답변 품질에 미치는 영향

### 3.3 fallback 우선

모든 슬롯은 데이터 부족 상태에서도 최소한의 답변을 해야 함. `fallback_answer_template`는 다음 원칙:

1. 무엇이 없는지 명시 (사용자가 답답해하지 않도록)
2. 현재 활용 가능한 단서는 무엇인지 제시
3. 언제 가능해지는지 알림 (Phase 1① 진행 중 등)

### 3.4 변수 치환 컨벤션

`answer_template` 내 변수는 `{변수명}` 또는 포맷 지정 `{변수명:+.1f}` 사용:

| 패턴 | 예 |
|---|---|
| 단순 텍스트 | `{user_name}`, `{top_change_program}` |
| 정수 천단위 | `{prev_dau:,}` |
| 부호 포함 1자리 소수 | `{change_pct:+.1f}` |
| 날짜 | `{peak_date}` (YYYY-MM-DD) |

---

## 4. 백엔드 연결 (Phase 2 작업)

이 JSON은 다음 엔드포인트의 단일 출처가 됨 (구현은 Phase 2 ③):

```
GET  /api/storyline/entry?role={role}
     → JSON.entry 직접 반환

POST /api/storyline/advance
     body: { role, slot_from, chip_intent, context }
     → 1) chip에서 next_slot 추출
       2) slot의 data_requirements 확인
       3) 가능한 데이터로 answer_template 채움 (없으면 fallback)
       4) chips_next 반환
       5) PostHog storyline_chip_click 이벤트 발사

POST /api/storyline/export
     body: { role, output_format_id, slots_visited }
     → outputsTo의 format에 따라 PPT/카톡 메시지 등 생성
```

---

## 5. Phase 4 환류 (자가 학습)

PostHog 이벤트 → 분석 → 가중치 업데이트:

```
storyline_chip_click 로그 집계
    ↓
chip_intent별 클릭 빈도
    ↓
isExplainedBy 관계 가중치 강화
    ↓
다음 cp.json 자동 patch (chips_next 순서 조정)
```

→ "사용자가 자주 클릭하는 원인 후보"가 자동으로 첫 칩으로 올라옴.
→ 데이터 직무가 인터뷰에서 명시한 **"질의·활용 기록으로 니즈 파악, 직무-데이터 관계 매핑"** 자동화 청사진.

---

## 6. 다음 작업

### Phase 1 ② 완료 (3직무)
- [x] cp.json
- [x] pd_program.json
- [x] pd_schedule.json
- [x] [_common_patterns.md](_common_patterns.md) — 공통 패턴 + 데이터 적재 통합 우선순위

### Phase 2 이후 작성 대상 (5직무 잔여)
- [ ] general_mgmt.json — CP 다음 1순위 (정형 보고 라인)
- [ ] platform_strategy.json — 매주 MAU 보고 가치
- [ ] service_ops.json — 실시간 동시사용자 연동 동반
- [ ] marketing.json — 사일로 별도 진입
- [ ] data_analyst.json — 자유 탐색 (큐레이션 X)

→ 3직무 만든 후 공통 패턴 검증됨 ([_common_patterns.md](_common_patterns.md) §2). **잔여 5개는 개당 30분~1시간**으로 빠르게 가능.

### 즉시 Phase 2/3 진입 가능 신호

3개 JSON이 검증한 것:
1. **데이터 적재 1순위 4건** = 3직무 동시 잠금해제 ([_common_patterns.md](_common_patterns.md) §1)
2. **백엔드 엔드포인트 4종** 스펙 확정 ([_common_patterns.md](_common_patterns.md) §3)
3. **PostHog 이벤트 6종** 통합 스키마 ([_common_patterns.md](_common_patterns.md) §5)
4. **PoC 시연 최단 경로** — 매핑 메타데이터만으로 3직무 동시 시연 가능 ([_common_patterns.md](_common_patterns.md) §1.3)
