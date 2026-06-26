# CP 스토리라인 스키마 v2 — 토글 + 공통 출구 계약

현재 `cp.json`(schema_version v1)을 대체할 to-be 스키마. 8노드로 수렴하며 두 가지
신규 개념 — **토글(toggle)** 과 **공통 출구 세트(common_exits)** — 를 정식 필드로 도입한다.

라이브 엔진은 v1을 읽으므로 **`cp_v2.json`을 나란히** 두고, 엔진 migration 완료 후 전환한다.
검증용 내비 프로토타입은 `data/storylines/cp_tobe.json` + `raas_tobe_sim.py`.

---

## 1. 노드 매핑 (v1 13 → v2 8)

| v2 슬롯 id | 이름 | v1에서 흡수 | 비고 |
|---|---|---|---|
| `1_anchor` | 앵커(통합) | `1_anchor` + `1_anchor_scan` + `1_anchor_more` | 지표·기간 토글 |
| `2_cause` | 원인 종합 | `2_cause` + `2_cause_weekly` + `2_cause_monthly` | 기간 토글 |
| `3_adjacent` | 인접 비교 | `3_adjacent` | — |
| `3_context` | PD 공유 | `3_context` | 출력 노드 |
| `4_revision` | 개편 자동감지 | `4_revision` | — |
| `4_revision_detail` | 개편 전후분석 | `4_revision_detail` + `4_loop` | 비교창 토글 |
| `5_closing` | 보고/산출물 | `5_closing` | 출력 노드 |
| `_end` | 산출물 종료 | `_end` | terminal |

기존 슬롯 id를 유지해 **slot computer 재사용**을 극대화한다(`_compute_cp_*`).

---

## 2. 토글 (`toggles`) — 슬롯 레벨 필드

```json
"toggles": {
  "metric": {
    "label": "지표",
    "options": [
      {"id": "dau",   "label": "DAU",   "default": true},
      {"id": "deep",  "label": "깊은청취"},
      {"id": "retention", "label": "리텐션"}
    ]
  },
  "period": {
    "label": "기간",
    "options": [
      {"id": "day",  "label": "일", "default": true},
      {"id": "week", "label": "주"},
      {"id": "month","label": "월"}
    ]
  }
}
```

### 엔진 계약
- 클라이언트는 활성 슬롯의 `toggle_state`(예: `{"metric":"deep","period":"week"}`)를 유지한다.
- **토글 변경**: `advance(slot_from=현재슬롯, chip_intent="__toggle__", toggle_state=새상태)` 호출.
  엔진은 `next_slot = slot_from` 으로 **같은 슬롯을 재렌더**하고, **이동 경로(step)로 계상하지 않는다.**
- **전진/진입**: 칩의 `set_toggle`로 초기 토글값을 주입할 수 있다(아래 4).
- slot computer는 `ctx["toggle_state"]`를 읽어 분기한다.
  - `_compute_cp_anchor` — `metric`으로 정렬 기준, `period`로 비교 창 선택.
  - `_compute_cp_cause` — `period`로 일/주/월 분해 선택(기존 `_weekly`/`_monthly` 컴퓨터를 흡수·통합).
  - `_compute_cp_revision_detail` — `window`(2/4/8주)로 전후 평균 창 선택.
- 토글 미정의 슬롯은 `toggles` 필드를 생략한다.

---

## 3. 공통 출구 세트 (`common_exits` + 슬롯의 `exits`)

최상위에 한 번 정의하고, 각 슬롯이 id로 참조한다(반복 제거).

```json
"common_exits": {
  "share":    {"label": "담당 PD에게 공유",   "intent": "draft_pd_dispatch", "next_slot": "3_context"},
  "report":   {"label": "국장 보고서에 넣기", "intent": "open_report",       "next_slot": "5_closing"},
  "anchor":   {"label": "← 다른 프로그램",    "intent": "back_to_anchor",    "next_slot": "1_anchor", "reset_scope": true},
  "freetext": {"label": "자유질의 입력",      "intent": "freetext",          "next_slot": "__freetext__"}
}
```

슬롯에서:
```json
"exits": ["share", "report", "anchor", "freetext"]
```

### 엔진 계약
- 슬롯 응답의 `chips_next` = `슬롯 고유 chips_next` + `exits를 resolve한 공통 칩`(순서대로 뒤에 append).
- 슬롯 computer가 동적 `chips_next`를 내놓아도 **exits는 항상 뒤에 보강**한다(막힘 방지의 핵심).
- `__freetext__`: 엔진은 라우터/쿼리엔진에 위임하고, 답변 후 **`slot_from`을 유지**해 직전 슬롯 칩으로 복귀(레일).
- `anchor` 출구의 `reset_scope:true`: 프로그램 스코프(top_change_program_code 등)를 비우고 앵커로.
- 출력 노드(`3_context`, `5_closing`)는 `exits`에 `anchor`만 두거나 생략한다.

---

## 4. 칩 확장 필드

| 필드 | 의미 |
|---|---|
| `set_toggle` | 진입 시 대상 슬롯의 토글 초기값 주입. 예: `{"metric":"deep","period":"day"}` |
| `output_format_id` | `_end` 칩의 산출물 식별자(기존 v1과 동일) |
| `reset_scope` | 공통 출구 `anchor` 전용 — 프로그램 스코프 초기화 |

---

## 5. 진입 자동 우선순위 (`entry.priority_rule`)

```json
"entry": {
  "priority_rule": {
    "by": "data_last_weekday",
    "promote": {"mon": "show_biggest_change_week", "month_start": "show_biggest_change_month"}
  },
  "default_chips": [ ... ]
}
```
엔진은 `data_last_date` 요일을 보고 `default_chips`를 재정렬한다(월요일→전주 칩 최상단, 1일 직후→전월 칩). 규칙이 없으면 정의된 순서 그대로.

---

## 6. 로깅 영향 (앞선 이력 분석 제안과 연결)

토글 슬롯은 `topic_key`를 슬롯만이 아니라 **`{slot}:{metric}:{period}`** 로 적재해
토글 내부의 관심사(어떤 지표·기간을 주로 보나)를 잃지 않는다. `__toggle__` 재렌더와
`__freetext__` 복귀는 step으로 계상하지 않되, **토글 변경/자유질의 이벤트는 별도 로깅**한다.

---

## 7. 엔진 Migration 체크리스트 (다음 단계)

1. `load_config` — v2 로드 경로 분기(`schema_version == "v2"`).
2. `advance()` — `toggle_state` 인자 수신, `__toggle__` 재렌더(무계상), `exits` resolve·append.
3. `__freetext__` 위임 + `slot_from` 복귀 처리.
4. computer 통합: `_compute_cp_cause`가 `period` 토글 흡수(`_weekly`/`_monthly` 제거), `_compute_cp_anchor`가 `metric` 정렬, `_compute_cp_revision_detail`가 `window` 흡수(`4_loop` 통합).
5. `entry` default_chips 요일 재정렬.
6. `save_query` topic_key를 토글 차원 포함으로.
7. 서버 `/api/storyline/advance` — `toggle_state` 패스스루.
