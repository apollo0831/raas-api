# 스토리라인 이력 DB 재설계 — 2계층(경로 로그 + 내용 캐시)

> 상태: **설계(검토 대기)**. 승인 후 단계 구현.
> 대상: `raas_history_db.py`(SQLite), 스토리라인 엔진/서버, 추천·캐싱.
> 작성 배경: 현재 "칩 클릭 1행" 적재는 **경로(어떻게 왔나)** 와 **내용(무엇을 계산했나)** 을
> 한 행에 섞어, 캐싱(토큰 절약)·이동경로 분석 양쪽에 비효율.

---

## 1. 문제 정의 (현행)

현재 스토리라인 이동은 `query_history`(원래 자연어 질의용)에 칩 단위로 적재된다.

| 컬럼 | 현재 용도 | 한계 |
|---|---|---|
| `topic_key` = `{intent}:{scope}:{metric}` | 그룹핑/인기 집계 | 같은 **내용**이 경로마다 다른 키 → dedup 실패 |
| `source` = `general`/`storyline` | 출처 구분 | 세션 경로(순서·전이) 복원 불가 |
| `input/output/cache_*_tokens` | 토큰 회계 | 측정은 되나, 재사용으로 줄일 구조가 없음 |

**핵심 결함 2가지**
1. **캐싱 불가** — "박소현의 러브게임 / DAU / 일간 / 2026-06-24" 답변을 앵커·질의창·KPI표 어디로 도달하든 **매번 LLM 재호출**. 내용 동일성이 키에 안 잡힘.
2. **경로 분석 불가** — 행은 있지만 `세션→전이 순서`가 없어 funnel·이탈지점·트랙 선호·**죽은 칩 탐지**를 못 함.

원인은 하나: **경로와 내용을 한 키(topic_key)에 묶었기 때문.** → 둘로 분리한다.

---

## 2. 목표 아키텍처 — 2계층 분리

```
사용자 행동
   │
   ├─(전이 1건)──────────────► [A] storyline_events   (경로 로그, append-only)
   │                                  │ analysis_key (FK)
   │                                  ▼
   └─(내용 계산 요청)──────────► [B] analysis_cache    (내용 캐시, data_date로 무효화)
                                      │
                                      ▼
                              [C] 추천/집계 (A·B에서 파생, 뷰/배치)
```

- **A 경로 로그**: 전이(edge) 1건 = 1행. "무엇을 봤나"는 `analysis_key`로 **참조만**.
- **B 내용 캐시**: "무엇을 계산했나"로 dedup. 경로와 무관. `data_date` 포함이 핵심.
- **C 추천/집계**: A의 빈출 다음행동 + B의 본인 최근 키.

`query_history`는 **자연어 일반질의 전용으로 환원**(현행 유지), 스토리라인 적재는 A/B로 이관.

---

## 3. 스키마

### [A] `storyline_events` — 이동경로(전이 단위)

```sql
CREATE TABLE IF NOT EXISTS storyline_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,         -- 스토리라인 세션(진입~종료) 단위
    seq         INTEGER NOT NULL,      -- 세션 내 순번(0,1,2,...)
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id     TEXT,
    user_role   TEXT,                  -- cp_v2 등
    event_type  TEXT NOT NULL,         -- enter|chip|toggle|freetext|exit|export
    slot_from   TEXT,
    slot_to     TEXT,
    chip_intent TEXT,                  -- 클릭한 칩 intent(또는 set_toggle 키)
    -- ── 분석 대상 구조화 저장 — 프로그램·기간·지표만(범주·정렬기준은 저장 제외, 검토 결정) ──
    program_code TEXT,                 -- 프로그램(있으면)
    program_name TEXT,                 -- 표시용 이름(예: 박소현의 러브게임)
    channel_code TEXT,                 -- F00|L00 (채널 집계 슬롯 식별자)
    metric      TEXT,                  -- DAU|WAU|깊은청취율 ... (focus 지표; kpi_metric 포함)
    period      TEXT,                  -- day|week|month
    window      TEXT,                  -- 2w|4w|8w (개편 전후분석)
    end_reason  TEXT,                  -- exit/export 이벤트에만: export|free_query|timeout
    analysis_key TEXT,                 -- [B] 캐시 참조(내용 식별). 출력 없는 전이는 NULL
    cache_hit   INTEGER,               -- 1=캐시 적중(0토큰), 0=신규 계산
    latency_ms  INTEGER,
    output_tokens INTEGER              -- 이 전이로 실제 소비한 토큰(hit면 0)
);
CREATE INDEX IF NOT EXISTS idx_se_session  ON storyline_events (session_id, seq);
CREATE INDEX IF NOT EXISTS idx_se_role_slot ON storyline_events (user_role, slot_from, slot_to);
CREATE INDEX IF NOT EXISTS idx_se_akey     ON storyline_events (analysis_key);
CREATE INDEX IF NOT EXISTS idx_se_user_time ON storyline_events (user_id, created_at DESC);
```

> **분석 대상 = 프로그램·기간·지표**(검토 결정; 범주·정렬기준은 저장 제외) → "박소현의 러브게임 · DAU · 일간"을
> 텍스트 파싱 없이 그대로 질의·집계 가능. 정렬기준은 뷰 속성이라 캐시·저장 모두 제외, 범주는 KPI표 캐시 키 내부에서만 사용(저장 안 함).

**계상 규칙**(스키마 문서 §6과 정합)
- `__toggle__` 제자리 재렌더, `__freetext__` 복귀 = **step 무계상**이지만 `event_type=toggle|freetext`로 **이벤트는 적재**.
- 공통 출구·동적 칩 모두 전이 1건으로 기록.

### [B] `analysis_cache` — 내용(계산결과)

```sql
CREATE TABLE IF NOT EXISTS analysis_cache (
    analysis_key TEXT PRIMARY KEY,     -- 아래 §4 해시
    data_date    TEXT NOT NULL,        -- 계산에 쓰인 KPI 데이터 일자(무효화 키)
    slot_type    TEXT NOT NULL,        -- anchor|cause|impact|adjacent|revision|revision_detail
    program_code TEXT,
    metric       TEXT,
    period       TEXT,
    window       TEXT,
    answer_md    TEXT NOT NULL,        -- 본문(분석대상 블록+polished+provenance)
    chart_json   TEXT,                 -- chart_data 직렬화
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hits         INTEGER DEFAULT 0,    -- 재사용 횟수(절약 측정)
    gen_tokens   INTEGER               -- 최초 생성 비용(절약량 = hits * gen_tokens)
);
CREATE INDEX IF NOT EXISTS idx_ac_date ON analysis_cache (data_date);
CREATE INDEX IF NOT EXISTS idx_ac_prog ON analysis_cache (program_code, slot_type);
```

---

## 4. `analysis_key` 정의 — 내용 동일성의 단일 진실원

```python
def analysis_key(slot_type, *, channel_code, program_code, category,
                 metric, period, window, data_date):
    # 경로(어떤 칩으로 왔는지)·정렬기준은 포함하지 않는다 — 내용을 바꾸는 것만.
    parts = [slot_type, channel_code or "-", program_code or "-",
             category or "-", metric or "-", period or "-",
             window or "-", data_date]
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:16]
```

- **`data_date` 필수 포함** → `raas_kpi_latest.csv`가 매일 **06:50 갱신**(project: Splunk 스케줄)되면
  날짜가 바뀌어 이전 키는 자연히 미적중 = **자동 무효화**. 명시적 invalidation 불필요.
- `slot_type`은 v1/v2 슬롯 id가 아니라 **계산기 종류**로 정규화(`2_cause`·`2_cause_weekly`·period 토글이 모두 `cause`로 수렴) → 버전·경로 무관 dedup.

**키 차원은 "내용을 바꾸는가"로 결정** (검토 의견 반영):
| slot_type | 키에 들어가는 차원 | 비고 |
|---|---|---|
| cause / impact / revision_detail | program_code, metric, period, (window) | 프로그램 단위 → channel은 program에 내포 |
| anchor / kpi(범주표) | **channel_code**, category, period | 채널 집계 → 파워FM/러브FM 내용이 다름, 충돌 방지 위해 channel 필수 |
| adjacent | program_code, period | 시간대 이웃 비교 |

- **정렬기준(sort_aspect)은 키에서 제외** — 변화율/변화량/현재값은 동일 행의 **표시 순서만** 바꾸는 뷰 속성(클라 재정렬). 내용 불변 → 캐시 1건 공유. (단 events에는 사후분석용으로 기록)
- **채널·권한은 "필터"로는 내용에 영향 없음**(검토 의견 — CP는 담당 채널 전체를 봄). 다만 **채널 집계 슬롯**은 두 채널의 데이터가 본질적으로 다르므로, 공유 캐시 충돌 방지를 위해 `channel_code`를 키에 포함(권한 필터가 아니라 데이터 식별자).

**`data_date` 취득**: 엔진은 이미 `_kpi_rows()`의 최신 `DATE`를 사용(예: `_compute_cp_revision_detail`의 `last_date`). 이를 캐시 키 인자로 표준화.

---

## 5. 흐름 (계산 1회 경로)

```
advance(slot, chip, ctx, toggle)
  → _compute_slot_data 가 (slot_type, program, metric, period, window, data_date) 결정
  → key = analysis_key(...)
  → B 조회:
       HIT  → 캐시 answer/chart 반환, output_tokens=0, cache_hit=1
       MISS → computer 실행(LLM polish 포함) → B 저장(gen_tokens), cache_hit=0
  → A 에 전이 1행 적재 (analysis_key, cache_hit, latency, tokens)
```

토큰 절약 = `Σ(analysis_cache.hits × gen_tokens)`. 대시보드로 가시화 가능.

---

## 6. [C] 추천/집계 (파생)

A·B에서 배치/뷰로 산출(쓰기 경로 부담 없음).

1. **시점별 빈출 다음행동** — `(user_role, slot_from)` → `slot_to/chip_intent` 상위 N.
   → "이 단계에서 다른 CP들이 자주 본 것" 추천. **거의 안 쓰는 칩 = 제거 후보**(이번에 지운 죽은 칩처럼 데이터로 탐지).
2. **본인 이어보기** — 사용자 최근 `analysis_key` → "어제 보던 *두시탈출 컬투쇼* 주간 이어보기".
3. **트랙 funnel** — 진입→앵커→원인→공유/보고 도달률, 이탈 슬롯.

---

## 7. 통합 지점 (현 코드 기준)

| 위치 | 변경 |
|---|---|
| `raas_history_db.py` | `storyline_events`·`analysis_cache` 테이블 + `log_event()`·`cache_get()`·`cache_put()` 추가. `query_history`는 일반질의 전용으로 유지 |
| `raas_storyline_engine.py` `_compute_slot_data`/`_cp_v2_compute` | 계산 전 `cache_get`, miss 시 `cache_put`. `slot_type`·`data_date` 정규화 헬퍼 |
| `raas_server.py` `/api/storyline/advance`, `/api/query/stream` | 전이마다 `log_event(session_id, seq, ...)`. `session_id`는 클라가 세션 시작 시 발급·헤더로 전달 |
| `raas_web.html` | 세션 id 생성·전달, 추천 칩 노출(선택) |
| `raas_querymap.py` | 집계 소스를 A로 확장(경로/트랙 통계) |

**하위호환**: 기존 `query_history` 스토리라인 행은 보존(읽기 전용 과거 데이터). 신규만 A/B로.

---

## 8. 구현 단계

1. **B 캐시** — ✅ **구현 완료(2026-06-24)**. `analysis_cache` 테이블 + `cache_get/put`(raas_history_db.py), `_analysis_key`·`_cached_compute`(raas_storyline_engine.py), `_cp_v2_compute`의 `2_cause`·`2_impact` 래핑. 적중 시 LLM 0회 검증(cause/impact 동일 답변·칩, hits 누적).
2. **A 경로 로그** — ✅ **구현 완료(2026-06-24)**. `storyline_events` 테이블 + `log_event()`/`get_session_events()`/`get_recent_sessions()`(raas_history_db.py). 서버 `/api/storyline/advance`(칩·토글·export)와 `/api/query/stream`(질의입력=freetext / 자유질의=exit·free_query)에서 전이 적재. 프론트가 진입 시 `sessionId` 발급해 모든 호출에 `storyline_session` 전달. seq는 세션 내 자동 증가, 분석대상(프로그램·지표·기간)·cache_hit·analysis_key 기록.
3. **C 추천** — ✅ **구현 완료(2026-06-24)**. 엔드포인트 `GET /api/storyline/recent`(최근 7일 세션 요약)·`/session?id=`(여정)·`/recommend?slot=`(빈출 다음행동) + DB `get_recent_sessions/get_session_events/get_frequent_next`. 프론트 사이드바에 **최근 분석 여정** 섹션(세션 카드 → 클릭 시 여정 펼침, 각 단계에 분석대상·이동수단·캐시적중 표시).
4. **정리** — ✅ **구현 완료(2026-06-24)**. `/api/storyline/advance`의 **칩 네비게이션 `save_query` 제거** → 경로는 `storyline_events`, 답변 캐시는 클라 localStorage(`_getCachedAnswer`)로 분리. `query_history`는 **사용자가 직접 입력한 질의 전용**(일반 + 라우팅된 typed 질의). 칩 이동은 "최근 질의"에서 빠지고 "최근 분석 여정"으로 이동.

전 단계(1~4) 구현 완료. 각 단계 독립 배포 가능.

### 4단계 메모
- 클라 답변캐시(`_getCachedAnswer`)는 localStorage라 query_history와 무관 → 칩 적재 제거해도 replay 영향 없음.
- 라우팅된 typed 질의(`intent="storyline_routed"`)는 `query_history`에 유지(사용자 입력 + query_id/feedback 연결) + `storyline_events`에도 기록. 순수 칩/토글 이동만 events 전용.

### 2단계 구현 메모
- 세션 발급: 프론트 `_RAAS_STORYLINE.sessionId`(진입 시 1건), advance·query/stream 모두 전달.
- 종료 신호: `_end` 도달 → `event_type=export`(`end_reason=export`); 라우팅 안 된 자유질의 → `event_type=exit`(`end_reason=free_query`). 유휴 타임아웃은 배치(추후).
- 적재 안전: `log_event`는 실패해도 본 흐름 방해 안 함(try/except).
- 분석대상은 프로그램·지표·기간만(범주·정렬 제외, §12). `analysis_key`로 [B] 캐시와 연결.
- 미적용(추후): 죽은 프론트 경로 `show_full_scan`→`1_anchor_scan`(삭제된 v1 슬롯)에는 세션 미부착 — v1 정리 시 함께 제거.

### 1단계 구현 메모
- 캐싱 대상: LLM polish가 무거운 `cause`·`impact`만(`_CACHE_SLOT_TYPES`). 앵커/KPI표/인접/개편은 추후.
- 키: `(slot_type, channel, program, metric, period, window, data_date)` — cause는 metric을 period로 결정(DAU/WAU/MAU), impact는 `kpi_metric` 우선.
- 절약 측정: `analysis_cache.gen_tokens × hits`. `data_date` 갱신 시 키 미적중으로 자동 만료(삭제 배치 불필요, 행은 영구 보존).
- 안전장치: DB 미가용 시 캐시 비활성(`_cache_get/_put=None`), 손상 payload는 재계산.

---

## 9. 결정 사항 (검토 반영 — 확정)

| 항목 | 결정 |
|---|---|
| **세션 수명** | 진입(entry) ~ 종료. 종료 = ① `_end`(산출물) 도달, ② **라우팅 안 된 자유질의 직전**, ③ 유휴 타임아웃(기본 30분). (§10) |
| **캐시 유효기간** | 당일 `data_date` 한정 — 다음날 06:50 데이터 갱신 전까지. 갱신되면 키 미적중으로 자동 만료. |
| **최근 질의 노출** | 최근 **7일**까지 UI 노출. |
| **DB 보존** | events·cache **영구 저장**(삭제 안 함). 지난 `data_date` 캐시 행은 적중만 안 될 뿐 사후분석용으로 보존. |
| **채널/권한** | 권한 필터로는 내용 영향 없음. 단 채널 집계 슬롯은 `channel_code`를 캐시 키에 포함(데이터 식별, §4). |

---

## 10. 세션 경계 — "스토리라인 잇기" vs "자유질의" 구분

**이미 구분 가능**(현 코드). `/api/query/stream`의 라우팅 결과가 신호:

```
질의창 입력
  → ROUTER.route(question, lenient=_v2)
       프로그램/지표 인식 + 활성 CP 슬롯  → 슬롯 라우팅, SSE: routed_via_storyline=True  ⇒ 세션 '잇기'(전이 1건)
       인식 실패(프로그램/지표 없음)        → 일반 쿼리엔진 경로(플래그 없음)              ⇒ '자유질의' = 세션 종료 신호
```

- **세션 종료 판정**: `routed_via_storyline`가 아닌 자유질의가 들어오면, **그 직전까지**를 한 세션으로 마감(`end_reason='free_query'`). 자유질의 자체는 세션 밖(또는 새 세션 아님).
- `_end` 도달 시 `end_reason='export'`. 마지막 이벤트 후 30분 무활동이면 배치가 `end_reason='timeout'`으로 마감.
- **경계 케이스**(명시): "박소현의 러브게임 게스트 누구야?"처럼 프로그램명을 포함하지만 의도는 자유질의인 입력은 현재 lenient 라우팅상 '잇기'로 잡힘. 정밀화가 필요하면 라우터에 *질의 의도(원인/일반)* 판별을 1단계 추가 가능 — 1차 구현은 현 휴리스틱 채택, 오분류는 로그(`event_type=freetext`)로 사후 측정.

---

## 11. 이력 세션 표현 (최근 질의 / 이력 보기)

세션 1건 = `storyline_events`의 `session_id` 그룹. 조회 편의를 위해 요약을 **뷰 또는 경량 요약 테이블**로 둔다.

### 세션 요약 (파생)
```sql
-- storyline_sessions: events에서 세션 마감 시 1행 upsert (또는 VIEW)
session_id, user_id, user_role,
started_at, ended_at, end_reason,
entry_intent,          -- 진입 칩(트랙): 일간변동|전주|전월|지표탐색|개편
primary_program, primary_category, primary_metric, primary_period, primary_sort,  -- 대표 분석 대상(첫 분석 노드 기준)
steps,                 -- 전이 수(무계상 재렌더 제외)
reached_report,        -- 5_closing/3_context 도달 여부
outputs_json           -- 생성 산출물 output_format_id 목록
```

### (a) 최근 질의 — 카드 리스트 (최근 7일)
세션 1건 = 카드 1개:
```
┌─────────────────────────────────────────────┐
│ 파워FM · 박소현의 러브게임            6/24 09:12 │   ← 채널 · 대표 프로그램 · 시각
│ 규모 · DAU · 일간 · 변화율                       │   ← 분석 대상 5종(_target_block)
│ 원인 분석 → 인접 비교 · 국장 보고서 ✓            │   ← 트랙 경로 + 산출물 여부
└─────────────────────────────────────────────┘
   클릭 → 마지막 슬롯에서 이어보기 | 길게 → 이력 상세
```
- 정렬: `started_at DESC`, 7일 이전은 숨김(DB엔 잔존).
- "이어보기": 세션의 마지막 `slot_to` + 그 전이의 분석 대상으로 재진입(캐시 적중 시 0토큰).

### (b) 이력 보기 — 세션 상세(여정)
선택 세션의 `events`를 순서대로 펼침:
```
진입: 일간 변동 큰 프로그램
 └ 1_anchor   앵커        대상: 박소현의 러브게임 · 규모 · DAU · 일간 · 변화율
 └ 2_cause    원인 종합   (칩: 왜 변했어?)            [cache ●]
 └ 3_adjacent 인접 비교   (칩: 비슷한 시간대)          [신규 ○]
 └ 질의입력  "컬투쇼 깊은청취율" → 2_impact            [라우팅·잇기]
 └ 5_closing  보고/산출물  → 국장 1장 PPT (export)
```
- 각 줄: 슬롯명 · **분석 대상 한 줄**(events 컬럼에서 조립) · 이동 수단(칩/토글/질의입력) · 캐시 적중 표시.
- 상단에 경로 스트립(entry→anchor→cause→adjacent→report) 미니맵.
- 분석 대상이 컬럼화돼 있어 **텍스트 파싱 없이** 한 줄 표기·필터·재현 가능.

---

## 12. 분석 대상 저장 현황 → 보강 (확인 결과)

현행 `query_history`(스토리라인 적재, `raas_server.py` `/api/storyline/advance`)는 **부분만** 저장:

| 분석 대상 | 현행 | 신규 events | 저장 여부(결정) |
|---|---|---|---|
| 프로그램 | ✅ `scope`(코드)+라벨 | `program_code`+`program_name` | **저장** |
| 기간 | ✅ `topic_key`의 dim | `period` 컬럼 | **저장** |
| 지표 | △ 토글 metric만(`kpi_metric` 누락) | `metric` 컬럼(focus 포함) | **저장**(보강) |
| 범주 | ❌ | — | 제외 |
| 정렬기준 | ❌ | — | 제외(뷰 속성) |

→ 저장 대상은 **프로그램·기간·지표** 3종. 적재 지점(2단계): `/api/storyline/advance`가 `result.get("toggle_state")` + `context_out`(program/`kpi_metric`)에서 3종을 뽑아 `log_event(...)`.
