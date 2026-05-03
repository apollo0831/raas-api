# RAAS 온톨로지 통합 설계 문서 (Phase 5-A)

> Phase 5-B (Claude Code 통합 작업)의 작업 지시서.
> 이 문서를 입력으로 Claude Code 세션에서 실제 RAAS 시스템에 온톨로지를 통합한다.

**문서 구조**:
1. 목표와 비목표
2. 아키텍처 개요
3. 모듈 명세
4. API 명세
5. 코드 변경 매트릭스
6. 마이그레이션 전략
7. 검증 계획
- 부록 A: 어댑터 참조 구현 (`raas_ontology_adapter.py`)
- 부록 B: 회귀 테스트 (`phase5_regression_test.py`)

---

# 챕터 1: 목표와 비목표

## 1.1 Phase 5의 목표

**최종 목표**: RAAS 운영 코드(`briefing_engine.py`, `query_engine.py`, `server.py`, `web.html`)에서 도메인 지식의 **단일 출처(Single Source of Truth)** 가 온톨로지 파일이 되도록 만든다.

**구체 목표**:
1. 어댑터 모듈을 신설해 RAAS 코드가 온톨로지를 일관된 인터페이스로 조회하게 함
2. 기존 코드의 하드코딩된 메타지식(약 235줄)을 어댑터 호출로 교체
3. 새 metric/프로그램/룰 추가 시 `.ttl` 파일만 수정하면 시스템 전체에 자동 반영되도록 함
4. 기존 동작과 출력의 완전 호환성 유지 (사용자가 변화를 느끼지 않아야 함)
5. 신규 능력 1개 추가: `/api/schema` 엔드포인트로 클라이언트에 온톨로지 노출

## 1.2 비목표 (이번 단계에서 하지 않는 것)

- ❌ Splunk SPL 쿼리 수정 (`raas_kpi_save.spl` 그대로 둠)
- ❌ CSV 데이터 스키마 변경 (`raas_kpi_latest.csv` 컬럼 그대로 둠)
- ❌ Phase 4.5에서 정의한 `AtRiskProgram_v2` 활성화 (필요 필드가 SPL에 없음)
- ❌ Person 인스턴스 도입 (Phase 2-C-3에서 별도 진행)
- ❌ `web.html` 구조 변경 (메타지식만 외부화, UI는 그대로)
- ❌ SPARQL 쿼리 엔진 도입 (Python으로 동등 기능 구현)
- ❌ 신규 비즈니스 룰 추가 (기존 룰의 통합만)

## 1.3 성공 기준

1. **기능 호환**: 기존 `/api/briefing`, `/api/query`의 응답이 변경 전후 동일
2. **코드 감축**: `briefing_engine.py` + `query_engine.py` 합계 줄 수가 235줄 이상 감소
3. **신규 능력**: `/api/schema` 호출 시 온톨로지 정보 반환
4. **테스트**: 회귀 테스트 12개 모두 통과
5. **문서**: 어댑터 모듈에 docstring 완비

---

# 챕터 2: 아키텍처 개요

## 2.1 현재 아키텍처 (Phase 5 이전)

```
┌─────────────────────────────────────────────────────┐
│  raas_web.html                                      │
│   - getMetricField() (24개 분기)                    │
│   - 모달 정의 텍스트                                │
└────────────┬────────────────────────────────────────┘
             │ HTTP
             ▼
┌─────────────────────────────────────────────────────┐
│  raas_server.py                                     │
│   - get_cached_timeline() ── 모든 API의 진입점      │
└──┬──────────────────────┬───────────────────────────┘
   │                      │
   ▼                      ▼
┌──────────────┐   ┌──────────────┐
│ briefing_    │   │ query_       │
│ engine.py    │   │ engine.py    │
│              │   │              │
│ FIELD_ALIASES│   │ KEYWORD_TO_  │
│  (87줄)      │   │ CODE (15줄)  │
│ PGM_NAMES    │   │ PGM_MAP(33줄)│
│  (33줄)      │   │ field_map    │
│ build_s7()   │   │  (8줄)       │
│  (25줄, 9룰) │   │ risks 룰     │
│              │   │  (10줄)      │
└──────┬───────┘   └──────┬───────┘
       │                  │
       ▼                  ▼
   ┌────────┐       ┌────────┐
   │ Splunk │       │ Splunk │
   └────────┘       └────────┘
```

**문제점**: 같은 도메인 지식이 여러 곳에 분산. 변경 시 6곳 동시 수정 필요.

## 2.2 목표 아키텍처 (Phase 5 이후)

```
┌─────────────────────────────────────────────────────┐
│  raas_web.html                                      │
│   - getMetricField() ── /api/schema 호출로 대체     │
│   - 모달 정의 ── 온톨로지에서 동적 로드             │
└────────────┬────────────────────────────────────────┘
             │ HTTP
             ▼
┌─────────────────────────────────────────────────────┐
│  raas_server.py                                     │
│   - get_cached_timeline()                           │
│   - /api/schema  ← 신규                             │
│   - /api/concept ← 신규                             │
└──┬──────────────────────┬───────────────────────────┘
   │                      │
   ▼                      ▼
┌──────────────┐   ┌──────────────┐
│ briefing_    │   │ query_       │
│ engine.py    │   │ engine.py    │
│              │   │              │
│ FIELD_ALIASES│   │ KEYWORD_TO_  │
│ → 어댑터     │   │ CODE → 어댑터│
│ build_s7()   │   │ risks       │
│ → 어댑터     │   │ → 어댑터    │
└──────┬───────┘   └──────┬───────┘
       │                  │
       └────────┬─────────┘
                ▼
   ┌─────────────────────────────────┐
   │ raas_ontology_adapter.py ★ 신규 │  ◄── 단일 출처
   │                                 │
   │  - get_field_info(field_name)   │
   │  - find_program_by_keyword(kw)  │
   │  - get_program_meta(code)       │
   │  - evaluate_alerts(kpi)         │
   │  - classify_program(kpi)        │
   │  - get_schema_dump()            │
   └────────────┬────────────────────┘
                │
                ▼
        ┌──────────────┐
        │ .ttl 파일들  │
        │ (5개)        │
        └──────────────┘
```

**핵심 변화**: 온톨로지 어댑터가 **유일한 도메인 지식 출처**가 됨.

## 2.3 데이터 흐름 (예시)

사용자가 `"컬투쇼 어제 청취자?"` 질의를 보낸 경우:

```
[현재] query_engine.py
   └─ KEYWORD_TO_CODE 사전에서 "컬투" 검색 → "F09"
   └─ field_map에서 "dau" → "dau_today"
   └─ Splunk 조회

[Phase 5 이후] query_engine.py
   └─ adapter.find_program_by_keyword("컬투")  → "F09"
   └─ adapter.get_field_info("dau")           → MetricVariant 정보
   └─ Splunk 조회 (변경 없음)
```

## 2.4 핵심 설계 원칙

**원칙 1 — 점진적 교체**: 한 번에 한 모듈씩 교체. 어댑터와 기존 코드 공존 시기를 거쳐 안전하게 전환.

**원칙 2 — 인터페이스 안정성**: 어댑터 API는 한 번 결정하면 쉽게 바꾸지 않음. 내부 구현은 자유롭게.

**원칙 3 — 캐시 우선**: 온톨로지 로드 비용 큼 → 싱글톤. 프로세스당 1개 인스턴스.

**원칙 4 — 폴백 가능**: 온톨로지 파일 손상돼도 서비스 지속. 경고 로그 후 기본값 동작.

**원칙 5 — 환각 방지를 구조로**: LLM 답변 시 어댑터가 정의 + 데이터 가용성 함께 반환.

---

# 챕터 3: 모듈 명세

## 3.1 모듈 개요

**파일**: `raas/raas_ontology_adapter.py` (RAAS 저장소 루트 또는 코드와 같은 디렉토리)

**책임**:
- 온톨로지 `.ttl` 파일 로드 및 메모리 캐시
- 의미 조회 API 제공 (필드/프로그램/메트릭/룰)
- 비즈니스 룰 평가
- LLM용 컨텍스트 생성

**의존성**: 표준 라이브러리만 사용 (PoC 검증). 운영 안정 후 `rdflib` 도입 검토.

## 3.2 클래스 구조

3개 클래스로 구성:
- `Ontology`: 그래프 조회 저수준 API
- `OntologyAdapter`: RAAS 코드용 고수준 API
- `_AdapterCache`: 싱글톤 인스턴스 관리

## 3.3 OntologyAdapter 메서드 카탈로그

| 메서드 | 우선순위 | 호출 빈도 | 대체 대상 |
|---|---|---|---|
| `get_field_info()` | 🔴 High | 매우 높음 | FIELD_ALIASES, field_map, getMetricField |
| `find_program_by_keyword()` | 🔴 High | 높음 | KEYWORD_TO_CODE |
| `get_program_meta()` | 🔴 High | 매우 높음 | PGM_NAMES, PGM_MAP |
| `evaluate_platform_alerts()` | 🔴 High | 중간 | build_s7() |
| `find_at_risk_programs()` | 🟡 Medium | 중간 | risks 룰 |
| `classify_program()` | 🟡 Medium | 중간 | (신규 능력) |
| `get_concept_definition()` | 🟡 Medium | 낮음 | (신규 능력) |
| `get_schema_dump()` | 🟡 Medium | 낮음 (캐시) | getMetricField + 모달 |
| `get_day_type()` | 🟢 Low | 신규 | (신규 능력) |
| 기타 6개 | 🟢 Low | 가끔 | — |

🔴 High 4개 + 🟡 Medium 4개 = 8개부터 구현하면 RAAS 코드 변경의 80%를 커버.

상세 시그니처와 입출력 예시는 **부록 A의 코드**를 참조.

---

# 챕터 4: API 명세

## 4.1 기존 API 변경 사항

| 엔드포인트 | 응답 형식 | 내부 구현 |
|---|---|---|
| `GET /api/briefing` | 변경 없음 | 어댑터 호출로 교체 |
| `POST /api/query` | 변경 없음 | 어댑터 호출로 교체 |
| 그 외 | 변경 없음 | 동일 |

## 4.2 신규 엔드포인트

### 4.2.1 `GET /api/schema`

**목적**: 클라이언트가 메타지식을 동적 로드. `web.html`의 하드코딩 제거.

**응답 (200)**:
```json
{
  "version": "0.5",
  "metrics": {
    "DAU": {
      "label": "활성 사용자 수",
      "unit": "명",
      "is_percent": false,
      "variants": {
        "Day":   {"field": "dau", "label": "일간"},
        "Week":  {"field": "wau", "label": "주간"},
        "Month": {"field": "mau", "label": "월간"}
      }
    }
  },
  "channels": {"F00": {...}},
  "programs": {"F09": {...}},
  "dayparts": {"MorningDrive": {...}},
  "concepts": {"AtRiskProgram": {...}},
  "field_to_meta": {"dau": {...}}
}
```

**캐싱**: ETag (온톨로지 파일 mtime 기반), 클라이언트 5분 캐시.

**클라이언트 활용**:
```javascript
const schema = await fetch('/api/schema').then(r => r.json());
function getMetricField(metric_id, granularity) {
    return schema.metrics[metric_id]?.variants[granularity]?.field;
}
```

### 4.2.2 `GET /api/concept/{concept_name}`

**목적**: 개념 정의 조회. (?) 모달 동적화 + LLM 컨텍스트 주입.

**응답 (200)**:
```json
{
  "id": "raas:AtRiskProgram",
  "label": "이탈 위험 프로그램",
  "definition": "DAU 1,000 이상이면서 ...",
  "condition": "dau >= 1000 AND ...",
  "alert_level": "yellow",
  "recommended_action": "편성 검토, 콘텐츠 분석..."
}
```

### 4.2.3 `GET /api/concept/search?q={keyword}`

**목적**: 자동완성, 검색 제안.

**응답 (200)**:
```json
{
  "query": "컬투",
  "matches": [
    {"id": "raas:F09", "code": "F09", "type": "Program",
     "label": "두시탈출 컬투쇼", "matched_label": "컬투", "score": 1.0}
  ]
}
```

## 4.3 응답 본문 명명 규칙

| 규칙 | 사용 |
|---|---|
| `snake_case` | JSON 키 |
| `id` 필드 | 온톨로지 IRI (raas:F09) |
| `code` 필드 | RAAS 시스템 코드 (F09) |
| `label` 필드 | 한글 정식 명칭 |
| `alt_labels` 필드 | 별칭 배열 |

## 4.4 에러 응답

```json
{
  "error": {
    "code": "ONTOLOGY_LOAD_FAILED",
    "message": "온톨로지 파일을 읽을 수 없습니다",
    "fallback_available": true
  }
}
```

`fallback_available: true`이면 어댑터가 기본값으로 동작 가능.

---

# 챕터 5: 코드 변경 매트릭스

## 5.1 briefing_engine.py 변경

### 5.1.1 FIELD_ALIASES 제거

```python
# 변경 전: 87줄의 dict + _alias_row 함수
FIELD_ALIASES = {
    'dau_today':            'dau',
    # ... 87개
}

# 변경 후
from raas_ontology_adapter import get_adapter

def _alias_row(row):
    adapter = get_adapter()
    aliases = adapter.get_legacy_field_aliases()
    return {**row, **{alias: row.get(new) for new, alias in aliases.items()}}
```

### 5.1.2 PGM_NAMES 제거

```python
# 변경 전: 33개 dict
# 변경 후
def _pgm_dict(code, ...):
    adapter = get_adapter()
    meta = adapter.get_program_meta(code)
    name = meta["label"] if meta else code
    ...
```

### 5.1.3 build_s7() 제거 (가장 큰 효과)

```python
def build_s7(s1, s2, s3, s4, s5):
    adapter = get_adapter()
    kpi = {
        "dau_chg": s1.get("dau_wow"),
        "deep_rate_diff": s3.get("deep_rate_diff"),
        "new_chg": s2.get("new_wow"),
        "churn_rate_diff": s2.get("churn_diff"),
        "react_rate": s2.get("react_rate"),
        "habit_rate": s4.get("habit_rate"),
    }
    alerts = adapter.evaluate_platform_alerts(kpi)
    risk_alerts = adapter.alerts_for_at_risk_programs(s5.get("risk_list", []))
    alerts.extend(risk_alerts)
    if not alerts:
        alerts.append({"level": "green", "msg": "전 지표 정상 범위 🟢"})
    return {"alerts": alerts}
```

### 5.1.4 risk_list 산출

```python
def build_s5(timeline, kpi):
    ...
    adapter = get_adapter()
    all_kpi = {code: row for code, row in latest_snapshot.items()}
    risk_list = adapter.find_at_risk_programs(all_kpi)
    return {..., "risk_list": risk_list}
```

## 5.2 query_engine.py 변경

### 5.2.1 KEYWORD_TO_CODE 제거

```python
# 변경 후
def find_code(keyword):
    adapter = get_adapter()
    matches = adapter.find_program_by_keyword(keyword)
    return matches[0]["code"] if matches else None
```

### 5.2.2 PGM_MAP 제거

```python
def get_pgm_map():
    adapter = get_adapter()
    result = {}
    for prog in adapter.get_all_programs():
        result[prog["code"]] = (
            prog["label"], 
            prog["channel"]["label"], 
            prog["time_slot"]["start"].replace(":", "")
        )
    return result
```

### 5.2.3 risks 룰 제거

```python
# 변경 후
adapter = get_adapter()
risks = adapter.find_at_risk_programs(all_kpi)
```

## 5.3 raas_server.py 변경

### 5.3.1 신규 엔드포인트 추가

```python
def do_GET(self):
    if self.path == "/api/schema":
        return self._handle_schema()
    if self.path.startswith("/api/concept/search"):
        return self._handle_concept_search()
    if self.path.startswith("/api/concept/"):
        return self._handle_concept_detail()
    # 기존 라우팅 유지
    ...

def _handle_schema(self):
    from raas_ontology_adapter import get_adapter
    try:
        adapter = get_adapter()
        schema = adapter.get_schema_dump()
        self._json_response(200, schema)
    except Exception as e:
        self._json_response(503, {"error": {...}})
```

### 5.3.2 LLM 호출 시 컨텍스트 주입

```python
def _handle_query(self):
    question = ...
    adapter = get_adapter()
    related = adapter.get_llm_context_for_query(question)
    enhanced_system = SYSTEM_PROMPT + "\n\n" + related
    answer = call_claude(enhanced_system, user_message_with_data)
```

## 5.4 web.html 변경

### 5.4.1 메타지식 함수 → API 활용

```javascript
let SCHEMA = null;

async function loadSchema() {
    if (SCHEMA) return SCHEMA;
    const res = await fetch('/api/schema');
    SCHEMA = await res.json();
    return SCHEMA;
}

function getMetricField(metric_alias, granularity) {
    return SCHEMA?.metrics?.[metric_alias]?.variants?.[granularity]?.field;
}

function isPercentMetric(metric_alias) {
    return SCHEMA?.metrics?.[metric_alias]?.measure_type === "Percentage";
}

window.addEventListener('DOMContentLoaded', async () => {
    await loadSchema();
});
```

### 5.4.2 모달 동적화

```javascript
async function showConceptModal(conceptName) {
    const res = await fetch(`/api/concept/${conceptName}`);
    const info = await res.json();
    document.getElementById('modal-title').textContent = info.label;
    document.getElementById('modal-definition').textContent = info.definition;
}
```

## 5.5 변경 파일 요약

| 파일 | 제거 라인 | 추가 라인 | 순감 |
|---|---|---|---|
| `briefing_engine.py` | ~150 | ~30 | -120 |
| `query_engine.py` | ~70 | ~20 | -50 |
| `raas_server.py` | 0 | ~80 | +80 |
| `web.html` | ~50 | ~30 | -20 |
| `raas_ontology_adapter.py` (신규) | 0 | ~600 | +600 |
| **합계** | **~270** | **~760** | **+490** |

순증가지만, 도메인 지식 분산이 어댑터로 통합되어 변경 시 1곳만 손대면 됨.

---

# 챕터 6: 마이그레이션 전략

## 6.1 점진적 교체 5단계

### Step 1 — 어댑터 모듈 신설 (위험 없음)

**작업**: `raas_ontology_adapter.py` 단독 생성. 기존 코드 미변경.

**검증**: 부록 B 회귀 테스트 단독 통과.

**롤백**: 파일 삭제.

### Step 2 — `briefing_engine.py`의 PGM_NAMES 교체 (낮은 위험)

**작업**: `_pgm_dict()` 함수만 어댑터 호출로 교체. PGM_NAMES 변수는 일단 유지.

**검증**: 브리핑 응답이 변경 전과 동일한지 비교.

**롤백**: 함수 본문 되돌림.

### Step 3 — FIELD_ALIASES 교체 (중간 위험)

**작업**: `_alias_row()`만 어댑터 호출로 교체. FIELD_ALIASES 사전 일단 유지.

**검증**: 100개 row를 변경 전후로 alias 비교.

**롤백**: `_alias_row()` 되돌림.

### Step 4 — `query_engine.py` 전체 교체 (중간 위험)

**작업**: KEYWORD_TO_CODE, SCOPE_MAP, PGM_MAP, field_map, risks 룰 모두 어댑터로 교체.

**검증**: `/api/query` 응답이 변경 전과 동일한지 50개 질의로 비교.

**롤백**: 변경 함수들 되돌림.

### Step 5 — 신규 API + web.html (낮은 위험)

**작업**: `/api/schema`, `/api/concept` 추가. `web.html` 점진 수정.

**검증**: 신규 API + 기존 화면 검증.

**롤백**: 엔드포인트 비활성화, web.html git revert.

## 6.2 단계별 위험과 완화책

| 단계 | 주요 위험 | 완화책 |
|---|---|---|
| Step 1 | 없음 | — |
| Step 2 | 프로그램명 누락 | 어댑터 fallback (code 그대로) |
| Step 3 | KPI alias 누락으로 화면 깨짐 | 변경 후 alias 키 비교 검증 |
| Step 4 | 자연어 질의 매칭 실패 | 50개 질의 회귀 테스트 |
| Step 5 | 화면 의존성 끊김 | SCHEMA 로드 실패 시 하드코딩 fallback |

## 6.3 공존 기간

각 단계마다 어댑터와 기존 코드가 공존. 무사 동작 + 1주 운영 후 기존 코드 제거.

```python
# 예: Step 2 진행 중
def _pgm_dict(code, ...):
    try:
        meta = get_adapter().get_program_meta(code)
        if meta:
            name = meta["label"]
        else:
            name = PGM_NAMES.get(code, code)  # fallback
    except Exception:
        name = PGM_NAMES.get(code, code)
    ...
```

## 6.4 Git 커밋 전략

각 Step을 별도 PR로:
```
phase5-step1-adapter-module
phase5-step2-pgm-names-migration
phase5-step3-field-aliases-migration
phase5-step4-query-engine-migration
phase5-step5-schema-api-and-web
```

각 PR마다 회귀 테스트 결과 첨부, 변경 라인 ~200 이하 유지.

## 6.5 환경별 진행 순서

```
[로컬 개발] → [스테이징] → [운영]
   1주          1주          
```

**스테이징**: Step 1~5 순차 적용, 각 Step 후 24시간 모니터링.

**운영**: 스테이징 검증 통과 후. **금요일 배포 금지**.

---

# 챕터 7: 검증 계획

## 7.1 회귀 테스트 — 12개

부록 B의 `phase5_regression_test.py`로 자동화.

### KPI 변환 회귀 (RT-01~04)
- RT-01: dau alias → DAU metric 매핑
- RT-02: wau alias → DAU + Week granularity
- RT-03: 핵심 변형 매핑 (8개 필드)
- RT-04: 미존재 필드 → None

### 프로그램 매칭 회귀 (RT-05~08)
- RT-05: 정식 명칭 매칭
- RT-06: 별칭 매칭
- RT-07: 영문명 매칭
- RT-08: 미존재 키워드 → 빈 리스트

### 비즈니스 룰 회귀 (RT-09~12)
- RT-09: 알림 룰 4개 동시 발동
- RT-10: AtRiskProgram 분류
- RT-11: 임계값 경계 (DAU 999/1000/1001)
- RT-12: 빈 KPI 처리

## 7.2 신규 능력 테스트 — 6개

- NT-01: get_schema_dump 응답 구조
- NT-02: get_concept_definition (AtRiskProgram)
- NT-03: 미존재 개념 → None
- NT-04: find_program_by_keyword score 1.0
- NT-05: DayType 판정 (5/1 공휴일, 5/2 주말)
- NT-06: LLM 컨텍스트 자동 주입

## 7.3 성능 테스트

| # | 항목 | 기대 | 실측 |
|---|---|---|---|
| PT-01 | 콜드 스타트 | <500ms | **10.7ms** |
| PT-02 | get_field_info | <1ms | **0.017ms** |
| PT-03 | get_schema_dump | <50ms | **1.9ms** |

성능은 매우 여유로움. rdflib 도입 없이도 운영 가능.

## 7.4 호환성 테스트

| # | 항목 | 기대 |
|---|---|---|
| CT-01 | 기존 /api/briefing | 변경 전후 JSON diff = 0 |
| CT-02 | 기존 /api/query (50개) | 49/50 이상 동일 |
| CT-03 | web.html 화면 | 시각적 차이 없음 |

## 7.5 모니터링 지표 (운영 후 1주)

| 지표 | 임계값 | 의미 |
|---|---|---|
| `/api/schema` 응답 시간 | <100ms | 캐시 정상 |
| 어댑터 fallback 발동 | 0건 | 어댑터 정상 |
| LLM 환각 신고 | 감소 추세 | 컨텍스트 효과 |
| `/api/query` p95 | 변경 전 동일 | 회귀 없음 |

## 7.6 성공 판정

다음 모두 만족 시 Phase 5 성공:
- ✅ 회귀 테스트 12/12 통과
- ✅ 신규 능력 테스트 6/6 통과
- ✅ 성능 테스트 4/4 통과
- ✅ 호환성 테스트 3/3 통과 (CT-02는 49/50 이상)
- ✅ 운영 1주 모니터링 시 사고 0건

---

# 부록 A: 어댑터 참조 구현

`raas_ontology_adapter.py` 파일 참조 (별도 첨부).

검증 결과:
- 1,522 트리플 자동 로드
- 자체 테스트 10가지 모두 동작
- 회귀 테스트 45개 모두 PASS
- 콜드 스타트 10.7ms, 캐시 히트 0.017ms

Claude Code에서 이 파일을 RAAS 저장소에 그대로 복사한 뒤 다음 변경:
1. `ONTOLOGY_DIR` 환경변수 또는 기본 경로를 운영 환경에 맞게 조정
2. `get_legacy_field_aliases()` 메서드 채우기 (Step 3 진행 시)
3. 필요하다면 logger를 RAAS의 표준 로거로 교체

---

# 부록 B: 회귀 테스트

`phase5_regression_test.py` 파일 참조 (별도 첨부).

**Phase 5-B 진행 시 활용 방법**:
- Step 1 완료 후: 모든 테스트 PASS 확인 후 Step 2 진행
- 각 Step마다 회귀 테스트 추가 (RAAS 실제 코드 호환성)
- CI에서 매 PR마다 자동 실행

현재 PoC 환경에서 측정한 결과: **45 PASS / 0 FAIL**

---

# Phase 5-B 진행 체크리스트

Claude Code 세션에서 이 문서를 따라 작업할 때 단계별로 체크:

## Step 1: 어댑터 모듈 신설
- [ ] `raas_ontology_adapter.py`를 RAAS 저장소에 복사
- [ ] `phase5_regression_test.py`를 RAAS 저장소에 복사
- [ ] `RAAS_ONTOLOGY_DIR` 환경변수 설정
- [ ] `python3 phase5_regression_test.py` 실행 → 45 PASS 확인
- [ ] Git 커밋: `feat: ontology adapter module added`

## Step 2: PGM_NAMES 교체
- [ ] `briefing_engine.py`의 `_pgm_dict()` 수정
- [ ] PGM_NAMES 변수는 일단 유지 (fallback용)
- [ ] 회귀: 동일 입력 → 동일 출력 확인
- [ ] Git 커밋: `refactor: replace PGM_NAMES lookup with adapter`

## Step 3: FIELD_ALIASES 교체
- [ ] 어댑터의 `get_legacy_field_aliases()` 구현 완성
- [ ] `_alias_row()` 어댑터 호출로 변경
- [ ] 회귀: 100개 row alias 키 100% 일치
- [ ] Git 커밋: `refactor: replace FIELD_ALIASES with adapter`

## Step 4: query_engine.py 전체 교체
- [ ] KEYWORD_TO_CODE 제거
- [ ] PGM_MAP 제거
- [ ] field_map 제거
- [ ] risks 룰 제거
- [ ] `extract_data()` 함수 어댑터 호출로 재작성
- [ ] 회귀: 50개 자연어 질의 응답 비교
- [ ] Git 커밋: `refactor: migrate query_engine to adapter`

## Step 5: 신규 API + web.html
- [ ] `raas_server.py`에 `/api/schema`, `/api/concept` 추가
- [ ] `web.html`에 `loadSchema()` 추가
- [ ] `getMetricField`, `isPercentMetric` 어댑터 활용으로 변경
- [ ] 모달 (?) 동적 로드 구현
- [ ] LLM 호출 시 컨텍스트 자동 주입
- [ ] Git 커밋: `feat: add /api/schema and migrate web.html`

## Step 6: 정리
- [ ] 공존 기간(1주) 후 기존 fallback 코드 제거
- [ ] PGM_NAMES, FIELD_ALIASES 등 변수 완전 삭제
- [ ] Git 커밋: `chore: remove legacy fallback code`

---

# 추가 참고

## 박사과정 연구 활용

이 통합 작업의 결과를 박사논문에 활용 시 다음 주제로 연결 가능:

- **온톨로지 기반 데이터 분석 시스템의 점진적 통합 방법론**
- **LLM 환각 방지를 위한 구조적 접근 (프롬프트 vs 온톨로지)**
- **산업용 시스템에서 도메인 지식의 단일 출처 확보 사례**

## 광고 시장 연구 활용

`/api/schema` 응답에 daypart 정보가 포함되므로:

- 광고 영업 시 자동으로 daypart × 청취자 매트릭스 생성 가능
- "프리미엄 시간대 청취자 풀" 같은 광고 제안서 자동 생성
- 공휴일 효과까지 반영한 광고 단가 산정 모델 구축 가능

## 향후 확장 (Phase 5 이후)

- **Phase 2-C-3**: Person 파싱 (출연자 분석)
- **Phase 2-C-4**: Episode 인스턴스화 (회차 단위)
- **Phase 4 확장**: 추가 비즈니스 룰 (예: 광고 효과 분석 룰)
- **rdflib 도입**: 운영 안정 후 표준 RDF 도구로 전환
- **공공데이터포털 API 연동**: 공휴일 자동 동기화

## 라이선스 / 출처

내부 PoC 단계. 외부 배포 전 보안 검토 필요.
