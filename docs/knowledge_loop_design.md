# RAAS 지식 개선 루프 상세 설계 — '아쉬움' → 개선하기 → 재질의(A/B) → 반영

> 상태: **상세 설계(검토 대기)**. 승인 후 단계 구현.
> 한 줄: 사용자가 아쉬운 답변을 **데이터·온톨로지 보강**으로 직접 개선하고, **재질의로 효과를 확인**하며,
> 효과가 입증된 기여는 **검토 후 전체에 반영**한다. (로드맵 2·3·4단계를 하나의 사용자 흐름으로 통합)
> 연계: [grounding_retrieval_design.md] · [ab_harness_design.md] · 1단계 투명성(참고 정보 provenance)

---

## 1. 전체 흐름

```
질의 → 답변 → (아쉬움 평가)
   → 이력 보기에서 '아쉬움' 질의 + [개선하기]
   → 개선하기 화면
        ├─ [데이터]   사용 필드 표시 + ⟨내 데이터 업로드(즉시)⟩ / ⟨스플렁크 필드 추가 요청(요청형)⟩
        └─ [온톨로지] 참고 항목 표시 + ⟨수정/추가(즉시)⟩
   → [재질의]  = 원답변(A) vs 보강 후 답변(B)  나란히 + 사용자 평가 + LLM judge 보조
   → 개선 시도 1건 레코드(이력 보기에 표시, 상태=검토대기)
   → (관리자/데이터 직무) 검토 → 효과 입증분 전체 반영 + 상태 업데이트
```

**핵심 enabler**: **지식 오버레이 + grounding 읽기시 병합**. 기여를 라이브 온톨로지/데이터에 직접 쓰지 않고
오버레이에 격리, grounding이 읽을 때 병합. (안전·롤백·승인 전 격리)

---

## 2. 기여의 두 종류 (보강①)

| | 즉시 반영형 | 요청형 |
|---|---|---|
| 예 | 온톨로지 수정·추가, **본인 데이터 업로드** | **스플렁크 데이터 항목 추가** |
| 재질의 반영 | **즉시**(내 candidate 오버레이) | **불가**(데이터가 아직 없음) — 요청만 |
| 경로 | candidate 오버레이 저장 | 관리자/데이터 직무 큐 → 상태 추적(`요청됨→처리중→완료/반려`) |

개선하기 화면은 두 종류를 **시각적으로 구분**(지금 재질의에 반영되는 것 vs 요청만 되는 것).

---

## 3. 격리 범위 (보강②)

- 기여는 **본인 candidate 오버레이**에만 저장 → **내 재질의에만** 반영(blast radius 최소).
- **승인되면 공유(approved) 오버레이**로 머지 → 그때부터 **모두에게** 적용.
- 잘못된 기여로 전체 품질이 흔들리지 않음.

```
오버레이 적용 우선순위(grounding 읽을 때):
  일반 질의:  approved(공유) + 본인 upload
  재질의:     approved(공유) + 본인 candidate(미승인) + 본인 upload
  → 같은 대상 충돌 시 candidate > approved > TTL(원본)
```

---

## 4. 데이터 모델

### 4.1 지식 오버레이 — `knowledge_items`
온톨로지를 TTL 그래프로 직접 수정하지 않는다. **구조화 지식 항목**을 엔티티로 키잉해 grounding context에 주입.
```sql
CREATE TABLE knowledge_items (
  id INTEGER PK, created_at TIMESTAMP,
  scope        TEXT,   -- candidate | approved
  contributor_id TEXT,
  type         TEXT,   -- metric_definition | field_meaning | program_note
                       --  | guest_policy | corner_note | decomposition_hint | fact
  target_kind  TEXT,   -- metric | field | program | channel | global
  target_id    TEXT,   -- 예: deep_rate / F09 / L00 / null
  content      TEXT,   -- 정의·메모·규칙 텍스트(또는 JSON)
  op           TEXT,   -- add | edit
  status       TEXT,   -- draft | submitted | approved | rejected
  improvement_id INTEGER,   -- 어느 개선 시도에서 나왔나
  reviewed_by  TEXT, reviewed_at TIMESTAMP
);
```
> 그래프 트리플 변형 대신 **'엔티티별 도메인 지식 텍스트'** 로 단순화 — grounding이 대상 엔티티의 항목을
> 끌어와 context에 "추가 도메인 지식/정의 보정"으로 넣는다. (TTL 머지보다 안전·간단, v1 적합)

### 4.2 데이터 요청 — `data_requests` (요청형)
```sql
CREATE TABLE data_requests (
  id INTEGER PK, created_at TIMESTAMP, contributor_id TEXT,
  kind TEXT,              -- splunk_field
  target_id TEXT,         -- 관련 프로그램/채널(있으면)
  field_name TEXT, description TEXT, splunk_spl TEXT,
  status TEXT,            -- 요청됨 | 처리중 | 완료 | 반려
  processed_by TEXT, processed_at TIMESTAMP,
  improvement_id INTEGER
);
```

### 4.3 본인 업로드 데이터 — `uploaded_data` (즉시형, 보강⑤·후속)
```sql
CREATE TABLE uploaded_data (
  id INTEGER PK, created_at TIMESTAMP, contributor_id TEXT,
  target_kind TEXT, target_id TEXT,   -- program/channel
  name TEXT, schema_json TEXT, rows_json TEXT,   -- 소규모는 인라인, 대용량은 파일ref
  status TEXT                         -- candidate | approved
);
```

### 4.4 개선 시도 — `improvements` (보강④)
```sql
CREATE TABLE improvements (
  id INTEGER PK, created_at TIMESTAMP, contributor_id TEXT,
  source_query_id INTEGER,            -- 원 query_history.id
  question TEXT,
  answer_original TEXT,
  answer_improved TEXT,
  contributions_json TEXT,            -- knowledge_items/data_requests/uploads id 묶음
  user_verdict TEXT,                  -- improved | same | worse
  judge_json TEXT,                    -- LLM judge 결과(보조)
  status TEXT,                        -- 검토대기 | 승인 | 반려
  reviewed_by TEXT, reviewed_at TIMESTAMP
);
```

---

## 5. 개선하기 화면

이력 보기에서 **'아쉬움' 질의**에 `[개선하기]` → 화면 진입. 상단에 원질의·원답변.
1단계 provenance 재사용 → **사용된 데이터 / 참고 온톨로지**를 그대로 재료로 표시.

### 5.1 [데이터] 섹션
- **사용된 필드**(provenance) 나열 + 각 의미(온톨로지 정의, 1a).
- 액션:
  - **내 데이터 업로드(즉시)** — CSV/표 붙여넣기 → `uploaded_data`(candidate) → 재질의에 반영.
  - **스플렁크 필드 추가 요청(요청형)** — 필드명·설명·(가능하면)SPL → `data_requests`(요청됨).

### 5.2 [온톨로지] 섹션
- **참고한 온톨로지 항목**(1b: 지표 정의·분해 프레임워크·특일·정책) 나열.
- 액션:
  - **수정** — 기존 정의/규칙을 보정 → `knowledge_items`(op=edit, candidate).
  - **추가** — 새 사실·메모·정책 → `knowledge_items`(op=add, candidate).
  - type/target은 항목에서 자동 추론(예: '깊은청취율 정의 수정' → type=metric_definition, target=deep_rate).

### 5.3 [재질의] 버튼
- 본인 candidate 오버레이 + 업로드를 병합한 grounding으로 **같은 질문 재생성** → §6.

---

## 6. 재질의 = A/B + 판정 (보강③)

```
A = answer_original(저장된 원답변)
B = grounding(질문, overlay=본인 candidate+업로드+approved) → LLM 재생성
→ 나란히 표시 + 사용자 평가(개선됨/비슷/더나쁨)
+ LLM judge 보조(정확/완결/근거충실/유용, 순서 무작위화) → 객관 점수
→ improvements 1건 저장(answer_improved, user_verdict, judge_json, status=검토대기)
```
- 사용자 판단이 트리거, **LLM judge는 보조 근거**(승인 시 참고).
- 만족스러우면 사용자가 추가 보강→재질의 반복 가능(여러 시도 → 같은 improvement에 누적 or 새 레코드).

---

## 7. 상태 · 이력 표시

- 개선 시도는 **이력 보기**에 `[개선] 질의 · 상태(검토대기/승인/반려) · 개선여부` 로 표시.
- 펼치면 원답변 ↔ 개선답변 비교 + 기여 내역(데이터/온톨로지) + judge 점수.
- 데이터 요청은 별도 상태(`요청됨→처리중→완료`)로 추적.

---

## 8. 거버넌스 (관리자/데이터 직무)

- **검토 큐**: `improvements`(user_verdict=improved) + `knowledge_items`(submitted) + `data_requests`.
- 검토자는 원답변↔개선답변 + 기여 + judge를 보고:
  - **승인** → `knowledge_items.scope=approved`(전체 적용) / `data_requests` 처리(완료) / `uploaded_data` approved.
  - **반려** → 상태 반려 + 사유.
- 승인 즉시 **공유 오버레이**로 들어가 모든 답변 grounding에 반영 → **선순환**.
- 권한: 온톨로지·규칙 = 데이터/총괄관리/관리자, 데이터 요청 처리 = 데이터 직무/관리자.

---

## 9. Grounding 통합 (읽기시 병합)

- `assemble(question, overlay_ctx=None)` 확장:
  - `overlay_ctx = {user_id, mode}` — mode=normal: approved+본인upload / mode=requery: +본인candidate.
  - 엔티티(프로그램·지표·채널) 매칭되는 `knowledge_items`·`uploaded_data`를 끌어와
    **'추가 도메인 지식'·'정의 보정'** 블록으로 context에 주입.
  - provenance(1단계)에 "적용된 사용자 기여" 표기 → 무엇이 반영됐는지 투명.

---

## 10. 구현 순서

1. **지식 오버레이 저장소 + grounding 읽기시 병합** — `knowledge_items`, `uploaded_data`, assemble 확장.
   *(핵심 enabler — 이게 있어야 재질의가 의미)*
2. **'개선하기' 진입** — 이력 보기 '아쉬움' 질의에 버튼 + 화면(원질의/원답변 + provenance).
3. **기여 입력** — [데이터](업로드/요청) · [온톨로지](수정/추가), 즉시형↔요청형 구분, candidate 저장.
4. **재질의 + 비교** — 오버레이 병합 재생성 → A/B 나란히 + 사용자 평가 + LLM judge 보조 → `improvements` 저장.
5. **상태·이력 표시** — 이력 보기 통합, `data_requests` 상태.
6. **거버넌스** — 검토 큐 + 승인/반려 → 공유 오버레이/데이터 처리 + 상태 업데이트.

후속: **본인 데이터 업로드(파일)**·요청형 데이터 처리 자동화는 3·6단계 내에서 점진(보강⑤).

---

## 11. 결정 필요

1. **온톨로지 기여 저장 형태**: 본 설계의 **구조화 지식 항목(엔티티별 텍스트, grounding 주입)** vs TTL 트리플 직접 편집.
   → 추천: **구조화 항목**(안전·단순, v1).
2. **개선 판정 가중**: 사용자 판단 단독 트리거 + LLM judge 보조(승인 참고) vs judge 점수 임계 자동 승인.
   → 추천: **사용자+judge 보조, 승인은 사람**(초기 신뢰 확보 후 자동화 검토).
3. **'본인 데이터 업로드' 시작 시점**: 1단계부터 포함 vs 온톨로지 기여·요청형 먼저 후 추가.
   → 추천: **온톨로지·요청형 먼저**, 업로드는 후속(범위·신뢰 처리 필요).
