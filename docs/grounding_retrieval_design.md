# RAAS 검색·Grounding 레이어 설계 — LLM 우선 + 데이터/온톨로지 강화

> 상태: **설계(검토 대기)**. 승인 후 단계 구현.
> 목적: 고정 출력 템플릿(스토리라인 페이지)에 의존하지 않고, **어떤 질문이 오든 관련 데이터·온톨로지를
> LLM context에 골라 넣어** LLM 성능을 100% 활용. 품질 개선 레버 = 데이터·온톨로지(검색 대상)와 검색 로직.

---

## 0. 원칙 (왜 이 설계인가)

- LLM은 룩업/온톨로지에 **직접 접근하지 못한다.** 매 질의마다 엔진이 **무엇을 context에 넣을지** 정하고,
  LLM은 그 안에서만 추론한다. → **답변 품질의 상한 = 검색(retrieval)·맥락 조립(grounding)의 품질.**
- "다 넣으면 된다"는 불가: **토큰 한도 + 노이즈**(무관 데이터는 품질 저하). → **관련 있는 것을 골라 넣는 검색**이 핵심.
- 따라서 작업은 *질문 유형별 템플릿*이 아니라 **질문이 무엇을 필요로 하든 관련 근거를 끌어오는 일반 검색 레이어**.
- **온톨로지 = "무엇이 관련 있는가 / 어떻게 정의되는가"의 사전.** 검색이 무엇을 끌어올지 온톨로지가 안내.
  → 사용자가 온톨로지를 보강하면 검색·grounding이 좋아지고 **모든 답변이 좋아지는 선순환**.

---

## 1. 현재 구조 (raas_query_engine.py)

```
질문 → classify_intent(LLM) → intent{intent,scope,metric,metrics}
     → 의도별 데이터 빌더(_build_funnel/engagement/growth/alert/corner_schedule …)
     + _build_attribute_context(LLM이 필요한 CSV 컬럼 선택)
     + 온톨로지 지표정의(get_metric_definitions_block)
     → format_for_claude → context → LLM
```

이미 **검색·grounding의 원형**이 있다(의도 분류 + 데이터 조립 + 동적 컬럼 + 지표정의). 본 설계는 이를
**일반화·강화**한다 — 특히 분석·인과형 질문에 필요한 *분해 데이터*와 *온톨로지 규칙*을 포함하도록.

**한계(지금)**: 단순 조회는 양호하나, 분석·인과·다차원 질문은 필요한 데이터(흐름분해·코호트·편성·게스트·특일
·인접 등)가 context에 충분히 들어가지 않아 LLM이 *분석 대신 추측*하게 됨. 이 데이터 조립 로직은
스토리라인 computer에 이미 있으나 **출력 템플릿에 묶여** 일반 질의에서 재사용되지 않음.

---

## 2. 목표 아키텍처

```
질문
  │
  ├─① 엔티티/의도 해석   (classify_intent + 온톨로지 directory)
  │     → {프로그램, 채널, 기간, 지표(들), 분석깊이}
  │
  ├─② 검색 — Retrieval Providers (관련 데이터 슬라이스)
  │     관련도·예산 내에서 선택 호출:
  │       metric_timeseries · flow_decomp · cohort · stickiness
  │       scheduling(편성/게스트/코너/생방송) · calendar(특일)
  │       adjacent(인접 시간대) · revision(개편) · kpi_table · attribute_columns
  │
  ├─③ Grounding — 온톨로지 팩
  │     지표정의 + 적용 가능한 규칙·정의(cause 분해 프레임워크, calendar 판정,
  │     프로그램 유형/게스트 정책, 관련 관계) — 엔티티·지표에 매칭된 것만
  │
  ├─④ 맥락 조립 (토큰 예산 내 우선순위·중복제거·라벨링)
  │
  └─⑤ LLM 자유 생성 (grounded) → 답변 + (1단계) provenance: 어떤 provider·온톨로지 사용
```

**핵심 전환**: 스토리라인 computer의 *데이터 추출 로직*을 **Retrieval Provider로 재사용**(출력 포맷이 아니라
구조화 데이터를 반환). 온톨로지의 역할은 *출력 규칙* → **검색 안내 + 프롬프트 grounding**.

---

## 3. 컴포넌트

### 3.1 Retrieval Provider 레지스트리
각 provider: `provider(entities, question) -> {data: <구조화>, relevance: 0~1, tokens_est}`.
- 스토리라인의 기존 추출기를 래핑(데이터만 반환):
  `_compute_flow_decomposition` · `_compute_cohort` · `_compute_stickiness`
  · `_decompose_scheduling_impact` · `_detect_program_revision`
  · calendar `get_day_type` · 인접(`_compute_cp_anchor_adjacent` 데이터부) · KPI표.
- 신규 공통: `metric_timeseries`(기간 시계열), `attribute_columns`(현행 `_build_attribute_context`).

### 3.2 관련도·예산 선택 (Relevance & Budget)
- **무엇을 호출할지** = 의도 + 온톨로지가 안내.
  - 예: 인과/분석 의도 → 분해 provider군 포함. 단순 조회 → metric_timeseries만.
  - 온톨로지 cause 프레임워크가 "원인 분석에 필요한 축"을 나열 → 그 축의 provider 호출(하드코딩 아님).
- **토큰 예산** 내에서 relevance 높은 순으로 채움. 1차는 간단한 규칙, 이후 학습/튜닝.

### 3.3 온톨로지 Grounding 팩
- 지표정의(현행) + 엔티티에 매칭된 규칙·정의·정책(프로그램 유형, 게스트 정책, 특일 판정 근거, 분해 프레임워크).
- 사용자 지식 오버레이(로드맵 4단계)가 머지되면 자동으로 팩이 풍부해짐.

### 3.4 맥락 조립기
- provider 데이터 + 온톨로지 팩을 **구조화·라벨링**해 context로. 중복 제거, 우선순위, 토큰 컷.

### 3.5 Provenance (1단계와 연결)
- 어떤 provider가 발동했고 어떤 온톨로지를 썼는지 = 이미 만든 '참고 정보' 푸터에 표기 → 투명성·A/B의 근거.

---

## 4. 구현 순서 (점진적)

1. **추출기 → Provider 분리** *(낮은 위험, 선행)*
   - 스토리라인 분석 데이터 추출 함수들이 **포맷 텍스트가 아니라 구조화 데이터**를 반환하도록 정리(또는 래퍼).
   - 출력 포맷(스토리라인 페이지)은 그 데이터를 받아 그리는 얇은 층으로.
2. **Grounding 조립기 추가 (query_engine)** *(핵심)*
   - 의도 깊이에 따라 관련 provider 호출 + 온톨로지 팩을 context에 추가. 단순 조회는 현행 유지.
   - 먼저 **인과/분석 의도**에 분해 provider를 붙여 체감 개선부터.
3. **관련도·토큰 예산 컨트롤** — 의도→provider 매핑으로 시작, 점진 정교화.
4. **A/B 비교 하니스 (로드맵 2단계)** — 같은 질문을 *기존 context* vs *강화 context* 두 버전으로 생성·비교.
   → "데이터·온톨로지를 더 넣으니 좋아지나"를 측정·튜닝. provider별 기여도 평가.
5. **온톨로지 주도 관련도** — 온톨로지 관계로 "무엇을 끌어올지" 결정 → 사용자 온톨로지 기여가 검색을 자동 개선.

각 단계 독립 배포·검증 가능. 1·2단계만으로도 분석형 질문 품질이 오른다.

---

## 5. 측정 / 선순환

- A/B 채택률·피드백(👍/아쉬움)·토큰을 이미 구축한 이력 인프라로 측정.
- provider/온톨로지 항목별 **기여도** → 어떤 데이터·지식을 더 보강할지 우선순위.
- 사용자 지식 기여(로드맵 3·4단계) → 온톨로지 오버레이 → grounding 강화 → 품질↑ → 측정 → 재투자.

---

## 6. 결정 필요

- **1단계 분리 범위**: 추출기 전부를 한 번에 provider화 vs **인과/분석에 핵심인 것부터**(흐름분해·편성·특일·코호트)
  점진. → 추천: **핵심부터**.
- **관련도 1차 방식**: 의도→provider 정적 매핑(단순·예측가능) vs LLM이 provider 선택(유연·비용). → 추천: **정적 매핑부터**, A/B 후 정교화.
