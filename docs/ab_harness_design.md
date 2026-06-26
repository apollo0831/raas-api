# RAAS A/B 비교 하니스 설계 — "grounding을 더 넣으니 좋아지는가" 측정

> 상태: **설계(검토 대기)**. 승인 후 단계 구현.
> 목적: 같은 질문을 **서로 다른 grounding 구성(A/B)** 으로 답하게 하고 품질을 비교해,
> ① grounding(데이터·온톨로지)이 답을 개선하는가 ② 어떤 provider·온톨로지가 기여하는가 를 측정.
> → 어디에 투자(데이터 적재·온톨로지 보강)할지, 그리고 **사용자 지식 기여가 실제로 품질을 올리는지** 검증.

---

## 1. 핵심 개념

- **Variant(구성)** = LLM에 넣어줄 맥락(context)의 설정:
  ```
  {name, providers: ["auto"] | [provider…], include_ontology: bool, lookback?: int}
  ```
  - `auto` = 현행(LLM이 provider 선택). 특정 목록 = 강제 set(ablation용).
  - `include_ontology` = 온톨로지 grounding 팩 포함 여부.
  - `providers: []` + `include_ontology:false` = **grounding OFF**(순수 LLM, 질문만).
- **A/B run** = 한 질문을 두(또는 그 이상) variant로 각각 답변 생성 → 비교.
- **판정(Judge)** = 두 답변을 기준(rubric)으로 비교해 우열·점수·사유 산출(LLM judge + 사람 보정).

---

## 2. 파이프라인

```
질문 + [variant_A, variant_B]
  ├─ for each variant: assemble_context(질문, variant) → LLM 답변
  ├─ judge(질문, 답변_A, 답변_B, rubric)   # 순서 무작위화로 편향 제거
  │     → {winner: A|B|tie, scores{정확/완결/근거충실/유용}, reason}
  ├─ (선택) 사람 선택(human_pick)
  └─ 저장(ab_runs)
```

---

## 3. 비교 축 (어떤 A vs B)

| Variant | providers | ontology | 의미 |
|---|---|---|---|
| **V0 no_grounding** | (없음) | off | 순수 LLM(질문만) — "grounding이 가치 있나?" |
| **V1 minimal** | kpi + timeseries | off | 기본 데이터만 |
| **V2 full** | auto | on | 현행 운영(분해·편성·특일 + 온톨로지) |
| (V1b) data_only | auto | off | 데이터는 다 주되 온톨로지만 뺌 → 온톨로지 기여 분리 |

**권장 순서**: ① **V0 vs V2**(헤드라인: grounding 전체가 가치 있나) → ② **V1b vs V2**(온톨로지 기여) →
③ provider 개별 ablation(어떤 provider가 핵심인가).

---

## 4. 판정 (Judge)

- **LLM judge**(확장성) — rubric:
  - **정확성**: 제공 근거와 일치, 환각(근거 없는 수치) 없음
  - **완결성**: 질문에 충분히 답
  - **근거 충실성**: 준 데이터·온톨로지를 실제로 활용
  - **유용성**: 실무 인사이트
  - 출력: `{winner, per_criterion 점수, reason}`. **A/B 순서 무작위화**(위치 편향 제거), 동률 허용.
- **사람 보정**: 답변 옆 👍/아쉬움 또는 'A가 나음/B가 나음/비슷' 선택 → ground truth. LLM judge와 일치율도 추적.

---

## 5. 저장 (`ab_runs`)

```sql
CREATE TABLE ab_runs (
  id INTEGER PK, created_at TIMESTAMP, user_id TEXT,
  question TEXT,
  variant_a TEXT, variant_b TEXT,           -- JSON(구성)
  answer_a TEXT, answer_b TEXT,
  providers_a TEXT, providers_b TEXT,        -- 실제 발동 provider(JSON)
  judge_winner TEXT, judge_scores TEXT, judge_reason TEXT,   -- LLM 판정
  human_pick TEXT,                           -- A|B|tie|null
  tokens_a INTEGER, tokens_b INTEGER
);
```

---

## 6. 진입점 / UI

- **단발 A/B**(관리자): 질문 입력 → 두 variant 답변 **나란히** + judge 판정 + 사람 선택 버튼.
- **배치 A/B**: 큐레이션 **평가셋**(직무·의도별 대표 질문 10~30개) 전체를 두 variant로 돌려 **승률 리포트**.
- 결과는 이미 만든 이력/토큰 인프라와 연결(기여도·비용 측정).

---

## 7. 집계 / 의사결정

- **승률**: variant 쌍별 win/tie/lose. 평가셋 전체 → "V2가 V0 대비 N% 우세".
- **Ablation 기여도**: provider/온톨로지를 뺐을 때 승률 하락폭 = 그 요소의 가치 → **투자 우선순위**.
- **선순환 연결**(로드맵 3·4단계): 사용자 지식 오버레이를 켠 variant vs 끈 variant를 A/B → **"이 기여가 답을 개선했나"** 를 수치로 승인 근거.

---

## 8. 구현 순서

1. **`assemble()` 변형 지원** — `providers_override`, `include_ontology`, `no_grounding` 인자. (그라운딩 모듈 확장)
2. **`raas_ab.py`** — `run_ab(question, variant_a, variant_b)` → 두 답변 생성 + judge. judge 프롬프트·순서 무작위화.
3. **저장 테이블 + save** (`ab_runs`).
4. **관리자 단발 A/B UI** — 나란히 답변 + judge + 사람 선택.
5. **평가셋 + 배치 + 승률 리포트**.

각 단계 독립 검증 가능. 1·2·4만으로도 "grounding이 좋아지나"를 체감·측정 가능.

---

## 9. 결정 필요 (3가지)

1. **비교 축 우선순위**: V0(OFF) vs V2(full) 헤드라인 먼저 / V1b vs V2(온톨로지 기여) 먼저?
   → 추천: **헤드라인 먼저**("grounding이 가치 있나"부터 확정).
2. **판정 방식**: LLM judge 자동(+사람 spot-check) / 사람 직접 / 둘 다 동시?
   → 추천: **LLM judge + 사람 보정**(확장성 + ground truth).
3. **시작 형태**: 단발 A/B UI 먼저(빠른 체감) / 큐레이션 평가셋 배치 먼저(반복 측정)?
   → 추천: **단발 A/B UI 먼저** → 평가셋은 그 다음.
