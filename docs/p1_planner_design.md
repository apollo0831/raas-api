# P-1 — LLM 질의 플래너 설계

## 목표
질의 해석(무엇을·어떤 기간·해상도·차원·형식으로 뽑을지)을 **흩어진 키워드 규칙 19종**에서
**LLM 플래너 하나**로 옮긴다. 플래너가 질의+온톨로지 카탈로그를 읽고 **검증된 구조화 요청
(PlanRequest)** 을 내면, 결정적 실행기가 통합 접근자(일간 `raas_series`·실시간 `raas_rt_series`)로
데이터를 뽑고 포매터가 렌더한다. 새 질의 유형 = 새 코드가 아니라 **카탈로그(온톨로지·레지스트리)만
커지면 플래너가 자동으로 더 똑똑해짐** — "코드 없이 똑똑해짐"의 마지막 조각(질의 해석 축).

## 무엇을 대체하나 (현 상태)
`_detect_realtime/_detect_compare/_detect_ranking/_detect_meta/_wants_engagement/_wants_program_demo/
_wants_correlate/_wants_editorial/_target_metric/_parse_abs_date/_rt_temporal/_rt_resolve_target/
detect_extract/…`(19종) + `select_providers`(Haiku). 각 새 케이스마다 규칙이 붙던 곳.

## 원칙 (기존 불변 + 확장)
- **LLM은 '무엇을'만 결정** — 스키마로 제약된 구조 요청. 값(숫자)은 **절대 만들지 않음**.
- **값은 코드가 결정적으로** — 실행기가 접근자·metrics_engine·analytics로 계산.
- **답변 LLM은 서술만** — 지금 grounding과 동일.
- 플래너는 **레지스트리·온톨로지 카탈로그**를 메뉴로 받는다 → 카탈로그가 곧 능력의 상한.
  존재하지 않는 지표/차원은 스키마 검증에서 걸러 **환각 fetch 차단**.

## PlanRequest 스키마 (실제 시스템에 grounding)
```
PlanRequest {
  intent:  series | snapshot | ranking | compare | correlate | extract | editorial | meta | digest
  domain:  daily | realtime                      # 어느 접근자
  metric:  <레지스트리 등록 키>                    # dau·engage_rate·concurrent·sex_ratio·SMS…
  entity:  { kind: program|channel|all, ref }     # '컬투쇼'→F09 / '파워FM'→F00 / 전체 T00
  time:    { when: today|date|relative, date?, lookback_days?, window?: program|HH:MM~HH:MM }
  period:  1D|1W|1M   (daily)   |  resolution: 1|10  (realtime)
  dims:    { device?, sex?, age?, depth? }        # 접근자 dims 슬롯
  format:  answer | chart | extract
  compare_with: [entity…]                          # intent=compare
  factors:      [metric…] | auto                   # intent=correlate
  confidence:   0..1                               # 낮으면 폴백/역질문
}
```
플래너 출력은 이 JSON. **검증기**가 (a) metric이 레지스트리에 있는지 (b) entity 해석(별칭=온톨로지
altLabel) (c) dims가 그 metric에 유효한지(예: sex→F00/L00만) (d) 범위 clamp 를 확인·교정.

## 3부 구조
### ① 플래너 (LLM, Haiku)
프롬프트 = 질의 + **컴팩트 카탈로그**(자동 생성):
- 지표 목록·정의(온톨로지 `get_metric_definitions_block` — 이미 동적)
- 실시간 지표·차원(RT_METRICS) + 제약(sex/age=인증 F00/L00, 보라 별도축)
- 엔티티 디렉토리(프로그램·채널 코드+별칭, 온톨로지 altLabel)
- intent·format 열거 + 몇 개 예시(few-shot)
출력 = PlanRequest JSON. 스키마 강제(도구호출/JSON 모드), 실패 시 1회 재시도.

### ② 검증·해석기 (결정적)
- metric/intent/domain 화이트리스트 대조 → 없으면 폴백.
- entity_ref → 코드(`extract_program`·`_CHANNEL_CODE`·온톨로지 altLabel 재사용).
- time 파싱은 규칙 재사용(`_parse_abs_date`) — 플래너가 자연어 시간을 넘기면 결정적 파서로 확정.
- dims 유효성(metric의 chans/dims), 범위 clamp(보관 범위=`rt_available`).
- 낮은 confidence·검증 실패 → **키워드 폴백 경로**(현 `assemble`) 또는 명확화 역질문.

### ③ 실행기 (결정적, 접근자 재사용)
PlanRequest → context. intent별 매핑(대부분 이미 있는 provider/접근자 재사용):
| intent | 실행 |
|---|---|
| series/snapshot | daily: `raas_series` · realtime: `rt_series/rt_table` |
| ranking | `raas_series.ranking`(+demo) / `_kpi_rows` |
| compare | 엔티티 N개 나란히(현 compare scope) |
| correlate | `raas_analytics` + 관계 온톨로지(현 metric_correlate) |
| extract | `build_extract`/`_build_extract_realtime` 표 |
| editorial | program_history/channel_history |
| meta/digest | metric_catalog / anomalies |
→ 실행기는 **흩어진 `_assemble_*` scope 분기를 plan 기반 단일 디스패치로** 수렴. 값은 접근자가.

## 온톨로지의 역할 (핵심)
플래너 카탈로그·검증기가 **온톨로지+레지스트리에서 자동 구성**된다. 지표/별칭/차원/제약을 TTL·
레지스트리에 추가하면 플래너가 **코드 없이** 그걸 계획에 쓸 수 있게 됨. 커버리지 맵·지표 카탈로그
동적화(이미 완료)가 여기서 두 번째로 값을 낸다.

## 신뢰성 가드레일 (정직)
- **환각 차단**: plan은 등록된 metric/entity/dim만 — 검증 실패 시 실행 안 함(폴백).
- **숫자 무결**: 플래너는 값 안 만듦. 값은 접근자·metrics_engine. 답변 LLM엔 계산된 근거만.
- **2콜 비용**: 플래너(Haiku, 저렴)+답변. 명백한 케이스는 **결정적 fast-path**로 플래너 생략 가능
  (하이브리드) — 조합형·모호형 long tail만 플래너.
- **점진 도입**: 처음엔 **shadow 모드**(플래너 plan을 생성만·기록, 실제는 기존 경로) → 평가 →
  scope별 전환 → 안정되면 키워드 감지기 은퇴. 한 번에 안 바꿈.
- **역질문**: confidence 낮거나 데이터 한계(예: '여성 동시자 수'=없음, 비율만)면 명확화/한계 안내
  (지난 교훈 — 조용한 폴백·날조 금지).

## 단계
- **P-1a**: PlanRequest 스키마 + 플래너(Haiku, 카탈로그 프롬프트) + 검증기. **shadow 평가 하네스**
  (질의 배터리 → plan 정확도, LLM 호출은 플래너만). 프로덕션 미연결.
- **P-1b**: 실행기(plan→context) + **realtime scope부터** 플래너 경로로(가장 조합적·최신). 기존
  키워드 경로와 A/B(출력·정확도 비교). 회귀 고정.
- **P-1c**: intent 확대(series·ranking·compare·correlate·extract). 키워드 감지기는 fast-path/폴백으로 강등.
- **P-1d**: 플래너가 안정된 scope의 키워드 라우팅 은퇴. `select_providers`는 실행기 내부 도구로 흡수.

## 경계·리스크
- **모든 걸 LLM이 풀 필요는 없음** — 단순·명시(예: '어제 컬투쇼 DAU')는 fast-path가 싸고 확실.
  플래너는 조합·모호·신규 유형에 값어치. 하이브리드가 현실적.
- **평가 없이는 위험** — P-1a의 shadow 평가 하네스가 전제. plan 정확도를 케이스로 고정(회귀).
- **레이턴시** — 플래너 Haiku ~수백ms. 스트리밍 첫 토큰 전 1콜 추가. fast-path로 상쇄.
- **부분 실패 우아하게** — plan 무효/저신뢰 → 기존 경로 폴백(사용자는 차이 못 느낌).
- 이 설계는 라우팅·해석을 바꾸는 것 — **값·답변 품질 경로는 불변**(접근자·온톨로지·답변 LLM 그대로).
