# RAAS 의사결정 로그 (Decisions)

[docs/RAAS_METRICS.md](RAAS_METRICS.md)와 [docs/RAAS_EVENTS.md](RAAS_EVENTS.md)의 변경 사유는 모두 여기에 기록한다. 4주 PoC 동안 정의가 흔들리면 NSM도 흔들리므로 변경은 추적 가능해야 한다.

기록 형식:
- 날짜 / 결정 / 변경 전 → 변경 후 / 사유

---

## 2026-06-15 (Week 1, Day 2)

### D-001 — `ask_ai.source` enum에 `welcome_chip` 추가

**변경 대상:** [docs/RAAS_EVENTS.md](RAAS_EVENTS.md) §3.2 `ask_ai` 이벤트 속성 `source` enum

**변경 전:**
```
source ∈ { briefing_card, main_input, sidebar_chip }
```

**변경 후:**
```
source ∈ { briefing_card, main_input, sidebar_chip, welcome_chip }
```

**사유:**
- 현재 RAAS는 환영 화면(질의 시작 전 빈 채팅 영역)에 직무 기반 추천 칩(`_onSuggestChip`)을 렌더링한다.
- 이 칩은 좌측 사이드바의 "빠른 질의" 칩(`_onQuickQuery` — `sidebar_chip`)과 진입 표면이 명확히 다르다.
- 두 칩을 같은 source로 합치면 D3 신호 분석 시 "환영 화면 추천이 얼마나 잘 들어맞았는가 vs 사이드바 단축이 얼마나 쓰였는가"를 구분 못 한다.
- Week 1 Day 1 명세 잠금 직후의 변경이며, 새 표면 추가에 해당해 [RAAS_EVENTS.md §9 변경 정책](RAAS_EVENTS.md)의 허용 범위 안.

**구현 위치:**
- `raas_web.html` — `_onSuggestChip(btn)` → `submitQuery(q, 'welcome_chip')`
- `raas_web.html` — `_onQuickQuery(i)` → `submitQuery(QUICK_QUERIES[i], 'sidebar_chip')`
- `raas_web.html` — 메인 입력창(Enter/btnSend) → `submitQuery(value, 'main_input')`
- `briefing_card` source는 Week 3 브리핑 카드 신설 시 추가

**영향:**
- NSM 정의 변경 없음
- D3 결정 규칙 변경 없음. `ask_ai(source=briefing_card) / view_briefing` 비율은 그대로 핵심 신호
- `welcome_chip`은 부가 분석 신호 — "직무 추천이 챗 진입을 얼마나 유도했나"를 별도 측정

---

### D-003 — `ask_ai.source` enum에 `history_replay` 추가

**변경 대상:** [docs/RAAS_EVENTS.md](RAAS_EVENTS.md) §3.2 `ask_ai` 이벤트 속성 `source` enum

**변경 전:**
```
source ∈ { briefing_card, main_input, sidebar_chip, welcome_chip }
```

**변경 후:**
```
source ∈ { briefing_card, main_input, sidebar_chip, welcome_chip, history_replay }
```

**사유:**
- 정적 검증 중 `submitQuery` 호출부 점검에서 누락 2건 발견:
  - 좌측 사이드바 "최근 질의" 클릭 (`_onQuickHistory` → 캐시 미스 시 재실행)
  - 이력 모달의 "재사용" 버튼 (`_reuseHistQuery`)
- 두 진입 모두 **본인이 과거에 던진 질문을 다시 던지는 행위** — 신규 질의(`main_input`)나 추천 진입(`welcome_chip`, `sidebar_chip`)과 분석 의미가 명확히 다름
- 같은 카테고리로 묶어 `history_replay` 단일 값으로 처리 (위치 차이는 분석상 무의미)
- D-001과 동일한 사유로 [RAAS_EVENTS.md §9 변경 정책](RAAS_EVENTS.md)의 "새 표면 추가" 허용 범위 안

**구현 위치:**
- `raas_web.html` — `_onQuickHistory(q)` → 캐시 미스 분기에 `submitQuery(q, 'history_replay')`
- `raas_web.html` — `_reuseHistQuery(btn)` → `submitQuery(btn.dataset.q, 'history_replay')`

**영향:**
- NSM/D3 결정 규칙 변경 없음
- D3 분석 시 `history_replay`는 "신규 의도가 아닌 재실행"으로 별도 군집화 가능 → 노이즈로 빼고 D3 계산 시 정확도 ↑

---

### D-004 — PostHog 자동 발사 추가 차단: Web vitals + Session Recording

**변경 대상:** `raas_web.html` `_bootPostHog()` 안의 `posthog.init()` 옵션

**변경 전:**
```js
posthog.init(cfg.key, {
  api_host, autocapture: false,
  capture_pageview: false, capture_pageleave: false,
  persistence: 'localStorage+cookie',
});
```

**변경 후 (추가 2줄):**
```js
posthog.init(cfg.key, {
  api_host, autocapture: false,
  capture_pageview: false, capture_pageleave: false,
  capture_performance: false,          // ← 추가
  disable_session_recording: true,     // ← 추가
  persistence: 'localStorage+cookie',
});
```

**사유:**
- Day 2 PostHog Activity 라이브 검증 중 다음 두 종류의 **스펙 외 자동 이벤트** 발견:
  - `Web vitals` (Core Web Vitals — LCP/FID/CLS 등 성능 지표) — `capture_performance` 기본값 `true`
  - 세션 녹화 (`/s/?...&compression=gzip-js` 형태로 19건 누적) — `disable_session_recording` 기본값 `false`
- [docs/RAAS_EVENTS.md §1·§5](RAAS_EVENTS.md)는 "5개 이벤트만 명시적으로 측정. autocapture: false"로 노이즈 차단 의도를 명시함 — 위 둘은 그 의도에 어긋남.
- 비용 영향:
  - Web vitals: 페이지 진입마다 5~10건 발사 → 1M 무료 한도를 빠르게 소진
  - Session recording: 트래픽 폭발 + 본문 압축 페이로드에 사용자 입력·화면이 포함될 수 있어 사내 도구 개인정보 정책상 부적절
- `Set person properties`는 `posthog.identify()` 호출의 불가피한 부산물 → 그대로 유지.

**구현 위치:**
- `raas_web.html` — `_bootPostHog()` 안의 `posthog.init()` 옵션에 두 줄 추가

**영향:**
- NSM/D3 결정 규칙 변경 없음
- PostHog Activity에 `ask_ai`, `Identify`, `Set person properties`만 남음 (Week 3 이후 `view_briefing` 등 추가)
- Network 탭의 `/s/?...` 세션 녹화 트래픽 0건으로 감소

---

### D-016 — 원인 분석 온톨로지 .ttl로 리팩터 (옵션 A: 프로젝트 컨벤션 일치)

**변경 대상:** `raas_onto/raas_ontology_cause.ttl` (신규) · `raas_storyline_engine.py` · `data/ontologies/` (삭제)

**배경:**
- 사용자 지적(2026-06-18): 기존 온톨로지(7개 파일, 110KB)가 모두 .ttl인데 cause_analysis만 .json으로 만든 것은 컨벤션 위반
- 확인 결과: `raas_onto/raas_ontology_kpi.ttl`에 이미 `raas:DAU_Day`, `raas:NewUser_Day`, `raas:ChurnRate_Day`, `raas:RetentionRate_D1_NewUser` 등 47개 메트릭이 정식 `raas:Metric`/`raas:MetricVariant` 클래스로 정의됨 → JSON으로 만든 cause_analysis는 명백한 중복
- 사용자 결정: 옵션 A (정통 — TTL + rdflib + SPARQL)

**구현:**

1. **신규 TTL** `raas_onto/raas_ontology_cause.ttl` (~200줄):
   - `owl:imports <http://raas.sbs.co.kr/onto>` — KPI 온톨로지 참조
   - 신규 클래스: `raas:CauseDecomposition`, `raas:InterpretationRule`, `raas:DataAvailability`, `raas:SplunkIngestHint`
   - 신규 속성: `raas:usesMetric`, `raas:dataAvailability`, `raas:hasIngestHint`, `raas:hasInterpretationRule`, `raas:purpose`, `raas:formula`, `raas:trigger`, `raas:interpretation`, `raas:splunkSPL`, `raas:metricKey`, `raas:priority`
   - 4축 인스턴스: `FlowDecomposition`, `CohortDecomposition`, `StickinessDecomposition`, `SegmentDecomposition`
   - 각 축이 기존 KPI 메트릭 직접 참조 (예: `FlowDecomposition raas:usesMetric raas:NewUser_Day, raas:ReactivatedUser_Day, raas:ChurnRate_Day, raas:DAU_Day`)
   - 19개 InterpretationRule 인스턴스 (trigger → interpretation 매핑)
   - 3개 SplunkIngestHint (`Hint_DAUByGender`, `Hint_DAUByAge`, `Hint_DAUByTimeslot`)

2. **엔진 리팩터** (`raas_storyline_engine.py`):
   - `_load_rdf_graph()` — `raas_onto/raas_ontology_kpi.ttl` + `raas_ontology_cause.ttl` 결합 그래프 캐시 (rdflib 7.6.0)
   - `_query_decompositions()` — SPARQL로 4축 + ingest hints를 priority 순서로 조회
   - `_query_interpretation_rules(decomp_local)` — 축별 해석 규칙 매핑 조회
   - `_flow_trigger(flow)` / `_cohort_trigger(cohort)` — 계산 결과 → 트리거 키 매핑 (해석 규칙 적용용)
   - `_build_cause_raw_text` — 온톨로지 4축 정의를 그대로 사용, 해석 규칙도 SPARQL에서 가져온 문구를 LLM polish 입력에 포함

3. **JSON 제거:**
   - `data/ontologies/cause_analysis.json` 삭제
   - `data/ontologies/` 폴더 삭제

**SPARQL 쿼리 예시:**
```sparql
PREFIX raas: <http://raas.sbs.co.kr/onto#>
SELECT ?ax ?label ?prio ?purpose ?availLabel WHERE {
    ?ax a raas:CauseDecomposition ;
        rdfs:label ?label ; raas:priority ?prio ;
        raas:purpose ?purpose ; raas:dataAvailability ?avail .
    ?avail rdfs:label ?availLabel .
    FILTER(LANG(?label) = "ko")
} ORDER BY ?prio
```

**검증 결과:**
- 결합 그래프: 793 triples (KPI 601 + cause 192)
- 4축 SPARQL 조회: priority 1~4 정상 ✅
- 세그먼트 ingest hints 3종 정상 추출 ✅
- 풀 흐름(apollo, 웬디의 영스트리트 +20% 변화) LLM polish 결과:
  - 4축 모두 한국어 자연 문장 + 해석 규칙 적용
  - "신규·복귀 유입은 양호하나, 이탈 증가가 DAU 상승폭을 제한..."
  - 세그먼트 부족 메트릭 3종 + SPL 예시 포함
- 서버 재시작 후 `/api/storyline/entry` 정상 응답 ✅

**의존성 신규:**
- rdflib 7.6.0 (`python -m pip install rdflib`)

**사유:**
- 데이터 직무 인터뷰: "직무-데이터 관계 매핑 시스템 만들고 싶음" → SPARQL이 이 관계 매핑의 자연스러운 표현
- 기존 KPI 온톨로지의 형식 정의를 재사용 → 메트릭 정의가 단일 source of truth
- 향후 SPARQL로 "어느 직무가 어떤 메트릭에 의존하는지" 같은 cross-onto 쿼리 가능
- 빌드 스텝 없이 런타임 직접 rdflib 사용 (옵션 B의 TTL→JSON 변환 빌드 스크립트보다 깔끔)

**영향:**
- 메트릭 정의 중복 제거 (47개 메트릭이 raas_ontology_kpi.ttl 한 곳에만 정의됨)
- 새 해석 규칙 추가 시 TTL만 편집 — 엔진 코드 변경 없음
- 향후 다른 분석 프레임(예: 이상신호 탐지, 보고서 자동 구성)도 같은 패턴 (TTL + SPARQL) 적용 가능
- rdflib 의존성 1개 추가 (~1.5MB)

**미적용 (다음 사이클):**
- 다른 직무 컴퓨터들도 TTL 온톨로지 직접 참조하도록 통일 (현재는 코드 내부에 KPI 키 하드코딩)
- 직무(role) 자체를 RDF 클래스로 모델링: `raas:Role`, `raas:CP a raas:Role` 등 → 직무-데이터 관계를 SPARQL로 조회
- `raas_ontology_storyline.ttl` 신규 — 스토리라인 5슬롯도 TTL로 표현 (스토리라인 JSON의 ontology 블록 분리)
- SPARQL 쿼리 캐싱 (현재는 매 호출마다 실행, 4축 + 19규칙 = 23회 쿼리 / cause 호출)

---

### D-015 — CP 담당 채널 선택 UI (옵션 A: 별도 channel 필드)

**변경 대상:** `raas_history_db.py` · `raas_auth.py` · `raas_server.py` · `raas_web.html` · `raas_storyline_engine.py`

**배경:**
- CP는 파워FM / 러브FM 중 한 채널을 담당. 사용자가 명시 선택할 수 있는 UI 필요
- 사용자 결정(2026-06-18): 옵션 A — 별도 `channel` 컬럼 + role=CP일 때만 select 노출
- 옵션 B(my_programs[0]에 채널 코드)·C(role 세분화)는 의미 혼동·시스템 영향 우려로 미채택

**구현 (전 스택):**

1. **DB schema** (`raas_history_db.py`):
   - `ALTER TABLE users ADD COLUMN channel TEXT` 자동 마이그레이션
   - 기존 사용자 영향 없음 (NULL 기본값)

2. **Auth 계층** (`raas_auth.py`):
   - `ALLOWED_CHANNELS = {'파워FM', '러브FM'}` 신설
   - `_user_dict`가 `channel` 포함하여 반환
   - `authenticate` / `resolve_session` SELECT에 `u.channel` 추가
   - `update_profile`에 `channel` 인자 추가:
     - role=CP가 아닌데 channel 설정 시도 → role 변경 후엔 channel 자동 NULL
     - 빈 문자열 → NULL 저장
     - ALLOWED_CHANNELS 외 값 → 거부

3. **API** (`raas_server.py`):
   - `/api/me/update`에 `channel` 필드 전달
   - `/api/me`은 `_user_dict` 사용하므로 자동 반환

4. **프론트엔드** (`raas_web.html`):
   - 프로필 모달에 신규 select `#pfChannel` (파워FM / 러브FM 옵션)
   - `#pfChannelField`는 직무가 CP일 때만 display:flex
   - `_pfOnRoleChange()` — role 변경 시 가시성 토글, CP 아니면 값 비움
   - `openProfileModal()` — RAAS_USER.channel로 초기값 셋팅
   - `saveProfile()` — CP면 channel 필수 검증, 채널 변경 시 `_resetStoryline()` 호출

5. **엔진** (`raas_storyline_engine.py`):
   - `_user_context`에서 CP 미설정 시 "파워FM" 기본값 **제거** (사용자 명시 선택 강제)
   - `entry()`가 CP + channel 미설정이면 setup_required 응답 분기:
     - first_question을 안내 메시지로 대체
     - chips는 빈 배열
     - `setup_action: "open_profile_modal"` 포함

6. **프론트 setup_required 처리** (`_renderStorylineWelcome`):
   - `entry.setup_required === true`면 칩 대신 **⚙ 내 정보 열기** 버튼 1개 노출
   - 클릭 시 `openProfileModal()` 호출
   - 배지 텍스트 "CP (책임 프로듀서) · 셋업 필요"

**검증 결과:**
- DB 마이그레이션: 기존 apollo 사용자 보존, channel 컬럼 NULL로 추가 ✅
- `_derive_cp_channel` 우선순위: `user.channel` → `my_programs[0]` prefix → None
- apollo(my_programs=['F05']) → 채널 자동 도출 '파워FM' ✅
- 신규CP(my_programs=[], channel=None) → setup_required: True ✅
- HTTP API: `/api/storyline/entry?role=cp&channel=러브FM` → first_question에 러브FM ✅

**우선순위 매트릭스:**
| user.channel | my_programs[0] | 결과 |
|---|---|---|
| 파워FM | (any) | 파워FM (명시 우선) |
| 러브FM | (any) | 러브FM |
| NULL | F01 | 파워FM (자동 도출) |
| NULL | L05 | 러브FM (자동 도출) |
| NULL | [] | **setup_required** 발동 |

**사유:**
- 명시 선택 가능하되 my_programs 기반 자동 도출 fallback 유지 → 신규 사용자도 빠른 진입
- 사용자가 채널 변경 즉시 스토리라인 재렌더 → 채널별 차이 즉시 체감 가능
- DB에 별도 컬럼이라 다른 직무(편성PD 등)도 차후 채널 기반 확장 가능

**영향:**
- 프로필 모달에 새 select 1개 추가 (CP 선택 시만 표시 → UI 부담 없음)
- 기존 사용자 데이터 100% 보존
- 엔진 행동: my_programs 없으면 더 이상 기본값 "파워FM" 자동 사용 X → 사용자가 의도 표명해야 함

**미적용 (다음 사이클):**
- 편성PD에 채널 선택 적용 (역시 채널 단위 그리드를 보므로 동일 패턴 가능)
- 채널 → 프로그램 매핑 자동화 (CP가 채널 선택 시 my_programs 빈 상태면 채널 프로그램 전체 자동 채움)
- 다른 직무들도 자기에게 의미있는 셋업이 누락되면 setup_required 패턴 적용

---

### D-014 — Phase 3 ⑥ 산출물 변환: matplotlib + python-pptx 서버 측 렌더링

**변경 대상:** 신규 `raas_report_engine.py` + `raas_server.py` 2개 라우트 + `raas_web.html` `_end` 핸들러

**산출:**
- `raas_report_engine.py` (~370줄):
  - matplotlib 차트 PNG 렌더러 (timeseries / multi / dual / bar — 4종 우선)
  - `_detect_korean_font()` — Malgun Gothic 자동 탐지 (Windows)
  - `_build_one_pager_ppt(title, body_text, chart_pngs, footer)` — 1장 PPT 레이아웃
  - `build_storyline_export(role, output_format_id, slots_visited, context)` — 통합 진입점
  - `store_for_download(result)` + `fetch_for_download(token)` — 일회용 토큰 캐시 (최대 32개)
- `raas_server.py`:
  - `POST /api/storyline/export` — stub → 실제 생성 (REPORT.build_storyline_export 호출)
  - 신규 `GET /api/storyline/exports/{token}` — 1회용 다운로드 (Content-Disposition 한글 파일명 UTF-8 인코딩)
- `raas_web.html` `_onStorylineChip` `_end` 케이스:
  - "생성 중…" 상태 표시 → POST export 요청 → 다운로드 링크 렌더
  - 파일 크기 표시 (KB)
  - 실패 시 빨강 경고 박스 + 재시작 칩

**산출물 매핑 (output_format_id → 파일 종류):**
| output_format_id | 파일 | 비고 |
|---|---|---|
| `director_one_pager_ppt` | PPT | CP 국장 1장 |
| `weekly_meeting_free_format` | PPT | 주간 회의 |
| `business_director_mau_brief` | PPT | 사업국장 MAU |
| `cp_one_pager` / `scheduling_meeting_formal_report` 등 | PPT | 1장 보고서 양식 |
| `pd_dispatch_message` / `writer_host_share_kakao` | TXT | 카톡 텍스트 |
| `graph_excel_download` / `png_save_mobile` | PPT(현재) | 후속에 xlsx/png 분기 |

**1장 PPT 레이아웃:**
```
[제목]                                              (상단)
[본문 텍스트 (방문한 슬롯의 answer 합본)] | [차트 1]
                                          | [차트 2]
                                          (slots_visited[].chart_data 있을 때)
[푸터 (사용자명 · 생성일 · RAAS 자동 생성)]
```

**검증 결과:**
- 자가 테스트: `python raas_report_engine.py` — PNG (30KB) + PPT (56KB) 정상 생성 ✅
- HTTP 검증: `POST /api/storyline/export` → 28KB PPT + 토큰 + download_url 반환 ✅
- 다운로드 검증: `GET /api/storyline/exports/{token}` → 28KB 바이트 + 정확한 Content-Type + Content-Disposition 한글 파일명 ✅

**의존성 신규:**
- matplotlib 3.11.0
- python-pptx 1.0.2
- Pillow 12.2.0
- 모두 `python -m pip install matplotlib python-pptx Pillow` 완료

**사유:**
- D-013(ECharts)는 인터랙티브 웹 차트, D-014(matplotlib)는 정적 보고서 — 두 방식이 보완 관계
- CP가 인터뷰에서 "변화 결과만이 아니라 왜 + 다음 액션"이 핵심 페인 → 1장 PPT가 즉시 가치 가장 큼
- 토큰 캐시 = 다운로드 1회 후 자동 폐기 → 인증 미들웨어 없이 안전한 일시 공유
- 한국어 폰트 자동 탐지로 시스템 의존성 최소화 (Malgun Gothic은 Windows 기본 탑재)

**영향:**
- 스토리라인 5_closing 칩 클릭이 즉시 다운로드 가능한 PPT/TXT 생성 (수 초 이내)
- 인터뷰의 "보고용 자료 만드는 데 반나절" 페인 직접 해소 (Track A·B·C 통합 가치)
- 향후 차트 chart_data만 보강하면 PPT 안의 차트도 자동 보강

**미적용 (다음 사이클):**
- 차트 9종 중 funnel/heatmap/scatter/range_band의 matplotlib 렌더러 (현재 line/bar만)
- xlsx 출력 (graph_excel_download) — openpyxl 별도 도입
- 모바일 PNG 단독 export (png_save_mobile) — 차트 1장만 PNG로
- 카톡 직접 발송 (현재는 TXT 다운로드 → 사용자가 복붙)
- 토큰 만료 시간 (현재는 1회 fetch로 자동 폐기)
- 산출물 미리보기 (다운로드 전 슬라이드 썸네일)

---

### D-013 — 차트 품질 개선: ECharts 점진 도입 (옵션 2)

**변경 대상:** `raas_web.html` (CSS + JS)

**배경:**
- 사용자가 외부 도구로 그린 matplotlib 풍 멀티라인 차트(러브FM 시간대별 청취자수 추이)와 RAAS 자체 SVG 차트의 품질 비교 → matplotlib 풍이 훨씬 깔끔
- 핵심 차이: 전용 차팅 라이브러리 vs 수동 SVG
- 옵션 비교: A=Chart.js, B=ECharts, C=서버 PNG. 사용자 결정 **옵션 2 (ECharts 점진 도입)**

**구현 (멀티라인 timeseries만 먼저 교체):**
- ECharts 5.5.1 CDN lazy 로드 (`https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js`)
- `_loadECharts()` Promise 캐시 — 1회만 다운로드
- `_buildEChartsMultiLineOption(chart)` — chart_data → ECharts option 어댑터
  - 테마(라이트/다크) 자동 반영 (`_eChartsThemeColors()`)
  - 8색 팔레트(예시 차트와 유사 톤)
  - SVG 렌더러 (애니메이션 부드러움)
  - 같은 단위면 공통 척도, 다르면 시리즈 독립
  - Y축 한글 단위 ("만")
- `_initEChartsInstance(el, chart, ec)` — 컨테이너에 인스턴스 + ResizeObserver
- `_ECHARTS_LAZY_OBSERVER` IntersectionObserver — 컨테이너가 200px 안에 들어오면 ECharts 다운로드·초기화
- 기존 SVG `renderMultiSparkline`은 `renderMultiSparklineSVG`로 이름 유지 → **fallback 자동**
  - CDN 로드 실패 시 SVG로 대체
  - IntersectionObserver 미지원 시 즉시 초기화
- 테마 토글 시 `_refreshEChartsTheme()` — 모든 ECharts 인스턴스 옵션 재설정

**범위 (이번 사이클 — timeseries_multi만):**
- ✅ timeseries_multi (다중 라인) → ECharts
- ⏸ timeseries (단일 라인) → 다음 단계
- ⏸ timeseries_dual (이중 축) → 다음 단계
- ⏸ comparison (막대) → 다음 단계
- ⏸ table / overview / ranking → 변경 없음 (HTML 테이블이 이미 충분)

**사유:**
- "점진"의 의미: 한 차트 타입을 먼저 교체해 검증 → 나머지로 확대
- timeseries_multi가 가장 효과 큼 — 사용자가 보여준 예시가 정확히 이 타입
- SVG fallback 보존 → CDN 다운 시에도 안전, 회귀 risk 0
- ECharts 5.5.1 = LTS, 약 1MB minified (gzipped ~300KB)

**영향:**
- 첫 멀티라인 차트 렌더 시 약간의 추가 지연(CDN 다운로드, 1회만)
- 이후엔 캐시되어 즉시 렌더
- 차트 호버 시 모든 시리즈 값 동시 표시 툴팁 (기존 SVG에 없던 UX)
- 테마 토글 시 ECharts도 자동 색상 전환

**D-013a 후속 (사용자 피드백 반영):**
- 단일 라인 `timeseries`도 ECharts로 확장 (`renderSparkline` 교체, SVG는 `renderSparklineSVG`로 보존)
- 어댑터 `_buildEChartsMultiLineOption`이 단일 라인(`chart.points`)을 series 1개로 자동 정규화
- fallback이 chart 형태에 따라 분기 (single/multi/dual)
- 디버그용 `console.log('[echarts] init ...')` 추가
- 적용 범위 (1차+1차 확장):
  - ✅ `timeseries` (단일 라인) → ECharts
  - ✅ `timeseries_multi` (다중 라인) → ECharts

**D-013b 후속 (사용자 피드백: 질의 의도된 기간 반영):**
- 백엔드가 보내는 `chart.initial_days`(질의 의도 분류 결과)를 ECharts가 기본 뷰 범위로 사용
- `dataZoom` 슬라이더(하단) + `inside`(휠/드래그) 도입 — 기존 SVG의 "최근 N일" 버튼을 대체
- 차트 제목 아래 `subtext`로 기간 라벨 자동 표시 ("최근 30일" / "최근 3개월" 등)
- `_formatPeriodLabel(cadence, n)` — cadence(1d/1w/1m)별 한국어 자동 변환
- `intentDays >= totalPts`면 슬라이더·라벨 모두 생략 (간섭 없음)

**D-013c 후속 (사용자 피드백: 이중 Y축도 ECharts):**
- `timeseries_dual` 차트도 ECharts로 확장 — 기존 SVG는 `renderDualSparklineSVG`로 보존
- 신규 `_buildEChartsDualAxisOption(chart)` 어댑터:
  - **같은 단위**(예: 두 시리즈 모두 명): 멀티라인 어댑터에 위임 (단일 Y축)
  - **다른 단위**(예: 명 + %): 좌/우 독립 Y축 (`yAxisIndex` + `position: 'left'/'right'`)
  - 각 Y축에 시리즈 색상 적용 (이름·티크 라벨이 시리즈와 같은 색)
- 툴팁이 각 시리즈의 단위를 개별 포맷 (명은 천단위 콤마, %는 소수 1자리)
- `_initEChartsInstance`에 `chart.type === 'timeseries_dual'` 분기
- `_refreshEChartsTheme`도 type별 분기 (라이트/다크 토글 시)
- IntersectionObserver의 CDN 실패 fallback도 3-way (dual → multi → single) 분기
- 적용 범위 (이번 확장 후):
  - ✅ `timeseries` (단일 라인) → ECharts
  - ✅ `timeseries_multi` (다중 라인) → ECharts
  - ✅ `timeseries_dual` (이중 Y축) → ECharts
  - ⏸ `comparison` (막대) → 다음 단계
  - ⏸ table 계열 → 변경 없음 (HTML 충분)

**D-013d 후속 (사용자 피드백: comparison + 9종 매핑 통일):**
- `renderBarChart`도 ECharts로 교체 (기존 SVG는 `renderBarChartSVG`로 보존)
- 통합 디스패처 `_buildEChartsOption(chart)` 도입 — `chart.chart_type`(9종) 우선, 없으면 `chart.type`(내부) 폴백
- 9종 chart_type 옵션 빌더 추가:
  - `_buildEChartsBarOption(chart, kind)` — bar_rank(내림차순 정렬) / bar_delta(±색 분기)
  - `_buildEChartsStackedBarOption(chart)` — series별 누적 막대
  - `_buildEChartsDivergingBarOption(chart)` — 좌우 분기 막대 (음수 좌, 양수 우)
  - `_buildEChartsFunnelOption(chart)` — 퍼널 (내림차순 + 단계별 인구 라벨)
  - `_buildEChartsHeatmapOption(chart)` — 히트맵 (visualMap 그라데이션)
  - `_buildEChartsScatterOption(chart)` — 산점도
  - `_buildEChartsRangeBandOption(chart)` — 라인 + 상/하한 영역 (예: 신뢰구간)
- `_initEChartsInstance`·`_refreshEChartsTheme`·fallback 분기 모두 통합 디스패처 사용
- 적용 범위 (최종):
  - ✅ `timeseries` (단일 라인) → ECharts
  - ✅ `timeseries_multi` (다중 라인) → ECharts
  - ✅ `timeseries_dual` (이중 Y축) → ECharts
  - ✅ `comparison` (막대) → ECharts
  - ✅ 9종 chart_type 옵션 빌더 8종 (line은 multi-line이 처리)
  - ⏸ `table` / `overview` / `ranking` → HTML 테이블 유지 (이미 충분)
- → 차트 라이브러리 통일 완료. AI가 9종 중 어느 것을 정하든 즉시 동작

**Phase 3 ⑥ 별도 진행:** D-014로 분리 — matplotlib + python-pptx 서버 측 렌더링은 정적 보고서용

---

### D-012 — Phase 2 ④ 프론트엔드 구현: 스토리라인 대화 UI + 직무 변경 즉시 반영

**변경 대상:** `raas_web.html` (CSS + JS) · `raas_storyline_engine.py` (역할 매핑 보정)

**산출:**
- 첫 화면이 자동으로 직무 감지 → 지원 직무(CP·제작·편성)면 스토리라인 모드, 미지원 직무는 기존 추천 칩
- 칩 클릭 → POST /api/storyline/advance → AI 버블로 답변 + 다음 칩 인라인 렌더
- `_end` 칩(산출물 선택) → ✅ 메시지 + "처음으로 돌아가기" 버튼
- `markdown table` + `**bold**` + `\n` 자동 렌더 (story-table CSS 클래스)
- **'내 정보' 직무 변경 → 저장 시 즉시 새 직무 스토리라인 자동 재렌더** (사용자 요청 핵심)

**라우팅 로직 (loadSuggestions):**
1. `/api/storyline/role-detect` 호출 → 지원 직무면 storyline 진행, 아니면 fallback
2. storyline 진행 시 `/api/storyline/entry?role={role}` 호출 → welcomeScreen 교체
3. 미지원 직무는 기존 `/api/suggestions` 추천 칩 그대로

**클라이언트 상태:**
```
window._RAAS_STORYLINE = {
  role: 'cp' | 'pd_program' | 'pd_schedule' | null,
  available: bool,
  context: {},     // 직전 advance의 context_out — 다음 호출의 prev_context로 보냄
  slot: 'entry' | '1_anchor' | ...,
}
```

**서버 측 보정 (사용자 테스트 편의):**
- KOREAN_ROLE_TO_ID에 `"제작"` `"편성"` 추가 — raas_auth.ALLOWED_ROLES와 일치 (기존엔 "제작PD"·"편성PD"만 있어서 매핑 실패)
- CP 사용자가 channel 미설정이면 엔진이 "파워FM" 기본값 — 시연·테스트 편의 (운영 시 Phase 2 ⑤에서 프로필에 channel 필드 추가 예정)

**PostHog 이벤트 (Phase 4 환류 학습 원천):**
- `storyline_chip_click` — 매 칩 클릭 (role, slot_from, chip_label, chip_intent)
- `storyline_complete` — _end 도달 (role, output_format_id)

**테스트 가능한 흐름 (지원 직무 3종):**
| 직무 변경 | 첫 화면 변화 |
|---|---|
| CP | "파워FM 채널에서 어제 크게 움직인..." + 실 데이터 답변 (KPI CSV) |
| 제작 | "[프로그램]의 직전 회차..." + my_programs 등록 후 본격 동작 |
| 편성 | "채널 grid... 흔들린 시간대부터..." (시간대 데이터 적재 후 본격) |
| 총괄관리·플랫폼전략·마케팅·서비스운영·데이터 | 기존 추천 칩 (legacy /api/suggestions) |

**사유:**
- 사용자 요청: "내 정보에서 직무를 바꿔가며 테스트할 거야"
- 직무는 페르소나의 핵심 정체성 → 변경 즉시 첫 화면이 바뀌어야 자연스러움
- 백엔드(D-011)가 이미 직무별 응답을 주므로 클라이언트는 라우팅·렌더만 책임
- 시뮬레이터의 render() 로직을 그대로 _renderStorylineText()로 이식 + 마크다운 테이블 보강 (CP 4_loop, 편성PD 1_anchor가 테이블 사용)

**영향:**
- 첫 화면이 "안녕하세요 👋 + 일반 추천 칩"에서 "직무별 맞춤 인사 + 의도된 5슬롯 진입 칩"으로 변경
- 미지원 직무는 변화 없음 (안전 fallback)
- 칩 클릭 → 답변 → 다음 칩 흐름으로 사용자가 5슬롯 전체 탐색 가능
- 자가 학습 데이터(PostHog) 수집 시작 — Phase 4 ⑦ 환류 학습 준비 완료

**미적용 (다음 사이클):**
- Phase 2 ⑤: 사용자 프로필에 `channel` 필드 추가 UI (CP 한정 — 파워FM/러브FM 선택)
- 시간대·인구통계·공감로그 데이터 적재 후 fallback → 본격 답변 전환 (Phase 1 ①)
- 산출물 변환 실제 구현 (Phase 3 ⑥) — PPT/카톡

**D-012a 후속 보강 (사용자 피드백 반영):**
- 사용자 보고: "제작PD 첫 인사에 `[program_name?]` placeholder가 보임. 내 정보에서 my_programs(컬투쇼) 등록했는데 왜?"
- 원인: 엔진의 `_user_context()`가 PD_PROGRAM 직무에서 `program_name`을 채워주지 않음 (미구현)
- 수정:
  1. `_parse_my_programs(user)` 헬퍼 — SQLite JSON 문자열 표준화
  2. `_derive_pd_primary_program(user)` — my_programs[0] 코드 반환
  3. `_user_context()` 분기 — PD_PROGRAM이면 `program_code`/`program_name`/`last_episode_date`/`weekday` 자동 채움 (KPI CSV에서 최신 행 조회)
  4. `_load_program_latest_row` + `_load_program_history` + `_kpi_rows()` mtime 캐시 추가
  5. `_compute_pd_program_anchor` 슬롯 컴퓨터 추가 — 동일 요일 4주 평균 대비 WoW 계산 (KPI CSV만으로 동작)
  6. `_SLOT_COMPUTERS[("PD_PROGRAM","1_anchor")]` 등록
- JSON 수정 (사용자 요청 직접 반영):
  - `pd_program.json` first_question을 사용자가 적은 톤으로: "**{program_name}** 어제 방송에서 눈에 띄는 변화가 있었어요. 먼저 보시겠어요?"
  - 1_anchor fallback_answer_template 정리 — 디자인 주석 ("*평소와 거의 같다면*: ... (인터뷰: ... 유도)")이 사용자에게 노출되던 부분 제거. KPI 실데이터(활성사용자·요일 평균·신규 변화율)로 깔끔히 재작성

**검증 결과 (apollo 사용자, role=제작, my_programs=["F09"]):**
- entry first_question: "**두시탈출 컬투쇼** 어제 방송에서 눈에 띄는 변화가 있었어요. 먼저 보시겠어요?" ✅
- 1_anchor advance 결과: "두시탈출 컬투쇼 직전 회차(2026-05-27 수요일) 결과입니다. 활성사용자: **40,955명** (동일 요일 4주 평균 대비 **-7.4%**) 신규 사용자 변화율: -7.5%..." ✅ (실 KPI 데이터)
- context_out에 program_code/name 자동 포함 → 다음 슬롯이 항상 "내 프로그램" 전제로 동작 ✅

---

### D-011 — Phase 2 ③ 백엔드 구현: 스토리라인 엔진 + 4개 엔드포인트

**변경 대상:** 신규 `raas_storyline_engine.py` + `raas_server.py` 4개 라우트 추가

**산출:**
- `raas_storyline_engine.py` (450줄) — JSON 로더·렌더러·`StorylineEngine`·`role_detect`·`export_stub`
- `raas_server.py`:
  - `GET /api/storyline/role-detect` — 사용자 role → 스토리라인 직무 매핑
  - `GET /api/storyline/entry?role=cp&channel=파워FM` — 첫 화면
  - `POST /api/storyline/advance` — 칩 클릭 → 다음 슬롯 답변 + chips_next 반환
  - `POST /api/storyline/export` — 산출물 생성 stub (Phase 3 ⑥에서 본격)
- CLI 자가 테스트(`python raas_storyline_engine.py`) + curl/urllib 4종 모두 검증 완료

**핵심 설계:**
- **JSON이 단일 출처** — 코드는 데이터 채우기·렌더링만. 스토리라인 변경 시 코드 수정 불필요
- **슬롯 디스패치 테이블** — `_SLOT_COMPUTERS[(role, slot_id)]` = 데이터 계산 함수. 적재 완료된 슬롯만 채우면 됨
- **render()** — 시뮬레이터의 검증된 로직 그대로 이식. `{변수:포맷}` 지원, 누락 변수 `[var?]` 표시
- **fallback 자동** — `data_requirements`의 `available=false + blocks_quality=high` 1개라도 있으면 `fallback_answer_template` 사용
- **context_out 패스스루** — `advance` 응답의 `context_out`을 다음 호출 `prev_context`에 넣어 슬롯 간 변수 전달

**MVP 상태:**
- **CP `1_anchor` 슬롯**: 실 데이터 동작 ✅ (raas_kpi_latest.csv → 파워FM 13개 프로그램 변화 TOP 3)
  - 검증 결과: "**파워FM**의 **13개 프로그램** 중 **웬디의 영스트리트**이 전주 대비 **+20.0%**..."
- **나머지 슬롯**: fallback 동작 ✅ (prev_context 변수는 전달됨 → "**웬디의 영스트리트**의 변화 원인을...")
- **Phase 1 ① 적재 완료 시 `_SLOT_COMPUTERS`에 함수 추가만 하면 본격 동작**

**검증 결과 (4개 엔드포인트 모두 통과):**

| Endpoint | 응답 |
|---|---|
| `GET role-detect` (no auth) | `role=null`, 사용 가능 직무 5개 리스트 |
| `GET entry?role=cp&channel=파워FM` | greeting + first_question + 칩 3개 |
| `POST advance` (entry→1_anchor) | 실 데이터 답변 + context_out + 칩 3개 |
| `POST advance` (1_anchor→2_cause) | fallback 답변 (prev_context 변수 보존) |
| `POST export` (CP director_one_pager_ppt) | stub 메시지 + Phase 3 ⑥ 예고 |

**사유:**
- 시뮬레이터의 render() 로직이 이미 검증됐으므로 엔진 구현 비용이 작음
- JSON 파일이 단일 출처 → 백엔드는 인프라(로딩·HTTP·인증)만 책임. 콘텐츠 수정 시 JSON만 편집
- `_SLOT_COMPUTERS` 디스패치 패턴 → Phase 1 ① 적재가 끝나는 슬롯부터 함수만 추가하면 점진 확장

**영향:**
- Phase 2 ④(프론트엔드 UI)에서 즉시 사용 가능한 API 4종 확보
- 칩 클릭 → `POST /api/storyline/advance` 1회 = 한 슬롯 진행. SSE 불필요(즉시 응답)
- 사용자 프로필 확장 필요: `channel` 필드 (CP 한정) — 현재는 쿼리/바디로 override 가능
- 서버 재시작 필요 — 기존 실행 중인 서버는 새 라우트 없음

**미적용 (다음 작업):**
- Phase 2 ④: 프론트엔드 첫 화면 직무 라우팅 + 칩 UI
- 사용자 프로필에 channel 필드 추가 UI (CP는 파워FM/러브FM 선택)
- 다른 슬롯 데이터 컴퓨터 추가 (Phase 1 ① 적재 완료 후):
  - `_compute_cp_cause` — 공감로그·게스트 데이터 적재 후
  - `_compute_pd_program_anchor` — 코너 단위 데이터 적재 후
  - `_compute_pd_schedule_anchor` — 시간대 데이터 적재 후
- Phase 3 ⑥: export_stub → 실 PPT/카톡 생성기
- PostHog 이벤트 발사 백엔드 (현재는 프론트 발사 가정)

---

### D-010b — Phase 1 ② 진행: 제작PD + 편성PD 스토리라인 추가 + 공통 패턴 도출

**변경 대상:** `data/storylines/pd_program.json` + `pd_schedule.json` + `_common_patterns.md` (신규)

**산출:**
- `pd_program.json` — 제작PD 5슬롯 (4 KPI / 4 outputs / 5 high+medium 차단 데이터)
- `pd_schedule.json` — 편성PD 5슬롯 (4 KPI / 4 outputs / 4 high + 4 medium 차단 데이터)
- `_common_patterns.md` — 3직무 공통 패턴 분석 + Phase 2 ③ 백엔드 스펙 + 데이터 적재 통합 우선순위
- README.md — 진행 상황 체크리스트 갱신

**3직무 교차 분석에서 드러난 핵심 발견:**

1. **공유 적재 1건 = 다수 직무 동시 잠금해제**
   - "공감로그/문자 건수" = 제작PD + CP (2직무)
   - "시간대별 활성사용자" = 편성PD + (서비스운영 + CP)
   - "인구통계(성별/연령대)" = 편성PD + 마케팅
   - "프로그램→인접 시간대 매핑" = 제작PD + 편성PD + CP (3직무)
   → 적재 우선순위 재정렬: 공유 차단 건이 1순위

2. **즉시 시연 가능 경로** (모든 직무 fallback만으로):
   - CP: 2분 (entry → 1_anchor → 2_cause fallback → 5_closing)
   - 제작PD: 2분 (entry → 1_anchor → 3_context WoW만 → 5_closing)
   - 편성PD: 3분 (entry → 1_anchor 일/주/월만 → 3_context WoW/MoM만 → 5_closing)
   → 매핑 메타데이터만 등록되면 PoC 3직무 동시 시연 가능

3. **5슬롯 의도가 모든 직무 공통**:
   - `1_anchor`: 1순위 KPI 즉답
   - `2_cause`: 원인 후보 + 근거
   - `3_context`: 비교축 적용
   - `4_loop`: 폐쇄루프
   - `5_closing`: 핵심 3가지 + 산출물
   → 백엔드 엔드포인트는 직무 독립적으로 설계 가능 (JSON만 다름)

4. **outputsTo 형식별 그룹화** — 총 13개 산출물 중 PPT가 5개 → Phase 3 ⑥ PPT 생성기는 5개 보고서 양식의 공통 백엔드가 됨

5. **편성PD 특이점**: `storyline_hypothesis_input` 이벤트 추가 — 가설 검증 슬롯이 다른 직무에 없는 특수 기능

**Phase 2 ③ 백엔드 엔드포인트 스펙 확정** (3개 JSON 분석 기반):
- `GET /api/storyline/entry?role={role}` — 첫 화면
- `POST /api/storyline/advance` — 슬롯 전이 + 답변 렌더링
- `POST /api/storyline/export` — 산출물 생성
- `GET /api/storyline/role-detect` — 직무 자동 감지

**Phase 1 ① 적재 통합 우선순위 (3직무 교차):**

| 1순위 적재 (3직무 합산) | 차단 슬롯 수 |
|---|---|
| 공감로그/문자 건수 | 3 (제작PD 1·2, CP 2) |
| 특별 게스트/이벤트 캘린더 | 3 (제작PD 2, CP 2·4) |
| 시간대별 활성사용자 | 3 (편성PD 1·2·3) |
| 인구통계 (성/연령대) | 1 (편성PD 2, + 마케팅 작성 시 추가) |
| 분기 단위 집계 뷰 | 2 (편성PD 1·3) |
| 26년 4월 이전 과거 | 1 (편성PD 3 — YoY 자체 불가능) |

→ **공감로그 + 시간대별 활성사용자 2건 적재가 가성비 최고** (5슬롯 동시 잠금해제)

**사유:**
- 사용자 결정대로 A(스토리라인 데이터 완성)부터 진행
- 3개 만든 후 패턴 검증 + 데이터 적재 가이드 도출 = Phase 1 ①·Phase 2 ③ 즉시 진입 가능
- 잔여 5직무는 같은 스키마라 빠르게 가능 (이제 30분~1시간/개)

**미적용 (다음 작업):**
- 잔여 5직무 스토리라인 (general_mgmt, platform_strategy, service_ops, marketing, data_analyst)
- 백엔드 4개 엔드포인트 구현 (Phase 2 ③)
- 첫 화면 직무별 진입 UI (Phase 2 ④)
- 데이터 적재 작업 시작 (Phase 1 ①) — 사용자 직접 진행

---

### D-010 — Phase 1 ② 시작: 직무별 스토리라인 JSON 스키마 v1 + CP 1차 구현

**변경 대상:** 신규 `data/storylines/` 폴더 + `cp.json` + 스키마 `README.md`

**결정 기록 (사용자 컨펌):**
- Phase 1① 데이터 적재: **본인이 직접** (apollo)
- Phase 2 대상 3직무: **제작PD · 편성PD · CP**
- Phase 3 보고서 1순위: **CP 국장 보고용 1장 PPT**
- 시작점: **`data/storylines/cp.json` 부터** (가장 빠른 검증 루프)

**산출:**
- `data/storylines/cp.json` — CP 직무 5슬롯 + 온톨로지 4관계 + 데이터 의존성 명세 + fallback + PostHog 이벤트 정의
- `data/storylines/README.md` — JSON 스키마 v1 명세 (메타·persona_summary·ontology·entry·slots·data_dependencies·expected_flows·instrumentation 8블록)

**스키마 핵심 설계:**
- **5슬롯 ID 고정** (`1_anchor` → `2_cause` → `3_context` → `4_loop` → `5_closing`) — 스토리라인 문서 §4 온톨로지 슬롯 구조 직접 반영
- **온톨로지 4관계** JSON 키: `hasPrimaryKPI` / `isExplainedBy` / `prefersComparison` / `outputsTo`
- **chips_next.next_slot** 으로 슬롯 전이 그래프 표현
- **data_requirements + available 플래그**로 데이터 의존성 명시
- **fallback_answer_template**로 차단 슬롯의 최소 답변 보장
- **instrumentation.posthog_events** 5종으로 Phase 4 환류 학습 원천 데이터 정의

**cp.json 핵심 발견 (스키마 채우면서 드러난 것):**
1. **1순위 적재 필요 데이터 2건** (`2_cause` 슬롯 unblock):
   - 공감로그/문자 건수 (프로그램별 일간) — Splunk → RAAS
   - 특별 게스트/이벤트 캘린더 — 별도 메타데이터 적재
2. **2순위 적재 필요 데이터 4건**:
   - 프로그램 개편 일자·내용 타임라인 (`4_loop` unblock)
   - CP→담당 프로그램 매핑 (`1_anchor`, `3_context` 전제)
   - 프로그램→담당 PD 매핑 (`3_context` PD 디스패치 unblock)
   - 인접 시간대 프로그램 매핑 (`2_cause`, `3_context` 인접 비교 unblock)
3. **Phase 1 부분 적재만으로 동작 가능한 경로**: `entry → 1_anchor → 2_cause(fallback) → 5_closing` (KPI CSV + CP 프로그램 매핑만 있으면 됨) — PoC 가치 증명 가능

**사유:**
- 스토리라인 문서(§5)의 "데이터 직무가 RAAS에 온톨로지를 구축하고 Claude를 연결할 때"가 이 JSON으로 시작
- 글이 아닌 코드(데이터)로 옮겨야 백엔드/프론트엔드가 즉시 사용 가능
- CP를 먼저 고른 이유: 정보 허브 + "설명 못하면 필요없다" 가장 날카로운 신호 + 사업국장 신규 지시(2026-06 MAU 별도 보고)로 가치 즉시 검증 가능

**영향:**
- Phase 1 ①(데이터 적재) 우선순위가 cp.json의 `data_dependencies` 블록으로 명확화됨
- Phase 2 ③(백엔드 엔드포인트 3종)의 입력 스펙이 README §4에 정의됨
- Phase 3 ⑥(국장 1장 PPT)의 입력 데이터 = `5_closing` 슬롯의 산출물 변수
- Phase 4 ⑦(환류 학습)의 원천 이벤트가 instrumentation 블록에 사전 정의됨

**미적용 (다음 작업):**
- `pd_program.json` · `pd_schedule.json` 작성 (Phase 2 ② 잔여)
- 백엔드 엔드포인트 3종 구현 (Phase 2 ③)
- 첫 화면 직무별 진입 UI (Phase 2 ④)
- 국장 1장 PPT 생성기 (Phase 3 ⑥)
- 사용자 프로필에 "CP→담당 프로그램 매핑" 필드 추가 (스토리라인 진입 전제)

---

### D-009 — 인터뷰 결과 → 직무별 떠먹이기 스토리라인 정립

**변경 대상:** 신규 `docs/interviews/_persona_storylines.md`

**입력:**
- 8직무 인터뷰 결과 ([docs/interviews/interview_result.md](interviews/interview_result.md))
- 7직무 페르소나 메모리 원칙 ([CLAUDE.md](../CLAUDE.md))

**산출:**
- 8직무 핵심 정보 매트릭스 (반드시 필요한 정보·있으면 좋은 정보·페인·시그널)
- 정보 순환 구조 다이어그램 + 5가지 순환 패턴
- 8직무별 5~6단계 떠먹이기 스토리라인 (첫 화면 → Q&A → 마무리)
- 공통 패턴 6개 (자동 큐레이션·칩 선택·자발적 알림·보고 종결·원인 우선·책임자 자동 매핑)

**핵심 발견 7가지:**
1. 모든 직무가 같은 페인을 다른 단어로: **"원인이 설명 안 됨"**
2. CP가 정보 허브 (위/아래/옆 모든 흐름 통과)
3. 시간축 두 종류 분리: 일/주(제작PD·CP·서비스운영) vs 분기(편성PD·플랫폼전략)
4. **"무슨 지표 봐야 할지 모르겠다"** 반복 등장 → 떠먹이기 디폴트 필수
5. 사일로 두 곳: 마케팅·서비스운영
6. 제작PD의 횡적 누락: "혼자만 인식하고 넘어가는 경우 대부분"
7. 폐쇄 루프 부재: "결정 후 다시 평가 안 함" (편성PD 명시)

**스토리라인 공통 구조:**
1. **첫 화면 = 자동 큐레이션 한 줄** — 사용자가 "뭐 봐야 할지" 고민 없이 답 미리 제시
2. **Q&A 2~5단계 = 칩 클릭 + 자유 입력** — 인터뷰의 우선 탐사를 칩으로 사용
3. **자발적 알림** — 사용자가 묻지 않은 것을 RAAS가 먼저 던짐 (횡적 누락 해결)
4. **마무리 = 보고/공유로 종결** — 카톡·메일·PNG·PPT 변환

**사유:**
- [project_role_personas memory](../CLAUDE.md): "데이터는 자유 탐색, 나머지 6직무는 떠먹이기형" 원칙 인터뷰로 검증됨
- [RAAS_METRICS.md](RAAS_METRICS.md) 성공 기준(브리핑 카드 자발적 진입) 충족을 위해 카드 콘텐츠가 **직무별로 달라야 함**
- 단일 페르소나 결정 전에 8개를 다 그려본 이유: 정보 순환 구조 분석에서 CP가 허브로 드러남 → 단일 페르소나로 미리 결정했으면 놓쳤을 인사이트

**영향:**
- Week 3 브리핑 카드 콘텐츠 와이어프레임의 단일 근거
- [docs/RAAS_FLOW.md](RAAS_FLOW.md) 작성 시 직접 참조
- 핵심 페르소나 추천: **CP** — 정보 허브이며 모든 흐름이 통과
- 직무 자동 라우팅 백엔드 작업 필요 (로그인 → 직무 식별 → 디폴트 스토리라인)

**미적용 (다음 사이클):**
- `docs/RAAS_FLOW.md` 작성
- 브리핑 카드 콘텐츠 와이어프레임 (1차 페르소나 1개)
- 직무 자동 인식 + 라우팅 백엔드
- 변환 출력물 인프라(카톡/PPT/광고주 양식)
- 데이터(apollo) 자가 회고 1차 작성
- 추가 인터뷰 후보: 작가·진행자(횡적 공유 대상), 국장·본부장(상향 보고 수신자)

---

### D-008 — 인터뷰 가이드 v2: 직무별 1페이지 패키지 분리

**변경 대상:** `docs/RAAS_INTERVIEW_GUIDE.md` + 신규 `docs/interviews/` 폴더

**변경 전:**
- 단일 마스터 문서에 5명(PD×2 / 편성팀장 / CP / 마케팅) 권장안 + 공통 질문 6개
- 모든 직무가 동일 질문 표현, 동일 신호 신호 항목
- 인터뷰 시 사용 자료가 단일 긴 문서 → 출력해 들고 다니기 불편
- 7직무 페르소나 원칙([project_role_personas memory](../CLAUDE.md))이 반영되지 않음
- 데이터 직무(apollo)는 누락

**변경 후:**
- 마스터 문서는 **목적·원칙·노트 템플릿**의 단일 출처로 유지 (v2 헤더 추가)
- `docs/interviews/` 폴더에 직무별 1페이지 가이드 8개 신설
  - `01_pd_program.md` — 제작PD
  - `02_pd_schedule.md` — 편성PD
  - `03_cp.md` — CP (RACI Accountable)
  - `04_platform_strategy.md` — 플랫폼전략 (미래·성장)
  - `05_general_management.md` — 총괄관리 (현재·운영 건강)
  - `06_marketing.md` — 마케팅 (외부 시점)
  - `07_data_self.md` — 데이터 자가 회고 노트 (apollo)
  - `08_service_operations.md` — 서비스 운영 (앱·서비스 안정성·CS·장애 대응) **— v2.1에서 추가**
- `docs/interviews/README.md` — 공통 원칙·도입/마무리 스크립트·5명 조합 권장안·인덱스
- 각 1페이지는: 페르소나 가설 → 도입 후 첫 한마디 → Q1~Q6 (본 질문 + 직무별 추가 탐사 + 직무별 듣고 싶은 신호) → 페르소나 결정 시그널 체크리스트

**사유:**
- [project_role_personas memory](../CLAUDE.md)의 7직무 원칙(데이터=Analyst 별격, 플랫폼전략 vs 총괄관리 분리, 제작PD·CP·총괄관리 = 1템플릿+scope)이 v1 단일 문서에 반영되지 않음
- 실전 인터뷰는 한 손으로 1페이지를 잡고 진행하는 게 자연스러움. 30분 인터뷰에 긴 문서 스크롤은 인지 부담
- 직무별 "듣고 싶은 신호"가 다르게 정의되어야 5명 결과 비교 시 페르소나 차이가 드러남
- 데이터 직무는 외부 인터뷰가 부적합(시스템 제작자 본인) → 자가 회고 노트로 대체. 5명 인터뷰의 편향 검사 대조군 역할

**구현 위치:**
- `docs/RAAS_INTERVIEW_GUIDE.md` 상단에 v2 포인터 헤더 추가
- `docs/interviews/README.md` 신설
- `docs/interviews/01_pd_program.md` ~ `07_data_self.md` 신설

**영향:**
- 5명 인터뷰 진행 방식 변경 — 인터뷰어가 해당 직무 1페이지를 미리 출력해 사용
- 페르소나 결정 단계 강화 — 직무별 시그널 체크리스트가 5명 합산 후 핵심/부차/지연 페르소나 결정 근거가 됨
- 5명 권장 조합 2가지 제시 (조합 A: 운영 중심, 조합 B: 전략 중심) → PoC 기간 한정 자원 안에서 의도된 선택 가능
- 데이터 직무 자가 회고는 5명 인터뷰 **시작 전** 1차 작성 → 본인 시점 편향 검사 대조군

**미적용 (다음 사이클):**
- 인터뷰 일정 요청 메일 템플릿
- 페르소나 결정 시 친화도 매트릭스 (직무 × 6질문 신호) 양식
- `docs/interviews/notes/` 폴더(인터뷰 직후 노트 저장소) 생성

**v2.1 후속 보강:**
- `08_service_operations.md` 추가 — 청취 데이터와 서비스 운영 데이터의 사일로 진단 시점 확보
- README 권장 조합 C(통합 진단 중심) 추가 — 콘텐츠 vs 서비스 책임 구분 시 필수
- 사유: 7직무 페르소나 메모리는 콘텐츠·전략·외부 시점 중심이라 **서비스 안정성 시점**이 누락되어 있었음. RAAS의 이상신호(s7_anomalies) 진단 품질을 높이려면 운영 시점이 필요

---

### D-007 — Track C 접근성 베이스라인 도입 (focus-visible + ARIA + placeholder 토큰)

**변경 대상:** `raas_web.html` + 신규 `docs/RAAS_INTERACTION_STATES.md`, `docs/RAAS_ACCESSIBILITY.md`

**변경 전:**
- 글로벌 focus 스타일 정의 없음 → 키보드 사용자가 현재 포커스 위치 추적 불가
- SVG only 아이콘 버튼(`#btnSend`, `#btnToggleSidebar`, `#btnToggleKpi`, `#btnCloseKpi`, `#btnCloseKpiMobile`) ARIA 라벨 없음
- `#chatInput` textarea placeholder만 있고 `aria-label` 없음
- 인증 메시지 (`#authLoginMsg`, `#authRegMsg`) live region 없음 → 스크린리더 통지 X
- 환영 화면 "안녕하세요" 헤딩이 `<div>` (시맨틱 부재)
- 인증 버튼이 비동기 요청 중 텍스트가 그대로 → 진행 상태 모호
- placeholder 색이 `--dim`(#4a5566, 2.1:1) → WCAG AA 미달

**변경 후:**
- 글로벌 `:focus-visible` 룰 적용 (Tab 키일 때만 outline, 마우스 클릭엔 안 보임)
- 5개 SVG 아이콘 버튼에 `aria-label` 추가, 내부 svg에 `aria-hidden="true"` 적용
- `#chatInput`에 `aria-label="질의 입력"` 추가
- `#authLoginMsg` / `#authRegMsg`에 `role="status" aria-live="polite"` 추가
- 환영 화면 헤딩 `<div class="welcome-logo">` → `<h1 class="welcome-logo">` (2곳, 정적 + btnNewChat 동적)
- `doLogin` / `doRegister`: 요청 중 `btn.textContent = '로그인 중…' / '가입 요청 중…'`, finally에서 원복
- 신규 토큰 `--color-text-placeholder: var(--sub)` (4.9:1) 신설, `.input-box::placeholder`에 적용
- `--color-text-disabled`는 그대로 `--dim` (WCAG §1.4.11 disabled 면제)

**사유:**
- [docs/RAAS_INTERACTION_STATES.md](RAAS_INTERACTION_STATES.md) 6패턴 중 hover/focus는 가장 빈번한 인터랙션이지만 RAAS에 전무 — 가장 큰 UX 격차
- [docs/RAAS_ACCESSIBILITY.md §2](RAAS_ACCESSIBILITY.md) 대비비 점검에서 `--dim` placeholder가 dark/light 양 테마 모두 AA 미달
- Track C 4주 PoC 목표는 "분석가가 일과에 자연스럽게 RAAS를 끼워 넣는 단계" — 키보드 전용 사용자(분석가 중 흔함) 차단되면 안 됨
- ARIA 라벨은 스크린리더뿐 아니라 음성 제어, AI 보조도구의 인지에도 사용됨
- 시맨틱 `<h1>`은 SEO/스크린리더 페이지 구조 인지에 필수

**구현 위치:**
- `raas_web.html:112-129` 글로벌 focus-visible 룰
- `raas_web.html:56-60` semantic 토큰에 `--color-text-placeholder` 추가
- `raas_web.html:853` `.input-box::placeholder` 토큰 교체
- `raas_web.html` 아이콘 버튼 5곳 + textarea 1곳에 `aria-label` 추가
- `raas_web.html` `#authLoginMsg` / `#authRegMsg`에 live region 추가
- `raas_web.html` welcome-logo `<h1>` 시맨틱 변경 (2곳)
- `raas_web.html` `doLogin` / `doRegister` 버튼 텍스트 복원

**영향:**
- 시각 변화 없음(focus-visible은 키보드 사용 시에만, h1 디폴트 margin은 전역 `*{margin:0}` 리셋이 흡수)
- Lighthouse Accessibility 점수 상승 예상 (P1 핵심 누락 4건 해결)
- 향후 새 컴포넌트는 [docs/RAAS_ACCESSIBILITY.md §9](RAAS_ACCESSIBILITY.md) 체크리스트 통과 후 PR 머지

**미적용 (P2/P3 백로그):**
- auth-tabs / 모달 ARIA tabs/dialog 패턴
- `#chatThread`의 `role="log" aria-live="polite"`
- `prefers-reduced-motion` 대응
- 차트 svg `role="img" aria-label`

---

### D-006 — `view_briefing` 트리거 표면 전환: auto-render → user-triggered

**변경 대상:** `raas_web.html` 브리핑 카드 렌더 시점 + 사이드바 빠른 질의

**변경 전:**
- 로그인/세션복원/가입 자동 승인/새 대화 4곳에서 `_renderBriefingCard('threadInner')` **자동 호출**
- 카드가 환영 화면 상단에 **항상 노출**
- `view_briefing` 이벤트가 페이지 진입 시 발사

**변경 후:**
- 자동 호출 4곳 모두 제거
- 사이드바 빠른 질의 **첫 항목으로 "이번 주 브리핑 보여줘" 추가** (`BRIEFING_QUERY_TEXT` 상수)
- 클릭 시 `_onQuickQuery`가 특수 분기 → Claude 호출 없이 챗 메시지로 카드 임베드 (`_showBriefingInChat`)
- `view_briefing` 이벤트는 **사용자가 의도적으로 클릭한 후**에만 발사

**사유:**
- [docs/RAAS_METRICS.md §4.1](RAAS_METRICS.md) 성공 기준의 단어는 "5명 중 3명 이상이 **자발적으로** 브리핑 카드를 연다"
- 자동 노출은 "들어오니까 보임" → **자발적 진입이 아님**
- 클릭 진입은 명세의 단어 "자발적"에 충실
- D3 결정 신호(`ask_ai(briefing) / view_briefing`)도 더 정확한 의미를 가짐:
  - 분모(view_briefing) = 의도 있는 사람
  - 분자(ask_ai briefing) = 그중 행동까지 이어진 사람
  - 비율 = "브리핑 본 사람 중 챗으로 이어진 비율" (이상적 D3 정의)
- 첫 화면 시각 정리: 환영 메시지 + 추천 칩만 남아 채팅 인터페이스 정체성 강화

**구현 위치:**
- `raas_web.html` 자동 호출 제거: `_bootAuth` 성공 / `doLogin` 성공 / `doRegister` 부트스트랩 자동 로그인 / `btnNewChat` 클릭 핸들러
- `raas_web.html` `QUICK_QUERIES`에 `BRIEFING_QUERY_TEXT` 첫 항목 추가
- `raas_web.html` `_onQuickQuery`에서 `q === BRIEFING_QUERY_TEXT` 시 `_showBriefingInChat()` 호출 (return)
- `raas_web.html` `_renderBriefingCard` 계열을 다중 인스턴스 지원으로 리팩터링 (고유 ID + `data-stat` 셀렉터)

**영향:**
- NSM(WAAU) 정의 변경 없음. 측정 분모만 감소 가능 → "노출" → "자발적 진입"으로 의미 강화
- 4주 PoC 성공 기준(5명 중 3명) 허들이 약간 높아짐 — 의도된 비용
- 신규 사용자 첫 진입 시 브리핑 가치 즉시 못 봄 → 사이드바 첫 항목으로 발견 가능성 확보 (시각 강조는 별도 추가하지 않음, 평범한 항목으로 유지)
- `_RAAS_BRIEFING_FIRED` 세션 dedupe 그대로 동작 — 클릭 여러 번 해도 view_briefing은 1회

---

### D-005 — 디자인 토큰 3-tier 체계 도입

**변경 대상:** `raas_web.html` `:root` CSS 변수 + 신규 `docs/RAAS_DESIGN_TOKENS.md`

**결정:**
- 토큰 체계 3층화: **Primitive → Semantic → Component**
- Primitive (기존 `--bg0..5`, `--accent`, `--green` 등) 유지
- Semantic 신규 추가 — `--color-bg-*`, `--color-text-*`, `--color-success/danger/warning/info`, `--color-interactive*`
- Spacing 8단계 (`--sp-1..--sp-8`), Radius 5단계 (`--rad-sm..--rad-pill`), Motion (`--ease-*`, `--dur-*`) 신규
- 새 컴포넌트는 Semantic 레이어만 참조

**사유:**
- 현재 `:root`에 Primitive만 있어서 새 컴포넌트가 의도 없이 `--green` 등을 가져다 씀 → 라이트/다크 외 다른 컨텍스트(예: 성공 ≠ 항상 green)에 취약
- Week 3 브리핑 카드 실콘텐츠·9종 차트 도입 시 일관된 시각 언어 필요
- Spacing/Radius/Motion은 현재 인라인 — 일관성 위협, 표준화 필요

**부수 효과:**
- 다크/라이트 테마 외 향후 추가 테마(고대비 등)에도 확장 가능
- 차트 9종이 같은 시맨틱 토큰 참조 → AI 응답·차트·UI가 한 톤
- 코드 리뷰 시 의도 추적 가능

**구현 위치:**
- `docs/RAAS_DESIGN_TOKENS.md` 신규 (12절 종합 가이드 + 레퍼런스 보드)
- `raas_web.html` `:root` 블록에 Semantic + Spacing + Radius + Motion 추가 (additive, 기존 변수 유지)

**영향 범위:**
- 기존 컴포넌트 무영향 (Primitive 그대로 계속 동작)
- 점진 마이그레이션: 새 컴포넌트는 Semantic만, 기존은 손대지 않음
- 부채는 §11 "알려진 부채" 절에 기록

---

### D-002 — PostHog 키 노출 방식: 서버 endpoint

**변경 대상:** 신규 결정 (`docs/RAAS_EVENTS.md` §5.1 SDK 로드 방식)

**결정:**
- `POSTHOG_KEY` / `POSTHOG_HOST`를 `.env`로 관리
- `GET /api/posthog-config` 라우트가 `{ enabled, key, host }` 반환
- 브라우저는 부팅 시 이 endpoint를 fetch → enabled면 `posthog.init` 호출
- 키 없으면 enabled=false → SDK init 스킵, `trackEvent()` no-op (기존 동작 무손실)

**대안:**
- (A) HTML 템플릿 치환: 서버가 HTML 읽을 때 `__POSTHOG_KEY__` 같은 플레이스홀더를 치환. 정적 캐싱 어려움
- (B) 키 하드코딩: 4주 PoC라 간단하지만 .env 표준에 어긋남

**채택 사유:**
- HTML 정적 캐싱 가능, .env 단일 진실 소스 유지, 키 회전 시 코드 변경 0
- Project API Key는 공개 키라 endpoint 응답으로 노출되어도 보안 위협 없음
- Personal API Key(서버 관리용)는 이 endpoint를 통해 노출되지 않도록 절대 `.env`에 함께 두지 않음

---

## 변경 정책 (재확인)

- `docs/RAAS_METRICS.md`와 `docs/RAAS_EVENTS.md`는 **4주가 끝날 때까지(2026-07-12) 잠금**
- 다음 경우만 변경 허용:
  - 이벤트가 의도와 다르게 발사됨이 발견된 경우
  - 새 표면(카드 종류·추천 칩 등) 추가로 속성/enum 값이 필요한 경우
  - 페르소나 가정이 인터뷰로 명백히 잘못된 것이 드러난 경우
- 모든 변경은 이 문서에 `D-NNN` 번호로 기록
