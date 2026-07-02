# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RAAS (Radio As A Service) — SBS 고릴라 라디오 앱의 데이터 분석 및 AI 브리핑 시스템.
Splunk에서 청취 KPI 데이터를 가져와 Claude API로 분석하고, 브라우저 대시보드에 제공하는 로컬 프록시 서버.

## Running the Server

```bash
# 로컬 프록시 서버 실행 (포트 5000)
python raas_server.py

# 자연어 질의 CLI (단독 실행)
python raas_query_engine.py "어제 DAU는?" --verbose
python raas_query_engine.py --demo   # 샘플 5개 질의 실행
python raas_query_engine.py "최근 트렌드" --date 2024-04-20
```

## Environment Variables (`.env`)

필수 환경변수 — `python-dotenv`로 로드:

| 변수 | 설명 |
|------|------|
| `SPLUNK_HOST` | Splunk REST API URL (예: `https://10.x.x.x:8089`) |
| `SPLUNK_USER` / `SPLUNK_PASSWORD` | Splunk 인증 |
| `SPLUNK_APP` | Splunk 앱 내부 ID (예: `gorealra_v4`) |
| `ANTHROPIC_API_KEY` | Claude API 키 |
| `CLAUDE_MODEL` | 답변 생성 모델 (현재: `claude-sonnet-4-6`) |
| `HAIKU_MODEL` | provider 선택·judge 등 경량 작업 모델 (`claude-haiku-4-5-...`) |
| `MAX_ANSWER_TOKENS` | 답변 max_tokens 상한 (기본 8000, 천장이며 모델이 끝나면 조기 종료) |
| `GMAIL_ADDRESS` / `GMAIL_APP_PW` | 이메일 발송용 Gmail 앱 비밀번호 |

## Architecture (LLM-first grounding)

핵심 방향: **모든 자유질의는 grounding 엔진이 답한다** — Splunk 데이터 × 온톨로지 × 사용자 오버레이를
LLM context에 골라 넣고 LLM이 본연의 성능으로 답변(고정 출력 템플릿 없음).

```
브라우저 (raas_web.html, 챗 UI)
    ↓ HTTP (CORS 해결)
raas_server.py  ← 진입점, ThreadingHTTPServer on port 5000
    ├── POST /api/query/stream → [핵심] 자유질의 SSE 스트리밍
    │       1) 편성표 의도면 STORY.build_program_schedule (룩업)
    │       2) GROUND.assemble(question) → ok면 grounding 답변 스트리밍
    │       3) 미식별이면 raas_query_engine(구 QE)로 폴백
    ├── POST /api/storyline/today → '어제 방송 특이사항' digest (grounding)
    ├── POST /api/improve/* · /api/knowledge/* → 지식 개선 루프
    ├── GET  /api/briefing · /api/suggestions · /api/query/history/* 등
    └── GET  /                → raas_web.html 정적 서빙

raas_grounding.py  ← [핵심] 검색·Grounding 레이어 (단일 답변 엔진)
    resolve_entities → scope 판별(program/channel/compare/ranking) → provider 선택(Haiku)
    → fetch(구조화 데이터) → 온톨로지 팩 + 사용자 오버레이 병합 → GROUNDING_SYSTEM으로 LLM 생성

raas_storyline_engine.py  ← grounding이 재사용하는 데이터 computer 모음
    _compute_flow_decomposition / _compute_cohort / _compute_stickiness /
    _compute_programming_impact / _compute_weekday_pattern_check / _detect_program_revision /
    build_program_schedule / build_query_provenance / _metric_definitions_lines / _query_decompositions
    (구 CP 다중슬롯 스토리라인 오케스트레이션은 은퇴·제거됨)

raas_query_engine.py  ← 폴백 엔진(거래성·메타 질의) + collect_briefing_data(s1~s7) + timeline 캐시
raas_onto/raas_ontology_adapter.py  ← TTL 온톨로지 8종 로더(지표/프로그램/게스트/특일/cause 등)
raas_history_db.py  ← SQLite: query_history·knowledge_items·improvements·data_requests·storyline_events
```

### 자유질의 답변 엔진 — scope (raas_grounding.assemble)
`assemble(question)`이 질문을 scope로 분기. 모든 scope가 동일 엔진(GROUNDING_SYSTEM·온톨로지·오버레이) 사용:

| scope | 트리거 예시 | 데이터 |
|-------|------------|--------|
| **program** | "컬투쇼 어제 왜 빠졌어?" | provider 10종(KPI·시계열·흐름분해·코호트·편성·요일·개편·편성표·특일) |
| **channel** | "러브FM 어때?", "전사 트렌드" | 채널행(F00/L00/G00/P00/T00) — 프로그램 전용 provider 제외 |
| **compare** | "파워FM vs 러브FM 비교" | 엔티티 2~4개 KPI·시계열 나란히 |
| **ranking** | "프로그램별 DAU 순위" | `_kpi_rows()`에서 최신일 전 프로그램 지표 정렬 |
| **digest** | "어제 방송 특이사항"(스토리라인 단일경로) | z-score 이상탐지(get_cached_anomalies) |
| (폴백) | "어제 MAU는?"(거래성), "어떤 지표 있나"(메타) | raas_query_engine |

- provider = `{name, needs, desc, fetch}`. LLM(Haiku)이 질문에 맞는 provider만 선택(`select_providers`).
- 온톨로지 팩: 지표 정의 + cause 분해 프레임워크. 오버레이: 사용자 기여 지식(read-time 병합).
- 관리자에게는 응답 끝에 `[small]` 참고 푸터(사용 provider·적용 오버레이)를 SSE로 덧붙임.

### 스토리라인 = '어제 방송 특이사항' 단일 경로
구 CP 다중슬롯 스토리라인(advance/슬롯/칩)은 **제품에서 은퇴**. 현재 스토리라인은 웰컴 CTA
`🗓 어제 방송 특이사항 보기` → `/api/storyline/today`(grounding digest) 하나. 결과의 특이 프로그램
드릴다운 칩은 일반 자유질의(통합 grounding 경로)로 흡수.

### 지식 개선 루프 (raas_history_db + raas_grounding + 검토 큐)
'아쉬움(👎)' → 개선하기 → 기여(온톨로지 즉시 / 데이터 요청형) → 재질의(A/B + 다수결 judge) →
검토 큐 승인 → 공유 오버레이(본체) 반영. candidate(본인)/approved(공유) 격리, read-time 병합.
- `knowledge_items`(scope=candidate|approved), `improvements`(judge_json·user_verdict·status),
  `data_requests`(요청형). 거버넌스 권한: is_admin OR role∈{총괄관리, 데이터}.
- 검토 큐: 개선 승인·반려, 데이터 요청 처리, **약점 신호**(👎 집계·미개선 질의),
  **승인된 공유 지식(본체)** 관리·회수.

## 프론트엔드 구조 (raas_web.html + raas_web.js)

HTML(마크업+CSS) + 분리된 메인 JS(`raas_web.js`, 서버가 `/raas_web.js?v=버전`으로 서빙) — **챗 인터페이스**
(구 5페이지 SPA는 제거됨). 둘 다 디스크 서빙이라 수정 시 서버 재시작 불필요. `_app_version()`=두 파일 mtime 중 최신:

```
사이드바          | 빠른 질의(QUICK_QUERIES) · 최근 질의 · 내 정보 · 이력 보기
chat-main        | 헤더(사이드바·KPI 토글) · 웰컴(CTA '🗓 어제 방송 특이사항 보기' + 추천칩)
                 | · chat-thread(질의/답변 스트리밍) · 입력창(#chatInput)
KPI 패널         | 우측 토글 패널(loadKpiPanel) — 핵심 지표 스냅샷
```

- 질의 진입: `submitQuery(question, source, opts)` — opts.endpoint로 `/api/storyline/today` 등 전환.
  답변은 SSE 토큰 스트리밍 → `renderAiText`(굵게·표·`[small]`푸터 지원).
- 추천칩(`/api/suggestions`): ① 어제 이상신호(직무 필터) → ② 개인화 → ③ 직무 템플릿 순 6개.
- 모달 5종: `profileModal`(내 정보) · `histModal`(이력 보기) · `improveModal`(개선하기) ·
  `adminModal`(관리자) · `statsModal`(질의맵). 이력 보기 탭: 전체/일반질의/개선이력/**검토 큐**(직무권한).
- 관리자 메타 토글: `body.hide-meta`로 답변 `[small]` 참고 푸터 표시/숨김.

## Splunk Lookups

코드 전반에서 사용하는 Splunk lookup 테이블:

- `data/raas_kpi_latest.csv` — PGM_CODE별 일간/주간/월간 KPI 전체 (briefing/query engine 로컬 fallback)
- `raas_top_programs_latest.csv` — 프로그램 순위 (rank, pgm_name, channel, dau)
- `raas_briefing_latest.csv` — 집계된 일간 요약 단일 행
- `raas_llm_context_day.csv` — 날짜별 트렌드 히스토리 (query engine 트렌드/비교 쿼리)

**로컬 CSV 경로 우선순위** (Splunk 미접속 환경):
1. `data/raas_kpi_latest.csv` (권장)
2. `raas_kpi_latest.csv` (구 위치, 호환용 fallback)

## Briefing Sections (raas_query_engine.collect_briefing_data)

`collect_briefing_data(timeline)` 반환 딕셔너리의 7개 섹션 (s7 이상탐지는 grounding digest가 재사용):

| 키 | 내용 |
|----|------|
| `s1_executive` | DAU/WAU/MAU, 신규/복귀 |
| `s2_funnel` | D1/D7 리텐션, 이탈율, 복귀율 |
| `s3_engagement` | 깊은청취율(10분이상/1분이상), 참여율 |
| `s4_growth` | 습관형성률, TOP3 프로그램 |
| `s5_rankings` | DAU TOP10, 깊은청취 TOP5, 리스크 프로그램 |
| `s6_channels` | 파워FM/러브FM/고릴라M/픽채널 채널별 지표 |
| `s7_anomalies` | 자동 이상 알림 (red/yellow/green) |

## Program Code Conventions

프로그램 매핑: `raas_storyline_router.PROGRAM_DIRECTORY` + `extract_program(text)` (질문→프로그램),
채널 매핑: `raas_storyline_engine._CHANNEL_CODE`, 코드→이름: `adapter.get_program_meta(code)`:
- `T00` = 전체, `F00` = 파워FM, `L00` = 러브FM, `G00` = 고릴라M, `P00` = 픽채널
- `F01`~`F13` = 파워FM 프로그램, `L01`~`L15` / `M05`~`M11` = 러브FM 프로그램
- 채널/집계 코드는 X00 패턴(ranking scope에서 제외)

## Key Metrics Definitions

- **DAU**: 청취시간 > 0인 고유 사용자 수
- **깊은청취율**: 10분 이상 연속 청취 / 1분 이상 연속 청취 비율 (몰입도)
- **WoW**: 전주 동일 요일 대비 증감률
- **습관형성률**: 신규 사용자 중 7일 내 재방문 비율
- **기간 평균(전주/전월 일평균 등)**: 룩업에 저장 필드 없음 → grounding이 제공한 **일별 시계열에서 LLM이 계산**(평균은 저장하지 않고 파생). 과거 모델링했던 dau_week_avg/dau_mon_avg/wau_mon_avg 변형은 실 데이터에 없어 제거됨.

## 최근 주요 변경사항 (2026-04 기준)

### raas_kpi_latest.csv 신규 필드
- new_d1_ret / new_d1_ret_pw / new_d1_diff: 신규코호트 D1유지율
- new_d7_ret / new_d7_ret_pw / new_d7_diff: 신규코호트 D7유지율
- new_w1_ret / new_w1_ret_pw / new_w1_diff: 신규코호트 W1유지율
- new_m1_ret / new_m1_ret_pw / new_m1_diff: 신규코호트 M1유지율

### Briefing Engine 섹션 추가 필드
s2_funnel 추가:
- new_d1_ret / new_d7_ret / new_w1_ret / new_m1_ret (신규 코호트 유지율 4종)

s3_engagement 추가:
- wau_1min / wau_10min (주간 1분이상/10분이상 사용자)
- mau_1min / mau_10min (월간 1분이상/10분이상 사용자)
- channel_deep 각 채널에 rate_week / rate_mon 추가

### 유지율 코호트 구분
- 전체 코호트: d1_ret / d7_ret / w1_ret / m1_ret (program_user_retention_*.csv)
- 신규 코호트: new_d1_ret / new_d7_ret / new_w1_ret / new_m1_ret (program_newuser_retention_*.csv)

### 1MIN/10MIN PERIOD 조건 (중요)
- 일간: PERIOD=1D (없으면 30D롤링 값이 들어옴)
- 주간: PERIOD=1W
- 월간: PERIOD=1M

## 설계 문서 / 개발 메모
- `docs/`: grounding_retrieval_design · knowledge_loop_design · ab_harness_design ·
  storyline_cp_v2_schema · storyline_history_db_design (설계 근거)
- 서버 재시작(윈도우): 포트 5000 종료 후 `nohup python raas_server.py > server_restart.log 2>&1 &`,
  py 코드 변경 시 재시작 필요. HTML/JS는 디스크 서빙이라 재시작 불필요.
  (구 CP 스토리라인 잔재 — simulator·report_engine·tobe_sim·data/storylines/ — 는 2026-07 폐기 삭제됨)
- grounding 헤드리스 테스트: `import raas_grounding as G` → `G.assemble(q)`(LLM 없이 데이터 경로 확인은
  `G.call_claude=None`). `_kpi_rows()`가 timeline 쓰려면 `G.S.set_timeline_provider(SRV.get_cached_timeline)`.
- JS 검증: `node --check raas_web.js` 직접 실행(html에서 분리됨). 큰 dead-code 제거는 ast 콜그래프 도달성으로 안전 판정.

## 접속 환경
- 회사 윈도우PC: localhost:5000 직접 접속, Splunk(10.10.15.31) 접근 가능
- 맥미니/외부: ngrok URL (https://unifier-verbose-schnapps.ngrok-free.dev)
- GitHub: https://github.com/apollo0831/raas-api

## ngrok 실행 (윈도우PC)
ngrok http 5000
