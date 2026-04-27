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
| `CLAUDE_MODEL` | 사용 모델 (현재: `claude-opus-4-7`) |
| `GMAIL_ADDRESS` / `GMAIL_APP_PW` | 이메일 발송용 Gmail 앱 비밀번호 |

## Architecture

```
브라우저 (raas_web.html)
    ↓ HTTP (CORS 해결)
raas_server.py  ← 진입점, HTTPServer on port 5000
    ├── GET  /api/briefing    → raas_briefing_engine.collect_all() → Claude
    ├── GET  /api/top_programs → Splunk inputlookup
    ├── POST /api/query       → raas_query_engine.query() (or fallback Claude)
    └── GET  /                → raas_web.html 정적 서빙

raas_briefing_engine.py  ← KPI 섹션별 계산 (s1~s7)
    └── Splunk lookups: raas_kpi_latest.csv, raas_top_programs_latest.csv

raas_query_engine.py  ← 자연어 질의 엔진 (3단계 파이프라인)
    1. classify_intent()  → Claude로 질의 의도 분류 (JSON 응답)
    2. get_data_for_intent() → 의도별 Splunk SPL 실행
    3. call_claude()      → 최종 답변 생성
```

## 대시보드 페이지 구조 (raas_web.html)

하단 네비게이션 기반 5페이지 SPA:

| page id | 네비 라벨 | 내용 |
|---------|-----------|------|
| `page-home` | 홈 | 핵심 지표(KPI), 이상 알림, 프로그램 흐름, 리텐션/품질 섹션 |
| `page-analytics` | 분석 | 채널 스코프 전환 + KPI/퍼널/코호트/품질/성장 상세 |
| `page-programs` | 프로그램 | 프로그램 랭킹 (DAU TOP, 깊은청취 TOP) |
| `page-ai` | AI | AI 브리핑 전문 텍스트 |
| `page-settings` | 더보기 | Phase 4 예정 (미구현) |

- 기간 탭(일/주/월)은 home·analytics 페이지에서만 표시 (`PERIOD_TAB_PAGES = ['home', 'analytics']`)
- 전역 상태: `_data`, `_scope` (채널코드), `_period` (day/week/mon), `_pgmTab`, `_cohortTab`

## 분석 페이지 채널 연동 (`page-analytics`)

### scope-bar 탭
```
전체(T00) | 파워FM(F00) | 러브FM(L00) | 고릴라M(G00) | 픽채널(P00)
```
`setScope(code)` → `_scope` 변경 → `renderAnalyticsPage(_data)` 재호출

### getScopeData(data, scope)
채널 스코프 선택 시 `s6_channels.channels[]`에서 해당 채널 데이터를 찾아 s1/s2/s3 형태로 재구성하는 어댑터 함수.

```js
// T00(전체)이면 원본 data 그대로 반환
// 채널 코드면 ch = s6_channels.channels.find(c => c.code === scope)
// → { s1_executive: {...}, s2_funnel: {...}, s3_engagement: {...} } 반환
```

**현재 매핑 완료 필드:**
- s1: dau, wau(=dau_week), mau(=dau_mon), new_user, new_pct, dau_week, dau_mon
- s2: dau, dau_week, dau_mon, new_user/week/mon, churn_rate/week/mon, react_rate/week/mon
- s3: dau, deep_rate/week/mon, wau_1min/10min, mau_1min/10min

**채널 스코프에서 null(미지원) 필드 (진행 중):**
- s1: dau_wow, react_user, react_pct, dau_week_wow, dau_mon_wow
- s3: dau_1min, dau_10min, deep_rate_diff, engage_rate/week/mon, channel_deep
- s4(성장 품질): 채널 스코프 무관하게 항상 전체 data.s4_growth 사용

**TODO:** P00(픽채널) s6_channels 데이터 연결, s4 채널별 스코핑

## Splunk Lookups

코드 전반에서 사용하는 Splunk lookup 테이블:

- `raas_kpi_latest.csv` — PGM_CODE별 일간/주간/월간 KPI 전체 (briefing engine 전용)
- `raas_top_programs_latest.csv` — 프로그램 순위 (rank, pgm_name, channel, dau)
- `raas_briefing_latest.csv` — 집계된 일간 요약 단일 행
- `raas_llm_context_day.csv` — 날짜별 트렌드 히스토리 (query engine 트렌드/비교 쿼리)

## Briefing Engine Sections (raas_briefing_engine.py)

`collect_all()` 반환 딕셔너리의 7개 섹션:

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

`PGM_NAMES` 딕셔너리 (briefing_engine.py):
- `T00` = 전체, `F00` = 파워FM, `L00` = 러브FM, `G00` = 고릴라M
- `F01`~`F13` = 파워FM 프로그램, `L01`~`L15` / `M05`~`M11` = 러브FM 프로그램

## Key Metrics Definitions

- **DAU**: 청취시간 > 0인 고유 사용자 수
- **깊은청취율**: 10분 이상 연속 청취 / 1분 이상 연속 청취 비율 (몰입도)
- **WoW**: 전주 동일 요일 대비 증감률
- **습관형성률**: 신규 사용자 중 7일 내 재방문 비율

## 최근 주요 변경사항 (2026-04 기준)

### raas_kpi_latest.csv 신규 필드
- dau_week_avg: 전주 월~일 7일간 DAU 일평균
- dau_mon_avg:  전월 1일~말일 DAU 일평균
- wau_mon_avg:  전월 1일~말일 7D롤링 평균
- new_d1_ret / new_d1_ret_pw / new_d1_diff: 신규코호트 D1유지율
- new_d7_ret / new_d7_ret_pw / new_d7_diff: 신규코호트 D7유지율
- new_w1_ret / new_w1_ret_pw / new_w1_diff: 신규코호트 W1유지율
- new_m1_ret / new_m1_ret_pw / new_m1_diff: 신규코호트 M1유지율

### Briefing Engine 섹션 추가 필드
s2_funnel 추가:
- new_d1_ret / new_d7_ret / new_w1_ret / new_m1_ret (신규 코호트 유지율 4종)
- dau_week_avg / dau_mon_avg / wau_mon_avg (기간별 평균 지표)

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

## 접속 환경
- 회사 윈도우PC: localhost:5000 직접 접속, Splunk(10.10.15.31) 접근 가능
- 맥미니/외부: ngrok URL (https://unifier-verbose-schnapps.ngrok-free.dev)
- GitHub: https://github.com/apollo0831/raas-api

## ngrok 실행 (윈도우PC)
ngrok http 5000
