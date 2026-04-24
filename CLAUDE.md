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
| `CLAUDE_MODEL` | 사용 모델 (예: `claude-sonnet-4-6`) |
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
