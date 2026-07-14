# RT-1 — 실시간 통합 접근자 설계 (`rt_series`)

## 목표
실시간(분단위) 조회를 **하나의 접근자**로 수렴시켜, 흩어진 분기(오늘·과거일·프로그램·채널·
디바이스·성별/연령·추출·해상도)를 없앤다. 새 질의 유형 = 접근자 호출 한 줄(데이터 접근 코드
재작성 없음). 일간 `raas_series`의 실시간 짝.

원칙(불변): 접근자는 **구조 어댑터 + 필드 템플릿**만. 도메인 규칙(동시방송 공리·인증 분모)은
온톨로지. 질의 해석(무엇을 원하는가)은 상위 플래너(P-1) 몫 — 접근자는 **구조화된 요청**을 받는다.

## 왜 별도 접근자인가 — 일간과의 형태 차이
- 일간: `{code:{date:row}}` — 엔티티(code)가 키, 지표는 필드, 시간(date)이 키.
- 실시간: `[{_time, field:val}]` — **시간이 키, 엔티티+지표는 필드명에 인코딩**, code 차원 없음.

따라서 실시간 접근자의 일은: 의미 요청(지표·대상·기간·해상도·차원) → ① 소스 선택
② **필드명 조립** ③ 시간 fetch → 균일 `[(시각, 값)]` 반환.

## 필드 네이밍 (2026-07, 실측 확인) — 템플릿이 규칙적
| 지표 family | 소스 | 필드 템플릿 | 대상(ch) | 차원 |
|---|---|---|---|---|
| 동시사용자 | rt_concurrent | `{ch}` | T00·F00·L00·G00·P00 | — |
| 디바이스별 | rt_concurrent | `DV_{dev}` / `DV_{dev}_{ch}` | 전체/채널 | device(AI·SP·PC·PW·MWEB·WATCH·CAR) |
| 보는라디오 | rt_concurrent | `BA` / `BA_{ch}` | 전체·F00·L00 | — |
| 인증 성별비율 | rt_concurrent | `R_{ch}_SEX_{s}` | F00·L00(인증채널) | sex(F/M) |
| 인증 연령비율 | rt_concurrent | `R_{ch}_AGE_{a}` | F00·L00 | age(0_19…60) |
| 인증자수(분모) | rt_concurrent | `{ch}_REALINFO` | F00·L00 | — |
| 분당 문자 | rt_msg | `{ch}_SMS` | F00·L00 | — |
| 분당 공감로그 | rt_msg | `{ch}_GG` | F00·L00 | — |
| 분당 유입 | rt_inflow | `{ch}_START` | F00·G00·L00·P00 | — |

**대상=프로그램**이면: 온톨로지로 채널+편성창 해석(`_rt_resolve_target` 재사용) → 그 채널 필드 +
편성창 시간필터. 편성형 채널 동시방송 공리(편성창 내 채널 동시청취 = 프로그램 동시청취)가 근거.

## 아키텍처 — 3부
### ① RT 메트릭 레지스트리 (필드 템플릿, 구조 선언)
```
RT_METRICS = {
  "concurrent": RTMetric(source="rt_concurrent", tmpl="{ch}", chans=ALL5, dims=[]),
  "device":     RTMetric(source="rt_concurrent", tmpl="DV_{device}{ch_suf}", dims=["device"]),
  "viewradio":  RTMetric(source="rt_concurrent", tmpl="BA{ch_suf}", chans=[T00,F00,L00]),
  "sex_ratio":  RTMetric(source="rt_concurrent", tmpl="R_{ch}_SEX_{sex}", chans=[F00,L00], auth=True),
  "age_ratio":  RTMetric(source="rt_concurrent", tmpl="R_{ch}_AGE_{age}", chans=[F00,L00], auth=True),
  "msg_sms":    RTMetric(source="rt_msg", tmpl="{ch}_SMS", chans=[F00,L00]),
  "msg_gg":     RTMetric(source="rt_msg", tmpl="{ch}_GG", chans=[F00,L00]),
  "inflow":     RTMetric(source="rt_inflow", tmpl="{ch}_START", chans=[F00,G00,L00,P00]),
}
```
새 실시간 지표 = 여기 한 줄(코드 로직 X). 온톨로지 RTMetric 정의(fields.ttl)와 1:1.

### ② 시간 fetch — `_rt_rows(source, when)` (datasource)
- `when="today"|"now"` → 기존 60초 캐시 Feed(get_rt_concurrent/msg/inflow) **재사용**(광역 필드).
- `when="YYYY-MM-DD"`(과거) → 그 날짜를 **광역 필드로** 온디맨드 조회(현 `get_rt_history`를
  narrow→wide 일반화), (source,date) 캐시. 보관 범위는 `get_rt_earliest`.
- `when="yesterday"|"lastweek"` → 기존 Feed.
- datasource 변경: RT SPL을 날짜 창으로 파라미터화(`_build_rt_concurrent_spl`을 earliest/latest
  인자화), 범용 `rt_fetch(source, earliest, latest)` 노출.

### ③ 공개 API (raas_rt_series.py)
```
rt_series(metric, target=None, when="today", resolution=1, dims=None) -> [(hhmm, value)]
    # 단일 지표·대상의 분단위 시계열. resolution=10이면 10분 그리드, 프로그램이면 편성창 필터.
rt_table(metric, targets=None, when="today", resolution=1) -> {header, rows}
    # 다열(채널들 또는 대상들) 표 — 추출/스냅샷용. resolution=1 원자료.
rt_available(when) -> {ok, earliest, reason}   # 보관 범위 확인
rt_resolve(question) -> {metric, target, when, resolution, dims}  # (임시) 키워드 파서 — P-1에서 LLM으로 대체
```
`target`: 채널코드 | 프로그램코드(→채널+편성창) | None(전체/전채널). `resolution`: 1|10(분).
`dims`: {device, sex, age} 등 필드 템플릿 슬롯.

## 소비자(consumer)가 얇아진다
| 경로 | 지금(흩어짐) | RT-1 이후 |
|---|---|---|
| 오늘 추이 | `_assemble_realtime` 내 인라인 필터/다운샘플 | `rt_series("concurrent", ch, "today", res)` |
| 과거일 차트 | `_rt_history_branch` 인라인 | `rt_series("concurrent", tgt, date, res)` |
| 엑셀 추출 | `_build_extract_realtime` 인라인 | `rt_table("concurrent", None, when, 1)` |
| 스냅샷/디바이스/성별 | `snap`·`_prof` 인라인 필드조립 | `rt_series/table(metric, …)` |
포매터(차트 다운샘플 / 엑셀 원자료 / 스냅샷 최신행)는 접근자 위 **얇은 층**. 해상도·형식이 파라미터.

## 지식 배치 (원칙 준수)
- **필드 템플릿·소스·차원** = RT 레지스트리(구조, 코드) — daily의 raas_metrics_registry와 동형.
- **의미·공리**(동시방송·인증 분모=인증자합계·보라는 별도 축 합산금지) = 온톨로지(fields.ttl·program.ttl).
- 커버리지 맵: RT 레지스트리도 반영(교차관계는 분 축이라 별개).

## 단계
- **RT-1a**: `raas_rt_series.py`(레지스트리+rt_series/rt_table) + datasource `rt_fetch` 광역·파라미터화.
  스모크: 접근자 값이 현 인라인 결과와 일치(회귀=답변 불변).
- **RT-1b**: 4개 소비자를 접근자 위임으로 전환(오늘·history·extract·스냅샷). 인라인 필드조립 제거.
- **RT-1c**: 디바이스·성별/연령 **과거일**까지 개방(광역 history 덕에 자동) + 추출 다열.

## 경계·리스크 (정직)
- **질의 해석은 접근자 밖**(P-1 플래너). RT-1의 `rt_resolve`는 임시 키워드 파서 — P-1에서 LLM으로 교체.
- **데이터 한계 노출**: 성별·연령은 **인증자 비율**만 존재(동시자 헤드카운트 아님). "여성 동시자 수"는
  없음 → 접근자가 있는 것(sex_ratio)만 주고, 답변이 한계를 명시(날조 금지).
- **보관 유한**(분단위 ~수개월, `get_rt_earliest` 동적). 그 이전 규모는 일간 아카이브.
- **광역 과거 조회 비용**: 하루 wide ~1.8만 이벤트 — 가벼움. (source,date) 캐시로 반복 방지.
- 회귀 안전: RT-1b 전환 시 소비자 출력이 한 글자도 안 바뀌는지 스모크로 고정.
