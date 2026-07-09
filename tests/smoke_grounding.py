# -*- coding: utf-8 -*-
"""grounding 라우팅 스모크 — 질의 해석(resolve_entities)·provider 데이터 경로 회귀 확인.

LLM 호출 없음(비용 0, ~수초). 스플렁크 미접속 환경에서도 CSV 폴백으로 동작.
  python tests/smoke_grounding.py

케이스 규칙: 채팅에서 이상한 답을 발견해 고치면, 그 질문을 여기 한 줄 추가한다
("고친 버그는 다시 안 생긴다"). 새 provider/라우팅 추가 시 기대동작도 한 줄.

케이스 출처(2026-07 초기 세트): 이번 리모델링에서 실제 발견된 버그들 —
  특정일 질의가 최신일로 답하던 버그 / 게스트 일자별 retrieval 공백 /
  엔티티 없는 지표질의 QE 유실 / 개념 질의 과잉 캡처.
"""
import datetime
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, __file__.rsplit("tests", 1)[0].rstrip("\\/"))

import raas_server as SRV                    # noqa: E402 — timeline provider 준비
import raas_grounding as G                   # noqa: E402
import raas_metrics_engine as METRICS        # noqa: E402
import raas_storyline_router as ROUTER       # noqa: E402
import raas_data_check as DC                 # noqa: E402
from raas_onto import get_adapter            # noqa: E402
G.S.set_timeline_provider(SRV.get_cached_timeline)

FAIL = []


def check(name, actual, expected):
    ok = actual == expected
    print(("  PASS " if ok else "✗ FAIL ") + f"{name}  (got={actual!r}, want={expected!r})")
    if not ok:
        FAIL.append(name)


def check_true(name, cond, detail=""):
    print(("  PASS " if cond else "✗ FAIL ") + name + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


# ── 준비: 타임라인에서 '존재하는 과거 날짜'와 '범위 밖 날짜'를 동적으로 산출 ──
tl = SRV.get_cached_timeline()
t00_dates = sorted((tl.get("T00") or {}).keys())
assert t00_dates, "타임라인 비어있음 — 서버/CSV 확인"
in_date = datetime.datetime.strptime(t00_dates[-5], "%Y/%m/%d")     # 범위 내(최신-4)
out_date = (datetime.datetime.strptime(t00_dates[-1], "%Y/%m/%d")
            + datetime.timedelta(days=45))                           # 범위 밖
IN_Q1 = f"{in_date.month}/{in_date.day} 신규사용자 몇 명이야?"
IN_Q2 = f"{in_date.month}월 {in_date.day}일 신규사용자"
IN_EXPECT = in_date.strftime("%Y/%m/%d")
OUT_Q = f"{out_date.month}/{out_date.day} 깊은청취율 어때?"

print("── 1. 엔티티 해석(완화·관심 기본값) ─────────────────")
e = G.resolve_entities("신규사용자 몇 명이야?")
check("지표만(관심X) → 전사", (e.get("code"), e.get("scope_kind")), ("T00", "channel"))
e = G.resolve_entities("신규사용자 몇 명이야?", "F00")
check("지표만(관심=파워FM채널)", (e.get("code"), e.get("scope_kind")), ("F00", "channel"))
e = G.resolve_entities("신규사용자 몇 명이야?", "F09")
check("지표만(관심=컬투쇼)", (e.get("code"), e.get("scope_kind")), ("F09", "program"))
e = G.resolve_entities("컬투쇼 어제 왜 빠졌어?")
check("프로그램 명시", (e.get("code"), e.get("scope_kind")), ("F09", "program"))
e = G.resolve_entities("어제 MAU는?")
check("MAU 지표만 → 전사", e.get("code"), "T00")
# KPI 패널발 질의 — 스코프명(T00='고릴라 플랫폼 전체')을 명시하면 관심 기본값이 가로채면 안 됨
e = G.resolve_entities("고릴라 플랫폼 전체 최근 4주 DAU 추이 분석해줘", "F09")
check("패널 T00 질의, 관심 무시", (e.get("code"), e.get("scope_kind")), ("T00", "channel"))

print("── 2. 개념신호 가드(과잉 캡처 방지) ─────────────────")
for q in ["신규사용자 유치 전략 알려줘", "DAU가 뭐야?", "신규 늘리려면 어떻게 해?", "MAU 높이는 방법", "안녕하세요"]:
    check(f"개념/비데이터 통과: {q}", G.resolve_entities(q).get("code"), None)
check("엔티티 명시+전략은 유지: 컬투쇼 신규 전략",
      G.resolve_entities("컬투쇼 신규 전략").get("code"), "F09")

print("── 3. 특정 날짜 파싱(point_snapshot) ────────────────")
check(f"'{IN_Q1}' as_of", G.resolve_entities(IN_Q1).get("as_of_date"), IN_EXPECT)
check(f"'{IN_Q2}' as_of", G.resolve_entities(IN_Q2).get("as_of_date"), IN_EXPECT)
check(f"범위 밖 '{OUT_Q}' 무시", G.resolve_entities(OUT_Q).get("as_of_date"), None)
snap = G._p_point_snapshot(G.resolve_entities(IN_Q1))
check_true("point_snapshot 값 존재", bool(snap and snap.get("values", {}).get("dau")),
           f"snap={snap and list(snap)}")

print("── 4. 범용 provider(속성·프로젝션) 데이터 경로 ───────")
e = G.resolve_entities("지난주 컬투쇼 게스트 일자별로")
rows = G._p_daily_lineup(e) or []
check_true("daily_lineup 게스트 반환", any(r.get("guestname") for r in rows), f"rows={len(rows)}")
check_true("daily_lineup 상한(≤120)", len(rows) <= 120, f"rows={len(rows)}")
fp = G._p_field_projection(G.resolve_entities("컬투쇼 DAU와 게스트 일자별로")) or {}
check_true("field_projection 혼합 필드", set(fp.get("fields", [])) >= {"dau", "guestname"},
           f"fields={fp.get('fields')}")
# 채널 내 소속 프로그램 질의 — '파워FM 내 깊은청취율 하락 프로그램' 이 집계행만 받아 답 못하던 버그
cp = G._p_channel_programs(G.resolve_entities("파워FM 내 깊은청취율이 많이 하락하고 있는 프로그램 2개는 뭐야?")) or {}
check_true("channel_programs 프로그램 행 반환",
           len(cp.get("programs", [])) >= 10 and all(p.get("deep_rate") for p in cp.get("programs", [])[:3]),
           f"n={len(cp.get('programs', []))}")
check_true("channel_programs는 채널 scope 전용",
           G._p_channel_programs(G.resolve_entities("컬투쇼 DAU는?")) is None)
# '모든 프로그램'은 관심 기본값(F09)을 무시하고 전사(T00)로 — 특정일도 존중
e_all = G.resolve_entities("7월1일 모든 프로그램 복귀율 어땠어?", "F09")
check("'모든 프로그램'은 관심 무시→T00", (e_all.get("code"), e_all.get("all_programs")), ("T00", True))
_cpall = G._p_channel_programs(e_all) or {}
check_true("모든 프로그램 30+개 반환", len(_cpall.get("programs", [])) >= 20, f"n={len(_cpall.get('programs', []))}")
check_true("특정일(as_of) 반영", _cpall.get("as_of") == "2026/07/01")
# channel_programs가 특정 지표만 아니라 표준 지표(복귀·신규 등) 전체를 담아야 함
_p0 = (_cpall.get("programs") or [{}])[0]
check_true("프로그램별 복귀·신규 포함", all(k in _p0 for k in ("react", "new", "churn_rate")),
           f"keys={[k for k in ('react','new','churn_rate') if k not in _p0]} 누락")

print("── 5. 비교/순위/편성표 분기 ─────────────────────────")
check_true("compare 감지: 파워FM vs 러브FM", bool(G._detect_compare("파워FM vs 러브FM 비교")))
check_true("ranking 감지: 프로그램별 DAU 순위", bool(G._detect_ranking("프로그램별 DAU 순위")))
# 누수 수정: '채널별 ~' 는 채널 4개(F00/L00/G00/P00) compare 로 (폴백 유출 방지)
_ch_cmp = G._detect_compare("채널별 핵심 지표 비교") or []
check("'채널별' → 4채널 compare", [e["code"] for e in _ch_cmp], ["F00", "L00", "G00", "P00"])
# 누수 수정: '가장 많이 증가/감소한 프로그램' 은 변화량(_chg) 순위로
_rk_up = G._detect_ranking("지난주 활성사용자가 가장 많이 증가한 프로그램은?") or {}
check("'가장 많이 증가한 프로그램' → 변화량 순위",
      (_rk_up.get("field"), _rk_up.get("by_change"), _rk_up.get("asc")), ("dau", True, False))
_rk_dn = G._detect_ranking("DAU 가장 많이 감소한 프로그램") or {}
check("'가장 많이 감소' → 오름차순(음수 우선)", _rk_dn.get("asc"), True)
# 오탐 방지: 단일 엔티티 변화 질의는 순위가 아니다
check_true("오탐 방지: '컬투쇼 DAU 증가했어?' 순위 아님",
           G._detect_ranking("컬투쇼 DAU 증가했어?") is None)

print("── 5.1 no-entity scope 흡수 (거래성·속성·메타) ────────")
# A. 지표 미지정 날짜 스냅샷 → 전사(T00)
_a = G.resolve_entities("6월 16일 주요 데이터 보여줘", None)
check("A '6월16일 주요 데이터' → T00", _a.get("code"), "T00")
# B. 엔티티 없는 속성 질의 → 전사 + 소속 프로그램 나열
_b = G.resolve_entities("어제 게스트 누구야?", None)
check("B '어제 게스트' → T00+all_programs", (_b.get("code"), _b.get("all_programs")), ("T00", True))
# C. 메타 카탈로그 질의 감지 + assemble
check_true("C 메타 감지: '어떤 지표들을 볼 수 있어?'", G._detect_meta("어떤 지표들을 볼 수 있어?"))
check_true("C 메타 assemble ok + catalog provider",
           (G._assemble_meta("어떤 지표 있나") or {}).get("providers_used") == ["metric_catalog"])
# 오탐 방지: 값/순위/날짜 질의는 메타 아님
check_true("오탐 방지: '어떤 프로그램이 DAU 가장 높아?' 메타 아님",
           not G._detect_meta("어떤 프로그램이 DAU 가장 높아?"))
check_true("오탐 방지: '6/17 데이터 있어?' 메타 아님",
           not G._detect_meta("6/17 신규사용자 데이터 있어?"))

print("── 5.2 실시간 동시사용자 scope ───────────────────────")
check_true("실시간 감지: '지금 동시 청취자 몇 명'", G._detect_realtime("지금 동시 청취자 몇 명이야?"))
check_true("실시간 감지: '실시간 현황'", G._detect_realtime("실시간 현황 보여줘"))
check_true("오탐 방지: '지금 이상 있어?' 실시간 아님", not G._detect_realtime("지금 이상 있어?"))
check_true("오탐 방지: '어제 DAU는?' 실시간 아님", not G._detect_realtime("어제 DAU는?"))
# 채널 매핑 결정성 — tempsummary는 필드명이 곧 RAAS 코드
check("실시간 채널 매핑(필드=코드)",
      G._RT_CHANNELS,
      [("파워FM", "F00"), ("러브FM", "L00"), ("고릴라M", "G00"), ("픽채널", "P00")])
check("실시간 증감% 계산", G._rt_pct(110, 100), 10.0)
check("실시간 시각 파싱", G._rt_hhmm({"_time": "2026-07-07T08:09:00.000+09:00"}), "08:09")
# 현재 방송 프로그램 역산 — STIME 기준(라디오 24h라 항상 하나 방송 중)
_curp = G._rt_current_program("F00")
check_true("현재 방송 프로그램 역산(파워FM)",
           bool(_curp) and all(k in (_curp or {}) for k in ("name", "code", "stime", "etime")),
           f"got={_curp}")
# '지금 방송 중인 프로그램' 질의 → 편성 컨텍스트 주입(과거 '알 수 없음' 답변 회귀 방지)
check_true("'지금 방송 프로그램' → realtime_program 주입",
           "realtime_program" in (G._assemble_realtime("파워FM 지금 방송 중인 프로그램 몇명?") or {}).get("providers_used", []))
# 도메인 공리는 코드가 아니라 온톨로지에서 — 편성형 채널에만 동시방송 공리 적용
check_true("온톨로지: 편성 공리 F00 적용", len(get_adapter().get_domain_axioms("F00")) >= 1)
check_true("온톨로지: 비편성 G00 공리 없음", len(get_adapter().get_domain_axioms("G00")) == 0)
check_true("실시간 온톨로지 블록(F00)에 편성 공리 포함",
           "공리" in G._rt_ontology_block("F00") and "편성형" in G._rt_ontology_block("F00"))
# 데이터 보유범위 — 라이브 스캔(하드코딩·온톨로지 저장 아님). '언제부터' 질의에 강제 포함
_cov = G._p_data_coverage({"code": "T00"}) or {}
check_true("data_coverage 라이브 스캔(dau 최초일 존재)",
           bool((_cov.get("지표별 최초 수집일·정의", {}).get("dau") or {}).get("최초일")))
check_true("'언제부터 데이터' → data_coverage 강제 포함",
           "data_coverage" in G.assemble("dau wau mau 데이터 언제부터 있나?",
                                         overlay_ctx={"mode": "normal"}).get("providers_used", []))
# 전체 필드 스캔 — 변형 지표(월간 복귀율)도 최초일+온톨로지 정의 포함(하드코딩 목록 제거 회귀 방지)
_covm = (G._p_data_coverage({"code": "T00"}) or {}).get("지표별 최초 수집일·정의", {})
check_true("data_coverage에 react_rate_mon(변형) 포함", "react_rate_mon" in _covm)
check_true("data_coverage에 온톨로지 정의 첨부(월간 복귀율)",
           "복귀율" in (_covm.get("react_rate_mon", {}).get("정의", "")) and
           "월간" in (_covm.get("react_rate_mon", {}).get("정의", "")))
# 장기 아카이브 — 신호 감지·지표 매핑·온톨로지 공리 (fetch는 Splunk 필요라 스모크 제외)
check_true("장기 신호: '2023년 DAU'", G._wants_history("2023년 DAU 어땠어?"))
check_true("장기 신호: '작년 대비'", G._wants_history("작년 대비 올해 청취자"))
check_true("장기 신호 오탐 방지: '어제 DAU'", not G._wants_history("어제 DAU는?"))
import raas_datasource as DSRC
check("아카이브 슬라이스→지표 매핑",
      DSRC.HISTORY_METRIC_MAP,
      {("1D", "ALL"): "dau", ("7D", "ALL"): "dau_r7",
       ("30D", "ALL"): "dau_r30", ("1D", "1MIN"): "dau_1min"})
check_true("온톨로지: 장기 아카이브 공리 존재",
           bool(get_adapter()._onto.get_one("raas:HistoricalArchiveAxiom", "rdfs:comment")))

print("── 5.4 데이터 추출(extract) ──────────────────────────")
check_true("추출 감지: '프로그램별 DAU 뽑아줘'", G.detect_extract("파워FM 프로그램별 DAU 데이터 뽑아줘"))
check_true("추출 감지: '엑셀로 다운로드'", G.detect_extract("이번주 DAU 엑셀로 다운로드"))
check_true("추출 오탐 방지: '어제 DAU는?'", not G.detect_extract("어제 DAU는?"))
check_true("추출 지원 지표에 dau·이탈율", "dau" in G._EXTRACT_FIELDS and "churn_rate" in G._EXTRACT_FIELDS)
check_true("편성표 의도", METRICS.is_schedule_query("컬투쇼 코너 편성 알려줘"))
check(("편성표 프로그램 라우팅"), ((ROUTER.route("컬투쇼 코너 편성 알려줘", lenient=True) or {})
                               .get("program") or {}).get("code"), "F09")

print("── 5.6 편성 이력(ProgramAiring 온톨로지) ─────────────")
# 종영 프로그램 편성 이력을 온톨로지에 박제 — 자리(채널·시간대) 승계 체인
_air = get_adapter().get_program_airings(name_contains="영스트리트")
check("영스트리트 역대 4대 승계", [a["name"] for a in _air],
      ["정소민의 영스트리트", "이준의 영스트리트", "웬디의 영스트리트", "권은비의 영스트리트"])
check_true("시작일 유도(직전 종료+1): 이준=2019-12-16",
           next((a["start_date"] for a in _air if a["name"] == "이준의 영스트리트"), "") == "2019-12-16")
# 정시 정규화(floor-to-hour): :05 뉴스 오프셋 흡수 — 어예진(16:00)이 지상렬(16:05) 뒤 승계
_l1600 = get_adapter().get_program_airings(channel_code="L00", name_contains="어예진")
check_true("16시 정시병합 → 어예진 시작일 유도됨",
           bool(_l1600) and _l1600[0]["start_date"] == "2025-04-07",
           f"got={_l1600 and _l1600[0].get('start_date')}")
# 실제 편성시각 보존(러브FM :05 뉴스 오프셋) + 분석 자리는 정시
_jc = get_adapter().get_program_airings(channel_code="L00", name_contains="정치쇼")
_jc0 = next((r for r in _jc if r["name"] == "정치쇼"), {})
check("정치쇼 실제시각 0905 보존 / 분석자리 0900",
      (_jc0.get("slot_start"), _jc0.get("analysis_slot")), ("0905", "0900"))
# 09시/10시 분리 — 정치쇼가 9시대→10시대로 이동한 시대차이를 다른 자리로 반영
check("이재익의 정치쇼는 10시 자리(시대 이동)",
      next((r["analysis_slot"] for r in _jc if r["name"] == "이재익의 정치쇼"), None), "1000")
# 도메인 공리: 시간 단위 분석(5분 뉴스 오프셋)이 러브FM에 적용
check_true("온톨로지: L00에 '시간 단위 편성 분석' 공리",
           any("시간 단위" in ax.get("label", "") for ax in get_adapter().get_domain_axioms("L00")))
# provider가 실제 편성시각(air_time)도 함께 노출
_phm = G._p_program_history({"code": "M07"}) or {}
check_true("program_history air_time 보존(0905)",
           any(e.get("air_time") == "0905" for e in _phm.get("ended", [])),
           f"ended={_phm.get('ended')}")
# 프랜차이즈 시간대 이동 이력 — 정치쇼(현행 오전7시, L06)가 9시대·10시대 이력을 함께 노출
_pl6 = G._p_program_history({"code": "L06"}) or {}
_fslots = {f["slot"] for f in (_pl6.get("franchise_moved_from") or [])}
check("정치쇼 franchise 이동: 9시·10시 노출", sorted(_fslots), ["09:00", "10:00"])
# 오탐 방지: '최영주의 러브FM'·'이숙영의 러브FM'은 각각 정식 프로그램명(전체명이 정체성) —
#   진행자 떼면 채널 브랜드('러브FM')만 남는 정식명은 서로 다른 프로그램으로 취급, 오묶임 없음
check_true("franchise 오탐 방지: 'OOO의 러브FM' 정식명은 전체명으로 구분",
           G._p_program_history({"code": "M07"}).get("franchise_moved_from") is None)
# provider: 프로그램 자리의 역대 편성 + 현행 결합
_ph = G._p_program_history({"code": "F12"}) or {}
check_true("program_history: 종영 4편 + 현행 반환",
           len(_ph.get("ended", [])) == 4 and (_ph.get("current") or {}).get("name"),
           f"ended={len(_ph.get('ended', []))}")
check_true("program_history: 채널 집계코드(F00)는 None",
           G._p_program_history({"code": "F00"}) is None)
# 라이브 우선 이름 해석(TTL 낡아도 현행명) — F03 회귀 방지
check("_pgm_name 라이브 우선(F03=파워 스테이션)", METRICS._pgm_name("F03"), "파워 스테이션")
# 채널 편성 이력(channel scope) — 채널 전체 시간대별 종영승계 + 현행(평일/주말 공존)
_chh = G._p_channel_history(G.resolve_entities("러브FM 시간대별 편성")) or {}
check_true("channel_history 자리 반환(종영+현행)",
           len(_chh.get("slots", [])) >= 10 and any(s.get("current") for s in _chh.get("slots", [])),
           f"slots={len(_chh.get('slots', []))}")
check_true("channel_history 07시 평일·주말 공존(current 리스트)",
           any(s["slot"] == "07:00" and len(s.get("current", [])) >= 2 for s in _chh.get("slots", [])),
           f"07:00={[s for s in _chh.get('slots',[]) if s['slot']=='07:00']}")
# '편성 변화' 의도(채널) → channel_history 주경로, KPI·아카이브 경쟁 제거(과거 '데이터 없음' 회귀 방지)
_edn = G.assemble("2023년부터 러브FM 시간대별 프로그램 편성 변화 알려줘",
                  overlay_ctx={"mode": "normal"}).get("providers_used", [])
check_true("'편성 변화' → channel_history 포함", "channel_history" in _edn, f"used={_edn}")
check_true("'편성 변화' → channel_programs 배제", "channel_programs" not in _edn, f"used={_edn}")
# 오탐 방지: '역대 최고 DAU'는 편성 의도 아님(KPI)
check_true("오탐 방지: '역대 최고 DAU' 편성의도 아님", not G._wants_editorial("러브FM 역대 최고 DAU"))

print("── 4.5 지표 용어→필드 + 기간 인지 시계열 ────────────")
# '주간 참여율/유지율/습관형성률 추이 분석'이 시계열에 해당 필드가 없어 '데이터 없음'으로 답하던 버그
for term, fld in [("참여율", "engage_rate"), ("습관형성률", "habit_rate"),
                  ("W1 유지율", "w1_ret"), ("복귀율", "react_rate")]:
    e = G.resolve_entities(f"고릴라 플랫폼 전체 주간 {term} 추이 분석해줘")
    check(f"용어 '{term}' → {fld}", e.get("focus_fields"), [fld])
_ts = G._p_metric_timeseries(G.resolve_entities("고릴라 플랫폼 전체 최근 3개월 주간 참여율 추이 분석해줘")) or ""
check_true("주간 분석 시계열에 engage_rate_week 포함", "engage_rate_week" in _ts)
check_true("주간 분석 시계열에 w1_ret 포함", "w1_ret" in _ts)
_ts_m = G._p_metric_timeseries(G.resolve_entities("고릴라 플랫폼 전체 월간 복귀율 추이 분석해줘")) or ""
check_true("월간 분석 시계열에 react_rate_mon 포함", "react_rate_mon" in _ts_m)
# 요일은 코드가 계산해 주입(LLM 계산 금지) — 6/8·15·22·29는 월요일
check("요일 계산 정확", [G._dow_ko(d) for d in ["2026-06-08", "2026-06-22", "2026-06-28"]], ["월", "월", "일"])
_ts_w = G._p_metric_timeseries(G.resolve_entities("고릴라 플랫폼 전체 최근 3개월 주간 참여율 추이 분석해줘")) or ""
check_true("주별 행에 요일(월) 표기", "2026-06-22(월)" in _ts_w)
check_true("주별 앵커 헤더 명시", "주 시작(월요일)" in _ts_w)

print("── 4.7 오타 퍼지매칭 · 특정일 속성 · 게스트 역검색 ────")
from raas_storyline_router import extract_program as _xp
check("오타 '주현연' → F08", (_xp("12시엔 주현연") or {}).get("code"), "F08")
check("오타 '컬투쑈' → F09", (_xp("컬투쑈") or {}).get("code"), "F09")
check("비프로그램 문장 → None", _xp("안녕하세요 반가워요"), None)
_ps = G._p_point_snapshot(G.resolve_entities("7월1일 12시엔 주현영 게스트 누구야?")) or {}
check_true("point_snapshot에 guestname 포함", "guestname" in _ps.get("values", {}),
           f"keys={list(_ps.get('values',{}))[:8]}")
check_true("절대날짜 시 lookback 억제", G.resolve_entities("7월1일 12시엔 주현영 게스트").get("lookback") == 0)
check_true("게스트 역검색 감지", G._detect_guest_search("임지연 게스트 어느 프로그램에 출연했어?"))
_gs = G._assemble_guest_search("임지연 게스트 어느 프로그램에 출연했어?")
check_true("게스트 역검색 결과", bool(_gs and "임지연" in _gs.get("context", "")))

print("── 5.5 기간 내 이벤트·특일 주석(period_events) ───────")
# '신규 추이 분석'이 6/16 고릴라데이(온톨로지 기록)를 활용 못 하던 버그 — 어댑터 범위조회 박제
from raas_onto import get_adapter
_ann = get_adapter().get_calendar_annotations("2026-06-01", "2026-06-30")
check_true("6월 주석에 고릴라 데이 포함", any("고릴라" in (a.get("label") or "") for a in _ann),
           f"labels={[a.get('label') for a in _ann]}")
check_true("공휴일도 포함(현충일)", any("현충일" in (a.get("label") or "") for a in _ann))
_pe = G._p_period_events(G.resolve_entities("고릴라 플랫폼 전체 최근 4주 신규 추이 분석해줘"))
check_true("period_events provider 동작(dict|None)", _pe is None or isinstance(_pe, dict),
           f"type={type(_pe).__name__}")

print("── 6. 필드 계보(데이터 점검 연동) ────────────────────")
check_true("lineage 로드", len(DC._load_lineage()) >= 50, f"n={len(DC._load_lineage())}")
lin = DC.lineage_for("habit_rate_prev")
check_true("변형(_prev) → base 해석", bool(lin and lin["sources"][0]["lookup"] == "program_newuser_funnel_day.csv"))

print()
if FAIL:
    print(f"✗ {len(FAIL)}개 실패: {FAIL}")
    sys.exit(1)
print("✓ 전체 통과")
