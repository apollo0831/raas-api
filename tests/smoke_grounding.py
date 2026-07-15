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
# 이름은 라이브 우선 — PROGRAM_DIRECTORY가 낡아도(L11='어예진의 방과후 목돈연구소') 현행명으로 교정
e = G.resolve_entities("요즘 '인생은 오디션' 어때?")
check("현행명 교정(L11)", (e.get("code"), e.get("name")), ("L11", "인생은 오디션"))
e = G.resolve_entities("어제 MAU는?")
check("MAU 지표만 → 전사", e.get("code"), "T00")
# KPI 패널발 질의 — 스코프명(T00='고릴라 플랫폼 전체')을 명시하면 관심 기본값이 가로채면 안 됨
e = G.resolve_entities("고릴라 플랫폼 전체 최근 4주 DAU 추이 분석해줘", "F09")
check("패널 T00 질의, 관심 무시", (e.get("code"), e.get("scope_kind")), ("T00", "channel"))

# intent 유도(연산 분류) — provenance 신호 → canonical intent (순수함수, LLM 무관)
check("intent realtime scope", G._derive_intent("지금 동시청취", {"scope": "realtime"}), "realtime")
check("intent compare scope",  G._derive_intent("A vs B", {"scope": "compare"}), "compare")
check("intent editorial provider",
      G._derive_intent("편성 연혁", {"scope": "program", "providers": ["program_history"]}), "editorial")
check("intent trend by long_history",
      G._derive_intent("연도별 추이", {"scope": "program", "providers": ["long_history"]}), "trend")
check("intent snapshot 기본(timeseries 번들은 trend 아님)",
      G._derive_intent("러브FM 어때", {"scope": "channel", "providers": ["metric_timeseries", "point_snapshot"]}), "snapshot")

# 맥락 재작성 게이트 — 지시어 없는 독립 질문은 LLM 미호출·원문 그대로(비용 0)
check_true("후속감지: '이 중에'", G._looks_followup("이 중에 특별게스트 누구야?"))
check_true("독립질문은 후속 아님", not G._looks_followup("어제 컬투쇼 DAU 알려줘"))
check("독립질문 재작성 안함(게이트)", G.rewrite_followup("어제 컬투쇼 DAU", [{"q":"a","a":"b"}]), ("어제 컬투쇼 DAU", False))
check("recent 없으면 재작성 안함", G.rewrite_followup("이 중에 특별게스트?", []), ("이 중에 특별게스트?", False))
# 게스트 특화 — '특별/고정 게스트' 반복판정: 게스트 이력(요일)+판정 공리를 provider가 얇게 제공
check_true("게스트 반복 감지: '특별게스트'", G._wants_guest_history("컬투쇼 특별게스트 누구야"))
check_true("독립 지표질의는 게스트반복 아님", not G._wants_guest_history("컬투쇼 어제 DAU"))
_gh = G._p_guest_history(G.resolve_entities("애프터 클럽 특별게스트 누구야")) or {}
check_true("guest_history: 이력+판정공리 제공", bool(_gh.get("게스트 이력(날짜·요일·게스트)")) and bool(_gh.get("판정 공리")))
# 다중 프로그램 특별게스트 → compare scope에 프로그램별 게스트 블록
_cc = G._detect_compare("애프터 클럽, 김영철의 파워FM 특별게스트 비교")
check_true("다중 게스트 → compare 2엔티티", bool(_cc) and len(_cc) == 2)
_rc = G._assemble_compare("애프터 클럽, 김영철의 파워FM 특별게스트 비교", _cc)
check_true("compare에 프로그램별 게스트 블록", _rc["context"].count("게스트 이력·고정/특별 판정") == 2
           and "guest_history" in _rc["provenance"]["providers"])
# 전 프로그램(오늘 모든 프로그램) 특별게스트 — 프로그램별 이번/같은요일 게스트 + 공리
_gst = G._p_guest_special_today(G.resolve_entities("오늘 나오는 모든 프로그램 게스트 중 특별게스트 누구야?")) or {}
check_true("guest_special_today: 프로그램별 게스트+공리", bool(_gst.get("프로그램별 게스트")) and bool(_gst.get("판정 공리")))

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
check_true("기간 없으면 전주대비(WoW) by_change", _rk_dn.get("by_change") and not _rk_dn.get("period_change"))
# '변동 큰 프로그램'(방향 무관) → 순위(관심프로그램 앵커 아님) + 절대변동폭
_rk_var = G._detect_ranking("어제 dau 변동수치 큰 프로그램 보여줘") or {}
check("'변동 큰 프로그램' → by_change·abs", (_rk_var.get("by_change"), _rk_var.get("abs")), (True, True))
check_true("'증가' 명시는 abs 아님(방향 정렬)", G._detect_ranking("가장 많이 증가한 프로그램").get("abs") == False)
# 기간 변화량 순위 — '올해 들어 롤링MAU 감소': dau_r30 매핑 + 연초 델타(전주대비 아님)
_rk_yr = G._detect_ranking("올해 들어 롤링MAU가 가장 많이 감소한 프로그램") or {}
check("'올해 롤링MAU 감소' → dau_r30·기간델타",
      (_rk_yr.get("field"), _rk_yr.get("period_change"), _rk_yr.get("period_from")),
      ("dau_r30", True, "2026-01-01"))
check_true("기간 변화량 순위 → program_ranking_period provider",
           "program_ranking_period" in G.assemble("올해 들어 롤링MAU 가장 많이 감소한 프로그램").get("providers_used", []))
check("period_from: 최근 3개월", bool(G._rank_period_from("최근 3개월 DAU 감소")), True)
# 기간 델타 '표'(방향/최상급 없어도) — '프로그램별 올해 초 대비 변화'
_rk_tbl = G._detect_ranking("프로그램별 올해 초 대비 롤링MAU 변화 보여줘") or {}
check("'프로그램별 올해초 대비 변화' → 기간델타 표",
      (_rk_tbl.get("field"), _rk_tbl.get("period_change")), ("dau_r30", True))
# 2자리 연도 파싱 + 절대일자 기간 시작
check("2자리 연도 '26년 1월1일' 파싱", G._parse_abs_date("26년 1월1일"), (2026, 1, 1))
check("'26년 1월1일 대비' → 기간 시작", G._rank_period_from("26년 1월1일 대비 롤링MAU"), "2026-01-01")
# 2엔티티 기간 델타 비교(compare 스냅샷 아님) — 아카이브 병합 codes 지정
_pcs = G._detect_period_compare("이숙영의 러브FM과 딘딘의 뮤직하이 둘 중 26년 1월1일 대비 롤링MAU 하락이 큰 프로그램은?") or {}
check("2엔티티 기간비교 → period_change+codes",
      (_pcs.get("field"), _pcs.get("period_change"), sorted(_pcs.get("codes") or [])),
      ("dau_r30", True, ["F01", "L07"]))
check_true("기간 없는 2엔티티는 여전히 compare(회귀)",
           G._detect_period_compare("파워FM vs 러브FM 비교") is None)
# 인구 카테고리 순위 — 분포 소스 랭킹(통합 접근자). 과거 'DAU로 폴백해 성별 없음' 회귀 방지
_rk_m = G._detect_ranking("남성 청취자 비율 가장 높은 프로그램") or {}
check("'남성 비율 높은 프로그램' → 분포 순위", (_rk_m.get("demo"), _rk_m.get("source"), _rk_m.get("field")),
      (True, "pgm_gender", "M"))
check_true("성별 순위 → program_ranking_demo provider",
           "program_ranking_demo" in G.assemble("여성 비율 가장 높은 프로그램").get("providers_used", []))
check_true("DAU 순위는 여전히 KPI 랭킹(회귀)",
           "program_ranking" in G.assemble("프로그램별 DAU 순위").get("providers_used", []))
# 인구 카테고리 순위 기간 반영 — '지난 한달간'→30일 평균, 기간 없으면 최신 스냅샷
check("'지난 한달간' → 30일 평균 순위", G._demo_rank_period("지난 한달간 남성 비율 높은 프로그램"), 30)
check("'지난주' → 7일 평균 순위", G._demo_rank_period("지난주 여성 비율 순위"), 7)
check("기간 없으면 최신 스냅샷", G._demo_rank_period("남성 비율 가장 높은 프로그램"), None)
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
# 카탈로그가 온톨로지에서 동적 집계 — 신규 지표군(참여·프로필분포·실시간) 자동 포함(하드코딩 아님)
_cat = (G._assemble_meta("어떤 데이터 있나") or {}).get("context", "")
check_true("메타 카탈로그: 참여·분포·실시간 모두 포함(동적)",
           all(k in _cat for k in ("문자 참여", "성별 분포", "실시간 동시사용자", "보유 데이터 소스")),
           "누락 지표군")
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
# '분당' = 분단위 tempsummary(실시간) — 지금/실시간 없이 프로그램+과거일도 realtime scope로
check_true("실시간 감지: '분당 청취자'", G._detect_realtime("7월 13일 컬투쇼 분당 청취자 수 알려줘"))
_rt = G._rt_resolve_target("7월 13일 컬투쇼 분당 청취자 수")
check("컬투쇼 분당 → 파워FM 편성창", (_rt.get("ch_field"), _rt.get("win")), ("F00", ("14:00", "16:00")))
# 과거 특정일 분단위 — 연도 파싱 + history 경로. 보관 범위 밖이면 '언제부터' 안내
check("절대일자 연도 파싱: '2025년 12월 31일'", G._parse_abs_date("2025년 12월 31일"), (2025, 12, 31))
_rh = G._assemble_realtime("2025년 12월 31일 파워FM 분당 동시사용자 추이") or {}
check("과거 분단위 → realtime_history provider", _rh.get("providers_used"), ["realtime_history"])
check_true("보관 범위 밖 → '부터' 안내(하드코딩 아님)", "부터" in _rh.get("context", ""))
check_true("연도없는 상대일('지금')은 history 아님(today 경로)",
           "realtime_history" not in (G._assemble_realtime("지금 동시 청취자 몇 명") or {}).get("providers_used", []))
# '동시자'(축약)도 실시간 신호 — KPI로 새지 않음
check_true("실시간 감지: '분당 동시자'", G._detect_realtime("파워FM 분당 동시자"))
# 실시간 다운로드 의도 → extract 경로이되 KPI가 아니라 1분 채널표(DAU 오답 방지)
check_true("실시간 다운로드 → extract True", G.detect_extract("동시사용자 뽑아줘"))
_rx = G.build_extract("동시사용자 뽑아줘")
check_true("실시간 추출: 1분 채널표(row_label=분)",
           _rx.get("ok") and _rx["payload"].get("row_label") == "분"
           and _rx["payload"]["sheets"][0]["header"][:2] == ["시각", "전체"])
check_true("실시간 추출: 월/기간 범위는 미지원 사유",
           not G.build_extract("2026년 4월 월평균 동시사용자 뽑아줘").get("ok"))
check_true("비다운로드 실시간은 extract 아님", not G.detect_extract("지금 동시사용자 몇 명"))
# 복수일 범위 realtime 추출 — '지난주/최근 N일/A부터 B까지'
check_true("범위 파싱: '지난주'→7일", len(G._rt_date_range("지난주 컬투쇼 뽑아줘") or []) == 7)
check("범위 파싱: 'A부터 B까지' 연도상속",
      len(G._rt_date_range("2026년 4월1일부터 4월5일까지") or []), 5)
_exr = G.build_extract("지난주 월요일부터 일요일까지 컬투쇼 분당 동시사용자 뽑아줘")
check_true("주간 프로그램 범위 추출: [날짜,시각,프로그램]",
           _exr.get("ok") and _exr["payload"]["sheets"][0]["header"][:2] == ["날짜", "시각"]
           and _exr["payload"]["row_count"] > 100)
# 공용 해석 계층 — 프로그램 지정 추출/차트가 한 곳(_rt_resolve_target)에서 채널+편성창 해석
_tg = G._rt_resolve_target("아침봉 분당 동시사용자")
check("실시간 대상해석: 아침봉→프로그램+편성창", (_tg["kind"], bool(_tg["win"])), ("program", True))
_ep = G.build_extract("2026년 4월10일 아침봉 분당 동시사용자 뽑아줘")
check_true("실시간 추출: 프로그램 지정→단일 채널열+편성창 제한",
           _ep.get("ok") and len(_ep["payload"]["sheets"][0]["header"]) == 2
           and _ep["payload"]["row_count"] < 200)   # 하루(1440)가 아니라 편성창(~120)
# 해상도 오버라이드 — '1분 단위로'면 차트도 1분
check_true("차트 해상도 오버라이드 감지: '1분 단위로'", G._rt_wants_1min("1분 단위로 그려줘"))
check_true("오버라이드 없으면 10분(기본)", not G._rt_wants_1min("동시자 추이 그려줘"))
_c1 = G._assemble_realtime("2026-03-20 파워FM 분당 동시자 1분 단위로 그려줘") or {}
check_true("1분 오버라이드 → 1분 간격 CSV", "[1분 간격 추이 CSV]" in _c1.get("context", ""))
# RT-1c: 성별/연령 인증비율 시계열(접근자 위임) — 연령 필드 dim 정정(과거 AGE_T 오표기로 비어있던 버그)
check("연령 dim 정정(AGE_T 아님)", G._RT_AGES[0], ("0-19", "0_19"))
_dsx = G._rt_demo_block("파워FM 성별 동시 추이", "F00", "today", 10)
check_true("성별 추이 헬퍼: 여%/남% 컬럼", _dsx and _dsx.startswith("time,여%,남%"))
_dag = G._rt_demo_block("연령대별 추이", "F00", "today", 10)
check_true("연령 추이 헬퍼: 실제값(빈칸 아님)", _dag and _dag.startswith("time,0-19%")
           and any(c.strip() not in ("", ",") for c in _dag.splitlines()[1].split(",")[1:]))
check_true("인증비율은 F00/L00만(G00 None)", G._rt_demo_block("성별 추이", "G00", "today", 10) is None)
# 과거일 성별/연령 개방(RT-1c ②)
_hd = G._assemble_realtime("2026-04-01 파워FM 성별 동시사용자 추이") or {}
check_true("과거일 성별 추이 개방 → realtime_history_demographic",
           "realtime_history_demographic" in _hd.get("context", ""))
# P-1b: 플래너 실행기(_execute_rt) 출력 == 키워드 경로(_rt_history_branch) — 공유 렌더러
_q = "2026-03-20 파워FM 분당 동시자 추이"
_kw = G._rt_history_branch(_q) or {}
_ex = G._execute_rt({"domain": "realtime", "metric": "concurrent",
                     "entity": {"kind": "channel", "code": "F00"},
                     "time": {"when": "date", "date": "2026-03-20"}}, _q) or {}
check_true("실행기==키워드 경로(동일 렌더러)", _kw.get("context") and _kw.get("context") == _ex.get("context"))
_exp = G._execute_rt({"domain": "realtime", "metric": "concurrent",
                      "entity": {"kind": "program", "code": "F09"},
                      "time": {"date": "2026-04-01"}}, "컬투쇼") or {}
check_true("실행기 프로그램 해석: 컬투쇼→편성창 14:00~16:00", "편성창 14:00~16:00" in _exp.get("context", ""))
check_true("플래너 게이트: 날짜없는 실시간은 플래너 스킵(폴백)",
           G._planner_realtime("지금 동시사용자 몇 명") is None)
# P-1c: 랭킹 플래너 spec 매핑 == 키워드 spec(동일 렌더러 공유)
check("plan→rank spec: 남성 gender_dist",
      G._plan_to_rank_spec({"metric": "gender_dist", "dims": {"sex": "M"}}, "남성 높은"),
      {"demo": True, "source": "pgm_gender", "field": "M", "label": "남성 비율", "asc": False, "by_change": False})
check("plan→rank spec: DAU KPI",
      G._plan_to_rank_spec({"metric": "dau"}, "DAU 순위"),
      {"field": "dau", "label": "DAU", "asc": False, "by_change": False})
check_true("plan→rank spec: 매핑 불가 metric → None",
           G._plan_to_rank_spec({"metric": "sms"}, "문자 순위") is None)
# 시간 의도 분류: 월평균·기간범위=미지원 / 단일일=single
check("temporal: 월평균 → unsupported", G._rt_temporal("2026년 4월 월평균 분당 동시사용자"), ("unsupported", None))
check("temporal: 기간범위(~) → unsupported", G._rt_temporal("4월1일~4월7일 분당 동시사용자")[0], "unsupported")
check("temporal: 단일일 → single", G._rt_temporal("2026-03-20 파워FM 분당 동시자"), ("single", "2026-03-20"))
# 프로그램 과거 분단위 — 채널·편성창 해석(T00 조용한 폴백·날조 방지)
_pw = G._program_window("F09")
check_true("프로그램 편성창 해석: 컬투쇼→F00 채널+STIME", bool(_pw) and _pw[0] == "F00" and _pw[1] == "14:00")
_rp = G._assemble_realtime("2026년 4월1일 컬투쇼 동시사용자") or {}
check_true("과거 프로그램 분단위 → 편성창·프로그램명(T00 폴백 아님)",
           "컬투쇼" in _rp.get("context", "") and "편성창" in _rp.get("context", ""))
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
check("아카이브 보유 지표 4종", list(DSRC.HISTORY_METRICS),
      ["dau", "dau_r7", "dau_r30", "dau_1min"])
check_true("온톨로지: 장기 아카이브 공리 존재",
           bool(get_adapter()._onto.get_one("raas:HistoricalArchiveAxiom", "rdfs:comment")))

print("── 5.4 데이터 추출(extract) ──────────────────────────")
check_true("추출 감지: '프로그램별 DAU 뽑아줘'", G.detect_extract("파워FM 프로그램별 DAU 데이터 뽑아줘"))
check_true("추출 감지: '엑셀로 다운로드'", G.detect_extract("이번주 DAU 엑셀로 다운로드"))
check_true("추출 오탐 방지: '어제 DAU는?'", not G.detect_extract("어제 DAU는?"))
# 시간대별 AU 미보유 → 추출표 대신 답변경로(프로그램별 갈음 안내). 온톨로지 공리 기반
check_true("시간대별 AU 감지", G._wants_hourly_au("시간대별 파워FM 청취자 수 뽑아줘"))
check_true("시간대별 AU는 extract 우회(답변경로)", not G.detect_extract("시간대별 파워FM 청취자 수 뽑아줘"))
check_true("실시간 '시간대별 동시사용자'는 hourly-AU 아님(별개)", not G._wants_hourly_au("시간대별 동시사용자 뽑아줘"))
check_true("갈음 공리 로드(온톨로지)", "프로그램별" in G._hourly_au_guidance() and "미보유" in G._hourly_au_guidance())
check_true("시간대별 AU 답변에 갈음 안내 주입",
           "프로그램별" in G.assemble("시간대별 파워FM 청취자 수 추이").get("context", ""))
# 채널명 대소문자 정규화 + compare에 프로필 분포 포함
check("fm 대소문자 정규화", G._norm_fmam("파워fm, 러브Fm"), "파워FM, 러브FM")
_cmpd = G.assemble("파워fm, 러브fm 연령대 비교해줘", overlay_ctx={"mode": "admin"}).get("context", "")
check_true("소문자 채널 비교 → 채널별 프로필 분포 2개", _cmpd.count("프로필 분포") == 2)
# 특정 월('N월') → 월간 소스(_mon) + 그 월 타겟
_jun = G._p_program_demographics({"code": "F00", "_question": "6월달 파워FM 연령대", "scope_kind": "channel"}) or {}
check("'6월달' → 월간(2026-06) 타겟", ((_jun.get("집계창"), (_jun.get("연령대 분포") or {}).get("as_of"))),
      ("월간(2026-06)", "2026/06/01"))
check_true("기간 없으면 일간(회귀)",
           G._p_program_demographics({"code": "F00", "_question": "파워FM 연령대", "scope_kind": "channel"}).get("집계창") == "일간(1D)")
# 대량 덤프 의도('전부'+'일자별/프로그램별')도 추출 경로 — '보여줘'라도(과거 '데이터 없음' 회귀 방지)
check_true("추출 감지: '전체 프로그램별 일자별 전부 보여줘'",
           G.detect_extract("25년 12월 전체 프로그램별 일자별 DAU 전부 보여줘"))
check_true("추출 감지: '일별 청취자수 전부 보여줘'",
           G.detect_extract("25년 12월 전체 프로그램별 일별 청취자수 전부 보여줘"))
check_true("대량덤프 오탐 방지: '프로그램별 DAU 순위'(전부 없음)", not G.detect_extract("프로그램별 DAU 순위"))
check_true("대량덤프 오탐 방지: '게스트 일자별로'(전부 없음)", not G.detect_extract("지난주 컬투쇼 게스트 일자별로"))
# 연도 인식: 2자리·올해(2026)까지. 상세 KPI가 연중 일부만 커버 → 올해 초도 아카이브 영역
check_true("장기 신호(2자리): '25년 12월 DAU 추이'", G._wants_history("25년 12월 전체 DAU 추이"))
check("연도 정규화 '25년'→2025", G._history_years("25년 12월 전체 DAU 추이"), ["2025"])
check("연도 오탐 없음 '2025년'", G._history_years("2025년 DAU"), ["2025"])
# 올해(2026) 초 개별 프로그램 과거 질의 → 아카이브 소환(2025 상단 하드코딩이 2026 놓치던 회귀)
check("연도 정규화 '26년'→2026(올해)", G._history_years("26년 1월 컬투쇼 DAU 일자별로"), ["2026"])
check_true("'26년 1월 컬투쇼' → long_history 포함",
           "long_history" in G.assemble("26년 1월 컬투쇼 롤링MAU 일자별로 보여줘",
                                        overlay_ctx={"mode": "normal"}).get("providers_used", []))
check("미래연도 가드 '2030년'", G._history_years("2030년 DAU"), [])
check("과거범위밖 가드 '13년'", G._history_years("13년 데이터"), [])
check_true("추출 지원 지표에 dau·이탈율", "dau" in G._EXTRACT_FIELDS and "churn_rate" in G._EXTRACT_FIELDS)
# 오늘 편성·게스트(broadplan 라이브, 하루 1회) — '오늘'+게스트/편성 신호 감지·provider 등록
check_true("오늘편성 감지: '오늘 컬투쇼 게스트 누구야?'", G._wants_today_lineup("오늘 컬투쇼 게스트 누구야?"))
check_true("오늘편성 감지: '오늘 보이는라디오 프로그램'", G._wants_today_lineup("오늘 보이는라디오 하는 프로그램 알려줘"))
check_true("오늘편성 오탐 방지: '오늘 DAU'", not G._wants_today_lineup("오늘 DAU는?"))
check_true("오늘편성 오탐 방지: '지난주 게스트'(오늘 아님)", not G._wants_today_lineup("지난주 컬투쇼 게스트"))
check_true("today_lineup provider 등록", "today_lineup" in G._PROVIDER_BY_NAME)
# 참여(문자 SMS·공감로그 GG) — 감지·provider·온톨로지 정의·extract·순위 라우팅
check_true("참여 감지: '컬투쇼 문자 참여'", G._wants_engagement("컬투쇼 문자 참여 얼마나 돼?"))
check_true("참여 감지: '공감로그 1인당'", G._wants_engagement("컬투쇼 공감로그 1인당 참여"))
check_true("참여 오탐 방지: '참여율 추이'(engage_rate와 구분)", not G._wants_engagement("참여율 추이 분석"))
check_true("engagement provider 등록", "engagement" in G._PROVIDER_BY_NAME)
check_true("온톨로지: SMS/GG 필드 정의", bool(get_adapter().get_field_info("SMS")) and bool(get_adapter().get_field_info("GG")))
check_true("extract에 참여 지표(SMS·TOTAL)", "SMS" in G._EXTRACT_FIELDS and "TOTAL" in G._EXTRACT_FIELDS)
check_true("참여 순위는 참여 provider(DAU랭킹 아님)", G._wants_engagement("프로그램별 공감로그 참여 순위"))
# 데이터 출처 주의문(온톨로지 raas:sourceCaveat) — 참여 provider 사용 시 전체 노출용
_cav = G.caveats_for(["engagement"])
check_true("참여 caveat(m&studio 안내) 온톨로지 기반", bool(_cav) and "m&studio" in _cav[0])
# 프로그램 성별·연령·디바이스 분포(룩업 3종) — 감지·provider·라우팅·온톨로지
check("분포 의도 감지(연령+성별+디바이스)",
      sorted(G._wants_program_demo("컬투쇼 어제 연령대 성별 디바이스 분포")), ["age", "device", "gender"])
check_true("분포 오탐 방지: '컬투쇼 DAU'는 분포 아님", not G._wants_program_demo("컬투쇼 어제 DAU"))
_pd = G._p_program_demographics(G.resolve_entities("컬투쇼 어제 연령대 분포 알려줘")) or {}
check_true("program_demographics: 연령 분포 반환(합≈100)",
           bool(_pd.get("연령대 분포")) and 95 <= sum(float(v) for v in _pd["연령대 분포"]["분포%"].values()) <= 105,
           f"keys={list(_pd)}")
check_true("'연령대 분포' → program_demographics 포함",
           "program_demographics" in G.assemble("컬투쇼 어제 연령대 분포", overlay_ctx={"mode": "normal"}).get("providers_used", []))
check_true("'지금 동시청취'는 분포 아님(realtime)",
           "program_demographics" not in G.assemble("파워FM 지금 동시청취자", overlay_ctx={"mode": "normal"}).get("providers_used", []))
check_true("온톨로지: 분포 3종 정의", all(bool(get_adapter()._onto.get_one(f"raas:{m}", "raas:definition"))
           for m in ("ProgramGenderDist", "ProgramAgeDist", "ProgramDeviceDist")))
# 분포 기간 모드 — '지난 30일'이면 스냅샷 아니라 기간평균+일별 추이(연령·디바이스도 동일)
_pdp = G._p_program_demographics(G.resolve_entities("컬투쇼 지난 30일간 연령대 분포")) or {}
check_true("분포 기간 모드: 연령 기간평균+추이CSV",
           _pdp.get("기준", "").startswith("기간") and bool((_pdp.get("연령대 분포") or {}).get("기간평균%"))
           and "평균은 코드계산" in str(_pdp.get("연령대 분포")),
           f"기준={_pdp.get('기준')}")
# 분포 창(PERIOD) 라우팅 — 통합 접근자(raas_series) 위임. 주간/월간 데이터 접근
_pdw = G._p_program_demographics(G.resolve_entities("컬투쇼 지난주 연령대 분포")) or {}
check("분포 주간 라우팅 → 집계창 1W", _pdw.get("집계창"), "주간(1W)")
_pdm = G._p_program_demographics(G.resolve_entities("컬투쇼 지난달 성별 디바이스 분포")) or {}
check("분포 월간 라우팅 → 집계창 1M", _pdm.get("집계창"), "월간(1M)")
check_true("분포 월간: device는 1M 미보유→자동 생략", "디바이스 분포" not in _pdm and bool(_pdm.get("성별 분포")))
check_true("caveat 오탐 방지: 비참여 provider는 주의문 없음",
           not G.caveats_for(["program_kpi", "metric_timeseries"]))
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
# 원자료에 실제 편성시각(air_time) 보존 — 정치쇼 0905, 분석자리는 09
_phm = G._p_program_history({"code": "M07"}) or {}
check_true("원자료 air_time 보존(0905)+analysis_hour(09)",
           any(e.get("air_time") == "0905" and e.get("analysis_hour") == "09"
               for e in _phm.get("ended_airings", []) if "정치쇼" in e.get("name", "")),
           f"n={len(_phm.get('ended_airings', []))}")
# B 구조: 프랜차이즈 이동·스패닝·오묶임 방지는 코드가 아니라 온톨로지 공리 → LLM 적용.
#   공리 존재 + 원자료에 정치쇼의 9시·10시 이력이 그대로 실려있는지 확인(조립은 LLM 몫).
_pl6 = G._p_program_history({"code": "L06"}) or {}
_jchrs = {e["analysis_hour"] for e in _pl6.get("ended_airings", []) if "정치쇼" in e.get("name", "")}
check("원자료에 정치쇼 9시·10시 이력 포함", sorted(_jchrs), ["09", "10"])
check_true("rules에 시간대 이동 공리(프랜차이즈·오묶임 방지 서술) 포함",
           any("시간대 이동" in r.get("label", "") and "러브FM" in r.get("text", "")
               for r in _pl6.get("rules", [])))
check_true("rules에 24h 연속 편성 공리(다시간 스패닝 서술) 포함",
           any("연속 편성" in r.get("label", "") and "04:00" in r.get("text", "")
               for r in _pl6.get("rules", [])))
# provider(B: 얇은 원자료+공리): focus 프로그램 + 채널 airing 원자료 + 해석 rules
_ph = G._p_program_history({"code": "F12"}) or {}
check_true("program_history: focus+원자료+rules 반환",
           (_ph.get("focus_program") or {}).get("analysis_hour") == "20"
           and len(_ph.get("ended_airings", [])) >= 4
           and any("연속 편성" in r.get("label", "") for r in _ph.get("rules", [])),
           f"keys={list(_ph)}")
check_true("program_history: 채널 집계코드(F00)는 None",
           G._p_program_history({"code": "F00"}) is None)
# 라이브 우선 이름 해석(TTL 낡아도 현행명) — F03 회귀 방지
check("_pgm_name 라이브 우선(F03=파워 스테이션)", METRICS._pgm_name("F03"), "파워 스테이션")
# 채널 편성 이력(channel scope, B: 얇은 원자료+공리) — 종영·현행 원자료와 rules 제공
_chh = G._p_channel_history(G.resolve_entities("러브FM 시간대별 편성")) or {}
check_true("channel_history 원자료(종영 30+·현행 15+·rules)",
           len(_chh.get("ended_airings", [])) >= 30 and len(_chh.get("current_programs", [])) >= 15
           and len(_chh.get("rules", [])) >= 3,
           f"ended={len(_chh.get('ended_airings', []))}, cur={len(_chh.get('current_programs', []))}")
# 07시 평일·주말 공존 — 원자료에 STIME=0700 프로그램 2개(정치쇼+드라이브뮤직) 그대로 노출
check_true("현행 원자료 07시 공존(2편)",
           sum(1 for p in _chh.get("current_programs", []) if p["start_time"] == "0700") >= 2,
           f"0700={[p['name'] for p in _chh.get('current_programs', []) if p['start_time']=='0700']}")
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
# 온톨로지 altLabel이 엔티티 별칭으로 자동 반영(하드코딩 아님) — '철파엠'→F05(김영철의 파워FM)
check("온톨로지 별칭 '철파엠' → F05", (_xp("어제 철파엠 문자 참여자수") or {}).get("code"), "F05")
check("온톨로지 별칭 '영철파워' → F05", (_xp("영철파워 어때") or {}).get("code"), "F05")
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
