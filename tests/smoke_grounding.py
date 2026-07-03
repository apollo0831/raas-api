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
import raas_storyline_engine as STORY        # noqa: E402
import raas_storyline_router as ROUTER       # noqa: E402
import raas_data_check as DC                 # noqa: E402

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

print("── 5. 비교/순위/편성표 분기 ─────────────────────────")
check_true("compare 감지: 파워FM vs 러브FM", bool(G._detect_compare("파워FM vs 러브FM 비교")))
check_true("ranking 감지: 프로그램별 DAU 순위", bool(G._detect_ranking("프로그램별 DAU 순위")))
check_true("편성표 의도", STORY.is_schedule_query("컬투쇼 코너 편성 알려줘"))
check(("편성표 프로그램 라우팅"), ((ROUTER.route("컬투쇼 코너 편성 알려줘", lenient=True) or {})
                               .get("program") or {}).get("code"), "F09")

print("── 6. 필드 계보(데이터 점검 연동) ────────────────────")
check_true("lineage 로드", len(DC._load_lineage()) >= 50, f"n={len(DC._load_lineage())}")
lin = DC.lineage_for("habit_rate_prev")
check_true("변형(_prev) → base 해석", bool(lin and lin["sources"][0]["lookup"] == "program_newuser_funnel_day.csv"))

print()
if FAIL:
    print(f"✗ {len(FAIL)}개 실패: {FAIL}")
    sys.exit(1)
print("✓ 전체 통과")
