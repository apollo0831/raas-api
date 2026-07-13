"""교차지표 연산(raas_analytics) + metric_correlate provider 스모크.

결정적 상관/분해의 수치 정합성과 provider의 대상감지·게이트를 확인(LLM 호출 0)."""
import io, sys, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import raas_analytics as AN

ok = fail = 0
def check(name, cond, extra=""):
    global ok, fail
    if cond: ok += 1; print(f"  PASS {name}")
    else: fail += 1; print(f"  FAIL {name}  {extra}")

print("── analytics 수치 정합 ──")
a = [("d1", 1), ("d2", 2), ("d3", 3), ("d4", 4), ("d5", 5)]
up = [("d1", 10), ("d2", 20), ("d3", 30), ("d4", 40), ("d5", 50)]
down = [("d1", 50), ("d2", 40), ("d3", 30), ("d4", 20), ("d5", 10)]
check("완전 동행 r=+1", AN.co_movement(a, up, min_n=3)["r"] == 1.0)
check("완전 역행 r=-1", AN.co_movement(a, down, min_n=3)["r"] == -1.0)
check("표본 부족(<min_n)→None", AN.co_movement(a[:2], up[:2]) is None)
check("분산 0 → None", AN.co_movement(a, [("d1", 5), ("d2", 5), ("d3", 5), ("d4", 5), ("d5", 5)], min_n=3) is None)
xs, ys, dates = AN.align(a, [("d2", 9), ("d3", 9), ("d9", 9)])
check("align 공통날짜만", dates == ["d2", "d3"])
ch = AN.change(a)
check("change pct", ch["pct"] == 400.0 and ch["abs"] == 4)
dec = AN.decompose(a, {"up": up, "down": down})
check("decompose |r| 내림차순", [f["factor"] for f in dec["factors"]] and abs(dec["factors"][0]["r"]) >= abs(dec["factors"][-1]["r"]))
rk = AN.rank_by_change({"X": down, "Y": up}, ascending=True)
check("rank_by_change 하락 상위", rk[0]["code"] == "X")

print("── metric_correlate provider ──")
import raas_grounding as G
G.call_claude = None
try:
    import raas_server as SRV
    G.S.set_timeline_provider(SRV.get_cached_timeline)
except Exception:
    pass

check("의도 감지: '같이 움직'", G._wants_correlate("DAU가 무엇과 같이 움직였나"))
check("의도 감지: '상관'", G._wants_correlate("참여율 상관"))
check("오탐 방지: 일반 현황", not G._wants_correlate("컬투쇼 어제 DAU 얼마"))
check("대상감지: DAU 우선", G._target_metric("DAU가 문자참여와 같이 갔나")[3] == "DAU")
check("대상감지: 참여율", G._target_metric("참여율이 무엇과 상관")[1] == "engage_rate")

ent = G.resolve_entities("컬투쇼 DAU가 무엇과 상관 있나"); ent["_question"] = "컬투쇼 DAU가 무엇과 상관 있나"
r = G._p_metric_correlate(ent)
check("provider 반환(대상=DAU)", bool(r) and r.get("대상지표") == "DAU")
check("동반움직임 |r| 내림차순", bool(r) and all(
    abs(r["동반움직임(|r| 상위)"][i]["r"]) >= abs(r["동반움직임(|r| 상위)"][i + 1]["r"])
    for i in range(len(r["동반움직임(|r| 상위)"]) - 1)))
check("게이트: 비상관 질의→None", G._p_metric_correlate({**ent, "_question": "컬투쇼 어제 DAU"}) is None)
res = G.assemble("컬투쇼 DAU가 무엇과 연관", overlay_ctx={"mode": "normal"})
check("라우팅: metric_correlate 포함", "metric_correlate" in res.get("providers_used", []))

print("── Phase 3: 온톨로지 관계 ──")
from raas_onto import get_adapter
_A = get_adapter()
facs = _A.get_correlation_factors()
check("CorrelationFactor 로드(>=10)", len(facs) >= 10, f"n={len(facs)}")
check("구성비/드라이버 분류 존재", any(f["relation"] == "compositional" for f in facs)
      and any(f["relation"] == "mayDrive" for f in facs))
blk = _A.get_metric_relations_block()
check("관계블록 구성비 공리 포함", "구성비 해석 공리" in blk and "합=100" in blk)
check("provider에 관계지식 주입", bool(r) and "관계지식(온톨로지)" in r)
check("팩터가 온톨로지 유래(라벨 일치)", bool(r) and any(
    x["지표"] in [f["label"] for f in facs] for x in r["동반움직임(|r| 상위)"]))

print(f"\n{'✓ 전체 통과' if not fail else f'✗ {fail}건 실패'} (ok={ok}, fail={fail})")
sys.exit(1 if fail else 0)
