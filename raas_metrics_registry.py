"""통합 지표 레지스트리 (A-lite 시드) — raas_metrics_registry.

각 데이터 소스가 자신의 grain·집계창·추가축(extra_dims)·온톨로지 링크를 **선언**한다.
이 선언을 두 곳이 소비한다:
  · Phase 1 통합 접근 계층(series/snapshot/period_avg/ranking)의 소스 카탈로그
  · Phase 0 커버리지 맵(tools/gen_coverage_map.py)의 노드/엣지 원천

원칙: 레지스트리는 **선언만** 한다 — provider·계산 로직·도메인 규칙을 여기 넣지 않는다.
온톨로지 지표 인벤토리는 실행 시 어댑터에서 읽어(coverage()) 선언과 대조하므로,
온톨로지에 지표를 추가하면 맵의 '커버리지'가 자동으로 갱신된다(코드 수정 없이).
"""
from dataclasses import dataclass, field


@dataclass
class Source:
    name: str                       # 내부 식별자
    label: str                      # 표시명
    grain: str                      # "daily" | "minute" | "editorial"
    axis: str                       # 격자 설명(맵 그룹 라벨)
    lookups: list                   # 원천(룩업 파일/인덱스명)
    index_fn: str                   # raas_datasource getter 이름(지연 바인딩용 문자열)
    available_periods: list         # 집계창 {1D,1W,1M} 등 — 소스가 실제 보유한 창만
    onto_metrics: list              # 이 소스가 채우는 온톨로지 지표(ko 라벨) — 없으면 속성 소스
    providers: list                 # 답변 경로(grounding provider / scope)
    extra_dims: dict = field(default_factory=dict)   # {"DEPTH":[...], "CATEGORY":[...]}
    note: str = ""
    # 인덱스 물리 형태 — 통합 접근자(raas_series)가 셀을 뜨는 방식:
    #   "flat"    : {code:{date:row}}                         (kpi·engagement·history)
    #   "profile" : {code:{date:{PERIOD:{TYPE:{CATEGORY:v}}}}} (분포)
    #   ""        : 접근자 비대상(분/편성)
    shape: str = "flat"


# ── 일간 격자 (코드 × 날짜) — 교차분석이 가능한 공통 좌표 ─────────────────
_DAILY = [
    Source(
        name="kpi", label="청취 KPI", grain="daily",
        axis="일간 격자 · (코드 × 날짜)",
        lookups=["raas_kpi_latest.csv"], index_fn="get_timeline",
        available_periods=["1D", "1W", "1M"],
        onto_metrics=["활성 사용자 수", "1분 이상 청취자 수", "10분 이상 청취자 수",
                      "신규 사용자", "복귀 사용자", "이탈률", "복귀율", "깊은청취율",
                      "참여율", "실청취율", "습관형성률", "유지율"],
        providers=["program_kpi", "metric_timeseries", "flow_decomp", "cohort", "stickiness"],
    ),
    Source(
        name="engagement", label="참여(문자·공감)", grain="daily",
        axis="일간 격자 · (코드 × 날짜)",
        lookups=["SUMMARY_INDEX_02"], index_fn="get_engagement_index",
        available_periods=["1D"],
        onto_metrics=["문자 참여", "공감로그 참여", "합계 참여(문자+공감로그)"],
        providers=["engagement"],
    ),
    Source(
        name="pgm_gender", label="성별 분포", grain="daily",
        axis="일간 격자 · (코드 × 날짜)",
        lookups=["program_user_gender_{day|week|mon}.csv"], index_fn="get_program_gender_index",
        available_periods=["1D", "1W", "1M"],
        onto_metrics=["프로그램 성별 분포"],
        providers=["program_demographics"],
        extra_dims={"DEPTH": ["ALL", "1MIN", "10MIN"], "CATEGORY": ["F", "M"]}, shape="profile",
    ),
    Source(
        name="pgm_age", label="연령대 분포", grain="daily",
        axis="일간 격자 · (코드 × 날짜)",
        lookups=["program_user_age_{day|week|mon}.csv"], index_fn="get_program_age_index",
        available_periods=["1D", "1W", "1M"],
        onto_metrics=["프로그램 연령대 분포"],
        providers=["program_demographics"],
        extra_dims={"DEPTH": ["ALL", "1MIN", "10MIN"], "CATEGORY": ["UNDER20", "…", "OVER60"]}, shape="profile",
    ),
    Source(
        name="pgm_device", label="디바이스 분포", grain="daily",
        axis="일간 격자 · (코드 × 날짜)",
        lookups=["program_user_device_day.csv"], index_fn="get_program_device_index",
        available_periods=["1D"],
        onto_metrics=["프로그램 디바이스 분포"],
        providers=["program_demographics"],
        extra_dims={"DEPTH": ["ALL", "1MIN", "10MIN"], "CATEGORY": ["SP", "PC", "AI", "…"]}, shape="profile",
    ),
    Source(
        name="history_archive", label="장기 아카이브", grain="daily",
        axis="일간 격자 · (코드 × 날짜)",
        lookups=["real_dau.csv", "summary_uuid_stats"], index_fn="get_history_index",
        available_periods=["1D"],
        onto_metrics=["활성 사용자 수", "1분 이상 청취자 수"],
        providers=["long_history"],
        note="최대 10년 — dau·dau_r7·dau_r30·dau_1min. 원천 상이(HistoricalArchiveAxiom)",
    ),
]

# ── 분 단위 · 실시간 (별도 좌표) ──────────────────────────────────────────
_MINUTE = [
    Source(
        name="realtime", label="실시간", grain="minute",
        axis="분 단위 · 실시간",
        lookups=["tempsummary", "infobank", "tempsummary3"],
        index_fn="get_rt_concurrent / get_rt_msg / get_rt_inflow",
        available_periods=["1min"],
        onto_metrics=["실시간 동시사용자", "실시간 디바이스별 동시사용자", "실시간 보는라디오 동시사용자",
                      "실시간 본인인증 동시사용자", "실시간 인증자 연령대 비율", "실시간 인증자 성별 비율",
                      "실시간 분당 유입수", "실시간 분당 문자·공감로그"],
        providers=["realtime scope"], shape="",
    ),
]

# ── 편성·서술형 (불변은 온톨로지, 당일은 라이브) ──────────────────────────
_EDITORIAL = [
    Source(
        name="airing_history", label="편성 연혁", grain="editorial",
        axis="편성·서술형",
        lookups=["종방프로그램.csv", "broadplan(현행)"], index_fn="get_program_airings",
        available_periods=[],
        onto_metrics=[],           # 지표가 아님 — ProgramAiring 51 사실 + 편성 해석 공리 3종
        providers=["program_history", "channel_history"],
        note="ProgramAiring(불변 사실) + 라이브 현행. 조립은 LLM(공리 적용)", shape="",
    ),
    Source(
        name="today_lineup", label="오늘 편성·게스트", grain="editorial",
        axis="편성·서술형",
        lookups=["broadplan(당일)"], index_fn="get_today_lineup",
        available_periods=[],
        onto_metrics=[],           # 게스트·보는라디오 = 속성(지표 수준 정의 없음)
        providers=["today_lineup"],
        note="게스트·view_radio — 상세 KPI 배치가 못 담은 '오늘' 공백 메움. 교차분석 대상 아님", shape="",
    ),
]

SOURCES = _DAILY + _MINUTE + _EDITORIAL


def get_source(name: str):
    return next((s for s in SOURCES if s.name == name), None)


def daily_sources():
    """교차지표 격자에 오르는 일간 소스(숫자 계열)."""
    return [s for s in _DAILY]


def ontology_metrics() -> list:
    """온톨로지에 정의된 raas:Metric 인벤토리(실행 시 어댑터에서 읽음)."""
    try:
        from raas_onto import get_adapter
        o = get_adapter()._onto
        out = []
        for m in o.instances_of("raas:Metric"):
            out.append({
                "label": o.label_ko(m),
                "section": (o.value_str(o.get_one(m, "raas:dictionarySection")) or "(무섹션)"),
            })
        return out
    except Exception as e:
        print(f"[registry] 온톨로지 지표 로드 실패: {e}")
        return []


def _count_relations() -> int:
    """교차지표 관계(metric↔metric) 선언 수 — Phase 3 전까지 0."""
    try:
        from raas_onto import get_adapter
        o = get_adapter()._onto
        n = 0
        for pred in ("raas:relatesTo", "raas:mayDrive", "raas:decomposesInto"):
            try:
                n += len(o.triples_with_predicate(pred))
            except Exception:
                pass
        return n
    except Exception:
        return 0


def coverage() -> dict:
    """레지스트리 선언 × 온톨로지 인벤토리 대조 → 커버리지 맵 데이터.

    데이터/온톨로지를 추가하면(레지스트리 onto_metrics 또는 TTL 지표) 이 결과가 자동 갱신된다.
    """
    metrics = ontology_metrics()
    all_labels = {m["label"] for m in metrics}
    declared = set()
    for s in SOURCES:
        declared |= set(s.onto_metrics)

    # 온톨로지엔 있는데 어떤 소스도 채운다고 선언 안 한 지표(= 데이터 공백)
    uncovered = sorted(all_labels - declared)
    # 소스가 채운다는데 온톨로지에 정의 없는 라벨(= 정의 공백/철자 불일치)
    undefined = sorted(declared - all_labels)
    # 지표를 하나도 안 채우는 소스(= 속성/서술형 소스)
    attribute_sources = [s.name for s in SOURCES if not s.onto_metrics]

    return {
        "sources": [s.__dict__ for s in SOURCES],
        "metrics": metrics,
        "stats": {
            "sources": len(SOURCES),
            "metrics": len(metrics),
            "covered": len(all_labels & declared),
            "uncovered": uncovered,
            "undefined": undefined,
            "attribute_sources": attribute_sources,
            "relations": _count_relations(),
        },
    }


if __name__ == "__main__":
    import json
    cov = coverage()
    st = cov["stats"]
    print(f"소스 {st['sources']} · 온톨로지 지표 {st['metrics']} · 커버 {st['covered']} · 교차관계 {st['relations']}")
    if st["uncovered"]:
        print("  데이터 공백(정의는 있으나 소스 미선언):", st["uncovered"])
    if st["undefined"]:
        print("  ⚠ 정의 공백(소스가 채운다는데 온톨로지에 없음):", st["undefined"])
    print("  속성 소스(지표 아님):", st["attribute_sources"])
