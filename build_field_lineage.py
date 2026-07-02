# -*- coding: utf-8 -*-
"""raas_kpi_latest.csv 필드 → 소스 룩업 → 정제작업(saved search)·서머리인덱스 계보 생성.

레이어 A (필드 ↔ 룩업): raas_kpi_save.spl 쿼리에서 추출 — 이 파일이 authority.
레이어 B (룩업 ↔ 정제작업/실행시간/서머리인덱스): RAAS온톨로지.xlsx
        'SummaryIndex 및 룩업_생성정보' 시트에서 추출.

두 레이어를 룩업 파일명으로 조인해 data/field_lineage.json 을 만든다.
데이터 확인(raas_data_check) 기능이 결측/이상 필드를 감지했을 때 "어느 룩업·정제작업이
원인인지"를 즉시 안내하는 데 사용.

키 규칙(중요): raas_kpi_latest.csv 의 **최종 컬럼명**을 키로 쓴다.
  - 일간은 접미사 없음(dau, habit_rate ...), 주간 _week, 월간 _mon.
  - _prev / _chg / _diff / _share 변형은 base 컬럼과 계보가 같다(리졸버가 접미사 제거).

재생성: python build_field_lineage.py   (엑셀/쿼리 변경 시 이 파일 갱신 후 재실행)
"""
from __future__ import annotations
import json
from pathlib import Path

# ── 레이어 B: 룩업/서머리인덱스 → {정제작업명, 실행시간, 주기} (엑셀에서 추출) ──────
# job = 스케줄러 로그 sourcetype=scheduler 의 savedsearch_name 과 대조할 한글 작업명.
JOB = {
    # program_user_funnel_day.csv 는 3개 배치가 같은 룩업에 씀(일/주롤링/월롤링)
    "funnel_day":        {"lookup": "program_user_funnel_day.csv",   "job": "프로그램별_사용자_퍼널_통계_일",        "exec": "01:21", "freq": "일"},
    "funnel_day_r7":     {"lookup": "program_user_funnel_day.csv",   "job": "프로그램별_사용자_퍼널_통계_주_롤링",   "exec": "01:35", "freq": "일"},
    "funnel_day_r30":    {"lookup": "program_user_funnel_day.csv",   "job": "프로그램별_사용자_퍼널_통계_월_롤링",   "exec": "02:50", "freq": "일"},
    "funnel_week":       {"lookup": "program_user_funnel_week.csv",  "job": "프로그램별_사용자_퍼널_통계_주",        "exec": "월 1:35", "freq": "주"},
    "funnel_mon":        {"lookup": "program_user_funnel_mon.csv",   "job": "프로그램별_사용자_퍼널_통계_월",        "exec": "1일 02:50", "freq": "월"},

    "newfunnel_day":     {"lookup": "program_newuser_funnel_day.csv","job": "프로그램별_신규사용자_퍼널_통계_일",   "exec": "01:20", "freq": "일"},
    "newfunnel_week":    {"lookup": "program_newuser_funnel_week.csv","job": "프로그램별_신규사용자_퍼널_통계_주",  "exec": "월 1:41", "freq": "주"},
    "newfunnel_mon":     {"lookup": "program_newuser_funnel_mon.csv","job": "프로그램별_신규사용자_퍼널_통계_월",   "exec": "1일 02:31", "freq": "월"},
    "habit_mon":         {"lookup": "program_newuser_funnel_mon.csv","job": "프로그램별_신규사용자_습관형성률_통계_월", "exec": "7일 3:30", "freq": "월"},

    "react_day":         {"lookup": "program_reactuser_funnel_day.csv", "job": "프로그램_복귀사용자_퍼널_통계_일", "exec": "02:10", "freq": "일"},
    "react_week":        {"lookup": "program_reactuser_funnel_week.csv","job": "프로그램_복귀사용자_퍼널_통계_주", "exec": "월 2:30", "freq": "주"},
    "react_mon":         {"lookup": "program_reactuser_funnel_mon.csv", "job": "프로그램_복귀사용자_퍼널_통계_월", "exec": "1일 3:10", "freq": "월"},

    "churn_day":         {"lookup": "program_churnuser_funnel_day.csv", "job": "프로그램_이탈사용자_퍼널_통계_일", "exec": "01:58", "freq": "일"},
    "churn_week":        {"lookup": "program_churnuser_funnel_week.csv","job": "프로그램_이탈사용자_퍼널_통계_주", "exec": "월 2:05", "freq": "주"},
    "churn_mon":         {"lookup": "program_churnuser_funnel_mon.csv", "job": "프로그램_이탈사용자_퍼널_통계_월", "exec": "1일 2:50", "freq": "월"},

    "dormant_day":       {"lookup": "program_dormant_pool_day.csv",  "job": "프로그램별_휴면사용자풀_통계_일",       "exec": "01:40", "freq": "일"},
    "dormant_week":      {"lookup": "program_dormant_pool_week.csv", "job": "프로그램별_휴면사용자풀_통계_주",       "exec": "월 1:45", "freq": "주"},
    "dormant_mon":       {"lookup": "program_dormant_pool_mon.csv",  "job": "프로그램별_휴면사용자풀_통계_월",       "exec": "1일 1:50", "freq": "월"},

    "ret_day":           {"lookup": "program_user_retention_day.csv", "job": "프로그램별_사용자_유지율_통계_일",     "exec": "01:13", "freq": "일"},
    "ret_week":          {"lookup": "program_user_retention_week.csv","job": "프로그램별_사용자_유지율_통계_주",     "exec": "월 1:22", "freq": "주"},
    "ret_mon":           {"lookup": "program_user_retention_mon.csv", "job": "프로그램별_사용자_유지율_통계_월",     "exec": "1일 3:10", "freq": "월"},

    "newret_day":        {"lookup": "program_newuser_retention_day.csv", "job": "프로그램별_신규사용자_유지율_통계_일", "exec": "01:21", "freq": "일"},
    "newret_week":       {"lookup": "program_newuser_retention_week.csv","job": "프로그램별_신규사용자_유지율_통계_주", "exec": "월 1:23", "freq": "주"},
    "newret_mon":        {"lookup": "program_newuser_retention_mon.csv", "job": "프로그램별_신규사용자_유지율_통계_월", "exec": "1일 1:50", "freq": "월"},

    # 참여: csv 앞단에 서머리인덱스(summary_index_14/21/26)가 있음 → summary 필드로 병기
    "engage_day":        {"lookup": "program_user_engage_day.csv",  "job": "프로그램별_사용자_참여_통계_일", "exec": "01:07", "freq": "일", "summary_index": "summary_index_14 (프로그램별_참여_일)"},
    "engage_week":       {"lookup": "program_user_engage_week.csv", "job": "프로그램별_사용자_참여_통계_주", "exec": "월 1:08", "freq": "주", "summary_index": "summary_index_21 (프로그램별_참여_주)"},
    "engage_mon":        {"lookup": "program_user_engage_mon.csv",  "job": "프로그램별_사용자_참여_통계_월", "exec": "1일 01:09", "freq": "월", "summary_index": "summary_index_26 (프로그램별_참여_월)"},

    # 편성(정적 룩업 / 원천 인덱스)
    "broadplan_static":  {"lookup": "BROADPLAN.csv", "job": "(정적 룩업 — 스케줄 배치 아님, 수동/편성 관리)", "exec": None, "freq": None},
    "idx_broadplan":     {"lookup": "index=broadplan", "job": "프로그램편성정보(원천 인덱스 적재)", "exec": "매시간", "freq": "시간"},
    "idx_nike":          {"lookup": "index=nike",      "job": "편성정보(원천 인덱스 적재)",         "exec": "매시간", "freq": "시간"},
}


def src(key, part=None, cond=None):
    d = dict(JOB[key])
    if part:
        d["part"] = part
    if cond:
        d["cond"] = cond
    return d


# ── 레이어 A: 최종 CSV 컬럼(base) → 계보. role: base|ratio|derived|meta ──────────
FIELDS = {
    # ===== 일간 =====
    "dau":        {"label": "일간 활성사용자(DAU)", "period": "day", "role": "base",
                   "sources": [src("funnel_day", cond="TYPE=ALL PERIOD=1D")]},
    "dau_d2":     {"label": "전일 DAU(흐름계산용)", "period": "day", "role": "base",
                   "sources": [src("funnel_day", cond="TYPE=ALL PERIOD=1D (+1d 시프트)")]},
    "dau_r7":     {"label": "WAU 롤링(7일)", "period": "day", "role": "base",
                   "sources": [src("funnel_day_r7", cond="TYPE=ALL PERIOD=7D")]},
    "dau_r30":    {"label": "MAU 롤링(30일)", "period": "day", "role": "base",
                   "sources": [src("funnel_day_r30", cond="TYPE=ALL PERIOD=30D")]},
    "dau_1min":   {"label": "1분이상 청취자(일)", "period": "day", "role": "base",
                   "sources": [src("funnel_day", cond="TYPE=1MIN PERIOD=1D")]},
    "dau_10min":  {"label": "10분이상 청취자(일)", "period": "day", "role": "base",
                   "sources": [src("funnel_day", cond="TYPE=10MIN PERIOD=1D")]},
    "real_rate":  {"label": "실청취율(일)=1분이상/DAU", "period": "day", "role": "derived",
                   "derived_from": ["dau_1min", "dau"],
                   "sources": [src("funnel_day", cond="TYPE=1MIN·ALL PERIOD=1D")]},
    "deep_rate":  {"label": "깊은청취율(일)=10분이상/1분이상", "period": "day", "role": "derived",
                   "derived_from": ["dau_10min", "dau_1min"],
                   "sources": [src("funnel_day", cond="TYPE=10MIN·1MIN PERIOD=1D")]},
    "new":        {"label": "신규 사용자(일)", "period": "day", "role": "base",
                   "sources": [src("newfunnel_day", cond="TYPE=ALL PERIOD=1D")]},
    "react":      {"label": "복귀 사용자(일)", "period": "day", "role": "base",
                   "sources": [src("react_day", cond="TYPE=ALL PERIOD=1D")]},
    "react_rate": {"label": "복귀율(일)=복귀/휴면풀", "period": "day", "role": "ratio",
                   "sources": [src("react_day", part="분자(복귀사용자)"),
                               src("dormant_day", part="분모(휴면풀)")]},
    "churn_rate": {"label": "이탈율(일)=이탈/전일DAU", "period": "day", "role": "ratio",
                   "sources": [src("churn_day", part="분자(이탈사용자)"),
                               src("funnel_day", part="분모(전일 DAU)")]},
    "d1_ret":     {"label": "D1 유지율(전체코호트)", "period": "day", "role": "base",
                   "sources": [src("ret_day", cond="TYPE=D1 COHORT=all")]},
    "d7_ret":     {"label": "D7 유지율(전체코호트)", "period": "day", "role": "base",
                   "sources": [src("ret_day", cond="TYPE=D7 COHORT=all")]},
    "new_d1_ret": {"label": "신규 D1 유지율(신규코호트)", "period": "day", "role": "base",
                   "sources": [src("newret_day", cond="TYPE=D1 COHORT=all")]},
    "new_d7_ret": {"label": "신규 D7 유지율(신규코호트)", "period": "day", "role": "base",
                   "sources": [src("newret_day", cond="TYPE=D7 COHORT=all")]},
    "engage_rate":{"label": "참여율(일)=참여/DAU", "period": "day", "role": "ratio",
                   "sources": [src("engage_day", part="분자(참여사용자)"),
                               src("funnel_day", part="분모(DAU)")]},
    "habit_rate": {"label": "습관형성률(일)=3D7D_1M/ALL", "period": "day", "role": "ratio",
                   "sources": [src("newfunnel_day", part="분자/분모(TYPE=3D7D_1M / TYPE=ALL)")]},

    # ===== 주간 =====
    "wau":        {"label": "주간 활성사용자(WAU)", "period": "week", "role": "base",
                   "sources": [src("funnel_week", cond="TYPE=ALL PERIOD=1W")]},
    "wau_1min":   {"label": "1분이상 청취자(주)", "period": "week", "role": "base",
                   "sources": [src("funnel_week", cond="TYPE=1MIN PERIOD=1W")]},
    "wau_10min":  {"label": "10분이상 청취자(주)", "period": "week", "role": "base",
                   "sources": [src("funnel_week", cond="TYPE=10MIN PERIOD=1W")]},
    "real_rate_week": {"label": "실청취율(주)", "period": "week", "role": "derived",
                   "derived_from": ["wau_1min", "wau"], "sources": [src("funnel_week")]},
    "deep_rate_week": {"label": "깊은청취율(주)", "period": "week", "role": "derived",
                   "derived_from": ["wau_10min", "wau_1min"], "sources": [src("funnel_week")]},
    "new_week":   {"label": "신규 사용자(주)", "period": "week", "role": "base",
                   "sources": [src("newfunnel_week", cond="TYPE=ALL")]},
    "react_week": {"label": "복귀 사용자(주)", "period": "week", "role": "base",
                   "sources": [src("react_week", cond="TYPE=ALL")]},
    "react_rate_week": {"label": "복귀율(주)", "period": "week", "role": "ratio",
                   "sources": [src("react_week", part="분자(복귀)"), src("dormant_week", part="분모(휴면풀)")]},
    "churn_rate_week": {"label": "이탈율(주)", "period": "week", "role": "ratio",
                   "sources": [src("churn_week", part="분자(이탈)"), src("funnel_week", part="분모(전주 WAU)")]},
    "w1_ret":     {"label": "W1 유지율(전체코호트)", "period": "week", "role": "base",
                   "sources": [src("ret_week", cond="TYPE=W1 COHORT=all")]},
    "new_w1_ret": {"label": "신규 W1 유지율(신규코호트)", "period": "week", "role": "base",
                   "sources": [src("newret_week", cond="TYPE=W1 COHORT=all")]},
    "engage_rate_week": {"label": "참여율(주)", "period": "week", "role": "ratio",
                   "sources": [src("engage_week", part="분자(참여)"), src("funnel_week", part="분모(WAU)")]},
    "habit_rate_week": {"label": "습관형성률(주)", "period": "week", "role": "ratio",
                   "sources": [src("newfunnel_week", part="분자/분모(TYPE=3D7D_1M / ALL)")]},

    # ===== 월간 =====
    "mau":        {"label": "월간 활성사용자(MAU)", "period": "mon", "role": "base",
                   "sources": [src("funnel_mon", cond="TYPE=ALL PERIOD=1M")]},
    "mau_1min":   {"label": "1분이상 청취자(월)", "period": "mon", "role": "base",
                   "sources": [src("funnel_mon", cond="TYPE=1MIN PERIOD=1M")]},
    "mau_10min":  {"label": "10분이상 청취자(월)", "period": "mon", "role": "base",
                   "sources": [src("funnel_mon", cond="TYPE=10MIN PERIOD=1M")]},
    "real_rate_mon": {"label": "실청취율(월)", "period": "mon", "role": "derived",
                   "derived_from": ["mau_1min", "mau"], "sources": [src("funnel_mon")]},
    "deep_rate_mon": {"label": "깊은청취율(월)", "period": "mon", "role": "derived",
                   "derived_from": ["mau_10min", "mau_1min"], "sources": [src("funnel_mon")]},
    "new_mon":    {"label": "신규 사용자(월)", "period": "mon", "role": "base",
                   "sources": [src("newfunnel_mon", cond="TYPE=ALL")]},
    "react_mon":  {"label": "복귀 사용자(월)", "period": "mon", "role": "base",
                   "sources": [src("react_mon", cond="TYPE=ALL")]},
    "react_rate_mon": {"label": "복귀율(월)", "period": "mon", "role": "ratio",
                   "sources": [src("react_mon", part="분자(복귀)"), src("dormant_mon", part="분모(휴면풀)")]},
    "churn_rate_mon": {"label": "이탈율(월)", "period": "mon", "role": "ratio",
                   "sources": [src("churn_mon", part="분자(이탈)"), src("funnel_mon", part="분모(전월 MAU)")]},
    "m1_ret":     {"label": "M1 유지율(전체코호트)", "period": "mon", "role": "base",
                   "sources": [src("ret_mon", cond="TYPE=M1 COHORT=all")]},
    "new_m1_ret": {"label": "신규 M1 유지율(신규코호트)", "period": "mon", "role": "base",
                   "sources": [src("newret_mon", cond="TYPE=M1 COHORT=all")]},
    "engage_rate_mon": {"label": "참여율(월)", "period": "mon", "role": "ratio",
                   "sources": [src("engage_mon", part="분자(참여)"), src("funnel_mon", part="분모(MAU)")]},
    "habit_rate_mon": {"label": "습관형성률(월)", "period": "mon", "role": "ratio",
                   "sources": [src("habit_mon", part="분자/분모(습관형성률 배치)"),
                               src("newfunnel_mon", part="신규퍼널(ALL 분모)")]},

    # ===== 편성/메타 =====
    "PGM_NAME":   {"label": "프로그램명", "period": "meta", "role": "meta",
                   "sources": [src("broadplan_static")]},
    "STIME":      {"label": "방송 시작시각", "period": "meta", "role": "meta",
                   "sources": [src("broadplan_static")]},
    "program_title": {"label": "회차/특집 제목", "period": "meta", "role": "meta",
                   "sources": [src("idx_broadplan"), src("broadplan_static")]},
    "guestname":  {"label": "게스트", "period": "meta", "role": "meta",
                   "sources": [src("idx_broadplan"), src("broadplan_static")]},
    "daily_corner": {"label": "매일 코너", "period": "meta", "role": "meta",
                   "sources": [src("idx_broadplan"), src("broadplan_static")]},
    "weekly_corner": {"label": "주간 코너", "period": "meta", "role": "meta",
                   "sources": [src("idx_broadplan"), src("broadplan_static")]},
    "view_radio_yn": {"label": "보이는 라디오 여부", "period": "meta", "role": "meta",
                   "sources": [src("idx_broadplan"), src("broadplan_static")]},
    "live_yn":    {"label": "생방송/녹음 여부", "period": "meta", "role": "meta",
                   "sources": [src("idx_nike"), src("broadplan_static")]},
}

SUFFIX_VARIANTS = ["_prev", "_chg", "_diff", "_share"]


def main():
    out = {
        "_meta": {
            "purpose": "raas_kpi_latest.csv 필드 → 소스 룩업 → 정제작업/서머리인덱스 계보. "
                       "데이터 확인 기능이 결측/이상 필드의 원인 배치를 안내하는 데 사용.",
            "layer_a": "raas_kpi_save.spl (필드↔룩업, authority)",
            "layer_b": "RAAS온톨로지.xlsx / 'SummaryIndex 및 룩업_생성정보' (룩업↔정제작업·실행시간·서머리인덱스)",
            "key_rule": "키=최종 CSV 컬럼명(일간 접미사 없음). _prev/_chg/_diff/_share 변형은 base로 해석.",
            "job_note": "job = 스케줄러 로그 sourcetype=scheduler 의 savedsearch_name 과 대조할 정제작업명.",
        },
        "suffix_variants": SUFFIX_VARIANTS,
        "fields": FIELDS,
    }
    dest = Path(__file__).parent / "data" / "field_lineage.json"
    dest.parent.mkdir(exist_ok=True)
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {dest}  ({len(FIELDS)} fields)")


if __name__ == "__main__":
    main()
