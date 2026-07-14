# -*- coding: utf-8 -*-
"""캘린더 온톨로지(raas_ontology_calendar.ttl) 섹션 6 — CalendarDay 인스턴스 생성기.

한국 공휴일은 음력(설날·추석·부처님오신날)·대체공휴일 규칙이 복잡해 손으로 타이핑하면
오류가 나기 쉽다 → `holidays` 라이브러리(생성 시점 전용 의존성)에서 정확히 뽑는다.
런타임은 정적 TTL만 읽으므로 서버는 holidays 의존성이 없다.

  python tools/gen_calendar_ttl.py           # 섹션 6 재생성(제자리 교체)
  python tools/gen_calendar_ttl.py --print   # 표준출력으로만 미리보기

범위: 아카이브(2018/07~) 커버 + 향후 1년 = 2018..2027.
라이브러리에 없는 것(근로자의 날 등)은 아래 _MANUAL_SUPPLEMENT로 보충한다.
"""
import sys, datetime

YEARS = range(2018, 2028)          # 아카이브 2018/07~ + 향후 여유
CAL_FILE = "raas_onto/raas_ontology_calendar.ttl"

# 라이브러리(관공서 공휴일)에 없지만 청취 패턴상 의미 있는 날 — 매년 반복.
# 근로자의 날: 관공서는 정상근무이나 다수 근로자가 휴무 → 라디오 청취 패턴은 준휴일.
_MANUAL_SUPPLEMENT = {
    (5, 1): ("근로자의 날", "NationalHoliday", "HolidayDay"),
}

_DOW = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# 섹션 경계(이 두 앵커 사이만 교체 — 나머지 섹션·스키마는 불변)
_ANCHOR_START = "# 공휴일·대체공휴일만 명시적 인스턴스화\n"
_ANCHOR_END = ("\n\n# ============================================================"
               "=================\n# 7. BroadcastEvent")


def _classify(name):
    """공휴일 이름 → (holidayType, dayType). 대체/임시/법정 구분."""
    if "대체" in name:
        return "SubstituteHoliday", "SubstituteDay"
    if "임시" in name:
        return "TemporaryHoliday", "HolidayDay"
    return "NationalHoliday", "HolidayDay"


def _collect():
    """{date: (name, holidayType, dayType)} — 라이브러리 + 수동 보충 병합."""
    import holidays
    out = {}
    for y in YEARS:
        for d, name in holidays.SouthKorea(years=y).items():
            ht, dt = _classify(name)
            out[d] = (name, ht, dt)
        for (mm, dd), (name, ht, dt) in _MANUAL_SUPPLEMENT.items():
            day = datetime.date(y, mm, dd)
            out.setdefault(day, (name, ht, dt))   # 라이브러리 우선(중복 시 보충 무시)
    return dict(sorted(out.items()))


def _render(days):
    lines = []
    for d, (name, ht, dt) in days.items():
        iid = "raas:D_%04d_%02d_%02d" % (d.year, d.month, d.day)
        lines.append(
            "%s a raas:CalendarDay ;\n"
            "    raas:dateValue \"%s\" ; raas:dayOfWeek raas:%s ;\n"
            "    raas:dayType raas:%s ; raas:holidayType raas:%s ;\n"
            "    raas:holidayName \"%s\"@ko ."
            % (iid, d.isoformat(), _DOW[d.weekday()], dt, ht, name)
        )
    return "\n\n".join(lines)


def main():
    days = _collect()
    block = _render(days)
    header = ("# =============================================================================\n"
              "# 6. CalendarDay 인스턴스 — 한국 공휴일 (%d~%d)\n"
              "# =============================================================================\n"
              "# 명명 규칙: D_YYYY_MM_DD\n"
              "# 평일/주말은 별도 인스턴스 생성 안 함 (계산으로 도출 가능)\n"
              "# 공휴일·대체공휴일만 명시적 인스턴스화\n"
              "# 자동 생성: python tools/gen_calendar_ttl.py (holidays 라이브러리 + 근로자의 날 보충)\n"
              % (min(YEARS), max(YEARS)))
    yr = {}
    for d in days:
        yr[d.year] = yr.get(d.year, 0) + 1
    sys.stderr.write("연도별 건수: %s (총 %d일)\n" % (dict(sorted(yr.items())), len(days)))

    if "--print" in sys.argv:
        print(header + "\n" + block)
        return

    with open(CAL_FILE, "r", encoding="utf-8") as f:
        txt = f.read()
    i = txt.index(_ANCHOR_START)
    j = txt.index(_ANCHOR_END)
    # 섹션 6 헤더 전체를 새 헤더로, 인스턴스 블록을 새 블록으로 교체.
    # 헤더 시작 = 직전 '# ===' 구분선. 앵커_START가 포함된 헤더 블록의 시작을 찾는다.
    hstart = txt.rindex("# ==========", 0, i)
    hstart = txt.rindex("\n\n", 0, hstart) + 2
    new = txt[:hstart] + header + "\n" + block + txt[j:]
    with open(CAL_FILE, "w", encoding="utf-8") as f:
        f.write(new)
    sys.stderr.write("[gen_calendar_ttl] %s 섹션 6 교체 완료 (%d일)\n" % (CAL_FILE, len(days)))


if __name__ == "__main__":
    main()
