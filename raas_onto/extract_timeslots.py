"""
CSV의 STIME 정보로부터 TimeSlot Turtle 자동 생성

- 채널별 프로그램 시작 시간을 정렬
- 다음 프로그램의 시작 시간을 종료 시간으로 자동 도출
- Daypart 분류 자동 적용
"""
import csv
import sys
from pathlib import Path
from collections import defaultdict
from raas_paths import get_csv_path

CSV_PATH = get_csv_path()

# ─── Daypart 분류 ──────────────────────────────────────────────────────────
# 시작 시간(시 단위) → daypart
def classify_daypart(hour):
    if 0 <= hour < 6:    return "LateNight"
    if 6 <= hour < 9:    return "MorningDrive"
    if 9 <= hour < 12:   return "Morning"
    if 12 <= hour < 14:  return "Midday"
    if 14 <= hour < 18:  return "AfternoonDrive"
    if 18 <= hour < 22:  return "Evening"
    if 22 <= hour < 24:  return "Night"
    return "Unknown"


def hhmm_to_label(hhmm):
    """0700 → '07:00'"""
    h = int(hhmm) // 100
    m = int(hhmm) % 100
    return f"{h:02d}:{m:02d}"


def main():
    # CSV에서 (PGM_CODE, STIME) 추출 — 최신 날짜 기준
    if CSV_PATH is None or not CSV_PATH.exists():
        print("[오류] raas_kpi_latest.csv 파일을 찾을 수 없습니다.", file=sys.stderr)
        print("       다음 위치 중 한 곳에 CSV를 두세요:", file=sys.stderr)
        print("       - 이 스크립트와 같은 폴더", file=sys.stderr)
        print("       - 상위 폴더의 data/ 디렉토리", file=sys.stderr)
        print("       또는 환경변수 RAAS_CSV_PATH를 지정하세요.", file=sys.stderr)
        sys.exit(1)

    program_times = {}  # code → stime
    program_channels = {}  # code → channel

    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        date_idx = header.index("DATE")
        code_idx = header.index("PGM_CODE")
        stime_idx = header.index("STIME")
        name_idx = header.index("PGM_NAME")

        # 최신 날짜 찾기
        latest_date = ""
        rows = list(reader)
        for row in rows:
            if row[date_idx] > latest_date:
                latest_date = row[date_idx]
        print(f"# 기준 날짜: {latest_date}")

        # 최신 날짜의 프로그램 추출
        for row in rows:
            if row[date_idx] != latest_date:
                continue
            code = row[code_idx]
            stime = row[stime_idx]
            name = row[name_idx]
            if not code or not stime or not name:
                continue
            # 채널 코드 (T00, F00, L00, G00, P00 등)는 제외
            if code in ("T00", "F00", "L00", "G00", "P00"):
                continue
            program_times[code] = stime
            # 채널 추정 (코드 첫 글자로)
            if code.startswith("F"):
                program_channels[code] = "F00"
            elif code.startswith("L"):
                program_channels[code] = "L00"
            elif code.startswith("M"):
                program_channels[code] = "L00"  # M은 러브FM 주말

    # 채널별로 시간 순 정렬
    channel_to_progs = defaultdict(list)
    for code, stime in program_times.items():
        ch = program_channels[code]
        channel_to_progs[ch].append((stime, code))

    # 종료 시간 계산
    timeslots = []  # (program_code, start, end, daypart)

    for ch, progs in channel_to_progs.items():
        # 일반 편성과 주말 편성 분리
        # M*는 주말 전용이므로 별도 처리
        regular = sorted([p for p in progs if not p[1].startswith("M")])
        weekend = sorted([p for p in progs if p[1].startswith("M")])

        # 일반 편성: 시간 순으로 정렬해 다음 시간이 종료 시간
        for i, (stime, code) in enumerate(regular):
            if i + 1 < len(regular):
                etime = regular[i + 1][0]
            else:
                # 마지막은 다음 날 첫 프로그램까지
                etime = regular[0][0]
            start_hour = int(stime) // 100
            timeslots.append({
                "code": code, "channel": ch,
                "start": stime, "end": etime,
                "daypart": classify_daypart(start_hour),
                "schedule": "Daily",
            })

        # 주말 편성
        for stime, code in weekend:
            # 주말 편성도 같은 채널의 일반 편성에서 종료 시간을 추정
            # (정확한 종료 시간은 별도 정보 필요 — 일단 2시간 가정)
            start_hour = int(stime) // 100
            end_hour = (start_hour + 2) % 24
            etime = f"{end_hour:02d}00"
            timeslots.append({
                "code": code, "channel": ch,
                "start": stime, "end": etime,
                "daypart": classify_daypart(start_hour),
                "schedule": "WeekendOnly",
            })

    # 결과 출력
    print(f"# 추출된 TimeSlot: {len(timeslots)}개\n")

    # Daypart별 카운트
    daypart_count = defaultdict(int)
    for t in timeslots:
        daypart_count[t["daypart"]] += 1
    print("# Daypart 분포:")
    for dp, n in sorted(daypart_count.items()):
        print(f"#   {dp}: {n}개")
    print()

    # 채널별 정렬 출력
    print("# ─── 채널별 시간표 ───")
    by_ch = defaultdict(list)
    for t in timeslots:
        by_ch[t["channel"]].append(t)
    for ch, ts in sorted(by_ch.items()):
        print(f"\n# {ch}")
        for t in sorted(ts, key=lambda x: (x["schedule"], x["start"])):
            print(f"#   {t['code']} {hhmm_to_label(t['start'])}~{hhmm_to_label(t['end'])} "
                  f"[{t['daypart']}, {t['schedule']}]")


if __name__ == "__main__":
    main()
