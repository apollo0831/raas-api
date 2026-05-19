"""
RAAS raas_kpi_latest.csv 백필 러너
- raas_kpi_backfill_template.spl을 읽어 토큰 치환 후 Splunk REST API로 실행
- 백필 기간의 각 날짜에 대해 1번씩 실행 (총 N회)
- 각 실행이 outputlookup append=true 로 누적

사용법:
  python raas_kpi_backfill_runner.py --start 2026/03/01 --end 2026/04/27
  python raas_kpi_backfill_runner.py --start 2026/03/01 --end 2026/03/01  # 1일치만 테스트
  python raas_kpi_backfill_runner.py --dry-run                            # SPL만 출력 (실행 안함)

사전 조건:
  1. .env에 SPLUNK_HOST, SPLUNK_USER, SPLUNK_PASSWORD, SPLUNK_APP 설정
  2. raas_kpi_latest.csv 백업 후 비어있는 상태 또는 사용자가 원하는 시작 상태
  3. raas_kpi_backfill_template.spl 파일이 같은 폴더에 있어야 함

토큰 치환 규칙:
  $BACKFILL_DATE$       → '2026/03/01' 형식
  $BACKFILL_DATE_NEXT$  → 다음날 '2026/03/02' 형식
  $BACKFILL_TS$         → 백필일 자정 epoch (예: 1772668800)
  $BACKFILL_TS_NEXT$    → 다음날 자정 epoch
"""

import argparse
import base64
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
load_dotenv()

# ── 설정 ─────────────────────────────────────────────────
SPLUNK_HOST     = os.getenv("SPLUNK_HOST")
SPLUNK_USER     = os.getenv("SPLUNK_USER")
SPLUNK_PASSWORD = os.getenv("SPLUNK_PASSWORD")
SPLUNK_APP      = os.getenv("SPLUNK_APP")

# 한국 시간대 (Splunk 인덱스가 KST 기준이라고 가정)
KST = timezone(timedelta(hours=9))

# SPL 템플릿 파일
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "raas_kpi_backfill_template.spl")

# SSL 설정 (자체 서명 인증서 허용)
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

# Splunk REST API 타임아웃 (초). 한 번 실행에 1~5분 걸릴 수 있음
SPLUNK_TIMEOUT = 600

# 실행 간격 (초). Splunk 부하 분산용
SLEEP_BETWEEN_RUNS = 5


def splunk_auth():
    """Basic Auth 헤더 생성"""
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()
    ).decode()


def parse_date(s):
    """'2026/03/01' 또는 '2026-03-01' 형식 파싱"""
    s = s.replace("-", "/").strip()
    return datetime.strptime(s, "%Y/%m/%d").replace(tzinfo=KST)


def date_range(start, end):
    """start ~ end 포함 날짜 리스트"""
    days = (end.date() - start.date()).days + 1
    return [start + timedelta(days=i) for i in range(days)]


def render_template(template_text, target_dt):
    """템플릿에 토큰 치환

    target_dt: datetime, KST timezone
    """
    next_dt = target_dt + timedelta(days=1)

    # 자정 epoch (KST 기준)
    midnight = target_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    next_midnight = next_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    return (template_text
            .replace("$BACKFILL_DATE$", target_dt.strftime("%Y/%m/%d"))
            .replace("$BACKFILL_DATE_NEXT$", next_dt.strftime("%Y/%m/%d"))
            .replace("$BACKFILL_TS_NEXT$", str(int(next_midnight.timestamp())))
            .replace("$BACKFILL_TS$", str(int(midnight.timestamp())))
            )


def execute_splunk_search(spl):
    """Splunk REST API search/jobs/export 호출"""
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{SPLUNK_APP}/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl,
        "output_mode": "json",
        "count": 0,
        "exec_mode": "blocking",  # 동기 실행
    }).encode()

    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", splunk_auth())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=SPLUNK_TIMEOUT) as resp:
            result_count = 0
            for line in resp:
                line = line.decode("utf-8").strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("result"):
                        result_count += 1
                    if obj.get("preview") is False:
                        # 검색 완료 신호
                        pass
                except Exception:
                    pass
            return result_count
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Splunk HTTP {e.code}: {body[:500]}")
    except Exception as e:
        raise RuntimeError(f"Splunk 호출 실패: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="raas_kpi_latest.csv 백필 러너"
    )
    parser.add_argument("--start", required=True,
                        help="백필 시작일 (YYYY/MM/DD 또는 YYYY-MM-DD)")
    parser.add_argument("--end", required=True,
                        help="백필 종료일 (포함)")
    parser.add_argument("--dry-run", action="store_true",
                        help="SPL만 출력하고 실행하지 않음")
    parser.add_argument("--single-output", action="store_true",
                        help="--dry-run과 함께: 첫 날짜만 출력")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="실패한 날짜가 있어도 다음 날짜로 진행")
    args = parser.parse_args()

    # 템플릿 로드
    if not os.path.exists(TEMPLATE_PATH):
        print(f"❌ 템플릿 파일 없음: {TEMPLATE_PATH}", file=sys.stderr)
        sys.exit(1)

    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    # 날짜 범위 결정
    start_dt = parse_date(args.start)
    end_dt = parse_date(args.end)
    if end_dt < start_dt:
        print("❌ end가 start보다 이전입니다", file=sys.stderr)
        sys.exit(1)

    dates = date_range(start_dt, end_dt)
    print(f"📅 백필 대상: {start_dt.strftime('%Y/%m/%d')} ~ {end_dt.strftime('%Y/%m/%d')} ({len(dates)}일)")

    # 환경변수 검증 (--dry-run 아닐 때만)
    if not args.dry_run:
        missing = []
        if not SPLUNK_HOST: missing.append("SPLUNK_HOST")
        if not SPLUNK_USER: missing.append("SPLUNK_USER")
        if not SPLUNK_PASSWORD: missing.append("SPLUNK_PASSWORD")
        if not SPLUNK_APP: missing.append("SPLUNK_APP")
        if missing:
            print(f"❌ .env에 다음 변수 누락: {', '.join(missing)}", file=sys.stderr)
            sys.exit(1)

    # 사용자 확인 (안전장치)
    if not args.dry_run:
        print()
        print("⚠️  주의: 이 작업은 raas_kpi_latest.csv 룩업에 데이터를 누적합니다.")
        print("   기존 데이터와 중복되는 날짜는 두 번 쌓일 수 있으니,")
        print("   백필 전에 룩업을 백업/초기화 했는지 확인하세요.")
        print()
        confirm = input(f"진짜 {len(dates)}일치 백필을 시작할까요? (yes/no): ").strip().lower()
        if confirm not in ("yes", "y"):
            print("취소됨.")
            sys.exit(0)

    # 실행
    success = 0
    failed = []
    started = time.time()

    for i, dt in enumerate(dates, 1):
        date_str = dt.strftime("%Y/%m/%d")
        print(f"\n[{i}/{len(dates)}] {date_str} 처리 중...", flush=True)

        spl = render_template(template, dt)

        if args.dry_run:
            print(f"  🔍 dry-run: SPL 길이 {len(spl):,}자, 토큰 치환 결과:")
            print(f"     - $BACKFILL_DATE$ → {date_str}")
            next_dt = dt + timedelta(days=1)
            print(f"     - $BACKFILL_DATE_NEXT$ → {next_dt.strftime('%Y/%m/%d')}")
            midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            print(f"     - $BACKFILL_TS$ → {int(midnight.timestamp())}")
            next_midnight = next_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            print(f"     - $BACKFILL_TS_NEXT$ → {int(next_midnight.timestamp())}")

            if args.single_output:
                # 첫 날짜의 치환된 SPL을 파일로 저장
                out_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f"raas_kpi_backfill_{dt.strftime('%Y%m%d')}.spl"
                )
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(spl)
                print(f"  💾 치환된 SPL 저장: {out_path}")
                break
            continue

        # 실제 실행
        t0 = time.time()
        try:
            result_count = execute_splunk_search(spl)
            elapsed = time.time() - t0
            print(f"  ✅ 완료 ({elapsed:.1f}초, {result_count}행 출력)")
            success += 1
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  ❌ 실패 ({elapsed:.1f}초): {e}")
            failed.append((date_str, str(e)))
            if not args.continue_on_error:
                print(f"\n중단됨 ({success}/{len(dates)} 성공). --continue-on-error 로 재시도 가능.")
                break

        # 부하 분산 슬립
        if i < len(dates):
            time.sleep(SLEEP_BETWEEN_RUNS)

    # 결과 요약
    total_elapsed = time.time() - started
    print()
    print("="*60)
    print(f"📊 백필 완료 요약")
    print("="*60)
    print(f"전체 기간: {start_dt.strftime('%Y/%m/%d')} ~ {end_dt.strftime('%Y/%m/%d')}")
    print(f"성공: {success} / {len(dates)}")
    if failed:
        print(f"실패: {len(failed)}")
        for d, err in failed:
            print(f"  - {d}: {err[:100]}")
    print(f"소요 시간: {total_elapsed:.1f}초 ({total_elapsed/60:.1f}분)")
    print()

    if not args.dry_run:
        print("다음 단계:")
        print("  1. Splunk Search Manager에서 룩업 확인:")
        print("     | inputlookup raas_kpi_latest.csv | stats count by DATE | sort DATE")
        print("  2. 행수 검증: 기대값 = 37 PGM_CODE × {0}일 = {1:,}행".format(
            len(dates), 37 * len(dates)))
        print("  3. raas_server.py 재시작 → /api/timeline/meta 로 확인")


if __name__ == "__main__":
    main()
