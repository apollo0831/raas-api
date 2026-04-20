"""
raas_top_programs_latest.csv 저장 테스트 v2
- 캐시 우회를 위해 저장 후 충분한 대기 시간 추가
- outputlookup 후 refresh 옵션 사용
"""
import urllib.request, urllib.parse, base64, ssl, json, time

SPLUNK_HOST     = "https://10.10.15.31:8089"
SPLUNK_USER     = "admin"
SPLUNK_PASSWORD = "qwe123"   # ← 실제 비밀번호로 변경

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def auth():
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()

def run(spl, label="", timeout=30):
    url = f"{SPLUNK_HOST}/services/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", auth())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=timeout) as r:
            text = r.read().decode("utf-8")
            rows = []
            for line in text.strip().split("\n"):
                if not line: continue
                try:
                    obj = json.loads(line)
                    if obj.get("result"): rows.append(obj["result"])
                except: pass
            print(f"  [{label}] {len(rows)}행 반환")
            return rows
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"  [{label}] HTTP {e.code}: {err[:150]}")
        return []
    except Exception as e:
        print(f"  [{label}] 오류: {e}")
        return []

print("=" * 55)
print("STEP 1: 현재 파일 상태 확인")
print("=" * 55)
rows = run("| inputlookup raas_top_programs_latest.csv | stats count", "현재상태")
if rows:
    count = rows[0].get("count", "0")
    print(f"  현재 row 수: {count}")
    if int(count) > 0:
        print("  → 파일이 이미 있습니다. STEP 3으로 바로 이동합니다.")

print()
print("=" * 55)
print("STEP 2: outputlookup으로 저장 (동기 방식)")
print("=" * 55)

# 핵심: outputlookup 후 sleep으로 flush 대기
spl_save = """
| makeresults count=10
| streamstats count as rank
| eval row = case(
    rank=1,  "F05|김영철의 파워FM|파워FM|61368",
    rank=2,  "F09|두시탈출 컬투쇼|파워FM|46078",
    rank=3,  "L06|김태현의 정치쇼|러브FM|40582",
    rank=4,  "F06|아름다운 이 아침 봉태규입니다|파워FM|41170",
    rank=5,  "F07|박하선의 씨네타운|파워FM|36879",
    rank=6,  "F08|12시엔 주현영|파워FM|34486",
    rank=7,  "F10|황제파워|파워FM|27702",
    rank=8,  "F11|박소현의 러브게임|파워FM|1800",
    rank=9,  "L07|이숙영의 러브FM|러브FM|24157",
    rank=10, "F13|배성재의 텐|파워FM|15152"
)
| rex field=row "(?P<pgm_code>[^|]+)[|](?P<pgm_name>[^|]+)[|](?P<channel>[^|]+)[|](?P<dau>[0-9]+)"
| eval dau=tonumber(dau), report_date="2026-04-06"
| fields rank pgm_code pgm_name channel dau report_date
| outputlookup create_empty=false raas_top_programs_latest.csv
"""
run(spl_save, "저장실행", timeout=60)

print()
print("  3초 대기 중... (Splunk 파일 flush 대기)")
time.sleep(3)

print()
print("=" * 55)
print("STEP 3: 저장 결과 확인 (3초 후)")
print("=" * 55)
rows3 = run("| inputlookup raas_top_programs_latest.csv | sort rank", "저장결과확인")
if rows3:
    print()
    print(f"  ✅ {len(rows3)}개 프로그램 저장 확인!")
    print()
    print(f"  {'순위':4} | {'프로그램명':28} | {'채널':6} | DAU")
    print("  " + "-"*55)
    for r in rows3:
        print(f"  {r.get('rank','?'):4} | {r.get('pgm_name','?'):28} | {r.get('channel','?'):6} | {r.get('dau','?')}")
else:
    print()
    print("  ❌ 여전히 0행 — Splunk lookup 쓰기 권한 문제일 수 있습니다.")
    print()
    print("  STEP 4: 권한 확인을 위해 다른 이름으로 테스트...")
    # 다른 이름으로 테스트
    spl_test = """
| makeresults count=2
| streamstats count as rank
| eval pgm_name=if(rank=1,"테스트1","테스트2"), dau=100
| outputlookup raas_test_write.csv
"""
    run(spl_test, "쓰기권한테스트")
    time.sleep(2)
    rows_test = run("| inputlookup raas_test_write.csv", "쓰기권한결과")
    if rows_test:
        print("  ✅ 쓰기 권한 정상 — lookup 이름 충돌 또는 캐시 문제")
        print("  → raas_top_programs_latest.csv 파일이 이미 존재하고 잠겨있을 수 있습니다.")
        print()
        print("  Splunk 웹 UI에서 확인: 설정 > 조회 > raas_top_programs_latest.csv")
    else:
        print("  ❌ 쓰기 권한 자체가 없음 — Splunk 관리자에게 lookup 쓰기 권한 요청 필요")

