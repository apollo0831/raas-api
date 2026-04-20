"""
raas_top_programs_latest.csv 진단 및 저장 테스트
실행: python test_top_programs.py
"""
import urllib.request, urllib.parse, base64, ssl, json

SPLUNK_HOST     = "https://10.10.15.31:8089"
SPLUNK_USER     = "admin"
SPLUNK_PASSWORD = "qwe123"   # ← 실제 비밀번호로 변경

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def auth():
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()

def run(spl, label=""):
    APP_NAME = "gorealra_v4"   # ← Splunk 내부 앱 ID 확인 필요
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{APP_NAME}/search/jobs/export"
    data = urllib.parse.urlencode({
        "search": spl, "output_mode": "json", "count": 0
    }).encode()
    req = urllib.request.Request(url, data=data)
    req.add_header("Authorization", auth())
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=30) as r:
            text = r.read().decode("utf-8")
            rows = []
            for line in text.strip().split("\n"):
                if not line: continue
                try:
                    obj = json.loads(line)
                    if obj.get("result"): rows.append(obj["result"])
                except: pass
            print(f"[{label}] 성공 — {len(rows)}행")
            if rows: print("  샘플:", json.dumps(rows[0], ensure_ascii=False))
            return rows
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"[{label}] HTTP {e.code}: {err[:200]}")
        return []
    except Exception as e:
        print(f"[{label}] 오류: {e}")
        return []

print("=" * 50)
print("STEP 1: 현재 raas_top_programs_latest.csv 상태")
print("=" * 50)
rows = run("| inputlookup raas_top_programs_latest.csv | head 3", "현재상태")

print()
print("=" * 50)
print("STEP 2: raas_top_programs_latest.csv 신규 생성 테스트")
print("=" * 50)

# makeresults count + streamstats 방식 (중첩 없음)
spl = """
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
    rank=8,  "F11|박소현의 러브게임|파워FM|26034",
    rank=9,  "L07|이숙영의 러브FM|러브FM|24157",
    rank=10, "F13|배성재의 텐|파워FM|15152"
)
| rex field=row "(?P<pgm_code>[^|]+)[|](?P<pgm_name>[^|]+)[|](?P<channel>[^|]+)[|](?P<dau>[0-9]+)"
| eval dau=tonumber(dau), report_date="2026-04-06"
| fields rank pgm_code pgm_name channel dau report_date
| outputlookup raas_top_programs_latest.csv
"""
run(spl, "저장실행")

print()
print("=" * 50)
print("STEP 3: 저장 결과 확인")
print("=" * 50)
rows2 = run("| inputlookup raas_top_programs_latest.csv | sort rank", "저장결과")
if rows2:
    print()
    print("  순위  | 프로그램명                    | 채널   | DAU")
    print("  " + "-"*55)
    for r in rows2:
        print(f"  {r.get('rank','?'):4} | {r.get('pgm_name','?'):28} | {r.get('channel','?'):6} | {r.get('dau','?')}")
