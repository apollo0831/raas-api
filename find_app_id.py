"""
Splunk 앱 내부 ID 확인 + 올바른 앱 컨텍스트로 outputlookup 테스트
실행: python find_app_id.py
"""
import urllib.request, urllib.parse, base64, ssl, json, time

SPLUNK_HOST     = "https://10.10.15.31:8089"
SPLUNK_USER     = "admin"
SPLUNK_PASSWORD = "qwe123"   # ← 변경

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

def auth():
    return "Basic " + base64.b64encode(
        f"{SPLUNK_USER}:{SPLUNK_PASSWORD}".encode()).decode()

def get(url, label=""):
    req = urllib.request.Request(url)
    req.add_header("Authorization", auth())
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"  [{label}] 오류: {e}")
        return {}

def run_in_app(spl, app_id, label="", timeout=30):
    """특정 앱 컨텍스트로 SPL 실행"""
    url = f"{SPLUNK_HOST}/servicesNS/nobody/{app_id}/search/jobs/export"
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
            print(f"  [{label}] {len(rows)}행")
            return rows
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"  [{label}] HTTP {e.code}: {err[:150]}")
        return []
    except Exception as e:
        print(f"  [{label}] 오류: {e}")
        return []


# ── STEP 1: 전체 앱 목록 조회 ──────────────────────────
print("=" * 55)
print("STEP 1: 설치된 Splunk 앱 목록")
print("=" * 55)
data = get(f"{SPLUNK_HOST}/services/apps/local?output_mode=json&count=50")
apps = data.get("entry", [])
print(f"  총 {len(apps)}개 앱 발견\n")
gorilla_apps = []
for app in apps:
    name    = app.get("name", "")
    label   = app.get("content", {}).get("label", "")
    version = app.get("content", {}).get("version", "")
    print(f"  내부ID: {name:30} | 표시명: {label}")
    if "gorilla" in name.lower() or "gorilla" in label.lower() \
       or "고릴라" in label or "raas" in name.lower() or "v4" in name.lower():
        gorilla_apps.append(name)


# ── STEP 2: 고릴라 앱 후보 특정 ────────────────────────
print()
print("=" * 55)
print("STEP 2: 고릴라분석 앱 후보")
print("=" * 55)
if gorilla_apps:
    for a in gorilla_apps:
        print(f"  ✅ {a}")
else:
    print("  자동 탐지 못함 — 앱 목록에서 직접 확인하세요")

# raas_briefing_latest.csv 가 있는 앱을 찾아 앱 ID 역추적
print()
print("=" * 55)
print("STEP 3: raas_briefing_latest.csv 소속 앱 확인")
print("(이 파일이 있는 앱이 고릴라분석 v4 앱입니다)")
print("=" * 55)
data2 = get(f"{SPLUNK_HOST}/services/data/lookup-table-files?output_mode=json&count=100&search=raas")
entries = data2.get("entry", [])
for e in entries:
    fname = e.get("name", "")
    app   = e.get("acl", {}).get("app", "?")
    owner = e.get("acl", {}).get("owner", "?")
    print(f"  파일: {fname}")
    print(f"  앱:   {app}  (owner: {owner})")
    print()

# ── STEP 4: 올바른 앱으로 outputlookup 테스트 ──────────
if entries:
    # raas_briefing_latest.csv 와 같은 앱에 저장
    target_app = None
    for e in entries:
        if "briefing" in e.get("name","") and e.get("acl",{}).get("app","") != "search":
            target_app = e.get("acl",{}).get("app","")
            break
    if not target_app and entries:
        target_app = entries[0].get("acl",{}).get("app","")

    if target_app:
        print("=" * 55)
        print(f"STEP 4: [{target_app}] 앱으로 outputlookup 테스트")
        print("=" * 55)
        spl = """
| makeresults count=3
| streamstats count as rank
| eval row=case(
    rank=1,"F05|김영철의 파워FM|파워FM|61368",
    rank=2,"F09|두시탈출 컬투쇼|파워FM|46078",
    rank=3,"L06|김태현의 정치쇼|러브FM|40582"
)
| rex field=row "(?P<pgm_code>[^|]+)[|](?P<pgm_name>[^|]+)[|](?P<channel>[^|]+)[|](?P<dau>[0-9]+)"
| eval dau=tonumber(dau), report_date="2026-04-07"
| fields rank pgm_code pgm_name channel dau report_date
| outputlookup raas_top_programs_latest.csv
"""
        run_in_app(spl, target_app, f"저장({target_app})", timeout=30)
        time.sleep(2)
        rows = run_in_app(
            "| inputlookup raas_top_programs_latest.csv | sort rank",
            target_app, f"확인({target_app})")
        if rows:
            print(f"\n  ✅ 성공! [{target_app}] 앱에 {len(rows)}행 저장됨")
            print(f"\n  → raas_morning_brief.py 에서 아래 설정 추가:")
            print(f'     APP_NAME = "{target_app}"')
            print(f'     url = f"{{SPLUNK_HOST}}/servicesNS/nobody/{{APP_NAME}}/search/jobs/export"')
        else:
            print(f"  ❌ 여전히 실패")
