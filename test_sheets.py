from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
SPREADSHEET_ID = '1DSHPRg8Fg7Ljotn-kZIZytGA3mlTYGQkquDK8gQsdwA'

# ── 토큰 로드 및 갱신 ──────────────────────────
creds = Credentials.from_authorized_user_file('token.json', SCOPES)
if creds.expired and creds.refresh_token:
    creds.refresh(Request())
    print("✅ 토큰 갱신 완료")

service = build('sheets', 'v4', credentials=creds)
sheets = service.spreadsheets()

# ── 읽기 테스트 ────────────────────────────────
print("\n========== 읽기 테스트 ==========")
result = sheets.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range='SummaryIndex 및 룩업_생성정보!A1:P5'
).execute()
rows = result.get('values', [])
print(f"✅ SummaryIndex 탭 읽기 성공 - {len(rows)}행 읽음")
for i, row in enumerate(rows):
    print(f"  행{i+1}: {row}")

# ── Splunk 탭 읽기 ─────────────────────────────
print("\n========== Splunk_룩업매핑 읽기 ==========")
result2 = sheets.values().get(
    spreadsheetId=SPREADSHEET_ID,
    range='Splunk_룩업매핑!A1:F5'
).execute()
rows2 = result2.get('values', [])
print(f"✅ Splunk_룩업매핑 탭 읽기 성공 - {len(rows2)}행 읽음")
for i, row in enumerate(rows2):
    print(f"  행{i+1}: {row}")

# ── 쓰기 테스트 ────────────────────────────────
print("\n========== 쓰기 테스트 ==========")
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
write_result = sheets.values().append(
    spreadsheetId=SPREADSHEET_ID,
    range='변경이력!A1',
    valueInputOption='USER_ENTERED',
    body={'values': [[now, 'API 연동 테스트', 'Claude-Google Sheets API 읽기/쓰기 연결 확인 완료']]}
).execute()
print(f"✅ 쓰기 성공: {write_result.get('updates')}")

print("\n🎉 모든 테스트 완료! Claude ↔ Google Sheets 연동 정상 작동!")