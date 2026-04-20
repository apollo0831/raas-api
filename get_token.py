from google_auth_oauthlib.flow import InstalledAppFlow
import json

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
creds = flow.run_local_server(port=0)

# 토큰 저장
with open('token.json', 'w') as f:
    f.write(creds.to_json())

print("✅ 토큰 발급 완료! token.json 저장됨")
print("Access Token:", creds.token[:30], "...")