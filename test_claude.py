from anthropic import Anthropic
from dotenv import load_dotenv
import os

# .env 파일에서 API 키 로드
load_dotenv()

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

message = client.messages.create(
    model="claude-sonnet-4-6",
    max_tokens=1024,
    messages=[
        {"role": "user", "content": "안녕! 고릴라 라디오 앱 RAAS 시스템 개발을 시작할게. 간단히 인사해줘."}
    ]
)

print(message.content[0].text)