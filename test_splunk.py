import splunklib.client as client
from dotenv import load_dotenv
import os

load_dotenv()

# Splunk 연결
service = client.connect(
    host=os.getenv("SPLUNK_HOST"),
    port=int(os.getenv("SPLUNK_PORT", 8089)),
    username=os.getenv("SPLUNK_USERNAME"),
    password=os.getenv("SPLUNK_PASSWORD")
)

print(f"✅ Splunk 연결 성공!")
print(f"Splunk 버전: {service.info['version']}")

# 간단한 검색 테스트
jobs = service.jobs
search_query = "search index=tempsummary6 | head 5"
job = jobs.create(search_query)

import time
while not job.is_done():
    time.sleep(1)

results = job.results()
print(f"✅ 검색 테스트 성공!")