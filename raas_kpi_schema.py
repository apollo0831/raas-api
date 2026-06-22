"""raas_kpi_latest.csv 컬럼 스키마 — 질문 분석용 컬럼 사전.

표준 지표 시계열 파이프라인(get_data_for_intent)이 직접 다루지 않는
'범주형/속성' 컬럼과, 그 컬럼으로 그룹 비교가 가능한 수치 지표를 정의한다.

용도: 질문 → 필요 컬럼 추출 → (실제 CSV에 존재하면) 데이터 첨부, 없으면 "없음" 안내.
"""

# 범주형/속성 컬럼 — group_by(그룹 비교) 또는 단순 노출 대상.
# 표준 KPI 파이프라인이 컨텍스트로 내보내지 않던 컬럼들.
ATTRIBUTE_COLUMNS = {
    'live_yn':       '생방송 여부 — 값: "생방송" 또는 "녹음"',
    'view_radio_yn': '보이는 라디오(영상 송출) 여부 — 값: "Y" 또는 "N"',
    'guestname':     '게스트(초대 출연자) 이름',
    'daily_corner':  '매일 진행되는 코너명',
    'weekly_corner': '요일별 주간 코너명',
    'program_title': '회차/특집 제목',
    'STIME':         '방송 시작 시각(HHMM)',
}

# 그룹 비교(value_metric)에 쓸 수 있는 수치 지표 컬럼.
GROUPABLE_METRICS = {
    'dau':       '일간 활성사용자',
    'wau':       '주간 활성사용자',
    'mau':       '월간 활성사용자',
    'dau_1min':  '1분이상 청취자(일)',
    'dau_10min': '10분이상 청취자(일)',
    'deep_rate': '깊은청취율',
    'real_rate': '실청취율',
    'new':       '신규 사용자(일)',
    'react':     '복귀 사용자(일)',
    'churn_rate':'이탈률(일)',
    'd1_ret':    'D1 유지율',
    'd7_ret':    'D7 유지율',
}


def schema_text() -> str:
    """LLM 컬럼 선택기에 넣을 스키마 설명 텍스트."""
    lines = ['[범주형/속성 컬럼]']
    for k, v in ATTRIBUTE_COLUMNS.items():
        lines.append(f'- {k}: {v}')
    lines.append('[그룹 비교용 수치 지표]')
    for k, v in GROUPABLE_METRICS.items():
        lines.append(f'- {k}: {v}')
    return '\n'.join(lines)
