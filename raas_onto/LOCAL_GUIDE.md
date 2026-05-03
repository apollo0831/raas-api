# 로컬 환경 실행 가이드 (Phase 5-A 포함)

## 빠른 시작 (3분)

### 1단계: ZIP 압축 해제

```bash
unzip raas_onto_poc.zip
cd raas_onto/
```

### 2단계: CSV 데이터 위치

`raas_kpi_latest.csv`를 다음 중 한 곳에 두세요. 우선순위 순:

1. 환경변수 `RAAS_CSV_PATH` 지정
2. `raas_onto/` 같은 폴더 (가장 쉬움)
3. `../data/raas_kpi_latest.csv` (Git 관리에 적합)
4. `/mnt/user-data/uploads/raas_kpi_latest.csv` (Anthropic 환경)

### 3단계: 의존성

표준 라이브러리만 사용. **별도 설치 불필요**. Python 3.10 이상.

```bash
python3 --version  # 3.10 이상이면 OK
```

### 4단계: 실행

```bash
# 경로 인식 확인
python3 raas_paths.py

# 통합 검증 (20/20 PASS)
python3 validate_full.py

# 어댑터 자체 테스트
python3 raas_ontology_adapter.py

# 회귀 테스트 (45/45 PASS) ⭐ Phase 5-A 핵심
python3 phase5_regression_test.py

# 데모 (가장 흥미로운 순서)
python3 example_usage_v5.py  # 캘린더/DayType
python3 example_usage_v4.py  # 비즈니스 룰
python3 example_usage_v3.py  # 시간/Daypart
python3 example_usage_v2.py  # 도메인 엔티티
python3 example_usage.py     # KPI
```

## Phase 5-B를 Claude Code에서 진행하려면

### 준비물

1. **이 ZIP 압축 해제 결과** (`raas_onto/` 폴더)
2. **RAAS 저장소** (briefing_engine.py, query_engine.py 등이 있는 곳)
3. **Claude Code 환경** (CLI 또는 IDE 통합)

### 작업 디렉토리 구조 권장

```
~/work/raas_integration/
├── raas/                       ← RAAS 운영 코드 (Git clone)
│   ├── raas_briefing_engine.py
│   ├── raas_query_engine.py
│   ├── raas_server.py
│   ├── raas_web.html
│   └── ...
└── raas_onto/                  ← 이 PoC (ZIP 압축 해제)
    ├── PHASE5_INTEGRATION_PLAN.md  ← Claude Code에 입력
    ├── raas_ontology_adapter.py
    ├── phase5_regression_test.py
    ├── *.ttl
    └── ...
```

### Claude Code 세션 시작

새 Claude Code 세션을 열고 다음 프롬프트로 시작:

```
~/work/raas_integration 디렉토리에서 작업합니다.

raas_onto/PHASE5_INTEGRATION_PLAN.md 의 설계에 따라 
raas/ 디렉토리의 RAAS 시스템에 온톨로지를 통합해주세요.

진행 방식:
- Step 1부터 차례로 (5-A 문서의 챕터 6 참조)
- 각 Step 완료 후 phase5_regression_test.py 실행 → 45 PASS 확인 후 다음 Step
- Git 커밋은 Step별로 분리
- 회귀가 발생하면 즉시 롤백 후 원인 분석

먼저 PHASE5_INTEGRATION_PLAN.md를 읽고 작업 계획을 정리해주세요.
```

Claude Code가 자동으로:
- 설계 문서 읽기
- 작업 계획 정리
- Step 1부터 차례로 실행
- 매 Step마다 회귀 테스트
- Git 커밋 분리

## 자주 묻는 질문

### Q1. 어댑터를 단독으로 테스트하려면?

```bash
python3 raas_ontology_adapter.py
```

10가지 자체 테스트가 출력됩니다.

### Q2. 회귀 테스트의 45개 항목이 뭔가요?

- KPI 변환 (RT-01~04): 4개
- 프로그램 매칭 (RT-05~08): 4개
- 비즈니스 룰 (RT-09~12): 4개 + 세부 = 16개
- 신규 능력 (NT-01~06): 13개
- 성능 (PT-01~04): 4개
- 합계: 45개

### Q3. 환경변수 설정 방법

```bash
# Linux/macOS
export RAAS_ONTOLOGY_DIR=/path/to/raas_onto
export RAAS_CSV_PATH=/path/to/raas_kpi_latest.csv

# Windows (PowerShell)
$env:RAAS_ONTOLOGY_DIR="C:\path\to\raas_onto"
```

### Q4. rdflib 도입을 권장하는 시점?

- PoC 단계: 표준 라이브러리만으로 충분 (현재 상태)
- 운영 1개월 후: 안정화 확인 후 검토
- 다음 단계 확장: SPARQL 쿼리 사용 시 도입

```bash
pip install rdflib
```

도입 후 `validate.py`의 `parse_turtle` 함수를 표준 RDF 파서로 교체.

## 문제 발생 시

```bash
# 환경 정보 수집
python3 --version
python3 raas_paths.py
python3 raas_ontology_adapter.py 2>&1 | head -20

# 디버그 정보와 함께 실행
RAAS_ONTOLOGY_DIR=$PWD python3 phase5_regression_test.py
```
