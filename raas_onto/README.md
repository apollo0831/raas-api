# RAAS 온톨로지 (raas_onto)

RAAS의 **안정적 도메인 지식**을 TTL(Turtle/RDF)로 담는 계층. LLM-first grounding이
답변 생성 시 온톨로지 팩을 context에 주입한다 — *데이터 상태(매일 갱신)는 라이브 스캔,
불변 사실·규칙은 여기(TTL)*가 원칙(코드에 도메인 규칙 하드코딩 금지, 확장은 TTL 편집).

## 구성

### TTL 파일 (10종)
| 파일 | 담는 지식 |
|------|-----------|
| `raas_ontology_fields.ttl` | KPI 지표 정의·필드(정의·공식·단위·측정유형) |
| `raas_ontology_program.ttl` | 프로그램·채널·시간대·진행자·게스트명 정책 · 채널 편성성격(channelNature) · 동시방송 공리(DomainAxiom) |
| `raas_ontology_airing.ttl` | **프로그램 편성 이력**(종영분 `raas:ProgramAiring`, 불변) + 편성 해석 공리 3종 |
| `raas_ontology_time.ttl` | 시간대·데이파트·스케줄 유형 |
| `raas_ontology_noteworthy.ttl` | 특이사항(noteworthy) 정의 |
| `raas_ontology_calendar.ttl` | 캘린더·공휴일·특일 주석 |
| `raas_ontology_person.ttl` | 인물(진행자·게스트) |
| `raas_ontology_episode.ttl` | 에피소드 인스턴스 |
| `raas_ontology_contributed.ttl` | 승인된 기여 지식(지식 개선 루프 승격 배치가 생성) |
| `raas_ontology_cause.ttl` | 원인 분해 프레임워크(활성사용자 변화의 신규/복귀/이탈 구조) |

### 로더
- `raas_ontology_adapter.py` — `ONTOLOGY_FILES` 9종을 로드(`cause` 제외). `get_adapter()`로 싱글턴.
- `raas_metrics_engine.py`는 `fields + cause + calendar`를 별도 결합해 파생지표·분해에 사용.

## 어댑터 주요 API (`get_adapter()`)
- `get_program_meta(code)` — 프로그램 메타(라벨·별칭·채널·시간대·진행자·데이파트·유형)
- `get_metric_definitions_block()` — 지표 정의 텍스트 블록(meta scope 카탈로그)
- `get_channel_nature(code)` / `get_domain_axioms(channel_code)` — 채널 성격·도메인 공리
- `get_program_airings(channel_code, name_contains)` / `get_program_history_block(...)` — 편성 이력
- `get_calendar_annotations(start, end)` / `get_guestname_policy(code)` 등
- 저수준: `_onto.label_ko / get_one / value_str / instances_of`

## 프로그램 편성 이력 (airing) — 불변 사실 + 해석 공리
종영 프로그램 편성 연혁은 `raas:ProgramAiring`에 박제(과거는 불변). 자리(slot)=(채널, 분석 정시).
실제 편성시각은 `slotStartTime/End`에 원본 보존, 분석 자리는 `analysisSlotStart`(정시). 종료일=권위,
시작일=유도(같은 자리 직전 종료+1일). 현행 방송분은 여기 없음(라이브 broadplan 소유).

편성 해석 규칙은 **코드가 아니라 공리**로 두고 LLM이 적용한다(provider는 원자료만 제공):
- `HourSlotAnalysisAxiom` — 러브FM 정시 5분 뉴스 오프셋(09:05→09:00 자리), 시간 단위 분석
- `ContinuousSchedulingAxiom` — 24h 연속 편성: 각 시각 방송분 = 시작≤그시각 중 최근(다시간 편성 스패닝)
- `FranchiseRelocationAxiom` — 같은 쇼의 시간대 이동(정치쇼 9→10→7시), 'OOO의 러브FM' 정식명 구분

### TTL 재생성
`종방프로그램.csv`(CHN/SEQ/PGM_NAME/START·END_TIME/LASTDAY) → `gen_airing_ttl.py`로 생성.
빈 LASTDAY 행은 현행 방송분이라 제외. 공리도 이 스크립트의 PRE 블록에 서술 → 규칙 수정은 여기서.

```bash
python raas_onto/gen_airing_ttl.py   # raas_ontology_airing.ttl 재생성
```

## 지식 개선 루프 연동
사용자 기여 지식은 후보(candidate)/승인(approved)으로 격리(SQLite), 승인분이 승격 배치로
`raas_ontology_contributed.ttl`에 반영되어 read-time 병합된다(상세: `docs/knowledge_loop_design.md`).
