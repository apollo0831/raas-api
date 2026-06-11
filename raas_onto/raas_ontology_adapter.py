"""
RAAS Ontology Adapter — 참조 구현 (Reference Implementation)

이 모듈은 Phase 5-A 설계 문서의 명세를 따르는 참조 구현입니다.
Claude Code(Phase 5-B)에서 이 코드를 시작점으로 RAAS 시스템에 통합합니다.

설계 원칙:
- 표준 라이브러리만 사용 (PoC와 동일, rdflib 미사용)
- 싱글톤 캐시로 1회 로드
- 폴백 가능 (온톨로지 실패해도 서비스 지속)
- 호출 패턴 단순 (get_adapter() 하나로 모든 메서드 접근)

운영 환경 도입 시:
1. 이 파일을 RAAS 저장소에 복사
2. ONTOLOGY_DIR 경로를 운영 환경에 맞게 조정
3. 필요시 rdflib으로 파서 교체 (성능 개선)
4. 점진적 통합 (챕터 6 마이그레이션 전략 참조)
"""
from __future__ import annotations
import re
import os
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Optional, Any

logger = logging.getLogger(__name__)

# ─── 설정 ──────────────────────────────────────────────────────────────────

def _find_ontology_dir() -> Path:
    """온톨로지 파일이 있는 디렉토리를 우선순위에 따라 탐색.

    탐색 순서:
      1. 환경변수 RAAS_ONTOLOGY_DIR
      2. 이 모듈과 같은 디렉토리 (.ttl 파일이 있는지 확인)
      3. 같은 디렉토리의 raas_onto/ 하위 폴더
      4. 상위 디렉토리의 raas_onto/
    """
    env_dir = os.environ.get("RAAS_ONTOLOGY_DIR")
    if env_dir:
        return Path(env_dir)

    base = Path(__file__).parent
    candidates = [
        base,                     # 같은 폴더
        base / "raas_onto",       # 하위 raas_onto
        base.parent / "raas_onto",# 상위 raas_onto
    ]
    for c in candidates:
        # 첫 번째 .ttl 파일 존재 여부로 판정
        if (c / "raas_ontology_kpi.ttl").exists():
            return c

    # 못 찾으면 같은 폴더로 (어댑터 init 시 경고 발생)
    return base


ONTOLOGY_DIR = _find_ontology_dir()

ONTOLOGY_FILES = [
    "raas_ontology_kpi.ttl",
    "raas_ontology_program.ttl",
    "raas_ontology_time.ttl",
    "raas_ontology_noteworthy.ttl",
    "raas_ontology_calendar.ttl",
    "raas_ontology_person.ttl",
    "raas_ontology_episode.ttl",
]


# =============================================================================
# 1. Turtle 파서 (PoC에서 검증된 코드 재사용)
# =============================================================================

def tokenize(text: str) -> list[tuple[str, str]]:
    """Turtle 텍스트 → 토큰 리스트. PoC validate.py의 tokenize 그대로."""
    tokens = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c in " \t\r\n":
            i += 1
            continue
        if c == "#":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if c == "@":
            j = i
            while j < n:
                if text[j] == "<":
                    while j < n and text[j] != ">":
                        j += 1
                    j += 1
                    continue
                if text[j] == ".":
                    break
                j += 1
            i = j + 1
            continue
        if c == '"':
            j = i + 1
            while j < n:
                if text[j] == "\\" and j + 1 < n:
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                j += 1
            if j < n and text[j] == "@":
                while j < n and text[j] not in " \t\r\n,;.":
                    j += 1
            tokens.append(("STR", text[i:j]))
            i = j
            continue
        if c in ".,;":
            tokens.append(("PUNCT", c))
            i += 1
            continue
        if c == "<":
            j = i + 1
            while j < n and text[j] != ">":
                j += 1
            tokens.append(("TERM", text[i:j + 1]))
            i = j + 1
            continue
        j = i
        while j < n and text[j] not in " \t\r\n.,;\"<>#":
            j += 1
        tokens.append(("TERM", text[i:j]))
        i = j
    return tokens


def parse_triples(tokens: list[tuple[str, str]]) -> list[tuple[str, str, str]]:
    """토큰 → 트리플."""
    triples = []
    i, n = 0, len(tokens)
    while i < n:
        if tokens[i][0] != "TERM":
            i += 1
            continue
        subject = tokens[i][1]
        i += 1
        current_pred = None
        while i < n:
            t_type, t_val = tokens[i]
            if t_type == "PUNCT":
                if t_val == ".":
                    i += 1
                    break
                if t_val == ";":
                    current_pred = None
                    i += 1
                    continue
                if t_val == ",":
                    i += 1
                    continue
            else:
                if current_pred is None:
                    current_pred = t_val
                else:
                    triples.append((subject, current_pred, t_val))
                i += 1
    return triples


def parse_turtle(path: Path) -> list[tuple[str, str, str]]:
    text = path.read_text(encoding="utf-8")
    return parse_triples(tokenize(text))


# =============================================================================
# 2. Ontology — 그래프 조회 저수준 API
# =============================================================================

class Ontology:
    """온톨로지 그래프의 저수준 조회 인터페이스."""

    def __init__(self, files: list[Path]):
        self.triples: list[tuple[str, str, str]] = []
        for f in files:
            if not f.exists():
                logger.warning(f"온톨로지 파일 없음: {f}")
                continue
            self.triples.extend(parse_turtle(f))

        self.spo: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        self.types: dict[str, list[str]] = defaultdict(list)
        for s, p, o in self.triples:
            self.spo[s][p].append(o)
            if p in ("a", "rdf:type"):
                self.types[o].append(s)

    def get(self, subject: str, predicate: str) -> list[str]:
        return self.spo.get(subject, {}).get(predicate, [])

    def get_one(self, subject: str, predicate: str, default: Any = None) -> Optional[str]:
        vals = self.get(subject, predicate)
        return vals[0] if vals else default

    def instances_of(self, cls: str) -> list[str]:
        return self.types.get(cls, [])

    def find_subjects(self, predicate: str, obj: str) -> list[str]:
        return [s for s, p, o in self.triples if p == predicate and o == obj]

    @staticmethod
    def value_str(raw: Optional[str]) -> str:
        """문자열 리터럴에서 따옴표/언어 태그 제거."""
        if not raw:
            return ""
        return re.sub(r'^"|"(?:@\w+)?$', '', raw)

    def label_ko(self, subject: str) -> str:
        labels = self.get(subject, "rdfs:label")
        for lbl in labels:
            if "@ko" in lbl:
                return re.sub(r'^"|"@ko$', '', lbl)
        return labels[0] if labels else subject


# =============================================================================
# 3. OntologyAdapter — RAAS 코드용 고수준 API
# =============================================================================

class OntologyAdapter:
    """RAAS 운영 코드가 호출할 의미 단위 API."""

    # 변형 접미사 — Turtle 명세에서 자동 추출하는 것이 이상적이지만 PoC 호환
    VARIANT_SUFFIXES = ["_prev", "_chg", "_diff", "_share", "_d2"]

    def __init__(self, ontology_dir: Path = ONTOLOGY_DIR):
        files = [ontology_dir / f for f in ONTOLOGY_FILES]
        self._onto = Ontology(files)

        # 자주 사용되는 인덱스 미리 구축
        self._build_indices()

    def _build_indices(self):
        """O(1) 조회를 위한 인덱스 구축."""
        # field_name → MetricVariant
        self._field_to_variant: dict[str, str] = {}
        # 베이스 fieldName 길이 내림차순 (긴 것 우선 매칭)
        self._sorted_field_names: list[str] = []

        for s, p, o in self._onto.triples:
            if p == "raas:fieldName":
                fn = self._onto.value_str(o)
                self._field_to_variant[fn] = s

        self._sorted_field_names = sorted(
            self._field_to_variant.keys(), key=len, reverse=True
        )

        # csvField → 속성 IRI (메타데이터 필드 역방향 조회)
        self._csv_field_to_property: dict[str, str] = {}
        for s, p, o in self._onto.triples:
            if p == "raas:csvField":
                col = self._onto.value_str(o)
                if col:
                    self._csv_field_to_property[col] = s

        # 키워드 → Subject (Platform/Channel/Program 통합)
        self._keyword_index: dict[str, list[str]] = defaultdict(list)
        for cls in ("raas:Platform", "raas:Channel", "raas:Program"):
            for subj in self._onto.instances_of(cls):
                for label in self._all_labels(subj):
                    self._keyword_index[label].append(subj)

    def _all_labels(self, subject: str) -> list[str]:
        """rdfs:label + raas:altLabel 모든 텍스트."""
        result = []
        for p in ("rdfs:label", "raas:altLabel"):
            for v in self._onto.get(subject, p):
                clean = self._onto.value_str(v)
                if clean:
                    result.append(clean)
        return result

    # ─── 3.1 메타데이터 조회 ────────────────────────────────────────────────

    def get_field_info(self, field_name: str) -> Optional[dict]:
        """필드명 → 의미 정보. 챕터 3.3.1 참조."""
        # 변형 접미사 분리 (가장 긴 매칭 우선)
        base_fn = None
        variant_suffix = ""
        for fn in self._sorted_field_names:
            if field_name == fn:
                base_fn = fn
                break
            if field_name.startswith(fn + "_"):
                rem = field_name[len(fn):]
                if rem in self.VARIANT_SUFFIXES:
                    base_fn = fn
                    variant_suffix = rem
                    break

        if not base_fn:
            return None

        mv = self._field_to_variant.get(base_fn)
        if not mv:
            return None

        metric = self._onto.get_one(mv, "raas:ofMetric")
        granularity = self._onto.get_one(mv, "raas:granularity")

        result = {
            "field": field_name,
            "metric": self._metric_dict(metric) if metric else None,
        }
        if granularity:
            result["granularity"] = {
                "id": granularity,
                "label": self._onto.label_ko(granularity),
            }

        if variant_suffix:
            for v in self._onto.instances_of("raas:Variant"):
                if self._onto.value_str(self._onto.get_one(v, "raas:variantSuffix")) == variant_suffix:
                    result["variant"] = {"id": v, "label": self._onto.label_ko(v)}
                    break
        else:
            result["variant"] = {"id": "raas:Current", "label": "현재값"}

        return result

    def _metric_dict(self, metric_iri: str) -> dict:
        """Metric IRI → 정의 dict."""
        return {
            "id": metric_iri,
            "label": self._onto.label_ko(metric_iri),
            "unit": self._onto.value_str(self._onto.get_one(metric_iri, "raas:unit")),
            "measure_type": self._onto.value_str(
                self._onto.get_one(metric_iri, "raas:hasMeasureType", "")
            ).replace("raas:", ""),
            "definition": self._onto.value_str(self._onto.get_one(metric_iri, "raas:definition")),
            "formula": self._onto.value_str(self._onto.get_one(metric_iri, "raas:formula")),
        }

    def get_metric_definitions_block(self) -> str:
        """TTL raas:definition/formula/rdfs:comment 기반 지표 정의 텍스트 블록.
        LLM 시스템 프롬프트의 [지표 정의] 섹션을 대체한다."""
        GROUPS = [
            ("사용자 규모", [
                ("raas:DAU_Day",   None),
                ("raas:DAU_Week",  None),
                ("raas:DAU_Month", None),
                ("raas:DAU_R7",    None),
                ("raas:DAU_R30",   None),
            ]),
            ("사용자 흐름 (신규/복귀/이탈)", [
                ("raas:NewUser",         "raas:NewUser"),
                ("raas:ReactivatedUser", "raas:ReactivatedUser"),
                ("raas:ReactRate",       "raas:ReactRate"),
                ("raas:ChurnRate",       "raas:ChurnRate"),
            ]),
            ("청취 품질", [
                ("raas:RealListenRate",  "raas:RealListenRate"),
                ("raas:DeepListenRate",  "raas:DeepListenRate"),
                ("raas:EngageRate",      "raas:EngageRate"),
                ("raas:HabitRate",       "raas:HabitRate"),
            ]),
            ("유지율", [
                ("raas:RetentionRate_D1_All",     None),
                ("raas:RetentionRate_D7_All",     None),
                ("raas:RetentionRate_W1_All",     None),
                ("raas:RetentionRate_M1_All",     None),
                ("raas:RetentionRate_D1_NewUser", None),
                ("raas:RetentionRate_D7_NewUser", None),
                ("raas:RetentionRate_W1_NewUser", None),
                ("raas:RetentionRate_M1_NewUser", None),
            ]),
        ]
        lines = [
            "[지표 체계 및 정의]",
            "- 지표 소개 시 반드시 아래 4개 범주로 분류하여 답변할 것: 사용자 규모 / 사용자 흐름(유입·이탈) / 청취 품질 / 유지율",
            "- 실청취자 = 1분이상 청취한 사용자 (dau_1min 필드) — '실청취자 수', '1분이상 청취자 수', 'dau_1min'은 동일한 값",
        ]
        for group_name, items in GROUPS:
            lines.append(f"\n{group_name}")
            for variant_iri, metric_iri in items:
                label = self._onto.label_ko(variant_iri)
                comment = self._onto.value_str(self._onto.get_one(variant_iri, "rdfs:comment"))
                if metric_iri:
                    m = self._metric_dict(metric_iri)
                    defn = m.get("definition") or comment or ""
                    formula = m.get("formula") or ""
                    label = label or m.get("label") or ""
                    line = f"- {label}: {defn}"
                    if formula:
                        line += f" (계산식: {formula})"
                else:
                    line = f"- {label}: {comment}" if comment else f"- {label}"
                lines.append(line)
        return "\n".join(lines)

    def get_all_fields(self) -> list[str]:
        """온톨로지에 정의된 모든 필드명 (변형 접미사 적용 전)."""
        return list(self._field_to_variant.keys())

    def get_metadata_field_info(self, csv_column: str) -> Optional[dict]:
        """CSV 컬럼명 → 메타데이터 속성 정보 (raas:csvField 어노테이션 기반).

        KPI 지표(raas:fieldName)가 아닌 메타데이터 필드 전용.
        예: get_metadata_field_info("view_radio_yn") →
            {"csv_column": "view_radio_yn",
             "property": "raas:viewRadioYn",
             "label": "보는라디오 편성유무",
             "domain": "raas:Episode",
             "range": "xsd:string"}
        """
        prop_iri = self._csv_field_to_property.get(csv_column)
        if not prop_iri:
            return None

        domain = self._onto.get_one(prop_iri, "rdfs:domain")
        range_ = self._onto.get_one(prop_iri, "rdfs:range")
        comment = self._onto.value_str(self._onto.get_one(prop_iri, "rdfs:comment"))

        return {
            "csv_column": csv_column,
            "property": prop_iri,
            "label": self._onto.label_ko(prop_iri),
            "domain": domain,
            "range": range_,
            "comment": comment,
        }

    def get_all_metadata_fields(self) -> list[dict]:
        """raas:csvField로 연결된 모든 메타데이터 컬럼 목록."""
        return [
            self.get_metadata_field_info(col)
            for col in self._csv_field_to_property
        ]

    def get_legacy_field_aliases(self) -> dict[str, str]:
        """레거시 alias 매핑 (briefing_engine FIELD_ALIASES 호환).
        
        주의: PoC 단계에서는 비어있음. 운영 통합 시 .ttl에 legacy_alias 속성 추가.
        """
        # TODO: 온톨로지에 raas:legacyName 속성 추가 후 여기서 추출
        return {}

    # ─── 3.2 프로그램/채널 조회 ─────────────────────────────────────────────

    def find_program_by_keyword(self, keyword: str) -> list[dict]:
        """자연어 키워드로 검색. 챕터 3.3.2 참조."""
        _TYPE_PRIORITY = {"Program": 0, "Channel": 1, "Platform": 2}
        results = []
        seen_subjects = set()

        # 정확 일치 먼저
        for label, subjects in self._keyword_index.items():
            if label == keyword:
                for subj in subjects:
                    if subj in seen_subjects:
                        continue
                    seen_subjects.add(subj)
                    results.append(self._search_result(subj, label, score=1.0))

        if results:
            results.sort(key=lambda r: (
                -r["score"], _TYPE_PRIORITY.get(r["type"], 3)
            ))
            return results

        # 순방향 부분 일치: keyword가 label 안에 포함
        for label, subjects in self._keyword_index.items():
            if keyword in label:
                for subj in subjects:
                    if subj in seen_subjects:
                        continue
                    seen_subjects.add(subj)
                    score = len(keyword) / len(label)
                    results.append(self._search_result(subj, label, score=score))

        # 역방향 부분 일치: label이 keyword 안에 포함 (예: "김영철"이 "김영철의파워FM"에 포함)
        # Program 타입에 +0.2 보너스를 주어 채널명 중복 매칭보다 우선
        for label, subjects in self._keyword_index.items():
            if label in keyword and label != keyword:
                for subj in subjects:
                    if subj in seen_subjects:
                        continue
                    seen_subjects.add(subj)
                    cls = self._classify(subj)
                    type_bonus = 0.2 if cls == "Program" else 0.0
                    score = len(label) / len(keyword) + type_bonus
                    results.append(self._search_result(subj, label, score=score))

        results.sort(key=lambda r: (
            -r["score"], _TYPE_PRIORITY.get(r["type"], 3)
        ))
        return results

    def _search_result(self, subj: str, matched_label: str, score: float) -> dict:
        code = self._onto.value_str(self._onto.get_one(subj, "raas:code"))
        cls = self._classify(subj)
        return {
            "id": subj,
            "code": code,
            "type": cls,
            "label": self._onto.label_ko(subj),
            "matched_label": matched_label,
            "score": round(score, 2),
        }

    def _classify(self, subj: str) -> str:
        """Subject가 Platform/Channel/Program 중 무엇인지."""
        for cls in ("raas:Platform", "raas:Channel", "raas:Program"):
            if subj in self._onto.instances_of(cls):
                return cls.replace("raas:", "")
        return "Unknown"

    def get_program_meta(self, code: str) -> Optional[dict]:
        """프로그램 코드 → 모든 메타정보. 챕터 3.3.2 참조."""
        subj = f"raas:{code}"
        if subj not in self._onto.instances_of("raas:Program"):
            return None

        result = {
            "code": code,
            "id": subj,
            "label": self._onto.label_ko(subj),
            "alt_labels": [self._onto.value_str(v)
                           for v in self._onto.get(subj, "raas:altLabel")],
        }

        # 채널
        channel = self._onto.get_one(subj, "raas:belongsToChannel")
        if channel:
            ch_code = self._onto.value_str(self._onto.get_one(channel, "raas:code"))
            result["channel"] = {
                "id": channel,
                "code": ch_code,
                "label": self._onto.label_ko(channel),
            }

        # TimeSlot
        ts = self._onto.get_one(subj, "raas:hasTimeSlot")
        if ts:
            start = self._onto.value_str(self._onto.get_one(ts, "raas:startTime"))
            end = self._onto.value_str(self._onto.get_one(ts, "raas:endTime"))
            result["time_slot"] = {"start": start, "end": end}

            daypart = self._onto.get_one(ts, "raas:hasDaypart")
            if daypart:
                ad_value = self._onto.value_str(
                    self._onto.get_one(daypart, "raas:adValueLevel")
                )
                result["daypart"] = {
                    "id": daypart,
                    "label": self._onto.label_ko(daypart),
                    "ad_value": ad_value,
                }

        # ScheduleType
        sch = self._onto.get_one(subj, "raas:hasScheduleType")
        if sch:
            result["schedule_type"] = {
                "id": sch,
                "label": self._onto.label_ko(sch),
            }

        # Phase 2-C-3: ProgramType (Hosted/Automated)
        ptype = self._onto.get_one(subj, "raas:hasProgramType")
        if ptype:
            result["program_type"] = {
                "id": ptype,
                "label": self._onto.label_ko(ptype),
            }

        # Phase 2-C-3: MainHost
        host = self._onto.get_one(subj, "raas:hasMainHost")
        if host:
            result["main_host"] = self._person_dict(host)

        # Phase 2-C-3: RegularGuests
        regulars = []
        for s, p, o in self._onto.triples:
            if s == subj and p == "raas:hasRegularGuest":
                regulars.append(self._person_dict(o))
        if regulars:
            result["regular_guests"] = regulars

        return result

    def _person_dict(self, person_iri: str) -> dict:
        """Person IRI → 정보 dict."""
        return {
            "id": person_iri,
            "label": self._onto.label_ko(person_iri),
            "name": self._onto.value_str(self._onto.get_one(person_iri, "raas:personName")),
            "stage_name": self._onto.value_str(self._onto.get_one(person_iri, "raas:stageName")),
            "occupation": self._onto.value_str(self._onto.get_one(person_iri, "raas:personOccupation")),
        }

    # ─── Phase 2-C-3 신규 메서드 ────────────────────────────────────────────

    def find_person_by_name(self, name_keyword: str) -> list[dict]:
        """사람 이름 키워드로 Person 검색 + 어떤 프로그램에 어떤 역할로 출연하는지.

        예: find_person_by_name("정상근") → [
          {"person": {...}, "as_main_host": [], "as_regular_guest": ["F05"]}
        ]
        """
        results = []
        for person in self._onto.instances_of("raas:Person"):
            labels = [self._onto.label_ko(person)]
            for pred in ("raas:personName", "raas:stageName"):
                v = self._onto.get_one(person, pred)
                if v:
                    labels.append(self._onto.value_str(v))

            matched = False
            matched_label = None
            for lbl in labels:
                if lbl and name_keyword in lbl:
                    matched = True
                    matched_label = lbl
                    break

            if not matched:
                continue

            as_host = self._onto.find_subjects("raas:hasMainHost", person)
            as_regular = self._onto.find_subjects("raas:hasRegularGuest", person)

            results.append({
                "person": self._person_dict(person),
                "matched_label": matched_label,
                "as_main_host": [self._onto.value_str(self._onto.get_one(p, "raas:code"))
                                 for p in as_host],
                "as_regular_guest": [self._onto.value_str(self._onto.get_one(p, "raas:code"))
                                     for p in as_regular],
            })

        return results

    def get_program_type(self, code: str) -> Optional[dict]:
        """프로그램의 ProgramType 조회 (Hosted/Automated)."""
        subj = f"raas:{code}"
        ptype = self._onto.get_one(subj, "raas:hasProgramType")
        if not ptype:
            return None
        return {
            "id": ptype,
            "label": self._onto.label_ko(ptype),
            "is_automated": ptype == "raas:AutomatedProgram",
        }

    def get_guestname_policy(self, code: str) -> Optional[dict]:
        """프로그램의 guestname 해석 정책 조회."""
        subj = f"raas:{code}"
        policy = self._onto.get_one(subj, "raas:guestnameInterpretation")
        if not policy:
            return None
        return {
            "id": policy,
            "label": self._onto.label_ko(policy),
            "comment": self._onto.value_str(self._onto.get_one(policy, "rdfs:comment")),
            "example": self._onto.value_str(self._onto.get_one(policy, "raas:exampleText")),
        }

    # ─── Phase 6 (브리핑 강화) 신규 메서드 ──────────────────────────────────
    
    # 게스트 분석 캐시 (옵션 B — 메모리 캐시)
    _guest_cache: Optional[dict] = None
    
    def _get_regular_guest_names(self) -> set[str]:
        """온톨로지에 등록된 모든 RegularGuest 이름 (분석 제외용)."""
        names = set()
        for s, p, o in self._onto.triples:
            if p == "raas:hasRegularGuest":
                # o는 Person IRI
                label = self._onto.label_ko(o)
                if label:
                    names.add(label)
                # personName도 추가
                pname = self._onto.value_str(self._onto.get_one(o, "raas:personName"))
                if pname:
                    names.add(pname)
        return names
    
    def _parse_guestname_for_analysis(self, raw_text: str, regular_names: set[str]) -> list[str]:
        """guestname 텍스트에서 비고정 게스트 이름 추출.
        
        예:
            '김헌, 정상근, 피터 / 배혜지' + regular={'정상근'}
            → ['김헌', '피터', '배혜지']  (정상근 제외)
        """
        if not raw_text:
            return []
        # 정규화: '/'와 ',' 모두 구분자로 처리
        parts = raw_text.replace("/", ",").split(",")
        result = []
        for p in parts:
            name = p.strip()
            # 'DJ XXX', '직업 + 이름' 같은 케이스에서 이름만 추출 시도
            # 단순화: 공백 기준 마지막 단어를 이름으로 (한국어 케이스)
            # 'DJ 푸디토리움' → '푸디토리움', '김성훈 감독' → '김성훈' (직업 제거 향후 개선)
            if not name:
                continue
            if name.startswith("DJ "):
                name = name[3:].strip()
            # 직업 접미어 제거 (간단 버전)
            for suffix in [" 감독", " 변호사", " 배우", " 가수", " 성우", " 작가", " 의사", " 교수"]:
                if name.endswith(suffix):
                    name = name[:-len(suffix)].strip()
                    break
            
            if name and name not in regular_names:
                result.append(name)
        return result
    
    def _build_guest_cache_from_csv(self, csv_path: Path, days: int = 30) -> dict:
        """CSV에서 게스트 효과 데이터를 캐시로 구축.
        
        반환 구조:
        {
            "F05": {
                "without_guest": {"weekday": [(date, dau), ...], "weekend": [...]},
                "with_guest": {"weekday": [(date, dau, [guest_names]), ...], "weekend": [...]},
                "guest_appearances": {
                    "이름": [(date, dau, daytype), ...]
                }
            },
            ...
        }
        """
        import csv
        from datetime import datetime, timedelta
        from collections import defaultdict
        
        if not csv_path.exists():
            logger.warning(f"CSV 파일 없음: {csv_path}")
            return {}
        
        regular_names = self._get_regular_guest_names()
        
        result = defaultdict(lambda: {
            "without_guest": {"weekday": [], "weekend": [], "holiday": []},
            "with_guest": {"weekday": [], "weekend": [], "holiday": []},
            "guest_appearances": defaultdict(list),
        })
        
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            try:
                pgm_idx = header.index("PGM_CODE")
                gn_idx = header.index("guestname")
                dau_idx = header.index("dau")
                date_idx = header.index("DATE")
            except ValueError as e:
                logger.warning(f"CSV 컬럼 누락: {e}")
                return {}
            
            cutoff = None
            if days:
                cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            for row in reader:
                code = row[pgm_idx]
                if not code:
                    continue
                
                date_raw = row[date_idx].replace("/", "-") if row[date_idx] else ""
                if cutoff and date_raw < cutoff:
                    continue
                
                if not row[dau_idx]:
                    continue
                try:
                    dau = int(float(row[dau_idx]))
                except (ValueError, IndexError):
                    continue
                
                # DayType 판정
                daytype_info = self.get_day_type(date_raw)
                dt_key = "holiday" if daytype_info.get("day_type") == "공휴일" else (
                    "weekend" if daytype_info.get("day_type") == "주말" else "weekday"
                )
                
                gn = row[gn_idx].strip() if gn_idx < len(row) else ""
                has_guest = bool(gn) and gn not in ("None", "null", "-")
                
                if has_guest:
                    guest_names = self._parse_guestname_for_analysis(gn, regular_names)
                    if not guest_names:
                        # RegularGuest만 있는 경우 → 게스트 없는 날로 분류
                        result[code]["without_guest"][dt_key].append((date_raw, dau))
                    else:
                        result[code]["with_guest"][dt_key].append((date_raw, dau, guest_names))
                        for name in guest_names:
                            result[code]["guest_appearances"][name].append((date_raw, dau, dt_key))
                else:
                    result[code]["without_guest"][dt_key].append((date_raw, dau))
        
        return dict(result)
    
    def find_top_guests(self, code: str, days: int = 30, 
                        csv_path: Optional[Path] = None,
                        top_n: int = 5) -> list[dict]:
        """프로그램의 효과 큰 비고정 게스트 Top N (RegularGuest 자동 제외).
        
        예: find_top_guests("L07", days=30) →
        [
            {"name": "송유택", "appearances": 1, "avg_dau": 26072, 
             "baseline_dau": 18140, "effect_pct": 43.7},
            ...
        ]
        """
        if csv_path is None:
            # raas_paths가 있으면 활용
            try:
                from raas_paths import get_csv_path
                csv_path = get_csv_path()
            except ImportError:
                pass
            
            if csv_path is None:
                # 자동 탐색 (폴백)
                base = Path(__file__).parent
                for candidate in [base / "raas_kpi_latest.csv",
                                  base.parent / "raas_kpi_latest.csv",
                                  base.parent / "data" / "raas_kpi_latest.csv",
                                  Path("/mnt/user-data/uploads/raas_kpi_latest.csv")]:
                    if candidate.exists():
                        csv_path = candidate
                        break
            
            if csv_path is None:
                logger.warning("CSV 파일을 찾을 수 없음")
                return []
        
        # 캐시 확인
        if self._guest_cache is None:
            self._guest_cache = self._build_guest_cache_from_csv(csv_path, days=days)
        
        prog_data = self._guest_cache.get(code)
        if not prog_data:
            return []
        
        # 게스트 없는 날의 DayType별 평균 (baseline)
        baselines = {}
        for dt_key in ["weekday", "weekend", "holiday"]:
            samples = prog_data["without_guest"][dt_key]
            if samples:
                baselines[dt_key] = sum(d for _, d in samples) / len(samples)
        
        # 게스트별 효과 계산
        guest_stats = []
        for name, appearances in prog_data["guest_appearances"].items():
            # DayType별 그룹화
            by_dt = {"weekday": [], "weekend": [], "holiday": []}
            for date, dau, dt_key in appearances:
                by_dt[dt_key].append(dau)
            
            # 가장 표본 많은 DayType으로 비교
            best_dt = max(by_dt.keys(), key=lambda k: len(by_dt[k]))
            if not by_dt[best_dt] or best_dt not in baselines:
                continue
            
            avg = sum(by_dt[best_dt]) / len(by_dt[best_dt])
            baseline = baselines[best_dt]
            if baseline <= 0:
                continue
            effect_pct = (avg - baseline) / baseline * 100
            
            guest_stats.append({
                "name": name,
                "appearances": len(appearances),
                "appearances_in_best_dt": len(by_dt[best_dt]),
                "avg_dau": int(avg),
                "baseline_dau": int(baseline),
                "baseline_daytype": best_dt,
                "effect_pct": round(effect_pct, 1),
            })
        
        # 효과 순위 (양의 효과 우선, 표본 2회 이상)
        guest_stats.sort(key=lambda x: (-x["appearances"], -x["effect_pct"]))
        return guest_stats[:top_n]
    
    def detect_consecutive_trend(self, values: list, threshold_pct: float = 1.0) -> dict:
        """N일치 값에서 연속 같은 방향 변화 감지.
        
        예: detect_consecutive_trend([100, 95, 90, 88, 85])
            → {"direction": "down", "consecutive_days": 4, "first_value": 100, "last_value": 85, ...}
        """
        if len(values) < 2:
            return {"direction": "unknown", "consecutive_days": 0}
        
        # 끝에서부터 거꾸로 같은 방향이 몇 일째인지
        consecutive = 0
        direction = None
        
        for i in range(len(values) - 1, 0, -1):
            curr = values[i]
            prev = values[i - 1]
            if prev is None or curr is None or prev == 0:
                break
            change_pct = (curr - prev) / prev * 100
            
            if abs(change_pct) < threshold_pct:
                # 변화 너무 작음 → 추세 아님
                break
            
            curr_dir = "up" if change_pct > 0 else "down"
            if direction is None:
                direction = curr_dir
                consecutive = 1
            elif direction == curr_dir:
                consecutive += 1
            else:
                break
        
        if consecutive == 0:
            return {"direction": "stable", "consecutive_days": 0}
        
        first_v = values[-(consecutive + 1)]
        last_v = values[-1]
        total_change_pct = ((last_v - first_v) / first_v * 100) if first_v else 0
        
        return {
            "direction": direction,
            "consecutive_days": consecutive,
            "first_value": first_v,
            "last_value": last_v,
            "total_change_pct": round(total_change_pct, 1),
        }
    
    def select_notable_programs(self, all_kpi: dict[str, dict], 
                                max_count: int = 2) -> list[dict]:
        """오늘 주목할 프로그램 1~2개 자동 선정.
        
        선정 기준 (우선순위 순):
        1. WoW 급증 (+10% 이상) — 양의 시그널
        2. WoW 급락 (-10% 이상) — 위험 시그널
        3. WoW +/-5~10% (중간 변화)
        """
        candidates = []
        
        for code, kpi in all_kpi.items():
            # DAU 1,000 이상만 (저트래픽 제외)
            try:
                dau = float(kpi.get("dau", 0) or 0)
                wow = float(kpi.get("dau_chg") or kpi.get("dau_wow") or 0)
            except (TypeError, ValueError):
                continue
            
            if dau < 1000:
                continue
            
            # 우선순위 점수
            if abs(wow) >= 10:
                priority = 1  # 급변
            elif abs(wow) >= 5:
                priority = 2  # 중변
            else:
                continue  # 보통은 제외
            
            meta = self.get_program_meta(code)
            candidates.append({
                "code": code,
                "label": meta.get("label", code) if meta else code,
                "dau": int(dau),
                "wow_pct": round(wow, 1),
                "direction": "up" if wow > 0 else "down",
                "priority": priority,
                "host": meta.get("main_host", {}).get("label") if meta else None,
            })
        
        # 우선순위 + 변화량 절대값 큰 순
        candidates.sort(key=lambda x: (x["priority"], -abs(x["wow_pct"])))
        return candidates[:max_count]
    
    def get_daytype_comparison(self, target_date: str) -> dict:
        """target_date와 같은 DayType 정보 반환 (브리핑 컨텍스트용).
        
        예: get_daytype_comparison("2026-05-02") → 
        {
            "date": "2026-05-02",
            "day_type": "주말",
            "comparison_text": "오늘은 토요일(주말). 평일 대비 DAU 감소는 정상 범위."
        }
        """
        info = self.get_day_type(target_date)
        if not info:
            return {}
        
        dt = info.get("day_type", "")
        weekday = info.get("day_of_week", "")
        holiday = info.get("holiday_name", "")
        
        # 텍스트 생성
        if dt == "공휴일" and holiday:
            text = f"오늘은 {weekday}이고 공휴일({holiday})입니다. 평일 대비 청취 패턴 차이가 큽니다."
        elif dt == "주말":
            text = f"오늘은 {weekday}({dt})입니다. 평일 대비 DAU 감소는 정상 범위입니다."
        else:
            text = f"오늘은 {weekday}({dt})입니다."
        
        return {
            "date": target_date,
            "day_type": dt,
            "day_of_week": weekday,
            "holiday_name": holiday,
            "comparison_text": text,
        }
    
    def reset_guest_cache(self) -> None:
        """게스트 캐시 무효화 (CSV 갱신 시 호출)."""
        self._guest_cache = None

    def get_all_programs(self) -> list[dict]:
        """모든 프로그램 목록."""
        result = []
        for subj in self._onto.instances_of("raas:Program"):
            code = self._onto.value_str(self._onto.get_one(subj, "raas:code"))
            if code:
                meta = self.get_program_meta(code)
                if meta:
                    result.append(meta)
        return sorted(result, key=lambda x: x["code"])

    def get_programs_by_channel(self, channel_code: str) -> list[dict]:
        """채널의 모든 소속 프로그램."""
        ch_subj = f"raas:{channel_code}"
        result = []
        for subj in self._onto.find_subjects("raas:belongsToChannel", ch_subj):
            code = self._onto.value_str(self._onto.get_one(subj, "raas:code"))
            if code:
                meta = self.get_program_meta(code)
                if meta:
                    result.append(meta)
        return result

    # ─── 3.3 시간 정보 ──────────────────────────────────────────────────────

    def get_day_type(self, date_str: str) -> dict:
        """날짜 → 일자 유형. 챕터 3.3.3 참조."""
        norm_date = date_str.replace("/", "-")
        try:
            dt = datetime.strptime(norm_date, "%Y-%m-%d")
        except ValueError:
            return {}

        weekday_kr = ["월", "화", "수", "목", "금", "토", "일"][dt.weekday()]

        # 공휴일 체크
        for cd in self._onto.instances_of("raas:CalendarDay"):
            cd_date = self._onto.value_str(self._onto.get_one(cd, "raas:dateValue"))
            if cd_date == norm_date:
                day_type = self._onto.get_one(cd, "raas:dayType")
                holiday_name = self._onto.value_str(
                    self._onto.get_one(cd, "raas:holidayName")
                )
                return {
                    "date": norm_date,
                    "day_of_week": weekday_kr,
                    "day_type": self._onto.label_ko(day_type) if day_type else "?",
                    "day_type_id": day_type,
                    "holiday_name": holiday_name,
                    "is_workday": False,
                }

        # 평일/주말 자동 판정
        is_weekend = dt.weekday() >= 5
        return {
            "date": norm_date,
            "day_of_week": weekday_kr,
            "day_type": "주말" if is_weekend else "평일",
            "day_type_id": "raas:Weekend" if is_weekend else "raas:Weekday",
            "holiday_name": None,
            "is_workday": not is_weekend,
        }

    # ─── 3.4 비즈니스 룰 평가 ──────────────────────────────────────────────

    # 룰 ID → 평가 함수 매핑
    _ALERT_EVALUATORS = {
        "DauPlunge":         lambda k: k.get("dau_chg") if k.get("dau_chg") is not None and k["dau_chg"] <= -10 else None,
        "DauSurge":          lambda k: k.get("dau_chg") if k.get("dau_chg") is not None and k["dau_chg"] >= 10 else None,
        "DeepRatePlunge":    lambda k: k.get("deep_rate_diff") if k.get("deep_rate_diff") is not None and k["deep_rate_diff"] <= -3 else None,
        "NewUserPlunge":     lambda k: k.get("new_chg") if k.get("new_chg") is not None and k["new_chg"] <= -20 else None,
        "ChurnRateRise":     lambda k: k.get("churn_rate_diff") if k.get("churn_rate_diff") is not None and k["churn_rate_diff"] >= 3 else None,
        "ReactRateAchieved": lambda k: k.get("react_rate") if k.get("react_rate") is not None and k["react_rate"] >= 5 else None,
        "HabitRateAchieved": lambda k: k.get("habit_rate") if k.get("habit_rate") is not None and k["habit_rate"] >= 30 else None,
        "HabitRateLow":      lambda k: k.get("habit_rate") if k.get("habit_rate") is not None and k["habit_rate"] <= 15 else None,
    }

    def evaluate_platform_alerts(self, kpi: dict) -> list[dict]:
        """고정 임계값 알림 룰 평가 (z-score fallback용)."""
        alerts = []
        for rule in self._onto.instances_of("raas:PlatformAlert"):
            rule_id = rule.replace("raas:Alert_", "")
            if rule_id not in self._ALERT_EVALUATORS:
                continue

            evaluator = self._ALERT_EVALUATORS[rule_id]
            try:
                value = evaluator(kpi)
            except (TypeError, ValueError):
                continue
            if value is None:
                continue

            level = self._onto.value_str(self._onto.get_one(rule, "raas:alertLevel"))
            msg_template = self._onto.value_str(self._onto.get_one(rule, "raas:alertMessage"))
            msg = msg_template.replace("{value:+.1f}", f"{value:+.1f}")
            msg = msg.replace("{value:.1f}", f"{value:.1f}")

            alerts.append({
                "rule_id": rule,
                "level": level,
                "msg": msg,
                "value": value,
            })

        return alerts

    def evaluate_zscore_alerts(
        self,
        timeline: dict,
        ref_date: str,
        is_holiday: bool = False,
    ) -> list[dict]:
        """
        Robust Z-score 기반 알림 평가 (메인 엔진).

        Args:
            timeline:   {code: {date_str: row_dict}}
            ref_date:   평가 기준일
            is_holiday: True면 볼륨 하락 알림 억제

        Returns:
            통합 알림 리스트 [{code, label, level, msg, z, value, ...}]
        """
        try:
            from .raas_zscore_detector import get_detector
        except ImportError:
            return []

        detector = get_detector()
        raw_alerts = detector.evaluate(timeline, ref_date, is_holiday=is_holiday)

        # query_engine / format_for_claude 호환 포맷으로 변환
        formatted: list[dict] = []
        _level_emoji = {'red': '🔴', 'yellow': '🟡', 'green': '🟢'}
        _sign = lambda v: f'{v:+.2f}' if isinstance(v, float) else str(v)

        for a in raw_alerts:
            emoji = _level_emoji.get(a['level'], '')
            scope = {'platform': '플랫폼', 'channel': '채널', 'program': '프로그램'}.get(a['category'], '')
            code_str = f"[{a['code']}]" if a['category'] != 'platform' else ''
            msg = (
                f"{emoji} {a['label']}{code_str} {_sign(a['value'])} "
                f"(z={a['z']:+.1f}, 기준 {a['baseline_median']:+.2f})"
            )
            formatted.append({
                'rule_id':   f"zscore_{a['field']}_{a['code']}",
                'level':     a['level'],
                'msg':       msg,
                'value':     a['value'],
                'z':         a['z'],
                'code':      a['code'],
                'category':  a['category'],
                'field':     a['field'],
                'label':     a['label'],
                'period':    a['period'],
                'baseline_median': a['baseline_median'],
                'baseline_n':      a['baseline_n'],
            })

        return formatted

    def find_at_risk_programs(self, all_kpi: dict[str, dict]) -> list[dict]:
        """모든 프로그램 중 AtRiskProgram 분류."""
        result = []
        for prog in self._onto.instances_of("raas:Program"):
            code = self._onto.value_str(self._onto.get_one(prog, "raas:code"))
            if not code or code not in all_kpi:
                continue
            kpi = all_kpi[code]
            try:
                dau = float(kpi.get("dau", 0) or 0)
                churn = float(kpi.get("churn_rate", 0) or 0)
                wow = float(kpi.get("dau_chg", 0) or 0)
            except (TypeError, ValueError):
                continue

            if dau >= 1000 and churn >= 30 and wow <= -5:
                result.append({
                    "code": code,
                    "name": self._onto.label_ko(prog),
                    "dau": int(dau),
                    "churn_rate": round(churn, 1),
                    "dau_wow": round(wow, 1),
                })

        result.sort(key=lambda x: -x["dau"])
        return result

    def classify_program(self, code: str, kpi: dict) -> list[str]:
        """프로그램의 ProgramConcept 분류."""
        try:
            dau = float(kpi.get("dau", 0) or 0)
            churn = float(kpi.get("churn_rate", 0) or 0)
            wow = float(kpi.get("dau_chg", 0) or 0)
            new = float(kpi.get("new", 0) or 0)
            habit = float(kpi.get("habit_rate", 0) or 0)
        except (TypeError, ValueError):
            return []

        concepts = []
        if dau >= 1000 and churn >= 30 and wow <= -5:
            concepts.append("raas:AtRiskProgram")
        if new >= 500 and habit >= 30:
            concepts.append("raas:HabitFormingProgram")
        if dau >= 1000 and wow <= -10:
            concepts.append("raas:DecliningProgram")
        if dau >= 1000 and wow >= 10:
            concepts.append("raas:GrowingProgram")
        return concepts

    # ─── 3.5 정의 조회 (LLM 컨텍스트) ───────────────────────────────────────

    def get_concept_definition(self, concept_name: str) -> Optional[dict]:
        """클래스/룰의 자연어 정의."""
        cls_iri = f"raas:{concept_name}"

        # 클래스인지 인스턴스인지 확인
        is_class = any(s == cls_iri and p == "a" and o == "owl:Class"
                       for s, p, o in self._onto.triples)
        is_alert = cls_iri in self._onto.instances_of("raas:PlatformAlert")

        if not (is_class or is_alert):
            return None

        return {
            "id": cls_iri,
            "label": self._onto.label_ko(cls_iri),
            "alt_labels": [self._onto.value_str(v)
                           for v in self._onto.get(cls_iri, "raas:altLabel")],
            "definition": self._onto.value_str(self._onto.get_one(cls_iri, "rdfs:comment")),
            "condition": self._onto.value_str(self._onto.get_one(cls_iri, "raas:condition")),
            "alert_level": self._onto.value_str(self._onto.get_one(cls_iri, "raas:alertLevel")),
            "recommended_action": self._onto.value_str(
                self._onto.get_one(cls_iri, "raas:recommendedAction")
            ),
        }

    def get_llm_context_for_query(self, question: str) -> str:
        """자연어 질문에서 관련 개념 자동 추출, LLM용 컨텍스트 텍스트 생성."""
        relevant_concepts = []

        # 키워드 매칭으로 관련 개념 찾기
        for keyword in ["위험", "이탈", "급락", "급증", "성장", "습관"]:
            if keyword in question:
                # 관련 클래스 매핑 (단순 룰 — 확장 여지)
                if "위험" in question or "이탈" in question:
                    relevant_concepts.append("AtRiskProgram")
                if "급락" in question:
                    relevant_concepts.append("DecliningProgram")
                if "급증" in question or "성장" in question:
                    relevant_concepts.append("GrowingProgram")
                if "습관" in question:
                    relevant_concepts.append("HabitFormingProgram")

        if not relevant_concepts:
            return ""

        lines = ["## 관련 정의:"]
        for cn in set(relevant_concepts):
            d = self.get_concept_definition(cn)
            if d:
                lines.append(f"\n- {d['label']} ({cn})")
                lines.append(f"  정의: {d['definition']}")
                if d.get('condition'):
                    lines.append(f"  조건: {d['condition']}")
        return "\n".join(lines)

    # ─── 3.6 스키마 노출 (web.html용) ──────────────────────────────────────

    def get_schema_dump(self) -> dict:
        """클라이언트용 전체 스키마. /api/schema 응답."""
        # Metric별 variants 그룹화
        metrics = {}
        for m in self._onto.instances_of("raas:Metric"):
            m_dict = self._metric_dict(m)
            m_dict["is_percent"] = m_dict["measure_type"] == "Percentage"
            m_dict["variants"] = {}
            metrics[m.replace("raas:", "")] = m_dict

        # MetricVariant를 metric별로 분류
        for mv in self._onto.instances_of("raas:MetricVariant"):
            metric_iri = self._onto.get_one(mv, "raas:ofMetric")
            if not metric_iri:
                continue
            metric_key = metric_iri.replace("raas:", "")
            if metric_key not in metrics:
                continue

            granularity = self._onto.get_one(mv, "raas:granularity")
            field = self._onto.value_str(self._onto.get_one(mv, "raas:fieldName"))

            if granularity:
                gran_key = granularity.replace("raas:", "")
                metrics[metric_key]["variants"][gran_key] = {
                    "field": field,
                    "label": self._onto.label_ko(granularity),
                }

        # Channels
        channels = {}
        for ch in self._onto.instances_of("raas:Channel"):
            code = self._onto.value_str(self._onto.get_one(ch, "raas:code"))
            channels[code] = {
                "label": self._onto.label_ko(ch),
                "alt_labels": [self._onto.value_str(v)
                               for v in self._onto.get(ch, "raas:altLabel")],
            }

        # Programs
        programs = {p["code"]: p for p in self.get_all_programs()}

        # Dayparts
        dayparts = {}
        for dp in self._onto.instances_of("raas:Daypart"):
            key = dp.replace("raas:", "")
            dayparts[key] = {
                "label": self._onto.label_ko(dp),
                "hour_start": int(self._onto.value_str(
                    self._onto.get_one(dp, "raas:hourStart") or "0"
                )),
                "hour_end": int(self._onto.value_str(
                    self._onto.get_one(dp, "raas:hourEnd") or "0"
                )),
                "ad_value": self._onto.value_str(
                    self._onto.get_one(dp, "raas:adValueLevel")
                ),
            }

        # Concepts
        concepts = {}
        for s, p, o in self._onto.triples:
            if p == "rdfs:subClassOf" and o == "raas:ProgramConcept":
                key = s.replace("raas:", "")
                d = self.get_concept_definition(key)
                if d:
                    concepts[key] = {
                        "label": d["label"],
                        "alert_level": d["alert_level"],
                        "definition": d["definition"],
                    }

        # field_to_meta — 빠른 역방향 조회
        field_to_meta = {}
        for fn, mv_iri in self._field_to_variant.items():
            metric_iri = self._onto.get_one(mv_iri, "raas:ofMetric")
            granularity = self._onto.get_one(mv_iri, "raas:granularity")
            field_to_meta[fn] = {
                "metric": metric_iri.replace("raas:", "") if metric_iri else None,
                "granularity": granularity.replace("raas:", "") if granularity else None,
                "variant": "Current",
            }

        # metadata_fields — csvField 어노테이션된 메타데이터 컬럼
        metadata_fields = {
            info["csv_column"]: {
                "property": info["property"],
                "label": info["label"],
                "domain": info["domain"],
                "range": info["range"],
            }
            for info in self.get_all_metadata_fields()
            if info
        }

        return {
            "version": "0.6",
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "metrics": metrics,
            "channels": channels,
            "programs": programs,
            "dayparts": dayparts,
            "concepts": concepts,
            "field_to_meta": field_to_meta,
            "metadata_fields": metadata_fields,
        }


# =============================================================================
# 4. 싱글톤 캐시
# =============================================================================

class _AdapterCache:
    _instance: Optional[OntologyAdapter] = None
    _mtime: float = 0.0

    @classmethod
    def _ttl_mtime(cls) -> float:
        try:
            mtimes = [
                (ONTOLOGY_DIR / f).stat().st_mtime
                for f in ONTOLOGY_FILES
                if (ONTOLOGY_DIR / f).exists()
            ]
            return max(mtimes) if mtimes else 0.0
        except Exception:
            return 0.0

    @classmethod
    def get(cls) -> OntologyAdapter:
        current_mtime = cls._ttl_mtime()
        if cls._instance is None or current_mtime > cls._mtime:
            if cls._instance is not None:
                logger.info("TTL 파일 변경 감지 — 어댑터 자동 재로드")
            try:
                cls._instance = OntologyAdapter()
                cls._mtime = current_mtime
                logger.info(f"OntologyAdapter 로드 완료: {len(cls._instance._onto.triples)}개 트리플")
            except Exception as e:
                logger.error(f"OntologyAdapter 로드 실패: {e}")
                raise
        return cls._instance

    @classmethod
    def reload(cls) -> OntologyAdapter:
        cls._instance = None
        cls._mtime = 0.0
        return cls.get()


def get_adapter() -> OntologyAdapter:
    """RAAS 코드의 어댑터 진입점."""
    return _AdapterCache.get()


def reload_adapter() -> OntologyAdapter:
    """온톨로지 파일 변경 시 캐시 재로드."""
    return _AdapterCache.reload()


# =============================================================================
# 5. 자체 테스트 (직접 실행 시)
# =============================================================================

if __name__ == "__main__":
    import json

    print("=" * 70)
    print("OntologyAdapter 자체 테스트")
    print("=" * 70)

    a = get_adapter()
    print(f"\n[1] 어댑터 로드: {len(a._onto.triples)}개 트리플")

    print(f"\n[2] get_field_info('dau'):")
    print(json.dumps(a.get_field_info("dau"), ensure_ascii=False, indent=2))

    print(f"\n[3] get_field_info('wau_chg'):")
    print(json.dumps(a.get_field_info("wau_chg"), ensure_ascii=False, indent=2))

    print(f"\n[4] find_program_by_keyword('컬투'):")
    matches = a.find_program_by_keyword("컬투")
    for m in matches[:3]:
        print(f"  {m}")

    print(f"\n[5] get_program_meta('F09'):")
    print(json.dumps(a.get_program_meta("F09"), ensure_ascii=False, indent=2))

    print(f"\n[6] get_day_type('2026-05-01'):")
    print(json.dumps(a.get_day_type("2026-05-01"), ensure_ascii=False, indent=2))

    print(f"\n[7] evaluate_platform_alerts:")
    test_kpi = {"dau_chg": -12, "habit_rate": 6.4, "react_rate": 8.6}
    alerts = a.evaluate_platform_alerts(test_kpi)
    for al in alerts:
        print(f"  {al}")

    print(f"\n[8] classify_program('F05', ...):")
    test_pgm = {"dau": 23327, "churn_rate": 44.0, "dau_chg": -11.4}
    print(f"  {a.classify_program('F05', test_pgm)}")

    print(f"\n[9] get_concept_definition('AtRiskProgram'):")
    print(json.dumps(a.get_concept_definition("AtRiskProgram"), ensure_ascii=False, indent=2))

    print(f"\n[10] get_schema_dump (top-level keys):")
    schema = a.get_schema_dump()
    print(f"  keys: {list(schema.keys())}")
    print(f"  metrics: {len(schema['metrics'])}")
    print(f"  programs: {len(schema['programs'])}")
    print(f"  field_to_meta: {len(schema['field_to_meta'])}")

    print("\n완료")
