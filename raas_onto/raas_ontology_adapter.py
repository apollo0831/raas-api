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
        if (c / "raas_kpi_ontology.ttl").exists():
            return c

    # 못 찾으면 같은 폴더로 (어댑터 init 시 경고 발생)
    return base


ONTOLOGY_DIR = _find_ontology_dir()

ONTOLOGY_FILES = [
    "raas_kpi_ontology.ttl",
    "raas_domain_entities.ttl",
    "raas_time_schema.ttl",
    "raas_business_rules.ttl",
    "raas_calendar.ttl",
]


# ─── 레거시 KPI 필드 alias 매핑 (Phase 5 Step 3) ──────────────────────────────
# 새 이름(SPL 출력, 2026-04 KPI 명명 규칙) → 옛 이름(엔진/화면이 사용 중인 이름)
# 단일 신뢰 출처: docs/raas_kpi_dictionary.yaml의 'note: 이전 이름: <old>'
# 향후 .ttl의 raas:legacyName 속성으로 마이그레이션 예정.
LEGACY_FIELD_ALIASES = {
    # B. user_size — DAU
    'dau':                  'dau_today',
    'dau_prev':             'dau_pw',
    'dau_chg':              'dau_wow',
    'dau_d2':               'dau_yday',
    # B. user_size — WAU (의미적 변경: 옛 이름은 dau_week)
    'wau':                  'dau_week',
    'wau_prev':             'dau_week_pw',
    'wau_chg':              'dau_week_wow',
    # B. user_size — MAU (의미적 변경: 옛 이름은 dau_mon)
    'mau':                  'dau_mon',
    'mau_prev':             'dau_mon_pw',
    'mau_chg':              'dau_mon_wow',
    # B. user_size — 롤링 7일 (옛: wau_today)
    'dau_r7':               'wau_today',
    'dau_r7_prev':          'wau_pw',
    # B. user_size — 롤링 30일 (옛: mau_today)
    'dau_r30':              'mau_today',
    'dau_r30_prev':         'mau_pw',
    # B. user_size — 1분/10분 *_prev (이름은 _pw였음)
    'dau_1min_prev':        'dau_1min_pw',
    'dau_10min_prev':       'dau_10min_pw',
    'wau_1min_prev':        'wau_1min_pw',
    'wau_10min_prev':       'wau_10min_pw',
    'mau_1min_prev':        'mau_1min_pw',
    'mau_10min_prev':       'mau_10min_pw',
    # C. user_growth — new
    'new':                  'new_today',
    'new_prev':             'new_pw',
    'new_chg':              'new_wow',
    'new_share':            'new_pct',
    'new_week_prev':        'new_week_pw',
    'new_week_chg':         'new_week_wow',
    'new_week_share':       'new_week_pct',
    'new_mon_prev':         'new_mon_pw',
    'new_mon_chg':          'new_mon_wow',
    'new_mon_share':        'new_mon_pct',
    # C. user_growth — react
    'react':                'react_today',
    'react_prev':           'react_pw',
    'react_chg':            'react_wow',
    'react_share':          'react_pct',
    'react_week_prev':      'react_week_pw',
    'react_week_chg':       'react_week_wow',
    'react_week_share':     'react_week_pct',
    'react_mon_prev':       'react_mon_pw',
    'react_mon_chg':        'react_mon_wow',
    'react_mon_share':      'react_mon_pct',
    # D. user_quality — react_rate (값/이름 변경 없음, _pw → _prev만)
    'react_rate_prev':      'react_rate_pw',
    'react_rate_week_prev': 'react_rate_week_pw',
    'react_rate_mon_prev':  'react_rate_mon_pw',
    # D. user_quality — churn (_rate 접미사 추가)
    'churn_rate_prev':      'churn_rate_pw',
    'churn_rate_diff':      'churn_diff',
    'churn_rate_week':      'churn_week',
    'churn_rate_week_prev': 'churn_week_pw',
    'churn_rate_week_diff': 'churn_week_diff',
    'churn_rate_mon':       'churn_mon',
    'churn_rate_mon_prev':  'churn_mon_pw',
    'churn_rate_mon_diff':  'churn_mon_diff',
    # D. user_quality — deep_rate (_pw → _prev)
    'deep_rate_prev':       'deep_rate_pw',
    'deep_rate_week_prev':  'deep_rate_week_pw',
    'deep_rate_mon_prev':   'deep_rate_mon_pw',
    # D. user_quality — engage (_rate 접미사 추가)
    'engage_rate_prev':     'engage_rate_pw',
    'engage_rate_diff':     'engage_diff',
    'engage_rate_week':     'engage_week',
    'engage_rate_week_prev':'engage_week_pw',
    'engage_rate_week_diff':'engage_week_diff',
    'engage_rate_mon':      'engage_mon',
    'engage_rate_mon_prev': 'engage_mon_pw',
    'engage_rate_mon_diff': 'engage_mon_diff',
    # E. user_habit — habit (_rate 접미사 추가)
    'habit_rate_prev':      'habit_rate_pw',
    'habit_rate_diff':      'habit_diff',
    'habit_rate_week':      'habit_week',
    'habit_rate_week_prev': 'habit_week_pw',
    'habit_rate_week_diff': 'habit_week_diff',
    'habit_rate_mon':       'habit_mon',
    'habit_rate_mon_prev':  'habit_mon_pw',
    'habit_rate_mon_diff':  'habit_mon_diff',
    # F. user_retention — d1/d7/w1/m1 (_pw → _prev, _diff prefix 통일)
    'd1_ret_prev':          'd1_ret_pw',
    'd1_ret_diff':          'd1_diff',
    'd7_ret_prev':          'd7_ret_pw',
    'd7_ret_diff':          'd7_diff',
    'w1_ret_prev':          'w1_ret_pw',
    'w1_ret_diff':          'w1_diff',
    'm1_ret_prev':          'm1_ret_pw',
    'm1_ret_diff':          'm1_diff',
    'new_d1_ret_prev':      'new_d1_ret_pw',
    'new_d1_ret_diff':      'new_d1_diff',
    'new_d7_ret_prev':      'new_d7_ret_pw',
    'new_d7_ret_diff':      'new_d7_diff',
    'new_w1_ret_prev':      'new_w1_ret_pw',
    'new_w1_ret_diff':      'new_w1_diff',
    'new_m1_ret_prev':      'new_m1_ret_pw',
    'new_m1_ret_diff':      'new_m1_diff',
    # A. meta — 편성정보
    'program_title':        'schedule_title',
}


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

    def get_all_fields(self) -> list[str]:
        """온톨로지에 정의된 모든 필드명 (변형 접미사 적용 전)."""
        return list(self._field_to_variant.keys())

    def get_legacy_field_aliases(self) -> dict[str, str]:
        """레거시 alias 매핑 (briefing_engine FIELD_ALIASES 호환).

        Phase 5 Step 3 통합: briefing_engine의 87개 alias를 SSoT(어댑터)로 이전.
        TODO (장기): .ttl에 raas:legacyName 속성 추가 후 그래프에서 추출.
        """
        return dict(LEGACY_FIELD_ALIASES)

    # ─── 3.2 프로그램/채널 조회 ─────────────────────────────────────────────

    def find_program_by_keyword(self, keyword: str) -> list[dict]:
        """자연어 키워드로 검색. 챕터 3.3.2 참조."""
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

        # 부분 일치
        if not results:
            for label, subjects in self._keyword_index.items():
                if keyword in label:
                    for subj in subjects:
                        if subj in seen_subjects:
                            continue
                        seen_subjects.add(subj)
                        score = len(keyword) / len(label)
                        results.append(self._search_result(subj, label, score=score))

        results.sort(key=lambda r: -r["score"])
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

        return result

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
        """플랫폼 알림 룰 평가. build_s7 대체."""
        alerts = []
        for rule in self._onto.instances_of("raas:PlatformAlert"):
            rule_id = rule.replace("raas:Alert_", "")
            if rule_id not in self._ALERT_EVALUATORS:
                continue  # AtRiskProgramDetected는 별도 처리

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

        return {
            "version": "0.5",
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "metrics": metrics,
            "channels": channels,
            "programs": programs,
            "dayparts": dayparts,
            "concepts": concepts,
            "field_to_meta": field_to_meta,
        }


# =============================================================================
# 4. 싱글톤 캐시
# =============================================================================

class _AdapterCache:
    _instance: Optional[OntologyAdapter] = None

    @classmethod
    def get(cls) -> OntologyAdapter:
        if cls._instance is None:
            try:
                cls._instance = OntologyAdapter()
                logger.info(f"OntologyAdapter 로드 완료: {len(cls._instance._onto.triples)}개 트리플")
            except Exception as e:
                logger.error(f"OntologyAdapter 로드 실패: {e}")
                raise
        return cls._instance

    @classmethod
    def reload(cls) -> OntologyAdapter:
        cls._instance = None
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
