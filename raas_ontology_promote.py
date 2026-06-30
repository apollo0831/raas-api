# -*- coding: utf-8 -*-
"""승인 지식 → TTL canonical 승격 배치 (구조화 graduation).

승인된 knowledge_items(scope=approved)를 raas_onto/raas_ontology_contributed.ttl에
'재생성'(멱등)한다. 단순 텍스트 아카이브가 아니라 **범주(type)에 따라 타입드 술어로
실제 대상 노드(raas:<프로그램코드> / 필드 metric 노드)에 연결**해 온톨로지를 구조적으로 성장시킨다.
- 각 CK 노드: raas:graduatedTo <대상노드> + 타입별 술어(content @ko) + 출처(provenance).
- answer_style은 승격 제외(style_policy 도메인).
- curated core ttl은 절대 자동 편집하지 않음(contributed.ttl만 생성).
"""
from __future__ import annotations
import os

_CONTRIB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "raas_onto", "raas_ontology_contributed.ttl")

# 범주(type) → 구조화 술어. 신규 6범주 + 레거시 호환.
_TYPE_PREDICATE = {
    # 신규 6범주
    "metric_term":      "raas:contributedDefinition",
    "program_corner":   "raas:contributedCorner",
    "episode_note":     "raas:contributedEpisodeNote",
    "analysis_method":  "raas:contributedAnalysis",
    "misc":             "raas:contributedNote",
    # answer_style → 승격 제외(아래에서 필터)
    # 레거시 호환
    "metric_definition":  "raas:contributedDefinition",
    "field_meaning":      "raas:contributedDefinition",
    "program_note":       "raas:contributedCorner",
    "corner_note":        "raas:contributedCorner",
    "guest_policy":       "raas:contributedCorner",
    "decomposition_hint": "raas:contributedAnalysis",
    "fact":               "raas:contributedNote",
}
_SKIP_TYPES = {"answer_style"}

_HEADER = """# =============================================================================
# raas_ontology_contributed.ttl  —  기여 지식(승인) canonical (구조화 graduation)
# 자동 생성: raas_ontology_promote.promote_approved_to_ttl()이 재생성. 직접 편집 금지.
# 각 기여는 raas:graduatedTo로 실제 대상 노드에 연결되고 타입별 술어로 구조화된다.
# =============================================================================
@prefix raas: <http://raas.sbs.co.kr/onto#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

raas:ContributedKnowledge a owl:Class ;
    rdfs:label "기여 지식(승인)"@ko ;
    rdfs:comment "사용자 기여 → 검토 승인된 도메인 지식. 대상 노드에 구조화 연결됨."@ko .

raas:graduatedTo a owl:ObjectProperty ;
    rdfs:label "졸업 대상"@ko ;
    rdfs:comment "이 기여가 연결되는 실제 온톨로지 노드(프로그램·채널·지표)."@ko .

raas:contributedDefinition a owl:AnnotationProperty ; rdfs:label "기여 정의 보정"@ko .
raas:contributedCorner     a owl:AnnotationProperty ; rdfs:label "기여 코너·게스트 정보"@ko .
raas:contributedEpisodeNote a owl:AnnotationProperty ; rdfs:label "기여 회차 특이사항"@ko .
raas:contributedAnalysis   a owl:AnnotationProperty ; rdfs:label "기여 분석 기법"@ko .
raas:contributedNote       a owl:AnnotationProperty ; rdfs:label "기여 기타 정보"@ko .

"""


def _ttl_str(s) -> str:
    """TTL 문자열 리터럴 이스케이프."""
    s = "" if s is None else str(s)
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    s = s.replace("\n", "\\n").replace("\r", "").replace("\t", "\\t")
    return s


def _resolve_target_node(it: dict):
    """(target_kind, target_id) → 연결할 온톨로지 노드 IRI. 실패 시 None(전역)."""
    tk = (it.get("target_kind") or "global")
    tid = it.get("target_id")
    if not tid:
        return None
    try:
        if tk in ("program", "channel"):
            # 프로그램/채널 코드는 raas:<CODE> 노드
            return f"raas:{str(tid).upper()}"
        if tk == "field":
            # 필드 → 그 필드의 metric 노드(raas:AU 등). 어댑터로 해석.
            from raas_onto.raas_ontology_adapter import get_adapter
            info = get_adapter().get_field_info(str(tid))
            mid = (info or {}).get("metric", {}).get("id") if info else None
            return mid  # 'raas:AU' 형태 또는 None
    except Exception:
        return None
    return None


def _render_node(it: dict):
    """승인 기여 1건 → 구조화 TTL 노드. answer_style 등 제외 대상이면 None."""
    typ = it.get("type") or "fact"
    if typ in _SKIP_TYPES:
        return None
    predicate = _TYPE_PREDICATE.get(typ, "raas:contributedNote")
    tk = (it.get("target_kind") or "global")
    tid = it.get("target_id")
    target_str = f'{tk}:{tid}' if tid else tk
    node = _resolve_target_node(it)

    lines = [f"raas:CK_{it['id']} a raas:ContributedKnowledge ;"]
    if node:
        lines.append(f'    raas:graduatedTo {node} ;')
    lines.append(f'    raas:aboutTarget "{_ttl_str(target_str)}" ;')
    lines.append(f'    raas:targetKind "{_ttl_str(tk)}" ;')
    if tid:
        lines.append(f'    raas:targetId "{_ttl_str(tid)}" ;')
    lines.append(f'    raas:knowledgeType "{_ttl_str(typ)}" ;')
    # 타입별 구조화 술어 + 원문(content) — 언어태그 @ko
    lines.append(f'    {predicate} "{_ttl_str(it.get("content"))}"@ko ;')
    if it.get("contributor_id"):
        lines.append(f'    raas:contributedBy "{_ttl_str(it.get("contributor_id"))}" ;')
    if it.get("improvement_id"):
        lines.append(f'    raas:sourceImprovement {int(it["improvement_id"])} ;')
    if it.get("reviewed_at"):
        lines.append(f'    raas:approvedAt "{_ttl_str(it.get("reviewed_at"))}" ;')
    lines[-1] = lines[-1].rstrip(" ;") + " ."
    return "\n".join(lines)


def _render_contributed_ttl(items: list) -> str:
    rendered = [r for r in (_render_node(it) for it in items) if r]
    body = "\n\n".join(rendered) if rendered else "# (승인된 기여 지식 없음)"
    return _HEADER + body + "\n"


def preview_promotion() -> dict:
    """승격 dry-run — 파일을 쓰지 않고 변경 예정 내용을 반환.
       반환 {ok, count, promoted, items:[{id,type,target,content,predicate,node,excluded,reason}],
             ttl_before, ttl_after}. UI에서 병합 전후·텍스트 변경을 검토하는 용도."""
    import raas_history_db as HDB
    items = HDB.list_approved_for_promotion()
    try:
        before = open(_CONTRIB_PATH, encoding="utf-8").read()
    except Exception:
        before = ""
    after = _render_contributed_ttl(items)
    rows = []
    for it in items:
        typ = it.get("type") or "fact"
        excluded = typ in _SKIP_TYPES
        tk = it.get("target_kind") or "global"
        tid = it.get("target_id")
        rows.append({
            "id": it["id"], "type": typ,
            "target": (f"{tk}:{tid}" if tid else tk),
            "content": it.get("content"),
            "predicate": (None if excluded else _TYPE_PREDICATE.get(typ, "raas:contributedNote")),
            "node": (None if excluded else _resolve_target_node(it)),
            "excluded": excluded,
            "reason": ("answer_style는 style_policy 도메인(승격 제외)" if excluded else None),
        })
    return {"ok": True, "count": len(items),
            "promoted": sum(1 for r in rows if not r["excluded"]),
            "items": rows, "ttl_before": before, "ttl_after": after}


def promote_approved_to_ttl() -> dict:
    """승인 지식 전체를 구조화 contributed.ttl로 재생성(멱등) + promoted_at 기록.
       반환: {ok, count, promoted, path}. count=대상 건수, promoted=실제 구조화된 건수."""
    import raas_history_db as HDB
    items = HDB.list_approved_for_promotion()
    ttl = _render_contributed_ttl(items)
    with open(_CONTRIB_PATH, "w", encoding="utf-8") as f:
        f.write(ttl)
    HDB.mark_promoted([it["id"] for it in items])
    promoted = sum(1 for it in items if (it.get("type") not in _SKIP_TYPES))
    return {"ok": True, "count": len(items), "promoted": promoted, "path": _CONTRIB_PATH}


if __name__ == "__main__":
    print(promote_approved_to_ttl())
