# -*- coding: utf-8 -*-
"""승인 지식 → TTL canonical 승격 배치 (진단서 2번 B / Option A: 아카이브).

승인된 knowledge_items(scope=approved)를 raas_onto/raas_ontology_contributed.ttl에
ContributedKnowledge 노드로 '재생성'(멱등) → git 버전관리되는 canonical 기록.
자유텍스트는 무손실 균일 주석(Level 1). 구조화 graduation(raas:definition 교정 등)은
온톨로지팀 수동 작업(별도). curated core ttl은 절대 자동 편집하지 않음.
"""
from __future__ import annotations
import os

_CONTRIB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "raas_onto", "raas_ontology_contributed.ttl")

_HEADER = """# =============================================================================
# raas_ontology_contributed.ttl  —  기여 지식(승인) canonical 미러
# 자동 생성 파일: raas_ontology_promote.promote_approved_to_ttl()이 재생성.
# 직접 편집 금지(다음 승격 시 덮어씌워짐). 구조화는 core ttl로 수동 graduation.
# =============================================================================
@prefix raas: <http://raas.sbs.co.kr/onto#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

raas:ContributedKnowledge a owl:Class ;
    rdfs:label "기여 지식(승인)"@ko ;
    rdfs:comment "사용자 기여 → 검토 승인된 도메인 지식. 오버레이에서 승격된 canonical 기록."@ko .

"""


def _ttl_str(s) -> str:
    """TTL 문자열 리터럴 이스케이프."""
    s = "" if s is None else str(s)
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    s = s.replace("\n", "\\n").replace("\r", "").replace("\t", "\\t")
    return s


def _render_node(it: dict) -> str:
    tk = (it.get("target_kind") or "global")
    tid = it.get("target_id")
    target = f'{tk}:{tid}' if tid else tk
    lines = [f"raas:CK_{it['id']} a raas:ContributedKnowledge ;"]
    lines.append(f'    raas:aboutTarget "{_ttl_str(target)}" ;')
    lines.append(f'    raas:targetKind "{_ttl_str(tk)}" ;')
    if tid:
        lines.append(f'    raas:targetId "{_ttl_str(tid)}" ;')
    lines.append(f'    raas:knowledgeType "{_ttl_str(it.get("type") or "fact")}" ;')
    lines.append(f'    raas:content "{_ttl_str(it.get("content"))}" ;')
    if it.get("contributor_id"):
        lines.append(f'    raas:contributedBy "{_ttl_str(it.get("contributor_id"))}" ;')
    if it.get("improvement_id"):
        lines.append(f'    raas:sourceImprovement {int(it["improvement_id"])} ;')
    if it.get("reviewed_at"):
        lines.append(f'    raas:approvedAt "{_ttl_str(it.get("reviewed_at"))}" ;')
    # 마지막 술어를 '.'로 종료
    lines[-1] = lines[-1].rstrip(" ;") + " ."
    return "\n".join(lines)


def _render_contributed_ttl(items: list) -> str:
    body = "\n\n".join(_render_node(it) for it in items) if items else "# (승인된 기여 지식 없음)"
    return _HEADER + body + "\n"


def promote_approved_to_ttl() -> dict:
    """승인 지식 전체를 contributed.ttl로 재생성(멱등) + promoted_at 기록.
       반환: {ok, count, path}."""
    import raas_history_db as HDB
    items = HDB.list_approved_for_promotion()
    ttl = _render_contributed_ttl(items)
    with open(_CONTRIB_PATH, "w", encoding="utf-8") as f:
        f.write(ttl)
    HDB.mark_promoted([it["id"] for it in items])
    return {"ok": True, "count": len(items), "path": _CONTRIB_PATH}


if __name__ == "__main__":
    print(promote_approved_to_ttl())
