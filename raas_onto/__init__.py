"""RAAS 온톨로지 모듈 — 도메인 지식의 단일 출처 (SSoT).

Phase 5 통합으로 도입. 운영 코드(briefing/query/server)는 이 패키지에서
어댑터를 가져와 메타지식(필드/프로그램/룰)을 조회한다.

사용 예:
    from raas_onto import get_adapter
    adapter = get_adapter()
    info = adapter.get_field_info('dau')

구성:
- raas_ontology_adapter.py — 어댑터 본체 (싱글톤 캐시)
- raas_paths.py            — CSV 경로 헬퍼
- raas_*.ttl               — 5개 온톨로지 파일 (도메인 지식 SSoT)
- phase5_regression_test.py — 회귀 테스트 (Phase 5 마이그레이션 검증)
"""
from .raas_ontology_adapter import get_adapter, OntologyAdapter, reload_adapter
from .raas_zscore_detector import get_detector, ZScoreDetector

__all__ = ['get_adapter', 'OntologyAdapter', 'reload_adapter', 'get_detector', 'ZScoreDetector']
