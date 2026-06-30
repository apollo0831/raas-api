"""
RAAS 온톨로지 PoC — 공통 경로 헬퍼

여러 환경(로컬, Anthropic 컨테이너)에서 동일한 코드가 동작하도록
CSV 파일을 우선순위에 따라 자동 탐색.

탐색 순서:
  1. 환경변수 RAAS_CSV_PATH가 지정되어 있으면 그 경로 사용
  2. 이 모듈과 같은 폴더에 raas_kpi_latest.csv가 있으면 사용
  3. 상위 폴더의 data/raas_kpi_latest.csv (권장 디렉토리 구조)
  4. /mnt/user-data/uploads/raas_kpi_latest.csv (Anthropic 환경)
  5. 어디에도 없으면 None 반환 + 사용자 안내 메시지

사용법:
    from raas_paths import get_csv_path
    csv_path = get_csv_path()
    if csv_path is None:
        print("CSV 파일을 찾을 수 없습니다.")
"""
from __future__ import annotations
import os
from pathlib import Path

CSV_FILENAME = "raas_kpi_latest.csv"


def get_csv_path(verbose: bool = False) -> Path | None:
    """raas_kpi_latest.csv를 우선순위에 따라 탐색."""
    base_dir = Path(__file__).parent

    candidates = []

    # 1. 환경변수
    env_path = os.environ.get("RAAS_CSV_PATH")
    if env_path:
        candidates.append(("환경변수 RAAS_CSV_PATH", Path(env_path)))

    # 2. 같은 폴더
    candidates.append(("같은 폴더", base_dir / CSV_FILENAME))

    # 3. 상위 폴더의 data/
    candidates.append(("상위 폴더 data/", base_dir.parent / "data" / CSV_FILENAME))

    # 4. Anthropic 환경
    candidates.append(("Anthropic 컨테이너", Path("/mnt/user-data/uploads") / CSV_FILENAME))

    for source, path in candidates:
        if path.exists():
            if verbose:
                print(f"  [경로] {source}: {path}")
            return path

    # 못 찾았을 때 안내
    if verbose:
        print(f"  [경고] CSV 파일을 찾을 수 없습니다. 다음 위치 중 한 곳에 두세요:")
        for source, path in candidates[1:]:  # 환경변수는 빼고
            print(f"    - {source}: {path}")
        print(f"  또는 환경변수 RAAS_CSV_PATH로 직접 지정 가능.")

    return None


def get_ontology_paths():
    """온톨로지 .ttl 파일들의 경로 반환."""
    base_dir = Path(__file__).parent
    return [
        base_dir / "raas_ontology_fields.ttl",
        base_dir / "raas_ontology_program.ttl",
        base_dir / "raas_ontology_time.ttl",
        base_dir / "raas_ontology_noteworthy.ttl",
        base_dir / "raas_ontology_calendar.ttl",
        base_dir / "raas_ontology_person.ttl",
        base_dir / "raas_ontology_episode.ttl",
    ]


if __name__ == "__main__":
    print("RAAS 경로 탐색 결과:")
    print()
    print("[CSV 파일]")
    csv = get_csv_path(verbose=True)
    if csv:
        print(f"  → 발견: {csv}")
    else:
        print(f"  → 발견 실패")
    print()
    print("[온톨로지 파일]")
    for p in get_ontology_paths():
        status = "OK" if p.exists() else "없음"
        print(f"  [{status}] {p}")
