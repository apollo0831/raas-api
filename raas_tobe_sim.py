"""raas_tobe_sim.py — CP TO-BE 스토리라인 내비게이션 시뮬레이터 (이동 경로 검증 전용).

데이터/답변 계산 없이 노드 이동 + 토글 + 공통 출구 세트 + 자유질의 복귀만 테스트.
템플릿: data/storylines/cp_tobe.json (schema tobe-nav-v1)

사용법:
    python raas_tobe_sim.py                      # 대화형
    python raas_tobe_sim.py --path 1,1,1,2       # 스크립트 경로 (메뉴 번호 시퀀스)
    python raas_tobe_sim.py --role cp_tobe

메뉴 번호 규칙: [토글] → [이동 칩] → [공통 출구] 순으로 위에서부터 1,2,3…
토글 선택 시 하위 옵션 번호를 한 번 더 입력(스크립트 모드는 'm1=주' 같은 형식 대신
토글도 단일 번호로 순환 — 아래 set_toggle 참고).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT = Path(__file__).parent
STORYLINES_DIR = ROOT / "data" / "storylines"

DUMMY = {"user_name": "김CP", "channel_name": "파워FM"}


# ─── 렌더 헬퍼 ────────────────────────────────────────────────────────────
def render(text: str, data: dict) -> str:
    """{key} 치환. 누락 시 '·' 표시 (스텁이므로 관대하게)."""
    if not text:
        return ""
    out = text
    for k, v in data.items():
        out = out.replace("{" + k + "}", str(v))
    # 남은 {x} 는 토글 미설정 placeholder → 그대로 두지 않고 표식
    import re
    out = re.sub(r"\{([^{}]+)\}", lambda m: f"〈{m.group(1)}?〉", out)
    return out


def box(title: str, body: str) -> None:
    w = 76
    print(f"\n┌─ {title} " + "─" * max(0, w - len(title) - 4))
    for ln in body.split("\n"):
        print(f"│ {ln}")
    print("└" + "─" * (w - 1))


# ─── 시뮬레이터 ───────────────────────────────────────────────────────────
class ToBeSim:
    def __init__(self, cfg: dict, script: list[str] | None = None):
        self.cfg = cfg
        self.nodes = cfg["nodes"]
        self.common_exits = cfg.get("common_exits", [])
        self.script = script or []
        self.script_i = 0
        # 토글 상태: node_id -> {toggle_key: value}
        self.toggle_state: dict[str, dict] = {}
        for nid, node in self.nodes.items():
            self.toggle_state[nid] = {
                k: t.get("default") for k, t in (node.get("toggles") or {}).items()
            }
        self.path: list[str] = []          # 방문 노드(이동만)
        self.toggle_log: list[str] = []    # 토글 변경 이력
        self.freetext_log: list[str] = []

    # 입력 추상화 (스크립트 or 대화형)
    def _input(self, prompt: str) -> str:
        if self.script_i < len(self.script):
            val = self.script[self.script_i]
            self.script_i += 1
            print(f"{prompt}{val}   [script]")
            return val
        return input(prompt)

    def _data_for(self, node_id: str) -> dict:
        d = dict(DUMMY)
        d.update(self.toggle_state.get(node_id, {}))
        return d

    def run(self) -> None:
        c = self.cfg
        print("\n" + "═" * 76)
        print(f"  RAAS TO-BE 내비 시뮬레이터 — {c['role_name_ko']}")
        print(f"  목적: 이동 경로 + 토글 + 공통 출구 검증 (데이터 없음)")
        print("═" * 76)

        # Entry
        entry = c["entry"]
        box("🤖 entry — 인사 + 첫 질문",
            f"{render(entry['greeting'], DUMMY)}\n\n{render(entry['question'], DUMMY)}"
            f"\n\n{entry.get('auto_priority_note','')}")
        chips = entry["chips"]
        self._show_menu([], chips, [])
        sel = self._pick(len(chips))
        if sel is None:
            return self._summary("entry에서 종료")
        self._goto(chips[sel])

    def _goto(self, chip: dict) -> None:
        """칩 따라 노드 진입 후 루프."""
        target = chip.get("to")
        # 진입 시 토글 초기값 주입 (entry의 set 등)
        if "set" in chip and target in self.toggle_state:
            for k, v in chip["set"].items():
                self.toggle_state.setdefault(target, {})[k] = v

        current = target
        while True:
            if current == "_end":
                return  # 호출부에서 처리됨 (여기 도달 안 함)
            node = self.nodes.get(current)
            if not node:
                print(f"  ⚠ 알 수 없는 노드: {current}")
                return self._summary("오류 종료")
            # 토글·자유질의 재렌더로 같은 노드를 다시 그릴 땐 경로 중복 계상 안 함
            if not self.path or self.path[-1] != current:
                self.path.append(current)

            # 노드 렌더
            data = self._data_for(current)
            tog_str = self._toggle_state_str(current)
            title = f"🤖 {current} — {node['name']}"
            body = render(node.get("stub", ""), data)
            if tog_str:
                body += f"\n\n  [현재 토글] {tog_str}"
            box(title, body)

            toggles = node.get("toggles") or {}
            chips = node.get("chips") or []
            exits = self.common_exits if node.get("use_common_exits") else []

            self._show_menu(list(toggles.items()), chips, exits)
            n_tog = len(toggles)
            n_chip = len(chips)
            n_exit = len(exits)
            total = n_tog + n_chip + n_exit
            sel = self._pick(total)
            if sel is None:
                return self._summary("사용자 종료")

            # 1) 토글 변경
            if sel < n_tog:
                tkey, tdef = list(toggles.items())[sel]
                self._set_toggle(current, tkey, tdef)
                continue  # 같은 노드 재렌더 (이동 없음)

            # 2) 이동 칩
            sel2 = sel - n_tog
            if sel2 < n_chip:
                chip = chips[sel2]
                if chip.get("to") == "_end":
                    return self._end(chip)
                # set 주입 후 이동
                if "set" in chip and chip["to"] in self.toggle_state:
                    for k, v in chip["set"].items():
                        self.toggle_state.setdefault(chip["to"], {})[k] = v
                current = chip["to"]
                continue

            # 3) 공통 출구
            ex = exits[sel2 - n_chip]
            if ex.get("to") == "__freetext__":
                self._freetext(current)
                continue  # 레일 복귀 — 같은 노드 재렌더
            if ex.get("to") == "_end":
                return self._end(ex)
            current = ex["to"]
            continue

    # ─── 토글 ───
    def _toggle_state_str(self, node_id: str) -> str:
        st = self.toggle_state.get(node_id) or {}
        node = self.nodes[node_id]
        parts = []
        for k, t in (node.get("toggles") or {}).items():
            parts.append(f"{t['label']}={st.get(k)}")
        return " · ".join(parts)

    def _set_toggle(self, node_id: str, tkey: str, tdef: dict) -> None:
        opts = tdef["options"]
        cur = self.toggle_state[node_id].get(tkey)
        print(f"\n  ▸ {tdef['label']} 선택 (현재 {cur}):")
        for i, o in enumerate(opts, 1):
            mark = " ←현재" if o == cur else ""
            print(f"     [{i}] {o}{mark}")
        raw = self._input(f"  토글 선택 (1~{len(opts)}, Enter=순환): ").strip()
        if raw == "":
            ni = (opts.index(cur) + 1) % len(opts) if cur in opts else 0
        else:
            try:
                ni = int(raw) - 1
                if not (0 <= ni < len(opts)):
                    print("   ⚠ 범위 밖 — 변경 없음")
                    return
            except ValueError:
                print("   ⚠ 숫자 아님 — 변경 없음")
                return
        new = opts[ni]
        self.toggle_state[node_id][tkey] = new
        self.toggle_log.append(f"{node_id}.{tkey}: {cur}→{new}")
        print(f"   ✓ {tdef['label']} = {new}")

    # ─── 자유질의 복귀 레일 ───
    def _freetext(self, node_id: str) -> None:
        q = self._input("\n  💬 자유질의 입력: ").strip()
        self.freetext_log.append(f"@{node_id}: {q}")
        box("🤖 자유질의 응답 (스텁)",
            f"'{q}'\n→ (실제 답변은 쿼리엔진/라우터가 처리)\n"
            f"레일 유지: 직전 노드 [{node_id}]로 복귀하며 칩 그대로 노출됩니다.")
        print("   ↩ 스토리라인 복귀")

    # ─── 메뉴 ───
    def _show_menu(self, toggles: list, chips: list, exits: list) -> None:
        print()
        i = 1
        if toggles:
            print("  [토글]")
            for tkey, tdef in toggles:
                print(f"   [{i}] {tdef['label']} 변경  (옵션: {' / '.join(tdef['options'])})")
                i += 1
        if chips:
            print("  [이동]")
            for c in chips:
                print(f"   [{i}] {c['label']}  →  {c['to']}")
                i += 1
        if exits:
            print("  [공통 출구]")
            for e in exits:
                tag = "  (자유질의·레일복귀)" if e["to"] == "__freetext__" else f"  →  {e['to']}"
                print(f"   [{i}] {e['label']}{tag}")
                i += 1

    def _pick(self, n: int) -> int | None:
        raw = self._input(f"\n선택 (1~{n}, q=종료): ").strip().lower()
        if raw in ("q", "quit", "exit"):
            return None
        try:
            v = int(raw)
            if 1 <= v <= n:
                return v - 1
        except ValueError:
            pass
        print(f"  ⚠ 1~{n} 또는 q")
        return self._pick(n)

    # ─── 종료 ───
    def _end(self, chip: dict) -> None:
        out = chip.get("output", chip.get("label", "산출물"))
        box("🤖 _end — 산출물 생성",
            f"✅ {out} 생성 완료 (스텁)\n   (실제 변환은 리포트 엔진에서 처리)")
        self._summary(f"산출물: {out}")

    def _summary(self, why: str) -> None:
        print("\n" + "─" * 76)
        print(f"  종료: {why}")
        print(f"  📊 이동 경로 ({len(self.path)}): entry → " + " → ".join(self.path))
        print(f"  🎚  토글 변경 ({len(self.toggle_log)}): "
              + ("; ".join(self.toggle_log) if self.toggle_log else "없음"))
        print(f"  💬 자유질의 ({len(self.freetext_log)}): "
              + ("; ".join(self.freetext_log) if self.freetext_log else "없음"))
        print("─" * 76)


def main() -> None:
    ap = argparse.ArgumentParser(description="CP TO-BE 내비 시뮬레이터")
    ap.add_argument("--role", default="cp_tobe", help="템플릿 stem (기본 cp_tobe)")
    ap.add_argument("--path", default="", help="스크립트 경로 — 메뉴 번호 쉼표 구분 (예: 1,1,1)")
    args = ap.parse_args()

    cfg_path = STORYLINES_DIR / f"{args.role}.json"
    if not cfg_path.exists():
        print(f"⚠ {cfg_path} 없음")
        return
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    script = [s.strip() for s in args.path.split(",") if s.strip()] if args.path else []
    ToBeSim(cfg, script=script).run()


if __name__ == "__main__":
    main()
