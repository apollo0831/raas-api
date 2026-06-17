# RAAS 인터랙션 상태 패턴 v1

이 문서는 사용자 인터랙션의 6개 상태(호버·포커스·활성·로딩·빈 상태·에러)에 대한 RAAS의 통일된 패턴입니다. [docs/RAAS_DESIGN_TOKENS.md](RAAS_DESIGN_TOKENS.md)의 시맨틱 토큰만 사용합니다.

새 컴포넌트는 이 문서의 패턴을 따라야 하고, 패턴을 벗어나는 경우 [docs/decisions.md](decisions.md)에 사유를 기록합니다.

---

## 0. 원칙

1. **시각 단서는 항상 시맨틱 토큰** — 16진수·인라인 색상 금지
2. **상태 변화는 0.12~0.2s 안에 완료** — `--dur-fast` / `--dur-base`
3. **하나의 컴포넌트에 여러 상태가 동시에 일어날 수 있음** — focus + hover 동시 OK, disabled는 다른 상태 무효화
4. **상태 메시지는 빈 곳을 채우는 게 아니라 다음 행동을 안내** — "데이터 없음"보다 "OO를 시도해 보세요"

---

## 1. 호버 (Hover)

### 원칙
- **모든 클릭 가능 요소**는 호버 시각 단서 필수
- 색이 아니라 **배경 농도 변화**를 우선 (시야 흐림 방지)
- `--dur-fast` 0.12s 이내

### 표준 패턴

```css
.btn {
  background: var(--color-bg-elevated);
  color:      var(--color-text-secondary);
  transition: background var(--dur-fast) var(--ease-quick),
              color      var(--dur-fast) var(--ease-quick);
}
.btn:hover {
  background: var(--color-bg-elevated-2);
  color:      var(--color-text-primary);
}

/* 강조 액션 */
.btn.primary {
  background: var(--color-interactive-soft);
  color:      var(--color-interactive);
}
.btn.primary:hover {
  background: rgba(79,142,247,.25);   /* 약간 진해짐, 액센트 동일 */
}
```

### 현재 RAAS 적용 상태
- ✅ `.brief-action-btn`, `.sgst-chip`, `.btn-icon` 등 대다수 적용
- ⚠️ 일부 `cursor:pointer` 있고 호버 없는 곳 산재 (예: 일부 인라인 div 클릭 핸들러)

---

## 2. 포커스 (Focus / Focus-visible)

### 원칙 — **가장 중요**
- 키보드 사용자가 "지금 어디에 있는지" 명확히 보여야 함
- `:focus-visible` 사용 권장 (마우스 클릭엔 안 뜨고, Tab 키엔 뜸)
- 포커스 링은 **2px 액센트 outline + 2px offset**

### 표준 패턴

```css
/* 모든 인터랙티브 요소 */
button, a, input, textarea, select, [tabindex] {
  outline: none;
}
button:focus-visible,
a:focus-visible,
input:focus-visible,
textarea:focus-visible,
select:focus-visible,
[tabindex]:focus-visible {
  outline: 2px solid var(--color-interactive);
  outline-offset: 2px;
  border-radius: var(--rad-sm); /* 라운드 일관성 */
}
```

### 현재 RAAS 적용 상태
- ❌ **글로벌 포커스 룰 없음** — 브라우저 기본 포커스 링이 일부 환경에서 약하거나 사라짐 (특히 사용자 정의 `outline:none` 컴포넌트)
- ⚠️ 일부 input(`.auth-input`, `.pf-input`)은 `:focus` 시 border-color만 변경 → 키보드 가시성 약함
- **이번 트랙 C에서 보완 권고**

---

## 3. 활성 / 눌림 (Active / Pressed)

### 원칙
- 클릭 시 미세한 시각 단서 — 너무 강하면 산만, 너무 약하면 무반응 의심
- 변화: 색은 그대로, **명도 약간 어둡게** 또는 **scale 0.98**

### 표준 패턴

```css
.btn:active {
  background: var(--color-bg-elevated-3);
  transform: scale(0.98);
  transition: transform 0.04s var(--ease-quick);   /* 즉각 */
}
```

### 현재 RAAS 적용 상태
- ⚠️ 대부분 active 상태 정의 없음 — 브라우저 기본 동작 (OS별 상이)
- 영향 미미 (사용자가 클릭 후 다음 상태로 즉시 이동하면 보이지 않음)
- **우선순위 낮음**

---

## 4. 로딩 (Loading / Progress)

### 원칙
- **두 종류**:
  - 짧은 (≤500ms): 스피너나 placeholder 텍스트
  - 긴 (>500ms): 스켈레톤 또는 명시적 진행
- 로딩 중 **버튼 disabled + 텍스트 "..." 또는 "처리 중"**으로 변경 → 더블 클릭 방지

### 표준 패턴

```css
/* 스켈레톤 (긴 로딩) */
.skeleton {
  background: linear-gradient(
    90deg,
    var(--color-bg-elevated-2) 25%,
    var(--color-bg-elevated-3) 50%,
    var(--color-bg-elevated-2) 75%
  );
  background-size: 200% 100%;
  animation: skel 1.4s ease-in-out infinite;
  border-radius: var(--rad-sm);
}
@keyframes skel {
  0%   { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}

/* 버튼 로딩 */
.btn[disabled] {
  opacity: 0.6;
  cursor: not-allowed;
  pointer-events: none;
}
```

```js
// JS 패턴
async function _doLogin(ev) {
  const btn = ev.target.querySelector('button[type="submit"]');
  btn.disabled = true;
  const orig = btn.textContent;
  btn.textContent = '로그인 중…';
  try {
    /* 로직 */
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
  }
}
```

### 현재 RAAS 적용 상태
- ✅ Auth 버튼: `btn.disabled = true; … finally { btn.disabled = false; }` 패턴 적용
- ✅ 브리핑 카드: `"..."` placeholder → fetch 후 실제 값
- ⚠️ Auth 버튼 텍스트는 그대로 "로그인" — **"로그인 중…"으로 변경 권고**
- ⚠️ 통계 페이지: `<div class="hist-empty-msg">로드 중...</div>` 정적 텍스트 — **스켈레톤으로 권고** (옵션)

---

## 5. 빈 상태 (Empty)

### 원칙
- "데이터가 없습니다" 보다 **다음 행동 제시**
- 일러스트레이션 X, **간결한 한 줄 + 다음 액션 단서**
- 색은 `--color-text-disabled` (눈에 띄게 흐림)

### 표준 패턴

```css
.empty-state {
  padding: var(--sp-7) var(--sp-5);
  text-align: center;
  color: var(--color-text-disabled);
  font-size: var(--fs-md);
  line-height: 1.7;
}
.empty-state-cta {
  color: var(--color-interactive);
  cursor: pointer;
  text-decoration: underline;
}
```

### 권장 카피 패턴

| 컨텍스트 | ❌ 나쁜 예 | ✅ 좋은 예 |
|---|---|---|
| 질의 이력 0건 | "데이터 없음" | "아직 질의 내역이 없어요. 첫 질문을 던져보세요." |
| 검색 결과 0건 | "결과 없음" | "검색어 OO에 맞는 결과가 없어요. 다른 표현으로 시도해 보세요." |
| 통계 페이지 fact 0건 | "데이터 없음" | "최근 30일 질의가 누적되면 분포가 보입니다." |

### 현재 RAAS 적용 상태
- ✅ 추천 칩이 그 자체로 다음 액션 제시 (좋은 패턴)
- ✅ 일부 모달: "기간 내 데이터 없음" 등 명시
- ⚠️ 일부 `(없음)`, `데이터 없음` 같이 행동 안내 없음 — **카피 개선 권고**

---

## 6. 에러 (Error)

### 원칙
- 빨강은 **사용자가 무엇을 할 수 있는지**가 있을 때만 (단순 시스템 오류엔 약하게)
- **무엇이 안 됐는지 + 다음 행동** 한 줄
- 영구 에러는 dismiss 불가, 일시 에러는 dismiss 가능

### 표준 패턴

```css
.error-banner {
  background: rgba(242,107,107,.08);
  border: 1px solid rgba(242,107,107,.35);
  border-radius: var(--rad-md);
  padding: var(--sp-3) var(--sp-4);
  color: var(--color-text-primary);
  font-size: var(--fs-sm);
}
.error-banner-action {
  color: var(--color-danger);
  cursor: pointer;
  text-decoration: underline;
  margin-left: var(--sp-2);
}

.input-error {
  border-color: var(--color-danger);
}
.input-error-msg {
  color: var(--color-danger);
  font-size: var(--fs-xs);
  margin-top: var(--sp-1);
}
```

### 권장 카피 패턴

| 종류 | ❌ 나쁜 예 | ✅ 좋은 예 |
|---|---|---|
| 네트워크 실패 | "오류 발생" | "서버에 연결할 수 없어요. 잠시 후 다시 시도해 보세요." |
| 인증 실패 | "Unauthorized" | "비밀번호가 맞지 않아요. 다시 입력해 주세요." |
| 데이터 부재 | "Failed" | "이번 주 데이터를 불러올 수 없어요. 새로고침해 보시겠어요?" |

### 현재 RAAS 적용 상태
- ✅ `.auth-msg`, `.pf-msg` 클래스로 에러 처리 표준화
- ✅ submitQuery는 error 발생 시 메시지 표시
- ⚠️ 일부 fallback "—" 처리는 사용자가 원인을 모름 (네트워크인지 데이터 없음인지 구분 X) — **권고: 데이터 0건과 fetch 실패를 시각 구분**

---

## 7. 비활성 (Disabled)

### 원칙
- 클릭 안 됨 + 시각적으로 **확실히 약함** (opacity 0.5~0.6)
- `cursor: not-allowed`
- 다른 상태(hover, focus)는 무효

### 표준 패턴

```css
button:disabled,
input:disabled {
  opacity: 0.5;
  cursor: not-allowed;
  pointer-events: none;
}
```

### 현재 RAAS 적용 상태
- ✅ `.auth-submit:disabled`, `.brief-action-btn` 등 `opacity: 0.5; cursor: not-allowed;` 패턴 적용
- ⚠️ `pointer-events: none` 누락 → 일부 환경에서 마우스 오버 시 색 변화 보임 (해당 사항 미미)

---

## 8. 적용 체크리스트 (새 컴포넌트 작성 시)

새 컴포넌트 만들 때 다음을 확인:

- [ ] **호버**: 배경 농도 변화 + `--dur-fast` 트랜지션
- [ ] **포커스**: `:focus-visible` 룰 적용 (`outline: 2px var(--color-interactive)`)
- [ ] **활성**: 작은 시각 단서 (옵션)
- [ ] **로딩**: 버튼 disabled + 텍스트 변경 / 또는 스켈레톤
- [ ] **빈 상태**: 다음 액션 안내 한 줄
- [ ] **에러**: 무엇이 안 됐는지 + 다음 행동 한 줄
- [ ] **비활성**: opacity 0.5 + cursor not-allowed + pointer-events none
- [ ] **모든 색은 시맨틱 토큰** (16진수 인라인 금지)
- [ ] **모션은 0.12~0.2s** 이내

---

## 9. 우선순위가 높은 미적용 항목 (Track C 후속 작업)

| 항목 | 영향 | 노력 | 우선순위 |
|---|---|---|---|
| **글로벌 `:focus-visible` 룰 추가** | 키보드 사용자 접근성 ↑ | 5분 | 🔴 높음 |
| Auth 버튼 로딩 시 텍스트 변경 ("로그인 중…") | 더블클릭 방지 + 명확성 | 10분 | 🟡 중간 |
| 통계 모달 로딩 스켈레톤 | 정적 텍스트 → 시각 진행감 | 30분 | 🟢 낮음 (기능 동작 시 빠름) |
| 빈 상태 카피 톤 통일 | 사용자 안내 일관성 | 20분 | 🟡 중간 |
| 에러: "—" 의미 분리 (no-data vs fetch-fail) | 사용자 혼란 ↓ | 15분 | 🟢 낮음 |

→ **이번 트랙 C 블록에서는 🔴 글로벌 focus-visible + 🟡 Auth 버튼 로딩 텍스트를 적용**합니다.

---

## 10. 한 줄 요약

> **6개 상태(호버·포커스·활성·로딩·빈·에러)에 시맨틱 토큰만 쓰고 0.12~0.2s 안에 반응한다. 빈/에러 상태는 다음 행동을 안내한다. 포커스 가시성이 가장 중요.**
