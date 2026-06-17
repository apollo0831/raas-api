# RAAS_ACCESSIBILITY.md

> RAAS 대시보드의 접근성(A11y) 기준선과 현재 감사 결과.
> 참고: WCAG 2.1 AA, WAI-ARIA 1.2, [RAAS_INTERACTION_STATES.md](RAAS_INTERACTION_STATES.md), [RAAS_DESIGN_TOKENS.md](RAAS_DESIGN_TOKENS.md)
> 최종 갱신: 2026-06-11

---

## 0. 왜 접근성인가

RAAS는 사내 분석 도구지만 다음 사용자 시나리오를 가정한다:
- **키보드 전용 조작** — 마우스가 어색한 분석가 (스크린리더 X, 키보드 O)
- **저시력 / 색약** — 빨강↔초록 신호의 색맹 안전성
- **반복 사용으로 인한 피로** — 스크린리더 안 써도 명확한 라벨이 인지 부담을 낮춤

목표: **WCAG 2.1 AA 준수.** AAA는 차트/그래프 안에서만 부분 적용.

---

## 1. 4대 접근성 원칙 (POUR)

| 원칙 | 약식 | RAAS에서 의미 |
|---|---|---|
| **P**erceivable | 인지 가능 | 색 의존 금지, 대비비 ≥ 4.5:1, 아이콘에 텍스트/라벨 |
| **O**perable | 조작 가능 | 모든 액션을 키보드만으로 도달·실행 가능 |
| **U**nderstandable | 이해 가능 | 라벨/플레이스홀더가 의도를 명확히, 에러는 어디가 왜 |
| **R**obust | 견고함 | 시맨틱 HTML + ARIA로 보조 기술이 해석 가능 |

---

## 2. 색 대비비 (Contrast Ratio)

WCAG 2.1 AA 기준:
- 일반 텍스트: **≥ 4.5:1**
- 대형 텍스트 (≥ 18pt 또는 14pt bold): **≥ 3:1**
- UI 컴포넌트 / 그래픽: **≥ 3:1**

### Dark 테마 대비비 점검

| 토큰 조합 | 비율 | AA 일반 | AA 대형 | 사용처 |
|---|---|---|---|---|
| `--text` #e8edf3 / `--bg0` #0d1117 | **13.6:1** | ✅ AAA | ✅ AAA | 본문 |
| `--text2` #d1d8e0 / `--bg0` #0d1117 | **11.2:1** | ✅ AAA | ✅ AAA | 보조 본문 |
| `--sub` #8b95a5 / `--bg0` #0d1117 | **4.9:1** | ✅ AA | ✅ AAA | 캡션, 메타 |
| `--dim` #4a5566 / `--bg0` #0d1117 | **2.1:1** | ❌ Fail | ❌ Fail | placeholder, disabled, kpi-loading |
| `--accent` #4f8ef7 / `--bg0` #0d1117 | **5.4:1** | ✅ AA | ✅ AAA | 링크, 버튼 |
| `--green` #34c78a / `--bg0` #0d1117 | **7.5:1** | ✅ AAA | ✅ AAA | success 신호 |
| `--red` #f26b6b / `--bg0` #0d1117 | **5.2:1** | ✅ AA | ✅ AAA | danger 신호 |
| `--yellow` #f5a623 / `--bg0` #0d1117 | **8.1:1** | ✅ AAA | ✅ AAA | warning 신호 |

### Light 테마 대비비 점검

| 토큰 조합 | 비율 | AA 일반 | 사용처 |
|---|---|---|---|
| `--text` #111827 / `--bg0` #f0f2f5 | **15.3:1** | ✅ AAA | 본문 |
| `--sub` #6b7a90 / `--bg0` #f0f2f5 | **4.7:1** | ✅ AA | 캡션 |
| `--dim` #9aa3b2 / `--bg0` #f0f2f5 | **2.8:1** | ❌ Fail | placeholder, disabled |
| `--accent` #2563eb / `--bg0` #f0f2f5 | **5.1:1** | ✅ AA | 링크 |

### ⚠ 결론: `--dim` 토큰의 disabled/placeholder 용도는 양 테마 모두 AA 미달

**대응 옵션:**
- (a) disabled는 contrast 3:1 면제 (WCAG 2.1 §1.4.11에 명시) — 그대로 유지
- (b) placeholder는 본문 텍스트로 간주됨 → 토큰 분리 (`--color-text-placeholder` 신설)
- (c) `--dim`을 살짝 밝게 조정 (#5a6680 → 3:1 통과)

→ **결정: (b) + (c) 혼합. dim은 disabled-only로 격하, placeholder 전용 토큰 신설.** (D-007에서 결정)

---

## 3. 키보드 조작 (Operable)

### 기본 원칙

1. **모든 인터랙티브 요소는 Tab으로 도달 가능** — `<button>` / `<a>` / `<input>` 사용
2. **포커스 순서는 시각적 순서와 일치** — `tabindex` 양수값 금지 (0 / -1만)
3. **포커스 트랩** — 모달 열림 시 Tab은 모달 안에서만 순환, Esc로 닫힘
4. **포커스 가시화** — `:focus-visible`로 키보드 사용자에게만 outline 표시

### 표준 키 조합

| 컨텍스트 | 키 | 동작 |
|---|---|---|
| 일반 | Tab / Shift+Tab | 다음/이전 포커스 |
| 일반 | Enter | 버튼 클릭, 링크 이동 |
| 일반 | Space | 버튼 클릭 (스크롤 X) |
| 모달 | Esc | 닫기 |
| chatInput | Enter | 전송 |
| chatInput | Shift+Enter | 줄바꿈 |
| chatInput | ↑ | 직전 질의 불러오기 (TODO) |
| 차트 | Tab | 다음 데이터포인트 (TODO) |

### 현재 RAAS 키보드 감사

| 요소 | Tab 도달 | Enter/Space | 비고 |
|---|---|---|---|
| `.quick-query-btn` (사이드바) | ✅ | ✅ | button 사용 |
| `.history-item` | ✅ | ✅ | button 사용 |
| `.btn-new-chat` | ✅ | ✅ | button 사용 |
| `.btn-icon` (사이드바/패널 토글) | ✅ | ✅ | button + `title` (라벨 부족) |
| `.btn-send` | ✅ | ✅ | SVG only, **`aria-label` 없음** ⚠ |
| `.suggest-chip` (welcome) | ✅ | ✅ | button 사용 |
| `.fb-btn` (피드백) | ✅ | ✅ | button 사용 |
| `.brief-action-btn` | ✅ | ✅ | button + 이모지+텍스트 |
| 모달 닫기 | ✅ | ✅ | Esc 동작 확인 필요 |
| 차트 svg | ❌ | ❌ | 키보드 인터랙션 미구현 (낮은 우선순위) |

---

## 4. 시맨틱 HTML & ARIA (Robust)

### 4.1 현재 ARIA 사용 (1건)

```
sgst-chip-icon: aria-label (3곳: 추천/동료/유사)
```

**라이브 리전 (aria-live), 시맨틱 역할 (role=…), 상태 (aria-selected/expanded) 사용 0건.**

### 4.2 보강 필요 ARIA — 우선순위

#### P1 (Critical, 이번 작업에서 수정)

| 요소 | 현재 | 수정 |
|---|---|---|
| `#btnSend` | SVG only | `aria-label="전송"` 추가 |
| `#btnToggleSidebar` / `#btnToggleKpi` / `#btnCloseKpi` / `#btnCloseKpiMobile` | `title=` 만 | `aria-label` 추가 |
| `#chatInput` (textarea) | placeholder만 | `aria-label="질의 입력"` 추가 |
| `#authLoginMsg` / `#authRegMsg` | 일반 div | `role="status" aria-live="polite"` |
| `#welcomeScreen .welcome-logo` | `<div>` | `<h1>`으로 변경 |

#### P2 (Important, 다음 사이클)

| 요소 | 보강 |
|---|---|
| `.auth-tabs > button` | `role="tablist"` + `role="tab"` + `aria-selected` |
| `.tab-content` | `role="tabpanel"` + `aria-labelledby` |
| `#chatThread` | `role="log" aria-live="polite" aria-relevant="additions"` |
| `.chat-bubble.assistant` 스트리밍 중 | `aria-busy="true"` |
| 모달 열림 | `role="dialog" aria-modal="true" aria-labelledby` |

#### P3 (Nice-to-have)

| 요소 | 보강 |
|---|---|
| 차트 svg | `role="img" aria-label="..."` |
| 정렬 가능한 테이블 헤더 | `aria-sort="ascending\|descending"` |
| 페이지 navigation | `aria-current="page"` |

---

## 5. 라이브 리전 (Live Regions)

### 사용 패턴

| 변화 종류 | 속성 | RAAS 사용처 |
|---|---|---|
| 채팅 답변 추가 | `role="log" aria-live="polite"` | chatThread |
| 저장/완료 토스트 | `role="status" aria-live="polite"` | authLoginMsg, authRegMsg |
| 위험 에러 | `role="alert" aria-live="assertive"` | (현재 없음) |
| 진행 표시 | `role="progressbar" aria-busy="true"` | 스트리밍 중 assistant 버블 |

**중요:** `aria-live="assertive"`는 사용자 입력을 방해할 수 있으므로 **치명적 에러에만 사용**. 일반 안내는 polite.

---

## 6. 폼 접근성

### 6.1 라벨 연결 패턴

```
<!-- (a) 명시적 for/id 연결 - 권장 -->
<label for="loginId">아이디</label>
<input id="loginId" name="login_id">

<!-- (b) 라벨 wrap - 간결 -->
<label>
  아이디
  <input name="login_id">
</label>

<!-- (c) 시각적 라벨 X / aria-label -->
<input aria-label="검색" placeholder="검색">
```

### 6.2 에러 표시 패턴

```
<input id="pw" aria-invalid="true" aria-describedby="pwErr">
<div id="pwErr" role="alert">비밀번호는 4자 이상이어야 합니다.</div>
```

### 6.3 현재 RAAS 폼 감사

- ✅ 로그인/가입: `<label>` wrap 패턴 사용 — OK
- ✅ `autocomplete="username/current-password/new-password/name"` 적용 — 비밀번호 매니저 호환
- ⚠ `#chatInput` textarea: placeholder만 있음 → **`aria-label` 추가 필요**
- ⚠ 에러 div: `role="alert"` 또는 `aria-live` 없음 → 스크린리더가 못 읽음

---

## 7. 모션과 애니메이션

### prefers-reduced-motion 대응

```css
@media (prefers-reduced-motion: reduce) {
  *,*::before,*::after{
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

**현재 RAAS:** 미적용. P3로 분류하고 다음 사이클에 추가.

---

## 8. 색 의존 금지 (Use of Color)

WCAG §1.4.1 — **색만으로** 상태/의미를 전달하면 안 됨.

| 상태 | 색 + 아이콘/텍스트 | 색 only |
|---|---|---|
| 성공 ▲ | 초록 + 화살표 + "+5.2%" | ❌ |
| 위험 ▼ | 빨강 + 화살표 + "-3.1%" | ❌ |
| 경고 | 노랑 + ⚠ + 텍스트 | ❌ |

**현재 RAAS:** 변화율 표시는 `▲/▼` 화살표 + 숫자 + 색 → ✅ OK

차트 라인의 채널 구분은 색 only → ⚠ 범례 위치/패턴/직접 라벨 보강 검토 (P3)

---

## 9. 이번 작업 적용 수정 (3건)

이 사이클에서 다음 3가지를 즉시 적용한다 (P1만):

### 9.1 ✅ 글로벌 `:focus-visible` (이미 완료)

[raas_web.html:112-129](../raas_web.html#L112-L129)

### 9.2 ✅ `aria-label` 추가 — SVG 아이콘 버튼

대상:
- `#btnSend` → `aria-label="전송"`
- `#btnToggleSidebar` → `aria-label="사이드바 토글"`
- `#btnToggleKpi` → `aria-label="KPI 패널 토글"`
- `#btnCloseKpi` / `#btnCloseKpiMobile` → `aria-label="닫기"`

### 9.3 ✅ `#chatInput` textarea — `aria-label` 추가

`aria-label="질의 입력"` (placeholder 보강)

### 9.4 ✅ 에러/안내 메시지 — `role="status" aria-live="polite"`

`#authLoginMsg`, `#authRegMsg`에 적용.

### 9.5 ✅ `welcome-logo` — `<h1>` 시맨틱 변경

`<div class="welcome-logo">` → `<h1 class="welcome-logo">`

### 9.6 ✅ disabled/placeholder 대비 — `--dim` 분리

`--color-text-placeholder` 신설 → `--sub`(4.9:1) 사용.
`--color-text-disabled`는 그대로 `--dim` 유지 (WCAG disabled 면제).

---

## 10. 다음 사이클 백로그 (P2/P3)

- [ ] auth-tabs / tab-content에 ARIA tabs 패턴 적용
- [ ] chatThread에 `role="log" aria-live="polite"`
- [ ] 모달에 `role="dialog" aria-modal="true"` + 포커스 트랩
- [ ] `prefers-reduced-motion` 대응 미디어 쿼리
- [ ] 차트 svg에 `role="img" aria-label`
- [ ] 정렬 테이블 헤더 `aria-sort`
- [ ] 키보드 단축키 도움말 모달 (?)

---

## 11. 점검 방법

### 자동 점검
```
# Chrome Lighthouse (Accessibility 점수)
# axe DevTools 확장 (브라우저)
# WAVE 확장
```

### 수동 점검
1. **키보드만**: Tab 키만 사용해 모든 작업 완수 가능한가?
2. **스크린리더**: NVDA(윈도우) / VoiceOver(맥)로 한 번 훑어보기
3. **확대**: 브라우저 200% 확대 시 가로 스크롤 없이 사용 가능한가?
4. **색약 시뮬레이터**: Chrome DevTools > Rendering > Emulate vision deficiencies

### 분기별 회귀 점검
- 매 분기 첫 주: Lighthouse Accessibility 점수 ≥ 95 유지 확인
- 새 컴포넌트 추가 시: 이 문서 §4.2 P1 항목 통과 후 PR 머지

---

## 12. 참고

- [WCAG 2.1 Quick Reference](https://www.w3.org/WAI/WCAG21/quickref/)
- [WAI-ARIA Authoring Practices 1.2](https://www.w3.org/WAI/ARIA/apg/)
- [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/)
- [GOV.UK Design System — Accessibility](https://design-system.service.gov.uk/accessibility/)
