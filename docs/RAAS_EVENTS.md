# RAAS PostHog 이벤트 명세 v1

이 문서는 RAAS의 사용 데이터를 측정하기 위한 5개 이벤트의 단일 진실 소스(Single Source of Truth)입니다.
[docs/RAAS_METRICS.md](docs/RAAS_METRICS.md)와 함께 사용. 측정 정의가 흔들리면 NSM도 흔들리므로, 이 문서의 변경은 [docs/decisions.md](docs/decisions.md)에 사유를 남깁니다.

---

## 1. PostHog 도입 결정

| 항목 | 결정 |
|------|------|
| **호스팅 방식** | PostHog Cloud 무료 플랜 (Personal — 월 1M 이벤트 무료) |
| **이유** | 자가 호스팅 도커 운영 부담 회피, 4주 PoC 단계에 적합. 데이터량 폭증 시 자가 호스팅 전환 검토 |
| **프로젝트명** | `raas-prod` |
| **데이터 리전** | EU (사용자 데이터 보호) |
| **SDK** | `posthog-js` (브라우저), CDN 로드 |

> **참고**: PostHog Cloud는 OSS 친화적이며 자가 호스팅으로 이관 시 동일 SDK·이벤트 정의를 재사용 가능. 4주 후 사용량·민감도 평가하고 자가 호스팅 검토.

---

## 2. 사용자 식별 (Identification)

PostHog의 사용자 식별은 측정 신뢰성의 기반.

### 식별 시점
- **로그인 직후**: `posthog.identify(user.login_id, { name, role, is_admin })`
- **로그아웃**: `posthog.reset()`
- **비로그인 상태**: 익명 ID(자동 생성)로 추적, 로그인 시 자동 병합

### 식별 속성 (user properties)
| 키 | 타입 | 값 |
|----|------|-----|
| `name` | string | 사용자 이름 |
| `role` | string | 직무 (제작/편성/서비스운영/...) |
| `is_admin` | bool | 관리자 여부 |
| `my_programs` | string[] | 담당 프로그램 코드 배열 |

---

## 3. 5개 이벤트 명세

이름은 **소문자 + 동사_명사** 규칙. 4주 동안 절대 변경하지 않음.

### 3.1 `view_briefing` — 브리핑 카드 노출

> **NSM 측정의 기반.** 이 이벤트가 잘못 발사되면 모든 지표가 망가짐.

| 항목 | 값 |
|------|-----|
| **발사 시점** | "이번 주 브리핑 카드"가 화면에 **렌더링되어 사용자 viewport에 진입**한 시점 |
| **발사 위치** | [raas_web.html](raas_web.html) 브리핑 카드 컴포넌트 (Week 3 신설) |
| **중복 방지** | 한 세션(30분) 안에서 1회만. `IntersectionObserver`로 50% 가시 시 발사 |
| **측정 의미** | NSM(WAAU)의 분자, Leading 지표 4종의 분모 |

**속성 (event properties)**
| 키 | 타입 | 예시 | 설명 |
|----|------|------|------|
| `card_type` | string | `"weekly_review"` | 4주차에는 항상 weekly_review. 향후 다른 카드 추가 시 구분 |
| `briefing_date` | string | `"2026-06-15"` | 브리핑 대상 주의 월요일 날짜 |
| `chart_count` | int | `3` | 카드 내 차트 개수 |
| `data_completeness` | float | `0.994` | 데이터 완전성 (0.0~1.0) |
| `is_first_visit_today` | bool | `true` | 오늘 첫 진입 여부 |

---

### 3.2 `ask_ai` — AI 챗 질문 전송 ★ D3 결정 신호

> [docs/RAAS_METRICS.md](docs/RAAS_METRICS.md) 5절 D3 결정 규칙의 입력 신호.

| 항목 | 값 |
|------|-----|
| **발사 시점** | 사용자가 채팅 입력창에서 **전송 버튼 클릭 또는 Enter** 누른 직후 (요청 전송 시점) |
| **발사 위치** | [raas_web.html](raas_web.html) `#btnSend` 핸들러 |
| **중복 방지** | 없음. 매 발신마다 1건 |
| **측정 의미** | D3 결정 신호의 분자 |

**속성**
| 키 | 타입 | 예시 | 설명 |
|----|------|------|------|
| `source` | enum | `"briefing_card"` / `"main_input"` / `"sidebar_chip"` | 어디서 챗을 시작했는지 ★ D3에 핵심 |
| `query_text` | string | `"왜 떨어졌어?"` | 질의 원문 (분석용, 향후 마스킹 검토) |
| `query_length` | int | `7` | 글자 수 |
| `briefing_open` | bool | `true` | 브리핑 카드가 같은 세션에 노출된 적 있는지 |
| `seconds_since_briefing` | int | `42` | 브리핑 노출 후 경과 초 (없으면 null) |

> **`source`가 D3 결정의 본질.** `briefing_card`에서 발생한 `ask_ai`만이 "브리핑 본 뒤 자연스럽게 챗으로 이어졌나"를 측정함. 다른 곳에서 발사된 챗은 D3 신호에 포함하지 않음.

---

### 3.3 `open_chart` — 차트 카드 노출

| 항목 | 값 |
|------|-----|
| **발사 시점** | 차트가 viewport에 진입한 시점 (50% 가시) |
| **발사 위치** | Vega-Lite 렌더러의 마운트 콜백 |
| **중복 방지** | 같은 차트 인스턴스 ID는 세션 내 1회만 |
| **측정 의미** | 차트 도달률, `chart_click` 분모 |

**속성**
| 키 | 타입 | 예시 | 설명 |
|----|------|------|------|
| `chart_type` | enum | `"line"` / `"bar_rank"` / `"funnel"` / ... | 9종 차트 타입 |
| `chart_id` | string | `"vg_brief_dau"` | 차트 인스턴스 식별자 |
| `metric` | string | `"dau"` | 표현 지표 |
| `data_completeness` | float | `0.994` | 데이터 완전성 |
| `source` | enum | `"briefing_card"` / `"chat_answer"` / `"sidebar"` | 차트 등장 맥락 |
| `rationale_visible` | bool | `true` | AI 선택 근거(rationale)가 화면에 표시되었나 |

---

### 3.4 `chart_click` — 차트 인터랙션

| 항목 | 값 |
|------|-----|
| **발사 시점** | 차트의 hover 정밀 툴팁 ≥ 1초 / 드래그 줌 / 더블클릭 리셋 / legend 클릭 |
| **발사 위치** | Vega-Lite signal 핸들러 |
| **중복 방지** | 동일 차트의 동일 액션 타입은 5초 디바운스 |
| **측정 의미** | 차트가 "보는 것"에서 "다루는 것"으로 전환됐는지 |

**속성**
| 키 | 타입 | 예시 | 설명 |
|----|------|------|------|
| `chart_type` | enum | `"line"` | open_chart와 동일 분류 |
| `chart_id` | string | `"vg_brief_dau"` | open_chart와 매칭 |
| `action` | enum | `"hover_tooltip"` / `"zoom"` / `"reset"` / `"legend_toggle"` | 인터랙션 종류 |
| `time_since_open_ms` | int | `3500` | 차트 노출 후 인터랙션까지 ms |

---

### 3.5 `share_view` — 캡처/링크 공유

| 항목 | 값 |
|------|-----|
| **발사 시점** | PNG 다운로드 버튼 클릭 **또는** 링크 복사 버튼 클릭 |
| **발사 위치** | 카드/차트 우상단 공유 단추 핸들러 |
| **중복 방지** | 없음. 매 클릭마다 1건 |
| **측정 의미** | "회의 도구"로의 진화 신호. 가드레일 보완 |

**속성**
| 키 | 타입 | 예시 | 설명 |
|----|------|------|------|
| `share_type` | enum | `"png"` / `"link"` | 공유 방식 |
| `target` | enum | `"briefing_card"` / `"chart"` | 무엇을 공유했나 |
| `chart_type` | enum or null | `"line"` | target=chart일 때만 |
| `briefing_date` | string | `"2026-06-15"` | 대상 주 |

---

## 4. 이벤트 명세 요약 표

| 이벤트 | 발사 시점 | 핵심 속성 | 측정 의미 |
|--------|-----------|-----------|-----------|
| `view_briefing` | 카드 50% 가시 | `card_type`, `briefing_date` | NSM 분자 |
| `ask_ai` | 전송 클릭/Enter | `source`(★), `briefing_open` | D3 신호 분자 |
| `open_chart` | 차트 50% 가시 | `chart_type`, `source`, `rationale_visible` | 차트 도달 |
| `chart_click` | hover≥1s/zoom/reset/legend | `action`, `time_since_open_ms` | 인터랙션 가치 |
| `share_view` | PNG/링크 클릭 | `share_type`, `target` | 회의 도구화 |

---

## 5. 구현 코드 가이드 (트랙 B 참조용)

### 5.1 SDK 로드 ([raas_web.html](raas_web.html) `<head>`에 추가)
```html
<script>
  !function(t,e){var o,n,p,r;e.__SV||(window.posthog=e,e._i=[],e.init=function(i,s,a){...})}(document,window.posthog||[]);
  posthog.init('phc_YOUR_KEY', { api_host: 'https://eu.posthog.com', autocapture: false });
</script>
```

> **`autocapture: false`** — 자동 캡처는 노이즈가 많음. 5개 이벤트만 명시적으로 측정.

### 5.2 식별 (로그인 직후)
```js
posthog.identify(user.login_id, {
  name: user.name,
  role: user.role,
  is_admin: user.is_admin,
  my_programs: user.my_programs
});
```

### 5.3 발사 헬퍼 (5개 이벤트 공통 진입점 권장)
```js
function trackEvent(name, props) {
  if (window.posthog) posthog.capture(name, props || {});
}
```

이벤트명 typo 방지 위해 상수로:
```js
const EVT = {
  VIEW_BRIEFING: 'view_briefing',
  ASK_AI: 'ask_ai',
  OPEN_CHART: 'open_chart',
  CHART_CLICK: 'chart_click',
  SHARE_VIEW: 'share_view',
};
```

### 5.4 view_briefing — IntersectionObserver 패턴
```js
const observer = new IntersectionObserver((entries) => {
  entries.forEach(e => {
    if (e.isIntersecting && e.intersectionRatio >= 0.5) {
      if (!card.dataset.viewed) {
        trackEvent(EVT.VIEW_BRIEFING, {
          card_type: 'weekly_review',
          briefing_date: card.dataset.date,
          chart_count: card.querySelectorAll('.chart').length,
          data_completeness: parseFloat(card.dataset.completeness),
          is_first_visit_today: isFirstToday(),
        });
        card.dataset.viewed = '1';
      }
    }
  });
}, { threshold: 0.5 });
observer.observe(card);
```

### 5.5 ask_ai — 전송 핸들러
```js
btnSend.addEventListener('click', () => {
  const briefingEl = document.querySelector('.brief-card');
  trackEvent(EVT.ASK_AI, {
    source: window._lastChatSource || 'main_input',
    query_text: chatInput.value,
    query_length: chatInput.value.length,
    briefing_open: !!briefingEl,
    seconds_since_briefing: getSecondsSinceBriefing(),
  });
  // 기존 전송 로직...
});
```

`source`는 챗 진입 트리거가 `briefing_card`(카드 안 "이 데이터 물어보기"), `main_input`(메인 입력창), `sidebar_chip`(빠른 질의 칩) 중 어디인지에 따라 변수에 미리 세팅.

---

## 6. PostHog 대시보드 사전 구성

Day 1에 만들어 둘 대시보드 위젯 4개.

| 위젯 | 내용 | 목적 |
|------|------|------|
| **WAAU 추이** | 일별 `view_briefing` unique users (7일 롤링) | NSM 모니터링 |
| **D3 신호** | `ask_ai` (source=briefing_card) / `view_briefing` 비율 일별 | 챗 위상 결정 |
| **행동 funnel** | `view_briefing` → `chart_click` → `share_view` | 행동 깊이 |
| **chart_type 분포** | `open_chart` 의 chart_type 도수 | AI 선택 패턴 |

PostHog Insights에 4개를 만들고 한 Dashboard("RAAS 4주 PoC")에 묶어 매주 금요일 15분 리뷰에서 본다.

---

## 7. 데이터 거버넌스·프라이버시

| 항목 | 결정 |
|------|------|
| **수집 PII** | login_id, name, role (사내 도구 한정) |
| **민감 데이터** | `query_text`는 자연어 질의 — 향후 정책 검토 (현재는 평문 저장, 4주 후 마스킹 검토) |
| **외부 공유** | PostHog는 사내 데이터 분석 용도. 외부 공유 금지 |
| **삭제 정책** | PostHog 기본 보관 7년. 4주 PoC 종료 시 정책 재검토 |

---

## 8. 검증 체크리스트 (Day 1 종료 시점)

- [ ] PostHog 프로젝트 `raas-prod` 생성
- [ ] API 키 [.env](.env)에 `POSTHOG_KEY` 추가
- [ ] [raas_web.html](raas_web.html)에 SDK 로드 + autocapture:false
- [ ] 로그인 직후 `identify()` 호출 동작 확인 (PostHog Live Events 탭)
- [ ] 5개 이벤트 명세대로 발사되는지 각각 1회 이상 라이브 확인
- [ ] 4개 대시보드 위젯 생성 + Dashboard 묶음
- [ ] [docs/RAAS_METRICS.md](docs/RAAS_METRICS.md)에 PostHog 대시보드 링크 추가

---

## 9. 이벤트 변경 정책

이 명세는 **4주 동안 잠금**. 다음 경우에만 변경 허용:
- 이벤트가 의도와 다르게 발사됨이 발견될 때 (예: view_briefing이 두 번 발사)
- 새 표면(카드 종류 추가 등)이 생겨 속성 1개 추가가 필요할 때

변경 시 [docs/decisions.md](docs/decisions.md)에 사유 + 변경 전후 명시.

---

## 10. 한 줄 요약

> **5개 이벤트만 측정. `view_briefing`은 NSM 분모, `ask_ai`의 `source` 속성이 D3 결정의 핵심. PostHog Cloud 무료 플랜으로 4주 PoC. 명세 변경은 잠금.**
