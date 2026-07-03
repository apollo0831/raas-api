
// ────────────────────────────────────────────────
// CONFIG
// ────────────────────────────────────────────────
// ── 세션 토큰 / 현재 사용자 ──
let RAAS_TOKEN = localStorage.getItem('raas_token') || '';
let RAAS_USER  = null;  // {id, login_id, name, role, status, is_admin, my_programs}

// ── PostHog (4주 PoC 측정) ─────────────────────────────────
// 5개 이벤트만 명시적으로 측정. RAAS_EVENTS.md 단일 진실 소스 준수.
const EVT = {
  VIEW_BRIEFING: 'view_briefing',   // Week 3 브리핑 카드 신설 후 발사
  ASK_AI:        'ask_ai',          // 챗 전송 시 (source 속성이 D3 신호의 본질)
  OPEN_CHART:    'open_chart',      // Vega-Lite 도입 후 발사
  CHART_CLICK:   'chart_click',     // Vega-Lite 도입 후 발사
  SHARE_VIEW:    'share_view',      // 공유 단추 추가 후 발사
};
let _RAAS_PH_READY = false;  // posthog.init 완료 플래그

// 부팅 시 1회 호출 — 서버 설정 받아 PostHog init
async function _bootPostHog() {
  try {
    const res = await fetch('/api/posthog-config');
    if (!res.ok) return;
    const cfg = await res.json();
    if (!cfg.ok || !cfg.enabled || !cfg.key || !window.posthog) return;
    posthog.init(cfg.key, {
      api_host:    cfg.host || 'https://eu.posthog.com',
      autocapture: false,                  // 노이즈 차단 — 5개만 명시 측정
      capture_pageview: false,             // SPA라 수동 관리가 명확
      capture_pageleave: false,
      capture_performance: false,          // Web vitals 자동 발사 차단 (D-004)
      disable_session_recording: true,     // 세션 녹화 차단 — 개인정보·1M 한도 보호 (D-004)
      persistence: 'localStorage+cookie',
    });
    _RAAS_PH_READY = true;
  } catch (_) { /* graceful: 측정 실패해도 앱 동작 무손실 */ }
}

// 모든 이벤트는 이 함수를 거친다. enabled=false일 때 no-op.
function trackEvent(name, props) {
  if (!_RAAS_PH_READY || !window.posthog) return;
  try { posthog.capture(name, props || {}); } catch (_) {}
}

// 로그인/세션복원 시 사용자 식별
function _phIdentify(user) {
  if (!_RAAS_PH_READY || !window.posthog || !user) return;
  try {
    posthog.identify(user.login_id, {
      name: user.name,
      role: user.role,
      is_admin: !!user.is_admin,
      my_programs: user.my_programs || [],
    });
  } catch (_) {}
}

// 로그아웃 시 식별 리셋
function _phReset() {
  if (!_RAAS_PH_READY || !window.posthog) return;
  try { posthog.reset(); } catch (_) {}
}

function _authHeaders() {
  return RAAS_TOKEN ? { 'Authorization': 'Bearer ' + RAAS_TOKEN } : {};
}

// 401 응답 시 토큰 클리어하고 게이트로 복귀
async function _authedFetch(url, options) {
  options = options || {};
  options.headers = Object.assign({}, options.headers || {}, _authHeaders());
  const res = await fetch(url, options);
  if (res.status === 401) {
    RAAS_TOKEN = '';
    RAAS_USER  = null;
    localStorage.removeItem('raas_token');
    _showAuthGate();
  }
  return res;
}

// ── 인증 게이트 UI 제어 ──
function _showAuthGate()  { document.getElementById('authGate').classList.add('open'); }
function _hideAuthGate()  { document.getElementById('authGate').classList.remove('open'); }

function setAuthTab(tab) {
  document.getElementById('authTabLogin').classList.toggle('active', tab === 'login');
  document.getElementById('authTabReg').classList.toggle('active',   tab === 'register');
  document.getElementById('authLoginForm').classList.toggle('hidden', tab !== 'login');
  document.getElementById('authRegForm').classList.toggle('hidden',   tab !== 'register');
  document.getElementById('authLoginMsg').classList.add('hidden');
  document.getElementById('authRegMsg').classList.add('hidden');
}

function _showAuthMsg(formId, text, kind) {
  const el = document.getElementById(formId);
  el.textContent = text;
  el.className = 'auth-msg ' + (kind || '');
}

async function doLogin(ev) {
  ev.preventDefault();
  const form = ev.target;
  const body = {
    login_id: form.login_id.value.trim(),
    password: form.password.value,
  };
  const btn = form.querySelector('button[type="submit"]');
  const origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = '로그인 중…';
  try {
    const res = await fetch('/api/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      _showAuthMsg('authLoginMsg', data.error || '로그인 실패', '');
      return false;
    }
    RAAS_TOKEN = data.token;
    RAAS_USER  = data.user;
    localStorage.setItem('raas_token', RAAS_TOKEN);
    _hideAuthGate();
    _renderSidebarUser();
    _phIdentify(RAAS_USER);
    refreshHistory();
    loadSuggestions();
  } catch (e) {
    _showAuthMsg('authLoginMsg', '네트워크 오류: ' + e.message, '');
  } finally {
    btn.disabled = false;
    btn.textContent = origText;
  }
  return false;
}

async function doRegister(ev) {
  ev.preventDefault();
  const form = ev.target;
  const body = {
    login_id: form.login_id.value.trim(),
    password: form.password.value,
    name:     form.name.value.trim(),
    role:     form.role.value,
  };
  const btn = form.querySelector('button[type="submit"]');
  const origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = '가입 요청 중…';
  try {
    const res = await fetch('/api/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      _showAuthMsg('authRegMsg', data.error || '가입 실패', '');
      return false;
    }
    if (data.status === 'approved') {
      // 부트스트랩 관리자 — 즉시 로그인
      RAAS_TOKEN = data.token;
      localStorage.setItem('raas_token', RAAS_TOKEN);
      const meRes = await _authedFetch('/api/me');
      const me = await meRes.json();
      if (me.ok) {
        RAAS_USER = me.user;
        _hideAuthGate();
        _renderSidebarUser();
        _phIdentify(RAAS_USER);
        refreshHistory();
        loadSuggestions();
      }
    } else {
      _showAuthMsg('authRegMsg',
        '가입 요청이 접수되었습니다. 관리자 승인 후 로그인하실 수 있습니다.', 'success');
      form.reset();
      setTimeout(() => setAuthTab('login'), 1500);
    }
  } catch (e) {
    _showAuthMsg('authRegMsg', '네트워크 오류: ' + e.message, '');
  } finally {
    btn.disabled = false;
    btn.textContent = origText;
  }
  return false;
}

async function doLogout() {
  try {
    await _authedFetch('/api/logout', { method: 'POST' });
  } catch (_) {}
  _phReset();
  RAAS_TOKEN = '';
  RAAS_USER  = null;
  localStorage.removeItem('raas_token');
  document.getElementById('sidebarUser').style.display = 'none';
  document.getElementById('historyList').innerHTML =
    '<div class="history-empty">아직 질의 내역이 없습니다.</div>';
  const sc = document.getElementById('suggestChips');
  if (sc) sc.innerHTML = '';
  _showAuthGate();
  setAuthTab('login');
}

function _renderSidebarUser() {
  if (!RAAS_USER) return;
  document.getElementById('suName').textContent = RAAS_USER.name;
  document.getElementById('suMeta').textContent = RAAS_USER.role || '';
  document.getElementById('sidebarUser').style.display = 'flex';
  document.getElementById('suAdminBtn').style.display =
    RAAS_USER.is_admin ? 'inline-block' : 'none';
  // 질의맵: is_admin OR role in (총괄관리, 데이터)
  const statsAllowed = RAAS_USER.is_admin
    || ['총괄관리','데이터'].includes(RAAS_USER.role);
  document.getElementById('suStatsBtn').style.display =
    statsAllowed ? 'inline-block' : 'none';
  const revTab = document.getElementById('histTabReview');
  if (revTab) revTab.style.display = statsAllowed ? 'inline-block' : 'none';
  // 관리자 전용 '참고 정보(메타 푸터)' 토글
  document.getElementById('suMetaBtn').style.display =
    RAAS_USER.is_admin ? 'inline-block' : 'none';
  if (RAAS_USER.is_admin) _applyAdminMeta();
}

// 답변의 관리자 참고 푸터(분석 대상·실데이터·추가 적재 필요) 표시/숨김 — localStorage 유지
function _applyAdminMeta() {
  const hidden = localStorage.getItem('raas_hide_meta') === '1';
  document.body.classList.toggle('hide-meta', hidden);
  const btn = document.getElementById('suMetaBtn');
  if (btn) btn.textContent = hidden ? '참고 정보: 꺼짐' : '참고 정보: 켜짐';
}
function toggleAdminMeta() {
  const hidden = localStorage.getItem('raas_hide_meta') === '1';
  localStorage.setItem('raas_hide_meta', hidden ? '0' : '1');
  _applyAdminMeta();
}

// 앱 부팅 시 호출 — 토큰이 있으면 /api/me로 복원, 없으면 게이트 표시
async function _bootAuth() {
  if (!RAAS_TOKEN) { _showAuthGate(); return false; }
  try {
    const res = await fetch('/api/me', { headers: _authHeaders() });
    if (!res.ok) throw new Error('token invalid');
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || 'no session');
    RAAS_USER = data.user;
    _renderSidebarUser();
    return true;
  } catch (_) {
    RAAS_TOKEN = '';
    localStorage.removeItem('raas_token');
    _showAuthGate();
    return false;
  }
}

// ── 관리자 모달 ──
function openAdminModal() {
  document.getElementById('adminModal').classList.add('open');
  // 데이터 유지보수(재적재) 버튼 — 데이터 직무 AND 관리자만 노출
  document.getElementById('adminMaintenance').style.display =
    (RAAS_USER && RAAS_USER.is_admin && RAAS_USER.role === '데이터') ? 'block' : 'none';
  _loadAdminUsers();
}
function closeAdminModal() {
  document.getElementById('adminModal').classList.remove('open');
}

// 🔄 최신 데이터 다시 가져오기 — 캐시 무효화+재적재+자동 재점검(SSE). 데이터 직무 관리자 전용.
function startDataRefresh() {
  if (!(RAAS_USER && RAAS_USER.is_admin && RAAS_USER.role === '데이터')) return;
  closeAdminModal();
  submitQuery('최신 데이터 다시 가져오기', 'data_refresh', { endpoint: '/api/data_refresh' });
}

async function _loadAdminUsers() {
  const body = document.getElementById('adminModalBody');
  body.innerHTML = '<div class="hist-empty-msg">로드 중…</div>';
  try {
    const res = await _authedFetch('/api/admin/users');
    if (!res.ok) {
      body.innerHTML = '<div class="hist-empty-msg" style="color:var(--red)">권한 없음 또는 오류</div>';
      return;
    }
    const data = await res.json();
    if (!data.ok) {
      body.innerHTML = `<div class="hist-empty-msg" style="color:var(--red)">${escapeHtml(data.error||'오류')}</div>`;
      return;
    }
    body.innerHTML = _renderAdminUsers(data.pending, data.all);
  } catch (e) {
    body.innerHTML = `<div class="hist-empty-msg" style="color:var(--red)">네트워크 오류: ${escapeHtml(e.message)}</div>`;
  }
}

function _renderAdminUsers(pending, all) {
  const _row = (u, showActions) => `
    <tr>
      <td>${escapeHtml(u.login_id)}</td>
      <td>${escapeHtml(u.name)}</td>
      <td>${escapeHtml(u.role)}</td>
      <td><span class="adm-badge ${u.status||'pending'}">${u.status||'pending'}</span></td>
      <td style="font-size:var(--fs-xs);color:var(--dim)">${escapeHtml(u.created_at||'')}</td>
      <td>${showActions ? `
        <button class="adm-action-btn approve" onclick="approveUser(${u.id})">승인</button>
        <button class="adm-action-btn reject"  onclick="rejectUser(${u.id})">거절</button>
      ` : ''}</td>
    </tr>`;
  const pendingHtml = pending.length ? `
    <div class="adm-section-title">승인 대기 (${pending.length})</div>
    <table class="adm-table">
      <thead><tr><th>ID</th><th>이름</th><th>직무</th><th>상태</th><th>가입일</th><th>처리</th></tr></thead>
      <tbody>${pending.map(u => _row(u, true)).join('')}</tbody>
    </table>` : '<div class="adm-section-title">승인 대기 (0)</div><div class="hist-empty-msg">대기 중인 사용자가 없습니다.</div>';
  const allHtml = `
    <div class="adm-section-title">전체 사용자 (${all.length})</div>
    <table class="adm-table">
      <thead><tr><th>ID</th><th>이름</th><th>직무</th><th>상태</th><th>가입일</th><th></th></tr></thead>
      <tbody>${all.map(u => _row(u, false)).join('')}</tbody>
    </table>`;
  return pendingHtml + allHtml;
}

async function approveUser(uid) {
  await _adminAction(uid, 'approve');
}
async function rejectUser(uid) {
  if (!confirm('이 사용자를 거절하시겠습니까? 활성 세션도 무효화됩니다.')) return;
  await _adminAction(uid, 'reject');
}
async function _adminAction(uid, action) {
  try {
    const res = await _authedFetch('/api/admin/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: uid, action }),
    });
    const data = await res.json();
    if (!data.ok) { alert(data.error || '실패'); return; }
    _loadAdminUsers();
  } catch (e) {
    alert('네트워크 오류: ' + e.message);
  }
}

// ── 질의맵 / 통계 ──────────────────────────────────────────
let _STM = { tab: 'overview', days: 30, me: null };

function openStatsModal() {
  document.getElementById('statsModal').classList.add('open');
  _loadStmTab();
}
function closeStatsModal() {
  document.getElementById('statsModal').classList.remove('open');
}
function setStmTab(tab) {
  _STM.tab = tab;
  document.querySelectorAll('#statsModal .stm-tab').forEach(b => {
    b.classList.toggle('active', b.dataset.tab === tab);
  });
  _loadStmTab();
}
function setStmPeriod(days) {
  _STM.days = days;
  document.querySelectorAll('#statsModal .stm-period-btn').forEach(b => {
    b.classList.toggle('active', String(b.dataset.d) === String(days));
  });
  _loadStmTab();
}

async function _loadStmTab() {
  const body = document.getElementById('stmBody');
  body.innerHTML = '<div class="stm-empty">로드 중…</div>';
  let path = ({
    'overview': '/api/admin/stats/overview',
    'by-role':  '/api/admin/stats/by-role',
    'by-user':  '/api/admin/stats/by-user',
    'topics':   '/api/admin/stats/topics',
    'heatmap':  '/api/admin/stats/heatmap',
    'graph':    '/api/admin/stats/graph',
  })[_STM.tab];
  // 쿼리 파라미터 — URLSearchParams로 조립('?' 전에 '&'를 붙여 404 나던 버그 방지)
  const qs = new URLSearchParams({ days: _STM.days, limit: 50 });
  if (_STM.tab === 'heatmap') {
    _STM.dim = _STM.dim || 'metric';   // 히트맵 차원 (기본 metric, 토글로 scope)
    qs.set('dimension', _STM.dim);
  }
  try {
    const res = await _authedFetch(`${path}?${qs.toString()}`);
    if (res.status === 403) {
      body.innerHTML = `<div class="stm-empty" style="color:var(--red)">통계 열람 권한이 없습니다.</div>`;
      return;
    }
    if (!res.ok) {
      body.innerHTML = `<div class="stm-empty" style="color:var(--red)">서버 오류 (${res.status})</div>`;
      return;
    }
    const json = await res.json();
    if (!json.ok) {
      body.innerHTML = `<div class="stm-empty" style="color:var(--red)">${escapeHtml(json.error || '오류')}</div>`;
      return;
    }
    if (json.me !== undefined) _STM.me = json.me;
    const render = ({
      'overview': _renderStmOverview,
      'by-role':  _renderStmByRole,
      'by-user':  _renderStmByUser,
      'topics':   _renderStmTopics,
      'heatmap':  _renderStmHeatmap,
      'graph':    _renderStmGraph,
    })[_STM.tab];
    body.innerHTML = render(json.data);
  } catch (e) {
    body.innerHTML = `<div class="stm-empty" style="color:var(--red)">네트워크 오류: ${escapeHtml(e.message)}</div>`;
  }
}

function _fmtN(n) { return (n||0).toLocaleString(); }
function _fmtDate(s) { return s ? s.slice(0,16).replace('T',' ') : '—'; }

// 전체 요약 탭
function _renderStmOverview(d) {
  if (!d || d.total === 0) {
    return `<div class="stm-empty">최근 ${_STM.days||'전체'}일 질의가 없습니다.<br>새 질의가 누적되면 통계가 표시됩니다.</div>`;
  }
  const fb = d.feedback || {};
  const fbTotal = (fb.positive||0) + (fb.negative||0);
  const fbRate = fbTotal ? Math.round(fb.positive / fbTotal * 100) : null;
  const spark = _renderSparkline(d.daily_series);
  const period = _STM.days === 0 ? '전체' : `최근 ${_STM.days}일`;
  return `
    <div class="stm-kpi-grid">
      <div class="stm-kpi"><div class="stm-kpi-label">총 질문수</div>
        <div class="stm-kpi-value">${_fmtN(d.total)}</div>
        <div class="stm-kpi-sub">fact 보유 ${_fmtN(d.fact_total)}건</div></div>
      <div class="stm-kpi"><div class="stm-kpi-label">활성 사용자</div>
        <div class="stm-kpi-value">${_fmtN(d.active_users)}</div>
        <div class="stm-kpi-sub">${period}</div></div>
      <div class="stm-kpi"><div class="stm-kpi-label">총 토큰</div>
        <div class="stm-kpi-value">${_fmtN((d.tokens?.input_sum||0)+(d.tokens?.output_sum||0))}</div>
        <div class="stm-kpi-sub">in ${_fmtN(d.tokens?.input_sum)} / out ${_fmtN(d.tokens?.output_sum)}</div></div>
      <div class="stm-kpi"><div class="stm-kpi-label">평균 토큰/질의</div>
        <div class="stm-kpi-value">${_fmtN(Math.round((d.tokens?.input_avg||0)+(d.tokens?.output_avg||0)))}</div>
        <div class="stm-kpi-sub">${fbRate !== null ? `👍 ${fbRate}% (${fb.positive}/${fbTotal})` : '피드백 없음'}</div></div>
    </div>
    <div class="stm-section">
      <div class="stm-section-title">일별 질의 추이 <span class="stm-section-sub">${period}</span></div>
      ${spark}
    </div>`;
}

function _renderSparkline(series) {
  if (!series || !series.length) return `<div class="stm-empty">기간 내 데이터 없음</div>`;
  const max = Math.max(...series.map(s => s.count || 0), 1);
  const bars = series.map(s => {
    const h = Math.round((s.count||0) / max * 100);
    return `<div class="stm-spark-bar" style="height:${h}%" title="${escapeHtml(s.date)}: ${s.count}건"></div>`;
  }).join('');
  return `<div class="stm-spark">${bars}</div>`;
}

// 직무별 탭
function _renderStmByRole(rows) {
  if (!rows || !rows.length) {
    return `<div class="stm-empty">직무 정보가 있는 질의가 없습니다.<br>로그인된 사용자가 질의를 누적하면 표시됩니다.</div>`;
  }
  const max = Math.max(...rows.map(r => r.queries || 0), 1);
  const bars = rows.map(r => `
    <div class="stm-bar-row">
      <div class="stm-bar-label">${escapeHtml(r.role || '—')}</div>
      <div class="stm-bar-track"><div class="stm-bar-fill" style="width:${(r.queries/max*100).toFixed(1)}%"></div></div>
      <div class="stm-bar-count">${_fmtN(r.queries)}</div>
    </div>`).join('');
  const tbody = rows.map(r => `
    <tr>
      <td>${escapeHtml(r.role || '—')}</td>
      <td class="stm-num">${_fmtN(r.queries)}</td>
      <td class="stm-num">${_fmtN(r.users)}</td>
      <td><span class="stm-tag">${escapeHtml(r.top_intent||'—')}</span></td>
      <td><span class="stm-tag">${escapeHtml(r.top_metric||'—')}</span></td>
      <td><span class="stm-tag">${escapeHtml(r.top_scope||'—')}</span></td>
      <td class="stm-num">${_fmtN(Math.round((r.avg_input_tokens||0)+(r.avg_output_tokens||0)))}</td>
    </tr>`).join('');
  return `
    <div class="stm-section">
      <div class="stm-section-title">직무별 질문 분포</div>
      <div class="stm-bar-list">${bars}</div>
    </div>
    <div class="stm-section">
      <div class="stm-section-title">직무별 상세</div>
      <table class="stm-table">
        <thead><tr><th>직무</th><th>질문수</th><th>사용자</th><th>주 intent</th><th>주 지표</th><th>주 scope</th><th>평균 토큰</th></tr></thead>
        <tbody>${tbody}</tbody>
      </table>
    </div>`;
}

// 사용자별 탭
function _renderStmByUser(rows) {
  if (!rows || !rows.length) {
    return `<div class="stm-empty">기간 내 사용자 활동이 없습니다.</div>`;
  }
  const tbody = rows.map(r => {
    const isMe = String(r.user_id) === String(_STM.me);
    const cls = isMe ? ' class="me"' : '';
    const fb = (r.feedback_pos||0) + (r.feedback_neg||0);
    return `
      <tr${cls}>
        <td>${escapeHtml(r.user_name)}${isMe ? ' <span class="stm-tag">나</span>' : ''}</td>
        <td>${escapeHtml(r.role || '—')}</td>
        <td class="stm-num">${_fmtN(r.queries)}</td>
        <td class="stm-num">${_fmtN((r.tokens_in||0)+(r.tokens_out||0))}</td>
        <td class="stm-num">${fb ? `👍${r.feedback_pos}/👎${r.feedback_neg}` : '—'}</td>
        <td><span class="stm-tag">${escapeHtml(r.top_topic || '—')}</span></td>
        <td style="font-size:var(--fs-xs);color:var(--dim)">${_fmtDate(r.last_active)}</td>
      </tr>`;
  }).join('');
  return `
    <div class="stm-section">
      <div class="stm-section-title">사용자별 통계 <span class="stm-section-sub">상위 ${rows.length}명</span></div>
      <table class="stm-table">
        <thead><tr><th>이름</th><th>직무</th><th>질문수</th><th>토큰</th><th>피드백</th><th>대표 주제</th><th>마지막 활동</th></tr></thead>
        <tbody>${tbody}</tbody>
      </table>
    </div>`;
}

// 인기 주제 탭
function _renderStmTopics(d) {
  const topics = (d && d.by_topic_key) || [];
  const qs     = (d && d.by_question)  || [];
  if (!topics.length && !qs.length) {
    return `<div class="stm-empty">기간 내 주제 통계가 없습니다.</div>`;
  }
  const tBody = topics.map(t => `
    <tr>
      <td><span class="stm-tag">${escapeHtml(t.topic_key)}</span></td>
      <td>${escapeHtml(t.intent||'—')}</td>
      <td>${escapeHtml(t.scope||'—')}</td>
      <td>${escapeHtml(t.metric||'—')}</td>
      <td class="stm-num">${_fmtN(t.count)}</td>
      <td style="font-size:var(--fs-xs);color:var(--dim)">${_fmtDate(t.last_asked)}</td>
    </tr>`).join('');
  const qBody = qs.map(q => `
    <tr>
      <td>${escapeHtml(q.question)}</td>
      <td class="stm-num">${_fmtN(q.count)}</td>
      <td style="font-size:var(--fs-xs);color:var(--dim)">${_fmtDate(q.last_asked)}</td>
    </tr>`).join('');
  return `
    <div class="stm-section">
      <div class="stm-section-title">topic_key 빈도 <span class="stm-section-sub">intent:scope:metric 그룹핑</span></div>
      ${topics.length ? `<table class="stm-table">
        <thead><tr><th>topic_key</th><th>intent</th><th>scope</th><th>metric</th><th>건수</th><th>마지막</th></tr></thead>
        <tbody>${tBody}</tbody>
      </table>` : '<div class="stm-empty">데이터 없음</div>'}
    </div>
    <div class="stm-section">
      <div class="stm-section-title">질문 텍스트 빈도</div>
      ${qs.length ? `<table class="stm-table">
        <thead><tr><th>질문</th><th>건수</th><th>마지막</th></tr></thead>
        <tbody>${qBody}</tbody>
      </table>` : '<div class="stm-empty">데이터 없음</div>'}
    </div>`;
}

// 관계(히트맵) 탭 — 직무 × 지표/scope 빈도 매트릭스
function setStmHeatDim(dim) {
  _STM.dim = dim;
  _loadStmTab();
}

function _renderStmHeatmap(d) {
  const dim = (d && d.dimension) || 'metric';
  const toolbar = `
    <div class="stm-heat-toolbar">
      <span class="label">차원:</span>
      <button class="${dim==='metric'?'active':''}" onclick="setStmHeatDim('metric')">지표</button>
      <button class="${dim==='scope' ?'active':''}" onclick="setStmHeatDim('scope')">scope</button>
    </div>`;
  if (!d || !d.cols || d.cols.length === 0 || d.grand_total === 0) {
    return toolbar + `<div class="stm-empty">데이터가 누적되면 직무별 관심 분포가 드러납니다.<br>
      현재 ${dim==='metric'?'지표':'scope'} 정보가 있는 질의가 없습니다.</div>`;
  }
  const roles = d.roles, cols = d.cols, cells = d.cells;
  const max = Math.max(...cells.flat(), 1);
  // grid-template-columns: 행 라벨 + cols + 행 합계
  const colCount = cols.length + 2;
  const gridStyle = `grid-template-columns: minmax(120px, 1.2fr) repeat(${cols.length}, minmax(60px,1fr)) minmax(60px,0.7fr);`;
  // 헤더 행
  let html = `<div class="stm-heat" style="${gridStyle}">`;
  html += `<div class="stm-heat-cell head">직무 \\ ${dim==='metric'?'지표':'scope'}</div>`;
  for (const c of cols) html += `<div class="stm-heat-cell head" title="${escapeHtml(c)}">${escapeHtml(c)}</div>`;
  html += `<div class="stm-heat-cell head">합계</div>`;
  // 데이터 행
  for (let ri = 0; ri < roles.length; ri++) {
    html += `<div class="stm-heat-cell row-head">${escapeHtml(roles[ri])}</div>`;
    for (let ci = 0; ci < cols.length; ci++) {
      const v = cells[ri][ci] || 0;
      const opacity = v === 0 ? 0 : Math.min(1, 0.12 + (v / max) * 0.78);
      const bg = `background:rgba(79,142,247,${opacity.toFixed(2)})`;
      const tip = `${escapeHtml(roles[ri])} × ${escapeHtml(cols[ci])}: ${v}건`;
      html += `<div class="stm-heat-cell data${v===0?' zero':''}" style="${bg}" title="${tip}">${v||'·'}</div>`;
    }
    html += `<div class="stm-heat-cell total">${d.row_totals[ri]||0}</div>`;
  }
  // 열 합계 행
  html += `<div class="stm-heat-cell total" style="padding-left:10px">합계</div>`;
  for (const t of d.col_totals) html += `<div class="stm-heat-cell total">${t||0}</div>`;
  html += `<div class="stm-heat-cell total">${d.grand_total||0}</div>`;
  html += `</div>`;
  // 범례
  const legend = `<div class="stm-heat-legend">
    <span>적음</span><div class="stm-heat-legend-bar"></div><span>많음 (최댓값 ${max}건)</span>
  </div>`;
  return toolbar + `<div class="stm-heat-wrap">${html}</div>` + legend;
}

// ── 그래프 탭 — 사용자/직무/지표/대상 관계 네트워크 ────────
// vanilla force simulation (외부 의존성 0)
const _GRAPH_ROLE_COLORS = {
  '제작':'#f5a623','편성':'#34c78a','서비스운영':'#a78bfa',
  'CP':'#dc4d4d','플랫폼전략':'#4f8ef7','데이터':'#1ec9ff',
  '총괄관리':'#f26b6b','마케팅(광고·협찬)':'#ec4899',
};

function _graphNodeColor(node) {
  if (node.type === 'role')   return _GRAPH_ROLE_COLORS[node.label] || '#8b95a5';
  if (node.type === 'user')   return _GRAPH_ROLE_COLORS[node.role]  || '#8b95a5';
  if (node.type === 'metric') return 'var(--accent)';
  if (node.type === 'scope')  return 'var(--green)';
  return 'var(--sub)';
}

function _renderStmGraph(d) {
  if (!d || !d.nodes || d.nodes.length === 0) {
    return `<div class="stm-graph-empty">사용자 질의 데이터가 누적되면 관계 그래프가 표시됩니다.<br>현재 fact 보유 사용자가 없습니다.</div>`;
  }
  const meta = d.meta || {};
  const html = `
    <div class="stm-graph-wrap">
      <div class="stm-graph-canvas" id="graphCanvas">
        <svg class="stm-graph-svg" id="graphSvg" viewBox="0 0 800 480" preserveAspectRatio="xMidYMid meet"></svg>
      </div>
      <div class="stm-graph-side" id="graphSide">
        <h4>관계 그래프</h4>
        <div class="meta">최근 ${_STM.days || 0}일 · 노드 ${d.nodes.length} · 엣지 ${d.edges.length}</div>
        <div class="row"><span class="k">사용자</span><span>${meta.n_users||0}</span></div>
        <div class="row"><span class="k">직무</span><span>${meta.n_roles||0}</span></div>
        <div class="row"><span class="k">지표</span><span>${meta.n_metrics||0}</span></div>
        <div class="row"><span class="k">대상</span><span>${meta.n_scopes||0}</span></div>
        <div class="stm-graph-legend">
          <span class="lg"><span class="dot" style="background:#4f8ef7"></span>지표</span>
          <span class="lg"><span class="dot" style="background:#34c78a"></span>대상</span>
          <span class="lg"><span class="dot" style="background:#8b95a5"></span>사용자/직무</span>
        </div>
        <div style="margin-top:14px;font-size:var(--fs-xs);color:var(--dim)">노드 드래그 · 휠 줌</div>
      </div>
    </div>`;
  // SVG 렌더는 다음 tick에 (DOM 삽입 후)
  setTimeout(() => _layoutAndRenderGraph(d), 0);
  return html;
}

function _layoutAndRenderGraph(data) {
  const svg = document.getElementById('graphSvg');
  if (!svg) return;
  const side = document.getElementById('graphSide');
  const W = 800, H = 480;

  // 노드/엣지 사본 + 위치 초기화 (원형 분산)
  const nodes = data.nodes.map((n, i) => {
    const angle = (i / data.nodes.length) * Math.PI * 2;
    const r = 150 + Math.random() * 60;
    return { ...n,
      x: W/2 + Math.cos(angle) * r,
      y: H/2 + Math.sin(angle) * r,
      vx: 0, vy: 0,
      pinned: false,
      r: _graphNodeRadius(n),
    };
  });
  const nodeById = Object.fromEntries(nodes.map(n => [n.id, n]));
  const edges = data.edges
    .map(e => ({ ...e, s: nodeById[e.source], t: nodeById[e.target] }))
    .filter(e => e.s && e.t);

  // ── force simulation (~150 iter, Euler) ──
  const REPULSION = 1400;   // 노드 간 척력 상수
  const SPRING    = 0.04;   // 엣지 스프링
  const REST      = 80;     // 엣지 기본 길이
  const GRAVITY   = 0.012;  // 중앙 끌림
  const DAMPING   = 0.82;
  for (let iter = 0; iter < 150; iter++) {
    // 척력
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i+1; j < nodes.length; j++) {
        const a = nodes[i], b = nodes[j];
        let dx = b.x - a.x, dy = b.y - a.y;
        let d2 = dx*dx + dy*dy;
        if (d2 < 1) { dx = Math.random()-0.5; dy = Math.random()-0.5; d2 = 1; }
        const f = REPULSION / d2;
        const d = Math.sqrt(d2);
        const fx = (dx/d) * f, fy = (dy/d) * f;
        a.vx -= fx; a.vy -= fy;
        b.vx += fx; b.vy += fy;
      }
    }
    // 인력 (엣지 스프링)
    for (const e of edges) {
      const a = e.s, b = e.t;
      const dx = b.x - a.x, dy = b.y - a.y;
      const d  = Math.sqrt(dx*dx + dy*dy) || 1;
      const rest = REST + (e.type === 'similar' ? 40 : 0);
      const f  = SPRING * (d - rest) * (1 + Math.log(1 + (e.weight || 1)) * 0.15);
      const fx = (dx/d) * f, fy = (dy/d) * f;
      a.vx += fx; a.vy += fy;
      b.vx -= fx; b.vy -= fy;
    }
    // 중력 (캔버스 중앙으로)
    for (const n of nodes) {
      n.vx += (W/2 - n.x) * GRAVITY;
      n.vy += (H/2 - n.y) * GRAVITY;
    }
    // 업데이트 + 댐핑
    for (const n of nodes) {
      if (n.pinned) continue;
      n.vx *= DAMPING; n.vy *= DAMPING;
      n.x += n.vx; n.y += n.vy;
      // 화면 안에 가두기
      n.x = Math.max(20, Math.min(W-20, n.x));
      n.y = Math.max(20, Math.min(H-20, n.y));
    }
  }

  // ── SVG 그리기 ──
  let s = '';
  // edges 먼저
  for (const e of edges) {
    const w = Math.min(4, 0.8 + Math.log(1 + (e.weight || 1)));
    const op = e.type === 'similar' ? 0.35 : e.type === 'co_query' ? 0.55 : e.type === 'membership' ? 0.25 : 0.45;
    const stroke = e.type === 'similar' ? '#a78bfa'
                 : e.type === 'co_query' ? '#1ec9ff'
                 : e.type === 'membership' ? '#8b95a5'
                 : '#4f8ef7';
    const dash = e.type === 'similar' ? ' stroke-dasharray="3 3"' : '';
    s += `<line x1="${e.s.x.toFixed(1)}" y1="${e.s.y.toFixed(1)}" x2="${e.t.x.toFixed(1)}" y2="${e.t.y.toFixed(1)}" stroke="${stroke}" stroke-width="${w}" stroke-opacity="${op}"${dash}/>`;
  }
  // nodes
  for (const n of nodes) {
    const c = _graphNodeColor(n);
    const r = n.r;
    let shape = '';
    if (n.type === 'user') {
      shape = `<circle cx="${n.x.toFixed(1)}" cy="${n.y.toFixed(1)}" r="${r}" fill="${c}" stroke="#13181f" stroke-width="2"/>`;
    } else if (n.type === 'role') {
      // 육각형
      shape = _hexagonSVG(n.x, n.y, r, c, '#13181f', 2);
    } else if (n.type === 'metric') {
      // 사각형
      shape = `<rect x="${(n.x-r).toFixed(1)}" y="${(n.y-r).toFixed(1)}" width="${(r*2).toFixed(1)}" height="${(r*2).toFixed(1)}" fill="${c}" stroke="#13181f" stroke-width="2" rx="3"/>`;
    } else if (n.type === 'scope') {
      // 다이아몬드 (회전 사각형)
      shape = `<polygon points="${n.x},${n.y-r} ${n.x+r},${n.y} ${n.x},${n.y+r} ${n.x-r},${n.y}" fill="${c}" stroke="#13181f" stroke-width="2"/>`;
    }
    s += `<g class="gnode" data-id="${escapeHtml(n.id)}" style="cursor:pointer">${shape}<text class="stm-graph-node-label" x="${n.x.toFixed(1)}" y="${(n.y + r + 12).toFixed(1)}" text-anchor="middle">${escapeHtml(n.label.length > 14 ? n.label.slice(0,13)+'…' : n.label)}</text></g>`;
  }
  svg.innerHTML = s;

  // ── 인터랙션: 클릭 → 사이드 패널 상세, 드래그 → 위치 조정, 휠 → 줌 ──
  const _nodeMap = Object.fromEntries(nodes.map(n => [n.id, n]));
  let viewBox = [0, 0, W, H];
  let dragging = null, panStart = null;

  function _svgPoint(ev) {
    const r = svg.getBoundingClientRect();
    const x = (ev.clientX - r.left) / r.width  * viewBox[2] + viewBox[0];
    const y = (ev.clientY - r.top)  / r.height * viewBox[3] + viewBox[1];
    return { x, y };
  }

  svg.addEventListener('mousedown', ev => {
    const target = ev.target.closest('.gnode');
    const p = _svgPoint(ev);
    if (target) {
      dragging = _nodeMap[target.dataset.id];
      dragging.pinned = true;
      _showNodeDetail(dragging, nodes, edges);
    } else {
      panStart = { x: ev.clientX, y: ev.clientY, vb: [...viewBox] };
    }
  });
  window.addEventListener('mousemove', ev => {
    if (dragging) {
      const p = _svgPoint(ev);
      dragging.x = p.x; dragging.y = p.y;
      _redrawGraphPositions(svg, nodes, edges);
    } else if (panStart) {
      const dx = (ev.clientX - panStart.x) * viewBox[2] / svg.getBoundingClientRect().width;
      const dy = (ev.clientY - panStart.y) * viewBox[3] / svg.getBoundingClientRect().height;
      viewBox[0] = panStart.vb[0] - dx;
      viewBox[1] = panStart.vb[1] - dy;
      svg.setAttribute('viewBox', viewBox.join(' '));
    }
  });
  window.addEventListener('mouseup', () => {
    if (dragging) dragging.pinned = false;
    dragging = null; panStart = null;
  });
  svg.addEventListener('wheel', ev => {
    ev.preventDefault();
    const factor = ev.deltaY > 0 ? 1.15 : 1 / 1.15;
    const p = _svgPoint(ev);
    viewBox[0] = p.x - (p.x - viewBox[0]) * factor;
    viewBox[1] = p.y - (p.y - viewBox[1]) * factor;
    viewBox[2] *= factor; viewBox[3] *= factor;
    svg.setAttribute('viewBox', viewBox.join(' '));
  }, { passive: false });
}

function _graphNodeRadius(n) {
  const w = n.weight || 1;
  if (n.type === 'user')   return Math.min(14, 7 + Math.log(1 + w));
  if (n.type === 'role')   return Math.min(20, 11 + Math.log(1 + w));
  if (n.type === 'metric') return Math.min(13, 6 + Math.log(1 + w));
  if (n.type === 'scope')  return Math.min(13, 6 + Math.log(1 + w));
  return 8;
}

function _hexagonSVG(cx, cy, r, fill, stroke, sw) {
  const pts = [];
  for (let i = 0; i < 6; i++) {
    const a = Math.PI / 3 * i - Math.PI / 6;
    pts.push(`${(cx + Math.cos(a) * r).toFixed(1)},${(cy + Math.sin(a) * r).toFixed(1)}`);
  }
  return `<polygon points="${pts.join(' ')}" fill="${fill}" stroke="${stroke}" stroke-width="${sw}"/>`;
}

function _redrawGraphPositions(svg, nodes, edges) {
  // 엣지·노드 위치만 빠르게 업데이트 (전체 재그리지 않음)
  const lines = svg.querySelectorAll('line');
  edges.forEach((e, i) => {
    const ln = lines[i]; if (!ln) return;
    ln.setAttribute('x1', e.s.x.toFixed(1)); ln.setAttribute('y1', e.s.y.toFixed(1));
    ln.setAttribute('x2', e.t.x.toFixed(1)); ln.setAttribute('y2', e.t.y.toFixed(1));
  });
  const groups = svg.querySelectorAll('.gnode');
  nodes.forEach((n, i) => {
    const g = groups[i]; if (!g) return;
    const shape = g.firstChild;
    if (shape.tagName === 'circle') {
      shape.setAttribute('cx', n.x.toFixed(1));
      shape.setAttribute('cy', n.y.toFixed(1));
    } else if (shape.tagName === 'rect') {
      shape.setAttribute('x', (n.x - n.r).toFixed(1));
      shape.setAttribute('y', (n.y - n.r).toFixed(1));
    } else if (shape.tagName === 'polygon' && n.type === 'role') {
      // 육각형 재계산
      const pts = [];
      for (let k = 0; k < 6; k++) {
        const a = Math.PI/3*k - Math.PI/6;
        pts.push(`${(n.x + Math.cos(a)*n.r).toFixed(1)},${(n.y + Math.sin(a)*n.r).toFixed(1)}`);
      }
      shape.setAttribute('points', pts.join(' '));
    } else if (shape.tagName === 'polygon') {
      // 다이아몬드
      shape.setAttribute('points',
        `${n.x},${n.y-n.r} ${n.x+n.r},${n.y} ${n.x},${n.y+n.r} ${n.x-n.r},${n.y}`);
    }
    const txt = g.querySelector('text');
    if (txt) { txt.setAttribute('x', n.x.toFixed(1)); txt.setAttribute('y', (n.y + n.r + 12).toFixed(1)); }
  });
}

function _showNodeDetail(node, nodes, edges) {
  const side = document.getElementById('graphSide');
  if (!side) return;
  const incident = edges.filter(e => e.s.id === node.id || e.t.id === node.id);
  const typeKr = {user:'사용자', role:'직무', metric:'지표', scope:'대상'}[node.type] || node.type;
  let neighborHtml = '';
  if (incident.length) {
    const list = incident.map(e => {
      const other = e.s.id === node.id ? e.t : e.s;
      const w = e.weight || 1;
      return `<div class="row"><span class="k">${escapeHtml(other.label)}</span><span>${w}</span></div>`;
    }).join('');
    neighborHtml = `<h4 style="margin-top:14px">연결 (${incident.length})</h4>${list}`;
  }
  side.innerHTML = `
    <h4>${escapeHtml(node.label)}</h4>
    <div class="meta">${typeKr}${node.role ? ' · ' + escapeHtml(node.role) : ''} · 가중치 ${node.weight||0}</div>
    ${neighborHtml}
    <div style="margin-top:14px;font-size:var(--fs-xs);color:var(--dim)">노드 드래그 · 휠 줌</div>`;
}

// ── 프로필 모달 ──
let _PROGRAMS_CACHE = null;  // [{code,label,programs:[{code,label,time}]}]

async function openProfileModal() {
  if (!RAAS_USER) return;
  document.getElementById('profileModal').classList.add('open');
  // 직무 드롭다운 동적 채우기 (현재 role 선택)
  await loadActiveRoles();
  _renderRoleOptions(document.getElementById('pfRole'), false, RAAS_USER.role);
  // 폼에 현재 값 채우기
  document.getElementById('pfLoginId').value = RAAS_USER.login_id || '';
  document.getElementById('pfName').value    = RAAS_USER.name || '';
  // CP 채널 초기값 + 가시성 (D-015 옵션 A)
  const chSel = document.getElementById('pfChannel');
  if (chSel) chSel.value = RAAS_USER.channel || '';
  _pfOnRoleChange();
  document.getElementById('pfMsg').className = 'pf-msg hidden';
  document.getElementById('pwMsg').className = 'pf-msg hidden';
  document.getElementById('pwForm').reset();
  await _loadProgramsForProfile();
  loadInterestMap();   // 내 관심 맵 비동기 로드 (실패해도 다른 섹션 영향 X)
}

// 직무 변경 시 CP 전용 채널 select 가시성 토글 (D-015 옵션 A)
function _pfOnRoleChange() {
  const role = document.getElementById('pfRole')?.value || '';
  const field = document.getElementById('pfChannelField');
  if (!field) return;
  field.style.display = (role === 'CP') ? 'flex' : 'none';
  // CP가 아니면 값도 비움
  if (role !== 'CP') {
    const ch = document.getElementById('pfChannel');
    if (ch) ch.value = '';
  }
}

async function loadInterestMap() {
  const host = document.getElementById('pfInterestMap');
  if (!host) return;
  host.innerHTML = '<div class="im-empty">로드 중…</div>';
  try {
    const res = await _authedFetch('/api/my/interest-map?days=30');
    if (!res.ok) {
      host.innerHTML = '<div class="im-empty">정보를 불러올 수 없습니다.</div>';
      return;
    }
    const json = await res.json();
    if (!json.ok) {
      host.innerHTML = '<div class="im-empty">정보를 불러올 수 없습니다.</div>';
      return;
    }
    host.innerHTML = _renderInterestMap(json.data);
  } catch (e) {
    host.innerHTML = '<div class="im-empty">네트워크 오류</div>';
  }
}

function _renderInterestMap(d) {
  if (!d) return '<div class="im-empty">데이터 없음</div>';
  const total = d.total_queries || 0;

  // 분포 막대 헬퍼
  const _bars = (items, key) => {
    if (!items.length) return '<div class="im-empty">기간 내 데이터 없음</div>';
    const max = Math.max(...items.map(i => i.count || 0), 1);
    return items.slice(0, 6).map(i => `
      <div class="im-bar-row">
        <div class="im-bar-label" title="${escapeHtml(i[key])}">${escapeHtml(i[key])}</div>
        <div class="im-bar-track"><div class="im-bar-fill" style="width:${(i.count/max*100).toFixed(1)}%"></div></div>
        <div class="im-bar-count">${i.count}</div>
      </div>`).join('');
  };

  // 사각지대
  let blindHtml = '';
  if (d.blind_spots && d.blind_spots.length) {
    blindHtml = `
      <div class="im-block" style="grid-column:1 / -1">
        <div class="im-block-title">사각지대 — ${d.role || '직무'} 직무에서 아직 안 보신 영역</div>
        <div class="im-blind-list">
          ${d.blind_spots.map(b => `
            <div class="im-blind-item" title="${escapeHtml(b.reason||'')}">
              <span class="im-blind-icon">💡</span>
              <div class="im-blind-text">${escapeHtml(b.question)}
                ${b.reason ? `<div class="im-blind-reason">${escapeHtml(b.reason)}</div>` : ''}
              </div>
            </div>`).join('')}
        </div>
      </div>`;
  }

  // 동료 평균 대비
  let peerHtml = '';
  if (d.peer_compare) {
    const p = d.peer_compare;
    peerHtml = `
      <div class="im-peer">
        <div style="font-weight:600;margin-bottom:6px">동료 평균 대비 — ${d.role} 직무</div>
        <div class="im-peer-row"><span class="k">같은 직무 동료</span><span>${p.same_role_users}명</span></div>
        <div class="im-peer-row"><span class="k">내 질문수 / 동료 평균</span>
          <span>${p.my_queries} / ${p.peer_avg_queries}</span></div>
        ${p.peer_top_metrics && p.peer_top_metrics.length ? `
          <div class="im-peer-row" style="border-top:1px solid var(--line);margin-top:6px;padding-top:8px">
            <span class="k">동료들이 자주 보는 지표</span>
            <span>${p.peer_top_metrics.map(m => escapeHtml(m.metric)).join(', ')}</span>
          </div>` : ''}
      </div>`;
  }

  // 빈 상태 (질의 0건)
  if (total === 0) {
    return `
      <div class="im-summary">
        <div class="kpi"><div class="label">최근 30일 질의</div><div class="val">0</div></div>
        <div class="kpi"><div class="label">직무</div><div class="val" style="font-size:var(--fs-md);font-family:var(--sans)">${escapeHtml(d.role||'—')}</div></div>
      </div>
      <div class="im-empty">아직 질의 이력이 없습니다. 칩 추천을 클릭하거나 질문을 던지면 분포가 누적됩니다.</div>
      ${blindHtml}
      ${peerHtml}`;
  }

  return `
    <div class="im-summary">
      <div class="kpi"><div class="label">최근 30일 질의</div><div class="val">${total}</div></div>
      <div class="kpi"><div class="label">직무</div><div class="val" style="font-size:var(--fs-md);font-family:var(--sans)">${escapeHtml(d.role||'—')}</div></div>
      <div class="kpi"><div class="label">주요 주제</div><div class="val" style="font-size:var(--fs-sm);font-family:var(--mono)">${(d.top_topics&&d.top_topics[0]&&d.top_topics[0].topic_key)?escapeHtml(d.top_topics[0].topic_key):'—'}</div></div>
    </div>
    <div class="im-grid">
      <div class="im-block">
        <div class="im-block-title">주로 보는 지표</div>
        ${_bars(d.by_metric||[], 'metric')}
      </div>
      <div class="im-block">
        <div class="im-block-title">주로 보는 대상</div>
        ${_bars(d.by_scope||[], 'scope')}
      </div>
      ${blindHtml}
    </div>
    ${peerHtml}`;
}

function closeProfileModal() {
  document.getElementById('profileModal').classList.remove('open');
}

async function _loadProgramsForProfile() {
  const host = document.getElementById('pfPrograms');
  try {
    if (!_PROGRAMS_CACHE) {
      const res = await _authedFetch('/api/programs');
      const data = await res.json();
      if (!data.ok) {
        host.innerHTML = `<div class="pf-empty-progs" style="color:var(--red)">${escapeHtml(data.error||'프로그램 로드 실패')}</div>`;
        return;
      }
      _PROGRAMS_CACHE = data.channels;
    }
    // 단일 선택 — RAAS_USER.my_programs 가 배열이지만 첫 번째 값만 사용
    const myList = RAAS_USER.my_programs || [];
    const current = myList.length ? myList[0] : '';
    const groups = _PROGRAMS_CACHE.map(ch => {
      const opts = ch.programs.map(p => {
        const sel = (p.code === current) ? ' selected' : '';
        const time = p.time ? ` · ${p.time}` : '';
        return `<option value="${escapeHtml(p.code)}"${sel}>${escapeHtml(p.label)}${escapeHtml(time)}</option>`;
      }).join('');
      return `<optgroup label="${escapeHtml(ch.label)}">${opts}</optgroup>`;
    }).join('');
    // 채널 단위 관심 — 리스트 상단에 추가(선택 시 해당 채널 전체가 기본 대상이 됨)
    const _chOpts = [['F00', '파워FM 채널'], ['L00', '러브FM 채널']].map(([c, l]) =>
      `<option value="${c}"${c === current ? ' selected' : ''}>${l}</option>`).join('');
    const channelGroup = `<optgroup label="채널">${_chOpts}</optgroup>`;
    host.innerHTML = `
      <select class="pf-select" id="pfMyProgram">
        <option value=""${current ? '' : ' selected'}>선택 없음</option>
        ${channelGroup}
        ${groups}
      </select>`;
  } catch (e) {
    host.innerHTML = `<div class="pf-empty-progs" style="color:var(--red)">네트워크 오류: ${escapeHtml(e.message)}</div>`;
  }
}

function _collectSelectedPrograms() {
  // 단일 선택 — 값 없으면 빈 배열, 있으면 [code]
  const sel = document.getElementById('pfMyProgram');
  const v = sel ? sel.value : '';
  return v ? [v] : [];
}

async function saveProfile(ev) {
  ev.preventDefault();
  const msg = document.getElementById('pfMsg');
  const roleVal = document.getElementById('pfRole').value;
  const channelVal = document.getElementById('pfChannel')?.value || '';
  // CP 선택 시 채널 필수
  if (roleVal === 'CP' && !channelVal) {
    msg.textContent = '담당 채널(파워FM 또는 러브FM)을 선택해 주세요.';
    msg.className = 'pf-msg';
    return false;
  }
  const payload = {
    name:        document.getElementById('pfName').value.trim(),
    role:        roleVal,
    my_programs: _collectSelectedPrograms(),
    channel:     channelVal,
  };
  try {
    const res = await _authedFetch('/api/me/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      msg.textContent = data.error || '저장 실패';
      msg.className = 'pf-msg';
      return false;
    }
    const prevRole = RAAS_USER && RAAS_USER.role;
    const prevChannel = RAAS_USER && RAAS_USER.channel;
    RAAS_USER = data.user;
    _renderSidebarUser();
    msg.textContent = '저장되었습니다.';
    msg.className = 'pf-msg success';

    // 직무 또는 채널이 바뀌면 웰컴 추천칩을 갱신(웰컴 화면일 때만 반영, 아니면 no-op)
    const newRole = data.user && data.user.role;
    const newChannel = data.user && data.user.channel;
    if (newRole !== prevRole || newChannel !== prevChannel) {
      try { loadSuggestions(); } catch (_) {}
    }
  } catch (e) {
    msg.textContent = '네트워크 오류: ' + e.message;
    msg.className = 'pf-msg';
  }
  return false;
}

async function changePw(ev) {
  ev.preventDefault();
  const msg = document.getElementById('pwMsg');
  const form = ev.target;
  const payload = {
    old_password: form.old_password.value,
    new_password: form.new_password.value,
  };
  try {
    const res = await _authedFetch('/api/me/password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      msg.textContent = data.error || '변경 실패';
      msg.className = 'pf-msg';
      return false;
    }
    msg.textContent = '비밀번호가 변경되었습니다.';
    msg.className = 'pf-msg success';
    form.reset();
  } catch (e) {
    msg.textContent = '네트워크 오류: ' + e.message;
    msg.className = 'pf-msg';
  }
  return false;
}

// BRIEFING_QUERY_TEXT는 이력 재생(카드 임베드) 호환용으로 유지 — 칩에서는 제거됨
const BRIEFING_QUERY_TEXT = '이번 주 브리핑 보여줘';
const QUICK_QUERIES = [
  // '이번 주 브리핑'은 우측 KPI 패널(주간 탭 + ✨ AI 요약)로 이전
  'RAAS에는 어떤 지표들이 있나',
  '어제 MAU는?',
  '이번 주 DAU 추이',
  '파워FM vs 러브FM 주간 신규유입 추이 비교',
  '프로그램별 DAU 순위',
  '지난주 컬투쇼 게스트 일자별로',
  '어제 프로그램별로 생방송/보라 편성정보 알려줘',
  '컬투쇼 코너 편성 알려줘',
];

// ────────────────────────────────────────────────
// QUERY CACHE
// 답변 캐시: 당일 REFRESH_HOUR 이후 생성된 항목만 유효 (Splunk 갱신 기준)
// 질문 보관: 7일 / 답변 보관: 당일 갱신 이후만
// ────────────────────────────────────────────────
const REFRESH_HOUR = 7;

function _todayRefresh() {
  const d = new Date(); d.setHours(REFRESH_HOUR, 0, 0, 0); return d;
}
function _loadQCache() {
  try { return JSON.parse(localStorage.getItem('raas_qcache') || '[]'); } catch { return []; }
}
function _saveQCache(arr) { localStorage.setItem('raas_qcache', JSON.stringify(arr)); }

function _cleanQCache() {
  const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  const refresh = _todayRefresh();
  const arr = _loadQCache()
    .filter(e => new Date(e.cachedAt) >= cutoff)
    .map(e => new Date(e.cachedAt) < refresh ? { ...e, answerHTML: null } : e);
  _saveQCache(arr);
}

// 데이터 버전 체크 — Splunk/CSV 갱신 시 캐시 자동 무효화
async function _checkDataVersion() {
  try {
    const res = await fetch('/api/timeline/meta');
    if (!res.ok) return;
    const meta = await res.json();
    const newVer = meta.date_max;
    if (!newVer) return;
    const storedVer = localStorage.getItem('raas_data_version');
    if (storedVer !== newVer) {
      // 데이터 갱신됨 → 캐시된 답변 HTML 전체 무효화
      const arr = _loadQCache().map(e => ({ ...e, answerHTML: null }));
      _saveQCache(arr);
      localStorage.setItem('raas_data_version', newVer);
    }
  } catch (_) {}
}

function _getCachedAnswer(question) {
  const refresh = _todayRefresh();
  return _loadQCache()
    .filter(e => e.question === question && e.answerHTML && new Date(e.cachedAt) >= refresh)
    .sort((a, b) => new Date(b.cachedAt) - new Date(a.cachedAt))[0]?.answerHTML ?? null;
}

function _saveCachedAnswer(question, answerHTML) {
  let arr = _loadQCache().filter(e => e.question !== question);
  arr.push({ question, answerHTML, cachedAt: new Date().toISOString() });
  if (arr.length > 100) arr = arr.slice(-100);
  _saveQCache(arr);
}

// ────────────────────────────────────────────────
// DATE
// ────────────────────────────────────────────────
(function(){
  const d = new Date();
  const s = `${d.getFullYear()}.${String(d.getMonth()+1).padStart(2,'0')}.${String(d.getDate()).padStart(2,'0')}`;
  document.getElementById('sidebarDate').textContent = s;
})();

// ────────────────────────────────────────────────
// FORMATTERS
// ────────────────────────────────────────────────
function escapeHtml(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}
function renderAiText(s) {
  if (!s) return '';
  // 차트 블록(```chart {json}```) → ECharts 컨테이너. 미완성(스트리밍)은 안내로 대체.
  const charts = [];
  s = s.replace(/```chart\s*\n([\s\S]*?)\n```/g, (m, body) => {
    let spec; try { spec = JSON.parse(body.trim()); } catch (e) { return ''; }
    if (!spec || !Array.isArray(spec.x) || !Array.isArray(spec.series)) return '';
    const id = 'lc' + (_llmChartSeq++);
    _llmChartStore[id] = { spec };
    charts.push(`<div class="llm-chart-box"><div class="llm-chart" id="${id}"></div>
      <div class="llm-chart-actions">
        <button class="dl-btn" onclick="_chartPNG('${id}')">🖼 이미지 저장</button>
        <button class="dl-btn" onclick="_chartShare('${id}')">↗ 공유</button></div></div>`);
    return `@@CHART${charts.length - 1}@@\n`;
  });
  s = s.replace(/```chart[\s\S]*$/g, '📊 차트 생성 중…');
  // 마크다운 표 지원(예: 주간 편성표). 본문은 pre-wrap이라 줄바꿈은 CSS가 처리.
  const tables = [];
  s = s.replace(/(\|[^\n]+\|\n\|[-:|\s]+\|\n(?:\|[^\n]+\|\n?)+)/g, (match) => {
    const rows = match.trim().split('\n');
    const header = rows[0].split('|').slice(1, -1).map(c => c.trim());
    const body = rows.slice(2).map(r => r.split('|').slice(1, -1).map(c => c.trim()));
    const thead = '<tr>' + header.map(c => `<th>${escapeHtml(c)}</th>`).join('') + '</tr>';
    const tbody = body.map(r => '<tr>' + r.map(c =>
      `<td>${escapeHtml(c).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')}</td>`).join('') + '</tr>').join('');
    tables.push(`<div class="story-table-scroll"><table class="story-table">${thead}${tbody}</table></div>`);
    return `@@TBL${tables.length - 1}@@\n`;   // \n로 줄 구조 보존(하드닝 줄단위 처리용; 재삽입 시 \n? 흡수)
  });
  // [small] 참고 블록 → 답변 끝 푸터(관리자 참고 정보, 참고 토글 연동)
  const meta = [];
  s = s.replace(/\[small\]([\s\S]+?)\[\/small\]/g, (_, b) => { meta.push(b); return ''; });
  let out = escapeHtml(s).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>');
  // 렌더러 하드닝 — LLM이 마크다운 기호를 emit해도 화면은 깨끗하게(표·차트는 이미 placeholder).
  //   보수적: '기호+공백' 또는 '기호만 있는 단독 줄'일 때만 변환 → #1순위·"DAU > 90%" 등 오탐 방지.
  out = out
    .replace(/^[ \t]*#{1,6}[ \t]+(.+)$/gm, '<b>$1</b>')   // 제목 #/##/### → 굵게
    .replace(/^[ \t]*([-*_])\1{2,}[ \t]*$/gm, '')          // 단독 구분선 ---/***/___ 제거
    .replace(/^[ \t]*&gt;[ \t]+/gm, '')                    // 줄머리 인용 > 제거(내용은 유지)
    .replace(/\n{3,}/g, '\n\n');                           // 과도한 빈 줄 축소
  out = out.replace(/@@TBL(\d+)@@\n?/g, (_, i) => tables[+i]);
  out = out.replace(/@@CHART(\d+)@@\n?/g, (_, i) => charts[+i]);
  if (meta.length) {
    out = out.replace(/\s+$/, '');
    const inner = meta.map(b =>
      `<div class="story-small-note">${escapeHtml(b.trim()).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>').replace(/\n/g, '<br>')}</div>`
    ).join('');
    out += `<div class="sl-meta-footer">${inner}</div>`;
  }
  return out;
}
function fmtK(v) {
  if (v == null || isNaN(v)) return '—';
  if (Math.abs(v) >= 10000) return (v/10000).toFixed(1) + '만';
  return v >= 1000 ? v.toLocaleString() : String(v);
}
function fmtP(v) { return (v == null || isNaN(v)) ? '—' : v.toFixed(1) + '%'; }
function fmtD(v, suf='%') { return (v == null || isNaN(v)) ? '—' : (v >= 0 ? '+' : '') + v.toFixed(1) + suf; }
function wowClass(v) { return v == null ? '' : v > 0 ? 'rk-pos' : v < 0 ? 'rk-neg' : ''; }
function formatRelTime(iso) {
  const d = new Date((iso.includes('Z')||iso.includes('+')) ? iso : iso+'Z');
  const m = Math.floor((Date.now() - d.getTime()) / 60000);
  if (m < 1) return '방금';
  if (m < 60) return m + '분 전';
  const h = Math.floor(m / 60);
  if (h < 24) return h + '시간 전';
  const days = Math.floor(h / 24);
  return days < 7 ? days + '일 전' : iso.slice(0, 10);
}

// ────────────────────────────────────────────────
// CHART STORE & PERIOD HANDLERS
// ────────────────────────────────────────────────
const _chartStore = {};
const _DAYS_KO = ['일','월','화','수','목','금','토'];
const PERIOD_BTNS_DAILY = [
  {days:7,    label:'7일'},
  {days:30,   label:'1개월'},
  {days:90,   label:'3개월'},
  {days:365,  label:'1년'},
  {days:1825, label:'5년'},
];
// 주간 지표: 단위 = 주(week 수)
const PERIOD_BTNS_WEEKLY = [
  {days:4,   label:'4주'},
  {days:13,  label:'3개월'},
  {days:26,  label:'6개월'},
  {days:52,  label:'1년'},
  {days:260, label:'5년'},
];
// 월간 지표: 단위 = 월(month 수)
const PERIOD_BTNS_MONTHLY = [
  {days:3,  label:'3개월'},
  {days:6,  label:'6개월'},
  {days:12, label:'1년'},
  {days:60, label:'5년'},
];

// ────────────────────────────────────────────────
// SVG CHART RENDERERS
// ────────────────────────────────────────────────
function _chartFs()     { return window.innerWidth < 600 ? '24px' : '15px'; } // SVG 축 라벨·값
function _chartFsMeta() { return window.innerWidth < 600 ? '15px' : '15px'; } // HTML legend·평균
function _chartFsSm()   { return window.innerWidth < 600 ? '15px' : '15px'; } // HTML 보조 텍스트

// ─── ECharts 점진 도입 (D-013, 옵션 2) ─────────────────────────────
// timeseries_multi(다중 라인)부터 ECharts로 교체. 기타 차트는 기존 SVG 유지.
// 네트워크 실패·미지원 시 SVG fallback 자동.

let _ECHARTS_PROMISE = null;
const ECHARTS_CDN = 'https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js';

function _loadECharts() {
  if (_ECHARTS_PROMISE) return _ECHARTS_PROMISE;
  _ECHARTS_PROMISE = new Promise((resolve, reject) => {
    if (window.echarts) return resolve(window.echarts);
    const s = document.createElement('script');
    s.src = ECHARTS_CDN;
    s.async = true;
    s.onload = () => resolve(window.echarts);
    s.onerror = (e) => reject(new Error('ECharts CDN load failed'));
    document.head.appendChild(s);
  });
  return _ECHARTS_PROMISE;
}

// 테마(라이트/다크) → ECharts 색상 매핑
function _eChartsThemeColors() {
  const isDark = (document.documentElement.dataset.theme || 'dark') === 'dark';
  return isDark ? {
    text:       '#e8edf3',
    textSub:    '#8b95a5',
    bg:         '#1a2130',
    line:       'rgba(255,255,255,.07)',
    tooltipBg:  '#13181f',
    tooltipBorder: 'rgba(255,255,255,.12)',
    palette: ['#e74c3c','#3498db','#16a085','#f39c12','#9b59b6','#1abc9c','#e67e22','#ecf0f1'],
  } : {
    text:       '#111827',
    textSub:    '#6b7a90',
    bg:         '#f7f8fa',
    line:       'rgba(0,0,0,.08)',
    tooltipBg:  '#ffffff',
    tooltipBorder: 'rgba(0,0,0,.13)',
    palette: ['#e74c3c','#2563eb','#0e9384','#d97706','#7c3aed','#0891b2','#c2410c','#475569'],
  };
}

// ─── LLM 답변 차트 (```chart 블록 → ECharts) ─────────────────────
let _llmChartSeq = 0;
const _llmChartStore = {};   // id → {spec, chart}

function _llmChartOption(spec, c) {
  const type = (spec.type === 'bar') ? 'bar' : 'line';
  const multi = (spec.series || []).length > 1;
  return {
    grid: { left: 6, right: 16, top: spec.title ? 34 : 14, bottom: 6, containLabel: true },
    title: spec.title ? { text: spec.title, left: 0, textStyle: { color: c.text, fontSize: 13, fontWeight: 600 } } : undefined,
    tooltip: { trigger: 'axis', backgroundColor: c.tooltipBg, borderColor: c.tooltipBorder, textStyle: { color: c.text } },
    legend: multi ? { top: 2, right: 0, textStyle: { color: c.textSub }, itemWidth: 14 } : undefined,
    xAxis: { type: 'category', data: spec.x, boundaryGap: type === 'bar',
             axisLabel: { color: c.textSub, fontSize: 11 }, axisLine: { lineStyle: { color: c.line } } },
    yAxis: { type: 'value', name: spec.unit || '', nameTextStyle: { color: c.textSub },
             scale: (type === 'line'),   // line은 데이터 범위로 자동 스케일(변화 강조), bar는 0부터
             axisLabel: { color: c.textSub, fontSize: 11 }, splitLine: { lineStyle: { color: c.line } } },
    series: (spec.series || []).map((s, i) => ({
      name: s.name, type, data: s.data, smooth: type === 'line',
      itemStyle: { color: c.palette[i % c.palette.length] },
      lineStyle: { width: 2 }, symbol: 'circle', symbolSize: 4,
    })),
  };
}

// 삽입된 답변 영역의 차트 컨테이너를 ECharts로 렌더 (컨테이너가 보일 때 호출)
function _initLLMCharts(root) {
  if (!root) return;
  const els = root.querySelectorAll('.llm-chart');
  if (!els.length) return;
  _loadECharts().then(echarts => {
    const c = _eChartsThemeColors();
    els.forEach(el => {
      if (el.dataset.initialized) return;
      const entry = _llmChartStore[el.id];
      if (!entry || !entry.spec) return;
      el.dataset.initialized = '1';
      try {
        const chart = echarts.init(el, null, { renderer: 'canvas' });
        chart.setOption(_llmChartOption(entry.spec, c));
        entry.chart = chart;
      } catch (e) { el.innerHTML = '<div class="ai-echarts-fallback-note">차트 렌더 실패</div>'; }
    });
  }).catch(() => {});
}

function _chartPNG(id) {
  const e = _llmChartStore[id];
  if (!e || !e.chart) return;
  const bg = (getComputedStyle(document.body).getPropertyValue('--bg1') || '#ffffff').trim();
  const url = e.chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: bg });
  const a = document.createElement('a');
  a.href = url; a.download = ((e.spec.title || 'chart').replace(/[\\/:*?"<>|]/g, '_')) + '.png';
  document.body.appendChild(a); a.click(); a.remove();
}

async function _chartShare(id) {
  const e = _llmChartStore[id];
  if (!e || !e.chart) return;
  const bg = (getComputedStyle(document.body).getPropertyValue('--bg1') || '#ffffff').trim();
  const url = e.chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: bg });
  try {
    const blob = await (await fetch(url)).blob();
    const file = new File([blob], ((e.spec.title || 'chart')) + '.png', { type: 'image/png' });
    if (navigator.canShare && navigator.canShare({ files: [file] })) {
      await navigator.share({ files: [file], title: e.spec.title || 'RAAS 차트' });
    } else {
      _chartPNG(id);
      alert('이 환경은 직접 공유를 지원하지 않아 이미지를 저장했습니다.\n저장된 이미지를 카카오톡에 첨부해 보내세요.');
    }
  } catch (err) { /* 사용자 취소 또는 미지원 */ }
}

// 창 크기 변경 시 답변 차트 리사이즈
window.addEventListener('resize', () => {
  Object.values(_llmChartStore).forEach(e => { try { e.chart && e.chart.resize(); } catch (_) {} });
});

// IntersectionObserver — 컨테이너가 보이면 ECharts 초기화 (lazy)

// ────────────────────────────────────────────────
// CHAT UI
// ────────────────────────────────────────────────
let _msgCount = 0;

function _hideWelcome() {
  const w = document.getElementById('welcomeScreen');
  if (w) w.style.display = 'none';
}

function addUserMsg(text) {
  _hideWelcome();
  const id = 'msg-'+(++_msgCount);
  document.getElementById('threadInner').insertAdjacentHTML('beforeend', `
    <div class="msg msg-user" id="${id}">
      <div class="msg-avatar">나</div>
      <div class="msg-body"><div class="msg-user-text">${escapeHtml(text)}</div></div>
    </div>`);
  // 질문을 화면 최상단으로 (답변 중 자동 추적은 _raas_query_inflight로 차단)
  _anchorMsgId = id;
  // 다음 프레임(=AI 타이핑 placeholder까지 추가된 뒤)에 앵커 적용
  requestAnimationFrame(() => _anchorQuestionTop(id));
  return id;
}

function addAiTyping() {
  const id = 'msg-'+(++_msgCount);
  document.getElementById('threadInner').insertAdjacentHTML('beforeend', `
    <div class="msg msg-ai" id="${id}">
      <div class="msg-avatar">R</div>
      <div class="msg-body">
        <div class="msg-ai-name">RAAS Assistant</div>
        <div class="typing-indicator">
          <div class="typing-dot"></div>
          <div class="typing-dot"></div>
          <div class="typing-dot"></div>
        </div>
      </div>
    </div>`);
  _scrollBottom();
  return id;
}

// IntersectionObserver — 새로 렌더된 .ai-chart-wrap이 50% 가시되면 open_chart 발사 (인스턴스 1회)

function fillAiError(id, errText) {
  const el = document.getElementById(id);
  if (!el) return;
  el.querySelector('.msg-body').innerHTML = `
    <div class="msg-ai-name">RAAS Assistant</div>
    <div class="msg-ai-text" style="color:var(--red);">오류: ${escapeHtml(errText)}</div>`;
}

// 질의 제출 시 질문 메시지를 화면 최상단으로 고정하고, 답변 생성 중에는 마지막 줄을
// 따라가지 않는다(자동 추적 스크롤 off). 사용자는 답변을 처음부터 읽고, 필요할 때만 직접 스크롤.
let _anchorMsgId = null;
let _anchoredScrollTop = 0;
const _ANCHOR_TOP_GAP = 12;        // 상단에 약간의 여백

function _scrollBottom() {
  if (_raas_query_inflight) return; // 답변 생성 중엔 따라 내려가지 않음(질문 상단 고정)
  const t = document.getElementById('chatThread');
  if (t) t.scrollTop = t.scrollHeight;
}

// 방금 제출한 질문 메시지를 뷰포트 최상단으로. 하단 스페이서로 항상 상단까지 오르게 보장.
function _anchorQuestionTop(id) {
  const thread = document.getElementById('chatThread');
  const inner  = document.getElementById('threadInner');
  const el     = document.getElementById(id);
  if (!thread || !inner || !el) return;
  let sp = document.getElementById('threadSpacer');
  if (!sp) { sp = document.createElement('div'); sp.id = 'threadSpacer'; sp.setAttribute('aria-hidden', 'true'); }
  inner.appendChild(sp);                          // 항상 맨 끝
  sp.style.height = thread.clientHeight + 'px';   // 질문이 최상단까지 오를 공간 확보
  const delta = el.getBoundingClientRect().top - thread.getBoundingClientRect().top;
  _anchoredScrollTop = Math.max(0, thread.scrollTop + delta - _ANCHOR_TOP_GAP);
  thread.scrollTop = _anchoredScrollTop;
}

// 답변 완료 후: 실제 답변 길이에 맞춰 하단 여백 정리(과도한 빈 공간 제거). 질문은 상단 유지.
function _finalizeAnchor() {
  const thread = document.getElementById('chatThread');
  const inner  = document.getElementById('threadInner');
  const sp     = document.getElementById('threadSpacer');
  const el     = _anchorMsgId && document.getElementById(_anchorMsgId);
  if (!thread || !inner || !sp || !el) return;
  const atAnchor = Math.abs(thread.scrollTop - _anchoredScrollTop) < 4;  // 사용자가 직접 스크롤했으면 위치 보정 안 함
  const elTop = el.getBoundingClientRect().top - inner.getBoundingClientRect().top;
  const belowH = inner.scrollHeight - sp.offsetHeight - elTop;
  sp.style.height = Math.max(16, thread.clientHeight - belowH) + 'px';
  if (atAnchor) thread.scrollTop = _anchoredScrollTop;
}

// ────────────────────────────────────────────────
// SUBMIT QUERY
// ────────────────────────────────────────────────
let _raas_query_inflight = false;   // 이전 질의 응답이 끝날 때까지 다음 질의 차단

// 송신 버튼 상태 토글 + in-flight 플래그. 일반 질의·스토리라인 칩 답변 공통 사용.
// 답변이 "완전히 끝날 때까지"(스트리밍/타자기 렌더 완료) on 유지.
function _setQueryGenerating(on) {
  _raas_query_inflight = on;
  if (!on) {
    // 답변 완료 → 하단 여백 정리(질문은 상단 유지)
    try { _finalizeAnchor(); } catch (_) {}
  }
  const btn = document.getElementById('btnSend');
  if (!btn) return;
  btn.classList.toggle('is-generating', on);   // 네모(정지)·회색 ↔ 윗화살표
  btn.setAttribute('aria-label', on ? '생성 중' : '전송');
}

// 어제 방송 특이사항 — 스토리라인(단일 경로). 통합 엔진(grounding digest)으로 스트리밍.
function startTodayDigest() {
  submitQuery('어제 방송 특이사항', 'today_digest', { endpoint: '/api/storyline/today' });
}
// 🩺 데이터 확인하기 — 데이터 직무 전용 칩(웰컴 CTA에 주입)
function startDataCheck() {
  submitQuery('데이터 확인하기', 'data_check', { endpoint: '/api/data_check' });
}
function _injectDataCheckChip() {
  if (!(RAAS_USER && (RAAS_USER.role === '데이터' || RAAS_USER.is_admin))) return;
  document.querySelectorAll('.welcome-cta').forEach(cta => {
    if (cta.querySelector('.data-check-btn')) return;
    const b = document.createElement('button');
    b.className = 'today-digest-btn data-check-btn';
    // 보조 CTA — 주 CTA(특이사항)와 위계 분리: 고스트(외곽선) 스타일
    b.style.cssText = 'margin-left:8px;background:transparent;color:var(--purple);' +
                      'border-color:rgba(167,139,250,.55);box-shadow:none';
    b.textContent = '🩺 데이터 확인하기';
    b.addEventListener('click', startDataCheck);
    cta.appendChild(b);
  });
}
// 드릴다운 칩 — 특이 프로그램 → 그 프로그램 원인 자유질의(통합 grounding 경로)
function _buildDrillChips(drill) {
  const chips = (drill || []).map(d =>
    `<button class="sgst-chip" onclick="submitQuery('${escapeHtml(d.name)} 어제 왜 그런지 원인 분석해줘','storyline_drill')">${escapeHtml(d.name)} 들여다보기</button>`
  ).join('');
  if (!chips) return '';
  return `<div class="story-chips-inline"><div class="drill-label">자세히 볼 프로그램</div>${chips}</div>`;
}

async function submitQuery(question, source, opts) {
  opts = opts || {};
  const _endpoint = opts.endpoint || '/api/query/stream';
  question = question.trim();
  if (!question) return;
  // 직전 질의의 답변(스트리밍)이 완전히 끝나기 전에는 새 질의를 막는다.
  // Enter·전송버튼·칩 등 모든 진입 경로에 동일 적용.
  if (_raas_query_inflight) return;
  _setQueryGenerating(true);
  const input = document.getElementById('chatInput');
  input.value = '';
  input.style.height = 'auto';
  addUserMsg(question);
  const aiId = addAiTyping();
  const msgBody = document.getElementById(aiId).querySelector('.msg-body');

  // PostHog ask_ai 이벤트 — source 핵심 (D3 결정 신호)
  // source enum: briefing_card | main_input | sidebar_chip | welcome_chip | history_replay
  trackEvent(EVT.ASK_AI, {
    source: source || 'main_input',
    query_text: question,
    query_length: question.length,
    briefing_open: !!window._raas_briefing_viewed_at,   // 세션 내 브리핑 본 적 있나
    seconds_since_briefing: window._raas_briefing_viewed_at
      ? Math.floor((Date.now() - window._raas_briefing_viewed_at) / 1000)
      : null,
  });

  try {
    const res = await _authedFetch(_endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    let rawText = '', chartData = null, initialized = false, streamDone = false;
    let _renderQueued = false;   // 토큰마다 재파싱하지 않고 프레임당 1회로 합쳐 깜빡임 감소
    const _flushRender = () => {
      _renderQueued = false;
      const el = document.getElementById(`${aiId}-txt`);
      if (el) el.innerHTML = renderAiText(rawText);
      _scrollBottom();
    };
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        let ev;
        try { ev = JSON.parse(line.slice(6)); } catch(e) { continue; }
        if (ev.type === 'meta') {
          chartData = ev.chart_data;
        } else if (ev.type === 'token') {
          if (!initialized) {
            msgBody.innerHTML = `<div class="msg-ai-name">RAAS Assistant</div><div class="msg-ai-text" id="${aiId}-txt"></div>`;
            initialized = true;
          }
          rawText += ev.text;
          if (!_renderQueued) { _renderQueued = true; requestAnimationFrame(_flushRender); }
        } else if (ev.type === 'done') {
          const qid = ev.query_id || null;
          msgBody.innerHTML = `<div class="msg-ai-name">RAAS Assistant</div><div class="msg-ai-text">${renderAiText(rawText)}</div>`;
          _initLLMCharts(msgBody);   // ```chart 블록 → ECharts 렌더
          // 드릴다운(어제 특이사항 → 프로그램별 원인) — 클릭 시 일반 자유질의(통합 grounding 경로)로
          if (ev.drill && ev.drill.length) msgBody.insertAdjacentHTML('beforeend', _buildDrillChips(ev.drill));
          // 원문 텍스트를 캐시(렌더된 HTML 아님) — 복원 시 재렌더로 차트를 다시 그림(canvas는 innerHTML에 안 담겨서).
          _saveCachedAnswer(question, rawText);
          if (qid) msgBody.insertAdjacentHTML('beforeend', _buildFeedbackBar(qid));
          refreshHistory();
          _scrollBottom();
          streamDone = true;
          break;  // inner for loop 탈출 → 아래에서 while도 탈출
        } else if (ev.type === 'error') {
          msgBody.innerHTML = `<div class="msg-ai-name">RAAS Assistant</div><div class="msg-ai-text" style="color:var(--red);">오류: ${escapeHtml(ev.message)}</div>`;
          streamDone = true;
          break;
        }
      }
      if (streamDone) break;  // while loop 탈출
    }
    if (!initialized && !streamDone) fillAiError(aiId, '응답이 없습니다.');
  } catch (err) {
    fillAiError(aiId, err.message);
  } finally {
    _setQueryGenerating(false);
  }
}

// ────────────────────────────────────────────────
// INPUT HANDLING
// ────────────────────────────────────────────────
const chatInput = document.getElementById('chatInput');
chatInput.addEventListener('input', () => {
  chatInput.style.height = 'auto';
  chatInput.style.height = Math.min(chatInput.scrollHeight, 200) + 'px';
});
chatInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) {
    // 한글(IME) 조합 중 Enter는 조합 확정용이므로 전송하지 않음.
    // (조합 확정 시 마지막 글자가 별도 질의로 중복 전송되던 버그 방지)
    if (e.isComposing || e.keyCode === 229) return;
    e.preventDefault();
    submitQuery(chatInput.value, 'main_input');
  }
});
document.getElementById('btnSend').addEventListener('click', () => submitQuery(chatInput.value, 'main_input'));

// ────────────────────────────────────────────────
// SIDEBAR & PANEL TOGGLES
// ────────────────────────────────────────────────
function isMobile() { return window.innerWidth <= 900; }

function openSidebar() {
  const sb = document.getElementById('sidebar');
  const bd = document.getElementById('sidebarBackdrop');
  if (isMobile()) {
    sb.classList.add('mobile-open');
    bd.classList.add('visible');
  } else {
    sb.classList.remove('collapsed');
  }
}
function closeSidebar() {
  const sb = document.getElementById('sidebar');
  const bd = document.getElementById('sidebarBackdrop');
  if (isMobile()) {
    sb.classList.remove('mobile-open');
    bd.classList.remove('visible');
  } else {
    sb.classList.add('collapsed');
  }
}
function toggleSidebar() {
  if (isMobile()) {
    document.getElementById('sidebar').classList.contains('mobile-open') ? closeSidebar() : openSidebar();
  } else {
    document.getElementById('sidebar').classList.contains('collapsed') ? openSidebar() : closeSidebar();
  }
}

document.getElementById('btnToggleSidebar').addEventListener('click', toggleSidebar);

function openKpi() {
  const kp = document.getElementById('kpiPanel');
  const bd = document.getElementById('sidebarBackdrop');
  if (isMobile()) {
    kp.classList.add('mobile-open');
    bd.classList.add('visible');
  } else {
    kp.classList.remove('collapsed');
  }
}
function closeKpi() {
  const kp = document.getElementById('kpiPanel');
  const bd = document.getElementById('sidebarBackdrop');
  if (isMobile()) {
    kp.classList.remove('mobile-open');
    bd.classList.remove('visible');
  } else {
    kp.classList.add('collapsed');
  }
}
function toggleKpi() {
  if (isMobile()) {
    document.getElementById('kpiPanel').classList.contains('mobile-open') ? closeKpi() : openKpi();
  } else {
    document.getElementById('kpiPanel').classList.contains('collapsed') ? openKpi() : closeKpi();
  }
}

document.getElementById('btnToggleKpi').addEventListener('click', toggleKpi);
document.getElementById('btnCloseKpi').addEventListener('click', closeKpi);
document.getElementById('btnCloseKpiMobile').addEventListener('click', closeKpi);

// backdrop: 사이드바 또는 KPI 중 열린 것 닫기
document.getElementById('sidebarBackdrop').addEventListener('click', () => {
  closeSidebar();
  closeKpi();
});

// resize 시 모바일 오버레이 상태 정리
window.addEventListener('resize', () => {
  if (!isMobile()) {
    document.getElementById('sidebarBackdrop').classList.remove('visible');
    document.getElementById('sidebar').classList.remove('mobile-open');
    document.getElementById('kpiPanel').classList.remove('mobile-open');
  }
});
document.getElementById('btnNewChat').addEventListener('click', () => {
  if (isMobile()) closeSidebar();
  document.getElementById('threadInner').innerHTML = `
    <div class="welcome" id="welcomeScreen">
      <h1 class="welcome-logo">안녕하세요 👋</h1>
      <div class="welcome-sub">SBS 고릴라 라디오의 청취 데이터를 자연어로 질의하세요. 지표, 트렌드, 프로그램 분석을 AI가 답합니다.</div>
        <div class="welcome-cta"><button class="today-digest-btn" onclick="startTodayDigest()">🗓 어제 방송 특이사항 보기</button></div>
      <div class="welcome-chips" id="suggestChips"></div>
    </div>`;
  _msgCount = 0;
  loadSuggestions();
});

// ────────────────────────────────────────────────
// QUICK QUERIES & WELCOME CHIPS (직무 기반 추천 칩)
// ────────────────────────────────────────────────
function _initWelcomeChips() { /* 보팅 시점엔 RAAS_TOKEN 가능 — _bootAuth 직후 loadSuggestions가 별도 호출 */ }

// 웰컴 추천 칩 — 스토리라인은 '어제 방송 특이사항' 단일 경로(웰컴 CTA 상시 노출)로 전환.
//   구 CP 다중슬롯 스토리라인은 제거됨. 웰컴은 직무 기반 추천칩만 렌더.
async function loadSuggestions() {
  _injectDataCheckChip();
  await _loadLegacySuggestions();
}

async function _loadLegacySuggestions() {
  const host = document.getElementById('suggestChips');
  if (!host) return;
  try {
    const res = await _authedFetch('/api/suggestions');
    if (!res.ok) { host.innerHTML = ''; return; }
    const data = await res.json();
    if (!data.ok || !Array.isArray(data.chips) || data.chips.length === 0) {
      host.innerHTML = ''; return;
    }
    host.innerHTML = data.chips.map(c => _renderChipHTML(c)).join('');
  } catch (_) {
    host.innerHTML = '';
  }
}

function _renderChipHTML(c) {
  const src = c.source || 'profile';
  // 소스별 클래스·아이콘
  let cls = 'sgst-chip';
  let icon = '';
  if (src === 'anomaly') {
    cls += ' anomaly' + (c.level === 'yellow' ? ' yellow' : '');
    icon = c.level === 'red'
      ? `<span class="sgst-chip-icon">🔴</span>`
      : `<span class="sgst-chip-icon">🟡</span>`;
  } else if (src === 'gap' || src === 'gap-metric') {
    cls += ' gap';
    icon = `<span class="sgst-chip-icon" aria-label="추천">💡</span>`;
  } else if (src === 'peer') {
    cls += ' peer';
    icon = `<span class="sgst-chip-icon" aria-label="동료">👥</span>`;
  } else if (src === 'similar') {
    cls += ' similar';
    icon = `<span class="sgst-chip-icon" aria-label="유사">✦</span>`;
  } else {
    icon = `<svg class="sgst-chip-icon" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/></svg>`;
  }
  const safe = escapeHtml(c.question || '');
  const tip  = c.reason ? ` title="${escapeHtml(c.reason)}"` : '';
  return `<button class="${cls}" onclick="_onSuggestChip(this)" data-q="${safe}"${tip}>${icon}<span class="sgst-chip-text">${safe}</span></button>`;
}

function _onSuggestChip(btn) {
  const q = btn.dataset.q;
  if (q) submitQuery(q, 'welcome_chip');
}

// ── 이번 주 브리핑 카드 (Week 1 Day 2 스켈레톤) ─────────────
// 측정 인프라 활성화용 — 실제 콘텐츠는 Week 3에 채움.
// 세션 내 view_briefing 1회만, ask_ai의 briefing_open·seconds_since_briefing 컨텍스트 제공.
let _RAAS_BRIEFING_FIRED = false;

function _getBriefingMonday() {
  // 이번 주 월요일을 YYYY-MM-DD로 — briefing_date 속성
  const d = new Date();
  const day = d.getDay();                     // 0=일
  const diff = day === 0 ? 6 : day - 1;
  d.setDate(d.getDate() - diff);
  return d.toISOString().slice(0, 10);
}

function _isFirstVisitToday() {
  // 오늘 첫 진입인지 (localStorage로 추적, 첫 호출 시 기록)
  const today = new Date().toISOString().slice(0, 10);
  const last = localStorage.getItem('raas_last_visit_date');
  if (last === today) return false;
  localStorage.setItem('raas_last_visit_date', today);
  return true;
}

// ── 수치 포맷 헬퍼 ────────────────────────────────────────
function _fmtN(n) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  return Math.round(n).toLocaleString();
}
function _fmtPctVal(n, digits = 1) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  return n.toFixed(digits) + '%';
}
function _fmtDelta(n, opts = {}) {
  // opts.unit: '%' or '%p' or '' (default %)
  // opts.up_is_good: 양수가 좋은 신호인지 (default true)
  if (n === null || n === undefined || isNaN(n)) {
    return { html: '<span class="brief-stat-sub-dim">—</span>', val: null };
  }
  const unit = opts.unit ?? '%';
  const upGood = opts.up_is_good !== false;
  const sign  = n > 0 ? '+' : '';
  const arrow = n > 0 ? '▲' : (n < 0 ? '▼' : '·');
  let colorCls = '';
  if (n > 0) colorCls = upGood ? 'brief-stat-up' : 'brief-stat-down';
  else if (n < 0) colorCls = upGood ? 'brief-stat-down' : 'brief-stat-up';
  return {
    html: `<span class="${colorCls}">${sign}${n.toFixed(1)}${unit} ${arrow}</span>`,
    val: n,
  };
}

// ── 브리핑 수치 fetch ──────────────────────────────────────
async function _fetchBriefingNumbers() {
  try {
    const res = await _authedFetch('/api/briefing');
    if (!res.ok) return null;
    const j = await res.json();
    if (!j.ok || !j.data) return null;
    const s1 = j.data.s1_executive || {};
    const s2 = j.data.s2_funnel     || {};
    const s3 = j.data.s3_engagement || {};
    return {
      wau:           s1.wau,
      wau_wow:       s1.wau_chg,            // % vs 전주
      deep_week:     s3.deep_rate_week,     // % 주간 평균
      new_week:      s2.new_week,           // 명 주간 누적
      new_week_wow:  s2.new_week_wow,       // % vs 전주
      date_max:      s1.date || j.data.collected_at,
    };
  } catch (_) { return null; }
}

function _renderBriefingCard(containerEl) {
  if (typeof containerEl === 'string') containerEl = document.getElementById(containerEl);
  if (!containerEl) return null;
  const monday = _getBriefingMonday();
  const uid = Math.random().toString(36).slice(2, 8);
  // 인스턴스 ID로 다중 호출 허용 (사이드바 클릭마다 새 카드 — 챗 메시지 안에 임베드)
  const html = `
    <div class="brief-card" id="bc_${uid}"
         data-card-type="weekly_review"
         data-date="${monday}"
         data-completeness="0.0">
      <div class="brief-card-hd">
        <span class="brief-card-icon">📊</span>
        <div class="brief-card-title">이번 주 브리핑</div>
        <div class="brief-card-date" data-role="date">${monday} (월)</div>
      </div>
      <div class="brief-card-body">
        <div class="brief-grid">
          <div class="brief-stat">
            <div class="brief-stat-label">주간 활성 사용자 (WAU)</div>
            <div class="brief-stat-value" data-stat="wau">…</div>
            <div class="brief-stat-sub" data-stat="wau-sub">최근 7일 · 전주 대비 —</div>
          </div>
          <div class="brief-stat">
            <div class="brief-stat-label">깊은청취율</div>
            <div class="brief-stat-value" data-stat="deep">…</div>
            <div class="brief-stat-sub" data-stat="deep-sub">주간 평균</div>
          </div>
          <div class="brief-stat">
            <div class="brief-stat-label">신규 사용자</div>
            <div class="brief-stat-value" data-stat="new">…</div>
            <div class="brief-stat-sub" data-stat="new-sub">주간 누적 · 전주 대비 —</div>
          </div>
        </div>
      </div>
      <div class="brief-card-actions">
        <button class="brief-action-btn primary" onclick="_briefAsk()">💬 이 데이터 물어보기</button>
        <button class="brief-action-btn" onclick="_briefShare(this)">📤 링크 복사</button>
        <button class="brief-action-btn" onclick="_briefSavePng(this)">📷 이미지 저장</button>
      </div>
    </div>`;
  containerEl.insertAdjacentHTML('beforeend', html);
  const card = containerEl.querySelector(`#bc_${uid}`);
  // 데이터 fetch + 채우기 (비동기) — 도착 후 Observer 부착
  _fillBriefingCard(card);
  return card;
}

async function _fillBriefingCard(cardEl) {
  if (!cardEl) return;
  const $ = sel => cardEl.querySelector(sel);
  const n = await _fetchBriefingNumbers();
  if (!n) {
    // 데이터 가져오지 못함 — placeholder를 "—"로 정리하고 Observer만 부착
    ['wau','deep','new'].forEach(k => {
      const el = $(`[data-stat="${k}"]`); if (el) el.textContent = '—';
    });
    _setupBriefingObserver(cardEl);
    return;
  }
  // 값 채우기
  $(`[data-stat="wau"]`).textContent  = _fmtN(n.wau);
  $(`[data-stat="deep"]`).textContent = _fmtPctVal(n.deep_week);
  $(`[data-stat="new"]`).textContent  = _fmtN(n.new_week);
  // 변화율
  const wauD = _fmtDelta(n.wau_wow,      { unit: '%', up_is_good: true });
  const newD = _fmtDelta(n.new_week_wow, { unit: '%', up_is_good: true });
  $(`[data-stat="wau-sub"]`).innerHTML = `최근 7일 · 전주 대비 ${wauD.html}`;
  $(`[data-stat="new-sub"]`).innerHTML = `주간 누적 · 전주 대비 ${newD.html}`;
  // 기준 날짜
  if (n.date_max) {
    $(`[data-role="date"]`).textContent = `${n.date_max.replace(/\//g,'-')} 기준`;
  }
  // 데이터 완전성 — 주요 3개 값 기준
  const have = [n.wau, n.deep_week, n.new_week].filter(v => v !== null && v !== undefined).length;
  cardEl.dataset.completeness = (have / 3).toFixed(2);
  // view_briefing 발사 — 정확한 completeness로
  _setupBriefingObserver(cardEl);
}

function _setupBriefingObserver(cardEl) {
  // 페이지 세션당 1회만 view_briefing 발사 (스펙: 한 세션 30분 안 1회만)
  if (_RAAS_BRIEFING_FIRED || !cardEl) return;
  if (!('IntersectionObserver' in window)) return;
  const observer = new IntersectionObserver((entries, obs) => {
    entries.forEach(e => {
      if (e.isIntersecting && e.intersectionRatio >= 0.5 && !_RAAS_BRIEFING_FIRED) {
        _RAAS_BRIEFING_FIRED = true;
        window._raas_briefing_viewed_at = Date.now();
        trackEvent(EVT.VIEW_BRIEFING, {
          card_type: cardEl.dataset.cardType,
          briefing_date: cardEl.dataset.date,
          chart_count: cardEl.querySelectorAll('.brief-stat').length,
          data_completeness: parseFloat(cardEl.dataset.completeness),
          is_first_visit_today: _isFirstVisitToday(),
        });
        obs.disconnect();
      }
    });
  }, { threshold: 0.5 });
  observer.observe(cardEl);
}

function _briefAsk() {
  // 브리핑 카드에서 챗으로 진입 — D3 결정 신호의 핵심
  submitQuery('이번 주 브리핑 요약해줘', 'briefing_card');
}

function _briefShare(btn) {
  const card = document.getElementById('briefCard');
  const url = location.href;
  try { navigator.clipboard.writeText(url); } catch (_) {}
  // 시각 피드백
  if (btn) {
    const orig = btn.innerHTML;
    btn.innerHTML = '✅ 복사됨';
    btn.classList.add('ok');
    setTimeout(() => { btn.innerHTML = orig; btn.classList.remove('ok'); }, 1500);
  }
  trackEvent(EVT.SHARE_VIEW, {
    share_type: 'link',
    target:     'briefing_card',
    chart_type: null,
    briefing_date: card ? card.dataset.date : _getBriefingMonday(),
  });
}

// html-to-image lazy CDN 로더 — 첫 PNG 저장 클릭 시에만 로드
let _RAAS_HTI_LOADING = null;
function _loadHtmlToImage() {
  if (window.htmlToImage) return Promise.resolve(window.htmlToImage);
  if (_RAAS_HTI_LOADING) return _RAAS_HTI_LOADING;
  _RAAS_HTI_LOADING = new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/html-to-image@1.11.13/dist/html-to-image.min.js';
    s.crossOrigin = 'anonymous';
    s.onload  = () => resolve(window.htmlToImage);
    s.onerror = () => reject(new Error('html-to-image 로드 실패'));
    document.head.appendChild(s);
  });
  return _RAAS_HTI_LOADING;
}

async function _briefSavePng(btn) {
  const card = document.getElementById('briefCard');
  if (!card) return;
  const orig = btn ? btn.innerHTML : null;
  if (btn) { btn.disabled = true; btn.innerHTML = '⏳ 준비 중…'; }
  try {
    const hti = await _loadHtmlToImage();
    // 캡처 시 배경 토큰 사용 (다크/라이트 모두 정확)
    const bg = getComputedStyle(document.documentElement).getPropertyValue('--color-bg-elevated-2').trim() || '#1a2130';
    const dataUrl = await hti.toPng(card, {
      backgroundColor: bg,
      pixelRatio: 2,    // Retina 대응
      cacheBust: true,
    });
    // 다운로드 트리거
    const a = document.createElement('a');
    const date = card.dataset.date || _getBriefingMonday();
    a.download = `raas-briefing-${date}.png`;
    a.href = dataUrl;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    // 시각 피드백
    if (btn) {
      btn.innerHTML = '✅ 저장됨';
      btn.classList.add('ok');
      setTimeout(() => { btn.innerHTML = orig; btn.classList.remove('ok'); btn.disabled = false; }, 1500);
    }
    // PostHog share_view 발사
    trackEvent(EVT.SHARE_VIEW, {
      share_type: 'png',
      target:     'briefing_card',
      chart_type: null,
      briefing_date: date,
    });
  } catch (e) {
    console.warn('[briefSavePng]', e);
    if (btn) {
      btn.innerHTML = '❌ 실패';
      setTimeout(() => { btn.innerHTML = orig; btn.disabled = false; }, 1500);
    }
  }
}

// 직무 드롭다운 동적 렌더 (가입 + 프로필) — /api/profiles에서 active=true만 로드
let _ROLES_CACHE = null;
async function loadActiveRoles() {
  if (_ROLES_CACHE) return _ROLES_CACHE;
  try {
    const res = await fetch('/api/profiles');
    if (!res.ok) throw new Error('profiles HTTP ' + res.status);
    const data = await res.json();
    if (!data.ok || !Array.isArray(data.profiles)) throw new Error('bad shape');
    _ROLES_CACHE = data.profiles;
  } catch (e) {
    console.warn('[profiles] load failed, using static fallback:', e.message);
    // graceful 폴백 — ALLOWED_ROLES와 동일 (8종)
    _ROLES_CACHE = ['제작','편성','서비스운영','CP','플랫폼전략',
                    '데이터','총괄관리','마케팅(광고·협찬)']
      .map(r => ({ role: r }));
  }
  return _ROLES_CACHE;
}

function _renderRoleOptions(selectEl, withEmpty, selected) {
  if (!selectEl || !_ROLES_CACHE) return;
  const opts = [];
  if (withEmpty) opts.push('<option value="">선택…</option>');
  for (const p of _ROLES_CACHE) {
    const sel = (selected && p.role === selected) ? ' selected' : '';
    opts.push(`<option${sel}>${escapeHtml(p.role)}</option>`);
  }
  selectEl.innerHTML = opts.join('');
}

async function _populateRoleDropdowns() {
  await loadActiveRoles();
  _renderRoleOptions(document.getElementById('authRegRole'), true);
  // 프로필 모달은 openProfileModal에서 매번 새로 채움
}

function _initSidebarQueries() {
  const el = document.getElementById('quickQueries');
  if (!el) return;
  el.innerHTML = QUICK_QUERIES.map((q, i) =>
    `<button class="quick-query-btn" onclick="_onQuickQuery(${i})">
      <svg width="11" height="11" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24" style="flex-shrink:0"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
      <span class="qq-text">${escapeHtml(q)}</span></button>`
  ).join('');
}

function _onQuickQuery(i) {
  if (isMobile()) closeSidebar();
  const q = QUICK_QUERIES[i];
  // 특수 분기: 브리핑 질의는 Claude 호출 없이 카드를 직접 챗에 임베드
  if (q === BRIEFING_QUERY_TEXT) {
    _showBriefingInChat();
    return;
  }
  submitQuery(q, 'sidebar_chip');
}

function _showBriefingInChat() {
  addUserMsg(BRIEFING_QUERY_TEXT);
  const id = 'msg-' + (++_msgCount);
  document.getElementById('threadInner').insertAdjacentHTML('beforeend', `
    <div class="msg msg-ai" id="${id}">
      <div class="msg-avatar">R</div>
      <div class="msg-body">
        <div class="msg-ai-name">RAAS Assistant</div>
        <div class="brief-host" data-role="brief-host"></div>
      </div>
    </div>`);
  const host = document.querySelector(`#${id} [data-role="brief-host"]`);
  _renderBriefingCard(host);
  _scrollBottom();
}

// ────────────────────────────────────────────────
// HISTORY
// ────────────────────────────────────────────────
let _histItems = [];

function restoreFromCache(question, answerHTML) {
  const ti = document.getElementById('threadInner');
  ti.innerHTML = '';
  _msgCount = 0;
  ti.insertAdjacentHTML('beforeend', `
    <div class="msg msg-user" id="msg-${++_msgCount}">
      <div class="msg-avatar">나</div>
      <div class="msg-body"><div class="msg-user-text">${escapeHtml(question)}</div></div>
    </div>`);
  // cached가 원문 텍스트면 재렌더(차트 재생성), 레거시 렌더HTML이면 그대로 삽입.
  const isLegacyHTML = /<div class="msg-ai-(name|text)"/.test(answerHTML || '');
  const bodyHTML = isLegacyHTML
    ? answerHTML
    : `<div class="msg-ai-name">RAAS Assistant</div><div class="msg-ai-text">${renderAiText(answerHTML || '')}</div>`;
  ti.insertAdjacentHTML('beforeend', `
    <div class="msg msg-ai" id="msg-${++_msgCount}">
      <div class="msg-avatar">R</div>
      <div class="msg-body">${bodyHTML}</div>
    </div>`);
  if (!isLegacyHTML) {
    const mb = ti.querySelector('.msg-ai:last-child .msg-body');
    if (mb) _initLLMCharts(mb);   // ```chart → ECharts 재렌더(리로드에도 안전)
  }
  _scrollBottom();
}

function _onHistoryClick(i) {
  const q = _histItems[i]?.question;
  if (!q) return;
  if (isMobile()) closeSidebar();
  const cached = _getCachedAnswer(q);
  if (cached) restoreFromCache(q, cached);
  else submitQuery(q, 'history_replay');
}

async function refreshHistory() {
  const list = document.getElementById('historyList');
  if (!RAAS_TOKEN) {
    list.innerHTML = '<div class="history-empty">로그인 후 이용 가능</div>';
    return;
  }
  try {
    const res = await _authedFetch('/api/query/history?limit=50');
    const data = await res.json();
    if (!data.items?.length) {
      list.innerHTML = '<div class="history-empty">아직 질의 내역이 없습니다.</div>';
      return;
    }
    // 중복 질문 제거 — 서버는 최신순 반환이므로 처음 등장한 것(=가장 최근)만 유지
    const seen = new Set();
    const deduped = data.items.filter(it => {
      if (seen.has(it.question)) return false;
      seen.add(it.question);
      return true;
    });
    _histItems = deduped;
    list.innerHTML = deduped.map((it, i) => {
      const q = it.question;
      const display = q.length > 46 ? q.slice(0, 46) + '…' : q;
      const hasCached = !!_getCachedAnswer(q);
      const isStory = it.source === 'storyline';
      const dot = hasCached
        ? `<span class="hist-cached${isStory ? ' story' : ''}" title="${isStory ? '스토리라인 질의 (캐시됨)' : '캐시된 답변'}"></span>`
        : '';
      return `<div class="history-item" onclick="_onHistoryClick(${i})">
        ${dot}<span class="history-item-q">${escapeHtml(display)}</span>
        <span class="hist-time">${formatRelTime(it.created_at)}</span>
      </div>`;
    }).join('');
  } catch (_) {
    list.innerHTML = '<div class="history-empty">히스토리 로드 실패</div>';
  }
}

// 최근 분석 여정 — 세션 단위(최근 7일). 카드 클릭 → 여정 펼침.
function _buildFeedbackBar(queryId) {
  return `<div class="feedback-bar" id="fb-${queryId}">
    <button class="fb-btn fb-good" onclick="sendFeedback(${queryId},1,this)">👍 도움됨</button>
    <button class="fb-btn fb-bad"  onclick="sendFeedback(${queryId},-1,this)">👎 아쉬움</button>
  </div>`;
}

async function sendFeedback(queryId, val, btn) {
  const bar = document.getElementById('fb-' + queryId);
  if (!bar) return;
  try {
    const res = await fetch('/api/query/feedback', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: queryId, feedback: val }),
    });
    const data = await res.json();
    if (data.ok) {
      bar.querySelectorAll('.fb-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      if (val === -1) _showReasonInput(queryId, bar);   // 👎 → 인라인 사유 입력(선택)
    }
  } catch (_) {}
}
// 👎 아쉬움 사유 — 누른 자리에서 인라인으로(모달 아님), 선택 입력
function _showReasonInput(queryId, bar) {
  bar.innerHTML = `<span class="fb-active">👎 아쉬움</span>
    <input class="fb-reason" id="fbr-${queryId}" maxlength="200" placeholder="무엇이 아쉬웠나요? (선택)"
      onkeydown="if(event.key==='Enter'){event.preventDefault();_sendReason(${queryId});}">
    <button class="fb-btn" onclick="_sendReason(${queryId})">보내기</button>
    <button class="fb-btn" onclick="_thankReason(${queryId},'')">건너뛰기</button>`;
  const inp = document.getElementById('fbr-' + queryId);
  if (inp) inp.focus();
}
async function _sendReason(queryId) {
  const inp = document.getElementById('fbr-' + queryId);
  const reason = inp ? inp.value.trim() : '';
  try {
    await fetch('/api/query/feedback', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: queryId, feedback: -1, reason }) });
  } catch (_) {}
  _thankReason(queryId, reason);
}
function _thankReason(queryId, reason) {
  const bar = document.getElementById('fb-' + queryId);
  if (bar) bar.innerHTML = `<span class="fb-active">👎 기록됨${reason ? ' · 감사합니다' : ''}</span>`;
}

// ────────────────────────────────────────────────
// HISTORY MODAL
// ────────────────────────────────────────────────
let _histDays = 0, _histOffset = 0, _histTotal = 0;
let _histMode = 'all';   // 'all' | 'journey'(분석 여정) | 'query'(일반 질의)
const HIST_PAGE_SIZE = 20;
let _histAllItems = [];

function openHistModal() {
  _histOffset = 0;
  document.getElementById('histSearch').value = '';
  document.getElementById('histModal').classList.add('open');
  loadHistAll();
}

function closeHistModal() {
  document.getElementById('histModal').classList.remove('open');
}

function setHistDays(days, btn) {
  _histDays = days;
  _histOffset = 0;
  document.querySelectorAll('.hf-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadHistAll();
}

function histPage(dir) {
  _histOffset = Math.max(0, _histOffset + dir * HIST_PAGE_SIZE);
  loadHistAll();
}

function _histFmt(iso) {
  const d = new Date((iso.includes('Z') || iso.includes('+')) ? iso : iso + 'Z');
  const now = new Date();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  const isToday = d.toDateString() === now.toDateString();
  return isToday ? hh + ':' + mi : mm + '-' + dd + ' ' + hh + ':' + mi;
}

function _fbIcon(fb) {
  if (fb === 1)  return '<span class="hm-fb-icon" title="도움됨">👍</span>';
  if (fb === -1) return '<span class="hm-fb-icon" title="아쉬움">👎</span>';
  return '<span class="hm-fb-icon" style="color:var(--dim)" title="미평가">—</span>';
}

function setHistMode(mode, btn) {
  _histMode = mode;
  _histOffset = 0;
  if (btn) {
    btn.parentElement.querySelectorAll('.hf-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
  }
  loadHistAll();
}

async function loadHistAll() {
  const body = document.getElementById('histModalBody');
  body.innerHTML = '<div class="hist-empty-msg">로드 중...</div>';
  if (_histMode === 'improve') { _loadMyImprovements(); return; }
  if (_histMode === 'review')  { _loadReviewQueue(); return; }
  try {
    const params = new URLSearchParams({ limit: HIST_PAGE_SIZE, offset: _histOffset, days: _histDays });
    const res = await fetch('/api/query/history/all?' + params);
    const data = await res.json();
    if (!data.ok) throw new Error(data.error);
    _histTotal = data.total;
    const queryItems = data.items || [];
    _histAllItems = queryItems;       // 검색은 질의 텍스트 대상
    _renderHistUnified(queryItems);
    _updateHistFooter();
  } catch (e) {
    body.innerHTML = `<div class="hist-empty-msg">로드 실패: ${escapeHtml(e.message)}</div>`;
  }
}

function _filterHistRows() {
  const kw = document.getElementById('histSearch').value.trim().toLowerCase();
  if (!kw) { loadHistAll(); return; }
  _renderHistUnified(_histAllItems.filter(it => it.question.toLowerCase().includes(kw)));
}

// 분석 여정(세션) + 일반 질의를 시간순 통합 렌더
function _renderHistUnified(queryItems) {
  const body = document.getElementById('histModalBody');
  window._histIndex = {};
  (queryItems || []).forEach(it => { if (it.id != null) window._histIndex[it.id] = it; });
  const rows = (queryItems || []).map(it => ({ t: it.created_at, html: _histQueryRow(it) }));
  if (!rows.length) { body.innerHTML = '<div class="hist-empty-msg">내역이 없습니다.</div>'; return; }
  rows.sort((a, b) => new Date(b.t) - new Date(a.t));
  body.innerHTML = rows.map(r => r.html).join('');
}

// 분석 여정 = 세션 1행. 제목 = 첫 진입 칩 이름. 펼치면 이동 경로(+단계별 토큰).
function _histQueryRow(it) {
  const time    = _histFmt(it.created_at);
  const user    = (it.user_name || '').length > 18 ? (it.user_name || '').slice(0, 16) + '…' : (it.user_name || '—');
  const qShort  = it.question.length > 60 ? it.question.slice(0, 58) + '…' : it.question;
  const fbStatus = it.feedback === 1 ? '👍 도움됨' : it.feedback === -1 ? '👎 아쉬움' : '미평가';
  return `<div class="hm-row" onclick="_toggleHmRow(this)">
      <div class="hm-summary">
        <span class="hm-time">${escapeHtml(time)}</span>
        <span class="hm-user">${escapeHtml(user)}</span>
        <span class="hm-q">${escapeHtml(qShort)}</span>
        ${_fbIcon(it.feedback)}
      </div>
      <div class="hm-detail">
        <div class="hm-detail-q">Q. ${escapeHtml(it.question)}</div>
        <div class="hm-answer">${renderAiText(it.answer || '')}</div>
        <div class="hm-actions">
          <span class="hm-fb-status">${escapeHtml(fbStatus)}</span>
          ${(it.input_tokens != null) ? `<span class="hm-fb-status" style="margin-left:8px">↑${it.input_tokens.toLocaleString()} ↓${(it.output_tokens||0).toLocaleString()} tok</span>` : ''}
          ${it.feedback === -1 ? `<button class="imp-btn" onclick="event.stopPropagation();_openImprove(${it.id})">✦ 개선하기</button>` : ''}
        </div>
      </div>
    </div>`;
}

function _toggleHmRow(row) {
  const det = row.querySelector('.hm-detail');
  det.classList.toggle('open');
  if (det.classList.contains('open')) _initLLMCharts(det);   // 펼칠 때 차트 렌더(숨김 상태 init 회피)
}

// ── 지식 개선 루프 (개선하기) ──
let _improveState = null;
function _openImprove(id) {
  const it = (window._histIndex || {})[id];
  if (!it) return;
  _openImproveWith(it.question, it.answer || '', id, it.feedback_reason || '');
}
// 약점 신호(미개선 👎 질의)에서 직접 개선 착수 — 사유 pre-fill
function _openImproveFromNeg(id) {
  const q = (window._reviewNegIndex || {})[id];
  if (q) _openImproveWith(q.question, q.answer || '', q.id, q.feedback_reason || '');
}
// 직접 진입 — 질의·답변을 직접 받아 개선 모달 오픈
function _openImproveWith(question, answer, qid, reason) {
  _improveState = { question: question, answer: answer || '', qid: qid, reason: reason || '' };
  document.getElementById('improveModal').classList.add('open');
  document.getElementById('improveQ').textContent = question;
  document.getElementById('improveOrig').innerHTML = renderAiText(answer || '');
  const ri = document.getElementById('improveReason');
  if (ri) ri.value = reason || '';   // 👎 인라인에서 남긴 사유 pre-fill
  _updateReasonBtn();
  const res = document.getElementById('improveResult');
  res.style.display = 'none'; res.innerHTML = '';
  _loadImproveContext();
}
// 사유 버튼 라벨 — 저장된 사유가 있고 (포커스 또는 값 변경) 이면 '수정하기', 아니면 '사유 저장'
function _updateReasonBtn() {
  const inp = document.getElementById('improveReason');
  const btn = document.getElementById('improveReasonBtn');
  if (!inp || !btn) return;
  const saved = (_improveState && _improveState.reason) || '';
  const editing = (document.activeElement === inp) || (inp.value !== saved);
  btn.textContent = (saved && editing) ? '수정하기' : '사유 저장';
}
async function _saveImproveReason() {
  const qid = _improveState && _improveState.qid;
  if (!qid) { alert('대상 질의를 찾을 수 없습니다.'); return; }
  const reason = (document.getElementById('improveReason').value || '').trim();
  try {
    const r = await fetch('/api/query/feedback', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: qid, feedback: -1, reason }) });
    const d = await r.json();
    if (d.ok) {
      _improveState.reason = reason;
      const inp = document.getElementById('improveReason');
      if (inp) inp.blur();
      _updateReasonBtn();
      alert('아쉬운 점 저장됨 — 약점 신호에 함께 표시됩니다.');
    } else throw new Error(d.error || '실패');
  } catch (e) { alert('실패: ' + e.message); }
}
function _closeImprove() { document.getElementById('improveModal').classList.remove('open'); }

async function _loadImproveContext() {
  const dataEl = document.getElementById('improveData');
  const ontoEl = document.getElementById('improveOnto');
  dataEl.innerHTML = ontoEl.innerHTML = '<div class="imp-mean">로딩…</div>';
  try {
    const r = await _authedFetch('/api/improve/context', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: _improveState.question }) });
    const d = await r.json();
    if (!d.ok) { dataEl.innerHTML = ontoEl.innerHTML = '<div class="imp-mean">프로그램 미식별 — 개선 대상 아님</div>'; return; }
    _improveState.program = d.program;
    _improveState.scopeKind = d.scope_kind || 'program';
    dataEl.innerHTML =
      '<div class="imp-sub">사용된 데이터 필드</div>' +
      (d.used_fields || []).map(f => `<div class="imp-item"><b>${escapeHtml(f.label || f.field)}</b> <code>${escapeHtml(f.field)}</code><div class="imp-mean">${escapeHtml(f.meaning || '')}</div>${f.source ? `<div class="imp-src">📄 ${escapeHtml(f.source)}</div>` : ''}</div>`).join('') +
      `<div class="imp-form"><div class="imp-sub">스플렁크 필드 추가 요청 <span class="imp-tag">요청형 · 관리자 처리 후 반영</span></div>
        <input id="impReqField" placeholder="필드명 (예: gender_segment)">
        <textarea id="impReqDesc" placeholder="왜 필요한지 설명"></textarea>
        <button class="imp-btn" onclick="_submitDataRequest()">요청 등록</button></div>`;
    ontoEl.innerHTML =
      '<div class="imp-sub">참고한 온톨로지</div>' +
      (d.ontology_items || []).map(o => `<div class="imp-item"><b>${escapeHtml(o.label)}</b><div class="imp-mean">${escapeHtml(o.purpose || '')}</div>${o.source ? `<div class="imp-src">📄 ${escapeHtml(o.source)}</div>` : ''}</div>`).join('') +
      ((d.my_knowledge || []).length ? '<div class="imp-sub">내 기여 (미승인)</div>' + d.my_knowledge.map(k => `<div class="imp-item">• ${escapeHtml(k.content)}</div>`).join('') : '') +
      `<div class="imp-form"><div class="imp-sub">온톨로지 수정·추가 <span class="imp-tag imp-tag-now">즉시 · 내 재질의에 반영</span></div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <select id="impKTarget" style="flex:1;min-width:130px">
            <option value="ent" selected>이 ${d.scope_kind === 'channel' ? '채널' : '프로그램'}: ${escapeHtml((d.program && d.program.name) || '')}(${escapeHtml((d.program && d.program.code) || '')})</option>
            <option value="unclassified">미지정(관리자 분류)</option>
            <option value="global">전역</option></select>
          <select id="impKType" style="flex:1;min-width:120px">${_KTYPE_OPTS}</select></div>
        <textarea id="impKContent" placeholder="추가/수정할 도메인 지식"></textarea>
        <button class="imp-btn" onclick="_submitKnowledge()">지식 추가</button></div>`;
  } catch (e) { dataEl.innerHTML = ontoEl.innerHTML = '<div class="imp-mean">로드 실패</div>'; }
}

async function _submitKnowledge() {
  const content = (document.getElementById('impKContent').value || '').trim();
  if (!content) return;
  // target: 'ent'(질의 scope 자동추론) | unclassified | global
  const sel = (document.getElementById('impKTarget') || {}).value || 'ent';
  let target_kind = sel, target_id = null;
  if (sel === 'ent') {
    target_kind = _improveState.scopeKind || 'program';   // program|channel
    target_id = _improveState.program && _improveState.program.code;
  }
  await _authedFetch('/api/improve/contribute', { method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ kind: 'knowledge', type: document.getElementById('impKType').value,
      target_kind, target_id, content, op: 'add' }) });
  document.getElementById('impKContent').value = '';
  _loadImproveContext();
}
// 능동 기여 (내 기여 탭) — 질문 없이 지식 추가
function _akKindHint() {
  const k = document.getElementById('akKind').value;
  const t = document.getElementById('akTarget');
  const ph = { unclassified:'미지정 — 관리자가 분류(전역 작동)', program:'프로그램 코드 (F09)',
    channel:'채널 코드 (L00 / F00 / T00)', field:'지표 필드 (deep_rate)', global:'전역 — 입력 불필요' };
  t.placeholder = ph[k] || ''; t.disabled = (k === 'global' || k === 'unclassified');
}
async function _submitMyKnowledge() {
  const kind = document.getElementById('akKind').value;
  const target = (document.getElementById('akTarget').value || '').trim();
  const type = document.getElementById('akType').value;
  const content = (document.getElementById('akContent').value || '').trim();
  if (!content) { alert('내용을 입력하세요.'); return; }
  const needTarget = (kind === 'program' || kind === 'channel' || kind === 'field');
  if (needTarget && !target) { alert('대상 코드를 입력하세요 (모르면 미지정 선택).'); return; }
  await _authedFetch('/api/improve/contribute', { method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ kind: 'knowledge', type, target_kind: kind,
      target_id: needTarget ? target : null, content, op: 'add' }) });
  _loadMyImprovements();
}
async function _retireMyKnowledge(id, btn) {
  if (!confirm('이 기여 지식을 삭제합니까?')) return;
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/knowledge/mine/retire', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    _loadMyImprovements();
  } catch (e) { btn.disabled = false; alert('실패: ' + e.message); }
}
async function _submitUpload() {
  const name = (document.getElementById('udName').value || '').trim();
  const kind = document.getElementById('udKind').value;
  const target = (document.getElementById('udTarget').value || '').trim();
  const raw = (document.getElementById('udCsv').value || '').trim();
  if (!raw) { alert('표 데이터를 붙여넣으세요.'); return; }
  if (kind !== 'global' && !target) { alert('대상 코드를 입력하세요.'); return; }
  const lines = raw.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) { alert('헤더 + 최소 1행이 필요합니다.'); return; }
  const sep = lines[0].indexOf('\t') >= 0 ? '\t' : ',';
  const columns = lines[0].split(sep).map(s => s.trim());
  const rows = lines.slice(1).map(l => l.split(sep).map(s => s.trim()));
  await _authedFetch('/api/upload/add', { method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, target_kind: kind, target_id: kind === 'global' ? null : target, columns, rows }) });
  ['udCsv', 'udName', 'udTarget'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
  _loadMyImprovements();
}
async function _retireMyUpload(id, btn) {
  if (!confirm('이 업로드를 삭제합니까?')) return;
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/upload/mine/retire', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    _loadMyImprovements();
  } catch (e) { btn.disabled = false; alert('실패: ' + e.message); }
}
async function _reviewUpload(id, btn) {
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/upload/review', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    _loadReviewQueue();
  } catch (e) { btn.disabled = false; alert('실패: ' + e.message); }
}
async function _submitDataRequest() {
  const field = (document.getElementById('impReqField').value || '').trim();
  const desc = (document.getElementById('impReqDesc').value || '').trim();
  if (!field && !desc) return;
  await _authedFetch('/api/improve/contribute', { method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ kind: 'data_request', field_name: field, description: desc,
      target_id: _improveState.program && _improveState.program.code }) });
  document.getElementById('impReqField').value = '';
  document.getElementById('impReqDesc').value = '';
  alert('데이터 요청이 등록되었습니다 (관리자/데이터 직무 처리 후 반영).');
}
async function _doRequery() {
  const res = document.getElementById('improveResult');
  res.style.display = 'block';
  res.innerHTML = '<div class="imp-mean">재질의 중… (보강 정보 반영)</div>';
  try {
    const r = await _authedFetch('/api/improve/requery', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: _improveState.question, answer_original: _improveState.answer, source_query_id: _improveState.qid }) });
    const d = await r.json();
    if (!d.ok) { res.innerHTML = '<div class="imp-mean">재질의 실패: ' + escapeHtml(d.error || '') + '</div>'; return; }
    const j = d.judge;
    const jt = _judgeDetail(j);
    res.innerHTML = `<div class="imp-sub">개선된 답변 (B) · 적용 오버레이 ${ (d.overlay_items||[]).length }건</div>
      <div class="imp-orig">${renderAiText(d.answer_improved || '')}</div>${jt}
      <div class="imp-form"><div class="imp-sub">이 개선이 더 나은가요? (검토 큐로 전달)</div>
        <button class="imp-btn imp-btn-primary" onclick="_improveVerdict(${d.improvement_id},'improved')">개선됨</button>
        <button class="imp-btn" onclick="_improveVerdict(${d.improvement_id},'same')">비슷</button>
        <button class="imp-btn" onclick="_improveVerdict(${d.improvement_id},'worse')">더 나쁨</button></div>`;
  } catch (e) { res.innerHTML = '<div class="imp-mean">오류: ' + escapeHtml(e.message) + '</div>'; }
}
async function _improveVerdict(id, v) {
  try {
    await _authedFetch('/api/improve/verdict', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, verdict: v }) });
  } catch (_) {}
  alert(v === 'improved' ? '개선됨으로 기록 — 검토 큐로 전달됩니다.' : '기록되었습니다.');
  _closeImprove();
}

// ── ⑤ 개선 이력 (본인 개선 시도 + 데이터 요청 상태) ──
const _IMP_STATUS_KO = { '검토대기': '검토대기', '승인': '승인 ✓', '반려': '반려' };
const _VERDICT_KO = { improved: '개선됨', same: '비슷', worse: '더 나쁨' };
// LLM judge(다수결) 표시 헬퍼
function _judgeDetail(j) {
  if (!j) return '';
  const w = j.winner === 'B' ? '개선됨(B)' : j.winner === 'A' ? '원본 우세(A)' : '비슷';
  const agree = j.agreement != null
    ? ` · 합의 ${j.agreement}%${j.tally ? ` (B${j.tally.B}:A${j.tally.A}:동률${j.tally.tie})` : ''}` : '';
  const sa = j.scores && j.scores.A != null ? j.scores.A : '-';
  const sb = j.scores && j.scores.B != null ? j.scores.B : '-';
  return `<div class="imp-judge">LLM 판정(다수결): <b>${w}</b> · A ${sa} / B ${sb}${agree} — ${escapeHtml(j.reason || '')}</div>`;
}
function _judgeBadge(j) {
  if (!j) return '';
  const w = j.winner === 'B' ? 'B우세' : j.winner === 'A' ? 'A우세' : '동률';
  return `LLM ${w}${j.agreement != null ? ' ' + j.agreement + '%' : ''}`;
}
// 사용자 verdict ↔ judge 불일치 (improved→B기대, worse→A기대)
function _verdictMismatch(verdict, j) {
  if (!verdict || !j) return false;
  if (verdict === 'improved' && j.winner !== 'B') return true;
  if (verdict === 'worse' && j.winner !== 'A') return true;
  return false;
}
function _impStatusBadge(st) {
  const cls = st === '승인' ? 'imp-st-ok' : st === '반려' ? 'imp-st-no' : 'imp-st-wait';
  return `<span class="imp-st ${cls}">${escapeHtml(_IMP_STATUS_KO[st] || st || '')}</span>`;
}
function _reqStatusBadge(st) {
  const cls = st === '완료' ? 'imp-st-ok' : st === '반려' ? 'imp-st-no' : st === '처리중' ? 'imp-st-mid' : 'imp-st-wait';
  return `<span class="imp-st ${cls}">${escapeHtml(st || '')}</span>`;
}
const _KTYPE_OPTS = `<option value="episode_note">회차별 방송 특이사항</option>
  <option value="program_corner">프로그램 및 코너 정보</option>
  <option value="metric_term">지표 및 용어 정의</option>
  <option value="answer_style">답변 스타일</option>
  <option value="analysis_method">분석 기법</option>
  <option value="misc" selected>기타 정보(미분류)</option>`;
const _KT_KO = { episode_note:'회차별 방송 특이사항', program_corner:'프로그램 및 코너 정보',
  metric_term:'지표 및 용어 정의', answer_style:'답변 스타일', analysis_method:'분석 기법', misc:'기타(미분류)',
  // 레거시 type 호환(기존 기여 표시용)
  metric_definition:'지표정의', field_meaning:'필드의미', program_note:'프로그램메모',
  guest_policy:'게스트정책', corner_note:'코너메모', decomposition_hint:'분해힌트', fact:'사실' };

async function _loadMyImprovements() {
  const body = document.getElementById('histModalBody');
  _setHistFooter('내 기여');
  try {
    const r = await _authedFetch('/api/improve/mine', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '로드 실패');
    const imps = d.improvements || [], reqs = d.data_requests || [], kn = d.knowledge || [];
    const ups = d.uploads || [];
    let html = '';
    // 능동 기여 — 질문 없이 도메인 지식 직접 추가
    html += `<div class="imp-sec-hd">＋ 지식 추가 (질문 없이 능동 기여 · 내 재질의에 즉시 반영, 승인 시 전체 반영)</div>
      <div class="imp-form" style="margin:0 14px">
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <select id="akKind" style="flex:1;min-width:110px" onchange="_akKindHint()">
            <option value="unclassified" selected>미지정(권장)</option>
            <option value="program">프로그램</option><option value="channel">채널</option>
            <option value="field">지표(필드)</option><option value="global">전역</option></select>
          <input id="akTarget" style="flex:2;min-width:140px" placeholder="대상 코드 (F09 · L00 · deep_rate)">
          <select id="akType" style="flex:1;min-width:120px">${_KTYPE_OPTS}</select>
        </div>
        <textarea id="akContent" placeholder="추가할 도메인 지식 (예: F09는 매주 금요일 특별 게스트 코너 운영)"></textarea>
        <button class="imp-btn imp-btn-primary" onclick="_submitMyKnowledge()">지식 추가</button>
      </div>`;
    if (kn.length) {
      html += '<div class="imp-sec-hd">내 기여 지식 ' + kn.length + '건</div>' + kn.map(k =>
        `<div class="hm-row"><div class="hm-summary">
          <span class="wk-chip" style="margin:0">${escapeHtml(_KT_KO[k.type] || k.type || '')}</span>
          <span class="wk-chip" style="margin:0">${escapeHtml(k.target_id || '전역')}</span>
          <span class="imp-st ${k.scope === 'approved' ? 'imp-st-ok' : 'imp-st-wait'}">${k.scope === 'approved' ? '승인·공유' : '내 후보'}</span>
          <span class="hm-q">${escapeHtml((k.content || '').slice(0, 56))}</span>
          <span class="hm-sess-meta">${k.scope === 'approved'
            ? '<span class="hm-sess-meta" style="opacity:.6">승인됨 · 관리자 회수만 가능</span>'
            : `<button class="imp-btn imp-mini" onclick="_retireMyKnowledge(${k.id},this)">삭제</button>`}</span>
        </div></div>`).join('');
    }
    // 본인 데이터 업로드 — 소규모 표(CSV/탭 붙여넣기)
    html += `<div class="imp-sec-hd">⬆ 데이터 업로드 (소규모 표 · 내 재질의에 즉시 반영, 승인 시 전체)</div>
      <div class="imp-form" style="margin:0 14px">
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <input id="udName" style="flex:2;min-width:120px" placeholder="데이터 이름 (예: 외부 설문 만족도)">
          <select id="udKind" style="flex:1;min-width:100px"><option value="program">프로그램</option><option value="channel">채널</option><option value="global">전역</option></select>
          <input id="udTarget" style="flex:1;min-width:110px" placeholder="대상 코드 (F09)">
        </div>
        <textarea id="udCsv" placeholder="CSV/탭 붙여넣기 — 첫 줄 헤더\n날짜,만족도\n2026-06-19,4.2"></textarea>
        <button class="imp-btn imp-btn-primary" onclick="_submitUpload()">업로드</button>
      </div>`;
    if (ups.length) {
      html += '<div class="imp-sec-hd">내 업로드 ' + ups.length + '건</div>' + ups.map(u => {
        let nc = 0, nr = 0; try { nc = JSON.parse(u.columns_json || '[]').length; nr = JSON.parse(u.rows_json || '[]').length; } catch (_) {}
        return `<div class="hm-row"><div class="hm-summary">
          <span class="wk-chip" style="margin:0">${escapeHtml(u.target_id || '전역')}</span>
          <span class="imp-st ${u.scope === 'approved' ? 'imp-st-ok' : 'imp-st-wait'}">${u.scope === 'approved' ? '승인·공유' : '내 후보'}</span>
          <span class="hm-q"><b>${escapeHtml(u.name || '업로드')}</b> · ${nc}열 ${nr}행</span>
          <span class="hm-sess-meta"><button class="imp-btn imp-mini" onclick="_retireMyUpload(${u.id},this)">삭제</button></span>
        </div></div>`;
      }).join('');
    }
    if (!imps.length && !reqs.length && !kn.length && !ups.length) {
      html += '<div class="hist-empty-msg" style="padding:16px">아직 개선 시도·기여 내역이 없습니다.</div>';
    }
    if (imps.length) {
      html += '<div class="imp-sec-hd">개선 시도</div>' + imps.map(it => {
        const j = it.judge;
        const jt = _judgeBadge(j);
        const vd = it.user_verdict ? `평가:${_VERDICT_KO[it.user_verdict] || it.user_verdict}` : '미평가';
        return `<div class="hm-row" onclick="_toggleHmRow(this)">
          <div class="hm-summary">
            <span class="hm-time">${escapeHtml(_histFmt(it.created_at))}</span>
            ${_impStatusBadge(it.status)}
            <span class="hm-q">${escapeHtml((it.question || '').slice(0, 56))}</span>
            <span class="hm-sess-meta">${escapeHtml(vd)}${jt ? ' · ' + escapeHtml(jt) : ''}</span>
          </div>
          <div class="hm-detail">
            <div class="imp-ab"><div class="imp-ab-col"><div class="imp-sub">원답변 (A)</div><div class="imp-orig">${renderAiText(it.answer_original || '')}</div></div>
              <div class="imp-ab-col"><div class="imp-sub">개선 답변 (B)</div><div class="imp-orig">${renderAiText(it.answer_improved || '')}</div></div></div>
            ${_judgeDetail(j)}
          </div></div>`;
      }).join('');
    }
    if (reqs.length) {
      html += '<div class="imp-sec-hd">내 데이터 요청</div>' + reqs.map(rq =>
        `<div class="hm-row"><div class="hm-summary">
          <span class="hm-time">${escapeHtml(_histFmt(rq.created_at))}</span>
          ${_reqStatusBadge(rq.status)}
          <span class="hm-q"><b>${escapeHtml(rq.field_name || '필드')}</b> ${escapeHtml((rq.description || '').slice(0, 50))}</span>
          ${rq.target_id ? `<span class="hm-sess-meta">${escapeHtml(rq.target_id)}</span>` : ''}
        </div></div>`).join('');
    }
    body.innerHTML = html;
  } catch (e) { body.innerHTML = `<div class="hist-empty-msg">로드 실패: ${escapeHtml(e.message)}</div>`; }
}

// ── ⑥ 거버넌스 검토 큐 (관리자/데이터/총괄관리) ──
async function _loadReviewQueue() {
  const body = document.getElementById('histModalBody');
  _setHistFooter('검토 큐');
  try {
    const r = await _authedFetch('/api/improve/queue', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '권한이 없거나 로드 실패');
    window._reviewIndex = {}; window._reviewNegIndex = {};
    const imps = d.improvements || [], reqs = d.data_requests || [];
    const wk = d.weakness || [], neg = d.negative_open || [];
    const _anyQueue = imps.length || reqs.length || wk.length || neg.length ||
      (d.approved_knowledge || []).length || (d.pending_uploads || []).length || (d.effect || []).length;
    // ✍ 답변 스타일 정책 — 매 답변에 주입되는 단일 큐레이션 블록. 관리자(is_admin) 전용 노출.
    const styleHtml = RAAS_USER && RAAS_USER.is_admin ? `<div class="imp-sec-hd">✍ 답변 스타일 정책 — <b>모든 답변</b>에 주입되는 단일 블록(짧게 유지) · 관리자 전용</div>
      <div class="imp-form" style="margin:0 14px">
        <textarea id="stylePolicyText" style="min-height:120px" placeholder="불러오는 중…" oninput="_styleCount()"></textarea>
        <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
          <span id="styleCount" class="imp-mean" style="margin:0">— / —자</span>
          <span id="styleDefaultTag" class="imp-tag" style="display:none">기본값(미저장)</span>
          <button class="imp-btn imp-btn-primary" onclick="_saveStylePolicy(this)">정책 저장</button>
        </div>
      </div>` : '';
    const dashHtml = _funnelDashboard(d.funnel);
    // 관리자 전용 승격 바 — 항상 노출(승인 0건·큐 비어도 닿을 수 있게)
    const promoteBar = (RAAS_USER && RAAS_USER.is_admin)
      ? `<div style="margin:0 14px 12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
          <button class="imp-btn imp-btn-primary imp-mini" onclick="_previewPromotion(this)">📦 TTL 승격(미리보기)</button>
          <span class="imp-mean" style="margin:0">승인 지식 ${(d.funnel && d.funnel.total_approved) || 0}건 → 온톨로지 졸업 · 관리자 전용</span>
        </div>` : '';
    if (!_anyQueue) {
      body.innerHTML = styleHtml + dashHtml + promoteBar + '<div class="hist-empty-msg" style="margin-top:8px">검토 대기·약점 신호가 없습니다.</div>';
      _loadStylePolicy(); return;
    }
    let html = styleHtml + dashHtml + promoteBar;
    // 약점 신호 — 👎 집계 + 미개선 아쉬움 질의(클릭→바로 개선 착수)
    if (wk.length || neg.length) {
      html += '<div class="imp-sec-hd">⚠ 약점 신호 · 최근 30일 \'아쉬움\' 피드백 — 클릭해 바로 개선 착수</div>';
      if (wk.length) {
        html += '<div class="wk-chips">' + wk.map(w =>
          `<span class="wk-chip" title="👎 ${w.neg} / 전체 ${w.total}">${escapeHtml(w.name || w.scope)} <b>👎${w.neg}</b><span class="wk-rate">${w.neg_rate}%</span></span>`
        ).join('') + '</div>';
      }
      html += neg.map(q => {
        window._reviewNegIndex[q.id] = q;
        return `<div class="hm-row" style="cursor:pointer" onclick="_openImproveFromNeg(${q.id})">
          <div class="hm-summary">
            <span class="hm-time">${escapeHtml(_histFmt(q.created_at))}</span>
            <span class="hm-user">${escapeHtml(q.user_name || '—')}</span>
            <span class="wk-badge">👎</span>
            <span class="hm-q">${escapeHtml((q.question || '').slice(0, 54))}</span>
            <span class="hm-sess-meta">${escapeHtml(q.scope || '')} · ✦ 개선하기</span>
          </div>${q.feedback_reason ? `<div class="hm-q" style="margin:2px 0 0 4px;color:var(--red);opacity:.9">💬 ${escapeHtml(q.feedback_reason)}</div>` : ''}</div>`;
      }).join('');
    }
    if (imps.length) {
      html += '<div class="imp-sec-hd">개선 검토 대기 · 승인 시 기여 지식이 전체에 반영됩니다</div>' + imps.map(it => {
        window._reviewIndex[it.id] = it;
        const j = it.judge;
        const jt = _judgeBadge(j);
        const mism = _verdictMismatch(it.user_verdict, j) ? '<span class="wk-badge" title="사용자 평가와 LLM 판정이 다름">⚠ 불일치</span>' : '';
        const contribs = (it.contributions || []).map(c => `<div class="imp-item">• [${escapeHtml(c.type || '')}] ${escapeHtml(c.content || '')}</div>`).join('') || '<div class="imp-mean">기여 지식 없음(데이터 요청만)</div>';
        return `<div class="hm-row"><div class="hm-summary" onclick="_toggleHmRow(this)">
            <span class="hm-time">${escapeHtml(_histFmt(it.created_at))}</span>
            <span class="hm-user">${escapeHtml(it.user_name || '—')}</span>
            <span class="hm-q">${escapeHtml((it.question || '').slice(0, 52))}</span>
            <span class="hm-sess-meta">${it.user_verdict ? _VERDICT_KO[it.user_verdict] || it.user_verdict : '미평가'}${jt ? ' · ' + escapeHtml(jt) : ''} ${mism}</span>
          </div>
          <div class="hm-detail open">
            <div class="imp-ab"><div class="imp-ab-col"><div class="imp-sub">원답변 (A)</div><div class="imp-orig">${renderAiText(it.answer_original || '')}</div></div>
              <div class="imp-ab-col"><div class="imp-sub">개선 답변 (B)</div><div class="imp-orig">${renderAiText(it.answer_improved || '')}</div></div></div>
            ${_judgeDetail(j)}
            <div class="imp-sub">기여 지식 (승인 시 공유 오버레이로 승격)</div>${contribs}
            <div class="imp-form">
              <button class="imp-btn imp-btn-primary" onclick="event.stopPropagation();_reviewImprove(${it.id},'approve',this)">승인 → 전체 반영</button>
              <button class="imp-btn" onclick="event.stopPropagation();_reviewImprove(${it.id},'reject',this)">반려</button>
            </div>
          </div></div>`;
      }).join('');
    }
    if (reqs.length) {
      html += '<div class="imp-sec-hd">데이터 요청 처리</div>' + reqs.map(rq =>
        `<div class="hm-row"><div class="hm-summary">
          <span class="hm-time">${escapeHtml(_histFmt(rq.created_at))}</span>
          ${_reqStatusBadge(rq.status)}
          <span class="hm-q"><b>${escapeHtml(rq.field_name || '필드')}</b> ${escapeHtml((rq.description || '').slice(0, 46))}</span>
          <span class="hm-sess-meta">
            <button class="imp-btn imp-mini" onclick="_processReq(${rq.id},'처리중',this)">처리중</button>
            <button class="imp-btn imp-mini imp-btn-primary" onclick="_processReq(${rq.id},'완료',this)">완료</button>
            <button class="imp-btn imp-mini" onclick="_processReq(${rq.id},'반려',this)">반려</button>
          </span>
        </div></div>`).join('');
    }
    // 📈 반영 효과 — 승인된 지식 대상 scope의 승인 전/후 👎 비교(롤백 판단 보조)
    const eff = d.effect || [];
    if (eff.length) {
      html += '<div class="imp-sec-hd">📈 반영 효과 · 승인 지식 대상의 전/후 \'아쉬움\' 비교 (after가 더 나쁘면 회수 검토)</div>' +
        eff.map(e => {
          const br = e.before.tot ? Math.round(e.before.neg / e.before.tot * 100) : null;
          const ar = e.after.tot ? Math.round(e.after.neg / e.after.tot * 100) : null;
          const worse = (ar != null && br != null && ar > br);
          const arr = (ar == null) ? '<span class="wk-rate">후속 표본 없음</span>'
            : `${br == null ? '–' : br + '%'} → <b style="color:${worse ? 'var(--red)' : 'var(--green)'}">${ar}%</b>`;
          return `<div class="hm-row"><div class="hm-summary">
            <span class="wk-chip" style="margin:0">${escapeHtml(e.target_id || '전역')}</span>
            <span class="hm-q">${escapeHtml((e.content || '').slice(0, 50))}</span>
            <span class="hm-sess-meta">👎 ${arr} (전 ${e.before.neg}/${e.before.tot} · 후 ${e.after.neg}/${e.after.tot})${worse ? ' <span class="wk-badge">⚠ 악화</span>' : ''}</span>
          </div></div>`;
        }).join('');
    }
    // 📚 승인된 공유 지식(본체) — 모든 답변에 주입되는 canonical 도메인 지식. 회수 가능.
    const ak = d.approved_knowledge || [];
    if (ak.length) {
      const KT = { metric_definition:'지표정의', field_meaning:'필드의미', program_note:'프로그램메모',
        guest_policy:'게스트정책', corner_note:'코너메모', decomposition_hint:'분해힌트', fact:'사실' };
      html += `<div class="imp-sec-hd">📚 승인된 공유 지식 (본체) · ${ak.length}건 — 모든 답변 grounding에 주입 중</div>` +
        ak.map(k => { const unc = (k.target_kind === 'unclassified' || (!k.target_id && k.target_kind !== 'global'));
          return `<div class="hm-row"><div class="hm-summary">
          <span class="wk-chip" style="margin:0">${escapeHtml(KT[k.type] || k.type || '')}</span>
          <span class="wk-chip ${unc ? 'imp-st-wait' : ''}" style="margin:0">${unc ? '⚠ 미분류' : escapeHtml((k.target_kind || '') + (k.target_id ? ':' + k.target_id : '전역'))}</span>
          <span class="hm-q">${escapeHtml((k.content || '').slice(0, 64))}</span>
          <span class="hm-sess-meta">
            <button class="imp-btn imp-mini" onclick="_reclassifyKnowledge(${k.id},this)">재분류</button>
            <button class="imp-btn imp-mini" onclick="_retireKnowledge(${k.id},this)">회수</button></span>
        </div></div>`; }).join('');
    }
    // ⬆ 업로드 데이터 승인 대기
    const pu = d.pending_uploads || [];
    if (pu.length) {
      html += '<div class="imp-sec-hd">⬆ 업로드 데이터 승인 대기 · 승인 시 전체 답변에 반영</div>' + pu.map(u => {
        let nc = 0, nr = 0; try { nc = JSON.parse(u.columns_json || '[]').length; nr = JSON.parse(u.rows_json || '[]').length; } catch (_) {}
        return `<div class="hm-row"><div class="hm-summary">
          <span class="wk-chip" style="margin:0">${escapeHtml(u.target_id || '전역')}</span>
          <span class="hm-q"><b>${escapeHtml(u.name || '업로드')}</b> · ${nc}열 ${nr}행</span>
          <span class="hm-sess-meta"><button class="imp-btn imp-mini imp-btn-primary" onclick="_reviewUpload(${u.id},this)">승인 → 전체</button></span>
        </div></div>`;
      }).join('');
    }
    body.innerHTML = html;
    _loadStylePolicy();
  } catch (e) { body.innerHTML = `<div class="hist-empty-msg">로드 실패: ${escapeHtml(e.message)}</div>`; }
}
// ✍ 답변 스타일 정책 — 불러오기/글자수/저장
let _styleMax = 1000;
async function _loadStylePolicy() {
  const ta = document.getElementById('stylePolicyText');
  if (!ta) return;
  try {
    const r = await _authedFetch('/api/style/get', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '로드 실패');
    ta.value = d.content || '';
    _styleMax = d.max || 1000;
    const tag = document.getElementById('styleDefaultTag');
    if (tag) tag.style.display = d.is_default ? '' : 'none';
    _styleCount();
  } catch (e) { ta.placeholder = '로드 실패: ' + e.message; }
}
function _styleCount() {
  const ta = document.getElementById('stylePolicyText');
  const el = document.getElementById('styleCount');
  if (!ta || !el) return;
  const n = (ta.value || '').length;
  el.textContent = `${n} / ${_styleMax}자`;
  el.style.color = n > _styleMax ? '#c0392b' : '';
}
async function _saveStylePolicy(btn) {
  const ta = document.getElementById('stylePolicyText');
  const content = (ta.value || '').trim();
  if (!content) { alert('내용이 비어 있습니다.'); return; }
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/style/set', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ content }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '저장 실패');
    const tag = document.getElementById('styleDefaultTag');
    if (tag) tag.style.display = 'none';
    alert('스타일 정책 저장 완료 — 이후 모든 답변에 반영됩니다.');
  } catch (e) { alert('실패: ' + e.message); }
  finally { btn.disabled = false; }
}
async function _reclassifyKnowledge(id, btn) {
  const kind = prompt('대상 종류를 입력: program / channel / field / global / unclassified', 'program');
  if (!kind) return;
  let tid = null;
  if (['program', 'channel', 'field'].includes(kind)) {
    tid = prompt('대상 코드 (예: F09 / L00 / deep_rate)', '');
    if (!tid) return;
  }
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/knowledge/reclassify', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id, target_kind: kind, target_id: tid }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    _loadReviewQueue();
  } catch (e) { btn.disabled = false; alert('실패: ' + e.message); }
}
// 📊 개선 루프 효과 대시보드 — 퍼널(👎→개선→승인→승격) + 주간 👎율 추세
function _funnelDashboard(f) {
  if (!f) return '';
  const conv = (a, b) => a > 0 ? Math.round(b / a * 100) + '%' : '—';
  const stage = (icon, label, n, sub) => `<div style="text-align:center;min-width:62px">
    <div style="font-size:22px;font-weight:700">${n}</div>
    <div style="font-size:12px;color:var(--dim)">${icon} ${label}</div>
    <div style="font-size:11px;color:var(--accent)">${sub || '&nbsp;'}</div></div>`;
  const arrow = `<div style="color:var(--dim);font-size:18px;margin-top:6px">→</div>`;
  const funnel = `<div style="display:flex;align-items:flex-start;gap:10px;flex-wrap:wrap;padding:10px;border:1px solid var(--line);border-radius:8px;background:var(--bg1)">
    ${stage('👎', '아쉬움', f.neg, '')}${arrow}${stage('✦', '개선착수', f.improvements, conv(f.neg, f.improvements))}${arrow}${stage('✓', '승인', f.approved, conv(f.improvements, f.approved))}${arrow}${stage('📦', '승격', f.promoted, conv(f.approved, f.promoted))}</div>`;
  const wk = f.neg_weekly || [];
  const maxRate = Math.max(...wk.map(w => w.rate), 1);
  const bars = wk.map(w => `<div title="${w.week}: 👎${w.neg}/${w.tot} (${w.rate}%)" style="flex:1;min-width:10px;background:var(--accent);opacity:.7;border-radius:2px 2px 0 0;height:${Math.max(6, Math.round(w.rate / maxRate * 100))}%"></div>`).join('');
  const trend = `<div style="margin-top:8px"><div style="font-size:12px;color:var(--dim);margin-bottom:4px">주간 👎율 추세 (6주)</div>
    <div style="display:flex;align-items:flex-end;gap:3px;height:46px">${bars || '<span class="imp-mean">데이터 없음</span>'}</div></div>`;
  return `<div class="imp-sec-hd">📊 개선 루프 효과 · 최근 ${f.window_days}일 (누계 공유 ${f.total_approved} · 승격 ${f.total_promoted})</div>
    <div style="margin:0 14px 10px">${funnel}${trend}</div>`;
}

// 📦 TTL 승격 — 미리보기(병합 전후·텍스트 변경) → 실행. 관리자 전용.
async function _previewPromotion(btn) {
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/knowledge/promote/preview', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '미리보기 실패');
    _renderPromotionPreview(d);
  } catch (e) { alert('실패: ' + e.message); } finally { btn.disabled = false; }
}
function _diffLines(before, after) {
  const A = (before || '').split('\n'), B = (after || '').split('\n');
  const sA = new Set(A), sB = new Set(B), out = [];
  A.forEach(l => { if (!sB.has(l) && l.trim()) out.push('<span style="color:#e06c75">- ' + escapeHtml(l) + '</span>'); });
  B.forEach(l => { if (!sA.has(l) && l.trim()) out.push('<span style="color:#98c379">+ ' + escapeHtml(l) + '</span>'); });
  return out.length ? out.join('\n') : '(변경 없음)';
}
function _renderPromotionPreview(d) {
  const body = document.getElementById('histModalBody');
  const items = (d.items || []).map(it => `<div class="hm-row"><div class="hm-summary">
      <span class="wk-chip" style="margin:0">${escapeHtml(_KT_KO[it.type] || it.type || '')}</span>
      <span class="wk-chip" style="margin:0">${escapeHtml(it.target || '')}</span>
      ${it.excluded
        ? `<span class="imp-st imp-st-wait">제외 · ${escapeHtml(it.reason || '')}</span>`
        : `<span class="wk-chip imp-st-ok" style="margin:0">→ ${escapeHtml(it.node || '전역')} · ${escapeHtml((it.predicate || '').replace('raas:', ''))}</span>`}
      <span class="hm-q" style="opacity:.85">${escapeHtml((it.content || '').slice(0, 60))}</span>
    </div></div>`).join('');
  body.innerHTML = `
    <div class="imp-sec-hd">📦 TTL 승격 미리보기 — 구조화 ${d.promoted}건 / 전체 ${d.count}건 (제외 ${d.count - d.promoted})</div>
    <div style="margin:0 14px 10px">${items || '<div class="hist-empty-msg">승인된 지식이 없습니다.</div>'}</div>
    <div class="imp-sec-hd">contributed.ttl 텍스트 변경 (− 이전 / + 이후)</div>
    <pre style="margin:0 14px;max-height:300px;overflow:auto;font-size:12px;line-height:1.5;white-space:pre-wrap;background:var(--bg1);border:1px solid var(--line);border-radius:8px;padding:10px">${_diffLines(d.ttl_before, d.ttl_after)}</pre>
    <div style="display:flex;gap:8px;margin:12px 14px 4px">
      <button class="imp-btn imp-btn-primary" onclick="_doPromote(this)">📦 승격 실행</button>
      <button class="imp-btn" onclick="_loadReviewQueue()">취소</button>
    </div>`;
}
async function _doPromote(btn) {
  if (!confirm('contributed.ttl로 승격합니까?\n(즉시 답변에 반영 · 레포 커밋은 별도로 영속)')) return;
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/knowledge/promote', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    alert(`TTL 승격 완료 — 구조화 ${d.promoted}/${d.count}건 기록.\n(레포에 커밋하면 영속)`);
    _loadReviewQueue();
  } catch (e) { alert('실패: ' + e.message); btn.disabled = false; }
}
async function _retireKnowledge(id, btn) {
  if (!confirm('이 공유 지식을 회수합니까? (모든 답변에서 제외됩니다)')) return;
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/knowledge/retire', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    _loadReviewQueue();
  } catch (e) { btn.disabled = false; alert('실패: ' + e.message); }
}
async function _reviewImprove(id, action, btn) {
  if (action === 'reject' && !confirm('이 개선을 반려합니까?')) return;
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/improve/review', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, action }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    _loadReviewQueue();
  } catch (e) { btn.disabled = false; alert('실패: ' + e.message); }
}
async function _processReq(id, status, btn) {
  btn.disabled = true;
  try {
    const r = await _authedFetch('/api/data_request/process', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, status }) });
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || '실패');
    _loadReviewQueue();
  } catch (e) { btn.disabled = false; alert('실패: ' + e.message); }
}
function _setHistFooter(label) {
  document.getElementById('histModalCount').textContent = label;
  document.getElementById('hmPgPrev').disabled = true;
  document.getElementById('hmPgNext').disabled = true;
}

// 세션 행 펼침 → 이동 경로 lazy 로드
function _reuseHistQuery(btn) {
  closeHistModal();
  submitQuery(btn.dataset.q, 'history_replay');
}

function _updateHistFooter() {
  const from = _histTotal ? _histOffset + 1 : 0;
  const to   = Math.min(_histOffset + HIST_PAGE_SIZE, _histTotal);
  document.getElementById('histModalCount').textContent =
    _histTotal ? `${from}–${to} / 전체 ${_histTotal.toLocaleString()}건` : '0건';
  document.getElementById('hmPgPrev').disabled = _histOffset === 0;
  document.getElementById('hmPgNext').disabled = _histOffset + HIST_PAGE_SIZE >= _histTotal;
}

document.addEventListener('click', e => {
  if (e.target.id === 'histModal') closeHistModal();
});
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeHistModal();
});

// ────────────────────────────────────────────────
// KPI PANEL
// ────────────────────────────────────────────────
// ── KPI 패널 — 기간(일/주/월)·스코프(전체/채널/관심) 스냅샷 ──────────────
// 구 '이번 주 브리핑' 칩의 숫자 기능을 흡수. 서술은 하단 ✨ AI 요약 버튼으로.
const _KPI = {
  period: localStorage.getItem('raas_kpi_period') || 'day',
  scope:  localStorage.getItem('raas_kpi_scope')  || 'T00',
};
const _KPI_SCOPES = [
  ['T00', '고릴라 전체'], ['F00', '파워FM'], ['L00', '러브FM'],
  ['G00', '고릴라M'], ['P00', '픽채널'],
];
// 카드 정의 — f:값필드, c:증감필드, pp:증감이 %p(비율), pct:값 자체가 %
const _KPI_CARDS = {
  day: [
    ['규모', [
      {l:'DAU', f:'dau', c:'dau_chg'},
      {l:'롤링WAU', f:'dau_r7', c:'dau_r7_chg'},
      {l:'롤링MAU', f:'dau_r30', c:'dau_r30_chg'},
    ]],
    ['사용자 흐름', [
      {l:'신규', f:'new', c:'new_chg'},
      {l:'복귀', f:'react', c:'react_chg'},
      {l:'이탈율', f:'churn_rate', c:'churn_rate_diff', pp:1, pct:1},
      {l:'복귀율', f:'react_rate', c:'react_rate_diff', pp:1, pct:1},
    ]],
    ['청취 품질', [
      {l:'실청취율', f:'real_rate', c:'real_rate_diff', pp:1, pct:1},
      {l:'깊은청취율', f:'deep_rate', c:'deep_rate_diff', pp:1, pct:1},
      {l:'참여율', f:'engage_rate', c:'engage_rate_diff', pp:1, pct:1},
      {l:'습관형성률', f:'habit_rate', c:'habit_rate_diff', pp:1, pct:1},
      {l:'D7 유지율', f:'d7_ret', c:'d7_ret_diff', pp:1, pct:1},
    ]],
  ],
  week: [
    ['규모', [ {l:'WAU', f:'wau', c:'wau_chg'} ]],
    ['사용자 흐름', [
      {l:'신규', f:'new_week', c:'new_week_chg'},
      {l:'복귀', f:'react_week', c:'react_week_chg'},
      {l:'이탈율', f:'churn_rate_week', c:'churn_rate_week_diff', pp:1, pct:1},
      {l:'복귀율', f:'react_rate_week', c:'react_rate_week_diff', pp:1, pct:1},
    ]],
    ['청취 품질', [
      {l:'실청취율', f:'real_rate_week', c:'real_rate_week_diff', pp:1, pct:1},
      {l:'깊은청취율', f:'deep_rate_week', c:'deep_rate_week_diff', pp:1, pct:1},
      {l:'참여율', f:'engage_rate_week', c:'engage_rate_week_diff', pp:1, pct:1},
      {l:'습관형성률', f:'habit_rate_week', c:'habit_rate_week_diff', pp:1, pct:1},
      {l:'W1 유지율', f:'w1_ret', c:'w1_ret_diff', pp:1, pct:1},
    ]],
  ],
  mon: [
    ['규모', [ {l:'MAU', f:'mau', c:'mau_chg'} ]],
    ['사용자 흐름', [
      {l:'신규', f:'new_mon', c:'new_mon_chg'},
      {l:'복귀', f:'react_mon', c:'react_mon_chg'},
      {l:'이탈율', f:'churn_rate_mon', c:'churn_rate_mon_diff', pp:1, pct:1},
      {l:'복귀율', f:'react_rate_mon', c:'react_rate_mon_diff', pp:1, pct:1},
    ]],
    ['청취 품질', [
      {l:'실청취율', f:'real_rate_mon', c:'real_rate_mon_diff', pp:1, pct:1},
      {l:'깊은청취율', f:'deep_rate_mon', c:'deep_rate_mon_diff', pp:1, pct:1},
      {l:'참여율', f:'engage_rate_mon', c:'engage_rate_mon_diff', pp:1, pct:1},
      {l:'습관형성률', f:'habit_rate_mon', c:'habit_rate_mon_diff', pp:1, pct:1},
      {l:'M1 유지율', f:'m1_ret', c:'m1_ret_diff', pp:1, pct:1},
    ]],
  ],
};
const _KPI_PERIOD_KO = { day: '일간', week: '주간', mon: '월간' };

function _kpiSetPeriod(p) {
  _KPI.period = p; localStorage.setItem('raas_kpi_period', p); loadKpiPanel();
}
function _kpiSetScope(s) {
  _KPI.scope = s; localStorage.setItem('raas_kpi_scope', s); loadKpiPanel();
}
function _kpiAiBrief() {
  const phrase = { day: '어제', week: '이번 주', mon: '이번 달' }[_KPI.period];
  if (isMobile()) closeKpi();
  submitQuery(`${_kpiScopeName()}${phrase} 핵심 지표 브리핑해줘`, 'kpi_panel');
}

// ── 카드 탭 → 인라인 미니 꺾은선 (아코디언, 동시 1개) ──────────────────
let _kpiOpenField = null;
let _kpiMiniChart = null;

function _kpiCloseChart() {
  if (_kpiMiniChart) { try { _kpiMiniChart.dispose(); } catch (_) {} }
  _kpiMiniChart = null;
  const el = document.getElementById('kpiMiniChartBox');
  if (el) el.remove();
  _kpiOpenField = null;
}

function _kpiScopeName() {
  // 패널발 질의는 T00 포함 항상 스코프명 명시 — 엔티티 없는 질의가 채팅의
  // '내 관심' 기본값으로 넘어가 다른 대상(예: 컬투쇼)으로 답하는 오귀속 방지.
  if (_KPI.scopeName) return _KPI.scopeName + ' ';
  const sc = _KPI_SCOPES.find(x => x[0] === _KPI.scope);
  return (sc ? sc[1] : _KPI.scope) + ' ';
}

function _kpiTrendAsk(label) {
  const per = { day: '최근 4주', week: '최근 3개월 주간', mon: '월간' }[_KPI.period];
  if (isMobile()) closeKpi();
  submitQuery(`${_kpiScopeName()}${per} ${label} 추이 분석해줘`, 'kpi_panel');
}

async function _kpiToggleChart(card) {
  const f = card.dataset.f, label = card.dataset.l, pct = card.dataset.pct === '1';
  if (_kpiOpenField === f) { _kpiCloseChart(); return; }
  _kpiCloseChart();
  _kpiOpenField = f;
  const box = document.createElement('div');
  box.id = 'kpiMiniChartBox';
  box.className = 'kpi-mini-chart';
  box.innerHTML = '<div class="kpi-loading">추이 로드 중…</div>';
  card.insertAdjacentElement('afterend', box);
  try {
    const res = await fetch(`/api/kpi-series?scope=${encodeURIComponent(_KPI.scope)}&metric=${encodeURIComponent(f)}&period=${_KPI.period}`);
    const j = await res.json();
    const pts = (j.points || []).filter(p => p[1] != null);
    if (!j.ok || !pts.length) { box.innerHTML = '<div class="kpi-loading">추이 데이터 없음</div>'; return; }
    box.innerHTML = `<div class="kpi-mini-head">${escapeHtml(label)} 추이</div>
      <div class="kpi-mini-canvas" style="width:100%;height:120px"></div>
      <button class="kpi-chart-link" onclick="_kpiTrendAsk('${escapeHtml(label)}')">이 추이 분석 →</button>`;
    const echarts = await _loadECharts();
    const css = getComputedStyle(document.documentElement);
    const col = v => css.getPropertyValue(v).trim();
    const dates = pts.map(p => String(p[0]).slice(5));   // MM/DD
    const vals = pts.map(p => p[1]);
    const few = vals.length <= 16;                        // 주간·월간 → 포인트 마커 강조
    _kpiMiniChart = echarts.init(box.querySelector('.kpi-mini-canvas'), null, { renderer: 'canvas' });
    _kpiMiniChart.setOption({
      grid: { left: 6, right: 12, top: 10, bottom: 4, containLabel: true },
      xAxis: { type: 'category', data: dates, axisTick: { show: false },
               axisLabel: { fontSize: 9, color: col('--sub'), interval: few ? 0 : 'auto' },
               axisLine: { lineStyle: { color: col('--line2') } } },
      yAxis: { type: 'value', scale: true, splitNumber: 3,
               axisLabel: { fontSize: 9, color: col('--sub'),
                            formatter: v => pct ? v + '%' : (v >= 10000 ? Math.round(v / 1000) + 'k' : v) },
               splitLine: { lineStyle: { color: col('--line') } } },
      tooltip: { trigger: 'axis', confine: true,
                 valueFormatter: v => pct ? (v == null ? '—' : v + '%') : (v == null ? '—' : Number(v).toLocaleString()) },
      series: [{ type: 'line', data: vals, smooth: !few, showSymbol: few, symbolSize: 5,
                 lineStyle: { width: 2, color: col('--accent') },
                 itemStyle: { color: col('--accent') },
                 areaStyle: { opacity: 0.08, color: col('--accent') } }],
    });
  } catch (e) {
    box.innerHTML = '<div class="kpi-loading">추이 로드 실패</div>';
  }
}

async function loadKpiPanel() {
  _kpiCloseChart();   // 열린 미니차트 dispose (innerHTML 교체 전 인스턴스 정리)
  const scroll = document.getElementById('kpiScroll');
  scroll.innerHTML = '<div class="kpi-loading">로드 중…</div>';
  try {
    const res = await fetch(`/api/kpi-panel?period=${_KPI.period}&scope=${encodeURIComponent(_KPI.scope)}`);
    const json = await res.json();
    if (!json.ok) {
      scroll.innerHTML = `<div class="kpi-loading">데이터 없음: ${escapeHtml(json.error||'')}</div>`;
      return;
    }
    const row = json.row || {};
    _KPI.scopeName = (json.scope || {}).name || '';   // 패널발 질의(_kpiScopeName)의 스코프 명시에 사용
    const num = v => { const x = parseFloat(String(v == null ? '' : v).replace(/,/g, '')); return isNaN(x) ? null : x; };
    const fmtVal = (v, pct) => { const x = num(v); if (x == null) return '—';
      return pct ? (x.toFixed(1) + '%') : Math.round(x).toLocaleString(); };
    const fmtChg = (v, pp) => { const x = num(v); if (x == null) return '';
      const cls = x > 0 ? 'up' : x < 0 ? 'dn' : 'flat';
      return `<div class="kpi-card-wow ${cls}">${x >= 0 ? '+' : ''}${x.toFixed(1)}${pp ? '%p' : '%'}</div>`; };

    // 툴바 — 스코프 선택 + 기간 세그먼트 (선택은 localStorage 기억, 기본 전체·일간)
    const myCode = (typeof RAAS_USER !== 'undefined' && RAAS_USER && (RAAS_USER.my_programs || [])[0]) || '';
    const scopeOpts = _KPI_SCOPES.map(([c, l]) =>
      `<option value="${c}"${c === _KPI.scope ? ' selected' : ''}>${l}</option>`).join('')
      + (myCode && !_KPI_SCOPES.some(([c]) => c === myCode)
         ? `<option value="${escapeHtml(myCode)}"${myCode === _KPI.scope ? ' selected' : ''}>⭐ 내 관심</option>` : '');
    let html = `<div class="kpi-toolbar">
      <select class="kpi-scope-select" onchange="_kpiSetScope(this.value)">${scopeOpts}</select>
      <div class="kpi-period-seg">${['day','week','mon'].map(p =>
        `<button class="kpi-period-btn${p === _KPI.period ? ' active' : ''}" onclick="_kpiSetPeriod('${p}')">${_KPI_PERIOD_KO[p]}</button>`).join('')}</div>
    </div>
    <div class="kpi-date">${escapeHtml((json.scope || {}).name || '')} · 기준일 ${escapeHtml(json.date || '—')}</div>`;

    // 지표 그룹 카드
    for (const [title, cards] of (_KPI_CARDS[_KPI.period] || [])) {
      html += `<div class="kpi-section"><div class="kpi-section-title">${title}</div><div class="kpi-grid">`;
      html += cards.map(cd =>
        `<div class="kpi-card clickable" data-f="${cd.f}" data-l="${cd.l}" data-pct="${cd.pct ? 1 : 0}"` +
        ` onclick="_kpiToggleChart(this)" title="탭하면 추이 그래프">` +
        `<div class="kpi-card-label">${cd.l}<span class="kpi-spark-hint">〰</span></div>` +
        `<div class="kpi-card-value">${fmtVal(row[cd.f], cd.pct)}</div>${fmtChg(row[cd.c], cd.pp)}</div>`).join('');
      html += '</div></div>';
    }

    // 이상 알림 (일간·전사 뷰에서만 서버가 내려줌)
    const alerts = json.alerts || [];
    if (alerts.length) {
      html += `<div class="kpi-section"><div class="kpi-section-title">이상 알림</div>`;
      html += alerts.map(a => {
        const cls = (a.level || '').toLowerCase() || 'green';
        return `<div class="alert-item ${cls}"><div class="alert-dot ${cls}"></div>
          <div class="alert-text">${escapeHtml(a.msg || a.message || '')}</div></div>`;
      }).join('');
      html += '</div>';
    }

    // 채널별 활성사용자 (기간에 맞는 dau/wau/mau)
    const channels = json.channels || [];
    if (channels.length) {
      html += `<div class="kpi-section"><div class="kpi-section-title">채널별 활성사용자 · ${_KPI_PERIOD_KO[_KPI.period]}</div><div class="channel-chips">`;
      html += channels.map(ch =>
        `<div class="channel-chip">${escapeHtml(ch.name || ch.code)}<strong>${fmtVal(ch.value)}</strong></div>`).join('');
      html += '</div></div>';
    }

    html += `<button class="kpi-ai-btn" onclick="_kpiAiBrief()">✨ AI 요약</button>`;
    scroll.innerHTML = html;
  } catch (err) {
    scroll.innerHTML = `<div class="kpi-loading">로드 실패: ${escapeHtml(err.message)}</div>`;
  }
}

document.getElementById('btnRefreshKpi').addEventListener('click', loadKpiPanel);

// ────────────────────────────────────────────────
// THEME
// ────────────────────────────────────────────────
const SVG_SUN  = `<svg width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>`;
const SVG_MOON = `<svg width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>`;

function _applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const btn = document.getElementById('btnTheme');
  if (btn) btn.innerHTML = theme === 'dark' ? SVG_SUN : SVG_MOON;
  // iOS Safari는 setAttribute로 즉시 반영 안 됨 — 제거 후 재삽입으로 강제 재읽기
  const color = theme === 'dark' ? '#0d1117' : '#fbfcfd';
  let meta = document.getElementById('metaThemeColor');
  if (meta) meta.remove();
  meta = document.createElement('meta');
  meta.id = 'metaThemeColor';
  meta.name = 'theme-color';
  meta.content = color;
  document.head.appendChild(meta);
}

(function(){
  const saved = localStorage.getItem('raas_theme') || 'dark';
  _applyTheme(saved);
})();

document.getElementById('btnTheme').addEventListener('click', function(){
  const cur = document.documentElement.getAttribute('data-theme') || 'dark';
  const next = cur === 'dark' ? 'light' : 'dark';
  localStorage.setItem('raas_theme', next);
  _applyTheme(next);
});

// ────────────────────────────────────────────────
// ── 사이드바 폭 드래그 리사이즈 (데스크톱 전용) ──────────────────────────
// 6px 핸들 드래그로 조절, 클램프(좌 200–400 / 우 260–480), 더블클릭=기본폭,
// localStorage 기억. 드래그 중 transition off, 종료 시 ECharts resize.
function _initPanelResize() {
  const DEF   = { sidebar: 256, kpi: 308 };
  const CHAT_MIN = 480;   // 채팅 본문 최소 보장폭 — 우측 패널 상한은 이걸 뺀 나머지(클로드 앱 방식 비례 상한)
  const LSKEY = { sidebar: 'raas_sidebar_w', kpi: 'raas_kpi_w' };
  const CSSVAR = { sidebar: '--sidebar-w', kpi: '--kpi-w' };
  const bounds = {
    sidebar: () => [200, 440],
    kpi: () => {
      const sb = document.getElementById('sidebar');
      const sbw = (sb && !sb.classList.contains('collapsed') && !isMobile())
        ? sb.getBoundingClientRect().width : 0;
      return [260, Math.max(320, Math.round(window.innerWidth - sbw - CHAT_MIN))];
    },
  };
  const apply = (k, w) => document.documentElement.style.setProperty(CSSVAR[k], w + 'px');
  const clamp = (k, w) => {
    const [lo, hi] = bounds[k]();
    return Math.min(Math.max(w, lo), hi);
  };
  // 저장된 폭 복원
  for (const k of ['sidebar', 'kpi']) {
    const sv = parseInt(localStorage.getItem(LSKEY[k]), 10);
    if (sv) apply(k, clamp(k, sv));
  }
  // 창 크기 변화 시 우측 패널 재클램프 (넓혀둔 상태에서 창을 줄여도 채팅 최소폭 보장)
  window.addEventListener('resize', () => {
    if (isMobile()) return;
    const kp = document.getElementById('kpiPanel');
    if (!kp || kp.classList.contains('collapsed')) return;
    const w = Math.round(kp.getBoundingClientRect().width);
    const c = clamp('kpi', w);
    if (c !== w) apply('kpi', c);
  });
  function attach(handleId, key, panelSel, dir) {   // dir: 사이드바=+1(오른쪽으로 넓힘), KPI=-1
    const h = document.getElementById(handleId);
    const panel = document.querySelector(panelSel);
    if (!h || !panel) return;
    let dragging = false, startX = 0, startW = 0;
    h.addEventListener('pointerdown', e => {
      if (isMobile()) return;
      dragging = true; startX = e.clientX;
      startW = panel.getBoundingClientRect().width;
      document.body.classList.add('panel-resizing');
      h.setPointerCapture(e.pointerId);
      e.preventDefault();
    });
    h.addEventListener('pointermove', e => {
      if (!dragging) return;
      apply(key, clamp(key, Math.round(startW + dir * (e.clientX - startX))));
    });
    const end = () => {
      if (!dragging) return;
      dragging = false;
      document.body.classList.remove('panel-resizing');
      localStorage.setItem(LSKEY[key], String(Math.round(panel.getBoundingClientRect().width)));
      if (key === 'kpi' && _kpiMiniChart) { try { _kpiMiniChart.resize(); } catch (_) {} }
    };
    h.addEventListener('pointerup', end);
    h.addEventListener('pointercancel', end);
    h.addEventListener('dblclick', () => {
      apply(key, DEF[key]);
      localStorage.removeItem(LSKEY[key]);
      if (key === 'kpi' && _kpiMiniChart) { try { _kpiMiniChart.resize(); } catch (_) {} }
    });
  }
  attach('sidebarResize', 'sidebar', '#sidebar', +1);
  attach('kpiResize', 'kpi', '#kpiPanel', -1);
}

// INIT
// ────────────────────────────────────────────────
_cleanQCache();
_initWelcomeChips();
_initSidebarQueries();
_initPanelResize();
_populateRoleDropdowns();   // 가입 폼 드롭다운 즉시 채움 (인증 불필요)
_bootPostHog();             // PostHog init (graceful — 키 없으면 no-op)

// 인증 게이트 — 토큰 복원 후 성공 시만 KPI/이력/추천 칩 로드
(async () => {
  const authed = await _bootAuth();
  if (authed) {
    _checkDataVersion();
    loadKpiPanel();
    refreshHistory();
    loadSuggestions();   // 직무 기반 추천 칩
    _phIdentify(RAAS_USER);  // 세션 복원 사용자 식별
  }
})();
