
const { ipcRenderer } = require('electron');

const API = 'http://localhost:5000';
const REQUEST_POLL_MS = 3000;
const SESSION_POLL_MS = 1500;

// ─── STATE ────────────────────────────────────────────────────────────────────
const state = {
  nodeId: null,
  balance: 0,
  tasksCompleted: 0,
  sessionStart: Date.now(),
  peers: Math.floor(140 + Math.random() * 80),
  activeTask: null,
  uptimeInterval: null,
  metricsInterval: null,
  pingInterval: null,
  currentView: 'auth',
  authToken: null,
  user: null,
  trackingToken: null,
  hardwareInfo: null,
  // WebRTC / DB-polled signaling state
  peerConnection: null,
  localStream: null,
  inputChannel: null,
  isStreaming: false,
  pendingRequestId: null,
  requestPollTimer: null,
  sessionPollTimer: null,
  hostIceCount: 0,
};

// ─── WEBRTC CONFIG ────────────────────────────────────────────────────────────
const STUN_SERVERS = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
  ],
};

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function getSession() {
  try {
    const token = localStorage.getItem('xc_auth_token');
    const nodeId = localStorage.getItem('xc_node_id');
    const isProv = localStorage.getItem('xc_is_provisioned') === 'true';
    return [token, nodeId, isProv];
  } catch {
    return [null, null, false];
  }
}

function setSession(token, nodeId, isProvisioned) {
  if (token) localStorage.setItem('xc_auth_token', token);
  if (nodeId) localStorage.setItem('xc_node_id', nodeId);
  localStorage.setItem('xc_is_provisioned', isProvisioned ? 'true' : 'false');
}

function apiReq(path, method = 'GET', body = null, auth = false) {
  const cleanPath = path.startsWith('/api') ? path : `/api${path.startsWith('/') ? path : '/' + path}`;
  const headers = { 'Content-Type': 'application/json' };
  
  if (auth && state.authToken) {
    headers['Authorization'] = 'Bearer ' + state.authToken;
  }
  
  const opts = { method, headers };
  if (body) opts.body = JSON.stringify(body);
  return fetch(API + cleanPath, opts).then(r => r.json());
}

function showError(msg, containerId) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.textContent = msg;
  el.style.display = 'block';
  setTimeout(() => { el.style.display = 'none'; }, 4000);
}

// ─── STREAM REQUEST POLLING ───────────────────────────────────────────────────
function startRequestPolling() {
  if (!state.nodeId) return;
  stopRequestPolling();
  setStreamStatus('STANDBY — NO ACTIVE CLIENT');

  const poll = async () => {
    if (state.pendingRequestId) return;
    try {
      const res = await apiReq(`/nodes/${state.nodeId}/requests/pending`, 'GET', null, true);
      if (res.request) {
        state.pendingRequestId = res.request._id;
        showStreamRequestModal(res.request.requesterInfo || {}, res.request._id);
      }
    } catch (_) {}
  };
  poll();
  state.requestPollTimer = setInterval(poll, REQUEST_POLL_MS);
}

function stopRequestPolling() {
  if (state.requestPollTimer) {
    clearInterval(state.requestPollTimer);
    state.requestPollTimer = null;
  }
}

function stopSessionPolling() {
  if (state.sessionPollTimer) {
    clearInterval(state.sessionPollTimer);
    state.sessionPollTimer = null;
  }
}

function startSessionPolling(requestId) {
  stopSessionPolling();
  state.hostIceCount = 0;

  const poll = async () => {
    try {
      const res = await apiReq(`/stream/${requestId}`, 'GET', null, true);
      const request = res.request;
      if (!request) return;

      if (request.status === 'ended') {
        endStreamSession();
        return;
      }

      // 1. Process SDP Answer First
      if (request.answer && state.peerConnection && !state.peerConnection.currentRemoteDescription) {
        try {
          await state.peerConnection.setRemoteDescription(new RTCSessionDescription(request.answer));
          console.log('[RTC] Remote description (answer) successfully applied.');
        } catch (e) {
          console.error('[RTC] Failed to set remote answer:', e);
          setStreamStatus('RTC ERROR: ' + e.message);
          return; // Abort this polling cycle immediately to retry cleanly next tick
        }
      }

      // 2. CRITICAL FIX: Only ingest candidates if the underlying connection engine is ready
      if (state.peerConnection && state.peerConnection.currentRemoteDescription) {
        const clientIce = request.clientIce || [];

        for (let i = state.hostIceCount; i < clientIce.length; i++) {
          try {
            await state.peerConnection.addIceCandidate(new RTCIceCandidate(clientIce[i]));
            console.log(`[ICE] Successfully bound client network candidate entries [Index: ${i}]`);
          } catch (e) {
            console.warn(`[ICE] Retrying candidate slot ${i} later. Mapping failed:`, e.message);
          }
        }
        // Only advance tracking markers once candidates are evaluated against an active remote context
        state.hostIceCount = clientIce.length;
      } else {
        console.log('[ICE] Sync blocked: Awaiting remote answer verification before extracting mobile ICE arrays.');
      }
    } catch (_) {}
  };
  poll();
  state.sessionPollTimer = setInterval(poll, SESSION_POLL_MS);
}

// ─── STREAM REQUEST MODAL (PC SIDE) ──────────────────────────────────────────
function showStreamRequestModal(requesterInfo, requestId) {
  const modal = document.getElementById('stream-request-modal');
  if (!modal) return;

  const emailEl = document.getElementById('sr-client-email');
  const timeEl = document.getElementById('sr-request-time');
  if (emailEl) emailEl.textContent = requesterInfo.email || 'Anonymous Operator';
  if (timeEl) timeEl.textContent = new Date().toLocaleTimeString();

  modal.classList.add('open');

  const autoRejectTimer = setTimeout(() => {
    rejectStreamRequest();
  }, 60000);
  modal._autoRejectTimer = autoRejectTimer;
}

async function acceptStreamRequest() {
  const modal = document.getElementById('stream-request-modal');
  if (modal) {
    clearTimeout(modal._autoRejectTimer);
    modal.classList.remove('open');
  }
  if (!state.pendingRequestId) return;
  const requestId = state.pendingRequestId;
  try {
    await apiReq(`/stream/${requestId}/respond`, 'POST', { action: 'accept' }, true);
  } catch (e) {
    console.error('[Stream] Accept failed:', e);
    state.pendingRequestId = null;
    return;
  }
  showStreamingActiveOverlay();
  initiateHostOffer(requestId);
}

async function rejectStreamRequest() {
  const modal = document.getElementById('stream-request-modal');
  if (modal) {
    clearTimeout(modal._autoRejectTimer);
    modal.classList.remove('open');
  }
  if (!state.pendingRequestId) return;
  const requestId = state.pendingRequestId;
  state.pendingRequestId = null;
  apiReq(`/stream/${requestId}/respond`, 'POST', { action: 'reject' }, true).catch(() => {});
}

// ─── WEBRTC HOST: INITIATE OFFER (HOST-SIDE) ─────────────────────────────────
async function initiateHostOffer(requestId) {
  try {
    if (state.peerConnection) {
      state.peerConnection.close();
      state.peerConnection = null;
    }

    const stream = await captureScreen();
    setStreamStatus('SCREEN CAPTURED — BUILDING PEER CONNECTION...');

    const pc = new RTCPeerConnection(STUN_SERVERS);
    state.peerConnection = pc;
    state.pendingRequestId = requestId;

    stream.getTracks().forEach(track => pc.addTrack(track, stream));

    // CRITICAL ARCHITECTURAL FIX: Host is the offeror, so Host MUST create the channel!
    console.log('[RTC] Creating host-initiated input data channel...');
    const hostChannel = pc.createDataChannel('inputChannel', {
      ordered: false,          // Out-of-order delivery = extreme performance for mouse/keys
      maxRetransmits: 0        // Drop lost input packets instantly; never buffer old coordinates
    });

    // Immediately wire up the input channel listeners locally
    setupInputChannel(hostChannel);

    pc.onnegotiationneeded = () => {
      pc.getSenders().forEach(sender => {
        if (sender.track?.kind === 'video') {
          const params = sender.getParameters();
          if (!params.encodings) params.encodings = [{}];
          params.encodings[0].priority = 'high';
          params.encodings[0].networkPriority = 'high';
          params.encodings[0].degradationPreference = 'maintain-framerate';
          params.encodings[0].maxBitrate = 8000000; 
          sender.setParameters(params).catch(() => {});
        }
      });
    };

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        apiReq(`/stream/${requestId}/ice`, 'POST', { role: 'host', candidate: event.candidate }, true).catch(() => {});
      }
    };

    pc.oniceconnectionstatechange = () => {
      const s = pc.iceConnectionState;
      console.log('[RTC] ICE state:', s);
      setStreamStatus('ICE: ' + s.toUpperCase());
      if (s === 'connected' || s === 'completed') {
        setStreamStatus('STREAMING ACTIVE');
        state.isStreaming = true;
        updateNodeStreamUI(true);
        apiReq('/nodes/heartbeat', 'POST', { nodeId: state.nodeId, status: 'streaming', metrics: {} }, true).catch(() => {});
      }
      if (s === 'disconnected' || s === 'failed') {
        endStreamSession();
      }
    };

    pc.ondatachannel = (event) => {
      console.log('[RTC] Remote side attempted to spawn data channel:', event.channel.label);
      if (event.channel.label === 'inputChannel') {
        setupInputChannel(event.channel);
      }
    };

    const offer = await pc.createOffer({
      offerToReceiveVideo: false, 
      offerToReceiveAudio: false,
    });
    await pc.setLocalDescription(offer);

    await apiReq(`/stream/${requestId}/offer`, 'POST', { offer }, true);
    setStreamStatus('SDP OFFER SENT — AWAITING CLIENT ANSWER...');
    console.log('[RTC] Host offer sent for request:', requestId);

    startSessionPolling(requestId);
  } catch (err) {
    console.error('[RTC] initiateHostOffer failed:', err);
    setStreamStatus('CAPTURE ERROR: ' + err.message);
    hideStreamingActiveOverlay();
  }
}

// FIX: Bypasses native module initialization failures inside renderer context
async function captureScreen() {
  try {
    // Invoke secure IPC handler on the main process to grab system screen streams securely
    const sources = await ipcRenderer.invoke('get-screen-sources');

    const screen = sources.find(s => s.name === 'Entire Screen' || s.name === 'Screen 1') || sources[0];
    if (!screen) throw new Error('No capture source found');

    const stream = await navigator.mediaDevices.getUserMedia({
      audio: false,
      video: {
        mandatory: {
          chromeMediaSource: 'desktop',
          chromeMediaSourceId: screen.id,
          maxWidth: 1920,
          maxHeight: 1080,
          maxFrameRate: 30,
        }
      }
    });

    state.localStream = stream;
    return stream;
  } catch (err) {
    console.error('[Capture] Screen capture failed:', err);
    throw err;
  }
}

// ─── INPUT CHANNEL: OS-LEVEL INPUT SIMULATION ─────────────────────────────────
function setupInputChannel(channel) {
  state.inputChannel = channel;
  channel.onopen = () => console.log('[DC] Input channel open');
  channel.onclose = () => console.log('[DC] Input channel closed');

  channel.onmessage = (event) => {
    let pkt;
    try { pkt = JSON.parse(event.data); } catch { return; }
    ipcRenderer.send('simulate-input', pkt);
  };
}

// ─── STREAM SESSION MANAGEMENT ────────────────────────────────────────────────
function showStreamingActiveOverlay() {
  const overlay = document.getElementById('streaming-active-overlay');
  if (overlay) overlay.classList.add('visible');
}

function hideStreamingActiveOverlay() {
  const overlay = document.getElementById('streaming-active-overlay');
  if (overlay) overlay.classList.remove('visible');
}

function endStreamSession() {
  if (state.peerConnection) {
    state.peerConnection.close();
    state.peerConnection = null;
  }
  if (state.localStream) {
    state.localStream.getTracks().forEach(t => t.stop());
    state.localStream = null;
  }
  stopSessionPolling();
  state.isStreaming = false;

  const requestId = state.pendingRequestId;
  state.pendingRequestId = null;
  hideStreamingActiveOverlay();
  setStreamStatus('STANDBY — NO ACTIVE CLIENT');
  updateNodeStreamUI(false);

  if (requestId) {
    apiReq(`/stream/${requestId}/end`, 'POST', {}, true).catch(() => {});
  }
  apiReq('/nodes/heartbeat', 'POST', { nodeId: state.nodeId, status: 'idle', metrics: null }, true).catch(() => {});
  console.log('[RTC] Stream session ended');
}

function setStreamStatus(text) {
  const el = document.getElementById('stream-status-text');
  if (el) el.textContent = text;
}

function updateNodeStreamUI(isStreaming) {
  const dot = document.querySelector('.node-dot');
  const statusText = document.querySelector('.node-status-text');
  if (dot) dot.style.background = isStreaming ? '#ff6432' : '';
  if (statusText) statusText.textContent = isStreaming ? 'STREAMING ACTIVE' : 'NODE ONLINE';
}

function setBtn(id, text, disabled = false) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.disabled = disabled;
  el.style.opacity = disabled ? '0.6' : '1';
}

document.addEventListener('DOMContentLoaded', async () => {
  const minBtn = document.getElementById('tb-min');
  const maxBtn = document.getElementById('tb-max');
  const closeBtn = document.getElementById('tb-close');
  if (minBtn) minBtn.addEventListener('click', () => ipcRenderer.send('win-minimize'));
  if (maxBtn) maxBtn.addEventListener('click', () => ipcRenderer.send('win-maximize'));
  if (closeBtn) closeBtn.addEventListener('click', () => ipcRenderer.send('win-close'));

  try {
    state.hardwareInfo = await ipcRenderer.invoke('get-hardware-info');
  } catch (e) {
    state.hardwareInfo = { fingerprint: 'fallback-' + Date.now(), platform: process.platform, arch: process.arch, cpuModel: 'Unknown', cores: 4, totalRamGB: 8 };
  }

  function showView(id) {
    document.querySelectorAll('.view').forEach(v => { v.classList.remove('active'); v.style.display = 'none'; });
    const target = document.getElementById('view-' + id);
    if (!target) return;
    target.classList.add('active');
    target.style.display = '';
    state.currentView = id;
    const showSidebar = !['auth', 'onboard'].includes(id);
    const sidebar = document.getElementById('sidebar');
    if (sidebar) sidebar.style.display = showSidebar ? 'flex' : 'none';
    setTimeout(() => initReveal(target), 60);
  }

  document.querySelectorAll('.nav-item[data-view]').forEach(item => {
    item.addEventListener('click', () => {
      document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
      item.classList.add('active');
      showView(item.dataset.view);
    });
  });

  // Reveal animations
  function initReveal(container) {
    const els = (container || document).querySelectorAll('.reveal, .reveal-l, .reveal-r');
    const obs = new IntersectionObserver(entries => {
      entries.forEach(e => { if (e.isIntersecting) { e.target.classList.add('in'); obs.unobserve(e.target); } });
    }, { threshold: 0.06, rootMargin: '0px 0px -30px 0px' });
    els.forEach(el => obs.observe(el));
  }

  document.querySelectorAll('.auth-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.auth-tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.auth-panel').forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      const panelId = 'auth-' + tab.dataset.tab;
      const targetPanel = document.getElementById(panelId);
      if (targetPanel) targetPanel.classList.add('active');
    });
  });

  function setupOtp(cells) {
    cells.forEach((cell, i) => {
      cell.addEventListener('input', () => { if (cell.value && i < cells.length - 1) cells[i + 1].focus(); });
      cell.addEventListener('keydown', e => { if (e.key === 'Backspace' && !cell.value && i > 0) cells[i - 1].focus(); });
      cell.addEventListener('paste', e => {
        const data = e.clipboardData.getData('text').replace(/\D/g, '').slice(0, 6);
        cells.forEach((c, j) => { if (data[j]) c.value = data[j]; });
        e.preventDefault();
      });
    });
  }

  const signinCells = document.querySelectorAll('#auth-signin .otp-grid .otp-cell');
  if (signinCells.length) setupOtp([...signinCells]);
  const registerCells = document.querySelectorAll('#auth-register .otp-grid .otp-cell');
  if (registerCells.length) setupOtp([...registerCells]);

  document.querySelectorAll('.auth-panel').forEach(panel => {
    const btn = panel.querySelector('.pw-toggle');
    const inputEl = panel.querySelector('input[type="password"]') || panel.querySelector('input[type="text"]');
    if (!btn || !inputEl) return;
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      const isHidden = inputEl.type === 'password';
      inputEl.type = isHidden ? 'text' : 'password';
      btn.querySelector('svg').innerHTML = isHidden
        ? `<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/>`
        : `<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>`;
    });
  });

  // ─── AUTH FLOWS ────────────────────────────────────────────────────────────
  async function bootUserSession(token, user) {
    state.authToken = token;
    state.user = user;
    
    const sbNodeId = document.getElementById('sidebar-node-id');
    if (user.isProvisioned) {
      setSession(token, state.nodeId, true);
      await loadUserData();
      if (state.nodeId && sbNodeId) sbNodeId.textContent = 'NODE_ID: ' + state.nodeId;
      showView('home');
      document.querySelectorAll('.nav-item[data-view]').forEach(n => n.classList.remove('active'));
      const homeNav = document.querySelector('.nav-item[data-view=\"home\"]');
      if (homeNav) homeNav.classList.add('active');
      bootWorkspace();
      if (window._setNodeOnlineState) window._setNodeOnlineState(true);
    } else {
      setSession(token, null, false);
      if (sbNodeId) sbNodeId.textContent = 'NODE_ID: PENDING';
      autoFillHardwareForm();
      showView('onboard');
      setTimeout(() => initReveal(document.getElementById('view-onboard')), 80);
    }
  }

  async function loadUserData() {
    try {
      const data = await apiReq('/wallet/ledger', 'GET', null, true);
      state.balance = data.balance || 0;
      state.tasksCompleted = data.tasksCompleted || 0;
      syncWalletDisplay();
      syncDevView();
      buildLedger(data.entries || []);
      
      const meData = await apiReq('/api/auth/me', 'GET', null, true);
      if (meData.node) {
        state.nodeId = meData.node.nodeId;
        setSession(state.authToken, state.nodeId, true);
        const sbNodeId = document.getElementById('sidebar-node-id');
        if (sbNodeId) sbNodeId.textContent = 'NODE_ID: ' + state.nodeId;
      }
    } catch (e) { console.error('loadUserData error', e); }
  }

  function autoFillHardwareForm() {
    if (!state.hardwareInfo) return;
    const hw = state.hardwareInfo;
    const cpuInput = document.querySelector('#view-onboard input[placeholder*="Core Ultra"]');
    const coresInput = document.querySelector('#view-onboard input[type="number"][placeholder*="24"]');
    if (cpuInput) cpuInput.value = hw.cpuModel;
    if (coresInput) coresInput.value = hw.cores;
  }

  const signinBtn = document.getElementById('btn-signin');
  if (signinBtn) {
    signinBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      const email = document.getElementById('auth-email')?.value?.trim();
      const password = document.getElementById('auth-password')?.value;
      if (!email || !password) return showError('Email and password required', 'signin-error');
      
      setBtn('btn-signin', 'AUTHENTICATING...', true);
      const res = await apiReq('/api/auth/login', 'POST', { email, password });
      setBtn('btn-signin', 'AUTHENTICATE OPERATOR →', false);
      
      if (res.error) return showError(res.error, 'signin-error');
      await bootUserSession(res.token, res.user);
    });
  }

  const registerBtn = document.getElementById('btn-register');
  if (registerBtn) {
    registerBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      const emailInput = document.querySelector('#auth-register input[type="email"]');
      const pwInput = document.getElementById('reg-password');
      const email = emailInput?.value?.trim();
      const password = pwInput?.value;
      if (!email || !password) return showError('Email and password required', 'register-error');
      
      setBtn('btn-register', 'CREATING ACCOUNT...', true);
      const res = await apiReq('/api/auth/register', 'POST', { email, password });
      setBtn('btn-register', 'CREATE OPERATOR ACCOUNT →', false);
      
      if (res.error) return showError(res.error, 'register-error');
      await bootUserSession(res.token, res.user);
    });
  }

  const googleBtn = document.getElementById('btn-google');
  if (googleBtn) {
    googleBtn.addEventListener('click', (e) => {
      e.preventDefault();
      const { shell } = require('electron');
      shell.openExternal(API + '/api/auth/google');
    });
  }

  // ─── PROVISION COMPUTE NODE HANDLER ─────────────────────────────────────────
  const provisionBtn = document.getElementById('btn-provision');
  if (provisionBtn) {
    provisionBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      setBtn('btn-provision', 'PROVISIONING NODE...', true);
      
      const deviceModel = document.querySelector('#view-onboard input[placeholder*="Custom Build"]')?.value || 'Custom Build';
      const systemAge = document.querySelector('#view-onboard .form-select')?.value || 'Unknown';
      const cpuArch = document.querySelector('input[name="cpu-arch"]:checked')?.value || 'intel';
      const cpuModel = document.querySelector('#view-onboard input[placeholder*="Core Ultra"]')?.value || state.hardwareInfo?.cpuModel || 'Unknown CPU';
      const cores = parseInt(document.querySelector('#view-onboard input[type="number"][placeholder*="24"]')?.value) || state.hardwareInfo?.cores || 4;
      
      const ramSel = document.querySelectorAll('#view-onboard .form-select');
      const totalRamGB = ramSel[1]?.value || '16 GB';
      const ramFrequency = document.querySelector('#view-onboard input[placeholder*="6400"]')?.value || '';
      const ramGen = document.querySelector('input[name="ram-gen"]:checked')?.value || 'ddr4';
      
      const gpuBrand = document.querySelector('input[name="gpu-brand"]:checked')?.value || 'nvidia';
      const gpuModel = document.querySelector('#view-onboard input[placeholder*="RTX"]')?.value || 'Unknown GPU';
      const vramSize = ramSel[2]?.value || '8 GB';
      const vramGen = document.querySelector('input[name="vram-gen"]:checked')?.value || 'gddr6';
      
      const localFingerprint = state.hardwareInfo?.fingerprint || 'fallback-' + Date.now();
      
      const hardwareSpecs = { 
        deviceModel, 
        systemAge, 
        platform: state.hardwareInfo?.platform || process.platform, 
        arch: state.hardwareInfo?.arch || process.arch, 
        cpuArch, 
        cpuModel, 
        cores, 
        totalRamGB, 
        ramFrequency, 
        ramGen, 
        gpuBrand, 
        gpuModel, 
        vramSize, 
        vramGen,
        fingerprint: localFingerprint 
      };
      
      const xeroScaleScore = computeXeroScale(hardwareSpecs);
      
      const res = await apiReq('/nodes/provision', 'POST', { specs: hardwareSpecs, xeroScaleScore }, true);
      
      if (res.error) { 
        setBtn('btn-provision', 'PROVISION HOST COMPUTE NODE', false); 
        return showError(res.error, 'provision-error'); 
      }
      
      state.nodeId = res.node?.nodeId || res.nodeId;
      setSession(state.authToken, state.nodeId, true);
      
      const sbNodeId = document.getElementById('sidebar-node-id');
      if (sbNodeId) sbNodeId.textContent = 'NODE_ID: ' + state.nodeId;
      setBtn('btn-provision', '✓ NODE PROVISIONED', false);
      
      setTimeout(async () => {
        await loadUserData();
        showView('home');
        document.querySelectorAll('.nav-item[data-view]').forEach(n => n.classList.remove('active'));
        const homeNav = document.querySelector('.nav-item[data-view=\"home\"]');
        if (homeNav) homeNav.classList.add('active');
        bootWorkspace();
        if (window._setNodeOnlineState) window._setNodeOnlineState(true);
      }, 1200);
    });
  }

  // ─── STREAM REQUEST MODAL BUTTONS ─────────────────────────────────────────
  const srAcceptBtn = document.getElementById('sr-accept-btn');
  const srRejectBtn = document.getElementById('sr-reject-btn');
  if (srAcceptBtn) srAcceptBtn.addEventListener('click', acceptStreamRequest);
  if (srRejectBtn) srRejectBtn.addEventListener('click', rejectStreamRequest);

  const streamKillBtn = document.getElementById('stream-emergency-kill');
  if (streamKillBtn) {
    streamKillBtn.addEventListener('click', () => {
      if (confirm('EMERGENCY STOP: End stream immediately?')) {
        endStreamSession();
      }
    });
  }

  // ─── NODE ONLINE/OFFLINE TOGGLE ────────────────────────────────────────────
  let nodeIsOnline = false;

  function setNodeOnlineState(online) {
    nodeIsOnline = online;
    const toggleBtn = document.getElementById('node-online-toggle');
    const statusDot = document.querySelector('.node-dot');
    const statusText = document.querySelector('.node-status-text');
    const sidebarDot = document.querySelector('.sidebar-status-dot');

    if (toggleBtn) {
      toggleBtn.textContent = online ? '⏹ TAKE NODE OFFLINE' : '▶ PUT NODE ONLINE';
      toggleBtn.classList.toggle('toggle-online', online);
      toggleBtn.classList.toggle('toggle-offline', !online);
    }
    if (statusDot) statusDot.style.background = online ? 'var(--cyan)' : '#666';
    if (statusText) statusText.textContent = online ? 'NODE ONLINE' : 'NODE OFFLINE';
    if (sidebarDot) sidebarDot.style.background = online ? 'var(--cyan)' : '#666';

    if (online) {
      startRequestPolling();
      startHeartbeat();
    } else {
      stopRequestPolling();
      if (state.pingInterval) {
        clearInterval(state.pingInterval);
        state.pingInterval = null;
      }
      if (state.nodeId) {
        apiReq('/nodes/set-status', 'POST', { nodeId: state.nodeId, status: 'offline' }, true).catch(() => {});
      }
    }
  }

  const nodeToggleBtn = document.getElementById('node-online-toggle');
  if (nodeToggleBtn) {
    nodeToggleBtn.addEventListener('click', () => {
      setNodeOnlineState(!nodeIsOnline);
    });
  }

  window._setNodeOnlineState = setNodeOnlineState;

  function computeXeroScale(specs) {
    let score = 1000;
    score += (parseInt(specs.cores) || 4) * 150;
    score += (parseInt(specs.totalRamGB) || 8) * 20;
    if (specs.gpuBrand === 'nvidia') score += 800;
    else if (specs.gpuBrand === 'amd') score += 600;
    score += (parseInt(specs.vramSize) || 4) * 40;
    if (specs.ramGen === 'ddr5') score += 300;
    if (specs.vramGen === 'gddr6') score += 200;
    return Math.min(9999, Math.max(500, Math.round(score + Math.random() * 300)));
  }

  // ─── HEARTBEAT ────────────────────────────────────────────────────────────
  function startHeartbeat() {
    if (state.pingInterval) clearInterval(state.pingInterval);
    const sendPing = () => {
      const cpu = Math.floor(18 + Math.random() * 55);
      const freq = parseFloat((2.8 + Math.random() * 2.0).toFixed(1));
      const net = Math.floor(22 + Math.random() * 258);
      apiReq('/nodes/heartbeat', 'POST', { nodeId: state.nodeId, metrics: { cpuUsagePct: cpu, frequencyGHz: freq, networkSpeedMBs: net, syncOptimal: true } }, true).catch(() => {});
    };
    sendPing();
    state.pingInterval = setInterval(sendPing, 20000);
  }

  // ─── WORKSPACE BOOT ────────────────────────────────────────────────────────
  function bootWorkspace() {
    buildAdBanner();
    buildComputeQueue();
    startMetrics();
    startUptime();
    syncWalletDisplay();
    syncDevView();
  }

  const adItems = [
    { tag: 'STREAMING', text: 'XeroStream v1 — P2P Cloud Rendering Now Live on Mesh Network' },
    { tag: 'NEW TOOL', text: 'XeroSync v2 — Distributed DB Replication Across Mesh Nodes' },
    { tag: 'UPGRADE', text: 'Boost your node yield rate by 30% with XeroBoost Turbo Mode' },
    { tag: 'ALERT', text: 'South-Asia-1 cluster peak demand — 2x yield multiplier active now' },
    { tag: 'FEATURE', text: 'GPU Compute Tasks now available — 5x base reward rate unlocked' },
    { tag: 'NETWORK', text: 'XeroCloud Mesh v2.4 protocol deployed — latency reduced by 40%' },
    { tag: 'REWARD', text: 'Referral Program live — earn 200 XC for every provisioned operator' },
    { tag: 'MARKET', text: 'AI inference task surge — 3x volume increase in compute queue' },
  ];

  function buildAdBanner() {
    const track = document.getElementById('ad-track');
    if (!track) return;
    const full = [...adItems, ...adItems];
    track.innerHTML = full.map(a => `<div class="ad-item"><span class="ad-item-tag">${a.tag}</span><span class="ad-item-text">${a.text}</span></div><span class="ad-divider">◆</span>`).join('');
  }

  const taskTypes = ['RENDER', 'AI-INFER', 'ML-TRAIN', 'HASH-OPS', 'PHYSICS-SIM', 'DATA-PROC', 'COMPRESS', 'ENCODE'];
  function genTaskId() { return 'TASK-' + Math.random().toString(36).substring(2, 10).toUpperCase(); }
  function formatDuration(mins) {
    if (mins < 60) return `${mins} Minutes`;
    const h = Math.floor(mins / 60), m = mins % 60;
    return m > 0 ? `${h}h ${m}m` : `${h} Hours`;
  }
  function genTask() {
    const mins = [15, 22, 30, 45, 48, 60, 72, 90, 120, 150, 180, 240][Math.floor(Math.random() * 12)];
    const xc = Math.floor(mins / 60 * 30 + Math.random() * 20 + 5);
    return { id: genTaskId(), type: taskTypes[Math.floor(Math.random() * taskTypes.length)], duration: mins, yield: xc, difficulty: ['LOW', 'MED', 'HIGH'][Math.floor(Math.random() * 3)] };
  }

  const tasks = Array.from({ length: 6 }, genTask);

  function buildComputeQueue() {
    const queue = document.getElementById('compute-queue');
    if (!queue) return;
    const qCount = document.getElementById('queue-count');
    if (qCount) qCount.textContent = tasks.length + ' TASKS PENDING';
    queue.innerHTML = tasks.map((t, i) => `
      <div class="task-card reveal" style="transition-delay:${i * 0.07}s" data-task-idx="${i}">
        <div class="task-type-badge">${t.type}</div>
        <div class="task-card-inner">
          <div class="task-id">${t.id}</div>
          <div class="task-meta">
            <div class="task-meta-cell"><div class="label">DURATION</div><div class="value">${formatDuration(t.duration)}</div></div>
            <div class="task-meta-cell"><div class="label">DIFFICULTY</div><div class="value">${t.difficulty}</div></div>
          </div>
          <div class="task-reward">
            <div><div class="task-reward-val">${t.yield} XC</div><div class="task-reward-unit">$${(t.yield * 2).toFixed(2)} USD</div></div>
            <div class="task-accept-btn">ACCEPT →</div>
          </div>
        </div>
      </div>`).join('');
    document.querySelectorAll('.task-card').forEach(card => {
      card.addEventListener('click', () => { state.activeTask = tasks[parseInt(card.dataset.taskIdx)]; openTaskModal(state.activeTask); });
    });
    setTimeout(() => initReveal(document.getElementById('view-home')), 50);
  }

  function openTaskModal(task) {
    const miId = document.getElementById('mi-id');
    const miDur = document.getElementById('mi-dur');
    const miYield = document.getElementById('mi-yield');
    const modal = document.getElementById('task-modal');
    if (miId) miId.textContent = task.id;
    if (miDur) miDur.textContent = formatDuration(task.duration);
    if (miYield) miYield.textContent = task.yield + ' XC';
    if (modal) modal.classList.add('open');
  }

  const cancelBtn = document.getElementById('modal-cancel');
  const authBtn = document.getElementById('modal-auth');
  const modalWrap = document.getElementById('task-modal');
  if (cancelBtn) { cancelBtn.addEventListener('click', () => { if (modalWrap) modalWrap.classList.remove('open'); state.activeTask = null; }); }
  if (authBtn) {
    authBtn.addEventListener('click', async () => {
      if (modalWrap) modalWrap.classList.remove('open');
      if (state.activeTask) {
        const task = state.activeTask;
        state.balance += task.yield;
        state.tasksCompleted++;
        syncWalletDisplay();
        syncDevView();
        state.activeTask = null;
        apiReq('/nodes/task/complete', 'POST', { taskId: task.id, taskType: task.type, duration: formatDuration(task.duration), earnedXC: task.yield }, true).catch(() => {});
      }
    });
  }
  if (modalWrap) { modalWrap.addEventListener('click', e => { if (e.target === modalWrap) modalWrap.classList.remove('open'); }); }

  function randBetween(a, b) { return Math.floor(a + Math.random() * (b - a)); }
  function startMetrics() {
    if (state.metricsInterval) clearInterval(state.metricsInterval);
    updateMetrics();
    state.metricsInterval = setInterval(updateMetrics, 2000);
  }
  function updateMetrics() {
    const cpu = randBetween(18, 72);
    const freq = (2.8 + Math.random() * 2.0).toFixed(1);
    const net = randBetween(22, 280);
    const mesh = randBetween(94, 100);
    const els = { 'm-cpu': cpu + '%', 'ms-cpu': cpu > 60 ? 'COMPUTE ACTIVE' : 'MESH IDLE', 'm-freq': freq, 'ms-freq': freq + ' GHz ACTIVE', 'm-net': net, 'ms-net': net + ' MB/s MESH', 'm-mesh': mesh + '%', 'ms-mesh': 'SYNC OPTIMAL' };
    Object.entries(els).forEach(([id, val]) => { const el = document.getElementById(id); if (el) el.textContent = val; });
    const mbCpu = document.getElementById('mb-cpu'); if (mbCpu) mbCpu.style.width = cpu + '%';
    const mbFreq = document.getElementById('mb-freq'); if (mbFreq) mbFreq.style.width = ((freq - 2.8) / 2.0 * 100) + '%';
    const mbNet = document.getElementById('mb-net'); if (mbNet) mbNet.style.width = Math.min(net / 300 * 100, 100) + '%';
    const mbMesh = document.getElementById('mb-mesh'); if (mbMesh) mbMesh.style.width = mesh + '%';
  }

  function formatUptime(ms) {
    const s = Math.floor(ms / 1000);
    return [Math.floor(s / 3600), Math.floor((s % 3600) / 60), s % 60].map(n => String(n).padStart(2, '0')).join(':');
  }
  function startUptime() {
    if (state.uptimeInterval) clearInterval(state.uptimeInterval);
    state.uptimeInterval = setInterval(() => {
      const str = formatUptime(Date.now() - state.sessionStart);
      ['sf-uptime', 'dv-uptime', 'api-uptime'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.textContent = id === 'sf-uptime' ? 'UPTIME: ' + str : str;
      });
    }, 1000);
  }

  function syncWalletDisplay() {
    const bal = state.balance;
    const usd = (bal * 2).toFixed(2);
    const map = { 'w-balance': bal.toLocaleString(), 'w-usd': '$' + usd, 'api-balance': bal, 'api-usd': '$' + usd };
    Object.entries(map).forEach(([id, val]) => { const el = document.getElementById(id); if (el) el.textContent = val; });
  }

  const payoutToggle = document.getElementById('payout-toggle');
  if (payoutToggle) { payoutToggle.addEventListener('click', () => { const d = document.getElementById('payout-drawer'); if (d) d.classList.toggle('open'); }); }

  const payoutSubmit = document.getElementById('payout-submit');
  if (payoutSubmit) {
    payoutSubmit.addEventListener('click', (e) => {
      e.preventDefault();
      const addrInput = document.getElementById('payout-addr');
      if (!addrInput) return;
      const addr = addrInput.value.trim();
      if (!addr) { addrInput.style.borderColor = '#ff3355'; return; }
      addrInput.style.borderColor = '';
      payoutSubmit.textContent = 'PROCESSING...';
      setTimeout(() => {
        payoutSubmit.textContent = '✓ SUBMITTED';
        const drawer = document.getElementById('payout-drawer');
        if (drawer) drawer.classList.remove('open');
        setTimeout(() => { payoutSubmit.textContent = 'INITIATE →'; }, 3000);
      }, 1400);
    });
  }

  function buildLedger(entries) {
    const tbody = document.getElementById('ledger-body');
    if (!tbody) return;
    if (!entries || entries.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-dim);padding:24px;letter-spacing:2px;font-size:11px">NO TRANSACTIONS YET</td></tr>';
      return;
    }
    tbody.innerHTML = entries.map(r => {
      const date = new Date(r.settledAt || r.createdAt).toISOString().split('T')[0];
      return `<tr><td>${date}</td><td style="font-family:var(--font-mono);font-size:10px;color:var(--text-dim)">${r.taskId}</td><td>${r.duration || '--'}</td><td class="ledger-earn">${r.earnedXC} XC</td><td><span class="ledger-status">${r.status}</span></td></tr>`;
    }).join('');
  }

  function syncDevView() {
    const trySet = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
    trySet('dv-nodeid', state.nodeId || '--');
    trySet('api-nodeid', state.nodeId || 'XC-MESH-0000');
    trySet('dv-peers', state.peers);
    trySet('dv-tasks', state.tasksCompleted);
    try {
      const os = require('os');
      trySet('dv-platform', os.platform() + ' ' + os.release());
      trySet('dv-arch', os.arch());
    } catch (e) {
      trySet('dv-platform', state.hardwareInfo?.platform || process.platform);
      trySet('dv-arch', state.hardwareInfo?.arch || process.arch);
    }
  }

  const bugSubmit = document.getElementById('bug-submit');
  if (bugSubmit) {
    bugSubmit.addEventListener('click', (e) => {
      e.preventDefault();
      const ta = document.querySelector('.bug-textarea');
      if (!ta || !ta.value.trim()) { if (ta) ta.style.borderColor = '#ff3355'; return; }
      ta.style.borderColor = '';
      const formWrap = document.getElementById('bug-form-wrap');
      if (formWrap) formWrap.style.display = 'none';
      const ref = 'BUG-' + Math.random().toString(36).substring(2, 10).toUpperCase();
      const bugRef = document.getElementById('bug-ref');
      if (bugRef) bugRef.textContent = ref;
      const successMsg = document.getElementById('bug-success');
      if (successMsg) successMsg.classList.add('visible');
    });
  }

  document.querySelectorAll('.view').forEach(view => { view.addEventListener('scroll', () => initReveal(view)); });

  // ─── SESSION RESTORE ──────────────────────────────────────────────────────
  const [savedToken, savedNodeId, savedIsProv] = getSession();
  if (savedToken) {
    state.authToken = savedToken;
    try {
      const meRes = await apiReq('/api/auth/me', 'GET', null, true);
      if (meRes.user) {
        state.user = meRes.user;
        if (meRes.node) {
          state.nodeId = meRes.node.nodeId;
          const sbNodeId = document.getElementById('sidebar-node-id');
          if (sbNodeId) sbNodeId.textContent = 'NODE_ID: ' + state.nodeId;
        }
        if (meRes.user.isProvisioned) {
          await loadUserData();
          showView('home');
          document.querySelectorAll('.nav-item[data-view]').forEach(n => n.classList.remove('active'));
          const homeNav = document.querySelector('.nav-item[data-view=\"home\"]');
          if (homeNav) homeNav.classList.add('active');
          bootWorkspace();
          if (window._setNodeOnlineState) window._setNodeOnlineState(true);
          return;
        } else {
          autoFillHardwareForm();
          showView('onboard');
          return;
        }
      }
    } catch (e) { 
      localStorage.clear();
    }
  }

  showView('auth');
  initReveal(document.getElementById('view-auth'));
});