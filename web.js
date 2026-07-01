/**
 * KnightBot MD - Comprehensive Web Dashboard
 * Auto-starts HTTP + WebSocket server on require()
 */

'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');
const fs   = require('fs');
const path = require('path');

// Lazy-load settings to avoid circular deps
let _settings = null;
const S = () => { if (!_settings) _settings = require('./settings'); return _settings; };

// ─── State ────────────────────────────────────────────────────────────────────
let botSocket    = null;
let botStatus    = 'offline';
let connectedAt  = null;
let messageCount = 0;
let logs         = [];
let groupList    = [];

const PORT     = process.env.PORT || 3000;
const MAX_LOGS = 300;
const DATA_DIR = path.join(__dirname, 'data');

// ─── Helpers ──────────────────────────────────────────────────────────────────
function readJSON(fp, def = null) {
    try { return JSON.parse(fs.readFileSync(fp, 'utf8')); }
    catch { return def; }
}
function writeJSON(fp, data) {
    fs.writeFileSync(fp, JSON.stringify(data, null, 2));
}
function getCommandList() {
    try {
        return fs.readdirSync(path.join(__dirname, 'commands'))
            .filter(f => f.endsWith('.js'))
            .map(f => f.replace('.js', ''))
            .sort();
    } catch { return []; }
}
function getStatusSnapshot() {
    const s   = S();
    const mem = process.memoryUsage();
    let phone = '–';
    try {
        const creds = JSON.parse(fs.readFileSync(path.join(__dirname, 'session/creds.json'), 'utf8'));
        phone = creds.me?.id?.split(':')[0]?.split('@')[0] || '–';
    } catch {}
    return {
        online: botStatus === 'online',
        phoneNumber: phone,
        messageCount,
        connectedAt,
        nodeVersion: process.version,
        memoryMB: Math.round(mem.rss / 1024 / 1024),
        groupCount: groupList.length,
        settings: {
            botName:     s.botName,
            botOwner:    s.botOwner,
            ownerNumber: s.ownerNumber,
            version:     s.version,
            commandMode: s.commandMode,
            description: s.description,
        },
        commands: getCommandList(),
    };
}
const FEATURE_FILES = {
    autoread:   'autoread.json',
    autotyping: 'autotyping.json',
    autostatus: 'autoStatus.json',
};
function getFeatures() {
    return {
        autoread:   !!(readJSON(path.join(DATA_DIR, 'autoread.json'),   { enabled: false })).enabled,
        autotyping: !!(readJSON(path.join(DATA_DIR, 'autotyping.json'), { enabled: false })).enabled,
        autostatus: !!(readJSON(path.join(DATA_DIR, 'autoStatus.json'), { enabled: false })).enabled,
    };
}

// ─── WebSocket broadcast ──────────────────────────────────────────────────────
const clients = new Set();
function broadcast(obj) {
    const msg = JSON.stringify(obj);
    for (const ws of clients) if (ws.readyState === 1) ws.send(msg);
}

// ─── Logging ──────────────────────────────────────────────────────────────────
function addLog(text, level = 'info') {
    const entry = { time: Date.now(), text, level };
    logs.push(entry);
    if (logs.length > MAX_LOGS) logs.shift();
    broadcast({ type: 'event', ...entry });
}

// ─── Body parser ──────────────────────────────────────────────────────────────
function parseBody(req) {
    return new Promise(resolve => {
        let body = '';
        req.on('data', c => { body += c; if (body.length > 8192) req.destroy(); });
        req.on('end', () => { try { resolve(JSON.parse(body)); } catch { resolve({}); } });
        req.on('error', () => resolve({}));
    });
}

// ─── HTML Dashboard (SPA) ─────────────────────────────────────────────────────
const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KnightBot MD — Dashboard</title>
<style>
:root{
  --green:#25d366;--dk-green:#128c7e;
  --bg:#0d1117;--bg2:#010409;--card:#161b22;
  --border:#30363d;--text:#e6edf3;--muted:#8b949e;
  --red:#f85149;--yellow:#d29922;--blue:#58a6ff;
  --sidebar-w:240px;
}
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,-apple-system,sans-serif;height:100vh;overflow:hidden}

/* ── Layout ── */
.app{display:flex;height:100vh}

/* ── Sidebar ── */
.sidebar{
  width:var(--sidebar-w);min-width:var(--sidebar-w);
  background:var(--card);border-right:1px solid var(--border);
  display:flex;flex-direction:column;height:100vh;overflow-y:auto;
  position:relative;z-index:50;transition:transform .3s;
}
.sb-brand{
  padding:18px 16px;border-bottom:1px solid var(--border);
  display:flex;align-items:center;gap:10px;flex-shrink:0;
}
.sb-icon{
  width:36px;height:36px;background:var(--green);border-radius:10px;
  display:flex;align-items:center;justify-content:center;font-size:1.2rem;flex-shrink:0;
}
.sb-name{font-weight:700;font-size:.95rem;color:var(--text);line-height:1.3}
.sb-ver{font-size:.7rem;color:var(--muted)}
.sb-nav{padding:10px 8px;flex:1}
.nav-item{
  display:flex;align-items:center;gap:10px;padding:9px 12px;
  border-radius:8px;cursor:pointer;color:var(--muted);
  font-size:.88rem;font-weight:500;transition:all .2s;
  margin-bottom:2px;border:none;background:none;width:100%;text-align:left;
}
.nav-item:hover{background:rgba(255,255,255,.05);color:var(--text)}
.nav-item.active{background:rgba(37,211,102,.12);color:var(--green)}
.nav-icon{font-size:1rem;width:20px;text-align:center;flex-shrink:0}
.sb-footer{padding:12px 16px;border-top:1px solid var(--border);font-size:.72rem;color:var(--muted);flex-shrink:0}
.sb-footer a{color:var(--green);text-decoration:none}

/* ── Main ── */
.main{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0}
.header{
  height:60px;background:var(--card);border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;
  padding:0 24px;flex-shrink:0;
}
.header-left{display:flex;align-items:center;gap:8px}
.header-title{font-size:.95rem;font-weight:600;color:var(--text)}
.menu-btn{display:none;background:none;border:none;color:var(--text);font-size:1.3rem;cursor:pointer;padding:4px;line-height:1}
.status-pill{
  display:flex;align-items:center;gap:6px;
  background:#1c2b22;border:1px solid var(--green);
  border-radius:20px;padding:5px 14px;
  font-size:.8rem;font-weight:600;color:var(--green);transition:all .3s;
}
.status-pill.offline{background:#2b1c1c;border-color:var(--red);color:var(--red)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--green);animation:blink 1.5s infinite}
.offline .dot{background:var(--red);animation:none}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}

/* ── Content ── */
.content{flex:1;overflow-y:auto;padding:28px 28px 48px}
.page{display:none}
.page.active{display:block}

/* ── Sections ── */
.sec-title{
  font-size:.75rem;font-weight:600;text-transform:uppercase;
  letter-spacing:.08em;color:var(--muted);
  margin-bottom:14px;margin-top:28px;
}
.sec-title:first-of-type{margin-top:0}
.sec-head{display:flex;align-items:center;gap:8px;margin-bottom:14px}
.sec-head .sec-title{margin:0}

/* ── Cards ── */
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px 22px}

/* ── Stats ── */
.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:24px}
.stat-card{
  background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:18px 20px;transition:border-color .2s;
}
.stat-card:hover{border-color:var(--green)}
.stat-icon{font-size:1.5rem;margin-bottom:10px}
.stat-label{font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}
.stat-value{font-size:1.75rem;font-weight:700;color:var(--text);line-height:1}
.stat-value.sm{font-size:1.1rem}
.stat-sub{font-size:.72rem;color:var(--muted);margin-top:5px}

/* ── Info rows ── */
.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:24px}
.info-row{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid var(--border);gap:12px}
.info-row:last-child{border-bottom:none}
.info-key{font-size:.82rem;color:var(--muted);flex-shrink:0}
.info-val{font-size:.82rem;font-weight:600;color:var(--text);text-align:right;word-break:break-all}
.info-val.dim{color:var(--muted);font-weight:400;font-size:.76rem}

/* ── Badges ── */
.badge{display:inline-block;padding:2px 9px;border-radius:6px;font-size:.73rem;font-weight:600}
.badge-green{background:rgba(37,211,102,.12);color:var(--green);border:1px solid rgba(37,211,102,.3)}
.badge-red{background:rgba(248,81,73,.12);color:var(--red);border:1px solid rgba(248,81,73,.3)}
.badge-blue{background:rgba(88,166,255,.12);color:var(--blue);border:1px solid rgba(88,166,255,.3)}
.badge-muted{background:rgba(139,148,158,.1);color:var(--muted);border:1px solid rgba(139,148,158,.2)}
.count-badge{
  display:inline-flex;align-items:center;justify-content:center;
  background:rgba(37,211,102,.12);color:var(--green);
  border:1px solid rgba(37,211,102,.3);border-radius:20px;
  padding:1px 10px;font-size:.75rem;font-weight:700;
}

/* ── Commands ── */
.cmd-search{
  width:100%;background:var(--bg);border:1px solid var(--border);
  border-radius:8px;padding:10px 14px;color:var(--text);
  font-size:.86rem;outline:none;margin-bottom:14px;transition:border-color .2s;
}
.cmd-search:focus{border-color:var(--green)}
.cmd-search::placeholder{color:var(--muted)}
.cmd-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(148px,1fr));gap:8px}
.cmd-chip{
  background:#0d1b2a;border:1px solid var(--border);border-radius:8px;
  padding:8px 12px;font-size:.78rem;color:var(--green);
  font-family:'Courier New',monospace;cursor:default;
  transition:all .2s;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.cmd-chip:hover{background:rgba(37,211,102,.07);border-color:rgba(37,211,102,.4)}
.cmd-empty{color:var(--muted);font-size:.85rem;padding:12px 0}

/* ── Features ── */
.features-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(255px,1fr));gap:14px}
.feature-card{
  background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:20px;display:flex;align-items:center;justify-content:space-between;
  gap:16px;transition:border-color .2s;
}
.feature-card:hover{border-color:rgba(37,211,102,.3)}
.feat-icon{font-size:1.5rem;margin-bottom:6px}
.feat-name{font-weight:600;font-size:.92rem;color:var(--text);margin-bottom:3px}
.feat-desc{font-size:.76rem;color:var(--muted)}
/* toggle switch */
.tgl-wrap{position:relative;flex-shrink:0}
.tgl-input{opacity:0;width:0;height:0;position:absolute}
.tgl-label{
  display:block;width:48px;height:26px;background:var(--border);
  border-radius:13px;cursor:pointer;position:relative;transition:background .3s;
}
.tgl-label::after{
  content:'';position:absolute;width:20px;height:20px;background:#fff;
  border-radius:50%;top:3px;left:3px;
  transition:transform .3s,box-shadow .3s;
  box-shadow:0 1px 4px rgba(0,0,0,.4);
}
.tgl-input:checked+.tgl-label{background:var(--green)}
.tgl-input:checked+.tgl-label::after{transform:translateX(22px)}
.tgl-input:disabled+.tgl-label{opacity:.5;cursor:not-allowed}

/* ── Groups ── */
.group-list{display:flex;flex-direction:column;gap:8px}
.group-item{
  background:var(--card);border:1px solid var(--border);border-radius:10px;
  padding:12px 16px;display:flex;align-items:center;justify-content:space-between;gap:12px;
}
.group-name{font-weight:600;font-size:.86rem;color:var(--text)}
.group-id{font-size:.71rem;color:var(--muted);font-family:monospace}
.group-size{font-size:.78rem;color:var(--muted);white-space:nowrap}
.empty-box{text-align:center;padding:48px 20px;color:var(--muted);font-size:.88rem}
.empty-icon{font-size:2.5rem;margin-bottom:12px}

/* ── Logs ── */
.log-controls{display:flex;gap:7px;margin-bottom:12px;flex-wrap:wrap;align-items:center}
.log-filter{
  padding:4px 11px;border-radius:6px;border:1px solid var(--border);
  background:var(--bg);color:var(--muted);font-size:.76rem;cursor:pointer;transition:all .2s;
}
.log-filter:hover,.log-filter.active{border-color:var(--green);color:var(--green);background:rgba(37,211,102,.07)}
.log-clear{
  margin-left:auto;padding:4px 11px;border-radius:6px;
  border:1px solid rgba(248,81,73,.4);background:rgba(248,81,73,.07);
  color:var(--red);font-size:.76rem;cursor:pointer;transition:all .2s;
}
.log-clear:hover{background:rgba(248,81,73,.18)}
.log-box{
  background:var(--bg2);border:1px solid var(--border);border-radius:10px;
  padding:14px;height:420px;overflow-y:auto;
  font-family:'Courier New',monospace;font-size:.77rem;
}
.log-line{padding:2px 0;display:flex;gap:8px;align-items:baseline}
.log-time{color:var(--muted);flex-shrink:0;font-size:.69rem}
.log-text{word-break:break-word}
.log-info .log-text{color:var(--blue)}
.log-success .log-text{color:var(--green)}
.log-warn .log-text{color:var(--yellow)}
.log-err .log-text{color:var(--red)}

/* ── Scrollbar ── */
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:var(--muted)}

/* ── Mobile ── */
.sb-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:45}
@media(max-width:768px){
  .sidebar{position:fixed;left:0;top:0;height:100vh;transform:translateX(-100%);z-index:50}
  .sidebar.open{transform:translateX(0)}
  .sb-overlay.open{display:block}
  .menu-btn{display:block}
  .info-grid{grid-template-columns:1fr}
  .content{padding:20px 16px 40px}
  .header{padding:0 16px}
}
.text-green{color:var(--green)}
.text-red{color:var(--red)}
</style>
</head>
<body>
<div class="app">

  <div class="sb-overlay" id="sbOverlay" onclick="closeSb()"></div>

  <!-- ── Sidebar ── -->
  <aside class="sidebar" id="sidebar">
    <div class="sb-brand">
      <div class="sb-icon">&#129302;</div>
      <div>
        <div class="sb-name">KnightBot MD</div>
        <div class="sb-ver">v<span id="sbVer">–</span></div>
      </div>
    </div>
    <nav class="sb-nav">
      <button class="nav-item active" data-page="overview" onclick="goPage(this)"><span class="nav-icon">&#128202;</span>Overview</button>
      <button class="nav-item" data-page="commands"  onclick="goPage(this)"><span class="nav-icon">&#9000;&#65039;</span>Commands</button>
      <button class="nav-item" data-page="features"  onclick="goPage(this)"><span class="nav-icon">&#9889;</span>Features</button>
      <button class="nav-item" data-page="groups"    onclick="goPage(this)"><span class="nav-icon">&#128101;</span>Groups</button>
      <button class="nav-item" data-page="settings"  onclick="goPage(this)"><span class="nav-icon">&#9881;&#65039;</span>Settings</button>
      <button class="nav-item" data-page="logs"      onclick="goPage(this)"><span class="nav-icon">&#128203;</span>Live Logs</button>
    </nav>
    <div class="sb-footer">
      Made by <a href="https://github.com/mruniquehacker/Knightbot-MD" target="_blank">MR UNIQUE HACKER</a>
    </div>
  </aside>

  <!-- ── Main ── -->
  <div class="main">
    <header class="header">
      <div class="header-left">
        <button class="menu-btn" onclick="toggleSb()">&#9776;</button>
        <span class="header-title" id="hTitle">Overview</span>
      </div>
      <div class="status-pill offline" id="statusPill">
        <div class="dot"></div>
        <span id="statusTxt">Offline</span>
      </div>
    </header>

    <div class="content">

      <!-- ═══ OVERVIEW ═══ -->
      <div class="page active" id="page-overview">
        <div class="sec-title">Overview</div>
        <div class="stats-grid">
          <div class="stat-card">
            <div class="stat-icon">&#129302;</div>
            <div class="stat-label">Bot Status</div>
            <div class="stat-value sm text-red" id="stStatus">Offline</div>
            <div class="stat-sub" id="stUptime">–</div>
          </div>
          <div class="stat-card">
            <div class="stat-icon">&#128222;</div>
            <div class="stat-label">Phone Number</div>
            <div class="stat-value sm" id="stPhone">–</div>
            <div class="stat-sub">WhatsApp Account</div>
          </div>
          <div class="stat-card">
            <div class="stat-icon">&#128232;</div>
            <div class="stat-label">Messages</div>
            <div class="stat-value" id="stMsgs">0</div>
            <div class="stat-sub">Since last restart</div>
          </div>
          <div class="stat-card">
            <div class="stat-icon">&#128101;</div>
            <div class="stat-label">Groups</div>
            <div class="stat-value" id="stGroups">–</div>
            <div class="stat-sub">Active groups</div>
          </div>
          <div class="stat-card">
            <div class="stat-icon">&#128190;</div>
            <div class="stat-label">Memory</div>
            <div class="stat-value sm" id="stMem">–</div>
            <div class="stat-sub">RAM Usage</div>
          </div>
          <div class="stat-card">
            <div class="stat-icon">&#9881;&#65039;</div>
            <div class="stat-label">Mode</div>
            <div class="stat-value sm" id="stMode">–</div>
            <div class="stat-sub">Command access</div>
          </div>
        </div>

        <div class="sec-title">Bot Information</div>
        <div class="info-grid">
          <div class="card">
            <div class="info-row"><span class="info-key">Bot Name</span>    <span class="info-val" id="iBotName">–</span></div>
            <div class="info-row"><span class="info-key">Owner</span>       <span class="info-val" id="iOwner">–</span></div>
            <div class="info-row"><span class="info-key">Version</span>     <span class="info-val"><span class="badge badge-green" id="iVersion">–</span></span></div>
            <div class="info-row"><span class="info-key">Description</span><span class="info-val dim" id="iDesc">–</span></div>
          </div>
          <div class="card">
            <div class="info-row"><span class="info-key">Owner Number</span> <span class="info-val" id="iOwnerNum">–</span></div>
            <div class="info-row"><span class="info-key">Command Mode</span> <span class="info-val"><span class="badge badge-blue" id="iMode">–</span></span></div>
            <div class="info-row"><span class="info-key">Node.js</span>      <span class="info-val" id="iNode">–</span></div>
            <div class="info-row"><span class="info-key">Memory Usage</span> <span class="info-val" id="iMem">–</span></div>
          </div>
        </div>

        <div class="sec-title">Auto Features</div>
        <div class="card">
          <div class="info-row"><span class="info-key">Auto Read</span>        <span class="info-val" id="ovAutoread"><span class="badge badge-muted">–</span></span></div>
          <div class="info-row"><span class="info-key">Auto Typing</span>      <span class="info-val" id="ovAutotyping"><span class="badge badge-muted">–</span></span></div>
          <div class="info-row"><span class="info-key">Auto Status View</span> <span class="info-val" id="ovAutostatus"><span class="badge badge-muted">–</span></span></div>
        </div>
      </div>

      <!-- ═══ COMMANDS ═══ -->
      <div class="page" id="page-commands">
        <div class="sec-head">
          <span class="sec-title">Commands</span>
          <span class="count-badge" id="cmdCntBadge">0</span>
        </div>
        <input class="cmd-search" id="cmdSearch" placeholder="&#128269;  Search commands..." oninput="filterCmds()">
        <div class="card">
          <div class="cmd-grid" id="cmdGrid"><span class="cmd-empty">Loading…</span></div>
        </div>
      </div>

      <!-- ═══ FEATURES ═══ -->
      <div class="page" id="page-features">
        <div class="sec-title">Auto Features</div>
        <div class="features-grid">
          <div class="feature-card">
            <div>
              <div class="feat-icon">&#128065;&#65039;</div>
              <div class="feat-name">Auto Read</div>
              <div class="feat-desc">Automatically mark messages as read</div>
            </div>
            <div class="tgl-wrap">
              <input type="checkbox" class="tgl-input" id="tgl-autoread" onchange="toggleFeat('autoread',this)">
              <label class="tgl-label" for="tgl-autoread"></label>
            </div>
          </div>
          <div class="feature-card">
            <div>
              <div class="feat-icon">&#9000;&#65039;</div>
              <div class="feat-name">Auto Typing</div>
              <div class="feat-desc">Show typing indicator while processing</div>
            </div>
            <div class="tgl-wrap">
              <input type="checkbox" class="tgl-input" id="tgl-autotyping" onchange="toggleFeat('autotyping',this)">
              <label class="tgl-label" for="tgl-autotyping"></label>
            </div>
          </div>
          <div class="feature-card">
            <div>
              <div class="feat-icon">&#128226;</div>
              <div class="feat-name">Auto Status View</div>
              <div class="feat-desc">Automatically view WhatsApp statuses</div>
            </div>
            <div class="tgl-wrap">
              <input type="checkbox" class="tgl-input" id="tgl-autostatus" onchange="toggleFeat('autostatus',this)">
              <label class="tgl-label" for="tgl-autostatus"></label>
            </div>
          </div>
        </div>
      </div>

      <!-- ═══ GROUPS ═══ -->
      <div class="page" id="page-groups">
        <div class="sec-head">
          <span class="sec-title">Active Groups</span>
          <span class="count-badge" id="grpCntBadge">0</span>
        </div>
        <div id="grpContainer">
          <div class="card empty-box">
            <div class="empty-icon">&#128101;</div>
            Connect bot to see active groups
          </div>
        </div>
      </div>

      <!-- ═══ SETTINGS ═══ -->
      <div class="page" id="page-settings">
        <div class="sec-title">Bot Configuration</div>
        <div class="card">
          <div class="info-row"><span class="info-key">Bot Name</span>     <span class="info-val" id="sBotName">–</span></div>
          <div class="info-row"><span class="info-key">Bot Owner</span>    <span class="info-val" id="sBotOwner">–</span></div>
          <div class="info-row"><span class="info-key">Owner Number</span> <span class="info-val" id="sOwnerNum">–</span></div>
          <div class="info-row"><span class="info-key">Version</span>      <span class="info-val"><span class="badge badge-green" id="sVersion">–</span></span></div>
          <div class="info-row"><span class="info-key">Command Mode</span> <span class="info-val"><span class="badge badge-blue" id="sMode">–</span></span></div>
          <div class="info-row"><span class="info-key">Description</span>  <span class="info-val dim" id="sDesc">–</span></div>
        </div>
        <div class="sec-title">Auto Features</div>
        <div class="card">
          <div class="info-row"><span class="info-key">Auto Read</span>        <span class="info-val" id="sAutoread">–</span></div>
          <div class="info-row"><span class="info-key">Auto Typing</span>      <span class="info-val" id="sAutotyping">–</span></div>
          <div class="info-row"><span class="info-key">Auto Status View</span> <span class="info-val" id="sAutostatus">–</span></div>
        </div>
        <div class="sec-title">Runtime</div>
        <div class="card">
          <div class="info-row"><span class="info-key">Node.js</span>       <span class="info-val" id="sNode">–</span></div>
          <div class="info-row"><span class="info-key">Memory</span>        <span class="info-val" id="sMem">–</span></div>
          <div class="info-row"><span class="info-key">Groups</span>        <span class="info-val" id="sGroups">–</span></div>
          <div class="info-row"><span class="info-key">Total Messages</span><span class="info-val" id="sMsgs2">–</span></div>
        </div>
      </div>

      <!-- ═══ LOGS ═══ -->
      <div class="page" id="page-logs">
        <div class="sec-title">Live Events</div>
        <div class="log-controls">
          <button class="log-filter active" onclick="setLvl('all',this)">All</button>
          <button class="log-filter" onclick="setLvl('info',this)">Info</button>
          <button class="log-filter" onclick="setLvl('success',this)">Success</button>
          <button class="log-filter" onclick="setLvl('warn',this)">Warning</button>
          <button class="log-filter" onclick="setLvl('err',this)">Error</button>
          <button class="log-clear" onclick="clearLogs()">Clear</button>
        </div>
        <div class="log-box" id="logBox">
          <div class="log-line log-info"><span class="log-time">–</span><span class="log-text">Connecting to bot…</span></div>
        </div>
      </div>

    </div><!-- /content -->
  </div><!-- /main -->
</div><!-- /app -->

<script>
var allCmds = [];
var logLevel = 'all';
var connTs = null;
var uptTick = null;

// ── Navigation ────────────────────────────────────────────────────────────────
function goPage(btn) {
  document.querySelectorAll('.page').forEach(function(p){ p.classList.remove('active'); });
  document.querySelectorAll('.nav-item').forEach(function(n){ n.classList.remove('active'); });
  document.getElementById('page-' + btn.dataset.page).classList.add('active');
  btn.classList.add('active');
  document.getElementById('hTitle').textContent = btn.textContent.trim();
  closeSb();
}
function toggleSb() {
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('sbOverlay').classList.toggle('open');
}
function closeSb() {
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sbOverlay').classList.remove('open');
}

// ── Status ────────────────────────────────────────────────────────────────────
function setOnline(on) {
  var pill = document.getElementById('statusPill');
  var st   = document.getElementById('stStatus');
  document.getElementById('statusTxt').textContent = on ? 'Online' : 'Offline';
  pill.className = on ? 'status-pill' : 'status-pill offline';
  st.textContent = on ? 'Online' : 'Offline';
  st.className = on ? 'stat-value sm text-green' : 'stat-value sm text-red';
  if (!on) { document.getElementById('stUptime').textContent = '–'; if (uptTick) { clearInterval(uptTick); uptTick = null; } connTs = null; }
}
function startUptimeTick() {
  if (uptTick) clearInterval(uptTick);
  uptTick = setInterval(function() {
    if (connTs) document.getElementById('stUptime').textContent = 'Up: ' + fmtDur(Date.now() - connTs);
  }, 1000);
}
function fmtDur(ms) {
  var s=Math.floor(ms/1000), m=Math.floor(s/60), h=Math.floor(m/60), d=Math.floor(h/24);
  if (d>0) return d+'d '+(h%24)+'h '+(m%60)+'m';
  if (h>0) return h+'h '+(m%60)+'m '+(s%60)+'s';
  if (m>0) return m+'m '+(s%60)+'s';
  return s+'s';
}

// ── Apply status data ─────────────────────────────────────────────────────────
function applyStatus(d) {
  setOnline(d.online);
  if (d.connectedAt) { connTs = d.connectedAt; startUptimeTick(); }
  function set(id, v) { var el = document.getElementById(id); if (el) el.textContent = (v !== undefined && v !== null && v !== '') ? v : '–'; }
  set('stPhone',   d.phoneNumber);
  set('stMsgs',    d.messageCount || 0);
  set('stGroups',  d.groupCount != null ? d.groupCount : '–');
  set('stMem',     d.memoryMB ? d.memoryMB + ' MB' : '–');
  set('stMode',    d.settings && d.settings.commandMode);
  set('iBotName',  d.settings && d.settings.botName);
  set('iOwner',    d.settings && d.settings.botOwner);
  set('iVersion',  d.settings && d.settings.version);
  set('iDesc',     d.settings && d.settings.description);
  set('iOwnerNum', d.settings && d.settings.ownerNumber);
  set('iMode',     d.settings && d.settings.commandMode);
  set('iNode',     d.nodeVersion);
  set('iMem',      d.memoryMB ? d.memoryMB + ' MB' : '–');
  set('sbVer',     d.settings && d.settings.version);
  set('sBotName',  d.settings && d.settings.botName);
  set('sBotOwner', d.settings && d.settings.botOwner);
  set('sOwnerNum', d.settings && d.settings.ownerNumber);
  set('sVersion',  d.settings && d.settings.version);
  set('sMode',     d.settings && d.settings.commandMode);
  set('sDesc',     d.settings && d.settings.description);
  set('sNode',     d.nodeVersion);
  set('sMem',      d.memoryMB ? d.memoryMB + ' MB' : '–');
  set('sGroups',   d.groupCount != null ? d.groupCount : '–');
  set('sMsgs2',    d.messageCount || 0);
  if (d.commands && d.commands.length) { allCmds = d.commands; renderCmds(allCmds); }
}

// ── Features ─────────────────────────────────────────────────────────────────
function statusBadge(on) {
  return on ? '<span class="badge badge-green">Enabled</span>' : '<span class="badge badge-red">Disabled</span>';
}
function applyFeatures(f) {
  ['autoread','autotyping','autostatus'].forEach(function(k) {
    var el = document.getElementById('tgl-' + k); if (el) el.checked = !!f[k];
  });
  var ov = {autoread:'ovAutoread',autotyping:'ovAutotyping',autostatus:'ovAutostatus'};
  var sv = {autoread:'sAutoread', autotyping:'sAutotyping', autostatus:'sAutostatus'};
  Object.keys(ov).forEach(function(k) {
    var o = document.getElementById(ov[k]); if (o) o.innerHTML = statusBadge(f[k]);
    var s = document.getElementById(sv[k]); if (s) s.innerHTML = statusBadge(f[k]);
  });
}
async function toggleFeat(feature, chk) {
  chk.disabled = true;
  try {
    var res = await fetch('/api/features', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({feature: feature})
    });
    var data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Error');
    applyFeatures(data.features);
  } catch(e) {
    chk.checked = !chk.checked;
    addLog('Failed to toggle ' + feature + ': ' + e.message, 'err');
  } finally { chk.disabled = false; }
}

// ── Commands ──────────────────────────────────────────────────────────────────
function renderCmds(list) {
  document.getElementById('cmdCntBadge').textContent = list.length;
  var grid = document.getElementById('cmdGrid');
  grid.innerHTML = '';
  if (!list.length) { grid.innerHTML = '<span class="cmd-empty">No commands found</span>'; return; }
  list.forEach(function(cmd) {
    var chip = document.createElement('div');
    chip.className = 'cmd-chip'; chip.title = cmd; chip.textContent = '.' + cmd;
    grid.appendChild(chip);
  });
}
function filterCmds() {
  var q = document.getElementById('cmdSearch').value.toLowerCase();
  renderCmds(q ? allCmds.filter(function(c){ return c.toLowerCase().includes(q); }) : allCmds);
}

// ── Groups ────────────────────────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function renderGroups(groups) {
  document.getElementById('grpCntBadge').textContent = groups.length;
  var stG = document.getElementById('stGroups'); if (stG) stG.textContent = groups.length;
  var sG  = document.getElementById('sGroups');  if (sG)  sG.textContent  = groups.length;
  var c = document.getElementById('grpContainer');
  if (!groups.length) {
    c.innerHTML = '<div class="card empty-box"><div class="empty-icon">&#128101;</div>No groups found</div>';
    return;
  }
  c.innerHTML = '<div class="group-list">' + groups.map(function(g) {
    return '<div class="group-item"><div><div class="group-name">' + esc(g.subject || 'Unknown') +
      '</div><div class="group-id">' + esc(g.id || '') + '</div></div>' +
      '<div class="group-size">&#128100; ' + (g.size || 0) + ' members</div></div>';
  }).join('') + '</div>';
}

// ── Logs ──────────────────────────────────────────────────────────────────────
function addLog(text, level, ts) {
  var box = document.getElementById('logBox');
  var div = document.createElement('div');
  div.className = 'log-line log-' + (level || 'info');
  div.dataset.lvl = level || 'info';
  div.innerHTML = '<span class="log-time">' + esc(new Date(ts || Date.now()).toLocaleTimeString()) +
    '</span><span class="log-text">' + esc(text) + '</span>';
  if (logLevel !== 'all' && (level || 'info') !== logLevel) div.style.display = 'none';
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
  while (box.children.length > 250) box.removeChild(box.firstChild);
}
function setLvl(lvl, btn) {
  logLevel = lvl;
  document.querySelectorAll('.log-filter').forEach(function(b){ b.classList.remove('active'); });
  btn.classList.add('active');
  document.querySelectorAll('.log-line').forEach(function(l){
    l.style.display = (lvl === 'all' || l.dataset.lvl === lvl) ? '' : 'none';
  });
}
function clearLogs() { document.getElementById('logBox').innerHTML = ''; }

// ── WebSocket ─────────────────────────────────────────────────────────────────
function connect() {
  var proto = location.protocol === 'https:' ? 'wss' : 'ws';
  var ws = new WebSocket(proto + '://' + location.host);
  ws.onopen = function() { addLog('Dashboard connected', 'success'); };
  ws.onmessage = function(e) {
    try {
      var msg = JSON.parse(e.data);
      if (msg.type === 'init') { applyStatus(msg.status); applyFeatures(msg.features); }
      else if (msg.type === 'status')   { applyStatus(msg.data); }
      else if (msg.type === 'features') { applyFeatures(msg.data); }
      else if (msg.type === 'event')    { addLog(msg.text, msg.level, msg.time); }
      else if (msg.type === 'tick') {
        if (msg.connectedAt && !connTs) { connTs = msg.connectedAt; startUptimeTick(); }
        var mem = msg.memoryMB ? msg.memoryMB + ' MB' : '–';
        ['stMem','iMem','sMem'].forEach(function(id){ var el = document.getElementById(id); if(el) el.textContent = mem; });
        ['stMsgs','sMsgs2'].forEach(function(id){ var el = document.getElementById(id); if(el) el.textContent = msg.messageCount || 0; });
      }
      else if (msg.type === 'message_count') {
        ['stMsgs','sMsgs2'].forEach(function(id){ var el = document.getElementById(id); if(el) el.textContent = msg.count || 0; });
      }
      else if (msg.type === 'groups') { renderGroups(msg.data || []); }
    } catch(_) {}
  };
  ws.onclose = function() { setOnline(false); addLog('Disconnected. Reconnecting in 3s…','warn'); setTimeout(connect, 3000); };
  ws.onerror = function() { ws.close(); };
}

// ── Init ──────────────────────────────────────────────────────────────────────
Promise.all([
  fetch('/api/status').then(function(r){ return r.json(); }).catch(function(){ return null; }),
  fetch('/api/features').then(function(r){ return r.json(); }).catch(function(){ return null; })
]).then(function(res) {
  if (res[0]) applyStatus(res[0]);
  if (res[1]) applyFeatures(res[1]);
  return fetch('/api/groups').then(function(r){ return r.json(); }).catch(function(){ return []; });
}).then(function(groups) { if (groups && groups.length) renderGroups(groups); });

connect();
</script>
</body>
</html>`;

// ─── HTTP Server ──────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
    const { pathname } = new URL(req.url, 'http://localhost');

    const json = (data, code = 200) => {
        res.writeHead(code, {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'X-Content-Type-Options': 'nosniff',
        });
        res.end(JSON.stringify(data));
    };

    if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

    if (pathname === '/api/status'   && req.method === 'GET') return json(getStatusSnapshot());
    if (pathname === '/api/features' && req.method === 'GET') return json(getFeatures());
    if (pathname === '/api/logs'     && req.method === 'GET') return json(logs.slice(-100));
    if (pathname === '/api/groups'   && req.method === 'GET') return json(groupList);

    if (pathname === '/api/features' && req.method === 'POST') {
        const body    = await parseBody(req);
        const feature = String(body.feature || '').trim();
        if (!FEATURE_FILES[feature]) return json({ error: 'Unknown feature' }, 400);

        const fp  = path.join(DATA_DIR, FEATURE_FILES[feature]);
        const cur = readJSON(fp, { enabled: false });
        cur.enabled = !cur.enabled;
        writeJSON(fp, cur);

        const features = getFeatures();
        broadcast({ type: 'features', data: features });
        addLog(`${feature} ${cur.enabled ? 'enabled' : 'disabled'} via dashboard`, cur.enabled ? 'success' : 'warn');
        return json({ success: true, enabled: cur.enabled, features });
    }

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(HTML);
});

// ─── WebSocket ────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server });

wss.on('connection', ws => {
    clients.add(ws);
    ws.send(JSON.stringify({ type: 'init', status: getStatusSnapshot(), features: getFeatures() }));
    for (const log of logs.slice(-50))
        ws.send(JSON.stringify({ type: 'event', text: log.text, level: log.level, time: log.time }));
    if (groupList.length)
        ws.send(JSON.stringify({ type: 'groups', data: groupList }));
    ws.on('close', () => clients.delete(ws));
    ws.on('error', () => clients.delete(ws));
});

// Periodic heartbeat every 10 s
setInterval(() => {
    broadcast({
        type: 'tick',
        connectedAt,
        memoryMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
        messageCount,
    });
}, 10_000);

// ─── Auto-start ───────────────────────────────────────────────────────────────
server.listen(PORT, () => {
    console.log(`\u{1F310} Web Dashboard: http://localhost:${PORT}`);
});

// ─── Public API (used by index.js) ────────────────────────────────────────────
module.exports = {
    /** @deprecated Server auto-starts on require; kept for backward compatibility */
    start() {},

    setBotOnline(sock) {
        botSocket   = sock;
        botStatus   = 'online';
        connectedAt = Date.now();

        // Fetch live group list
        Promise.resolve()
            .then(() => sock.groupFetchAllParticipating())
            .then(groups => {
                groupList = Object.values(groups).map(g => ({
                    id:      g.id,
                    subject: g.subject || 'Unknown',
                    size:    (g.participants || []).length,
                }));
                broadcast({ type: 'groups', data: groupList });
            })
            .catch(() => {});

        broadcast({ type: 'status', data: getStatusSnapshot() });
        addLog('Bot connected to WhatsApp \u2705', 'success');
    },

    setBotOffline() {
        botStatus   = 'offline';
        connectedAt = null;
        broadcast({ type: 'status', data: getStatusSnapshot() });
        addLog('Bot disconnected \u274C', 'err');
    },

    logEvent(text, level = 'info') {
        addLog(text, level);
    },

    incrementMessage() {
        messageCount++;
    },
};
