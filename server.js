// RUST 2D — сервер мультиплеера (Node.js + Socket.IO)
// Запуск: npm install && node server.js   → http://localhost:3000
const path = require('path');
const fs = require('fs');

const CFG = { port: process.env.PORT || 3000, maxPlayers: 16, tick: 66, maxHit: 150, maxRange: 1600, hitsPerSec: 12 };

// Папка для сохранений: DATA_DIR, либо том Railway (RAILWAY_VOLUME_MOUNT_PATH), либо ./data
const DATA_DIR = process.env.DATA_DIR || process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
const SAVE_FILE = path.join(DATA_DIR, 'world.json');
const KEY_RE = /^[tkpbcow][A-Za-z0-9_:\-]{1,48}$/, BASE_RE = /^[tkpbcow]\d{1,9}$/;

function loadState(file) {
  try { const j = JSON.parse(fs.readFileSync(file, 'utf8')); if (j && j.seed) return j; } catch (e) { /* нет сохранения */ }
  return null;
}
function saveState(file, st) {
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = file + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(st)); fs.renameSync(tmp, file);
    return true;
  } catch (e) { console.error('save failed:', e.message); return false; }
}

function attach(io, now = Date.now, opts = {}) {
  const saveFile = opts.file === undefined ? SAVE_FILE : opts.file;   // null — без сохранения (тесты)
  const saved = saveFile ? loadState(saveFile) : null;
  const store = { ov: (saved && saved.ov) || {}, players: (saved && saved.players) || {} };   // ov — изменения мира, players — прогресс по токену
  const world = { seed: saved ? saved.seed : 1 + Math.floor(Math.random() * 999999998), t0: now() - ((saved && saved.elapsed) || 0) * 1000 };
  let hostId = null, storeDirty = false;
  const sockets = new Map();
  const pickHost = () => { const first = players.keys().next(); const id = first.done ? null : first.value; if (id !== hostId) { hostId = id; io.emit('host', hostId); } };
  const persist = (force) => {
    if (!saveFile || (!storeDirty && force !== true)) return false; storeDirty = false;
    return saveState(saveFile, { seed: world.seed, elapsed: (now() - world.t0) / 1000, ov: store.ov, players: store.players, savedAt: now() });
  };
  const players = new Map();    // id -> { name, x, y, dir, sel, armor, hp, dead, sw, ... }
  const dirty = new Set();
  const clean = s => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim();
  const num = (v, a, b, d = 0) => (typeof v === 'number' && isFinite(v)) ? Math.max(a, Math.min(b, v)) : d;
  const say = (text, sys, name) => io.emit('chat', { text, sys: !!sys, name });

  io.on('connection', (sock) => {
    let me = null;
    sock.on('hello', (d) => {
      if (me) return;
      if (players.size >= CFG.maxPlayers) { sock.emit('chat', { text: 'Сервер полон', sys: true }); sock.disconnect(true); return; }
      let name = clean(d && d.name).slice(0, 16) || 'Игрок';
      const taken = new Set([...players.values()].map(p => p.name));
      for (let i = 2, base = name; taken.has(name); i++) name = base + i;
      const tok = (d && typeof d.tok === 'string' && /^[A-Za-z0-9]{8,64}$/.test(d.tok)) ? d.tok : null;
      me = { tok, wBudget: 600, wT: 0, id: sock.id, name, x: 0, y: 0, dir: 0, sel: 'hands', armor: null, hp: 100, dead: false, sw: 0, hitT: 0, hitN: 0, chatT: 0, lastBy: null, lastByT: 0, seen: false };
      players.set(sock.id, me); sockets.set(sock.id, sock);
      if (!hostId) pickHost();
      const snap = {}; for (const [id, p] of players) snap[id] = pub(p);
      sock.emit('init', { id: sock.id, seed: world.seed, t: (now() - world.t0) / 1000, players: snap, ov: store.ov, save: (tok && store.players[tok]) || null, host: hostId });
      sock.broadcast.emit('joined', { id: sock.id, p: pub(me) });
      say(name + ' зашёл в игру', true);
    });

    sock.on('s', (s) => {
      if (!me || !s) return;
      me.x = num(s.x, 0, 1e6, me.x); me.y = num(s.y, 0, 1e6, me.y); me.dir = num(s.dir, -10, 10, me.dir);
      me.sel = clean(s.sel).slice(0, 24) || 'hands'; me.armor = s.armor ? clean(s.armor).slice(0, 24) : null;
      me.hp = num(s.hp, -1000, 1000, me.hp); me.dead = !!s.dead; me.sw = num(s.sw, 0, 1e9, me.sw) | 0;
      me.swim = !!s.swim; me.cr = num(s.cr, 0, 1, 0); me.boat = !!s.boat; me.mounted = !!s.mounted; me.seen = true;
      if (me.dead && !me.wasDead) { me.wasDead = true; const by = me.lastBy && players.get(me.lastBy); say(by && now() - me.lastByT < 15000 ? by.name + ' убил ' + me.name : me.name + ' погиб', true); }
      if (!me.dead) me.wasDead = false;
      dirty.add(sock.id);
    });

    // PvP: попадание заявляет стреляющий, сервер проверяет дальность и частоту
    sock.on('hit', (h) => {
      if (!me || me.dead || !h) return;
      const t = players.get(h.id); if (!t || t.dead || t === me) return;
      const tn = now(); if (tn - me.hitT > 1000) { me.hitT = tn; me.hitN = 0; } if (++me.hitN > CFG.hitsPerSec) return;
      if (Math.hypot(me.x - t.x, me.y - t.y) > CFG.maxRange) return;
      const d = num(h.d, 0, CFG.maxHit, 0); if (d <= 0) return;
      t.lastBy = me.id; t.lastByT = tn;
      io.to(t.id).emit('hurt', { d, from: me.id });
    });

    // эффект выстрела для остальных (звук и визуальные пули)
    sock.on('fx', (f) => {
      if (!me || me.dead || !f) return;
      sock.broadcast.emit('fx', { x: num(f.x, 0, 1e6), y: num(f.y, 0, 1e6), dir: num(f.dir, -10, 10), v: num(f.v, 0, 5000, 900), life: num(f.life, 0, 5, 1), kind: clean(f.kind).slice(0, 12), n: num(f.n, 1, 12, 1) | 0, sp: num(f.sp, 0, 1, 0), snd: clean(f.snd).slice(0, 12) });
    });

    // ---- общий мир: клиент присылает пачки изменений [[ключ, значение|null], ...] ----
    sock.on('w', (batch) => {
      if (!me || !Array.isArray(batch)) return;
      const tn = now(); if (tn - me.wT > 1000) { me.wT = tn; me.wBudget = 600; }
      const out = [];
      for (const e of batch.slice(0, 300)) {
        if (!Array.isArray(e) || me.wBudget-- <= 0) break;
        const k = e[0], v = e[1];
        if (typeof k !== 'string' || !KEY_RE.test(k)) continue;
        if (v === null) { if (BASE_RE.test(k)) store.ov[k] = null; else delete store.ov[k]; }
        else if (v && typeof v === 'object' && !Array.isArray(v)) { if (JSON.stringify(v).length > 20000) continue; store.ov[k] = v; }
        else continue;
        out.push([k, v]);
      }
      if (out.length) { storeDirty = true; sock.broadcast.emit('w', out); }
    });

    // прогресс игрока (инвентарь, позиция, опыт) — по секретному токену из браузера
    sock.on('ps', (data) => {
      if (!me || !me.tok || !data || typeof data !== 'object') return;
      const fin = data.fin === 1; delete data.fin;   // fin — последнее сохранение при выходе, лимит частоты не применяется
      const tn = now(); if (!fin && tn - (me.psT || 0) < 1000) return; me.psT = tn;
      let js; try { js = JSON.stringify(data); } catch (e) { return; }
      if (js.length > 200000) return;
      store.players[me.tok] = data; storeDirty = true;
    });

    // звери: хозяин (ближайший игрок) шлёт снимки, остальные только показывают
    sock.on('ms', (a) => { if (!me || !Array.isArray(a) || a.length > 400) return; sock.volatile.broadcast.emit('ms', { by: sock.id, a }); });
    sock.on('mh', (h) => {
      if (!me || !h || typeof h.k !== 'string' || h.k.length > 40) return;
      const tn = now(); if (tn - (me.mhT || 0) > 1000) { me.mhT = tn; me.mhN = 0; } if (++me.mhN > CFG.hitsPerSec * 2) return;
      sock.broadcast.emit('mh', { k: h.k, d: num(h.d, 0, CFG.maxHit, 0), by: sock.id });
    });
    // добыча за убитого чужого зверя уходит убийце
    sock.on('loot', (l) => {
      if (!me || !l || typeof l.to !== 'string' || !l.items || typeof l.items !== 'object') return;
      const items = {}; let n = 0;
      for (const k of Object.keys(l.items).slice(0, 40)) { const c = num(l.items[k], 0, 500, 0) | 0; if (c > 0 && /^[a-z0-9_]{1,24}$/i.test(k)) { items[k] = c; n += c; } }
      if (!n) return;
      io.to(l.to).emit('loot', { items, xp: num(l.xp, 0, 1000, 0) });
    });

    sock.on('chat', (text) => {
      if (!me) return; const tn = now(); if (tn - me.chatT < 500) return; me.chatT = tn;
      text = clean(text).slice(0, 120); if (text) say(text, false, me.name);
    });

    sock.on('disconnect', () => {
      if (!me) return; players.delete(sock.id); dirty.delete(sock.id); sockets.delete(sock.id);
      io.emit('left', sock.id); say(me.name + ' вышел', true);
      if (hostId === sock.id) { hostId = null; pickHost(); }
      persist();
    });
  });

  function pub(p) { return { name: p.name, x: p.x, y: p.y, dir: p.dir, sel: p.sel, armor: p.armor, hp: p.hp, dead: p.dead, sw: p.sw, swim: p.swim, cr: p.cr, boat: p.boat, mounted: p.mounted }; }

  const timer = setInterval(() => {
    if (!dirty.size) return;
    const out = {}; for (const id of dirty) { const p = players.get(id); if (p) out[id] = pub(p); }
    dirty.clear(); io.volatile.emit('s', out);
  }, CFG.tick);
  const saver = setInterval(persist, 10000); if (saver.unref) saver.unref();
  return { world, players, store, persist, getHost: () => hostId, stop: () => { clearInterval(timer); clearInterval(saver); persist(true); } };
}

module.exports = { attach, CFG };

if (require.main === module) {
  const express = require('express'), http = require('http'), { Server } = require('socket.io');
  const app = express(), server = http.createServer(app);
  const io = new Server(server, { cors: { origin: '*' }, maxHttpBufferSize: 5e6 });
  app.use(express.static(path.join(__dirname, 'public')));
  const A = attach(io);
  const bye = () => { try { A.persist(true); } catch (e) { /* */ } process.exit(0); };
  process.on('SIGTERM', bye); process.on('SIGINT', bye);
  console.log('Сохранения: ' + SAVE_FILE);
  server.listen(CFG.port, () => console.log('Rust 2D server: http://localhost:' + CFG.port));
}
