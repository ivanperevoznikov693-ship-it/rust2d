// RUST 2D — сервер мультиплеера (Node.js + Socket.IO)
// Запуск: npm install && node server.js   → http://localhost:3000
const path = require('path');

const CFG = { port: process.env.PORT || 3000, maxPlayers: 16, tick: 66, maxHit: 150, maxRange: 1600, hitsPerSec: 12 };

function attach(io, now = Date.now) {
  const world = { seed: 1 + Math.floor(Math.random() * 999999998), t0: now() };
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
      me = { id: sock.id, name, x: 0, y: 0, dir: 0, sel: 'hands', armor: null, hp: 100, dead: false, sw: 0, hitT: 0, hitN: 0, chatT: 0, lastBy: null, lastByT: 0, seen: false };
      players.set(sock.id, me);
      const snap = {}; for (const [id, p] of players) snap[id] = pub(p);
      sock.emit('init', { id: sock.id, seed: world.seed, t: (now() - world.t0) / 1000, players: snap });
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

    sock.on('chat', (text) => {
      if (!me) return; const tn = now(); if (tn - me.chatT < 500) return; me.chatT = tn;
      text = clean(text).slice(0, 120); if (text) say(text, false, me.name);
    });

    sock.on('disconnect', () => {
      if (!me) return; players.delete(sock.id); dirty.delete(sock.id);
      io.emit('left', sock.id); say(me.name + ' вышел', true);
    });
  });

  function pub(p) { return { name: p.name, x: p.x, y: p.y, dir: p.dir, sel: p.sel, armor: p.armor, hp: p.hp, dead: p.dead, sw: p.sw, swim: p.swim, cr: p.cr, boat: p.boat, mounted: p.mounted }; }

  const timer = setInterval(() => {
    if (!dirty.size) return;
    const out = {}; for (const id of dirty) { const p = players.get(id); if (p) out[id] = pub(p); }
    dirty.clear(); io.volatile.emit('s', out);
  }, CFG.tick);
  return { world, players, stop: () => clearInterval(timer) };
}

module.exports = { attach, CFG };

if (require.main === module) {
  const express = require('express'), http = require('http'), { Server } = require('socket.io');
  const app = express(), server = http.createServer(app);
  const io = new Server(server, { cors: { origin: '*' } });
  app.use(express.static(path.join(__dirname, 'public')));
  attach(io);
  server.listen(CFG.port, () => console.log('Rust 2D server: http://localhost:' + CFG.port));
}
