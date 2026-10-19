// Сервер Rust 2D: HTTP (отдаёт игру) + WebSocket (без внешних библиотек) + авторитарная симуляция мира.
//   node server/server.js                 -> http://localhost:8080
//   PORT=3000 SEED=12345 node server/server.js
//   SAVE=путь/к/world.json (по умолчанию server/world.json; SAVE=0 — без сохранения), SAVE_EVERY=60 (секунд)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { loadGame } = require('./sim.js');
const authMod = require('./auth.js');

const PORT = +process.env.PORT || 8080;
const SAVE_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || __dirname;   // на Railway с подключённым Volume мир автоматически лежит на нём и переживает деплои
const SAVE = process.env.SAVE === '0' ? null : (process.env.SAVE || path.join(SAVE_DIR, 'world.json'));
const SAVE_EVERY = (+process.env.SAVE_EVERY || 60) * 1000;
let store = null;   // { v, seed, world: '<json>', players: { <sha256 токена>: { at, name, d: {…} } } }
if (SAVE && fs.existsSync(SAVE)) {
  try { store = JSON.parse(fs.readFileSync(SAVE, 'utf8')); if (!store || store.v !== 1 || !store.seed || typeof store.world !== 'string') throw new Error('неизвестный формат'); }
  catch (e) { console.error('Сохранение не прочитано (' + e.message + ') — оставляю копию и начинаю новый мир'); aside(); store = null; }
}
function aside() { try { fs.renameSync(SAVE, SAVE + '.bad-' + Date.now()); } catch (e) { /* ignore */ } }
if (store && process.env.SEED && +process.env.SEED !== store.seed) { console.log('SEED отличается от сохранённого (' + store.seed + ') — старое сохранение отложено, мир создаётся заново'); aside(); store = null; }
const SEED = store ? store.seed : (+process.env.SEED || (1 + Math.floor(Math.random() * 999999998)));
const GAME = process.env.GAME || path.join(__dirname, '..', 'Rust_2D_mp_step11.html');
const TICK = 1 / 20, MAX_PLAYERS = +process.env.MAX_PLAYERS || 32;
// ---------- защита и лимиты ----------
const ON_RAILWAY = !!(process.env.RAILWAY_ENVIRONMENT || process.env.RAILWAY_PUBLIC_DOMAIN);
const flag = (name, def) => process.env[name] === undefined ? def : process.env[name] !== '0';
const TRUST_PROXY = flag('TRUST_PROXY', ON_RAILWAY);        // сервер за прокси (Railway): настоящий IP берём из X-Forwarded-For
const FORCE_HTTPS = flag('FORCE_HTTPS', ON_RAILWAY);        // http -> https, ws:// без TLS не принимаем (токен не должен ходить открытым текстом)
const USE_CSP = flag('CSP', true);                          // CSP=0 — выключить, если вдруг мешает
const MAX_PER_IP = +process.env.MAX_PER_IP || 6;            // одновременных подключений с одного IP
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(x => x.trim()).filter(Boolean);   // доп. разрешённые Origin для WebSocket
const SLEEPER_MS = (process.env.SLEEPER_HOURS === undefined ? 72 : +process.env.SLEEPER_HOURS) * 3600e3;   // 0 — слиперы лежат вечно
const MAX_SLEEPERS = process.env.MAX_SLEEPERS === undefined ? 300 : +process.env.MAX_SLEEPERS;
const PLAYER_KEEP_MS = (+process.env.PLAYER_DAYS || 60) * 86400e3;   // записи давно не заходивших игроков чистим
const hostOf = req => String(req.headers.host || '').replace(/[^A-Za-z0-9.:\-\[\]]/g, '').slice(0, 100);
const protoOf = req => String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
function ipOf(req) {
  let ip = req.socket.remoteAddress || '';
  if (TRUST_PROXY) { const f = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim(); if (/^[0-9a-fA-F:.]{3,45}$/.test(f)) ip = f; }
  return ip;
}
function originOk(req) {   // браузер всегда шлёт Origin: пускаем только страницы с этого же хоста (и ALLOWED_ORIGINS); клиенты без Origin — не браузеры
  const o = req.headers.origin; if (!o) return true;
  try { const u = new URL(o); return u.host === req.headers.host || ORIGINS.includes(u.origin); } catch (e) { return false; }
}
const ipCount = new Map(), joinLog = new Map();
function joinAllowed(ip) {   // не больше 20 входов в минуту с одного IP
  const now = Date.now(), l = (joinLog.get(ip) || []).filter(t => now - t < 60000); l.push(now); joinLog.set(ip, l);
  if (joinLog.size > 5000) for (const [k, v] of joinLog) if (!v.length || now - v[v.length - 1] > 60000) joinLog.delete(k);
  return l.length <= 20;
}
let auth = null;   // регистрация по почте (создаётся после загрузки хранилища)
const issued = new Set();   // ключи токенов, выданных этим процессом (до первого сохранения игрока)

// ---------- минимальный WebSocket (RFC 6455) ----------
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
class Conn {
  constructor(socket, onMsg, onClose) {
    this.s = socket; this.buf = Buffer.alloc(0); this.frag = []; this.open = true; this.onMsg = onMsg; this.onClose = onClose; this.rx = Date.now();
    socket.setNoDelay(true);
    socket.on('data', d => { this.rx = Date.now(); this.buf = Buffer.concat([this.buf, d]); try { this.parse(); } catch (e) { this.close(); } });
    socket.on('close', () => this.close()); socket.on('error', () => this.close());
  }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 15, masked = !!(b[1] & 0x80); let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; } else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20 || !masked) throw new Error('bad frame');
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 8) { this.close(); return; }
      if (op === 9) { this.raw(0x8A, data); continue; }
      if (op === 10) continue;
      if (op === 1 || op === 0) { this.frag.push(data); if (fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = []; this.onMsg(s); } }
    }
  }
  raw(first, data) {
    if (!this.open) return; const n = data.length; let h;
    if (n < 126) h = Buffer.from([first, n]); else if (n < 65536) { h = Buffer.alloc(4); h[0] = first; h[1] = 126; h.writeUInt16BE(n, 2); } else { h = Buffer.alloc(10); h[0] = first; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
    this.s.write(Buffer.concat([h, data]));
  }
  ping() { this.raw(0x89, Buffer.alloc(0)); }
  send(str) { if (this.s.writableLength > 2e6) return; this.raw(0x81, Buffer.from(str, 'utf8')); }
  close() { if (!this.open) return; this.open = false; try { this.s.destroy(); } catch (e) { /* ignore */ } this.onClose(); }
}

// ---------- игра ----------
console.log('Генерация мира, seed =', SEED, '...'); let t0 = Date.now();
const g = loadGame(GAME, SEED), run = g.run;
run('netGhost()'); run('netIndex()'); if (process.env.PVP === '0') run('NET.pvp = false');
if (store) {
  let r = 'ERR'; try { r = run(`netWorldLoad(${JSON.stringify(store.world)})`); } catch (e) { console.error('ERR load', e && e.stack); }
  if (r === 'ok') console.log('Мир загружен из', SAVE, '| сохранённых игроков:', Object.keys(store.players || {}).length);
  else if (r === 'fp') { console.error('Сохранение не подходит к этому миру (изменилась генерация мира?) — старый файл отложен, начинаем новый мир'); aside(); store = { v: 1, seed: SEED, world: '', players: {} }; }   // fp проверяется до любых изменений, мир ещё чистый
  else { console.error('Сохранение повреждено, продолжать небезопасно — файл отложен, перезапустите сервер'); aside(); process.exit(1); }
} else store = { v: 1, seed: SEED, world: '', players: {} };
store.players = store.players || {};
auth = authMod.ENABLED ? authMod.create(store) : null;
console.log(auth ? 'Регистрация: ник+пароль' + (authMod.MAIL_ON ? ' и почта' + (authMod.DEV && !require('./mail.js').configured ? ' (DEV: коды в консоли)' : '') : ' (вход по почте выключен: задайте GMAIL_USER и GMAIL_APP_PASSWORD)') : 'Регистрация выключена (AUTH=0)');
console.log('Мир готов за', ((Date.now() - t0) / 1000).toFixed(1), 'с');

const players = new Map();   // id -> { conn, id, last }
const uidOf = key => crypto.createHash('sha256').update('uid|' + key).digest('hex').slice(0, 16);   // устойчивая личность в мире (замки, владельцы): из токена не восстановить
const keyOf = tok => typeof tok === 'string' && /^[A-Za-z0-9]{16,64}$/.test(tok) ? crypto.createHash('sha256').update(tok).digest('hex') : '';
function keepPlayer(pl) {   // снимок игрока в хранилище (по ключу токена)
  if (!pl.joined || !pl.key) return;
  try { const j = run(`netPlayerDump(${JSON.stringify(pl.id)})`); if (j) store.players[pl.key] = { at: Date.now(), name: pl.name, d: JSON.parse(j) }; } catch (e) { console.error('ERR keep', e && e.message); }
}
let saving = false;
function saveWorld() {
  if (!SAVE || saving) return; saving = true; const t = Date.now();
  try {
    for (const pl of players.values()) keepPlayer(pl);
    for (const k of Object.keys(store.players)) if (Date.now() - (store.players[k].at || 0) > PLAYER_KEEP_MS) delete store.players[k];
    store.world = String(run('netWorldDump()'));
    const tmp = SAVE + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(store)); fs.renameSync(tmp, SAVE);   // атомарно: при сбое остаётся прошлое сохранение
    console.log('Сохранено:', (fs.statSync(SAVE).size / 1048576).toFixed(2), 'МБ за', Date.now() - t, 'мс');
  } catch (e) { console.error('ERR save', e && e.message); }
  saving = false;
}
if (SAVE) { setInterval(saveWorld, SAVE_EVERY); for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { console.log('Остановка…'); saveWorld(); process.exit(0); }); }
let nextId = 1, firstId = null;
const num = (v, a, b, d) => (typeof v === 'number' && Number.isFinite(v) ? Math.max(a, Math.min(b, v)) : d);
const W = +run('W');

function onMessage(pl, raw) { try { onMessage0(pl, raw); } catch (e) { console.error('ERR msg', e && e.message); } }
function onMessage0(pl, raw) {
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  const now = Date.now(); if (now - pl.winT > 1000) { pl.winT = now; pl.cnt = 0; } if (++pl.cnt > 80) return;   // антиспам: не больше 80 сообщений в секунду
  if (m.t === 'join' && !pl.stage) {
    if (!joinAllowed(pl.ip)) { pl.conn.send(JSON.stringify({ k: 'busy' })); pl.conn.close(); return; }
    // Токен выдаёт только сервер (256 бит). Присланный клиентом принимаем лишь если он уже известен (старые игроки) — выдуманный «aaaa…» ничего не откроет.
    let key = keyOf(m.tok), tok = '';
    if (auth) {   // регистрация включена: пускаем только по токену аккаунта (или старого игрока, уже сохранённого в мире)
      if (!key || !(auth.isKnownKey(key) || store.players[key])) { pl.conn.send(JSON.stringify({ k: 'auth' })); pl.conn.close(); return; }
    } else if (!key || !(store.players[key] || issued.has(key) || [...players.values()].some(o => o.key === key))) { tok = crypto.randomBytes(32).toString('hex'); key = keyOf(tok); if (issued.size > 5000) issued.clear(); issued.add(key); }
    for (const o of [...players.values()]) if (o !== pl && o.key === key) { o.conn.send(JSON.stringify({ k: 'kick' })); o.conn.close(); }   // тот же токен входит снова — прежняя сессия закрывается (и оставляет слипера), а не ломает вход
    if (players.size > MAX_PLAYERS) { pl.conn.send(JSON.stringify({ k: 'full' })); pl.conn.close(); return; }
    pl.stage = 1; pl.rawName = (auth && auth.nickOf(key)) || String(m.name || '');   // у аккаунта с ником имя в игре — его ник pl.key = key;
    clearTimeout(pl.hs); pl.hs = setTimeout(() => { if (!pl.joined) pl.conn.close(); }, 120000);   // мир строится у клиента несколько секунд; зависшее подключение не держим
    pl.conn.send(JSON.stringify({ k: 'welcome', id: pl.id, seed: SEED, tok: tok || undefined }));   // дальше клиент сам строит мир по seed (~несколько секунд) и шлёт ready
  } else if (m.t === 'ready' && pl.stage === 1) {
    clearTimeout(pl.hs);
    pl.stage = 2; pl.joined = true; const uid = uidOf(pl.key);
    const woke = String(run(`netTake(${JSON.stringify(pl.key)})`)), sv = !woke && store.players[pl.key], saved = woke || (sv ? JSON.stringify(sv.d) : '');   // слипер (или запись о гибели во сне) важнее старого снимка
    pl.name = String(run(`netJoin(${JSON.stringify(pl.id)}, ${JSON.stringify(pl.rawName)}, ${JSON.stringify(saved)}, ${JSON.stringify(uid)}, ${!!woke})`));
    if (process.env.SPAWN_NEAR) { if (!firstId) firstId = pl.id; else run(`(() => { const a = S.players[${JSON.stringify(firstId)}], b = S.players[${JSON.stringify(pl.id)}]; if (a && b) { b.x = a.x + 120; b.y = a.y; } })()`); }   // отладка: новые игроки появляются рядом с первым
    console.log('+', pl.id, pl.name, woke ? '(проснулся)' : '', '| игроков:', players.size);
  } else if (!pl.joined) return;
  else if (m.t === 'in') {
    run(`netIn(${JSON.stringify(pl.id)},${num(m.mx, -1, 1, 0)},${num(m.my, -1, 1, 0)},${!!m.sprint},${!!m.crouch},${!!m.e},${num(m.ax, -1e5, 1e5, 0)},${num(m.ay, -1e5, 1e5, 0)},${!!m.down})`);
  } else if (m.t === 'dbg' && process.env.TEST_HOOKS && !ON_RAILWAY) { let r; try { r = String(run(String(m.code))); } catch (e) { r = 'ERR ' + e.message; } pl.conn.send(JSON.stringify({ k: 'dbg', r })); }
  else if (m.t === 'pa') {
    const a = Array.isArray(m.a) ? m.a.slice(0, 3).map(x => typeof x === 'number' ? (Number.isFinite(x) ? x : 0) : typeof x === 'boolean' ? x : String(x).slice(0, 40)) : [], c = m.ctx && typeof m.ctx === 'object' ? m.ctx : {};
    const ctx = { cc: c.cc === 'crates' || c.cc === 'props' ? c.cc : null, cn: num(c.cn, 0, 1e9, 0), dep: !!c.dep, q: String(c.q || 'all').slice(0, 8), hp: c.hp == null ? null : num(c.hp, 0, 20, 0) };
    run(`netPanelSrv(${JSON.stringify(pl.id)},${JSON.stringify(String(m.f).slice(0, 20))},${JSON.stringify(a)},${JSON.stringify(ctx)})`);
  } else if (m.t === 'rpc') {
    const a = Array.isArray(m.a) ? m.a.slice(0, 3).map(v => typeof v === 'number' ? (Number.isFinite(v) ? v : 0) : String(v).slice(0, 40)) : [];
    run(`netRpcSrv(${JSON.stringify(pl.id)},${JSON.stringify(String(m.f).slice(0, 20))},${JSON.stringify(a)})`);
  }
}

// ---------- HTTP + upgrade ----------
const server = http.createServer((req, res) => {
  const hdr = { 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' }, url = req.url.split('?')[0], host = hostOf(req);
  if (FORCE_HTTPS && protoOf(req) === 'http') { res.writeHead(301, Object.assign({ Location: 'https://' + host + req.url }, hdr)); res.end(); return; }
  if (/^\/auth\/(send|verify|register|login)$/.test(url)) return authRoute(req, res, url, hdr);
  if (url === '/healthz') { res.writeHead(200, Object.assign({ 'Content-Type': 'text/plain' }, hdr)); res.end('ok'); return; }
  if (url !== '/') { res.writeHead(404, hdr); res.end('not found'); return; }
  let html = fs.readFileSync(GAME, 'utf8').replace('<script>', '<script>window.__ONLINE__=1;' + (auth ? 'window.__AUTH__={mail:' + (authMod.MAIL_ON ? 1 : 0) + '};' : '') + '</script>\n<script>');   // страница с сервера сама подключается к нему
  const h = Object.assign({ 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' }, hdr);
  if (FORCE_HTTPS) h['Strict-Transport-Security'] = 'max-age=31536000';
  // CSP: страница может ходить только на свой сервер — даже при XSS токен из localStorage некуда отправить
  if (USE_CSP) h['Content-Security-Policy'] = ["default-src 'none'", "script-src 'unsafe-inline'", "style-src 'unsafe-inline'", "img-src 'self' data: blob:", "media-src 'self' data: blob:", "font-src 'self' data:", `connect-src 'self' ws://${host} wss://${host}`, "base-uri 'none'", "form-action 'none'", "frame-ancestors 'none'"].join('; ');
  res.writeHead(200, h); res.end(html);
});
function authRoute(req, res, url, hdr) {
  const out = (code, obj) => { res.writeHead(code, Object.assign({ 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }, hdr)); res.end(JSON.stringify(obj)); };
  if (!auth) return out(404, { err: 'off' });
  if (req.method !== 'POST' || !originOk(req) || !req.headers.origin) return out(403, { err: 'forbidden' });
  let body = '', big = false;
  req.on('data', d => { body += d; if (body.length > 2048) { big = true; req.destroy(); } });
  req.on('end', async () => {
    if (big) return;
    let m; try { m = JSON.parse(body); } catch (e) { return out(400, { err: 'bad' }); }
    if (!m || typeof m !== 'object') return out(400, { err: 'bad' });
    const ip = ipOf(req);
    try {
      if (url === '/auth/send') { const r = await auth.requestCode(m.email, ip); return out(r.ok ? 200 : r.err === 'email' ? 400 : r.err === 'mail' ? 502 : 429, r); }
      if (!authAllowed(ip)) return out(429, { err: 'limit' });
      if (url === '/auth/register') { const r = await auth.register(m.nick, m.pass, ip); return out(r.tok ? 200 : r.err === 'limit' ? 429 : r.err === 'taken' ? 409 : 400, r.tok ? { tok: r.tok, nick: r.nick } : r); }
      if (url === '/auth/login') { const r = await auth.login(m.nick, m.pass, ip); return out(r.tok ? 200 : r.err === 'wait' ? 429 : 400, r.tok ? { tok: r.tok, nick: r.nick } : r); }
      const r = auth.verify(m.email, m.code); return out(r.tok ? 200 : 400, r.tok ? { tok: r.tok } : r);
    } catch (e) { console.error('ERR auth', e && e.message); out(500, { err: 'server' }); }
  });
}
const authLog = new Map();
function authAllowed(ip) {   // не больше 30 запросов входа/регистрации/проверки кода в минуту с одного IP
  const now = Date.now(), l = (authLog.get(ip) || []).filter(t => now - t < 60000); l.push(now); authLog.set(ip, l);
  if (authLog.size > 5000) for (const [k, v] of authLog) if (!v.length || now - v[v.length - 1] > 60000) authLog.delete(k);
  return l.length <= 30;
}
server.on('upgrade', (req, socket) => {
  socket.on('error', () => {});
  const reject = line => { try { socket.write('HTTP/1.1 ' + line + '\r\nConnection: close\r\n\r\n'); } catch (e) { /* ignore */ } socket.destroy(); };
  const key = req.headers['sec-websocket-key']; if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  if (FORCE_HTTPS && protoOf(req) === 'http') return reject('426 Upgrade Required');
  if (!originOk(req)) return reject('403 Forbidden');
  const ip = ipOf(req); if ((ipCount.get(ip) || 0) >= MAX_PER_IP) return reject('429 Too Many Requests');
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  const pl = { id: 'p' + nextId++, joined: false, winT: 0, cnt: 0, name: '', ip, key: '' };
  pl.hs = setTimeout(() => { if (!pl.stage) pl.conn.close(); }, 10000);   // не прислал join за 10 с — отключаем
  pl.conn = new Conn(socket, s => onMessage(pl, s), () => {
    clearTimeout(pl.hs); const n = (ipCount.get(ip) || 1) - 1; if (n > 0) ipCount.set(ip, n); else ipCount.delete(ip);
    let slept = 0;
    if (players.has(pl.id) && pl.joined) { keepPlayer(pl); try { slept = +run(`netSleep(${JSON.stringify(pl.id)}, ${JSON.stringify(pl.key)}, ${JSON.stringify(uidOf(pl.key))})`); } catch (e) { console.error('ERR sleep', e && e.message); } }
    if (players.delete(pl.id) && pl.joined) { if (!slept) run(`netLeave(${JSON.stringify(pl.id)})`); console.log('-', pl.id, pl.name, slept ? '(уснул)' : '', '| игроков:', players.size); }
  });
  players.set(pl.id, pl);
});
setInterval(() => { const now = Date.now(); for (const pl of [...players.values()]) { if (now - pl.conn.rx > 30000) pl.conn.close(); else pl.conn.ping(); } }, 10000);   // мёртвые соединения (пропала сеть) закрываем за 30 с — тело становится слипером
setInterval(() => { try { run(`netSleeperSweep(${SLEEPER_MS}, ${MAX_SLEEPERS})`); } catch (e) { console.error('ERR sweep', e && e.message); } }, 60000);
server.listen(PORT, () => console.log('Сервер: http://localhost:' + PORT));

// ---------- игровой цикл 20 тиков/с ----------
let next = Date.now(), ticks = 0, busy = 0, lastLog = Date.now();
function loop() {
  const s = Date.now();
  try {
    run(`serverTick(${TICK})`);
    for (const pl of players.values()) if (pl.joined) { const snap = run(`netSnap(${JSON.stringify(pl.id)})`); if (snap) pl.conn.send(snap); }
  } catch (e) { console.error('ERR tick', e && e.stack ? e.stack.split('\n').slice(0, 3).join(' | ') : e); }
  try { run('netEndTick()'); } catch (e) { /* ignore */ }
  busy += Date.now() - s; ticks++;
  if (Date.now() - lastLog > 10000) {
    console.log(`тик ${(busy / ticks).toFixed(1)} мс (бюджет ${TICK * 1000}), игроков ${players.size}, мобов ${run('S.mobs.length')}`); busy = 0; ticks = 0; lastLog = Date.now();
  }
  next += TICK * 1000; const d = next - Date.now();
  if (d < -1000) next = Date.now();   // сильно отстали — не пытаемся «догнать» пачкой тиков
  setTimeout(loop, Math.max(0, d));
}
loop();
