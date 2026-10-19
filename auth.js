// Регистрация/вход по почте: ввод e-mail -> 6-значный код на почту -> постоянный токен аккаунта.
// Токен = HMAC(секрет сервера, хэш почты): один и тот же на любом устройстве после подтверждения кода, поэтому прогресс, слиперы и замки
// остаются за человеком. В файле мира хранятся только хэши почт и секрет, самих адресов нет.
const crypto = require('crypto'), mail = require('./mail.js');

const DEV = process.env.AUTH === 'dev';              // коды печатаются в консоль вместо отправки (только локально)
const ENABLED = process.env.AUTH !== '0';           // вход обязателен; AUTH=0 — старый режим без регистрации
const MAIL_ON = mail.configured || DEV;             // вход по почте доступен, только если настроена отправка писем
const NICK_RE = /^[A-Za-z0-9А-Яа-яЁё_-]{3,16}$/, PASS_MIN = 6, PASS_MAX = 72;
const CODE_TTL = 10 * 60e3, RESEND_MS = 60e3, MAX_TRIES = 5, MAX_SENDS_HOUR = 5, MAX_SENDS_IP_HOUR = 10;
const EMAIL_RE = /^[A-Za-z0-9.!#$%&'*+\/=?^_`{|}~-]{1,64}@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$/;

const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const normEmail = e => { e = String(e || '').trim().toLowerCase(); return e.length <= 254 && EMAIL_RE.test(e) ? e : ''; };

function create(store) {
  if (!store.secret) store.secret = crypto.randomBytes(32).toString('hex');
  store.accounts = store.accounts || {};   // sha256(почта) -> { at }
  store.nicks = store.nicks || {};         // ник в нижнем регистре -> { nick, salt, hash (scrypt), at }
  const tokenOf = eh => crypto.createHmac('sha256', store.secret).update('tok|' + eh).digest('hex');   // 64 hex-символа
  const pending = new Map();               // почта -> { hash, exp, tries, sent, sends: [время…] }
  const ipSends = new Map();               // ip -> [время…]
  const keyHash = tok => sha(tok);         // тот же ключ, что и keyOf в server.js
  const nickTok = nl => crypto.createHmac('sha256', store.secret).update('nick|' + nl).digest('hex');
  const keys = new Set(Object.keys(store.accounts).map(eh => keyHash(tokenOf(eh))));
  const nickByKey = new Map();             // ключ токена -> ник (имя в игре задаёт сервер, подделать нельзя)
  for (const nl of Object.keys(store.nicks)) { const k = keyHash(nickTok(nl)); keys.add(k); nickByKey.set(k, store.nicks[nl].nick); }
  const scrypt = (pw, salt) => new Promise((res, rej) => crypto.scrypt(pw.normalize('NFKC'), salt, 64, { N: 16384, r: 8, p: 1 }, (e, d) => e ? rej(e) : res(d)));
  const regLog = new Map(), fails = new Map();   // ip -> [время регистраций]; ник|ip -> { n, until }

  function sweep() { const now = Date.now(); for (const [k, v] of pending) if (v.exp < now && now - v.sent > 3600e3) pending.delete(k); for (const [k, v] of ipSends) if (!v.length || now - v[v.length - 1] > 3600e3) ipSends.delete(k); }
  setInterval(sweep, 600e3).unref();

  // -> { ok: true } | { err: 'email'|'wait'|'limit'|'mail', wait? }
  async function requestCode(emailRaw, ip) {
    if (!MAIL_ON) return { err: 'off' };
    const email = normEmail(emailRaw); if (!email) return { err: 'email' };
    const now = Date.now();
    const il = (ipSends.get(ip) || []).filter(t => now - t < 3600e3); if (il.length >= MAX_SENDS_IP_HOUR) return { err: 'limit' };
    let p = pending.get(email); if (!p) { p = { hash: '', exp: 0, tries: 0, sent: 0, sends: [] }; pending.set(email, p); }
    p.sends = p.sends.filter(t => now - t < 3600e3);
    if (now - p.sent < RESEND_MS) return { err: 'wait', wait: Math.ceil((RESEND_MS - (now - p.sent)) / 1000) };
    if (p.sends.length >= MAX_SENDS_HOUR) return { err: 'limit' };
    const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
    p.sent = now; p.sends.push(now); il.push(now); ipSends.set(ip, il);   // считаем попытку до отправки: нельзя заспамить, провоцируя ошибки SMTP
    const hash = sha('code|' + email + '|' + code);
    try {
      if (DEV && !mail.configured) console.log('[AUTH dev] код для', email, '=', code);
      else await mail.send(email, 'Код входа Rust 2D: ' + code, 'Ваш код для входа в Rust 2D: ' + code + '\n\nКод действует 10 минут. Если вы его не запрашивали — просто проигнорируйте это письмо.\n');
    } catch (e) { console.error('ERR mail', e && e.message); p.sent = 0; return { err: 'mail' }; }
    p.hash = hash; p.exp = now + CODE_TTL; p.tries = 0;
    return { ok: true };
  }

  // -> { tok } | { err: 'code'|'expired'|'locked' }
  function verify(emailRaw, codeRaw) {
    const email = normEmail(emailRaw), code = String(codeRaw || '').trim();
    const p = email && pending.get(email);
    if (!p || !p.hash || p.exp < Date.now()) return { err: 'expired' };
    if (p.tries >= MAX_TRIES) { p.hash = ''; return { err: 'locked' }; }
    p.tries++;
    const ok = /^\d{6}$/.test(code) && crypto.timingSafeEqual(Buffer.from(sha('code|' + email + '|' + code)), Buffer.from(p.hash));
    if (!ok) { if (p.tries >= MAX_TRIES) p.hash = ''; return { err: p.tries >= MAX_TRIES ? 'locked' : 'code' }; }
    p.hash = '';   // код одноразовый
    const eh = sha('mail|' + email); if (!store.accounts[eh]) store.accounts[eh] = { at: Date.now() };
    const tok = tokenOf(eh); keys.add(keyHash(tok));
    return { tok, created: true };
  }

  // ---------- ник + пароль ----------
  const checkNick = n => { n = String(n || '').trim(); return NICK_RE.test(n) ? n : ''; };
  const checkPass = p => typeof p === 'string' && p.length >= PASS_MIN && p.length <= PASS_MAX;
  // -> { tok, nick } | { err: 'nick'|'pass'|'taken'|'limit' }
  async function register(nickRaw, pass, ip) {
    const nick = checkNick(nickRaw); if (!nick) return { err: 'nick' };
    if (!checkPass(pass)) return { err: 'pass' };
    const now = Date.now(), l = (regLog.get(ip) || []).filter(t => now - t < 3600e3); if (l.length >= 5) return { err: 'limit' };
    const nl = nick.toLowerCase(); if (store.nicks[nl]) return { err: 'taken' };
    l.push(now); regLog.set(ip, l);
    const salt = crypto.randomBytes(16), hash = await scrypt(pass, salt);
    if (store.nicks[nl]) return { err: 'taken' };   // пока считался хэш, ник могли занять
    store.nicks[nl] = { nick, salt: salt.toString('hex'), hash: hash.toString('hex'), at: now };
    const tok = nickTok(nl), k = keyHash(tok); keys.add(k); nickByKey.set(k, nick);
    return { tok, nick };
  }
  // -> { tok, nick } | { err: 'bad'|'wait'|'limit', wait? }   (на неверный ник и неверный пароль отвечаем одинаково)
  async function login(nickRaw, pass, ip) {
    const nl = String(nickRaw || '').trim().toLowerCase(), now = Date.now(), fk = nl + '|' + ip;
    if (!NICK_RE.test(nl) || typeof pass !== 'string' || pass.length > PASS_MAX) return { err: 'bad' };
    const f = fails.get(fk); if (f && f.until > now) return { err: 'wait', wait: Math.ceil((f.until - now) / 1000) };
    const acc = store.nicks[nl];
    const salt = acc ? Buffer.from(acc.salt, 'hex') : Buffer.alloc(16), h = await scrypt(pass, salt);   // для несуществующего ника считаем хэш впустую — время ответа не выдаёт, есть ли такой ник
    const ok = acc && crypto.timingSafeEqual(h, Buffer.from(acc.hash, 'hex'));
    if (!ok) {
      const n = (f ? f.n : 0) + 1; fails.set(fk, { n, until: n >= 5 ? now + Math.min(900e3, 30e3 * 2 ** (n - 5)) : 0 });   // с 5-й ошибки подряд пауза 30 с, 1 мин, 2 мин… до 15 мин
      if (fails.size > 5000) for (const [k, v] of fails) if (v.until < now && now - v.until > 3600e3) fails.delete(k);
      return { err: 'bad' };
    }
    fails.delete(fk);
    return { tok: nickTok(nl), nick: acc.nick };
  }
  return { requestCode, verify, register, login, isKnownKey: k => keys.has(k), nickOf: k => nickByKey.get(k) || '', mailOn: MAIL_ON };
}
module.exports = { create, ENABLED, DEV, MAIL_ON };
