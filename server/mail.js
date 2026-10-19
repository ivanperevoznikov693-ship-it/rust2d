// Отправка писем через SMTP (по умолчанию Gmail, порт 465 с TLS) без внешних библиотек.
//   GMAIL_USER=you@gmail.com  GMAIL_APP_PASSWORD=xxxxxxxxxxxxxxxx  (пароль приложения, не обычный пароль!)
//   SMTP_HOST / SMTP_PORT — другой почтовый сервер; SMTP_FROM — адрес отправителя (по умолчанию = GMAIL_USER)
const tls = require('tls'), net = require('net');

const USER = process.env.GMAIL_USER || '', PASS = String(process.env.GMAIL_APP_PASSWORD || '').replace(/\s+/g, '');   // Google показывает пароль группами по 4 символа с пробелами
const HOST = process.env.SMTP_HOST || 'smtp.gmail.com', PORT = +process.env.SMTP_PORT || 465;
const FROM = process.env.SMTP_FROM || USER;
const PLAIN = process.env.SMTP_INSECURE === '1' && /^(localhost|127\.0\.0\.1)$/.test(HOST);   // только для локальных тестов с поддельным SMTP-сервером
const configured = !!(USER && PASS);

const clean = s => String(s).replace(/[\r\n]+/g, ' ');   // защита от вставки заголовков
const b64 = s => Buffer.from(s, 'utf8').toString('base64');

function send(to, subject, text) {
  return new Promise((resolve, reject) => {
    if (!configured) return reject(new Error('SMTP не настроен'));
    let done = false, buf = '', step = 0, sock;
    const fin = err => { if (done) return; done = true; clearTimeout(timer); try { sock.destroy(); } catch (e) { /* ignore */ } err ? reject(err) : resolve(); };
    const timer = setTimeout(() => fin(new Error('SMTP: таймаут')), 15000);
    const body = [
      'From: Rust 2D <' + clean(FROM) + '>', 'To: <' + clean(to) + '>', 'Subject: =?UTF-8?B?' + b64(clean(subject)) + '?=',
      'Date: ' + new Date().toUTCString(), 'MIME-Version: 1.0', 'Content-Type: text/plain; charset=UTF-8', 'Content-Transfer-Encoding: base64', '',
      b64(text).replace(/(.{76})/g, '$1\r\n'),
    ].join('\r\n');
    // шаги: [что отправить, ожидаемый код ответа]
    const script = [
      [null, 220], ['EHLO rust2d', 250], ['AUTH LOGIN', 334], [b64(USER), 334], [b64(PASS), 235],
      ['MAIL FROM:<' + clean(FROM) + '>', 250], ['RCPT TO:<' + clean(to) + '>', 250], ['DATA', 354], [body + '\r\n.', 250], ['QUIT', 221],
    ];
    const onData = d => {
      buf += d.toString('utf8');
      for (;;) {   // ответ может быть многострочным ("250-..." ... "250 ..."): берём по последней строке с пробелом после кода
        const m = buf.match(/^(?:\d{3}-.*\r?\n)*(\d{3}) .*\r?\n/);
        if (!m) return;
        buf = buf.slice(m[0].length);
        const code = +m[1], want = script[step][1];
        if (code !== want) return fin(new Error('SMTP: ожидался ' + want + ', получен ' + code + (step === 4 ? ' (проверьте GMAIL_USER и пароль приложения)' : '')));
        step++;
        if (step >= script.length) return fin();
        sock.write(script[step][0] + '\r\n');
      }
    };
    sock = PLAIN ? net.connect(PORT, HOST) : tls.connect({ host: HOST, port: PORT, servername: HOST });
    sock.on(PLAIN ? 'connect' : 'secureConnect', () => { /* ждём приветствие 220 */ });
    sock.on('data', onData); sock.on('error', e => fin(e)); sock.on('close', () => fin(new Error('SMTP: соединение закрыто')));
  });
}
module.exports = { send, configured };
