// Загружает ИГРОВОЙ HTML-файл как есть (единый источник правды) и запускает его симуляцию без браузера.
const fs = require('fs'), vm = require('vm'), path = require('path');

function makeStub() {   // «бесконечная» заглушка для document/canvas/audio: любое обращение возвращает её же
  const proxy = new Proxy(function () {}, {
    get(_, k) {
      if (k === Symbol.toPrimitive) return () => 0;
      if (typeof k === 'symbol' || k === 'then') return undefined;
      if (k === 'children' || k === 'childNodes' || k === 'options' || k === 'touches') return [];
      return proxy;
    },
    set() { return true; }, apply() { return proxy; }, construct() { return proxy; },
  });
  return proxy;
}

function loadGame(htmlPath, seed) {
  const html = fs.readFileSync(htmlPath, 'utf8');
  const script = html.match(/<script>([\s\S]*)<\/script>/)[1];
  const stub = makeStub();
  const sb = {
    console, HEADLESS: true, setTimeout, clearTimeout, setInterval, clearInterval,
    document: stub, navigator: { userAgent: 'node', maxTouchPoints: 0 }, screen: stub, location: stub, history: stub,
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} }, performance,
    requestAnimationFrame: () => 0, cancelAnimationFrame() {}, requestIdleCallback: undefined,
    matchMedia: () => ({ matches: false, addEventListener() {} }), addEventListener() {}, removeEventListener() {},
    innerWidth: 1280, innerHeight: 720, devicePixelRatio: 1,
    Image: function () { return stub; }, Audio: function () { return stub; },
  };
  sb.window = sb;
  const ctx = vm.createContext(sb);
  vm.runInContext(script, ctx, { filename: 'game.js' });
  const run = code => vm.runInContext(code, ctx);
  run('muted = true; running = true; started = true;');
  if (seed !== undefined) run(`newWorld(${seed})`);
  return { ctx, run };
}
module.exports = { loadGame };
