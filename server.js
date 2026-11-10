// Tapball oyun sunucusu — bağımlılıksız (yalnızca Node.js), WebSocket röle + eşleştirme
// Odalar: her oyuncunun "presence" durumu odadaki herkese iletilir (maçı lider oyuncu yönetir).
// Eşleştirme: 6 kişi olursa hemen; 10 sn sonra en az 2 gerçek oyuncu varsa onlarla başlar
// (eksikler lider tarafında botla doldurulur); tek kalan oyuncuya "solo" denir, botlarla oynar.
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_MSG = 64 * 1024, MAX_ROOM = 8, WAIT_MS = 10000, MAX_MATCH = 6;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

let nextId = 1;
const rooms = new Map();   // ad -> Map(id -> client)
const queue = [];          // {c, t0}
const stats = { conn: 0, matches: 0, started: Date.now() };

// oyunun kendisi de buradan açılabilir (test için): index.html + three.min.js aynı klasörde ise
const fs = require('fs'), path = require('path'), zlib = require('zlib');
const FILES = { '/': ['index.html', 'text/html; charset=utf-8'], '/index.html': ['index.html', 'text/html; charset=utf-8'], '/three.min.js': ['three.min.js', 'text/javascript; charset=utf-8'] };
const cache = {};
for (const k in FILES) { try { const raw = fs.readFileSync(path.join(__dirname, FILES[k][0])); cache[k] = { raw, gz: zlib.gzipSync(raw, { level: 9 }) }; } catch (e) {} }
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/status' || (url === '/' && !cache['/'])) {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
    return res.end(`Tapball server · online ${stats.conn} · queue ${queue.length} · rooms ${rooms.size} · matches ${stats.matches}`);
  }
  const f = cache[url];
  if (!f) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('not found'); }
  const gz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  res.writeHead(200, { 'Content-Type': FILES[url][1], 'Cache-Control': url === '/three.min.js' ? 'public, max-age=86400' : 'no-cache', ...(gz ? { 'Content-Encoding': 'gzip' } : {}) });
  res.end(gz ? f.gz : f.raw);
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') return sock.destroy();
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { id: String(nextId++), sock, buf: Buffer.alloc(0), frag: [], room: null, p: {}, alive: true, nick: '', closed: false, cnt: 0, ct: Date.now() };
  stats.conn++;
  sock.on('data', d => onData(c, d));
  sock.on('close', () => drop(c));
  sock.on('error', () => drop(c));
  send(c, { t: 'hi', id: c.id });
});

// ---------- WebSocket çerçeveleri ----------
function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) { h = Buffer.alloc(2); h[1] = len; }
  else if (len < 65536) { h = Buffer.alloc(4); h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  h[0] = 0x80 | op; return Buffer.concat([h, payload]);
}
function sendRaw(c, op, payload) { if (!c.closed && c.sock.writable) { try { c.sock.write(frame(op, payload)); } catch (e) {} } }
function send(c, obj) { sendRaw(c, 1, Buffer.from(JSON.stringify(obj))); }
function onData(c, d) {
  c.buf = c.buf.length ? Buffer.concat([c.buf, d]) : d;
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], fin = b0 & 0x80, op = b0 & 0x0f, masked = b1 & 0x80;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG) return drop(c);
    if (!masked) return drop(c);
    if (c.buf.length < off + 4 + len) return;
    const mask = c.buf.slice(off, off + 4), data = Buffer.from(c.buf.slice(off + 4, off + 4 + len));
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
    c.buf = c.buf.slice(off + 4 + len);
    if (op === 8) { sendRaw(c, 8, Buffer.alloc(0)); return drop(c); }
    if (op === 9) { sendRaw(c, 10, data); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 0 || op === 1 || op === 2) {
      c.frag.push(data);
      if (fin) { const msg = Buffer.concat(c.frag).toString('utf8'); c.frag = []; c.alive = true; onMsg(c, msg); }
    }
  }
}

// ---------- mesajlar ----------
function onMsg(c, s) {
  // basit taşma koruması: saniyede en fazla 120 mesaj
  const now = Date.now(); if (now - c.ct > 1000) { c.ct = now; c.cnt = 0; } if (++c.cnt > 120) return;
  let m; try { m = JSON.parse(s); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  if (m.t === 'j') joinRoom(c, String(m.r || '').slice(0, 40));
  else if (m.t === 'l') leaveRoom(c);
  else if (m.t === 'p' && c.room && m.p && typeof m.p === 'object') {
    for (const k in m.p) { if (m.p[k] === null) delete c.p[k]; else c.p[k] = m.p[k]; }
    const out = JSON.stringify({ t: 'p', id: c.id, p: m.p });
    const R = rooms.get(c.room); if (R) for (const o of R.values()) if (o !== c) sendRaw(o, 1, Buffer.from(out));
  }
  else if (m.t === 'q') { c.nick = String(m.nick || '').slice(0, 16); unqueue(c); queue.push({ c, t0: Date.now() }); mmTick(); }
  else if (m.t === 'qx') { unqueue(c); mmTick(); }
}
function joinRoom(c, name) {
  if (!name) return;
  leaveRoom(c);
  let R = rooms.get(name); if (!R) { R = new Map(); rooms.set(name, R); }
  if (R.size >= MAX_ROOM) return send(c, { t: 'full', r: name });
  c.room = name; c.p = {}; R.set(c.id, c);
  send(c, { t: 's', r: name, peers: [...R.values()].filter(o => o !== c).map(o => [o.id, o.p]) });
  const out = JSON.stringify({ t: 'p', id: c.id, p: {} });
  for (const o of R.values()) if (o !== c) sendRaw(o, 1, Buffer.from(out));
}
function leaveRoom(c) {
  if (!c.room) return;
  const R = rooms.get(c.room);
  if (R) { R.delete(c.id); const out = JSON.stringify({ t: 'x', id: c.id }); for (const o of R.values()) sendRaw(o, 1, Buffer.from(out)); if (!R.size) rooms.delete(c.room); }
  c.room = null; c.p = {};
}
function drop(c) {
  if (c.closed) return; c.closed = true; stats.conn--;
  unqueue(c); leaveRoom(c); try { c.sock.destroy(); } catch (e) {}
  mmTick();
}

// ---------- eşleştirme ----------
function unqueue(c) { const i = queue.findIndex(q => q.c === c); if (i >= 0) queue.splice(i, 1); }
function code() { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let s = ''; for (let i = 0; i < 6; i++) s += A[crypto.randomInt(A.length)]; return s; }
function makeMatch(list) {
  const k = 'Q' + code(); stats.matches++;
  list.forEach((q, i) => { unqueue(q.c); send(q.c, { t: 'm', code: k, host: i === 0, n: list.length }); });
}
let lastQc = '';
function mmTick() {
  for (let i = queue.length - 1; i >= 0; i--) if (queue[i].c.closed) queue.splice(i, 1);
  while (queue.length >= MAX_MATCH) makeMatch(queue.slice(0, MAX_MATCH));
  const now = Date.now();
  if (queue.length >= 2 && now - queue[0].t0 >= WAIT_MS) makeMatch(queue.slice(0, MAX_MATCH));
  else if (queue.length === 1 && now - queue[0].t0 >= WAIT_MS) { const q = queue.shift(); send(q.c, { t: 'solo' }); }
  if (queue.length) {
    const left = Math.max(0, Math.ceil((WAIT_MS - (now - queue[0].t0)) / 1000));
    const s = queue.length + ':' + left; if (s !== lastQc) { lastQc = s; for (const q of queue) send(q.c, { t: 'qc', n: queue.length, left }); }
  } else lastQc = '';
}
setInterval(mmTick, 250);

// ---------- canlılık kontrolü ----------
setInterval(() => {
  for (const R of rooms.values()) for (const c of R.values()) ping(c);
  for (const q of queue) ping(q.c);
}, 25000);
const pinged = new WeakSet();
function ping(c) { if (pinged.has(c) && !c.alive) return drop(c); pinged.add(c); c.alive = false; sendRaw(c, 9, Buffer.alloc(0)); }

server.listen(PORT, () => console.log('Tapball server on :' + PORT));
