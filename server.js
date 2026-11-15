// Stock Rogue relay server v2 — rooms of up to 4 players joined with a 5-digit code.
// No dependencies (plain Node 18+).
//   Control (text JSON):
//     -> {"t":"host","max":4}            <- {"t":"room","code":"12345","slot":0}
//     -> {"t":"join","code":"12345"}      <- {"t":"joined","slot":k}   (others get {"t":"peer_in","slot":k})
//     -> {"t":"lock","on":true|false}     host only: stop / allow new joins (match in progress)
//     <- {"t":"peer_out","slot":k}        someone left;  <- {"t":"host_left"} the host left (room closes)
//   Game data (binary): forwarded to every other member, prefixed with 1 byte = sender slot.
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const MAX_FRAME = 256 * 1024;
const rooms = new Map(); // code -> { host, guest }
let nextId = 1;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain" });
  res.end(`Stock Rogue relay OK. Rooms open: ${rooms.size}\n`);
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.headers.upgrade || "").toLowerCase() !== "websocket") return socket.destroy();
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  socket.setNoDelay(true);
  const c = { id: nextId++, socket, buf: Buffer.alloc(0), frag: null, room: null, slot: -1, alive: true, open: true };
  socket.on("data", (d) => onData(c, d));
  socket.on("close", () => onClose(c));
  socket.on("error", () => socket.destroy());
});

function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

function send(c, op, payload) {
  if (c && c.open && !c.socket.destroyed) c.socket.write(frame(op, payload));
}
const sendJSON = (c, o) => send(c, 0x1, Buffer.from(JSON.stringify(o)));

function onData(c, d) {
  c.buf = Buffer.concat([c.buf, d]);
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1];
    const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_FRAME) return c.socket.destroy();
    const need = off + (masked ? 4 : 0) + len;
    if (c.buf.length < need) return;
    let payload = c.buf.subarray(off + (masked ? 4 : 0), need);
    if (masked) {
      const m = c.buf.subarray(off, off + 4);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3];
    }
    c.buf = c.buf.subarray(need);
    if (op === 0x0) { // continuation
      if (!c.frag) continue;
      c.frag.parts.push(payload);
      if (fin) { const f = c.frag; c.frag = null; onMessage(c, f.op, Buffer.concat(f.parts)); }
    } else if (op === 0x1 || op === 0x2) {
      if (fin) onMessage(c, op, payload); else c.frag = { op, parts: [payload] };
    } else if (op === 0x8) { send(c, 0x8, Buffer.alloc(0)); c.socket.end(); }
    else if (op === 0x9) send(c, 0xA, payload);
    else if (op === 0xA) c.alive = true;
  }
}

function newCode() {
  for (let i = 0; i < 100; i++) {
    const code = String(10000 + crypto.randomInt(90000));
    if (!rooms.has(code)) return code;
  }
  return null;
}

function others(r, c) { return r.members.filter((m) => m && m !== c); }

function onMessage(c, op, payload) {
  c.alive = true;
  const r = c.room && rooms.get(c.room);
  if (op === 0x2) { // game data: to everyone else, tagged with the sender's slot
    if (!r) return;
    const out = Buffer.concat([Buffer.from([c.slot]), payload]);
    for (const m of others(r, c)) send(m, 0x2, out);
    return;
  }
  let m;
  try { m = JSON.parse(payload.toString("utf8")); } catch { return; }
  if (m.t === "host" && !c.room) {
    const code = newCode();
    if (!code) return sendJSON(c, { t: "err", msg: "Server is full, try again later" });
    const max = Math.max(2, Math.min(6, Number(m.max) || 2));
    const members = new Array(max).fill(null);
    members[0] = c;
    rooms.set(code, { members, locked: false });
    c.room = code; c.slot = 0;
    sendJSON(c, { t: "room", code, slot: 0 });
    log(`room ${code} opened by #${c.id} (max ${max})`);
  } else if (m.t === "join" && !c.room) {
    const code = String(m.code || "");
    const room = rooms.get(code);
    if (!room) return sendJSON(c, { t: "err", msg: `No room with code ${code}` });
    if (room.locked) return sendJSON(c, { t: "err", msg: "That room is in a match right now. Try again in a minute." });
    const slot = room.members.indexOf(null);
    if (slot < 0) return sendJSON(c, { t: "err", msg: "That room is full" });
    room.members[slot] = c; c.room = code; c.slot = slot;
    sendJSON(c, { t: "joined", slot });
    for (const o of others(room, c)) sendJSON(o, { t: "peer_in", slot });
    log(`room ${code}: #${c.id} joined as slot ${slot}`);
  } else if (m.t === "lock" && r && c.slot === 0) {
    r.locked = !!m.on;
  }
}

function onClose(c) {
  if (!c.open) return;
  c.open = false;
  const r = c.room && rooms.get(c.room);
  if (!r) return;
  r.members[c.slot] = null;
  if (c.slot === 0) { // host left: close the room
    for (const o of r.members) if (o) { sendJSON(o, { t: "host_left" }); o.room = null; }
    rooms.delete(c.room);
    log(`room ${c.room} closed (host left)`);
  } else {
    for (const o of r.members) if (o) sendJSON(o, { t: "peer_out", slot: c.slot });
    log(`room ${c.room}: slot ${c.slot} left`);
  }
}

function log(s) { console.log(new Date().toISOString(), s); }

// drop dead connections (closed laptop, lost Wi-Fi) every 30 s
setInterval(() => {
  for (const r of rooms.values()) for (const c of r.members) {
    if (!c || !c.open) continue;
    if (!c.alive) { c.socket.destroy(); continue; }
    c.alive = false; send(c, 0x9, Buffer.alloc(0));
  }
}, 30000);

server.listen(PORT, () => log(`Stock Rogue relay listening on ${PORT}`));
