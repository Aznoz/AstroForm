// Terraformer co-op relay: nepatrí do Unity, běží na Renderu (nebo kdekoli, kde je Node 18+).
// Žádné závislosti (WebSocket je napsaný ručně), takže stačí "node server.js".
//
// Protokol (pro Unity klienta):
//   ws://HOST/?role=host                  -> server pošle hostu text {"t":"room","code":"AB12"}
//   ws://HOST/?role=join&code=AB12        -> klient dostane {"t":"id","id":N}, host dostane {"t":"join","id":N}
//   klient -> host (binary): server přidá na začátek 2 bajty (id klienta, little endian)
//   host -> klient (binary): první 2 bajty = cíl (id klienta), 0xFFFF = všichni, 0xFFFE = všichni kromě id v dalších 2 bajtech
//   odchod klienta: host dostane {"t":"leave","id":N}; odchod hosta zavře celou místnost.
"use strict";
const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8080", 10);
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || "8", 10);   // klientů na místnost (bez hosta)
const MAX_ROOMS = parseInt(process.env.MAX_ROOMS || "200", 10);
const MAX_MSG = 16 * 1024 * 1024;
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

const rooms = new Map();    // code -> { host: Conn, clients: Map<id, Conn>, nextId }

// ───────────────────────── minimální WebSocket ─────────────────────────

class Conn {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frag = null;           // { op, parts: [], size }
    this.alive = true;
    this.lastSeen = Date.now();
    this.room = null;
    this.id = 0;                // 0 = host
    this.isHost = false;
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => this.onClose());
    socket.setNoDelay(true);
  }

  onData(d) {
    this.lastSeen = Date.now();
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    for (;;) {
      const f = this.parse();
      if (!f) break;
      this.onFrame(f.op, f.fin, f.payload);
      if (!this.alive) return;
    }
  }

  // vrátí jeden celý rámec nebo null, když ještě nedorazil
  parse() {
    const b = this.buf;
    if (b.length < 2) return null;
    const fin = (b[0] & 0x80) !== 0;
    const op = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) {
      if (b.length < 10) return null;
      const hi = b.readUInt32BE(2), lo = b.readUInt32BE(6);
      if (hi !== 0) { this.close(1009); return null; }
      len = lo; off = 10;
    }
    if (len > MAX_MSG) { this.close(1009); return null; }
    const need = off + (masked ? 4 : 0) + len;
    if (b.length < need) return null;
    let payload;
    if (masked) {
      const mask = b.subarray(off, off + 4);
      payload = Buffer.allocUnsafe(len);
      const src = b.subarray(off + 4, off + 4 + len);
      for (let i = 0; i < len; i++) payload[i] = src[i] ^ mask[i & 3];
    } else payload = Buffer.from(b.subarray(off, off + len));
    this.buf = b.subarray(need);
    return { op, fin, payload };
  }

  onFrame(op, fin, payload) {
    if (op === 0x8) { this.close(1000); return; }                     // close
    if (op === 0x9) { this.sendFrame(0xA, payload); return; }         // ping -> pong
    if (op === 0xA) return;                                           // pong
    if (op === 0x1 || op === 0x2) {
      if (fin) { this.onMessage(op, payload); return; }
      this.frag = { op, parts: [payload], size: payload.length };
      return;
    }
    if (op === 0x0 && this.frag) {                                    // pokračování
      this.frag.parts.push(payload);
      this.frag.size += payload.length;
      if (this.frag.size > MAX_MSG) { this.close(1009); return; }
      if (fin) {
        const all = Buffer.concat(this.frag.parts);
        const o = this.frag.op;
        this.frag = null;
        this.onMessage(o, all);
      }
    }
  }

  sendFrame(op, payload) {
    if (!this.alive || this.socket.destroyed) return;
    const len = payload.length;
    let head;
    if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
    else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
    head[0] = 0x80 | op;
    try { this.socket.write(Buffer.concat([head, payload])); } catch (e) { /* zavře se samo */ }
  }

  sendText(obj) { this.sendFrame(0x1, Buffer.from(JSON.stringify(obj))); }
  sendBinary(buf) { this.sendFrame(0x2, buf); }

  close(code) {
    if (!this.alive) return;
    const p = Buffer.alloc(2);
    p.writeUInt16BE(code || 1000, 0);
    this.sendFrame(0x8, p);
    this.alive = false;
    try { this.socket.end(); } catch (e) {}
    setTimeout(() => { try { this.socket.destroy(); } catch (e) {} }, 500);
    this.cleanup();
  }

  onClose() {
    this.alive = false;
    this.cleanup();
  }

  cleanup() {
    const room = this.room;
    if (!room) return;
    this.room = null;
    if (this.isHost) {
      for (const c of room.clients.values()) { c.room = null; c.sendText({ t: "err", m: "Host left" }); c.close(4000); }
      room.clients.clear();
      rooms.delete(room.code);
      log("room " + room.code + " closed (host left)");
    } else {
      room.clients.delete(this.id);
      if (room.host && room.host.alive) room.host.sendText({ t: "leave", id: this.id });
    }
  }

  onMessage(op, payload) {
    const room = this.room;
    if (!room || op !== 0x2) return;                    // text od klienta ignoruju
    if (this.isHost) {
      if (payload.length < 2) return;
      const target = payload.readUInt16LE(0);
      if (target === 0xFFFF) {
        const body = payload.subarray(2);
        for (const c of room.clients.values()) c.sendBinary(body);
      } else if (target === 0xFFFE) {
        if (payload.length < 4) return;
        const except = payload.readUInt16LE(2);
        const body = payload.subarray(4);
        for (const c of room.clients.values()) if (c.id !== except) c.sendBinary(body);
      } else {
        const c = room.clients.get(target);
        if (c) c.sendBinary(payload.subarray(2));
      }
    } else if (room.host && room.host.alive) {
      const head = Buffer.alloc(2);
      head.writeUInt16LE(this.id, 0);
      room.host.sendBinary(Buffer.concat([head, payload]));
    }
  }
}

// ───────────────────────── místnosti ─────────────────────────

function log(m) { console.log(new Date().toISOString() + " " + m); }

function newCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}

function onUpgrade(req, socket) {
  const url = new URL(req.url, "http://x");
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.headers["upgrade"] || "").toLowerCase() !== "websocket") { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  const c = new Conn(socket);

  const role = url.searchParams.get("role");
  if (role === "host") {
    if (rooms.size >= MAX_ROOMS) { c.sendText({ t: "err", m: "Server is full" }); c.close(1013); return; }
    const code = newCode();
    if (!code) { c.sendText({ t: "err", m: "No free room code" }); c.close(1013); return; }
    const room = { code, host: c, clients: new Map(), nextId: 1 };
    rooms.set(code, room);
    c.room = room; c.isHost = true; c.id = 0;
    c.sendText({ t: "room", code, max: MAX_PLAYERS });
    log("room " + code + " opened");
  } else if (role === "join") {
    const code = (url.searchParams.get("code") || "").toUpperCase().trim();
    const room = rooms.get(code);
    if (!room || !room.host || !room.host.alive) { c.sendText({ t: "err", m: "Room not found" }); c.close(1008); return; }
    if (room.clients.size >= MAX_PLAYERS) { c.sendText({ t: "err", m: "Room is full" }); c.close(1008); return; }
    let id = room.nextId;
    do { id = id >= 0xFFFD ? 1 : id + 1; } while (room.clients.has(id));
    room.nextId = id;
    c.room = room; c.id = id; c.isHost = false;
    room.clients.set(id, c);
    c.sendText({ t: "id", id });
    room.host.sendText({ t: "join", id });
    log("room " + code + ": client " + id + " joined (" + room.clients.size + ")");
  } else {
    c.sendText({ t: "err", m: "Unknown role" });
    c.close(1008);
  }
}

const server = http.createServer((req, res) => {
  let clients = 0;
  for (const r of rooms.values()) clients += r.clients.size;
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: true, service: "terraformer-relay", rooms: rooms.size, clients }));
});
server.on("upgrade", onUpgrade);

// keepalive: ping každých 20 s, mrtvá spojení po 70 s ticha zavřít (Render jinak nečinná spojení utíná)
setInterval(() => {
  const now = Date.now();
  for (const r of rooms.values()) {
    const all = [r.host, ...r.clients.values()];
    for (const c of all) {
      if (!c || !c.alive) continue;
      if (now - c.lastSeen > 70000) { c.close(1001); continue; }
      c.sendFrame(0x9, Buffer.alloc(0));
    }
  }
}, 20000).unref();

if (require.main === module) {
  server.listen(PORT, () => log("relay listening on :" + PORT));
}
module.exports = { server, rooms };
