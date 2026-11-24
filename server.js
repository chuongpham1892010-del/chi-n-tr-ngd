// Chiến Trường — máy chủ trung chuyển lệnh cho lockstep.
// Máy chủ KHÔNG chạy mô phỏng: chỉ ghép phòng, chuyển lệnh theo tick và chuyển mã hash để kiểm tra đồng bộ.
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;
const MAX = 5, MAX_CMD_BYTES = 20000;
const rooms = new Map();
const wss = new WebSocketServer({ port: PORT, maxPayload: 8 * 1024 * 1024 });

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bcast = (room, o, except) => room.clients.forEach(c => { if (c !== except) send(c, o); });
function newCode() {
  const alphabet = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  for (;;) {
    let c = '';
    for (let i = 0; i < 4; i++) c += alphabet[Math.floor(Math.random() * alphabet.length)];
    if (!rooms.has(c)) return c;
  }
}
function lobby(room) { // gán lại pid theo thứ tự vào phòng và báo cho mọi người
  room.clients.forEach((c, i) => { c.pid = i; send(c, { type: 'room', code: room.code, pid: i, count: room.clients.length }); });
}

wss.on('connection', ws => {
  ws.room = null; ws.pid = -1; ws.lastT = -1;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.type !== 'string') return;
    if (m.type === 'create' && !ws.room) {
      const room = { code: newCode(), clients: [ws], started: false };
      rooms.set(room.code, room); ws.room = room; lobby(room); return;
    }
    if (m.type === 'join' && !ws.room) {
      const room = rooms.get(String(m.code || '').toUpperCase());
      if (!room || room.started) return send(ws, { type: 'err', msg: 'Không tìm thấy phòng hoặc phòng đã bắt đầu' });
      if (room.clients.length >= MAX) return send(ws, { type: 'err', msg: 'Phòng đã đủ người' });
      room.clients.push(ws); ws.room = room; lobby(room); return;
    }
    const room = ws.room; if (!room) return;
    if (m.type === 'start') {
      if (ws.pid !== 0 || room.started) return;
      const n = room.clients.length;
      if (n < 2 || !m.payload || m.payload.n !== n) return send(ws, { type: 'err', msg: 'Số người chơi không khớp' });
      room.started = true;
      room.clients.forEach((c, i) => { if (i > 0) send(c, { type: 'start', pid: i, n, payload: m.payload }); });
      return;
    }
    if (!room.started) return;
    if (m.type === 'in') {
      if (!Array.isArray(m.c) || !Number.isInteger(m.t)) return;
      if (JSON.stringify(m.c).length > MAX_CMD_BYTES) return;
      ws.lastT = m.t;
      bcast(room, { type: 'in', pid: ws.pid, t: m.t, c: m.c }, ws); return;
    }
    if (m.type === 'h') { bcast(room, { type: 'h', pid: ws.pid, t: m.t, hash: m.hash }, ws); return; }
  });
  ws.on('close', () => {
    const room = ws.room; if (!room) return;
    room.clients = room.clients.filter(c => c !== ws);
    if (room.clients.length === 0) { rooms.delete(room.code); return; }
    if (room.started) bcast(room, { type: 'drop', pid: ws.pid, last: ws.lastT });
    else lobby(room);
  });
});
console.log('Chiến Trường relay đang chạy ở cổng', PORT);
