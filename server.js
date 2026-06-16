import WebSocket, { WebSocketServer } from 'ws';
import http from 'http';
import { v4 as uuidv4 } from 'uuid';

const PORT = process.env.PORT || 8080;
const server = http.createServer();
const wss = new WebSocketServer({ server });

const connections = new Map();
const rooms = new Map();

wss.on('connection', (socket) => {
  const socketId = uuidv4();

  socket.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'JOIN_ROOM') {
        const { room, username, publicKey } = msg;
        connections.set(socketId, { socket, room, username, publicKey });
        if (!rooms.has(room)) rooms.set(room, new Set());
        rooms.get(room).add(socketId);
        socket.send(JSON.stringify({ type: 'JOINED', socketId, room, username }));
        broadcastToRoom(room, { type: 'USER_JOINED', username, totalUsers: rooms.get(room).size });
      } else if (msg.type === 'SEND_MESSAGE') {
        const conn = connections.get(socketId);
        if (!conn) return;
        broadcastToRoom(conn.room, {
          type: 'MESSAGE',
          from: conn.username,
          fromPublicKey: conn.publicKey,
          encryptedMessage: msg.encryptedMessage,
          timestamp: new Date().toISOString()
        });
      } else if (msg.type === 'GET_USERS') {
        const conn = connections.get(socketId);
        if (!conn) return;
        const users = [];
        rooms.get(conn.room)?.forEach(id => {
          const c = connections.get(id);
          if (c && id !== socketId) users.push({ username: c.username, publicKey: c.publicKey });
        });
        socket.send(JSON.stringify({ type: 'USERS_LIST', users }));
      }
    } catch (e) {
      console.error('Message error:', e);
      socket.send(JSON.stringify({ type: 'ERROR', message: 'Invalid message' }));
    }
  });

  socket.on('close', () => {
    const conn = connections.get(socketId);
    if (conn) {
      const { room, username } = conn;
      rooms.get(room)?.delete(socketId);
      if (rooms.get(room)?.size === 0) {
        rooms.delete(room);
      } else {
        broadcastToRoom(room, { type: 'USER_LEFT', username, totalUsers: rooms.get(room)?.size || 0 });
      }
      connections.delete(socketId);
    }
  });
});

function broadcastToRoom(room, message) {
  const str = JSON.stringify(message);
  rooms.get(room)?.forEach(id => {
    const c = connections.get(id);
    if (c?.socket.readyState === WebSocket.OPEN) {
      c.socket.send(str);
    }
  });
}

server.listen(PORT, () => {
  console.log(`🚀 SecureChat server running on ws://localhost:${PORT}`);
});