import WebSocket, { WebSocketServer } from 'ws';
import http from 'http';
import { v4 as uuidv4 } from 'uuid';

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('SecureChat WebSocket server is running.');
});

const wss = new WebSocketServer({ server });

const connections = new Map(); // socketId -> { socket, room, username, publicKey }
const rooms = new Map();       // room -> Set of socketIds
const roomHistory = new Map(); // room -> array of last 50 messages
const typingUsers = new Map(); // room -> Set of usernames

// Ping interval to keep connections alive
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  });
}, 30000);

wss.on('connection', (socket) => {
  const socketId = uuidv4();
  let room = null;
  let username = null;

  socket.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      const conn = connections.get(socketId);

      switch (msg.type) {
        case 'JOIN_ROOM': {
          const { room: r, username: u, publicKey } = msg;
          if (!r || !u || !publicKey) {
            socket.send(JSON.stringify({ type: 'ERROR', message: 'Missing fields' }));
            return;
          }
          room = r;
          username = u;
          connections.set(socketId, { socket, room, username, publicKey });
          if (!rooms.has(room)) rooms.set(room, new Set());
          rooms.get(room).add(socketId);
          socket.send(JSON.stringify({ type: 'JOINED', socketId, room, username }));

          // Send history
          const history = roomHistory.get(room) || [];
          socket.send(JSON.stringify({ type: 'HISTORY', messages: history }));

          broadcastToRoom(room, { type: 'USER_JOINED', username, totalUsers: rooms.get(room).size });
          console.log(`👤 ${username} joined ${room} (Total: ${rooms.get(room).size})`);
          break;
        }

        case 'SEND_MESSAGE': {
          if (!conn) return;
          const { encryptedMessage, from, fromPublicKey, messageId, fileInfo } = msg;
          // Store in history
          if (!roomHistory.has(conn.room)) roomHistory.set(conn.room, []);
          const history = roomHistory.get(conn.room);
          history.push({
            id: messageId || uuidv4(),
            from: conn.username,
            fromPublicKey: conn.publicKey,
            encryptedMessage,
            timestamp: new Date().toISOString(),
            fileInfo: fileInfo || null,
          });
          if (history.length > 50) history.shift();

          // Broadcast to room (including sender)
          broadcastToRoom(conn.room, {
            type: 'MESSAGE',
            from: conn.username,
            fromPublicKey: conn.publicKey,
            encryptedMessage,
            timestamp: new Date().toISOString(),
            messageId: messageId || uuidv4(),
            fileInfo: fileInfo || null,
          });
          break;
        }

        case 'TYPING': {
          if (!conn) return;
          const { room: r, isTyping } = msg;
          if (!typingUsers.has(r)) typingUsers.set(r, new Set());
          const typingSet = typingUsers.get(r);
          if (isTyping) typingSet.add(conn.username);
          else typingSet.delete(conn.username);
          broadcastToRoom(r, { type: 'TYPING', username: conn.username, isTyping });
          break;
        }

        case 'READ_RECEIPT': {
          if (!conn) return;
          const { room: r, messageId } = msg;
          broadcastToRoom(r, { type: 'READ_RECEIPT', messageId, username: conn.username });
          break;
        }

        case 'GET_USERS': {
          if (!conn) return;
          const users = [];
          rooms.get(conn.room)?.forEach(id => {
            const c = connections.get(id);
            if (c && id !== socketId) users.push({ username: c.username, publicKey: c.publicKey });
          });
          socket.send(JSON.stringify({ type: 'USERS_LIST', users }));
          break;
        }

        default:
          console.log('Unknown message type:', msg.type);
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
      if (typingUsers.has(room)) {
        typingUsers.get(room).delete(username);
      }
      rooms.get(room)?.delete(socketId);
      if (rooms.get(room)?.size === 0) {
        rooms.delete(room);
        roomHistory.delete(room);
        typingUsers.delete(room);
      } else {
        broadcastToRoom(room, { type: 'USER_LEFT', username, totalUsers: rooms.get(room)?.size || 0 });
      }
      connections.delete(socketId);
      console.log(`❌ ${username} left ${room}`);
    }
  });

  socket.on('error', (err) => {
    console.error(`Socket error ${socketId}:`, err);
  });

  // Handle pong to keep connection alive
  socket.on('pong', () => { /* connection alive */ });
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

// Clean up interval on server close
process.on('SIGINT', () => {
  clearInterval(pingInterval);
  server.close(() => process.exit(0));
});
