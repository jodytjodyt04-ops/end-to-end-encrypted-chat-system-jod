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
const roomHistory = new Map(); // room -> array of messages
const typingUsers = new Map(); // room -> Set of usernames

wss.on('connection', (socket) => {
  const socketId = uuidv4();

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
          connections.set(socketId, { socket, room: r, username: u, publicKey });
          if (!rooms.has(r)) rooms.set(r, new Set());
          rooms.get(r).add(socketId);
          socket.send(JSON.stringify({ type: 'JOINED', socketId, room: r, username: u }));

          const history = roomHistory.get(r) || [];
          socket.send(JSON.stringify({ type: 'HISTORY', messages: history }));

          broadcastToRoom(r, { type: 'USER_JOINED', username: u, totalUsers: rooms.get(r).size });
          console.log(`👤 ${u} joined ${r}`);
          break;
        }

        case 'SEND_MESSAGE': {
          if (!conn) return;
          const { encryptedMessage, from, fromPublicKey, messageId, fileInfo, replyToId } = msg;
          if (!roomHistory.has(conn.room)) roomHistory.set(conn.room, []);
          const history = roomHistory.get(conn.room);
          const newMsg = {
            id: messageId || uuidv4(),
            from: conn.username,
            fromPublicKey: conn.publicKey,
            encryptedMessage,
            timestamp: new Date().toISOString(),
            fileInfo: fileInfo || null,
            replyToId: replyToId || null,
            deleted: false,
          };
          history.push(newMsg);
          if (history.length > 50) history.shift();
          broadcastToRoom(conn.room, { type: 'MESSAGE', ...newMsg });
          break;
        }

        case 'DELETE_MESSAGE': {
          if (!conn) return;
          const { messageId, forEveryone } = msg;
          const history = roomHistory.get(conn.room);
          if (!history) return;
          const idx = history.findIndex(m => m.id === messageId);
          if (idx === -1) return;
          const targetMsg = history[idx];
          if (forEveryone && targetMsg.from !== conn.username) return;
          if (forEveryone) {
            targetMsg.deleted = true;
            broadcastToRoom(conn.room, { type: 'MESSAGE_DELETED', messageId });
          } else {
            broadcastToRoom(conn.room, { type: 'SELF_DELETE', messageId, username: conn.username });
          }
          break;
        }

        // ---- WebRTC Signaling ----
        case 'CALL_OFFER':
        case 'CALL_ANSWER':
        case 'CALL_CANDIDATE':
        case 'CALL_END': {
          if (!conn) return;
          // Relay to target user in same room
          const targetSocketId = msg.targetSocketId;
          const targetConn = connections.get(targetSocketId);
          if (targetConn && targetConn.socket.readyState === WebSocket.OPEN) {
            targetConn.socket.send(JSON.stringify({
              type: msg.type,
              from: conn.username,
              fromSocketId: socketId,
              payload: msg.payload || null,
            }));
          }
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
            if (c && id !== socketId) users.push({ username: c.username, publicKey: c.publicKey, socketId: id });
          });
          socket.send(JSON.stringify({ type: 'USERS_LIST', users }));
          break;
        }

        case 'PING': {
          socket.send(JSON.stringify({ type: 'PONG' }));
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
      if (typingUsers.has(room)) typingUsers.get(room).delete(username);
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

process.on('SIGINT', () => {
  server.close(() => process.exit(0));
});
