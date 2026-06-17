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
const roomHistory = new Map(); // room -> array of last 50 messages (each with id, from, text, timestamp, type, file?)
const typingUsers = new Map(); // room -> Set of usernames currently typing

wss.on('connection', (socket) => {
  const socketId = uuidv4();

  socket.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      const conn = connections.get(socketId);
      if (!conn && msg.type !== 'JOIN_ROOM') return;

      switch (msg.type) {
        case 'JOIN_ROOM': {
          const { room, username, publicKey } = msg;
          if (!room || !username || !publicKey) {
            socket.send(JSON.stringify({ type: 'ERROR', message: 'Missing fields' }));
            return;
          }
          connections.set(socketId, { socket, room, username, publicKey });
          if (!rooms.has(room)) rooms.set(room, new Set());
          rooms.get(room).add(socketId);
          socket.send(JSON.stringify({ type: 'JOINED', socketId, room, username }));

          // Send history (last 50 messages) to the new user
          const history = roomHistory.get(room) || [];
          socket.send(JSON.stringify({ type: 'HISTORY', messages: history }));

          broadcastToRoom(room, { type: 'USER_JOINED', username, totalUsers: rooms.get(room).size });
          console.log(`👤 ${username} joined ${room} (Total: ${rooms.get(room).size})`);
          break;
        }

        case 'SEND_MESSAGE': {
          const { room, encryptedMessage, from, fromPublicKey, messageId, fileInfo } = msg;
          const sender = connections.get(socketId);
          if (!sender) return;
          // Store in history
          if (!roomHistory.has(room)) roomHistory.set(room, []);
          const history = roomHistory.get(room);
          history.push({
            id: messageId || uuidv4(),
            from: sender.username,
            fromPublicKey: sender.publicKey,
            encryptedMessage,
            timestamp: new Date().toISOString(),
            fileInfo: fileInfo || null,
          });
          // Keep only last 50
          if (history.length > 50) history.shift();

          // Broadcast to room, including sender info
          broadcastToRoom(room, {
            type: 'MESSAGE',
            from: sender.username,
            fromPublicKey: sender.publicKey,
            encryptedMessage,
            timestamp: new Date().toISOString(),
            messageId: messageId || uuidv4(),
            fileInfo: fileInfo || null,
          });
          break;
        }

        case 'TYPING': {
          const { room, isTyping } = msg;
          const sender = connections.get(socketId);
          if (!sender) return;
          if (!typingUsers.has(room)) typingUsers.set(room, new Set());
          const typingSet = typingUsers.get(room);
          if (isTyping) {
            typingSet.add(sender.username);
          } else {
            typingSet.delete(sender.username);
          }
          broadcastToRoom(room, {
            type: 'TYPING',
            username: sender.username,
            isTyping,
          });
          break;
        }

        case 'READ_RECEIPT': {
          const { room, messageId } = msg;
          const sender = connections.get(socketId);
          if (!sender) return;
          broadcastToRoom(room, {
            type: 'READ_RECEIPT',
            messageId,
            username: sender.username,
          });
          break;
        }

        case 'GET_USERS': {
          const conn = connections.get(socketId);
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
      // Remove from typing set
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
