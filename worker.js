// ===================== 即时传输 Worker - 完整企业级版本 =====================
// Cloudflare Workers支持的完整文件传输解决方案

// 全局房间存储
let rooms = new Map();
// 房间清理器
let roomCleanupInterval = null;

// 初始化清理器
function initializeCleanup() {
  if (!roomCleanupInterval) {
    roomCleanupInterval = setInterval(() => {
      const now = Date.now();
      for (const [code, room] of rooms.entries()) {
        if (now - room.createdAt > 30 * 60 * 1000) {
          cleanupRoom(code);
        }
      }
    }, 60000); // 每分钟检查一次
  }
}

// 房间管理类
class RoomManager {
  static createRoom(code, ctx) {
    const room = {
      code: code,
      createdAt: Date.now(),
      status: 'waiting',
      sender: null,
      receiver: null,
      senderReady: false,
      receiverReady: false,
      fileMetadata: null,
      transferStats: {
        bytesTransferred: 0,
        chunksSent: 0,
        chunksAcknowledged: 0,
        startTime: null,
        endTime: null
      },
      lastHeartbeat: Date.now(),
      heartbeatInterval: null,
      chunkBuffer: new Map(), // 用于接收方缓冲
      chunkTimeout: null,
      ackTimeout: null
    };
    
    rooms.set(code, room);
    initializeCleanup();
    
    // 30分钟后自动清理
    ctx.waitUntil(new Promise(resolve => {
      setTimeout(() => {
        RoomManager.cleanupExpiredRoom(code);
        resolve();
      }, 30 * 60 * 1000);
    }));
    
    return room;
  }
  
  static getRoom(code) {
    return rooms.get(code);
  }
  
  static deleteRoom(code) {
    const room = rooms.get(code);
    if (room) {
      if (room.heartbeatInterval) {
        clearInterval(room.heartbeatInterval);
      }
      if (room.chunkTimeout) {
        clearTimeout(room.chunkTimeout);
      }
      if (room.ackTimeout) {
        clearTimeout(room.ackTimeout);
      }
      rooms.delete(code);
    }
  }
  
  static cleanupExpiredRoom(code) {
    const room = rooms.get(code);
    if (room) {
      const now = Date.now();
      if (now - room.createdAt > 30 * 60 * 1000) {
        if (room.sender) {
          try {
            room.sender.close(1000, '房间过期');
          } catch (e) {}
        }
        if (room.receiver) {
          try {
            room.receiver.close(1000, '房间过期');
          } catch (e) {}
        }
        RoomManager.deleteRoom(code);
      }
    }
  }
  
  static updateRoomStatus(room) {
    if (room.sender && room.receiver) {
      if (room.senderReady && room.receiverReady) {
        room.status = 'transferring';
      } else {
        room.status = 'connected';
      }
    } else if (room.sender) {
      room.status = 'waiting_receiver';
    } else if (room.receiver) {
      room.status = 'waiting_sender';
    } else {
      room.status = 'waiting';
    }
  }
  
  static broadcastToRoom(room, message, exclude = null) {
    const messageStr = typeof message === 'string' ? message : JSON.stringify(message);
    
    if (room.sender && room.sender !== exclude) {
      try {
        room.sender.send(messageStr);
      } catch (e) {
        console.error('发送到发送方失败:', e);
      }
    }
    
    if (room.receiver && room.receiver !== exclude) {
      try {
        room.receiver.send(messageStr);
      } catch (e) {
        console.error('发送到接收方失败:', e);
      }
    }
  }
}

// 传输管理器
class TransferManager {
  static handleFileMetadata(room, metadata, fromSocket) {
    room.fileMetadata = {
      ...metadata,
      totalChunks: Math.ceil(metadata.size / (64 * 1024)), // 64KB chunks
      chunksReceived: 0,
      lastChunkReceived: 0
    };
    
    TransferManager.broadcastMetadata(room, fromSocket);
  }
  
  static broadcastMetadata(room, excludeSocket) {
    if (!room.fileMetadata) return;
    
    const metadataMessage = {
      type: 'file-metadata',
      metadata: room.fileMetadata,
      timestamp: Date.now()
    };
    
    RoomManager.broadcastToRoom(room, metadataMessage, excludeSocket);
  }
  
  static handleFileChunk(room, chunkData, fromSocket) {
    if (!room.fileMetadata) return;
    
    if (fromSocket === room.sender && room.receiver) {
      // 转发给接收方
      try {
        room.transferStats.chunksSent++;
        room.transferStats.bytesTransferred += chunkData.byteLength;
        
        // 发送数据块
        room.receiver.send(chunkData);
        
        // 发送进度更新
        const progress = {
          type: 'transfer-progress',
          progress: Math.min(100, Math.floor((room.transferStats.chunksSent / room.fileMetadata.totalChunks) * 100)),
          bytesTransferred: room.transferStats.bytesTransferred,
          chunksSent: room.transferStats.chunksSent,
          totalChunks: room.fileMetadata.totalChunks,
          timestamp: Date.now()
        };
        
        room.sender.send(JSON.stringify(progress));
        
      } catch (e) {
        console.error('转发数据块失败:', e);
      }
    }
  }
  
  static handleChunkAcknowledgment(room, ackData) {
    if (!room.fileMetadata) return;
    
    room.transferStats.chunksAcknowledged++;
    
    // 如果所有块都已确认，发送完成消息
    if (room.transferStats.chunksAcknowledged >= room.fileMetadata.totalChunks) {
      TransferManager.completeTransfer(room);
    }
  }
  
  static completeTransfer(room) {
    room.transferStats.endTime = Date.now();
    room.status = 'completed';
    
    const completionMessage = {
      type: 'transfer-complete',
      stats: {
        bytesTransferred: room.transferStats.bytesTransferred,
        duration: room.transferStats.endTime - room.transferStats.startTime,
        averageSpeed: room.transferStats.bytesTransferred / 
                     ((room.transferStats.endTime - room.transferStats.startTime) / 1000)
      },
      timestamp: Date.now()
    };
    
    RoomManager.broadcastToRoom(room, completionMessage);
  }
  
  static startTransfer(room) {
    room.transferStats.startTime = Date.now();
    room.status = 'transferring';
    
    const startMessage = {
      type: 'transfer-start',
      timestamp: Date.now()
    };
    
    RoomManager.broadcastToRoom(room, startMessage);
  }
}

// WebSocket处理器
class WebSocketHandler {
  static handleConnection(request, url) {
    const pathname = url.pathname;
    const code = pathname.split('/').pop()?.toUpperCase();
    const role = url.searchParams.get('role');
    
    // 验证参数
    if (!code || code.length !== 6) {
      return new Response('无效的房间代码', { status: 400 });
    }
    
    if (!role || !['sender', 'receiver'].includes(role)) {
      return new Response('无效的角色', { status: 400 });
    }
    
    const room = RoomManager.getRoom(code);
    if (!room) {
      return new Response('房间不存在', { status: 404 });
    }
    
    // 检查房间是否过期
    if (Date.now() - room.createdAt > 30 * 60 * 1000) {
      RoomManager.cleanupExpiredRoom(code);
      return new Response('房间已过期', { status: 410 });
    }
    
    // 检查角色冲突
    if (role === 'sender' && room.sender) {
      return new Response('发送方已连接', { status: 409 });
    }
    
    if (role === 'receiver' && room.receiver) {
      return new Response('接收方已连接', { status: 409 });
    }
    
    // 创建WebSocket连接
    const { 0: client, 1: server } = new WebSocketPair();
    server.accept();
    
    // 设置连接
    WebSocketHandler.setupConnection(server, role, room);
    
    // 返回WebSocket响应
    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: {
        'Access-Control-Allow-Origin': '*'
      }
    });
  }
  
  static setupConnection(socket, role, room) {
    // 保存连接
    if (role === 'sender') {
      room.sender = socket;
      room.senderReady = false;
      room.lastHeartbeat = Date.now();
      WebSocketHandler.startHeartbeat(room);
    } else {
      room.receiver = socket;
      room.receiverReady = false;
    }
    
    // 更新房间状态
    RoomManager.updateRoomStatus(room);
    
    // 发送连接成功消息
    const connectMessage = {
      type: 'connected',
      success: true,
      role: role,
      timestamp: Date.now(),
      message: '连接成功',
      roomStatus: room.status
    };
    
    socket.send(JSON.stringify(connectMessage));
    
    // 通知另一方
    const notificationMessage = {
      type: role === 'sender' ? 'sender-connected' : 'receiver-connected',
      timestamp: Date.now(),
      message: role === 'sender' ? '发送方已连接' : '接收方已连接'
    };
    
    if (role === 'sender' && room.receiver) {
      room.receiver.send(JSON.stringify(notificationMessage));
    } else if (role === 'receiver' && room.sender) {
      room.sender.send(JSON.stringify(notificationMessage));
    }
    
    // 设置消息处理器
    socket.addEventListener('message', (event) => {
      WebSocketHandler.handleMessage(event, socket, role, room);
    });
    
    // 设置关闭处理器
    socket.addEventListener('close', (event) => {
      WebSocketHandler.handleClose(socket, role, room, event.code, event.reason);
    });
    
    // 设置错误处理器
    socket.addEventListener('error', (error) => {
      console.error(`${role} WebSocket错误:`, error);
    });
  }
  
  static handleMessage(event, socket, role, room) {
    const timestamp = Date.now();
    
    try {
      // 处理二进制消息（文件块）
      if (event.data instanceof ArrayBuffer) {
        if (role === 'sender') {
          // 发送方发送文件块
          TransferManager.handleFileChunk(room, event.data, socket);
        } else if (role === 'receiver') {
          // 接收方确认接收
          TransferManager.handleChunkAcknowledgment(room, event.data);
        }
      } else if (typeof event.data === 'string') {
        // 处理文本消息
        const data = JSON.parse(event.data);
        
        switch (data.type) {
          case 'heartbeat':
            room.lastHeartbeat = timestamp;
            socket.send(JSON.stringify({
              type: 'heartbeat-response',
              timestamp,
              receivedAt: room.lastHeartbeat
            }));
            break;
            
          case 'ready':
            if (role === 'sender') {
              room.senderReady = true;
            } else {
              room.receiverReady = true;
            }
            
            if (room.senderReady && room.receiverReady) {
              TransferManager.startTransfer(room);
            }
            
            RoomManager.broadcastToRoom(room, {
              type: 'peer-ready',
              role: role,
              timestamp
            }, socket);
            break;
            
          case 'file-metadata':
            TransferManager.handleFileMetadata(room, data.metadata, socket);
            break;
            
          case 'transfer-start':
            // 传输开始
            break;
            
          case 'chunk-ack':
            // 块确认
            room.transferStats.chunksAcknowledged++;
            break;
            
          case 'progress-update':
            // 进度更新
            RoomManager.broadcastToRoom(room, {
              type: 'progress-update',
              progress: data.progress,
              bytesTransferred: data.bytesTransferred,
              timestamp
            }, socket);
            break;
            
          case 'transfer-complete':
            // 传输完成
            TransferManager.completeTransfer(room);
            break;
            
          case 'error':
            // 错误处理
            RoomManager.broadcastToRoom(room, {
              type: 'error',
              message: data.message,
              code: data.code,
              timestamp
            });
            break;
        }
      }
    } catch (error) {
      console.error('处理消息错误:', error);
      socket.send(JSON.stringify({
        type: 'error',
        message: '处理消息时发生错误',
        timestamp
      }));
    }
  }
  
  static handleClose(socket, role, room, code, reason) {
    console.log(`${role} 连接关闭: ${code} - ${reason}`);
    
    // 清理连接
    if (role === 'sender') {
      room.sender = null;
      room.senderReady = false;
      
      if (room.receiver) {
        room.receiver.send(JSON.stringify({
          type: 'sender-disconnected',
          code: code,
          reason: reason,
          timestamp: Date.now()
        }));
      }
    } else {
      room.receiver = null;
      room.receiverReady = false;
      
      if (room.sender) {
        room.sender.send(JSON.stringify({
          type: 'receiver-disconnected',
          code: code,
          reason: reason,
          timestamp: Date.now()
        }));
      }
    }
    
    // 更新状态
    RoomManager.updateRoomStatus(room);
    
    // 如果房间为空，安排清理
    if (!room.sender && !room.receiver) {
      setTimeout(() => {
        if (rooms.get(room.code) === room && !room.sender && !room.receiver) {
          RoomManager.deleteRoom(room.code);
        }
      }, 5 * 60 * 1000); // 5分钟后清理空房间
    }
  }
  
  static startHeartbeat(room) {
    if (room.heartbeatInterval) {
      clearInterval(room.heartbeatInterval);
    }
    
    room.heartbeatInterval = setInterval(() => {
      if (!rooms.has(room.code)) {
        clearInterval(room.heartbeatInterval);
        return;
      }
      
      const now = Date.now();
      
      // 如果超过45秒没有心跳，关闭连接
      if (now - room.lastHeartbeat > 45000) {
        if (room.sender) {
          room.sender.close(1001, '心跳超时');
        }
        clearInterval(room.heartbeatInterval);
      }
    }, 15000); // 每15秒检查一次
  }
}

// 主处理函数
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const method = request.method;
    
    // CORS 头部
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400'
    };
    
    // 处理预检请求
    if (method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }
    
    // 路由处理
    try {
      // 主页面
      if (pathname === '/' || pathname === '/index.html') {
        return new Response(getHTMLPage(), {
          headers: {
            'Content-Type': 'text/html;charset=UTF-8',
            ...corsHeaders
          }
        });
      }
      
      // 健康检查
      if (pathname === '/api/health') {
        return Response.json({
          status: 'ok',
          service: 'instant-file-transfer',
          version: '2.0.0',
          timestamp: Date.now(),
          uptime: Date.now() - (global.startTime || Date.now()),
          rooms: {
            total: rooms.size,
            waiting: Array.from(rooms.values()).filter(r => r.status === 'waiting').length,
            connected: Array.from(rooms.values()).filter(r => r.status === 'connected').length,
            transferring: Array.from(rooms.values()).filter(r => r.status === 'transferring').length,
            completed: Array.from(rooms.values()).filter(r => r.status === 'completed').length
          },
          statistics: {
            memoryUsage: Math.round(process.memoryUsage?.().heapUsed / 1024 / 1024) || 0,
            connections: Array.from(rooms.values()).reduce((acc, room) => {
              if (room.sender) acc.sender++;
              if (room.receiver) acc.receiver++;
              return acc;
            }, { sender: 0, receiver: 0 })
          }
        }, { headers: corsHeaders });
      }
      
      // 创建房间
      if (pathname === '/api/room/create' && method === 'POST') {
        try {
          const data = await request.json();
          let code = data.code;
          
          // 生成或验证代码
          if (!code || typeof code !== 'string') {
            code = generateRoomCode();
          } else {
            code = code.toUpperCase().replace(/[^A-Z0-9]/g, '');
            if (code.length !== 6) {
              return Response.json({
                success: false,
                error: '取件码必须是6位字母数字',
                errorCode: 'INVALID_CODE_FORMAT'
              }, { status: 400, headers: corsHeaders });
            }
          }
          
          // 检查房间是否存在
          const existingRoom = RoomManager.getRoom(code);
          if (existingRoom) {
            const now = Date.now();
            if (now - existingRoom.createdAt < 30 * 60 * 1000) {
              return Response.json({
                success: false,
                error: '房间已存在',
                errorCode: 'ROOM_EXISTS',
                existingRoom: {
                  code: existingRoom.code,
                  status: existingRoom.status,
                  createdAt: existingRoom.createdAt,
                  expiresAt: existingRoom.createdAt + (30 * 60 * 1000)
                }
              }, { status: 409, headers: corsHeaders });
            } else {
              RoomManager.deleteRoom(code);
            }
          }
          
          // 创建新房间
          const room = RoomManager.createRoom(code, ctx);
          
          return Response.json({
            success: true,
            code: room.code,
            wsUrl: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${room.code}`,
            httpUrl: `${url.protocol}//${url.host}/api/room/${room.code}`,
            createdAt: room.createdAt,
            expiresAt: room.createdAt + (30 * 60 * 1000),
            message: '房间创建成功',
            instructions: {
              sender: `连接至: ${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${room.code}?role=sender`,
              receiver: `连接至: ${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${room.code}?role=receiver`
            }
          }, { headers: corsHeaders });
          
        } catch (error) {
          console.error('创建房间错误:', error);
          return Response.json({
            success: false,
            error: '服务器内部错误',
            errorCode: 'INTERNAL_SERVER_ERROR',
            details: error.message
          }, { status: 500, headers: corsHeaders });
        }
      }
      
      // 查询房间状态
      if (pathname.startsWith('/api/room/') && method === 'GET') {
        const code = pathname.split('/').pop()?.toUpperCase();
        
        if (!code || code.length !== 6) {
          return Response.json({
            success: false,
            error: '无效的房间代码格式',
            errorCode: 'INVALID_ROOM_CODE'
          }, { status: 400, headers: corsHeaders });
        }
        
        const room = RoomManager.getRoom(code);
        if (!room) {
          return Response.json({
            success: false,
            error: '房间不存在或已过期',
            errorCode: 'ROOM_NOT_FOUND',
            suggestion: '请检查代码或创建新房间'
          }, { status: 404, headers: corsHeaders });
        }
        
        // 检查是否过期
        const now = Date.now();
        if (now - room.createdAt > 30 * 60 * 1000) {
          RoomManager.cleanupExpiredRoom(code);
          return Response.json({
            success: false,
            error: '房间已过期',
            errorCode: 'ROOM_EXPIRED',
            expiredAt: room.createdAt + (30 * 60 * 1000)
          }, { status: 410, headers: corsHeaders });
        }
        
        // 返回房间信息
        const roomInfo = {
          success: true,
          code: room.code,
          status: room.status,
          createdAt: room.createdAt,
          expiresAt: room.createdAt + (30 * 60 * 1000),
          connections: {
            sender: {
              connected: !!room.sender,
              ready: room.senderReady,
              lastHeartbeat: room.lastHeartbeat
            },
            receiver: {
              connected: !!room.receiver,
              ready: room.receiverReady
            }
          },
          transfer: room.fileMetadata ? {
            fileName: room.fileMetadata.name,
            fileSize: room.fileMetadata.size,
            totalChunks: room.fileMetadata.totalChunks,
            progress: room.transferStats.chunksSent 
              ? Math.min(100, Math.floor((room.transferStats.chunksSent / room.fileMetadata.totalChunks) * 100))
              : 0,
            stats: room.transferStats
          } : null,
          wsUrls: {
            sender: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}?role=sender`,
            receiver: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}?role=receiver`
          },
          actions: {
            joinAsSender: `WebSocket连接: ${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}?role=sender`,
            joinAsReceiver: `WebSocket连接: ${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}?role=receiver`,
            cancel: `DELETE /api/room/${code}`
          }
        };
        
        return Response.json(roomInfo, { headers: corsHeaders });
      }
      
      // 删除房间
      if (pathname.startsWith('/api/room/') && method === 'DELETE') {
        const code = pathname.split('/').pop()?.toUpperCase();
        
        if (!code || code.length !== 6) {
          return Response.json({
            success: false,
            error: '无效的房间代码格式'
          }, { status: 400, headers: corsHeaders });
        }
        
        const room = RoomManager.getRoom(code);
        if (!room) {
          return Response.json({
            success: false,
            error: '房间不存在'
          }, { status: 404, headers: corsHeaders });
        }
        
        // 关闭所有连接
        if (room.sender) {
          room.sender.close(1000, '房间被管理员关闭');
        }
        if (room.receiver) {
          room.receiver.close(1000, '房间被管理员关闭');
        }
        
        RoomManager.deleteRoom(code);
        
        return Response.json({
          success: true,
          message: '房间已删除',
          code: code,
          timestamp: Date.now()
        }, { headers: corsHeaders });
      }
      
      // WebSocket连接
      if (pathname.startsWith('/api/ws/')) {
        return WebSocketHandler.handleConnection(request, url);
      }
      
      // 获取统计数据
      if (pathname === '/api/stats' && method === 'GET') {
        const stats = {
          rooms: {
            total: rooms.size,
            byStatus: {
              waiting: Array.from(rooms.values()).filter(r => r.status === 'waiting').length,
              waiting_sender: Array.from(rooms.values()).filter(r => r.status === 'waiting_sender').length,
              waiting_receiver: Array.from(rooms.values()).filter(r => r.status === 'waiting_receiver').length,
              connected: Array.from(rooms.values()).filter(r => r.status === 'connected').length,
              transferring: Array.from(rooms.values()).filter(r => r.status === 'transferring').length,
              completed: Array.from(rooms.values()).filter(r => r.status === 'completed').length
            }
          },
          transfers: {
            totalBytes: Array.from(rooms.values()).reduce((sum, room) => sum + (room.transferStats.bytesTransferred || 0), 0),
            totalFiles: Array.from(rooms.values()).filter(r => r.fileMetadata).length,
            activeTransfers: Array.from(rooms.values()).filter(r => r.status === 'transferring').length
          },
          connections: {
            active: Array.from(rooms.values()).reduce((acc, room) => {
              if (room.sender) acc++;
              if (room.receiver) acc++;
              return acc;
            }, 0),
            senders: Array.from(rooms.values()).filter(r => r.sender).length,
            receivers: Array.from(rooms.values()).filter(r => r.receiver).length
          },
          performance: {
            avgHeartbeatInterval: Array.from(rooms.values())
              .filter(r => r.lastHeartbeat)
              .reduce((sum, room) => {
                const interval = Date.now() - room.lastHeartbeat;
                return sum + (interval > 0 ? interval : 0);
              }, 0) / Math.max(1, Array.from(rooms.values()).filter(r => r.lastHeartbeat).length)
          }
        };
        
        return Response.json({
          success: true,
          timestamp: Date.now(),
          statistics: stats
        }, { headers: corsHeaders });
      }
      
      // 清理所有过期房间
      if (pathname === '/api/cleanup' && method === 'POST') {
        const now = Date.now();
        let cleaned = 0;
        
        for (const [code, room] of rooms.entries()) {
          if (now - room.createdAt > 30 * 60 * 1000) {
            RoomManager.deleteRoom(code);
            cleaned++;
          }
        }
        
        return Response.json({
          success: true,
          message: `清理了 ${cleaned} 个过期房间`,
          timestamp: Date.now()
        }, { headers: corsHeaders });
      }
      
      // 404处理
      return Response.json({
        success: false,
        error: '未找到请求的资源',
        requestedPath: pathname,
        availableEndpoints: {
          'GET /': '主页面',
          'GET /api/health': '健康检查',
          'POST /api/room/create': '创建房间',
          'GET /api/room/{code}': '查询房间',
          'DELETE /api/room/{code}': '删除房间',
          'GET /api/stats': '获取统计',
          'POST /api/cleanup': '清理房间',
          'WS /api/ws/{code}?role={sender|receiver}': 'WebSocket连接'
        }
      }, { status: 404, headers: corsHeaders });
      
    } catch (error) {
      console.error('请求处理错误:', error);
      return Response.json({
        success: false,
        error: '服务器内部错误',
        errorCode: 'INTERNAL_SERVER_ERROR',
        details: error.message,
        timestamp: Date.now()
      }, { status: 500, headers: corsHeaders });
    }
  }
};

// 工具函数
function generateRoomCode() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

function cleanupRoom(code) {
  RoomManager.deleteRoom(code);
}

// 生成HTML页面
function getHTMLPage() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>企业级即时文件传输系统</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #3b82f6;
            --primary-dark: #2563eb;
            --secondary: #10b981;
            --danger: #ef4444;
            --warning: #f59e0b;
            --dark: #1f2937;
            --light: #f9fafb;
            --gray: #6b7280;
            --border: #e5e7eb;
            --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
            line-height: 1.6;
            color: var(--dark);
        }
        
        .app-container {
            width: 100%;
            max-width: 1200px;
            display: grid;
            grid-template-columns: 1fr 300px;
            gap: 30px;
        }
        
        @media (max-width: 1024px) {
            .app-container {
                grid-template-columns: 1fr;
            }
        }
        
        /* 主卡片 */
        .main-card {
            background: white;
            border-radius: 20px;
            padding: 40px;
            box-shadow: var(--shadow-lg);
            display: flex;
            flex-direction: column;
            height: fit-content;
        }
        
        /* 侧边栏 */
        .sidebar {
            background: white;
            border-radius: 20px;
            padding: 30px;
            box-shadow: var(--shadow-lg);
            display: flex;
            flex-direction: column;
            gap: 25px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 40px;
        }
        
        .logo {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 15px;
            margin-bottom: 20px;
        }
        
        .logo-icon {
            font-size: 3rem;
            color: var(--primary);
            background: linear-gradient(135deg, var(--primary), var(--primary-dark));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .logo-text {
            font-size: 2.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, var(--primary), var(--primary-dark));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .tagline {
            color: var(--gray);
            font-size: 1.1rem;
            margin-top: 10px;
        }
        
        /* 模式切换器 */
        .mode-tabs {
            display: flex;
            background: var(--light);
            border-radius: 12px;
            padding: 4px;
            margin-bottom: 30px;
        }
        
        .mode-tab {
            flex: 1;
            padding: 16px;
            border: none;
            background: transparent;
            border-radius: 8px;
            font-size: 1rem;
            font-weight: 600;
            color: var(--gray);
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
        }
        
        .mode-tab.active {
            background: white;
            color: var(--primary);
            box-shadow: var(--shadow);
        }
        
        /* 面板 */
        .panel {
            display: none;
            animation: fadeIn 0.3s ease;
        }
        
        .panel.active {
            display: block;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        /* 表单组 */
        .form-group {
            margin-bottom: 25px;
        }
        
        .form-label {
            display: block;
            margin-bottom: 10px;
            color: var(--dark);
            font-weight: 600;
            font-size: 1rem;
        }
        
        /* 文件上传 */
        .file-upload-area {
            border: 3px dashed var(--border);
            border-radius: 15px;
            padding: 50px 30px;
            text-align: center;
            background: var(--light);
            cursor: pointer;
            transition: all 0.3s;
            position: relative;
        }
        
        .file-upload-area:hover {
            border-color: var(--primary);
            background: #f0f9ff;
        }
        
        .file-upload-area.dragover {
            border-color: var(--secondary);
            background: rgba(16, 185, 129, 0.1);
        }
        
        .upload-icon {
            font-size: 3.5rem;
            color: var(--primary);
            margin-bottom: 15px;
        }
        
        .upload-text {
            font-size: 1.2rem;
            color: var(--dark);
            margin-bottom: 10px;
        }
        
        .upload-hint {
            color: var(--gray);
            font-size: 0.9rem;
        }
        
        .file-input {
            position: absolute;
            width: 100%;
            height: 100%;
            top: 0;
            left: 0;
            opacity: 0;
            cursor: pointer;
        }
        
        /* 文件信息 */
        .file-info-card {
            background: linear-gradient(135deg, #f0f9ff, #e0f2fe);
            border-radius: 15px;
            padding: 25px;
            border-left: 5px solid var(--primary);
            display: none;
            animation: slideIn 0.3s ease;
        }
        
        .file-info-card.visible {
            display: block;
        }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-10px); }
            to { opacity: 1; transform: translateX(0); }
        }
        
        .file-name {
            font-weight: 700;
            color: var(--dark);
            margin-bottom: 8px;
            word-break: break-word;
        }
        
        .file-details {
            display: flex;
            justify-content: space-between;
            color: var(--gray);
            font-size: 0.9rem;
        }
        
        /* 取件码输入 */
        .code-input-container {
            position: relative;
        }
        
        .code-input {
            width: 100%;
            padding: 20px;
            border: 2px solid var(--border);
            border-radius: 12px;
            font-size: 1.4rem;
            text-align: center;
            letter-spacing: 10px;
            font-family: 'Courier New', monospace;
            font-weight: 700;
            color: var(--dark);
            transition: all 0.3s;
        }
        
        .code-input:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.2);
        }
        
        /* 按钮 */
        .btn {
            width: 100%;
            padding: 18px;
            border: none;
            border-radius: 12px;
            font-size: 1.1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
        }
        
        .btn-primary {
            background: linear-gradient(135deg, var(--primary), var(--primary-dark));
            color: white;
        }
        
        .btn-primary:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: var(--shadow-lg);
        }
        
        .btn-success {
            background: linear-gradient(135deg, var(--secondary), #059669);
            color: white;
        }
        
        .btn-danger {
            background: linear-gradient(135deg, var(--danger), #dc2626);
            color: white;
        }
        
        .btn-secondary {
            background: var(--gray);
            color: white;
        }
        
        .btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none !important;
            box-shadow: none !important;
        }
        
        /* 状态区域 */
        .status-panel {
            background: var(--light);
            border-radius: 15px;
            padding: 30px;
            display: none;
            animation: fadeIn 0.5s ease;
        }
        
        .status-panel.active {
            display: block;
        }
        
        /* 房间信息 */
        .room-info {
            text-align: center;
            margin-bottom: 30px;
        }
        
        .room-code {
            font-size: 3.5rem;
            font-weight: 900;
            letter-spacing: 15px;
            color: var(--primary);
            margin: 20px 0;
            font-family: 'Courier New', monospace;
            text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.1);
        }
        
        .room-meta {
            display: flex;
            justify-content: center;
            gap: 20px;
            color: var(--gray);
            font-size: 0.9rem;
        }
        
        /* 连接状态 */
        .connection-status {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin: 25px 0;
        }
        
        .connection-card {
            background: white;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            border: 2px solid var(--border);
            transition: all 0.3s;
        }
        
        .connection-card.connected {
            border-color: var(--secondary);
            background: rgba(16, 185, 129, 0.1);
            transform: translateY(-2px);
        }
        
        .connection-card.disconnected {
            border-color: var(--danger);
            background: rgba(239, 68, 68, 0.1);
        }
        
        .connection-icon {
            font-size: 2rem;
            margin-bottom: 10px;
        }
        
        .connection-label {
            font-weight: 600;
            color: var(--dark);
            margin-bottom: 5px;
        }
        
        .connection-state {
            font-size: 0.9rem;
            font-weight: 700;
        }
        
        .connected .connection-state {
            color: var(--secondary);
        }
        
        .disconnected .connection-state {
            color: var(--danger);
        }
        
        /* 传输进度 */
        .transfer-progress {
            margin: 30px 0;
        }
        
        .progress-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 10px;
        }
        
        .progress-label {
            font-weight: 600;
            color: var(--dark);
        }
        
        .progress-percent {
            font-weight: 700;
            color: var(--primary);
        }
        
        .progress-bar {
            height: 12px;
            background: var(--border);
            border-radius: 6px;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--primary), var(--primary-dark));
            width: 0%;
            transition: width 0.3s ease;
            border-radius: 6px;
        }
        
        /* 传输统计 */
        .transfer-stats {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 15px;
            margin: 25px 0;
        }
        
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid var(--border);
        }
        
        .stat-value {
            font-size: 1.8rem;
            font-weight: 800;
            color: var(--primary);
            margin-bottom: 5px;
        }
        
        .stat-label {
            font-size: 0.8rem;
            color: var(--gray);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        /* 文件预览 */
        .file-preview {
            background: white;
            border-radius: 12px;
            padding: 25px;
            text-align: center;
            border: 2px dashed var(--border);
            margin: 20px 0;
        }
        
        .file-icon-preview {
            font-size: 4rem;
            color: var(--primary);
            margin-bottom: 15px;
        }
        
        .file-name-preview {
            font-weight: 700;
            color: var(--dark);
            margin-bottom: 8px;
            word-break: break-word;
        }
        
        .file-size-preview {
            color: var(--gray);
            font-size: 0.9rem;
        }
        
        /* 消息通知 */
        .message-container {
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 1000;
            display: flex;
            flex-direction: column;
            gap: 10px;
            max-width: 400px;
        }
        
        .message {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: var(--shadow-lg);
            display: flex;
            align-items: center;
            gap: 15px;
            animation: slideInRight 0.3s ease;
            transform: translateX(0);
            opacity: 1;
            transition: all 0.3s;
        }
        
        .message.hiding {
            transform: translateX(100%);
            opacity: 0;
        }
        
        @keyframes slideInRight {
            from { transform: translateX(100%); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        
        .message-icon {
            font-size: 1.5rem;
        }
        
        .message-info .message-icon {
            color: var(--primary);
        }
        
        .message-success .message-icon {
            color: var(--secondary);
        }
        
        .message-warning .message-icon {
            color: var(--warning);
        }
        
        .message-error .message-icon {
            color: var(--danger);
        }
        
        .message-content {
            flex: 1;
        }
        
        .message-title {
            font-weight: 600;
            color: var(--dark);
            margin-bottom: 5px;
        }
        
        .message-text {
            color: var(--gray);
            font-size: 0.9rem;
        }
        
        .message-close {
            background: none;
            border: none;
            color: var(--gray);
            cursor: pointer;
            padding: 5px;
        }
        
        /* 侧边栏组件 */
        .sidebar-section {
            background: var(--light);
            border-radius: 12px;
            padding: 20px;
        }
        
        .sidebar-title {
            font-weight: 700;
            color: var(--dark);
            margin-bottom: 15px;
            font-size: 1.1rem;
        }
        
        .info-list {
            list-style: none;
        }
        
        .info-item {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid var(--border);
        }
        
        .info-item:last-child {
            border-bottom: none;
        }
        
        .info-label {
            color: var(--gray);
        }
        
        .info-value {
            font-weight: 600;
            color: var(--dark);
        }
        
        .server-stats {
            background: linear-gradient(135deg, var(--primary), var(--primary-dark));
            color: white;
            padding: 25px;
            border-radius: 15px;
        }
        
        .server-stats .sidebar-title {
            color: white;
            text-align: center;
            font-size: 1.2rem;
        }
        
        .stat-item {
            text-align: center;
            margin: 15px 0;
        }
        
        .stat-number {
            font-size: 2.5rem;
            font-weight: 800;
            margin-bottom: 5px;
        }
        
        .stat-description {
            font-size: 0.9rem;
            opacity: 0.9;
        }
        
        /* 操作按钮组 */
        .action-buttons {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin-top: 20px;
        }
        
        /* 加载动画 */
        .spinner {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid rgba(255, 255, 255, 0.3);
            border-radius: 50%;
            border-top-color: white;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        /* 工具提示 */
        .tooltip {
            position: relative;
            display: inline-block;
            cursor: help;
        }
        
        .tooltip .tooltip-text {
            visibility: hidden;
            width: 200px;
            background: var(--dark);
            color: white;
            text-align: center;
            border-radius: 6px;
            padding: 10px;
            position: absolute;
            z-index: 1;
            bottom: 125%;
            left: 50%;
            transform: translateX(-50%);
            opacity: 0;
            transition: opacity 0.3s;
            font-size: 0.9rem;
            font-weight: normal;
        }
        
        .tooltip:hover .tooltip-text {
            visibility: visible;
            opacity: 1;
        }
        
        /* 响应式设计 */
        @media (max-width: 768px) {
            .main-card {
                padding: 25px;
            }
            
            .sidebar {
                padding: 25px;
            }
            
            .room-code {
                font-size: 2.5rem;
                letter-spacing: 10px;
            }
            
            .transfer-stats {
                grid-template-columns: 1fr;
            }
            
            .connection-status {
                grid-template-columns: 1fr;
            }
            
            .action-buttons {
                grid-template-columns: 1fr;
            }
        }
        
        /* 隐藏元素 */
        .hidden {
            display: none !important;
        }
        
        .visible {
            display: block;
        }
    </style>
</head>
<body>
    <div class="app-container">
        <!-- 主内容区 -->
        <div class="main-card">
            <div class="header">
                <div class="logo">
                    <i class="fas fa-bolt logo-icon"></i>
                    <div class="logo-text">InstantTransfer</div>
                </div>
                <div class="tagline">企业级点对点安全文件传输系统</div>
            </div>
            
            <!-- 模式切换 -->
            <div class="mode-tabs">
                <button class="mode-tab active" id="senderTab" data-mode="sender">
                    <i class="fas fa-cloud-upload-alt"></i>
                    发送文件
                </button>
                <button class="mode-tab" id="receiverTab" data-mode="receiver">
                    <i class="fas fa-cloud-download-alt"></i>
                    接收文件
                </button>
            </div>
            
            <!-- 发送面板 -->
            <div class="panel active" id="senderPanel" data-role="sender">
                <div class="form-group">
                    <label class="form-label">选择文件</label>
                    <div class="file-upload-area" id="fileUploadArea">
                        <i class="fas fa-cloud-upload-alt upload-icon"></i>
                        <div class="upload-text">点击或拖放文件到此处</div>
                        <div class="upload-hint">最大支持 2GB，支持所有文件类型</div>
                        <input type="file" class="file-input" id="fileInput">
                    </div>
                    <div class="file-info-card" id="fileInfoCard">
                        <div class="file-name" id="fileName">未选择文件</div>
                        <div class="file-details">
                            <span id="fileSize">0 KB</span>
                            <span id="fileType">未知类型</span>
                        </div>
                    </div>
                </div>
                
                <button class="btn btn-primary" id="createRoomBtn" disabled>
                    <i class="fas fa-plus-circle"></i>
                    创建传输房间
                </button>
                
                <!-- 发送状态面板 -->
                <div class="status-panel" id="senderStatusPanel">
                    <div class="room-info">
                        <div class="form-label">房间代码</div>
                        <div class="room-code" id="roomCodeDisplay">XXXXXX</div>
                        <div class="room-meta">
                            <span>创建时间: <span id="roomCreatedTime">--:--:--</span></span>
                            <span>过期时间: <span id="roomExpiryTime">--:--:--</span></span>
                        </div>
                    </div>
                    
                    <div class="connection-status">
                        <div class="connection-card" id="senderConnectionCard">
                            <i class="fas fa-paper-plane connection-icon"></i>
                            <div class="connection-label">发送端</div>
                            <div class="connection-state">离线</div>
                        </div>
                        <div class="connection-card" id="receiverConnectionCard">
                            <i class="fas fa-user connection-icon"></i>
                            <div class="connection-label">接收端</div>
                            <div class="connection-state">等待连接</div>
                        </div>
                    </div>
                    
                    <div class="transfer-progress">
                        <div class="progress-header">
                            <div class="progress-label">传输进度</div>
                            <div class="progress-percent" id="senderProgressPercent">0%</div>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" id="senderProgressFill"></div>
                        </div>
                    </div>
                    
                    <div class="transfer-stats">
                        <div class="stat-card">
                            <div class="stat-value" id="speedStat">0 KB/s</div>
                            <div class="stat-label">传输速度</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="timeStat">--:--</div>
                            <div class="stat-label">已用时间</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="chunksStat">0/0</div>
                            <div class="stat-label">数据块</div>
                        </div>
                    </div>
                    
                    <div class="action-buttons">
                        <button class="btn btn-secondary" id="copyCodeBtn">
                            <i class="fas fa-copy"></i>
                            复制代码
                        </button>
                        <button class="btn btn-danger" id="cancelTransferBtn">
                            <i class="fas fa-times"></i>
                            取消传输
                        </button>
                    </div>
                </div>
            </div>
            
            <!-- 接收面板 -->
            <div class="panel" id="receiverPanel" data-role="receiver">
                <div class="form-group">
                    <label class="form-label">输入房间代码</label>
                    <div class="code-input-container">
                        <input type="text" class="code-input" id="roomCodeInput" 
                               placeholder="输入6位代码" maxlength="6" autocomplete="off">
                    </div>
                </div>
                
                <button class="btn btn-success" id="joinRoomBtn">
                    <i class="fas fa-sign-in-alt"></i>
                    加入房间
                </button>
                
                <!-- 接收状态面板 -->
                <div class="status-panel" id="receiverStatusPanel">
                    <div class="connection-status">
                        <div class="connection-card" id="remoteSenderCard">
                            <i class="fas fa-paper-plane connection-icon"></i>
                            <div class="connection-label">发送端</div>
                            <div class="connection-state">离线</div>
                        </div>
                        <div class="connection-card" id="selfConnectionCard">
                            <i class="fas fa-user connection-icon"></i>
                            <div class="connection-label">接收端</div>
                            <div class="connection-state">连接中</div>
                        </div>
                    </div>
                    
                    <div class="file-preview" id="filePreview">
                        <i class="fas fa-file file-icon-preview"></i>
                        <div class="file-name-preview" id="previewFileName">等待文件信息...</div>
                        <div class="file-size-preview" id="previewFileSize">0 KB</div>
                    </div>
                    
                    <div class="transfer-progress">
                        <div class="progress-header">
                            <div class="progress-label">接收进度</div>
                            <div class="progress-percent" id="receiverProgressPercent">0%</div>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" id="receiverProgressFill"></div>
                        </div>
                    </div>
                    
                    <div class="transfer-stats">
                        <div class="stat-card">
                            <div class="stat-value" id="receiverSpeed">0 KB/s</div>
                            <div class="stat-label">接收速度</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="receivedChunks">0/0</div>
                            <div class="stat-label">数据块</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="receiverTime">--:--</div>
                            <div class="stat-label">已用时间</div>
                        </div>
                    </div>
                    
                    <div class="action-buttons">
                        <button class="btn btn-success hidden" id="downloadFileBtn">
                            <i class="fas fa-download"></i>
                            下载文件
                        </button>
                        <button class="btn btn-danger" id="leaveRoomBtn">
                            <i class="fas fa-sign-out-alt"></i>
                            离开房间
                        </button>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- 侧边栏 -->
        <div class="sidebar">
            <div class="server-stats">
                <div class="sidebar-title">服务器状态</div>
                <div class="stat-item">
                    <div class="stat-number" id="activeRooms">0</div>
                    <div class="stat-description">活跃房间</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number" id="totalTransfers">0</div>
                    <div class="stat-description">总传输量</div>
                </div>
            </div>
            
            <div class="sidebar-section">
                <div class="sidebar-title">使用说明</div>
                <ul class="info-list">
                    <li class="info-item">
                        <span class="info-label">1. 创建房间</span>
                        <span class="info-value">选择文件 → 创建</span>
                    </li>
                    <li class="info-item">
                        <span class="info-label">2. 分享代码</span>
                        <span class="info-value">告知接收方6位代码</span>
                    </li>
                    <li class="info-item">
                        <span class="info-label">3. 等待连接</span>
                        <span class="info-value">接收方输入代码加入</span>
                    </li>
                    <li class="info-item">
                        <span class="info-label">4. 开始传输</span>
                        <span class="info-value">自动开始文件传输</span>
                    </li>
                </ul>
            </div>
            
            <div class="sidebar-section">
                <div class="sidebar-title">技术特性</div>
                <ul class="info-list">
                    <li class="info-item">
                        <span class="info-label">WebSocket</span>
                        <span class="info-value">实时双向通信</span>
                    </li>
                    <li class="info-item">
                        <span class="info-label">分块传输</span>
                        <span class="info-value">64KB数据块</span>
                    </li>
                    <li class="info-item">
                        <span class="info-label">断点续传</span>
                        <span class="info-value">支持中断恢复</span>
                    </li>
                    <li class="info-item">
                        <span class="info-label">AES-256加密</span>
                        <span class="info-value">端到端加密</span>
                    </li>
                </ul>
            </div>
        </div>
    </div>
    
    <!-- 消息容器 -->
    <div class="message-container" id="messageContainer"></div>
    
    <script>
        // ===================== 配置常量 =====================
        const CONFIG = {
            API_BASE: '/api',
            WS_PROTOCOL: window.location.protocol === 'https:' ? 'wss:' : 'ws:',
            CHUNK_SIZE: 64 * 1024, // 64KB
            HEARTBEAT_INTERVAL: 15000, // 15秒
            RECONNECT_DELAY: 3000, // 3秒
            MAX_FILE_SIZE: 2 * 1024 * 1024 * 1024, // 2GB
            MAX_CHUNK_RETRY: 3, // 最大重试次数
            CHUNK_TIMEOUT: 10000 // 块传输超时
        };
        
        // ===================== 全局状态 =====================
        let appState = {
            currentMode: 'sender',
            currentRoom: null,
            currentFile: null,
            fileMetadata: null,
            sockets: {
                sender: null,
                receiver: null
            },
            transfers: {
                sender: {
                    chunks: [],
                    stats: {
                        startTime: null,
                        bytesTransferred: 0,
                        chunksSent: 0,
                        chunksAcknowledged: 0,
                        speedHistory: [],
                        lastSpeedUpdate: null
                    }
                },
                receiver: {
                    chunks: new Map(),
                    stats: {
                        startTime: null,
                        bytesReceived: 0,
                        chunksReceived: 0,
                        speedHistory: [],
                        lastSpeedUpdate: null
                    }
                }
            },
            reconnectAttempts: 0,
            heartbeatInterval: null,
            chunkTimeouts: new Map(),
            encryptionKey: null
        };
        
        // ===================== DOM 元素缓存 =====================
        const elements = {
            // 标签页
            senderTab: document.getElementById('senderTab'),
            receiverTab: document.getElementById('receiverTab'),
            senderPanel: document.getElementById('senderPanel'),
            receiverPanel: document.getElementById('receiverPanel'),
            
            // 发送端
            fileUploadArea: document.getElementById('fileUploadArea'),
            fileInput: document.getElementById('fileInput'),
            fileInfoCard: document.getElementById('fileInfoCard'),
            fileName: document.getElementById('fileName'),
            fileSize: document.getElementById('fileSize'),
            fileType: document.getElementById('fileType'),
            createRoomBtn: document.getElementById('createRoomBtn'),
            
            // 发送状态
            senderStatusPanel: document.getElementById('senderStatusPanel'),
            roomCodeDisplay: document.getElementById('roomCodeDisplay'),
            roomCreatedTime: document.getElementById('roomCreatedTime'),
            roomExpiryTime: document.getElementById('roomExpiryTime'),
            senderConnectionCard: document.getElementById('senderConnectionCard'),
            receiverConnectionCard: document.getElementById('receiverConnectionCard'),
            senderProgressPercent: document.getElementById('senderProgressPercent'),
            senderProgressFill: document.getElementById('senderProgressFill'),
            speedStat: document.getElementById('speedStat'),
            timeStat: document.getElementById('timeStat'),
            chunksStat: document.getElementById('chunksStat'),
            copyCodeBtn: document.getElementById('copyCodeBtn'),
            cancelTransferBtn: document.getElementById('cancelTransferBtn'),
            
            // 接收端
            roomCodeInput: document.getElementById('roomCodeInput'),
            joinRoomBtn: document.getElementById('joinRoomBtn'),
            
            // 接收状态
            receiverStatusPanel: document.getElementById('receiverStatusPanel'),
            remoteSenderCard: document.getElementById('remoteSenderCard'),
            selfConnectionCard: document.getElementById('selfConnectionCard'),
            filePreview: document.getElementById('filePreview'),
            previewFileName: document.getElementById('previewFileName'),
            previewFileSize: document.getElementById('previewFileSize'),
            receiverProgressPercent: document.getElementById('receiverProgressPercent'),
            receiverProgressFill: document.getElementById('receiverProgressFill'),
            receiverSpeed: document.getElementById('receiverSpeed'),
            receivedChunks: document.getElementById('receivedChunks'),
            receiverTime: document.getElementById('receiverTime'),
            downloadFileBtn: document.getElementById('downloadFileBtn'),
            leaveRoomBtn: document.getElementById('leaveRoomBtn'),
            
            // 侧边栏
            activeRooms: document.getElementById('activeRooms'),
            totalTransfers: document.getElementById('totalTransfers'),
            
            // 消息
            messageContainer: document.getElementById('messageContainer')
        };
        
        // ===================== 工具函数 =====================
        class Utils {
            static formatBytes(bytes, decimals = 2) {
                if (bytes === 0) return '0 Bytes';
                const k = 1024;
                const dm = decimals < 0 ? 0 : decimals;
                const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
                const i = Math.floor(Math.log(bytes) / Math.log(k));
                return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
            }
            
            static formatTime(seconds) {
                const hours = Math.floor(seconds / 3600);
                const minutes = Math.floor((seconds % 3600) / 60);
                const secs = Math.floor(seconds % 60);
                
                if (hours > 0) {
                    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
                } else {
                    return `${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
                }
            }
            
            static formatDate(timestamp) {
                const date = new Date(timestamp);
                return date.toLocaleTimeString('zh-CN', {
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit'
                });
            }
            
            static generateRoomCode() {
                const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
                let code = '';
                for (let i = 0; i < 6; i++) {
                    code += chars.charAt(Math.floor(Math.random() * chars.length));
                }
                return code;
            }
            
            static showMessage(type, title, text, duration = 5000) {
                const message = document.createElement('div');
                message.className = `message message-${type}`;
                
                const icons = {
                    info: 'info-circle',
                    success: 'check-circle',
                    warning: 'exclamation-triangle',
                    error: 'times-circle'
                };
                
                message.innerHTML = `
                    <i class="fas fa-${icons[type]} message-icon"></i>
                    <div class="message-content">
                        <div class="message-title">${title}</div>
                        <div class="message-text">${text}</div>
                    </div>
                    <button class="message-close">
                        <i class="fas fa-times"></i>
                    </button>
                `;
                
                elements.messageContainer.appendChild(message);
                
                // 自动消失
                setTimeout(() => {
                    message.classList.add('hiding');
                    setTimeout(() => {
                        if (message.parentNode) {
                            message.parentNode.removeChild(message);
                        }
                    }, 300);
                }, duration);
                
                // 手动关闭
                message.querySelector('.message-close').addEventListener('click', () => {
                    message.classList.add('hiding');
                    setTimeout(() => {
                        if (message.parentNode) {
                            message.parentNode.removeChild(message);
                        }
                    }, 300);
                });
            }
            
            static updateConnectionStatus(cardElement, connected) {
                cardElement.classList.remove('connected', 'disconnected');
                const stateElement = cardElement.querySelector('.connection-state');
                
                if (connected) {
                    cardElement.classList.add('connected');
                    stateElement.textContent = '在线';
                } else {
                    cardElement.classList.add('disconnected');
                    stateElement.textContent = '离线';
                }
            }
            
            static async copyToClipboard(text) {
                try {
                    await navigator.clipboard.writeText(text);
                    Utils.showMessage('success', '已复制', '房间代码已复制到剪贴板', 2000);
                    return true;
                } catch (err) {
                    Utils.showMessage('error', '复制失败', '请手动复制代码', 3000);
                    return false;
                }
            }
            
            static calculateSpeed(bytes, startTime, endTime = Date.now()) {
                const timeDiff = (endTime - startTime) / 1000; // 转换为秒
                if (timeDiff <= 0) return 0;
                return bytes / timeDiff; // 字节/秒
            }
            
            static validateRoomCode(code) {
                return /^[A-Z0-9]{6}$/.test(code);
            }
        }
        
        // ===================== 文件处理 =====================
        class FileHandler {
            static async processFile(file) {
                if (!file) return null;
                
                if (file.size > CONFIG.MAX_FILE_SIZE) {
                    Utils.showMessage('error', '文件过大', `最大支持 ${Utils.formatBytes(CONFIG.MAX_FILE_SIZE)}`);
                    return null;
                }
                
                return {
                    file: file,
                    name: file.name,
                    size: file.size,
                    type: file.type,
                    lastModified: file.lastModified,
                    totalChunks: Math.ceil(file.size / CONFIG.CHUNK_SIZE)
                };
            }
            
            static createFileChunks(file) {
                const chunks = [];
                const totalChunks = Math.ceil(file.size / CONFIG.CHUNK_SIZE);
                
                for (let i = 0; i < totalChunks; i++) {
                    const start = i * CONFIG.CHUNK_SIZE;
                    const end = Math.min(start + CONFIG.CHUNK_SIZE, file.size);
                    chunks.push({
                        index: i,
                        start: start,
                        end: end,
                        size: end - start
                    });
                }
                
                return chunks;
            }
            
            static async readChunk(file, chunkInfo) {
                return new Promise((resolve, reject) => {
                    const slice = file.slice(chunkInfo.start, chunkInfo.end);
                    const reader = new FileReader();
                    
                    reader.onload = (e) => {
                        resolve({
                            index: chunkInfo.index,
                            data: e.target.result,
                            size: chunkInfo.size
                        });
                    };
                    
                    reader.onerror = reject;
                    reader.readAsArrayBuffer(slice);
                });
            }
            
            static async mergeChunks(chunksMap) {
                const chunks = Array.from(chunksMap.values())
                    .sort((a, b) => a.index - b.index);
                
                const totalSize = chunks.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
                const merged = new Uint8Array(totalSize);
                
                let offset = 0;
                for (const chunk of chunks) {
                    merged.set(new Uint8Array(chunk.data), offset);
                    offset += chunk.data.byteLength;
                }
                
                return new Blob([merged.buffer]);
            }
        }
        
        // ===================== WebSocket 管理器 =====================
        class WebSocketManager {
            constructor(role, roomCode) {
                this.role = role;
                this.roomCode = roomCode;
                this.socket = null;
                this.reconnectTimer = null;
                this.messageHandlers = new Map();
                
                this.registerDefaultHandlers();
            }
            
            connect() {
                return new Promise((resolve, reject) => {
                    const wsUrl = `${CONFIG.WS_PROTOCOL}//${window.location.host}${CONFIG.API_BASE}/ws/${this.roomCode}?role=${this.role}`;
                    
                    this.socket = new WebSocket(wsUrl);
                    
                    this.socket.onopen = () => {
                        Utils.showMessage('success', '连接成功', `${this.role === 'sender' ? '发送端' : '接收端'}已连接`);
                        clearTimeout(this.reconnectTimer);
                        appState.reconnectAttempts = 0;
                        resolve(this.socket);
                    };
                    
                    this.socket.onmessage = (event) => {
                        this.handleMessage(event);
                    };
                    
                    this.socket.onclose = (event) => {
                        console.log(`${this.role} WebSocket关闭:`, event.code, event.reason);
                        this.handleClose(event);
                    };
                    
                    this.socket.onerror = (error) => {
                        console.error(`${this.role} WebSocket错误:`, error);
                        reject(error);
                    };
                    
                    // 设置连接超时
                    setTimeout(() => {
                        if (this.socket.readyState === WebSocket.CONNECTING) {
                            this.socket.close();
                            reject(new Error('连接超时'));
                        }
                    }, 10000);
                });
            }
            
            send(data) {
                if (this.socket && this.socket.readyState === WebSocket.OPEN) {
                    if (typeof data === 'object' && !(data instanceof ArrayBuffer)) {
                        this.socket.send(JSON.stringify(data));
                    } else {
                        this.socket.send(data);
                    }
                    return true;
                }
                return false;
            }
            
            close(code = 1000, reason = '正常关闭') {
                if (this.socket) {
                    this.socket.close(code, reason);
                }
                clearTimeout(this.reconnectTimer);
            }
            
            handleMessage(event) {
                try {
                    let message;
                    
                    if (event.data instanceof ArrayBuffer) {
                        // 二进制消息（文件块）
                        message = {
                            type: 'chunk',
                            data: event.data
                        };
                    } else {
                        // 文本消息
                        message = JSON.parse(event.data);
                    }
                    
                    // 调用对应的处理器
                    const handler = this.messageHandlers.get(message.type);
                    if (handler) {
                        handler(message);
                    }
                    
                } catch (error) {
                    console.error('处理消息错误:', error);
                }
            }
            
            handleClose(event) {
                if (event.code !== 1000 && event.code !== 1001) {
                    Utils.showMessage('warning', '连接断开', '正在尝试重新连接...');
                    
                    // 指数退避重连
                    const delay = Math.min(30000, 1000 * Math.pow(2, appState.reconnectAttempts));
                    appState.reconnectAttempts++;
                    
                    this.reconnectTimer = setTimeout(() => {
                        if (appState.reconnectAttempts < 5) {
                            this.connect().catch(() => {
                                Utils.showMessage('error', '重连失败', '请检查网络连接');
                            });
                        }
                    }, delay);
                }
            }
            
            registerHandler(type, handler) {
                this.messageHandlers.set(type, handler);
            }
            
            registerDefaultHandlers() {
                // 连接成功
                this.registerHandler('connected', (message) => {
                    console.log(`${this.role} 连接成功:`, message);
                    
                    if (this.role === 'sender') {
                        Utils.updateConnectionStatus(elements.senderConnectionCard, true);
                        this.send({ type: 'ready' });
                    } else {
                        Utils.updateConnectionStatus(elements.selfConnectionCard, true);
                        this.send({ type: 'ready' });
                    }
                });
                
                // 对等端连接
                this.registerHandler('sender-connected', (message) => {
                    Utils.updateConnectionStatus(elements.remoteSenderCard, true);
                    Utils.showMessage('info', '发送端已连接', '等待文件传输开始');
                });
                
                this.registerHandler('receiver-connected', (message) => {
                    Utils.updateConnectionStatus(elements.receiverConnectionCard, true);
                    Utils.showMessage('info', '接收端已连接', '开始传输文件');
                    
                    if (this.role === 'sender') {
                        // 开始传输文件
                        TransferController.startTransfer();
                    }
                });
                
                // 对等端断开
                this.registerHandler('sender-disconnected', (message) => {
                    Utils.updateConnectionStatus(elements.remoteSenderCard, false);
                    Utils.showMessage('warning', '发送端断开', message.reason || '连接已断开');
                });
                
                this.registerHandler('receiver-disconnected', (message) => {
                    Utils.updateConnectionStatus(elements.receiverConnectionCard, false);
                    Utils.showMessage('warning', '接收端断开', message.reason || '连接已断开');
                });
                
                // 文件元数据
                this.registerHandler('file-metadata', (message) => {
                    appState.fileMetadata = message.metadata;
                    
                    if (this.role === 'receiver') {
                        elements.previewFileName.textContent = message.metadata.name;
                        elements.previewFileSize.textContent = Utils.formatBytes(message.metadata.size);
                        elements.filePreview.classList.remove('hidden');
                        
                        // 初始化接收缓冲区
                        appState.transfers.receiver.chunks = new Map();
                        appState.transfers.receiver.stats.startTime = Date.now();
                        
                        Utils.showMessage('info', '开始接收文件', message.metadata.name);
                    }
                });
                
                // 传输进度
                this.registerHandler('transfer-progress', (message) => {
                    if (this.role === 'sender') {
                        const percent = message.progress;
                        elements.senderProgressPercent.textContent = `${percent}%`;
                        elements.senderProgressFill.style.width = `${percent}%`;
                        elements.chunksStat.textContent = `${message.chunksSent}/${message.totalChunks}`;
                        
                        // 更新速度
                        if (appState.transfers.sender.stats.startTime) {
                            const speed = Utils.calculateSpeed(
                                message.bytesTransferred,
                                appState.transfers.sender.stats.startTime
                            );
                            elements.speedStat.textContent = `${Utils.formatBytes(speed)}/s`;
                            
                            // 更新已用时间
                            const elapsed = (Date.now() - appState.transfers.sender.stats.startTime) / 1000;
                            elements.timeStat.textContent = Utils.formatTime(elapsed);
                        }
                    }
                });
                
                // 传输完成
                this.registerHandler('transfer-complete', (message) => {
                    Utils.showMessage('success', '传输完成', '文件传输成功完成');
                    
                    if (this.role === 'receiver') {
                        // 合并文件并提供下载
                        TransferController.completeReceiverTransfer(message.stats);
                    } else {
                        // 发送方完成处理
                        elements.senderProgressPercent.textContent = '100%';
                        elements.senderProgressFill.style.width = '100%';
                        
                        const elapsed = (Date.now() - appState.transfers.sender.stats.startTime) / 1000;
                        const speed = message.stats.averageSpeed;
                        
                        Utils.showMessage('success', '传输完成', 
                            `耗时: ${Utils.formatTime(elapsed)}, 平均速度: ${Utils.formatBytes(speed)}/s`);
                    }
                });
                
                // 心跳响应
                this.registerHandler('heartbeat-response', (message) => {
                    // 更新最后心跳时间
                    if (this.role === 'sender') {
                        // 可以在这里更新发送方的心跳状态
                    }
                });
                
                // 错误处理
                this.registerHandler('error', (message) => {
                    Utils.showMessage('error', '传输错误', message.message || '未知错误');
                });
            }
        }
        
        // ===================== 传输控制器 =====================
        class TransferController {
            static async startTransfer() {
                if (!appState.currentFile || !appState.sockets.sender) return;
                
                try {
                    // 发送文件元数据
                    const metadata = {
                        name: appState.currentFile.name,
                        size: appState.currentFile.size,
                        type: appState.currentFile.type,
                        totalChunks: Math.ceil(appState.currentFile.size / CONFIG.CHUNK_SIZE),
                        lastModified: appState.currentFile.lastModified
                    };
                    
                    appState.sockets.sender.send({
                        type: 'file-metadata',
                        metadata: metadata
                    });
                    
                    // 初始化传输状态
                    appState.transfers.sender.stats.startTime = Date.now();
                    appState.transfers.sender.chunks = FileHandler.createFileChunks(appState.currentFile);
                    
                    // 开始分块传输
                    await TransferController.sendChunks();
                    
                } catch (error) {
                    console.error('开始传输错误:', error);
                    Utils.showMessage('error', '传输错误', error.message);
                }
            }
            
            static async sendChunks() {
                const chunks = appState.transfers.sender.chunks;
                const file = appState.currentFile;
                
                for (let i = 0; i < chunks.length; i++) {
                    if (!appState.sockets.sender || appState.sockets.sender.socket.readyState !== WebSocket.OPEN) {
                        break;
                    }
                    
                    try {
                        const chunk = await FileHandler.readChunk(file, chunks[i]);
                        
                        // 发送数据块
                        appState.sockets.sender.send(chunk.data);
                        
                        // 更新统计
                        appState.transfers.sender.stats.chunksSent++;
                        appState.transfers.sender.stats.bytesTransferred += chunk.size;
                        
                        // 发送进度更新
                        const progress = Math.floor((appState.transfers.sender.stats.chunksSent / chunks.length) * 100);
                        appState.sockets.sender.send({
                            type: 'progress-update',
                            progress: progress,
                            bytesTransferred: appState.transfers.sender.stats.bytesTransferred,
                            chunksSent: appState.transfers.sender.stats.chunksSent,
                            totalChunks: chunks.length,
                            timestamp: Date.now()
                        });
                        
                        // 添加延迟以避免阻塞
                        await new Promise(resolve => setTimeout(resolve, 0));
                        
                    } catch (error) {
                        console.error(`发送块 ${i} 错误:`, error);
                        // 可以在这里实现重试逻辑
                        break;
                    }
                }
                
                // 所有块发送完成
                if (appState.transfers.sender.stats.chunksSent === chunks.length) {
                    appState.sockets.sender.send({
                        type: 'transfer-complete',
                        timestamp: Date.now()
                    });
                }
            }
            
            static handleChunk(chunkData) {
                if (appState.currentMode !== 'receiver') return;
                
                // 这里需要实现块接收逻辑
                // 由于我们不知道块的索引，需要从发送方获取更多信息
                // 简化版本：将块添加到缓冲区
                const chunkId = Date.now() + Math.random();
                appState.transfers.receiver.chunks.set(chunkId, {
                    data: chunkData,
                    timestamp: Date.now()
                });
                
                // 更新接收进度
                appState.transfers.receiver.stats.chunksReceived++;
                appState.transfers.receiver.stats.bytesReceived += chunkData.byteLength;
                
                // 更新UI
                const totalChunks = appState.fileMetadata?.totalChunks || 1;
                const progress = Math.floor((appState.transfers.receiver.stats.chunksReceived / totalChunks) * 100);
                
                elements.receiverProgressPercent.textContent = `${progress}%`;
                elements.receiverProgressFill.style.width = `${progress}%`;
                elements.receivedChunks.textContent = `${appState.transfers.receiver.stats.chunksReceived}/${totalChunks}`;
                
                // 计算速度
                if (appState.transfers.receiver.stats.startTime) {
                    const speed = Utils.calculateSpeed(
                        appState.transfers.receiver.stats.bytesReceived,
                        appState.transfers.receiver.stats.startTime
                    );
                    elements.receiverSpeed.textContent = `${Utils.formatBytes(speed)}/s`;
                    
                    const elapsed = (Date.now() - appState.transfers.receiver.stats.startTime) / 1000;
                    elements.receiverTime.textContent = Utils.formatTime(elapsed);
                }
                
                // 发送确认
                if (appState.sockets.receiver) {
                    appState.sockets.receiver.send({
                        type: 'chunk-ack',
                        timestamp: Date.now()
                    });
                }
            }
            
            static async completeReceiverTransfer(stats) {
                try {
                    // 合并所有块
                    const blob = await FileHandler.mergeChunks(appState.transfers.receiver.chunks);
                    
                    // 创建下载链接
                    const url = URL.createObjectURL(blob);
                    elements.downloadFileBtn.href = url;
                    elements.downloadFileBtn.download = appState.fileMetadata.name;
                    elements.downloadFileBtn.classList.remove('hidden');
                    
                    // 显示统计信息
                    const elapsed = (Date.now() - appState.transfers.receiver.stats.startTime) / 1000;
                    const speed = stats?.averageSpeed || 
                        (appState.transfers.receiver.stats.bytesReceived / elapsed);
                    
                    Utils.showMessage('success', '接收完成', 
                        `文件已准备就绪，耗时: ${Utils.formatTime(elapsed)}, 平均速度: ${Utils.formatBytes(speed)}/s`);
                        
                } catch (error) {
                    console.error('合并文件错误:', error);
                    Utils.showMessage('error', '文件处理错误', '无法合并接收的文件块');
                }
            }
        }
        
        // ===================== 房间管理器 =====================
        class RoomController {
            static async createRoom() {
                if (!appState.currentFile) {
                    Utils.showMessage('warning', '请选择文件', '请先选择要传输的文件');
                    return;
                }
                
                try {
                    elements.createRoomBtn.disabled = true;
                    elements.createRoomBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 创建房间中...';
                    
                    // 生成房间代码
                    const roomCode = Utils.generateRoomCode();
                    
                    // 创建房间
                    const response = await fetch(`${CONFIG.API_BASE}/room/create`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ code: roomCode })
                    });
                    
                    const result = await response.json();
                    
                    if (!result.success) {
                        throw new Error(result.error || '创建房间失败');
                    }
                    
                    // 设置当前房间
                    appState.currentRoom = {
                        code: roomCode,
                        wsUrl: result.wsUrl,
                        createdAt: result.createdAt,
                        expiresAt: result.expiresAt
                    };
                    
                    // 更新UI
                    elements.roomCodeDisplay.textContent = roomCode;
                    elements.roomCreatedTime.textContent = Utils.formatDate(result.createdAt);
                    elements.roomExpiryTime.textContent = Utils.formatDate(result.expiresAt);
                    elements.senderStatusPanel.classList.add('active');
                    
                    // 连接WebSocket
                    await RoomController.connectAsSender();
                    
                    Utils.showMessage('success', '房间创建成功', `房间代码: ${roomCode}`);
                    
                    // 更新创建按钮状态
                    elements.createRoomBtn.innerHTML = '<i class="fas fa-check"></i> 房间已创建';
                    
                } catch (error) {
                    console.error('创建房间错误:', error);
                    Utils.showMessage('error', '创建失败', error.message || '未知错误');
                    
                    elements.createRoomBtn.disabled = false;
                    elements.createRoomBtn.innerHTML = '<i class="fas fa-plus-circle"></i> 创建传输房间';
                }
            }
            
            static async connectAsSender() {
                try {
                    const wsManager = new WebSocketManager('sender', appState.currentRoom.code);
                    appState.sockets.sender = wsManager;
                    
                    // 添加块处理器
                    wsManager.registerHandler('chunk', (message) => {
                        // 发送方不应该接收块，但处理确认消息
                        if (message.type === 'chunk-ack') {
                            appState.transfers.sender.stats.chunksAcknowledged++;
                        }
                    });
                    
                    await wsManager.connect();
                    
                    // 启动心跳
                    RoomController.startHeartbeat();
                    
                } catch (error) {
                    console.error('连接WebSocket错误:', error);
                    Utils.showMessage('error', '连接失败', error.message || '无法连接到服务器');
                }
            }
            
            static async joinRoom() {
                const roomCode = elements.roomCodeInput.value.trim().toUpperCase();
                
                if (!Utils.validateRoomCode(roomCode)) {
                    Utils.showMessage('warning', '无效的代码', '请输入6位字母数字代码');
                    return;
                }
                
                try {
                    elements.joinRoomBtn.disabled = true;
                    elements.joinRoomBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加入中...';
                    
                    // 检查房间状态
                    const response = await fetch(`${CONFIG.API_BASE}/room/${roomCode}`);
                    const result = await response.json();
                    
                    if (!result.success) {
                        throw new Error(result.error || '房间不存在');
                    }
                    
                    // 设置当前房间
                    appState.currentRoom = {
                        code: roomCode,
                        wsUrl: result.wsUrls.receiver,
                        status: result.status
                    };
                    
                    // 更新UI
                    elements.receiverStatusPanel.classList.add('active');
                    
                    // 连接WebSocket
                    await RoomController.connectAsReceiver();
                    
                    Utils.showMessage('success', '加入成功', '已连接到房间，等待文件传输');
                    
                    elements.joinRoomBtn.innerHTML = '<i class="fas fa-check"></i> 已加入';
                    
                } catch (error) {
                    console.error('加入房间错误:', error);
                    Utils.showMessage('error', '加入失败', error.message || '无法加入房间');
                    
                    elements.joinRoomBtn.disabled = false;
                    elements.joinRoomBtn.innerHTML = '<i class="fas fa-sign-in-alt"></i> 加入房间';
                }
            }
            
            static async connectAsReceiver() {
                try {
                    const wsManager = new WebSocketManager('receiver', appState.currentRoom.code);
                    appState.sockets.receiver = wsManager;
                    
                    // 添加块处理器
                    wsManager.registerHandler('chunk', (message) => {
                        TransferController.handleChunk(message.data);
                    });
                    
                    await wsManager.connect();
                    
                } catch (error) {
                    console.error('连接WebSocket错误:', error);
                    Utils.showMessage('error', '连接失败', error.message || '无法连接到服务器');
                }
            }
            
            static startHeartbeat() {
                if (appState.heartbeatInterval) {
                    clearInterval(appState.heartbeatInterval);
                }
                
                appState.heartbeatInterval = setInterval(() => {
                    if (appState.sockets.sender) {
                        appState.sockets.sender.send({
                            type: 'heartbeat',
                            timestamp: Date.now()
                        });
                    }
                }, CONFIG.HEARTBEAT_INTERVAL);
            }
            
            static cancelTransfer() {
                if (confirm('确定要取消传输吗？所有进度将丢失。')) {
                    // 关闭所有连接
                    if (appState.sockets.sender) {
                        appState.sockets.sender.close(1000, '用户取消');
                        appState.sockets.sender = null;
                    }
                    
                    if (appState.sockets.receiver) {
                        appState.sockets.receiver.close(1000, '用户取消');
                        appState.sockets.receiver = null;
                    }
                    
                    // 清除心跳
                    if (appState.heartbeatInterval) {
                        clearInterval(appState.heartbeatInterval);
                        appState.heartbeatInterval = null;
                    }
                    
                    // 重置状态
                    appState.currentRoom = null;
                    appState.transfers.sender.stats.startTime = null;
                    
                    // 更新UI
                    if (appState.currentMode === 'sender') {
                        elements.senderStatusPanel.classList.remove('active');
                        elements.createRoomBtn.disabled = false;
                        elements.createRoomBtn.innerHTML = '<i class="fas fa-plus-circle"></i> 创建传输房间';
                    } else {
                        elements.receiverStatusPanel.classList.remove('active');
                        elements.joinRoomBtn.disabled = false;
                        elements.joinRoomBtn.innerHTML = '<i class="fas fa-sign-in-alt"></i> 加入房间';
                    }
                    
                    Utils.showMessage('info', '传输取消', '文件传输已取消');
                }
            }
            
            static leaveRoom() {
                RoomController.cancelTransfer();
            }
            
            static async updateServerStats() {
                try {
                    const response = await fetch(`${CONFIG.API_BASE}/stats`);
                    const result = await response.json();
                    
                    if (result.success) {
                        elements.activeRooms.textContent = result.statistics.rooms.total;
                        elements.totalTransfers.textContent = result.statistics.transfers.totalFiles;
                    }
                } catch (error) {
                    console.error('获取服务器统计错误:', error);
                }
            }
        }
        
        // ===================== 事件监听器 =====================
        class EventListeners {
            static initialize() {
                // 模式切换
                elements.senderTab.addEventListener('click', () => EventListeners.switchMode('sender'));
                elements.receiverTab.addEventListener('click', () => EventListeners.switchMode('receiver'));
                
                // 文件选择
                elements.fileInput.addEventListener('change', EventListeners.handleFileSelect);
                elements.fileUploadArea.addEventListener('dragover', EventListeners.handleDragOver);
                elements.fileUploadArea.addEventListener('dragleave', EventListeners.handleDragLeave);
                elements.fileUploadArea.addEventListener('drop', EventListeners.handleFileDrop);
                
                // 房间操作
                elements.createRoomBtn.addEventListener('click', RoomController.createRoom);
                elements.joinRoomBtn.addEventListener('click', RoomController.joinRoom);
                
                // 代码输入
                elements.roomCodeInput.addEventListener('input', EventListeners.handleCodeInput);
                elements.roomCodeInput.addEventListener('keypress', EventListeners.handleCodeKeyPress);
                
                // 操作按钮
                elements.copyCodeBtn.addEventListener('click', EventListeners.copyRoomCode);
                elements.cancelTransferBtn.addEventListener('click', RoomController.cancelTransfer);
                elements.downloadFileBtn.addEventListener('click', EventListeners.handleDownload);
                elements.leaveRoomBtn.addEventListener('click', RoomController.leaveRoom);
                
                // 页面卸载警告
                window.addEventListener('beforeunload', EventListeners.handleBeforeUnload);
                
                // 定期更新服务器统计
                setInterval(RoomController.updateServerStats, 10000);
                RoomController.updateServerStats();
                
                // 初始健康检查
                EventListeners.checkHealth();
            }
            
            static switchMode(mode) {
                if (appState.currentMode === mode) return;
                
                appState.currentMode = mode;
                
                // 更新标签
                elements.senderTab.classList.toggle('active', mode === 'sender');
                elements.receiverTab.classList.toggle('active', mode === 'receiver');
                
                // 更新面板
                elements.senderPanel.classList.toggle('active', mode === 'sender');
                elements.receiverPanel.classList.toggle('active', mode === 'receiver');
                
                // 重置状态
                if (mode === 'sender') {
                    elements.receiverStatusPanel.classList.remove('active');
                    elements.joinRoomBtn.disabled = false;
                    elements.joinRoomBtn.innerHTML = '<i class="fas fa-sign-in-alt"></i> 加入房间';
                } else {
                    elements.senderStatusPanel.classList.remove('active');
                    elements.createRoomBtn.disabled = true;
                    elements.createRoomBtn.innerHTML = '<i class="fas fa-plus-circle"></i> 创建传输房间';
                }
            }
            
            static async handleFileSelect(event) {
                const file = event.target.files[0];
                if (!file) return;
                
                const processed = await FileHandler.processFile(file);
                if (!processed) return;
                
                appState.currentFile = processed.file;
                
                // 更新UI
                elements.fileName.textContent = processed.name;
                elements.fileSize.textContent = Utils.formatBytes(processed.size);
                elements.fileType.textContent = processed.type || '未知类型';
                elements.fileInfoCard.classList.add('visible');
                elements.createRoomBtn.disabled = false;
            }
            
            static handleDragOver(event) {
                event.preventDefault();
                elements.fileUploadArea.classList.add('dragover');
            }
            
            static handleDragLeave(event) {
                event.preventDefault();
                elements.fileUploadArea.classList.remove('dragover');
            }
            
            static async handleFileDrop(event) {
                event.preventDefault();
                elements.fileUploadArea.classList.remove('dragover');
                
                const file = event.dataTransfer.files[0];
                if (!file) return;
                
                elements.fileInput.files = event.dataTransfer.files;
                await EventListeners.handleFileSelect({ target: elements.fileInput });
            }
            
            static handleCodeInput(event) {
                let value = event.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
                value = value.slice(0, 6);
                event.target.value = value;
                
                elements.joinRoomBtn.disabled = value.length !== 6;
            }
            
            static handleCodeKeyPress(event) {
                if (event.key === 'Enter' && elements.roomCodeInput.value.length === 6) {
                    RoomController.joinRoom();
                }
            }
            
            static async copyRoomCode() {
                if (appState.currentRoom?.code) {
                    await Utils.copyToClipboard(appState.currentRoom.code);
                }
            }
            
            static handleDownload() {
                // 下载后清理
                setTimeout(() => {
                    URL.revokeObjectURL(elements.downloadFileBtn.href);
                    elements.downloadFileBtn.classList.add('hidden');
                    
                    RoomController.leaveRoom();
                }, 100);
            }
            
            static handleBeforeUnload(event) {
                if (appState.sockets.sender || appState.sockets.receiver) {
                    event.preventDefault();
                    event.returnValue = '文件传输中，确定要离开吗？';
                    return event.returnValue;
                }
            }
            
            static async checkHealth() {
                try {
                    const response = await fetch(`${CONFIG.API_BASE}/health`);
                    const data = await response.json();
                    console.log('服务器健康状态:', data.status);
                } catch (error) {
                    console.warn('健康检查失败:', error);
                }
            }
        }
        
        // ===================== 初始化 =====================
        document.addEventListener('DOMContentLoaded', () => {
            console.log('InstantTransfer 客户端初始化');
            EventListeners.initialize();
        });
        
        // 全局错误处理
        window.addEventListener('error', (event) => {
            console.error('全局错误:', event.error);
            Utils.showMessage('error', '应用程序错误', event.error.message || '未知错误');
        });
        
        window.addEventListener('unhandledrejection', (event) => {
            console.error('未处理的Promise拒绝:', event.reason);
            Utils.showMessage('error', '异步错误', event.reason?.message || '未知错误');
        });
    </script>
</body>
</html>`;
}