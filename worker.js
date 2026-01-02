// ===================== 即时传输 Worker - 修复版 =====================

// 存储所有房间数据
let rooms = new Map();

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    
    // CORS 头
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    
    // 处理预检请求
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }
    
    // 主页面
    if (pathname === '/' || pathname === '/index.html') {
      return new Response(HTML_CONTENT, {
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
        timestamp: Date.now(),
        service: '即时传输',
        roomCount: rooms.size
      });
    }
    
    // 创建房间
    if (pathname === '/api/room/create' && request.method === 'POST') {
      try {
        const data = await request.json();
        const code = data.code?.toUpperCase();
        
        if (!code || code.length !== 6) {
          return Response.json({ 
            success: false, 
            error: '取件码必须是6位字符' 
          }, { status: 400 });
        }
        
        // 检查房间是否已存在且未过期
        if (rooms.has(code)) {
          const room = rooms.get(code);
          const now = Date.now();
          
          // 如果房间超过30分钟，清理它
          if (now - room.createdAt > 30 * 60 * 1000) {
            cleanupRoom(code);
          } else {
            return Response.json({ 
              success: false, 
              error: '房间已存在' 
            }, { status: 409 });
          }
        }
        
        // 创建新房间
        const room = {
          code: code,
          createdAt: Date.now(),
          status: 'waiting',
          sender: null,
          receiver: null,
          senderReady: false,
          receiverReady: false,
          fileMetadata: null,
          lastHeartbeat: Date.now(),
          heartbeatInterval: null
        };
        
        rooms.set(code, room);
        
        // 30分钟后自动清理
        ctx.waitUntil(setTimeout(() => {
          cleanupRoom(code);
        }, 30 * 60 * 1000));
        
        return Response.json({ 
          success: true, 
          code: code,
          wsUrl: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}`,
          message: '房间创建成功'
        });
        
      } catch (error) {
        console.error('创建房间错误:', error);
        return Response.json({ 
          success: false, 
          error: '服务器错误' 
        }, { status: 500 });
      }
    }
    
    // 查询房间
    if (pathname.startsWith('/api/room/') && request.method === 'GET') {
      const code = pathname.split('/').pop()?.toUpperCase();
      
      if (!code || code.length !== 6) {
        return Response.json({ 
          success: false, 
          error: '无效的取件码格式' 
        }, { status: 400 });
      }
      
      const room = rooms.get(code);
      if (!room) {
        return Response.json({ 
          success: false, 
          error: '房间不存在或已过期' 
        }, { status: 404 });
      }
      
      // 清理过期的房间
      if (Date.now() - room.createdAt > 30 * 60 * 1000) {
        cleanupRoom(code);
        return Response.json({ 
          success: false, 
          error: '房间已过期' 
        }, { status: 404 });
      }
      
      return Response.json({
        success: true,
        exists: true,
        code: room.code,
        status: room.status,
        createdAt: room.createdAt,
        connections: {
          sender: !!room.sender,
          receiver: !!room.receiver
        },
        wsUrl: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}`
      });
    }
    
    // WebSocket 连接
    if (pathname.startsWith('/api/ws/')) {
      const code = pathname.split('/').pop()?.toUpperCase();
      const role = url.searchParams.get('role');
      
      if (!code || code.length !== 6) {
        return new Response('无效的房间代码', { status: 400 });
      }
      
      if (!role || !['sender', 'receiver'].includes(role)) {
        return new Response('无效的角色', { status: 400 });
      }
      
      const room = rooms.get(code);
      if (!room) {
        return new Response('房间不存在', { status: 404 });
      }
      
      // 检查房间是否过期
      if (Date.now() - room.createdAt > 30 * 60 * 1000) {
        cleanupRoom(code);
        return new Response('房间已过期', { status: 410 });
      }
      
      // 检查角色冲突
      if (role === 'sender' && room.sender) {
        return new Response('发送方已连接', { status: 409 });
      }
      
      if (role === 'receiver' && room.receiver) {
        return new Response('接收方已连接', { status: 409 });
      }
      
      // 创建 WebSocket 连接
      const { 0: client, 1: server } = new WebSocketPair();
      
      // 接受连接
      server.accept();
      
      // 保存连接
      if (role === 'sender') {
        room.sender = server;
        room.senderReady = false;
        room.lastHeartbeat = Date.now();
        startHeartbeatCheck(room, ctx);
      } else {
        room.receiver = server;
        room.receiverReady = false;
      }
      
      // 更新房间状态
      updateRoomStatus(room);
      
      // 设置连接成功消息
      const connectMessage = JSON.stringify({
        type: 'connected',
        success: true,
        role: role,
        timestamp: Date.now(),
        message: '连接成功'
      });
      
      // 发送给当前连接
      server.send(connectMessage);
      
      // 通知另一方
      if (role === 'sender' && room.receiver) {
        room.receiver.send(JSON.stringify({
          type: 'sender-connected',
          timestamp: Date.now(),
          message: '发送方已连接'
        }));
      } else if (role === 'receiver' && room.sender) {
        room.sender.send(JSON.stringify({
          type: 'receiver-connected',
          timestamp: Date.now(),
          message: '接收方已连接'
        }));
      }
      
      // 设置消息处理
      server.addEventListener('message', async (event) => {
        try {
          await handleWebSocketMessage(event, server, role, room);
        } catch (error) {
          console.error('WebSocket消息处理错误:', error);
        }
      });
      
      // 设置连接关闭处理
      server.addEventListener('close', () => {
        handleWebSocketClose(server, role, room, code);
      });
      
      // 设置错误处理
      server.addEventListener('error', (error) => {
        console.error('WebSocket错误:', error);
      });
      
      return new Response(null, {
        status: 101,
        webSocket: client,
        headers: corsHeaders
      });
    }
    
    // 404 处理
    return new Response('Not Found', { 
      status: 404,
      headers: corsHeaders
    });
  }
};

// 清理房间
function cleanupRoom(code) {
  const room = rooms.get(code);
  if (room) {
    if (room.sender) {
      try { 
        room.sender.close(1000, '房间过期'); 
      } catch {}
    }
    if (room.receiver) {
      try { 
        room.receiver.close(1000, '房间过期'); 
      } catch {}
    }
    if (room.heartbeatInterval) {
      clearInterval(room.heartbeatInterval);
    }
    rooms.delete(code);
  }
}

// 更新房间状态
function updateRoomStatus(room) {
  if (room.sender && room.receiver) {
    room.status = 'active';
  } else if (room.sender) {
    room.status = 'waiting_receiver';
  } else if (room.receiver) {
    room.status = 'waiting_sender';
  } else {
    room.status = 'waiting';
  }
}

// 处理 WebSocket 消息
async function handleWebSocketMessage(event, socket, role, room) {
  const timestamp = Date.now();
  
  try {
    // 检查是否是二进制数据
    if (typeof event.data === 'string') {
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
          
          // 双方都准备好后开始传输
          if (room.senderReady && room.receiverReady) {
            room.sender.send(JSON.stringify({
              type: 'transfer-ready',
              timestamp
            }));
          }
          break;
          
        case 'file-metadata':
          room.fileMetadata = data.metadata;
          
          // 转发给接收方
          if (room.receiver) {
            room.receiver.send(JSON.stringify({
              type: 'file-metadata',
              metadata: data.metadata,
              timestamp
            }));
          }
          break;
          
        case 'transfer-complete':
          // 转发给接收方
          if (room.receiver) {
            room.receiver.send(JSON.stringify({
              type: 'transfer-complete',
              timestamp,
              metadata: room.fileMetadata
            }));
          }
          break;
          
        case 'error':
          // 转发错误消息
          if (role === 'sender' && room.receiver) {
            room.receiver.send(JSON.stringify({
              type: 'sender-error',
              message: data.message,
              timestamp
            }));
          } else if (role === 'receiver' && room.sender) {
            room.sender.send(JSON.stringify({
              type: 'receiver-error',
              message: data.message,
              timestamp
            }));
          }
          break;
      }
    } else if (event.data instanceof ArrayBuffer) {
      // 二进制数据，直接转发
      if (role === 'sender' && room.receiver) {
        room.receiver.send(event.data);
      } else if (role === 'receiver' && room.sender) {
        // 接收方发送确认消息
        const ack = JSON.stringify({
          type: 'chunk-received',
          timestamp
        });
        room.sender.send(ack);
      }
    }
  } catch (error) {
    console.error('处理消息错误:', error);
    
    // 发送错误消息
    socket.send(JSON.stringify({
      type: 'error',
      message: '消息处理失败',
      timestamp
    }));
  }
}

// 处理 WebSocket 关闭
function handleWebSocketClose(socket, role, room, code) {
  console.log(`${role} 连接关闭`);
  
  // 清理连接
  if (role === 'sender') {
    room.sender = null;
    room.senderReady = false;
    
    // 通知接收方
    if (room.receiver) {
      room.receiver.send(JSON.stringify({
        type: 'sender-disconnected',
        timestamp: Date.now(),
        message: '发送方已断开连接'
      }));
      room.receiver.close(1000, '发送方断开');
    }
  } else {
    room.receiver = null;
    room.receiverReady = false;
    
    // 通知发送方
    if (room.sender) {
      room.sender.send(JSON.stringify({
        type: 'receiver-disconnected',
        timestamp: Date.now(),
        message: '接收方已断开连接'
      }));
      room.sender.close(1000, '接收方断开');
    }
  }
  
  // 更新状态
  updateRoomStatus(room);
  
  // 如果双方都断开，10分钟后清理房间
  if (!room.sender && !room.receiver) {
    room.status = 'closed';
    setTimeout(() => {
      if (rooms.get(code) === room && !room.sender && !room.receiver) {
        rooms.delete(code);
      }
    }, 10 * 60 * 1000);
  }
}

// 启动心跳检查
function startHeartbeatCheck(room, ctx) {
  if (room.heartbeatInterval) {
    clearInterval(room.heartbeatInterval);
  }
  
  room.heartbeatInterval = setInterval(() => {
    if (!rooms.has(room.code) || rooms.get(room.code) !== room) {
      clearInterval(room.heartbeatInterval);
      return;
    }
    
    const now = Date.now();
    
    // 如果超过60秒没有心跳，断开发送方连接
    if (room.lastHeartbeat && now - room.lastHeartbeat > 60000) {
      console.log('心跳超时，关闭连接');
      if (room.sender) {
        room.sender.close(1001, '心跳超时');
        room.sender = null;
      }
      clearInterval(room.heartbeatInterval);
    }
  }, 30000); // 每30秒检查一次
  
  // 1小时后清理定时器
  setTimeout(() => {
    if (room.heartbeatInterval) {
      clearInterval(room.heartbeatInterval);
    }
  }, 60 * 60 * 1000);
}

// HTML 页面内容
const HTML_CONTENT = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>即时传输</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', Arial, sans-serif;
        }
        
        body {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }
        
        .container {
            width: 100%;
            max-width: 500px;
        }
        
        .app-card {
            background: white;
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.15);
            text-align: center;
            animation: fadeIn 0.5s ease;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .logo {
            font-size: 3.5rem;
            color: #3b82f6;
            margin-bottom: 15px;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.05); }
            100% { transform: scale(1); }
        }
        
        h1 {
            font-size: 2.2rem;
            color: #1e293b;
            margin-bottom: 10px;
            background: linear-gradient(45deg, #3b82f6, #8b5cf6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .subtitle {
            color: #64748b;
            margin-bottom: 30px;
            font-size: 1.1rem;
        }
        
        .mode-selector {
            display: flex;
            background: #f1f5f9;
            border-radius: 12px;
            padding: 5px;
            margin-bottom: 30px;
            overflow: hidden;
        }
        
        .mode-btn {
            flex: 1;
            padding: 16px;
            border: none;
            background: transparent;
            border-radius: 8px;
            font-size: 1rem;
            font-weight: 600;
            color: #64748b;
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
        }
        
        .mode-btn.active {
            background: white;
            color: #3b82f6;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
        }
        
        .panel {
            display: none;
            animation: slideIn 0.3s ease;
        }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-20px); }
            to { opacity: 1; transform: translateX(0); }
        }
        
        .panel.active {
            display: block;
        }
        
        .form-group {
            margin-bottom: 25px;
            text-align: left;
        }
        
        label {
            display: block;
            margin-bottom: 10px;
            color: #1e293b;
            font-weight: 600;
            font-size: 1rem;
        }
        
        .file-upload {
            border: 3px dashed #cbd5e1;
            border-radius: 15px;
            padding: 50px 20px;
            text-align: center;
            background: #f8fafc;
            cursor: pointer;
            transition: all 0.3s;
            position: relative;
        }
        
        .file-upload:hover {
            border-color: #3b82f6;
            background: #f0f9ff;
            transform: translateY(-2px);
        }
        
        .file-upload.dragover {
            border-color: #10b981;
            background: rgba(16, 185, 129, 0.1);
        }
        
        .file-upload i {
            font-size: 3rem;
            color: #3b82f6;
            margin-bottom: 15px;
        }
        
        .file-upload .hint {
            font-size: 0.9rem;
            color: #94a3b8;
            margin-top: 10px;
        }
        
        input[type="file"] {
            position: absolute;
            width: 100%;
            height: 100%;
            top: 0;
            left: 0;
            opacity: 0;
            cursor: pointer;
        }
        
        .file-info {
            background: linear-gradient(135deg, #f0f9ff, #e0f2fe);
            border-radius: 15px;
            padding: 20px;
            margin-top: 15px;
            border-left: 5px solid #3b82f6;
            display: none;
            animation: fadeIn 0.3s ease;
        }
        
        .file-info.show {
            display: block;
        }
        
        .file-name {
            font-weight: 600;
            color: #1e293b;
            margin-bottom: 8px;
            word-break: break-word;
        }
        
        .file-size {
            color: #64748b;
            font-size: 0.9rem;
        }
        
        .code-input-container {
            position: relative;
        }
        
        .code-input {
            width: 100%;
            padding: 18px;
            border: 2px solid #e2e8f0;
            border-radius: 12px;
            font-size: 1.4rem;
            text-align: center;
            letter-spacing: 8px;
            font-family: 'Courier New', monospace;
            font-weight: 700;
            color: #1e293b;
            transition: all 0.3s;
        }
        
        .code-input:focus {
            outline: none;
            border-color: #3b82f6;
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.2);
        }
        
        .code-input::placeholder {
            letter-spacing: normal;
            color: #94a3b8;
        }
        
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
            background: linear-gradient(135deg, #3b82f6, #2563eb);
            color: white;
        }
        
        .btn-primary:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 10px 25px rgba(59, 130, 246, 0.4);
        }
        
        .btn-success {
            background: linear-gradient(135deg, #10b981, #059669);
            color: white;
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #ef4444, #dc2626);
            color: white;
        }
        
        .btn-secondary {
            background: #64748b;
            color: white;
        }
        
        .btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none !important;
            box-shadow: none !important;
        }
        
        .spinner {
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
        
        .status-area {
            margin-top: 30px;
            padding: 25px;
            background: #f8fafc;
            border-radius: 16px;
            display: none;
            animation: slideIn 0.3s ease;
        }
        
        .status-area.active {
            display: block;
        }
        
        .alert {
            padding: 18px;
            border-radius: 12px;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 12px;
            animation: fadeIn 0.3s ease;
        }
        
        .alert-warning {
            background: #fffbeb;
            border: 2px solid #fbbf24;
            color: #92400e;
        }
        
        .alert-info {
            background: #f0f9ff;
            border: 2px solid #3b82f6;
            color: #1e40af;
        }
        
        .alert-success {
            background: #f0fdf4;
            border: 2px solid #10b981;
            color: #065f46;
        }
        
        .alert-danger {
            background: #fef2f2;
            border: 2px solid #ef4444;
            color: #991b1b;
        }
        
        .code-display {
            font-size: 3rem;
            font-weight: 800;
            letter-spacing: 15px;
            color: #3b82f6;
            margin: 25px 0;
            font-family: 'Courier New', monospace;
            text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.1);
            background: linear-gradient(45deg, #3b82f6, #8b5cf6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            padding: 15px;
            border-radius: 10px;
            background-color: #f8fafc;
        }
        
        .progress-container {
            margin: 25px 0;
        }
        
        .progress-info {
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
            color: #64748b;
            font-size: 0.9rem;
        }
        
        .progress-bar {
            height: 12px;
            background: #e2e8f0;
            border-radius: 6px;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
            width: 0%;
            transition: width 0.3s ease;
            border-radius: 6px;
        }
        
        .connection-status {
            display: flex;
            gap: 15px;
            margin: 25px 0;
        }
        
        .status-item {
            flex: 1;
            padding: 18px;
            border-radius: 12px;
            text-align: center;
            border: 2px solid #e2e8f0;
            transition: all 0.3s;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .status-item.active {
            border-color: #10b981;
            background: rgba(16, 185, 129, 0.1);
            transform: translateY(-2px);
        }
        
        .status-item.inactive {
            border-color: #ef4444;
            background: rgba(239, 68, 68, 0.1);
        }
        
        .status-label {
            font-weight: 600;
            color: #1e293b;
        }
        
        .status-value {
            font-size: 1.1rem;
            font-weight: 700;
        }
        
        .status-item.active .status-value {
            color: #10b981;
        }
        
        .status-item.inactive .status-value {
            color: #ef4444;
        }
        
        .hidden {
            display: none !important;
        }
        
        .instructions {
            margin-top: 30px;
            padding: 20px;
            background: #f8fafc;
            border-radius: 16px;
            text-align: left;
            font-size: 0.9rem;
            color: #64748b;
            line-height: 1.6;
        }
        
        .instructions h3 {
            color: #1e293b;
            margin-bottom: 10px;
            font-size: 1rem;
        }
        
        .instructions ul {
            padding-left: 20px;
        }
        
        .instructions li {
            margin-bottom: 8px;
        }
        
        .transfer-stats {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
            margin: 20px 0;
        }
        
        .stat-card {
            background: white;
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid #e2e8f0;
        }
        
        .stat-value {
            font-size: 1.5rem;
            font-weight: 700;
            color: #3b82f6;
            margin-bottom: 5px;
        }
        
        .stat-label {
            font-size: 0.8rem;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .file-preview {
            margin: 20px 0;
            padding: 20px;
            background: #f8fafc;
            border-radius: 12px;
            border: 2px dashed #cbd5e1;
        }
        
        .file-icon {
            font-size: 4rem;
            color: #3b82f6;
            margin-bottom: 15px;
        }
        
        .file-details {
            text-align: center;
        }
        
        .file-name-preview {
            font-weight: 600;
            color: #1e293b;
            margin-bottom: 5px;
            word-break: break-word;
        }
        
        .file-size-preview {
            color: #64748b;
            font-size: 0.9rem;
        }
        
        @media (max-width: 480px) {
            .app-card {
                padding: 20px;
            }
            
            .code-display {
                font-size: 2.2rem;
                letter-spacing: 10px;
            }
            
            .connection-status {
                flex-direction: column;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="app-card">
            <div class="logo">
                <i class="fas fa-bolt"></i>
            </div>
            <h1>即时传输</h1>
            <p class="subtitle">快速安全的点对点文件传输</p>
            
            <div class="mode-selector">
                <button class="mode-btn active" id="senderModeBtn">
                    <i class="fas fa-cloud-upload-alt"></i>
                    发送文件
                </button>
                <button class="mode-btn" id="receiverModeBtn">
                    <i class="fas fa-cloud-download-alt"></i>
                    接收文件
                </button>
            </div>
            
            <!-- 发送端面板 -->
            <div class="panel active" id="senderPanel">
                <div class="form-group">
                    <label>选择要发送的文件</label>
                    <div class="file-upload" id="fileUpload">
                        <i class="fas fa-cloud-upload-alt"></i>
                        <div>点击或拖放文件到此区域</div>
                        <div class="hint">最大支持 2GB 文件</div>
                        <input type="file" id="fileInput">
                    </div>
                    <div class="file-info" id="fileInfo">
                        <div class="file-name" id="fileName">未选择文件</div>
                        <div class="file-size" id="fileSize">0 KB</div>
                    </div>
                </div>
                
                <button class="btn btn-primary" id="generateBtn" disabled>
                    <i class="fas fa-barcode"></i>
                    生成取件码
                </button>
                
                <div class="status-area" id="senderStatusArea">
                    <div class="alert alert-warning">
                        <i class="fas fa-exclamation-triangle"></i>
                        <div>请保持页面打开，等待接收方连接</div>
                    </div>
                    
                    <div class="code-display" id="codeDisplay">ABCDEF</div>
                    
                    <div class="connection-status">
                        <div class="status-item active" id="senderStatusItem">
                            <div class="status-label">发送方</div>
                            <div class="status-value">在线</div>
                        </div>
                        <div class="status-item inactive" id="receiverStatusItem">
                            <div class="status-label">接收方</div>
                            <div class="status-value">等待连接</div>
                        </div>
                    </div>
                    
                    <div class="transfer-stats" id="senderStats">
                        <div class="stat-card">
                            <div class="stat-value" id="speedStat">0 KB/s</div>
                            <div class="stat-label">传输速度</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="timeStat">0s</div>
                            <div class="stat-label">剩余时间</div>
                        </div>
                    </div>
                    
                    <div class="progress-container">
                        <div class="progress-info">
                            <span>传输进度</span>
                            <span id="progressPercent">0%</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" id="senderProgressFill"></div>
                        </div>
                    </div>
                    
                    <button class="btn btn-danger" id="cancelBtn">
                        <i class="fas fa-times"></i>
                        取消传输
                    </button>
                </div>
            </div>
            
            <!-- 接收端面板 -->
            <div class="panel" id="receiverPanel">
                <div class="form-group">
                    <label>输入取件码</label>
                    <div class="code-input-container">
                        <input type="text" class="code-input" id="codeInput" 
                               placeholder="输入6位取件码" maxlength="6" autocomplete="off">
                    </div>
                </div>
                
                <button class="btn btn-success" id="connectBtn">
                    <i class="fas fa-plug"></i>
                    连接房间
                </button>
                
                <div class="status-area" id="receiverStatusArea">
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle"></i>
                        <div>正在连接到发送方...</div>
                    </div>
                    
                    <div class="connection-status">
                        <div class="status-item inactive" id="remoteSenderStatus">
                            <div class="status-label">发送方</div>
                            <div class="status-value">离线</div>
                        </div>
                        <div class="status-item active" id="selfStatus">
                            <div class="status-label">接收方</div>
                            <div class="status-value">连接中</div>
                        </div>
                    </div>
                    
                    <div class="file-preview hidden" id="filePreview">
                        <div class="file-icon">
                            <i class="fas fa-file"></i>
                        </div>
                        <div class="file-details">
                            <div class="file-name-preview" id="previewFileName">未知文件</div>
                            <div class="file-size-preview" id="previewFileSize">0 KB</div>
                        </div>
                    </div>
                    
                    <div class="progress-container">
                        <div class="progress-info">
                            <span>接收进度</span>
                            <span id="receiverProgressPercent">0%</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" id="receiverProgressFill"></div>
                        </div>
                    </div>
                    
                    <button class="btn btn-success hidden" id="downloadBtn">
                        <i class="fas fa-download"></i>
                        下载文件
                    </button>
                    
                    <button class="btn btn-danger" id="disconnectBtn">
                        <i class="fas fa-times"></i>
                        断开连接
                    </button>
                </div>
            </div>
            
            <div class="instructions">
                <h3><i class="fas fa-info-circle"></i> 使用说明</h3>
                <ul>
                    <li><strong>发送文件：</strong>选择文件 → 生成取件码 → 将取件码告知接收方</li>
                    <li><strong>接收文件：</strong>输入6位取件码 → 连接房间 → 等待传输完成</li>
                    <li><strong>注意：</strong>传输期间请勿关闭页面，房间30分钟后自动失效</li>
                    <li><strong>安全：</strong>文件直接在浏览器间传输，服务器不存储任何数据</li>
                </ul>
            </div>
        </div>
    </div>

    <script>
        // ===================== 配置 =====================
        const API_BASE = '/api';
        const CHUNK_SIZE = 64 * 1024; // 64KB 分块
        const MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024; // 2GB
        
        // ===================== 状态变量 =====================
        let currentMode = 'sender';
        let currentRoomCode = null;
        let currentFile = null;
        let senderSocket = null;
        let receiverSocket = null;
        let fileChunks = [];
        let fileMetadata = null;
        let transferStartTime = null;
        let transferStats = {
            bytesTransferred: 0,
            lastUpdateTime: null,
            speed: 0
        };
        
        // ===================== DOM 元素 =====================
        const senderModeBtn = document.getElementById('senderModeBtn');
        const receiverModeBtn = document.getElementById('receiverModeBtn');
        const senderPanel = document.getElementById('senderPanel');
        const receiverPanel = document.getElementById('receiverPanel');
        
        // 发送端元素
        const fileInput = document.getElementById('fileInput');
        const fileUpload = document.getElementById('fileUpload');
        const fileInfo = document.getElementById('fileInfo');
        const fileName = document.getElementById('fileName');
        const fileSize = document.getElementById('fileSize');
        const generateBtn = document.getElementById('generateBtn');
        const senderStatusArea = document.getElementById('senderStatusArea');
        const codeDisplay = document.getElementById('codeDisplay');
        const senderProgressFill = document.getElementById('senderProgressFill');
        const progressPercent = document.getElementById('progressPercent');
        const speedStat = document.getElementById('speedStat');
        const timeStat = document.getElementById('timeStat');
        const cancelBtn = document.getElementById('cancelBtn');
        
        // 接收端元素
        const codeInput = document.getElementById('codeInput');
        const connectBtn = document.getElementById('connectBtn');
        const receiverStatusArea = document.getElementById('receiverStatusArea');
        const receiverProgressFill = document.getElementById('receiverProgressFill');
        const receiverProgressPercent = document.getElementById('receiverProgressPercent');
        const filePreview = document.getElementById('filePreview');
        const previewFileName = document.getElementById('previewFileName');
        const previewFileSize = document.getElementById('previewFileSize');
        const downloadBtn = document.getElementById('downloadBtn');
        const disconnectBtn = document.getElementById('disconnectBtn');
        
        // ===================== 工具函数 =====================
        function formatBytes(bytes, decimals = 2) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        }
        
        function formatTime(seconds) {
            if (seconds < 60) return Math.round(seconds) + '秒';
            if (seconds < 3600) return Math.floor(seconds / 60) + '分' + Math.round(seconds % 60) + '秒';
            return Math.floor(seconds / 3600) + '小时' + Math.floor((seconds % 3600) / 60) + '分';
        }
        
        function showMessage(message, type = 'info') {
            const alert = document.createElement('div');
            alert.className = `alert alert-${type}`;
            alert.innerHTML = `
                <i class="fas fa-${type === 'success' ? 'check-circle' : 
                                  type === 'warning' ? 'exclamation-triangle' : 
                                  type === 'danger' ? 'times-circle' : 'info-circle'}"></i>
                <div>${message}</div>
            `;
            
            const appCard = document.querySelector('.app-card');
            appCard.insertBefore(alert, document.querySelector('.instructions'));
            
            setTimeout(() => {
                alert.style.opacity = '0';
                alert.style.transition = 'opacity 0.3s';
                setTimeout(() => alert.remove(), 300);
            }, 4000);
        }
        
        function generateRoomCode() {
            const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
            let code = '';
            for (let i = 0; i < 6; i++) {
                code += chars.charAt(Math.floor(Math.random() * chars.length));
            }
            return code;
        }
        
        function updateConnectionStatus(element, connected) {
            element.classList.remove('active', 'inactive');
            element.classList.add(connected ? 'active' : 'inactive');
            element.querySelector('.status-value').textContent = connected ? '在线' : '离线';
        }
        
        function updateTransferStats(bytes) {
            const now = Date.now();
            transferStats.bytesTransferred += bytes;
            
            if (transferStats.lastUpdateTime) {
                const timeDiff = (now - transferStats.lastUpdateTime) / 1000;
                if (timeDiff > 0) {
                    const bytesPerSecond = bytes / timeDiff;
                    // 平滑处理速度计算
                    transferStats.speed = transferStats.speed * 0.7 + bytesPerSecond * 0.3;
                    speedStat.textContent = formatBytes(transferStats.speed) + '/s';
                    
                    if (transferStats.speed > 0) {
                        const remainingBytes = fileMetadata.size - transferStats.bytesTransferred;
                        const remainingTime = remainingBytes / transferStats.speed;
                        timeStat.textContent = formatTime(remainingTime);
                    }
                }
            }
            
            transferStats.lastUpdateTime = now;
        }
        
        // ===================== 模式切换 =====================
        senderModeBtn.addEventListener('click', () => {
            if (currentMode === 'sender') return;
            currentMode = 'sender';
            senderModeBtn.classList.add('active');
            receiverModeBtn.classList.remove('active');
            senderPanel.classList.add('active');
            receiverPanel.classList.remove('active');
            resetSenderState();
        });
        
        receiverModeBtn.addEventListener('click', () => {
            if (currentMode === 'receiver') return;
            currentMode = 'receiver';
            receiverModeBtn.classList.add('active');
            senderModeBtn.classList.remove('active');
            receiverPanel.classList.add('active');
            senderPanel.classList.remove('active');
            resetReceiverState();
        });
        
        // ===================== 拖放文件处理 =====================
        fileUpload.addEventListener('dragover', (e) => {
            e.preventDefault();
            fileUpload.classList.add('dragover');
        });
        
        fileUpload.addEventListener('dragleave', (e) => {
            e.preventDefault();
            fileUpload.classList.remove('dragover');
        });
        
        fileUpload.addEventListener('drop', (e) => {
            e.preventDefault();
            fileUpload.classList.remove('dragover');
            
            if (e.dataTransfer.files.length > 0) {
                fileInput.files = e.dataTransfer.files;
                handleFileSelect();
            }
        });
        
        // ===================== 文件选择 =====================
        function handleFileSelect() {
            if (fileInput.files.length > 0) {
                currentFile = fileInput.files[0];
                
                if (currentFile.size > MAX_FILE_SIZE) {
                    showMessage('文件太大，最大支持 2GB', 'danger');
                    resetFileInput();
                    return;
                }
                
                fileName.textContent = currentFile.name;
                fileSize.textContent = formatBytes(currentFile.size);
                fileInfo.classList.add('show');
                generateBtn.disabled = false;
            }
        }
        
        fileInput.addEventListener('change', handleFileSelect);
        
        function resetFileInput() {
            fileInput.value = '';
            currentFile = null;
            fileInfo.classList.remove('show');
            generateBtn.disabled = true;
        }
        
        // ===================== 发送文件 =====================
        generateBtn.addEventListener('click', async () => {
            if (!currentFile) {
                showMessage('请先选择文件', 'warning');
                return;
            }
            
            generateBtn.disabled = true;
            generateBtn.innerHTML = '<i class="fas fa-spinner fa-spin spinner"></i> 创建房间...';
            
            currentRoomCode = generateRoomCode();
            
            try {
                const response = await fetch(API_BASE + '/room/create', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ code: currentRoomCode })
                });
                
                const result = await response.json();
                
                if (!result.success) {
                    throw new Error(result.error || '创建房间失败');
                }
                
                codeDisplay.textContent = currentRoomCode;
                senderStatusArea.classList.add('active');
                
                // 连接 WebSocket
                connectAsSender(result.wsUrl);
                
                showMessage(`房间创建成功！取件码：${currentRoomCode}`, 'success');
                
                // 复制取件码到剪贴板
                navigator.clipboard.writeText(currentRoomCode)
                    .then(() => showMessage('取件码已复制到剪贴板', 'info'))
                    .catch(() => {});
                
            } catch (error) {
                showMessage('创建失败: ' + error.message, 'danger');
                generateBtn.disabled = false;
                generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
            }
        });
        
        function connectAsSender(wsUrl) {
            senderSocket = new WebSocket(wsUrl + '?role=sender');
            
            senderSocket.onopen = () => {
                console.log('发送方 WebSocket 连接成功');
                updateConnectionStatus(document.getElementById('senderStatusItem'), true);
                
                // 发送准备消息
                senderSocket.send(JSON.stringify({
                    type: 'ready',
                    timestamp: Date.now()
                }));
                
                // 启动心跳
                const heartbeatInterval = setInterval(() => {
                    if (senderSocket.readyState === WebSocket.OPEN) {
                        senderSocket.send(JSON.stringify({
                            type: 'heartbeat',
                            timestamp: Date.now()
                        }));
                    } else {
                        clearInterval(heartbeatInterval);
                    }
                }, 15000);
            };
            
            senderSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    
                    switch (data.type) {
                        case 'receiver-connected':
                            updateConnectionStatus(document.getElementById('receiverStatusItem'), true);
                            showMessage('接收方已连接，开始传输文件...', 'success');
                            startFileTransfer();
                            break;
                            
                        case 'receiver-disconnected':
                            updateConnectionStatus(document.getElementById('receiverStatusItem'), false);
                            showMessage('接收方已断开连接', 'warning');
                            break;
                            
                        case 'chunk-received':
                            // 块接收确认
                            break;
                            
                        case 'transfer-ready':
                            // 传输准备就绪
                            break;
                            
                        case 'heartbeat-response':
                            // 心跳响应
                            break;
                    }
                } catch (error) {
                    console.error('解析消息错误:', error);
                }
            };
            
            senderSocket.onclose = (event) => {
                console.log('发送方连接关闭:', event.code, event.reason);
                updateConnectionStatus(document.getElementById('receiverStatusItem'), false);
                
                if (event.code !== 1000) {
                    showMessage('连接已断开: ' + (event.reason || '未知错误'), 'danger');
                }
                
                resetSenderState();
            };
            
            senderSocket.onerror = (error) => {
                console.error('WebSocket错误:', error);
                showMessage('连接错误，请重试', 'danger');
                resetSenderState();
            };
        }
        
        function startFileTransfer() {
            if (!currentFile || !senderSocket) return;
            
            transferStartTime = Date.now();
            transferStats = {
                bytesTransferred: 0,
                lastUpdateTime: null,
                speed: 0
            };
            
            // 发送文件元数据
            fileMetadata = {
                name: currentFile.name,
                size: currentFile.size,
                type: currentFile.type,
                lastModified: currentFile.lastModified
            };
            
            senderSocket.send(JSON.stringify({
                type: 'file-metadata',
                metadata: fileMetadata,
                timestamp: Date.now()
            }));
            
            // 分块发送文件
            const totalChunks = Math.ceil(currentFile.size / CHUNK_SIZE);
            let chunkIndex = 0;
            
            function sendNextChunk() {
                if (chunkIndex >= totalChunks || senderSocket.readyState !== WebSocket.OPEN) {
                    // 发送完成
                    senderSocket.send(JSON.stringify({
                        type: 'transfer-complete',
                        timestamp: Date.now()
                    }));
                    
                    showMessage('文件传输完成！', 'success');
                    return;
                }
                
                const start = chunkIndex * CHUNK_SIZE;
                const end = Math.min(start + CHUNK_SIZE, currentFile.size);
                const chunk = currentFile.slice(start, end);
                
                const reader = new FileReader();
                
                reader.onload = function(e) {
                    if (senderSocket.readyState === WebSocket.OPEN) {
                        senderSocket.send(e.target.result);
                        
                        // 更新进度
                        const progress = Math.round(((chunkIndex + 1) / totalChunks) * 100);
                        senderProgressFill.style.width = progress + '%';
                        progressPercent.textContent = progress + '%';
                        
                        updateTransferStats(chunk.size);
                        
                        chunkIndex++;
                        
                        // 使用 setTimeout 避免阻塞
                        setTimeout(sendNextChunk, 0);
                    }
                };
                
                reader.readAsArrayBuffer(chunk);
            }
            
            sendNextChunk();
        }
        
        function resetSenderState() {
            if (senderSocket) {
                senderSocket.close();
                senderSocket = null;
            }
            
            senderStatusArea.classList.remove('active');
            senderProgressFill.style.width = '0%';
            progressPercent.textContent = '0%';
            speedStat.textContent = '0 KB/s';
            timeStat.textContent = '0s';
            
            generateBtn.disabled = false;
            generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
            
            currentRoomCode = null;
            fileMetadata = null;
            transferStats = {
                bytesTransferred: 0,
                lastUpdateTime: null,
                speed: 0
            };
        }
        
        cancelBtn.addEventListener('click', () => {
            if (confirm('确定要取消传输吗？')) {
                resetSenderState();
                showMessage('传输已取消', 'info');
            }
        });
        
        // ===================== 接收文件 =====================
        codeInput.addEventListener('input', function() {
            this.value = this.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
            
            if (this.value.length === 6) {
                connectBtn.disabled = false;
            } else {
                connectBtn.disabled = true;
            }
        });
        
        codeInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter' && this.value.length === 6) {
                connectBtn.click();
            }
        });
        
        connectBtn.addEventListener('click', async () => {
            const code = codeInput.value.trim();
            
            if (code.length !== 6) {
                showMessage('请输入6位取件码', 'warning');
                return;
            }
            
            connectBtn.disabled = true;
            connectBtn.innerHTML = '<i class="fas fa-spinner fa-spin spinner"></i> 检查房间...';
            
            try {
                const response = await fetch(API_BASE + '/room/' + code);
                const result = await response.json();
                
                if (!result.success) {
                    throw new Error(result.error || '房间不存在');
                }
                
                // 连接 WebSocket
                connectAsReceiver(result.wsUrl);
                
                receiverStatusArea.classList.add('active');
                showMessage('房间连接成功！等待文件...', 'success');
                
            } catch (error) {
                showMessage('连接失败: ' + error.message, 'danger');
                connectBtn.disabled = false;
                connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
            }
        });
        
        function connectAsReceiver(wsUrl) {
            receiverSocket = new WebSocket(wsUrl + '?role=receiver');
            
            receiverSocket.onopen = () => {
                console.log('接收方 WebSocket 连接成功');
                updateConnectionStatus(document.getElementById('selfStatus'), true);
                
                // 发送准备消息
                receiverSocket.send(JSON.stringify({
                    type: 'ready',
                    timestamp: Date.now()
                }));
            };
            
            receiverSocket.onmessage = async (event) => {
                try {
                    if (typeof event.data === 'string') {
                        const data = JSON.parse(event.data);
                        
                        switch (data.type) {
                            case 'sender-connected':
                                updateConnectionStatus(document.getElementById('remoteSenderStatus'), true);
                                showMessage('发送方已连接，等待文件...', 'info');
                                break;
                                
                            case 'sender-disconnected':
                                updateConnectionStatus(document.getElementById('remoteSenderStatus'), false);
                                showMessage('发送方已断开连接', 'warning');
                                break;
                                
                            case 'file-metadata':
                                fileMetadata = data.metadata;
                                previewFileName.textContent = fileMetadata.name;
                                previewFileSize.textContent = formatBytes(fileMetadata.size);
                                filePreview.classList.remove('hidden');
                                
                                // 初始化接收数组
                                fileChunks = new Array(Math.ceil(fileMetadata.size / CHUNK_SIZE));
                                showMessage('开始接收文件: ' + fileMetadata.name, 'info');
                                break;
                                
                            case 'transfer-complete':
                                // 合并文件
                                const blob = new Blob(fileChunks);
                                const url = URL.createObjectURL(blob);
                                
                                downloadBtn.onclick = () => {
                                    const a = document.createElement('a');
                                    a.href = url;
                                    a.download = fileMetadata.name;
                                    document.body.appendChild(a);
                                    a.click();
                                    document.body.removeChild(a);
                                    
                                    // 清理 URL
                                    setTimeout(() => URL.revokeObjectURL(url), 100);
                                };
                                
                                downloadBtn.classList.remove('hidden');
                                showMessage('文件接收完成！点击下载按钮保存文件', 'success');
                                break;
                        }
                    } else if (event.data instanceof ArrayBuffer) {
                        // 处理二进制数据
                        if (!fileMetadata) return;
                        
                        // 确定块的索引（简单轮询）
                        for (let i = 0; i < fileChunks.length; i++) {
                            if (!fileChunks[i]) {
                                fileChunks[i] = event.data;
                                
                                // 更新进度
                                const receivedChunks = fileChunks.filter(chunk => chunk).length;
                                const progress = Math.round((receivedChunks / fileChunks.length) * 100);
                                receiverProgressFill.style.width = progress + '%';
                                receiverProgressPercent.textContent = progress + '%';
                                
                                // 发送确认
                                if (receiverSocket.readyState === WebSocket.OPEN) {
                                    receiverSocket.send(JSON.stringify({
                                        type: 'chunk-received',
                                        timestamp: Date.now()
                                    }));
                                }
                                break;
                            }
                        }
                    }
                } catch (error) {
                    console.error('接收消息错误:', error);
                    showMessage('处理文件数据时出错', 'danger');
                }
            };
            
            receiverSocket.onclose = (event) => {
                console.log('接收方连接关闭:', event.code, event.reason);
                updateConnectionStatus(document.getElementById('remoteSenderStatus'), false);
                
                if (event.code !== 1000) {
                    showMessage('连接已断开: ' + (event.reason || '未知错误'), 'danger');
                }
            };
            
            receiverSocket.onerror = (error) => {
                console.error('WebSocket错误:', error);
                showMessage('连接错误，请重试', 'danger');
                resetReceiverState();
            };
        }
        
        function resetReceiverState() {
            if (receiverSocket) {
                receiverSocket.close();
                receiverSocket = null;
            }
            
            receiverStatusArea.classList.remove('active');
            receiverProgressFill.style.width = '0%';
            receiverProgressPercent.textContent = '0%';
            filePreview.classList.add('hidden');
            downloadBtn.classList.add('hidden');
            
            connectBtn.disabled = false;
            connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
            
            currentRoomCode = null;
            fileMetadata = null;
            fileChunks = [];
        }
        
        disconnectBtn.addEventListener('click', () => {
            if (confirm('确定要断开连接吗？')) {
                resetReceiverState();
                showMessage('已断开连接', 'info');
            }
        });
        
        downloadBtn.addEventListener('click', () => {
            // 点击后隐藏下载按钮
            setTimeout(() => {
                downloadBtn.classList.add('hidden');
                resetReceiverState();
            }, 1000);
        });
        
        // ===================== 页面生命周期 =====================
        window.addEventListener('beforeunload', (e) => {
            if (senderSocket || receiverSocket) {
                e.preventDefault();
                e.returnValue = '文件传输中，确定要离开吗？';
                return e.returnValue;
            }
        });
        
        // 健康检查
        async function checkHealth() {
            try {
                const response = await fetch(API_BASE + '/health');
                const data = await response.json();
                console.log('服务状态:', data.status, '房间数:', data.roomCount);
            } catch (err) {
                console.warn('健康检查失败:', err);
            }
        }
        
        // 初始健康检查
        checkHealth();
        
        // 每5分钟检查一次
        setInterval(checkHealth, 5 * 60 * 1000);
        
        console.log('即时传输客户端已加载');
    </script>
</body>
</html>`;