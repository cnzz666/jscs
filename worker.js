// ===================== 即时传输 Worker =====================
// 使用内存存储，简化实现，避免 Durable Objects 配置问题

// 存储所有房间数据（内存中）
let rooms = new Map();

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    
    // 记录请求
    console.log(`${new Date().toISOString()} ${request.method} ${pathname}`);
    
    // CORS 头
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    
    // 处理预检请求
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }
    
    // 主页面
    if (pathname === '/' || pathname === '/index.html') {
      return new Response(getHTML(), {
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
        roomCount: rooms.size,
        uptime: process.uptime ? process.uptime() : 0
      });
    }
    
    // 创建房间
    if (pathname === '/api/room/create' && request.method === 'POST') {
      try {
        const data = await request.json();
        const code = data.code;
        
        console.log(`创建房间请求: ${code}`);
        
        if (!code || code.length !== 6) {
          return Response.json({ 
            success: false, 
            error: '取件码必须是6位字符' 
          }, { status: 400 });
        }
        
        // 检查房间是否已存在
        if (rooms.has(code)) {
          const room = rooms.get(code);
          const now = Date.now();
          
          // 如果房间超过30分钟，清理它
          if (now - room.createdAt > 30 * 60 * 1000) {
            console.log(`清理过期房间: ${code}`);
            if (room.sender) {
              try { room.sender.close(1000, '房间过期'); } catch {}
            }
            if (room.receiver) {
              try { room.receiver.close(1000, '房间过期'); } catch {}
            }
            rooms.delete(code);
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
          senderId: null,
          receiverId: null,
          lastHeartbeat: null,
          fileInfo: null,
          history: []
        };
        
        rooms.set(code, room);
        console.log(`房间创建成功: ${code}, 当前房间数: ${rooms.size}`);
        
        // 30分钟后自动清理
        ctx.waitUntil(setTimeout(() => {
          if (rooms.has(code) && Date.now() - rooms.get(code).createdAt > 30 * 60 * 1000) {
            console.log(`自动清理房间: ${code}`);
            const oldRoom = rooms.get(code);
            if (oldRoom.sender) {
              try { oldRoom.sender.close(1000, '房间过期'); } catch {}
            }
            if (oldRoom.receiver) {
              try { oldRoom.receiver.close(1000, '房间过期'); } catch {}
            }
            rooms.delete(code);
          }
        }, 30 * 60 * 1000));
        
        return Response.json({ 
          success: true, 
          code: code,
          wsUrl: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}`,
          message: '房间创建成功',
          timestamp: Date.now()
        });
        
      } catch (error) {
        console.error('创建房间错误:', error);
        return Response.json({ 
          success: false, 
          error: '服务器错误: ' + error.message 
        }, { status: 500 });
      }
    }
    
    // 查询房间
    if (pathname.startsWith('/api/room/') && request.method === 'GET') {
      const code = pathname.split('/').pop();
      
      console.log(`查询房间: ${code}`);
      
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
        console.log(`房间已过期: ${code}`);
        if (room.sender) {
          try { room.sender.close(1000, '房间过期'); } catch {}
        }
        if (room.receiver) {
          try { room.receiver.close(1000, '房间过期'); } catch {}
        }
        rooms.delete(code);
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
        fileInfo: room.fileInfo,
        wsUrl: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}`
      });
    }
    
    // WebSocket 连接
    if (pathname.startsWith('/api/ws/')) {
      const code = pathname.split('/').pop();
      const role = url.searchParams.get('role');
      const clientId = url.searchParams.get('clientId');
      
      console.log(`WebSocket连接请求: ${code}, 角色: ${role}, 客户端ID: ${clientId}`);
      
      if (!code || code.length !== 6) {
        return new Response('无效的房间代码', { status: 400 });
      }
      
      if (!role || !['sender', 'receiver'].includes(role)) {
        return new Response('无效的角色，必须是 sender 或 receiver', { status: 400 });
      }
      
      const room = rooms.get(code);
      if (!room) {
        return new Response('房间不存在', { status: 404 });
      }
      
      // 检查房间是否过期
      if (Date.now() - room.createdAt > 30 * 60 * 1000) {
        console.log(`连接时房间已过期: ${code}`);
        rooms.delete(code);
        return new Response('房间已过期', { status: 410 });
      }
      
      // 如果是发送方，检查是否已有发送方连接
      if (role === 'sender' && room.sender) {
        return new Response('发送方已连接', { status: 409 });
      }
      
      // 如果是接收方，检查是否已有接收方连接
      if (role === 'receiver' && room.receiver) {
        return new Response('接收方已连接', { status: 409 });
      }
      
      // 创建 WebSocket 连接
      const { 0: client, 1: server } = new WebSocketPair();
      
      // 接受连接
      server.accept();
      
      // 保存连接
      const actualClientId = clientId || `${role}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      
      if (role === 'sender') {
        room.sender = server;
        room.senderId = actualClientId;
        room.lastHeartbeat = Date.now();
        console.log(`发送方连接: ${code}, 客户端ID: ${actualClientId}`);
      } else {
        room.receiver = server;
        room.receiverId = actualClientId;
        console.log(`接收方连接: ${code}, 客户端ID: ${actualClientId}`);
      }
      
      // 更新房间状态
      if (room.sender && room.receiver) {
        room.status = 'active';
      } else if (room.sender) {
        room.status = 'waiting_receiver';
      } else if (room.receiver) {
        room.status = 'waiting_sender';
      }
      
      // 记录历史
      room.history.push({
        timestamp: Date.now(),
        event: `${role}_connected`,
        clientId: actualClientId
      });
      
      // 发送连接成功的消息
      server.send(JSON.stringify({
        type: 'connected',
        success: true,
        role: role,
        clientId: actualClientId,
        roomCode: code,
        timestamp: Date.now(),
        message: '连接成功'
      }));
      
      // 通知另一方
      if (role === 'sender' && room.receiver) {
        room.receiver.send(JSON.stringify({
          type: 'sender-connected',
          clientId: actualClientId,
          timestamp: Date.now(),
          message: '发送方已连接'
        }));
      } else if (role === 'receiver' && room.sender) {
        room.sender.send(JSON.stringify({
          type: 'receiver-connected',
          clientId: actualClientId,
          timestamp: Date.now(),
          message: '接收方已连接'
        }));
      }
      
      // 设置消息处理
      server.addEventListener('message', async (event) => {
        try {
          const data = JSON.parse(event.data);
          await handleWebSocketMessage(data, server, role, room);
        } catch (error) {
          console.error('WebSocket消息处理错误:', error);
        }
      });
      
      // 设置连接关闭处理
      server.addEventListener('close', (event) => {
        console.log(`WebSocket连接关闭: ${code}, 角色: ${role}, 代码: ${event.code}, 原因: ${event.reason}`);
        
        // 记录历史
        room.history.push({
          timestamp: Date.now(),
          event: `${role}_disconnected`,
          code: event.code,
          reason: event.reason
        });
        
        // 清理连接
        if (role === 'sender') {
          room.sender = null;
          room.senderId = null;
          
          // 通知接收方
          if (room.receiver) {
            room.receiver.send(JSON.stringify({
              type: 'sender-disconnected',
              timestamp: Date.now(),
              message: '发送方已断开连接'
            }));
          }
        } else {
          room.receiver = null;
          room.receiverId = null;
          
          // 通知发送方
          if (room.sender) {
            room.sender.send(JSON.stringify({
              type: 'receiver-disconnected',
              timestamp: Date.now(),
              message: '接收方已断开连接'
            }));
          }
        }
        
        // 更新状态
        if (!room.sender && !room.receiver) {
          room.status = 'closed';
          // 10分钟后清理空房间
          ctx.waitUntil(setTimeout(() => {
            if (rooms.get(code) === room && !room.sender && !room.receiver) {
              console.log(`清理空房间: ${code}`);
              rooms.delete(code);
            }
          }, 10 * 60 * 1000));
        } else if (room.sender && !room.receiver) {
          room.status = 'waiting_receiver';
        } else if (!room.sender && room.receiver) {
          room.status = 'waiting_sender';
        }
      });
      
      // 设置错误处理
      server.addEventListener('error', (error) => {
        console.error(`WebSocket错误 (${code}, ${role}):`, error);
      });
      
      // 如果是发送方，启动心跳检查
      if (role === 'sender') {
        startHeartbeatCheck(room, ctx);
      }
      
      return new Response(null, {
        status: 101,
        webSocket: client
      });
    }
    
    // 其他 API 路由
    if (pathname === '/api/stats') {
      const roomStats = [];
      for (const [code, room] of rooms) {
        roomStats.push({
          code,
          status: room.status,
          createdAt: room.createdAt,
          age: Date.now() - room.createdAt,
          hasSender: !!room.sender,
          hasReceiver: !!room.receiver,
          historyLength: room.history.length
        });
      }
      
      return Response.json({
        success: true,
        totalRooms: rooms.size,
        rooms: roomStats,
        timestamp: Date.now()
      });
    }
    
    // 清理所有房间（调试用）
    if (pathname === '/api/cleanup' && request.method === 'POST') {
      const oldCount = rooms.size;
      
      for (const [code, room] of rooms) {
        if (room.sender) {
          try { room.sender.close(1000, '清理'); } catch {}
        }
        if (room.receiver) {
          try { room.receiver.close(1000, '清理'); } catch {}
        }
      }
      
      rooms.clear();
      
      return Response.json({
        success: true,
        message: `清理了 ${oldCount} 个房间`,
        timestamp: Date.now()
      });
    }
    
    // 404 处理
    return new Response('Not Found', { status: 404 });
  }
};

// 处理 WebSocket 消息
async function handleWebSocketMessage(data, socket, role, room) {
  const timestamp = Date.now();
  
  // 记录历史
  room.history.push({
    timestamp,
    event: 'message',
    role,
    type: data.type,
    dataSize: JSON.stringify(data).length
  });
  
  switch (data.type) {
    case 'heartbeat':
      room.lastHeartbeat = timestamp;
      // 发送响应
      socket.send(JSON.stringify({
        type: 'heartbeat-response',
        timestamp,
        receivedAt: room.lastHeartbeat
      }));
      break;
      
    case 'file-metadata':
      room.fileInfo = data.metadata;
      console.log(`文件元数据: ${room.code}, 文件名: ${data.metadata.name}, 大小: ${data.metadata.size}`);
      
      // 转发给接收方
      if (room.receiver) {
        room.receiver.send(JSON.stringify({
          type: 'file-metadata',
          metadata: data.metadata,
          timestamp
        }));
      }
      break;
      
    case 'file-chunk':
      // 直接转发给接收方
      if (room.receiver) {
        room.receiver.send(JSON.stringify({
          type: 'file-chunk',
          chunk: data.chunk,
          index: data.index,
          total: data.total,
          timestamp
        }));
      }
      break;
      
    case 'transfer-complete':
      console.log(`文件传输完成: ${room.code}`);
      
      // 转发给接收方
      if (room.receiver) {
        room.receiver.send(JSON.stringify({
          type: 'transfer-complete',
          timestamp,
          metadata: room.fileInfo
        }));
      }
      break;
      
    case 'sender-status':
      // 发送方状态更新
      if (room.receiver && role === 'sender') {
        room.receiver.send(JSON.stringify({
          type: 'sender-status',
          connected: data.connected || true,
          timestamp,
          message: data.message || '发送方在线'
        }));
      }
      break;
      
    case 'error':
      console.error(`客户端错误 (${room.code}, ${role}):`, data.message);
      
      // 转发错误给另一方
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
      
    default:
      console.log(`未知消息类型: ${data.type}`);
  }
}

// 启动心跳检查
function startHeartbeatCheck(room, ctx) {
  // 每隔30秒检查一次心跳
  const interval = setInterval(() => {
    // 如果房间已被清理，停止检查
    if (!rooms.has(room.code) || rooms.get(room.code) !== room) {
      clearInterval(interval);
      return;
    }
    
    const now = Date.now();
    
    // 如果超过45秒没有心跳，断开发送方连接
    if (room.lastHeartbeat && now - room.lastHeartbeat > 45000) {
      console.log(`心跳超时: ${room.code}, 最后心跳: ${room.lastHeartbeat}, 当前: ${now}`);
      
      if (room.sender) {
        room.sender.close(1001, '心跳超时');
        room.sender = null;
        room.senderId = null;
      }
      
      clearInterval(interval);
    }
  }, 30000);
  
  // 清理定时器
  ctx.waitUntil(Promise.resolve().then(() => {
    return new Promise(resolve => {
      // 60分钟后清理定时器
      setTimeout(() => {
        clearInterval(interval);
        resolve();
      }, 60 * 60 * 1000);
    });
  }));
}

// HTML 页面
function getHTML() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ 即时传输 - 极速文件快传</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
        }
        
        :root {
            --primary: #3b82f6;
            --primary-dark: #1d4ed8;
            --secondary: #8b5cf6;
            --success: #10b981;
            --success-dark: #059669;
            --warning: #f59e0b;
            --danger: #ef4444;
            --danger-dark: #dc2626;
            --light: #f8fafc;
            --dark: #1e293b;
            --gray-50: #f8fafc;
            --gray-100: #f1f5f9;
            --gray-200: #e2e8f0;
            --gray-300: #cbd5e1;
            --gray-400: #94a3b8;
            --gray-500: #64748b;
            --gray-600: #475569;
            --gray-700: #334155;
            --gray-800: #1e293b;
            --gray-900: #0f172a;
            
            --shadow-sm: 0 1px 3px rgba(0,0,0,0.12);
            --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.1);
            --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.1);
            --shadow-xl: 0 20px 25px -5px rgba(0,0,0,0.1);
            
            --radius-sm: 0.375rem;
            --radius-md: 0.5rem;
            --radius-lg: 0.75rem;
            --radius-xl: 1rem;
            --radius-2xl: 1.5rem;
            
            --transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        body {
            background: linear-gradient(135deg, 
                #667eea 0%, 
                #764ba2 25%, 
                #f093fb 50%, 
                #f5576c 75%, 
                #f093fb 100%);
            background-size: 400% 400%;
            animation: gradient 15s ease infinite;
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 1rem;
            color: var(--gray-800);
            line-height: 1.5;
        }
        
        @keyframes gradient {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        .container {
            width: 100%;
            max-width: 480px;
            margin: 0 auto;
        }
        
        .app-card {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: var(--radius-2xl);
            padding: 2.5rem 2rem;
            box-shadow: var(--shadow-xl), 0 0 0 1px rgba(255, 255, 255, 0.1);
            border: 1px solid rgba(255, 255, 255, 0.2);
            position: relative;
            overflow: hidden;
        }
        
        .app-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, var(--primary), var(--secondary));
        }
        
        .header {
            text-align: center;
            margin-bottom: 2.5rem;
        }
        
        .logo {
            font-size: 3.5rem;
            margin-bottom: 1rem;
            display: inline-block;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        h1 {
            font-size: 2.25rem;
            font-weight: 800;
            color: var(--gray-900);
            margin-bottom: 0.5rem;
            letter-spacing: -0.025em;
        }
        
        .subtitle {
            font-size: 1rem;
            color: var(--gray-600);
            font-weight: 500;
            margin-bottom: 0.25rem;
        }
        
        .mode-selector {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.75rem;
            margin-bottom: 2rem;
            background: var(--gray-100);
            padding: 0.5rem;
            border-radius: var(--radius-lg);
        }
        
        .mode-btn {
            padding: 1rem 1.25rem;
            border: none;
            background: transparent;
            border-radius: var(--radius-md);
            font-size: 1rem;
            font-weight: 600;
            color: var(--gray-600);
            cursor: pointer;
            transition: var(--transition);
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.75rem;
        }
        
        .mode-btn:hover {
            background: rgba(255, 255, 255, 0.5);
            color: var(--gray-700);
        }
        
        .mode-btn.active {
            background: white;
            color: var(--primary);
            box-shadow: var(--shadow-md);
        }
        
        .mode-btn i {
            font-size: 1.25rem;
        }
        
        .panel {
            display: none;
            animation: slideIn 0.3s ease-out;
        }
        
        .panel.active {
            display: block;
        }
        
        @keyframes slideIn {
            from {
                opacity: 0;
                transform: translateY(10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        .form-group {
            margin-bottom: 1.5rem;
        }
        
        .form-label {
            display: block;
            margin-bottom: 0.5rem;
            font-weight: 600;
            color: var(--gray-700);
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .file-select-area {
            border: 2px dashed var(--gray-300);
            border-radius: var(--radius-lg);
            padding: 3rem 1.5rem;
            text-align: center;
            cursor: pointer;
            transition: var(--transition);
            position: relative;
            background: var(--gray-50);
            margin-bottom: 1rem;
        }
        
        .file-select-area:hover {
            border-color: var(--primary);
            background: rgba(59, 130, 246, 0.05);
        }
        
        .file-select-area.active {
            border-color: var(--success);
            background: rgba(16, 185, 129, 0.05);
        }
        
        .file-select-icon {
            width: 4rem;
            height: 4rem;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 1rem;
            color: white;
            font-size: 1.5rem;
            transition: var(--transition);
        }
        
        .file-select-area:hover .file-select-icon {
            transform: scale(1.05);
        }
        
        .file-select-text {
            font-size: 1.125rem;
            font-weight: 600;
            color: var(--gray-700);
            margin-bottom: 0.25rem;
        }
        
        .file-select-subtext {
            font-size: 0.875rem;
            color: var(--gray-500);
        }
        
        .file-info {
            background: var(--gray-50);
            border-radius: var(--radius-lg);
            padding: 1.25rem;
            border-left: 4px solid var(--primary);
            display: none;
        }
        
        .file-info.active {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        
        .file-icon {
            width: 3rem;
            height: 3rem;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            border-radius: var(--radius-md);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 1.25rem;
        }
        
        .file-details {
            flex: 1;
        }
        
        .file-name {
            font-weight: 600;
            color: var(--gray-800);
            margin-bottom: 0.25rem;
            word-break: break-all;
        }
        
        .file-size {
            font-size: 0.875rem;
            color: var(--gray-500);
        }
        
        .code-input-container {
            position: relative;
        }
        
        .code-input {
            width: 100%;
            padding: 1rem 1.25rem;
            font-size: 1.5rem;
            font-weight: 700;
            text-align: center;
            letter-spacing: 0.5em;
            font-family: 'Courier New', Monaco, monospace;
            border: 2px solid var(--gray-300);
            border-radius: var(--radius-lg);
            background: white;
            color: var(--gray-800);
            transition: var(--transition);
            text-transform: uppercase;
        }
        
        .code-input:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
        }
        
        .code-input::placeholder {
            letter-spacing: normal;
            color: var(--gray-400);
        }
        
        .btn {
            width: 100%;
            padding: 1rem 1.5rem;
            border: none;
            border-radius: var(--radius-lg);
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: var(--transition);
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.75rem;
            position: relative;
            overflow: hidden;
        }
        
        .btn::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: linear-gradient(rgba(255,255,255,0.1), rgba(255,255,255,0));
            opacity: 0;
            transition: var(--transition);
        }
        
        .btn:hover::after {
            opacity: 1;
        }
        
        .btn-primary {
            background: linear-gradient(135deg, var(--primary), var(--primary-dark));
            color: white;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.25);
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(59, 130, 246, 0.35);
        }
        
        .btn-success {
            background: linear-gradient(135deg, var(--success), var(--success-dark));
            color: white;
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.25);
        }
        
        .btn-success:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(16, 185, 129, 0.35);
        }
        
        .btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none !important;
            box-shadow: none !important;
        }
        
        .btn i.fa-spinner {
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
        
        .status-area {
            background: var(--gray-50);
            border-radius: var(--radius-xl);
            padding: 1.5rem;
            margin-top: 2rem;
            border: 1px solid var(--gray-200);
            display: none;
        }
        
        .status-area.active {
            display: block;
            animation: slideIn 0.3s ease-out;
        }
        
        .alert {
            padding: 1rem 1.25rem;
            border-radius: var(--radius-lg);
            margin-bottom: 1.25rem;
            display: flex;
            align-items: flex-start;
            gap: 0.75rem;
            line-height: 1.4;
        }
        
        .alert i {
            font-size: 1.25rem;
            margin-top: 0.125rem;
        }
        
        .alert-warning {
            background: rgba(245, 158, 11, 0.1);
            border: 1px solid rgba(245, 158, 11, 0.2);
            color: #92400e;
        }
        
        .alert-info {
            background: rgba(59, 130, 246, 0.1);
            border: 1px solid rgba(59, 130, 246, 0.2);
            color: #1e40af;
        }
        
        .alert-success {
            background: rgba(16, 185, 129, 0.1);
            border: 1px solid rgba(16, 185, 129, 0.2);
            color: #065f46;
        }
        
        .alert-danger {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.2);
            color: #991b1b;
        }
        
        .code-display-container {
            text-align: center;
            margin: 1.5rem 0;
        }
        
        .code-label {
            font-size: 0.875rem;
            color: var(--gray-500);
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .code-display {
            font-size: 2.5rem;
            font-weight: 800;
            letter-spacing: 0.5em;
            color: var(--primary);
            font-family: 'Courier New', Monaco, monospace;
            padding: 1rem;
            background: white;
            border-radius: var(--radius-lg);
            border: 2px solid var(--gray-200);
            margin-bottom: 1rem;
            user-select: all;
        }
        
        .copy-btn {
            background: var(--gray-100);
            color: var(--gray-700);
            border: 1px solid var(--gray-300);
            padding: 0.75rem 1.5rem;
            border-radius: var(--radius-md);
            font-weight: 600;
            cursor: pointer;
            transition: var(--transition);
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .copy-btn:hover {
            background: var(--gray-200);
            color: var(--gray-800);
        }
        
        .connection-status {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
            margin: 1.5rem 0;
        }
        
        .connection-item {
            background: white;
            border-radius: var(--radius-lg);
            padding: 1.25rem;
            text-align: center;
            border: 2px solid var(--gray-200);
            transition: var(--transition);
        }
        
        .connection-item.active {
            border-color: var(--success);
            background: rgba(16, 185, 129, 0.05);
        }
        
        .connection-item.inactive {
            border-color: var(--danger);
            background: rgba(239, 68, 68, 0.05);
        }
        
        .connection-label {
            font-size: 0.875rem;
            color: var(--gray-500);
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .connection-value {
            font-size: 1.125rem;
            font-weight: 700;
            color: var(--gray-800);
        }
        
        .progress-container {
            margin: 1.5rem 0;
        }
        
        .progress-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 0.75rem;
            font-weight: 600;
            color: var(--gray-700);
        }
        
        .progress-bar {
            height: 0.75rem;
            background: var(--gray-200);
            border-radius: 9999px;
            overflow: hidden;
            margin-bottom: 0.5rem;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--primary), var(--secondary));
            border-radius: 9999px;
            width: 0%;
            transition: width 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        
        .progress-fill::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(
                90deg,
                rgba(255, 255, 255, 0) 0%,
                rgba(255, 255, 255, 0.3) 50%,
                rgba(255, 255, 255, 0) 100%
            );
            animation: shimmer 2s infinite;
        }
        
        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }
        
        .progress-text {
            text-align: center;
            font-size: 0.875rem;
            color: var(--gray-500);
        }
        
        .download-section {
            text-align: center;
            margin-top: 1.5rem;
        }
        
        .action-buttons {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
            margin-top: 1.5rem;
        }
        
        .action-btn {
            padding: 0.875rem 1.25rem;
        }
        
        .action-btn-secondary {
            background: var(--gray-100);
            color: var(--gray-700);
            border: 1px solid var(--gray-300);
        }
        
        .action-btn-secondary:hover {
            background: var(--gray-200);
            color: var(--gray-800);
        }
        
        .instructions {
            margin-top: 2rem;
            padding: 1.5rem;
            background: var(--gray-50);
            border-radius: var(--radius-xl);
            border-left: 4px solid var(--primary);
        }
        
        .instructions h3 {
            font-size: 1.125rem;
            font-weight: 700;
            color: var(--gray-800);
            margin-bottom: 1rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .instructions ul {
            list-style: none;
            padding: 0;
        }
        
        .instructions li {
            margin-bottom: 0.75rem;
            padding-left: 1.5rem;
            position: relative;
            color: var(--gray-600);
        }
        
        .instructions li::before {
            content: '→';
            position: absolute;
            left: 0;
            color: var(--primary);
            font-weight: bold;
        }
        
        .instructions strong {
            color: var(--gray-800);
        }
        
        .hidden {
            display: none !important;
        }
        
        .toast {
            position: fixed;
            bottom: 1.5rem;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: var(--gray-800);
            color: white;
            padding: 1rem 1.5rem;
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-xl);
            z-index: 1000;
            transition: transform 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
            display: flex;
            align-items: center;
            gap: 0.75rem;
            max-width: 90%;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
        }
        
        .toast-success {
            background: var(--success);
        }
        
        .toast-error {
            background: var(--danger);
        }
        
        .toast-warning {
            background: var(--warning);
        }
        
        @media (max-width: 480px) {
            .app-card {
                padding: 1.5rem 1.25rem;
            }
            
            h1 {
                font-size: 1.75rem;
            }
            
            .code-display {
                font-size: 1.75rem;
                letter-spacing: 0.3em;
                padding: 0.75rem;
            }
            
            .connection-status {
                grid-template-columns: 1fr;
            }
        }
        
        .debug-info {
            font-size: 0.75rem;
            color: var(--gray-400);
            text-align: center;
            margin-top: 1rem;
            padding-top: 1rem;
            border-top: 1px solid var(--gray-200);
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="app-card">
            <div class="header">
                <div class="logo">
                    <i class="fas fa-bolt"></i>
                </div>
                <h1>即时传输</h1>
                <p class="subtitle">极速安全的文件传输工具</p>
                <p class="subtitle">点对点直传，服务器不存储文件</p>
            </div>
            
            <div class="mode-selector">
                <button class="mode-btn active" id="senderModeBtn">
                    <i class="fas fa-cloud-upload-alt"></i>
                    <span>发送文件</span>
                </button>
                <button class="mode-btn" id="receiverModeBtn">
                    <i class="fas fa-cloud-download-alt"></i>
                    <span>接收文件</span>
                </button>
            </div>
            
            <!-- 发送端面板 -->
            <div class="panel active" id="senderPanel">
                <div class="form-group">
                    <label class="form-label">选择要发送的文件</label>
                    <div class="file-select-area" id="fileSelectArea">
                        <div class="file-select-icon">
                            <i class="fas fa-cloud-upload-alt"></i>
                        </div>
                        <div class="file-select-text">点击选择文件</div>
                        <div class="file-select-subtext">或拖放文件到这里</div>
                        <input type="file" id="fileInput" class="hidden">
                    </div>
                    
                    <div class="file-info" id="fileInfo">
                        <div class="file-icon">
                            <i class="fas fa-file"></i>
                        </div>
                        <div class="file-details">
                            <div class="file-name" id="fileName">未选择文件</div>
                            <div class="file-size" id="fileSize">0 B</div>
                        </div>
                        <button class="btn action-btn-secondary" id="changeFileBtn">
                            <i class="fas fa-exchange-alt"></i>
                            更换
                        </button>
                    </div>
                </div>
                
                <button class="btn btn-primary" id="generateBtn" disabled>
                    <i class="fas fa-barcode"></i>
                    <span>生成取件码并创建房间</span>
                </button>
                
                <div class="status-area" id="senderStatusArea">
                    <div class="alert alert-warning">
                        <i class="fas fa-exclamation-triangle"></i>
                        <div>
                            <strong>重要提示：</strong> 请勿关闭此页面或刷新，否则传输会中断。取件码将在30分钟后过期。
                        </div>
                    </div>
                    
                    <div class="code-display-container">
                        <div class="code-label">取件码</div>
                        <div class="code-display" id="codeDisplay">ABCDEF</div>
                        <button class="copy-btn" id="copyCodeBtn">
                            <i class="fas fa-copy"></i>
                            复制取件码
                        </button>
                    </div>
                    
                    <div class="connection-status">
                        <div class="connection-item active" id="senderConnectionItem">
                            <div class="connection-label">发送方</div>
                            <div class="connection-value">在线</div>
                        </div>
                        <div class="connection-item inactive" id="receiverConnectionItem">
                            <div class="connection-label">接收方</div>
                            <div class="connection-value">等待连接</div>
                        </div>
                    </div>
                    
                    <div class="alert alert-info" id="senderStatusInfo">
                        <i class="fas fa-info-circle"></i>
                        <div id="senderStatusText">等待接收方连接...</div>
                    </div>
                    
                    <div class="progress-container hidden" id="senderProgressContainer">
                        <div class="progress-header">
                            <span>发送进度</span>
                            <span id="senderProgressPercent">0%</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" id="senderProgressFill"></div>
                        </div>
                        <div class="progress-text" id="senderProgressText">等待传输开始...</div>
                    </div>
                    
                    <div class="action-buttons">
                        <button class="btn btn-success" id="startTransferBtn" disabled>
                            <i class="fas fa-play"></i>
                            开始传输
                        </button>
                        <button class="btn action-btn-secondary" id="cancelBtn">
                            <i class="fas fa-times"></i>
                            取消
                        </button>
                    </div>
                </div>
            </div>
            
            <!-- 接收端面板 -->
            <div class="panel" id="receiverPanel">
                <div class="form-group">
                    <label class="form-label">输入取件码</label>
                    <div class="code-input-container">
                        <input type="text" class="code-input" id="codeInput" 
                               placeholder="ABCDEF" maxlength="6" 
                               pattern="[A-Z0-9]{6}" title="请输入6位大写字母或数字">
                    </div>
                </div>
                
                <button class="btn btn-success" id="connectBtn">
                    <i class="fas fa-plug"></i>
                    连接房间
                </button>
                
                <div class="status-area" id="receiverStatusArea">
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle"></i>
                        <div id="receiverStatusText">正在连接到房间...</div>
                    </div>
                    
                    <div class="connection-status">
                        <div class="connection-item inactive" id="remoteSenderStatus">
                            <div class="connection-label">发送方</div>
                            <div class="connection-value">离线</div>
                        </div>
                        <div class="connection-item active" id="selfStatus">
                            <div class="connection-label">接收方</div>
                            <div class="connection-value">连接中</div>
                        </div>
                    </div>
                    
                    <div class="alert" id="connectionStatusAlert">
                        <i class="fas fa-sync fa-spin"></i>
                        <div>等待发送方连接...</div>
                    </div>
                    
                    <div class="progress-container">
                        <div class="progress-header">
                            <span>接收进度</span>
                            <span id="receiverProgressPercent">0%</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" id="receiverProgressFill"></div>
                        </div>
                        <div class="progress-text" id="receiverProgressText">等待文件传输...</div>
                    </div>
                    
                    <div class="download-section hidden" id="downloadSection">
                        <a class="btn btn-success" id="downloadBtn" download>
                            <i class="fas fa-download"></i>
                            下载文件
                        </a>
                    </div>
                    
                    <button class="btn action-btn-secondary" id="disconnectBtn">
                        <i class="fas fa-plug"></i>
                        断开连接
                    </button>
                </div>
            </div>
            
            <div class="instructions">
                <h3><i class="fas fa-lightbulb"></i> 使用说明</h3>
                <ul>
                    <li><strong>发送文件</strong>：选择文件 → 生成取件码 → 将取件码告知接收方 → 等待连接 → 开始传输</li>
                    <li><strong>接收文件</strong>：输入取件码 → 连接房间 → 等待文件 → 下载保存</li>
                    <li><strong>注意事项</strong>：传输期间请勿关闭页面，文件直接在双方之间传输，服务器不存储</li>
                    <li><strong>传输限制</strong>：单个文件最大支持2GB，取件码30分钟内有效</li>
                </ul>
            </div>
            
            <div class="debug-info" id="debugInfo">
                服务状态: <span id="serviceStatus">检查中...</span>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast"></div>

    <script>
        // ===================== 配置 =====================
        const API_BASE = '/api';
        const WS_RECONNECT_DELAY = 3000;
        const HEARTBEAT_INTERVAL = 2000;
        const CONNECTION_CHECK_INTERVAL = 5000;
        
        // ===================== 状态变量 =====================
        let currentRoomCode = null;
        let currentFile = null;
        let senderSocket = null;
        let receiverSocket = null;
        let heartbeatTimer = null;
        let connectionCheckTimer = null;
        let senderStartTime = null;
        let senderTimerInterval = null;
        let lastSenderStatusTime = null;
        
        // 文件传输状态
        let fileMetadata = null;
        let receivedChunks = [];
        let fileSize = 0;
        let fileName = '';
        let receivedSize = 0;
        let totalChunks = 0;
        let currentChunk = 0;
        
        // ===================== DOM 元素 =====================
        const senderModeBtn = document.getElementById('senderModeBtn');
        const receiverModeBtn = document.getElementById('receiverModeBtn');
        const senderPanel = document.getElementById('senderPanel');
        const receiverPanel = document.getElementById('receiverPanel');
        
        // 发送端元素
        const fileSelectArea = document.getElementById('fileSelectArea');
        const fileInput = document.getElementById('fileInput');
        const fileInfo = document.getElementById('fileInfo');
        const fileNameEl = document.getElementById('fileName');
        const fileSizeEl = document.getElementById('fileSize');
        const changeFileBtn = document.getElementById('changeFileBtn');
        const generateBtn = document.getElementById('generateBtn');
        const senderStatusArea = document.getElementById('senderStatusArea');
        const codeDisplay = document.getElementById('codeDisplay');
        const copyCodeBtn = document.getElementById('copyCodeBtn');
        const senderStatusInfo = document.getElementById('senderStatusInfo');
        const senderStatusText = document.getElementById('senderStatusText');
        const senderProgressContainer = document.getElementById('senderProgressContainer');
        const senderProgressFill = document.getElementById('senderProgressFill');
        const senderProgressPercent = document.getElementById('senderProgressPercent');
        const senderProgressText = document.getElementById('senderProgressText');
        const startTransferBtn = document.getElementById('startTransferBtn');
        const cancelBtn = document.getElementById('cancelBtn');
        const senderConnectionItem = document.getElementById('senderConnectionItem');
        const receiverConnectionItem = document.getElementById('receiverConnectionItem');
        
        // 接收端元素
        const codeInput = document.getElementById('codeInput');
        const connectBtn = document.getElementById('connectBtn');
        const receiverStatusArea = document.getElementById('receiverStatusArea');
        const receiverStatusText = document.getElementById('receiverStatusText');
        const receiverProgressFill = document.getElementById('receiverProgressFill');
        const receiverProgressPercent = document.getElementById('receiverProgressPercent');
        const receiverProgressText = document.getElementById('receiverProgressText');
        const downloadSection = document.getElementById('downloadSection');
        const downloadBtn = document.getElementById('downloadBtn');
        const disconnectBtn = document.getElementById('disconnectBtn');
        const remoteSenderStatus = document.getElementById('remoteSenderStatus');
        const selfStatus = document.getElementById('selfStatus');
        const connectionStatusAlert = document.getElementById('connectionStatusAlert');
        const debugInfo = document.getElementById('debugInfo');
        const serviceStatus = document.getElementById('serviceStatus');
        
        // 全局元素
        const toast = document.getElementById('toast');
        
        // ===================== 工具函数 =====================
        
        function formatBytes(bytes, decimals = 2) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        }
        
        function generateRoomCode() {
            const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
            let code = '';
            for (let i = 0; i < 6; i++) {
                code += chars.charAt(Math.floor(Math.random() * chars.length));
            }
            return code;
        }
        
        function showToast(message, type = 'info', duration = 4000) {
            toast.textContent = message;
            toast.className = 'toast';
            toast.classList.add(`toast-${type}`);
            
            setTimeout(() => {
                toast.classList.add('show');
            }, 10);
            
            setTimeout(() => {
                toast.classList.remove('show');
            }, duration);
        }
        
        function updateConnectionStatus(element, connected, text = null) {
            element.classList.remove('active', 'inactive');
            element.classList.add(connected ? 'active' : 'inactive');
            if (text) {
                element.querySelector('.connection-value').textContent = text;
            }
        }
        
        function updateServiceStatus(status) {
            serviceStatus.textContent = status;
            serviceStatus.style.color = status === '正常' ? '#10b981' : 
                                      status === '异常' ? '#ef4444' : 
                                      '#f59e0b';
        }
        
        // ===================== 模式切换 =====================
        senderModeBtn.addEventListener('click', () => {
            senderModeBtn.classList.add('active');
            receiverModeBtn.classList.remove('active');
            senderPanel.classList.add('active');
            receiverPanel.classList.remove('active');
            showToast('切换到发送模式', 'info', 2000);
        });
        
        receiverModeBtn.addEventListener('click', () => {
            receiverModeBtn.classList.add('active');
            senderModeBtn.classList.remove('active');
            receiverPanel.classList.add('active');
            senderPanel.classList.remove('active');
            showToast('切换到接收模式', 'info', 2000);
        });
        
        // ===================== 文件选择处理 =====================
        fileSelectArea.addEventListener('click', (e) => {
            // 只在点击区域本身时触发，不触发子元素
            if (e.target === fileSelectArea || e.target.closest('.file-select-icon')) {
                fileInput.click();
            }
        });
        
        fileSelectArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            fileSelectArea.classList.add('active');
        });
        
        fileSelectArea.addEventListener('dragleave', (e) => {
            e.preventDefault();
            fileSelectArea.classList.remove('active');
        });
        
        fileSelectArea.addEventListener('drop', (e) => {
            e.preventDefault();
            fileSelectArea.classList.remove('active');
            
            if (e.dataTransfer.files.length > 0) {
                handleFileSelection(e.dataTransfer.files[0]);
            }
        });
        
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                handleFileSelection(e.target.files[0]);
            }
        });
        
        changeFileBtn.addEventListener('click', () => {
            fileInput.click();
        });
        
        function handleFileSelection(file) {
            if (!file) return;
            
            currentFile = file;
            fileNameEl.textContent = file.name;
            fileSizeEl.textContent = formatBytes(file.size);
            fileInfo.classList.add('active');
            generateBtn.disabled = false;
            
            showToast(`已选择文件: ${file.name}`, 'success', 3000);
        }
        
        // ===================== 发送端逻辑 =====================
        
        generateBtn.addEventListener('click', async () => {
            if (!currentFile) {
                showToast('请先选择要发送的文件', 'error', 3000);
                return;
            }
            
            // 生成取件码
            currentRoomCode = generateRoomCode();
            codeDisplay.textContent = currentRoomCode;
            
            // 更新UI状态
            generateBtn.disabled = true;
            generateBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 创建房间中...';
            
            try {
                // 1. 创建房间
                const createResponse = await fetch(`${API_BASE}/room/create`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        code: currentRoomCode
                    })
                });
                
                const createResult = await createResponse.json();
                
                if (!createResult.success) {
                    throw new Error(createResult.error || '创建房间失败');
                }
                
                // 2. 更新UI
                senderStatusArea.classList.add('active');
                senderStatusText.textContent = '房间创建成功，等待接收方连接...';
                generateBtn.innerHTML = '<i class="fas fa-check"></i> 房间已创建';
                
                // 3. 连接WebSocket
                await connectAsSender(createResult.wsUrl);
                
                showToast('房间创建成功！请将取件码告知接收方', 'success', 5000);
                
            } catch (error) {
                console.error('创建房间失败:', error);
                showToast(`创建失败: ${error.message}`, 'error', 5000);
                generateBtn.disabled = false;
                generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码并创建房间';
            }
        });
        
        async function connectAsSender(wsUrl) {
            const clientId = `sender_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            const url = new URL(wsUrl);
            url.searchParams.set('role', 'sender');
            url.searchParams.set('clientId', clientId);
            
            console.log('连接WebSocket:', url.toString());
            
            senderSocket = new WebSocket(url.toString());
            
            senderSocket.onopen = () => {
                console.log('发送方WebSocket连接已打开');
                senderStatusText.textContent = '已连接，等待接收方...';
                updateConnectionStatus(senderConnectionItem, true, '在线');
                
                // 启动心跳
                startHeartbeat();
                
                // 启动连接状态检查
                startConnectionCheck();
                
                showToast('WebSocket连接成功', 'success', 3000);
            };
            
            senderSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    handleSenderMessage(data);
                } catch (error) {
                    console.error('解析消息失败:', error);
                }
            };
            
            senderSocket.onclose = (event) => {
                console.log('发送方WebSocket连接关闭:', event.code, event.reason);
                senderStatusText.textContent = '连接已断开';
                updateConnectionStatus(senderConnectionItem, false, '离线');
                
                // 停止定时器
                stopHeartbeat();
                stopConnectionCheck();
                
                if (event.code !== 1000) {
                    showToast('连接已断开，请刷新页面重试', 'warning', 5000);
                }
            };
            
            senderSocket.onerror = (error) => {
                console.error('发送方WebSocket错误:', error);
                showToast('连接错误，请检查网络', 'error', 5000);
            };
        }
        
        function handleSenderMessage(data) {
            console.log('发送方收到消息:', data);
            
            switch (data.type) {
                case 'connected':
                    senderStatusText.textContent = '连接成功，等待接收方...';
                    break;
                    
                case 'receiver-connected':
                    senderStatusText.textContent = '接收方已连接！';
                    updateConnectionStatus(receiverConnectionItem, true, '在线');
                    startTransferBtn.disabled = false;
                    showToast('接收方已连接，可以开始传输', 'success', 3000);
                    break;
                    
                case 'receiver-disconnected':
                    senderStatusText.textContent = '接收方已断开，等待重新连接...';
                    updateConnectionStatus(receiverConnectionItem, false, '离线');
                    startTransferBtn.disabled = true;
                    showToast('接收方已断开', 'warning', 3000);
                    break;
                    
                case 'heartbeat-response':
                    // 心跳响应
                    break;
            }
        }
        
        // 开始传输文件
        startTransferBtn.addEventListener('click', () => {
            if (!currentFile || !senderSocket) {
                showToast('无法开始传输', 'error', 3000);
                return;
            }
            
            startTransferBtn.disabled = true;
            senderProgressContainer.classList.remove('hidden');
            senderStatusText.textContent = '正在发送文件...';
            
            // 发送文件元数据
            senderSocket.send(JSON.stringify({
                type: 'file-metadata',
                metadata: {
                    name: currentFile.name,
                    size: currentFile.size,
                    type: currentFile.type,
                    lastModified: currentFile.lastModified
                }
            }));
            
            // 分块发送文件
            const CHUNK_SIZE = 64 * 1024; // 64KB每块
            const totalChunks = Math.ceil(currentFile.size / CHUNK_SIZE);
            let currentChunk = 0;
            
            function sendNextChunk(start) {
                if (senderSocket.readyState !== WebSocket.OPEN) {
                    showToast('连接已断开，传输失败', 'error', 5000);
                    return;
                }
                
                if (currentChunk >= totalChunks) {
                    // 传输完成
                    senderSocket.send(JSON.stringify({
                        type: 'transfer-complete'
                    }));
                    
                    senderProgressPercent.textContent = '100%';
                    senderProgressFill.style.width = '100%';
                    senderProgressText.textContent = '传输完成';
                    senderStatusText.textContent = '文件发送完成！';
                    
                    showToast('文件传输完成！', 'success', 5000);
                    return;
                }
                
                const end = Math.min(start + CHUNK_SIZE, currentFile.size);
                const chunk = currentFile.slice(start, end);
                
                const reader = new FileReader();
                reader.onload = (event) => {
                    senderSocket.send(JSON.stringify({
                        type: 'file-chunk',
                        chunk: event.target.result,
                        index: currentChunk,
                        total: totalChunks
                    }));
                    
                    currentChunk++;
                    const progress = Math.round((currentChunk / totalChunks) * 100);
                    senderProgressPercent.textContent = `${progress}%`;
                    senderProgressFill.style.width = `${progress}%`;
                    senderProgressText.textContent = `发送中: ${formatBytes(start + CHUNK_SIZE)} / ${formatBytes(currentFile.size)}`;
                    
                    // 继续发送下一块
                    setTimeout(() => sendNextChunk(end), 0);
                };
                
                reader.readAsArrayBuffer(chunk);
            }
            
            sendNextChunk(0);
        });
        
        // 心跳机制
        function startHeartbeat() {
            heartbeatTimer = setInterval(() => {
                if (senderSocket && senderSocket.readyState === WebSocket.OPEN) {
                    senderSocket.send(JSON.stringify({
                        type: 'heartbeat',
                        timestamp: Date.now()
                    }));
                }
            }, HEARTBEAT_INTERVAL);
        }
        
        function stopHeartbeat() {
            if (heartbeatTimer) {
                clearInterval(heartbeatTimer);
                heartbeatTimer = null;
            }
        }
        
        // 连接状态检查
        function startConnectionCheck() {
            connectionCheckTimer = setInterval(() => {
                if (senderSocket && senderSocket.readyState === WebSocket.OPEN) {
                    senderSocket.send(JSON.stringify({
                        type: 'sender-status',
                        connected: true,
                        timestamp: Date.now(),
                        message: '发送方在线'
                    }));
                }
            }, CONNECTION_CHECK_INTERVAL);
        }
        
        function stopConnectionCheck() {
            if (connectionCheckTimer) {
                clearInterval(connectionCheckTimer);
                connectionCheckTimer = null;
            }
        }
        
        // 复制取件码
        copyCodeBtn.addEventListener('click', () => {
            navigator.clipboard.writeText(currentRoomCode)
                .then(() => {
                    copyCodeBtn.innerHTML = '<i class="fas fa-check"></i> 已复制';
                    showToast('取件码已复制到剪贴板', 'success', 3000);
                    setTimeout(() => {
                        copyCodeBtn.innerHTML = '<i class="fas fa-copy"></i> 复制取件码';
                    }, 2000);
                })
                .catch(() => {
                    showToast('复制失败，请手动复制', 'error', 3000);
                });
        });
        
        // 取消传输
        cancelBtn.addEventListener('click', () => {
            if (senderSocket) {
                senderSocket.close(1000, '用户取消');
            }
            resetSender();
            showToast('已取消传输', 'info', 3000);
        });
        
        function resetSender() {
            senderStatusArea.classList.remove('active');
            fileInfo.classList.remove('active');
            fileInput.value = '';
            currentFile = null;
            currentRoomCode = null;
            generateBtn.disabled = true;
            generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码并创建房间';
            startTransferBtn.disabled = true;
            senderProgressContainer.classList.add('hidden');
            senderProgressFill.style.width = '0%';
            senderProgressPercent.textContent = '0%';
            updateConnectionStatus(senderConnectionItem, false, '离线');
            updateConnectionStatus(receiverConnectionItem, false, '离线');
        }
        
        // ===================== 接收端逻辑 =====================
        
        // 取件码输入处理
        codeInput.addEventListener('input', function() {
            this.value = this.value.toUpperCase().replace(/[^A-Z0-9]/g, '').substring(0, 6);
        });
        
        codeInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                connectBtn.click();
            }
        });
        
        connectBtn.addEventListener('click', async () => {
            const code = codeInput.value.trim();
            
            if (code.length !== 6) {
                showToast('请输入6位取件码', 'error', 3000);
                return;
            }
            
            currentRoomCode = code;
            
            // 更新UI状态
            connectBtn.disabled = true;
            connectBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 连接中...';
            receiverStatusArea.classList.add('active');
            receiverStatusText.textContent = '正在连接房间...';
            
            try {
                // 1. 查询房间
                const roomResponse = await fetch(`${API_BASE}/room/${code}`);
                const roomResult = await roomResponse.json();
                
                if (!roomResult.success) {
                    throw new Error(roomResult.error || '房间不存在');
                }
                
                // 2. 更新UI
                receiverStatusText.textContent = '房间连接成功，等待发送方...';
                updateConnectionStatus(selfStatus, true, '已连接');
                
                // 3. 连接WebSocket
                await connectAsReceiver(roomResult.wsUrl);
                
                showToast('房间连接成功！', 'success', 3000);
                
            } catch (error) {
                console.error('连接房间失败:', error);
                showToast(`连接失败: ${error.message}`, 'error', 5000);
                receiverStatusArea.classList.remove('active');
                connectBtn.disabled = false;
                connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
            }
        });
        
        async function connectAsReceiver(wsUrl) {
            const clientId = `receiver_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            const url = new URL(wsUrl);
            url.searchParams.set('role', 'receiver');
            url.searchParams.set('clientId', clientId);
            
            console.log('连接WebSocket:', url.toString());
            
            receiverSocket = new WebSocket(url.toString());
            
            receiverSocket.onopen = () => {
                console.log('接收方WebSocket连接已打开');
                receiverStatusText.textContent = '已连接，等待文件...';
                connectionStatusAlert.innerHTML = '<i class="fas fa-sync fa-spin"></i> 等待发送方连接...';
                connectionStatusAlert.className = 'alert alert-info';
                
                // 开始监控发送方状态
                startSenderStatusMonitoring();
            };
            
            receiverSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    handleReceiverMessage(data);
                } catch (error) {
                    console.error('解析消息失败:', error);
                }
            };
            
            receiverSocket.onclose = (event) => {
                console.log('接收方WebSocket连接关闭:', event.code, event.reason);
                receiverStatusText.textContent = '连接已断开';
                updateConnectionStatus(selfStatus, false, '离线');
                
                if (event.code !== 1000) {
                    showToast('连接已断开', 'warning', 5000);
                }
            };
            
            receiverSocket.onerror = (error) => {
                console.error('接收方WebSocket错误:', error);
                showToast('连接错误', 'error', 5000);
            };
        }
        
        function handleReceiverMessage(data) {
            console.log('接收方收到消息:', data);
            
            switch (data.type) {
                case 'connected':
                    receiverStatusText.textContent = '连接成功，等待文件...';
                    break;
                    
                case 'sender-connected':
                    receiverStatusText.textContent = '发送方已连接！';
                    updateConnectionStatus(remoteSenderStatus, true, '在线');
                    connectionStatusAlert.innerHTML = '<i class="fas fa-check-circle"></i> 发送方已连接，等待文件...';
                    connectionStatusAlert.className = 'alert alert-success';
                    showToast('发送方已连接', 'success', 3000);
                    break;
                    
                case 'sender-status':
                    updateConnectionStatus(remoteSenderStatus, data.connected, data.connected ? '在线' : '离线');
                    break;
                    
                case 'sender-disconnected':
                    receiverStatusText.textContent = '发送方已断开，等待重新连接...';
                    updateConnectionStatus(remoteSenderStatus, false, '离线');
                    connectionStatusAlert.innerHTML = '<i class="fas fa-exclamation-triangle"></i> 发送方已断开连接';
                    connectionStatusAlert.className = 'alert alert-warning';
                    showToast('发送方已断开', 'warning', 3000);
                    break;
                    
                case 'file-metadata':
                    fileMetadata = data.metadata;
                    fileName = data.metadata.name;
                    fileSize = data.metadata.size;
                    totalChunks = Math.ceil(fileSize / (64 * 1024));
                    receivedChunks = new Array(totalChunks);
                    receivedSize = 0;
                    
                    receiverStatusText.textContent = `准备接收: ${fileName}`;
                    connectionStatusAlert.innerHTML = `<i class="fas fa-file"></i> 准备接收: ${fileName} (${formatBytes(fileSize)})`;
                    connectionStatusAlert.className = 'alert alert-info';
                    
                    showToast(`开始接收文件: ${fileName}`, 'info', 3000);
                    break;
                    
                case 'file-chunk':
                    const chunkIndex = data.index;
                    const chunkData = data.chunk;
                    
                    // 存储数据块
                    receivedChunks[chunkIndex] = chunkData;
                    receivedSize += chunkData.byteLength;
                    currentChunk = chunkIndex + 1;
                    
                    // 更新进度
                    const percent = Math.round((receivedSize / fileSize) * 100);
                    receiverProgressFill.style.width = `${percent}%`;
                    receiverProgressPercent.textContent = `${percent}%`;
                    receiverProgressText.textContent = `接收中: ${percent}% (${formatBytes(receivedSize)} / ${formatBytes(fileSize)})`;
                    
                    connectionStatusAlert.innerHTML = `<i class="fas fa-download"></i> 接收中: ${percent}%`;
                    break;
                    
                case 'transfer-complete':
                    // 合并文件
                    const blob = new Blob(receivedChunks);
                    const url = URL.createObjectURL(blob);
                    
                    // 更新UI
                    receiverProgressFill.style.width = '100%';
                    receiverProgressPercent.textContent = '100%';
                    receiverProgressText.textContent = `接收完成: ${formatBytes(fileSize)}`;
                    receiverProgressFill.style.background = 'linear-gradient(90deg, #10b981, #059669)';
                    
                    receiverStatusText.textContent = '文件接收完成！';
                    connectionStatusAlert.innerHTML = '<i class="fas fa-check-circle"></i> 文件接收完成！';
                    connectionStatusAlert.className = 'alert alert-success';
                    
                    // 显示下载按钮
                    downloadBtn.href = url;
                    downloadBtn.download = fileName;
                    downloadBtn.textContent = `下载 ${fileName}`;
                    downloadSection.classList.remove('hidden');
                    
                    showToast('文件接收完成！点击下载按钮保存', 'success', 5000);
                    break;
            }
        }
        
        // 监控发送方状态
        function startSenderStatusMonitoring() {
            let lastStatusTime = Date.now();
            
            const checkInterval = setInterval(() => {
                const now = Date.now();
                if (now - lastStatusTime > 10000) {
                    // 超过10秒没有状态更新，认为发送方离线
                    updateConnectionStatus(remoteSenderStatus, false, '离线');
                    connectionStatusAlert.innerHTML = '<i class="fas fa-exclamation-triangle"></i> 发送方可能已断开';
                    connectionStatusAlert.className = 'alert alert-warning';
                }
            }, 5000);
            
            // 监听消息更新状态时间
            const originalMessageHandler = receiverSocket.onmessage;
            receiverSocket.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    if (data.type === 'sender-status' || data.type === 'sender-connected') {
                        lastStatusTime = Date.now();
                    }
                } catch (error) {
                    console.error('解析消息失败:', error);
                }
                
                // 调用原始处理器
                if (originalMessageHandler) {
                    originalMessageHandler.call(this, event);
                }
            };
            
            // 清理定时器
            receiverSocket.addEventListener('close', () => {
                clearInterval(checkInterval);
            });
        }
        
        // 断开连接
        disconnectBtn.addEventListener('click', () => {
            if (receiverSocket) {
                receiverSocket.close(1000, '用户断开');
            }
            resetReceiver();
            showToast('已断开连接', 'info', 3000);
        });
        
        function resetReceiver() {
            receiverStatusArea.classList.remove('active');
            codeInput.value = '';
            downloadSection.classList.add('hidden');
            receiverProgressFill.style.width = '0%';
            receiverProgressPercent.textContent = '0%';
            receiverProgressText.textContent = '等待文件传输...';
            receiverProgressFill.style.background = 'linear-gradient(90deg, var(--primary), var(--secondary))';
            connectBtn.disabled = false;
            connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
            updateConnectionStatus(remoteSenderStatus, false, '离线');
            updateConnectionStatus(selfStatus, false, '离线');
        }
        
        // ===================== 初始化 =====================
        
        // 健康检查
        async function checkServiceHealth() {
            try {
                const response = await fetch(`${API_BASE}/health`);
                const data = await response.json();
                updateServiceStatus('正常');
                debugInfo.innerHTML = `服务状态: <span id="serviceStatus">正常</span> | 房间数: ${data.roomCount || 0}`;
            } catch (error) {
                console.error('健康检查失败:', error);
                updateServiceStatus('异常');
            }
        }
        
        // 页面加载
        window.addEventListener('load', () => {
            console.log('即时传输页面已加载');
            
            // 初始健康检查
            checkServiceHealth();
            
            // 每隔30秒检查一次服务状态
            setInterval(checkServiceHealth, 30000);
            
            // 页面关闭警告
            window.addEventListener('beforeunload', (e) => {
                if (senderSocket || receiverSocket) {
                    e.preventDefault();
                    e.returnValue = '文件传输正在进行中，确定要离开吗？';
                    return e.returnValue;
                }
            });
        });
        
        // 导出全局变量用于调试
        window.appState = {
            currentRoomCode,
            currentFile,
            rooms: () => rooms.size,
            resetSender,
            resetReceiver
        };
        
        console.log('即时传输前端已初始化完成');
    </script>
</body>
</html>`;
}