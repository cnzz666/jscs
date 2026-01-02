// ===================== 即时传输 Worker =====================
// 简化的内存存储版本

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
        const code = data.code;
        
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
          lastHeartbeat: Date.now()
        };
        
        rooms.set(code, room);
        
        // 30分钟后自动清理
        ctx.waitUntil(setTimeout(() => {
          if (rooms.has(code) && Date.now() - rooms.get(code).createdAt > 30 * 60 * 1000) {
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
          message: '房间创建成功'
        });
        
      } catch (error) {
        return Response.json({ 
          success: false, 
          error: '服务器错误' 
        }, { status: 500 });
      }
    }
    
    // 查询房间
    if (pathname.startsWith('/api/room/') && request.method === 'GET') {
      const code = pathname.split('/').pop();
      
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
        wsUrl: `${url.protocol === 'https:' ? 'wss:' : 'ws:'}//${url.host}/api/ws/${code}`
      });
    }
    
    // WebSocket 连接
    if (pathname.startsWith('/api/ws/')) {
      const code = pathname.split('/').pop();
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
      if (role === 'sender') {
        room.sender = server;
        room.lastHeartbeat = Date.now();
      } else {
        room.receiver = server;
      }
      
      // 更新房间状态
      if (room.sender && room.receiver) {
        room.status = 'active';
      } else if (room.sender) {
        room.status = 'waiting_receiver';
      } else if (room.receiver) {
        room.status = 'waiting_sender';
      }
      
      // 发送连接成功的消息
      server.send(JSON.stringify({
        type: 'connected',
        success: true,
        role: role,
        timestamp: Date.now(),
        message: '连接成功'
      }));
      
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
      server.addEventListener('message', (event) => {
        try {
          const data = JSON.parse(event.data);
          handleWebSocketMessage(data, server, role, room);
        } catch (error) {
          console.error('WebSocket消息处理错误:', error);
        }
      });
      
      // 设置连接关闭处理
      server.addEventListener('close', () => {
        // 清理连接
        if (role === 'sender') {
          room.sender = null;
          
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
        console.error('WebSocket错误:', error);
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
    
    // 404 处理
    return new Response('Not Found', { status: 404 });
  }
};

// 处理 WebSocket 消息
function handleWebSocketMessage(data, socket, role, room) {
  const timestamp = Date.now();
  
  switch (data.type) {
    case 'heartbeat':
      room.lastHeartbeat = timestamp;
      socket.send(JSON.stringify({
        type: 'heartbeat-response',
        timestamp,
        receivedAt: room.lastHeartbeat
      }));
      break;
      
    case 'file-metadata':
      room.fileInfo = data.metadata;
      
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
      if (room.sender) {
        room.sender.close(1001, '心跳超时');
        room.sender = null;
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

// HTML 页面内容（使用简单的字符串，避免模板字符串问题）
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
            font-family: Arial, sans-serif;
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
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.1);
            text-align: center;
        }
        
        .logo {
            font-size: 3rem;
            color: #3b82f6;
            margin-bottom: 15px;
        }
        
        h1 {
            font-size: 2rem;
            color: #1e293b;
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: #64748b;
            margin-bottom: 30px;
        }
        
        .mode-selector {
            display: flex;
            background: #f1f5f9;
            border-radius: 12px;
            padding: 5px;
            margin-bottom: 30px;
        }
        
        .mode-btn {
            flex: 1;
            padding: 15px;
            border: none;
            background: transparent;
            border-radius: 8px;
            font-size: 1rem;
            font-weight: 600;
            color: #64748b;
            cursor: pointer;
            transition: all 0.3s;
        }
        
        .mode-btn.active {
            background: white;
            color: #3b82f6;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
        }
        
        .mode-btn i {
            margin-right: 8px;
        }
        
        .panel {
            display: none;
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
            margin-bottom: 8px;
            color: #1e293b;
            font-weight: 600;
        }
        
        .file-upload {
            border: 2px dashed #cbd5e1;
            border-radius: 12px;
            padding: 40px 20px;
            text-align: center;
            background: #f8fafc;
            cursor: pointer;
            transition: all 0.3s;
            position: relative;
        }
        
        .file-upload:hover {
            border-color: #3b82f6;
            background: #f0f9ff;
        }
        
        .file-upload i {
            font-size: 2.5rem;
            color: #3b82f6;
            margin-bottom: 10px;
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
            background: #f0f9ff;
            border-radius: 12px;
            padding: 15px;
            margin-top: 15px;
            border-left: 4px solid #3b82f6;
            display: none;
        }
        
        .code-input {
            width: 100%;
            padding: 15px;
            border: 2px solid #e2e8f0;
            border-radius: 12px;
            font-size: 1.2rem;
            text-align: center;
            letter-spacing: 5px;
            font-family: 'Courier New', monospace;
        }
        
        .code-input:focus {
            outline: none;
            border-color: #3b82f6;
        }
        
        .btn {
            width: 100%;
            padding: 16px;
            border: none;
            border-radius: 12px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
        }
        
        .btn-primary {
            background: #3b82f6;
            color: white;
        }
        
        .btn-primary:hover:not(:disabled) {
            background: #2563eb;
            transform: translateY(-2px);
            box-shadow: 0 10px 20px rgba(59, 130, 246, 0.3);
        }
        
        .btn-success {
            background: #10b981;
            color: white;
        }
        
        .btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }
        
        .status-area {
            margin-top: 30px;
            padding: 25px;
            background: #f8fafc;
            border-radius: 16px;
            display: none;
        }
        
        .status-area.active {
            display: block;
        }
        
        .alert {
            padding: 15px;
            border-radius: 12px;
            margin-bottom: 20px;
        }
        
        .alert-warning {
            background: #fffbeb;
            border: 2px solid #fde68a;
            color: #92400e;
        }
        
        .alert-info {
            background: #f0f9ff;
            border: 2px solid #bae6fd;
            color: #0369a1;
        }
        
        .code-display {
            font-size: 2.5rem;
            font-weight: 800;
            letter-spacing: 10px;
            color: #3b82f6;
            margin: 20px 0;
            font-family: 'Courier New', monospace;
        }
        
        .progress-bar {
            height: 10px;
            background: #e2e8f0;
            border-radius: 5px;
            margin: 15px 0;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background: #3b82f6;
            width: 0%;
            transition: width 0.3s;
        }
        
        .connection-status {
            display: flex;
            gap: 15px;
            margin: 20px 0;
        }
        
        .status-item {
            flex: 1;
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            border: 2px solid #e2e8f0;
        }
        
        .status-item.active {
            border-color: #10b981;
            background: rgba(16, 185, 129, 0.1);
        }
        
        .status-item.inactive {
            border-color: #ef4444;
            background: rgba(239, 68, 68, 0.1);
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
        }
        
        @media (max-width: 480px) {
            .app-card {
                padding: 20px;
            }
            
            .code-display {
                font-size: 2rem;
                letter-spacing: 8px;
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
            <p class="subtitle">快速安全的文件传输工具</p>
            
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
                    <label>选择文件</label>
                    <div class="file-upload" id="fileUpload">
                        <i class="fas fa-cloud-upload-alt"></i>
                        <div>点击或拖放文件</div>
                        <input type="file" id="fileInput">
                    </div>
                    <div class="file-info" id="fileInfo">
                        <div id="fileName">未选择文件</div>
                        <div id="fileSize">0 KB</div>
                    </div>
                </div>
                
                <button class="btn btn-primary" id="generateBtn" disabled>
                    <i class="fas fa-barcode"></i>
                    生成取件码
                </button>
                
                <div class="status-area" id="senderStatusArea">
                    <div class="alert alert-warning">
                        <i class="fas fa-exclamation-triangle"></i>
                        请不要关闭页面或刷新
                    </div>
                    
                    <div class="code-display" id="codeDisplay">ABCDEF</div>
                    
                    <div class="connection-status">
                        <div class="status-item active" id="senderStatusItem">
                            发送方：<span>在线</span>
                        </div>
                        <div class="status-item inactive" id="receiverStatusItem">
                            接收方：<span>等待连接</span>
                        </div>
                    </div>
                    
                    <div class="progress-bar hidden" id="senderProgress">
                        <div class="progress-fill" id="senderProgressFill"></div>
                    </div>
                    
                    <button class="btn" id="cancelBtn">
                        <i class="fas fa-times"></i>
                        取消
                    </button>
                </div>
            </div>
            
            <!-- 接收端面板 -->
            <div class="panel" id="receiverPanel">
                <div class="form-group">
                    <label>输入取件码</label>
                    <input type="text" class="code-input" id="codeInput" placeholder="ABCDEF" maxlength="6">
                </div>
                
                <button class="btn btn-success" id="connectBtn">
                    <i class="fas fa-plug"></i>
                    连接房间
                </button>
                
                <div class="status-area" id="receiverStatusArea">
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle"></i>
                        正在连接到发送方...
                    </div>
                    
                    <div class="connection-status">
                        <div class="status-item inactive" id="remoteSenderStatus">
                            发送方：<span>离线</span>
                        </div>
                        <div class="status-item active" id="selfStatus">
                            接收方：<span>连接中</span>
                        </div>
                    </div>
                    
                    <div class="progress-bar">
                        <div class="progress-fill" id="receiverProgressFill"></div>
                    </div>
                    
                    <a class="btn btn-success hidden" id="downloadBtn" download>
                        <i class="fas fa-download"></i>
                        下载文件
                    </a>
                    
                    <button class="btn" id="disconnectBtn">
                        <i class="fas fa-times"></i>
                        断开连接
                    </button>
                </div>
            </div>
            
            <div class="instructions">
                <p><strong>使用说明：</strong></p>
                <p>1. 发送方生成取件码并告知接收方</p>
                <p>2. 传输期间请勿关闭页面</p>
                <p>3. 文件直接传输，服务器不存储</p>
            </div>
        </div>
    </div>

    <script>
        // ===================== 配置 =====================
        const API_BASE = '/api';
        
        // ===================== 状态变量 =====================
        let currentRoomCode = null;
        let currentFile = null;
        let senderSocket = null;
        let receiverSocket = null;
        
        // ===================== DOM 元素 =====================
        const senderModeBtn = document.getElementById('senderModeBtn');
        const receiverModeBtn = document.getElementById('receiverModeBtn');
        const senderPanel = document.getElementById('senderPanel');
        const receiverPanel = document.getElementById('receiverPanel');
        
        // 发送端元素
        const fileInput = document.getElementById('fileInput');
        const fileInfo = document.getElementById('fileInfo');
        const fileName = document.getElementById('fileName');
        const fileSize = document.getElementById('fileSize');
        const generateBtn = document.getElementById('generateBtn');
        const senderStatusArea = document.getElementById('senderStatusArea');
        const codeDisplay = document.getElementById('codeDisplay');
        const senderProgress = document.getElementById('senderProgress');
        const senderProgressFill = document.getElementById('senderProgressFill');
        const cancelBtn = document.getElementById('cancelBtn');
        
        // 接收端元素
        const codeInput = document.getElementById('codeInput');
        const connectBtn = document.getElementById('connectBtn');
        const receiverStatusArea = document.getElementById('receiverStatusArea');
        const receiverProgressFill = document.getElementById('receiverProgressFill');
        const downloadBtn = document.getElementById('downloadBtn');
        const disconnectBtn = document.getElementById('disconnectBtn');
        
        // ===================== 工具函数 =====================
        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
        
        function showMessage(message, type) {
            // 创建临时提示
            const alert = document.createElement('div');
            alert.className = 'alert alert-' + type;
            alert.innerHTML = '<i class="fas fa-info-circle"></i> ' + message;
            
            document.querySelector('.app-card').insertBefore(alert, document.querySelector('.instructions'));
            
            setTimeout(() => {
                alert.style.opacity = '0';
                setTimeout(() => alert.remove(), 300);
            }, 3000);
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
        }
        
        // ===================== 模式切换 =====================
        senderModeBtn.addEventListener('click', () => {
            senderModeBtn.classList.add('active');
            receiverModeBtn.classList.remove('active');
            senderPanel.classList.add('active');
            receiverPanel.classList.remove('active');
        });
        
        receiverModeBtn.addEventListener('click', () => {
            receiverModeBtn.classList.add('active');
            senderModeBtn.classList.remove('active');
            receiverPanel.classList.add('active');
            senderPanel.classList.remove('active');
        });
        
        // ===================== 文件选择 =====================
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                currentFile = e.target.files[0];
                fileName.textContent = currentFile.name;
                fileSize.textContent = formatBytes(currentFile.size);
                fileInfo.style.display = 'block';
                generateBtn.disabled = false;
            }
        });
        
        // 发送文件
        generateBtn.addEventListener('click', async () => {
            if (!currentFile) {
                showMessage('请先选择文件', 'warning');
                return;
            }
            
            generateBtn.disabled = true;
            generateBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 创建中...';
            
            currentRoomCode = generateRoomCode();
            
            try {
                const response = await fetch(API_BASE + '/room/create', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ code: currentRoomCode })
                });
                
                const result = await response.json();
                
                if (!result.success) {
                    throw new Error(result.error);
                }
                
                codeDisplay.textContent = currentRoomCode;
                senderStatusArea.classList.add('active');
                
                // 连接 WebSocket
                connectAsSender(result.wsUrl);
                
                showMessage('房间创建成功！', 'info');
                
            } catch (error) {
                showMessage('创建失败: ' + error.message, 'warning');
                generateBtn.disabled = false;
                generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
            }
        });
        
        function connectAsSender(wsUrl) {
            senderSocket = new WebSocket(wsUrl + '?role=sender');
            
            senderSocket.onopen = () => {
                console.log('发送方已连接');
                
                // 开始心跳
                setInterval(() => {
                    if (senderSocket.readyState === WebSocket.OPEN) {
                        senderSocket.send(JSON.stringify({
                            type: 'heartbeat',
                            timestamp: Date.now()
                        }));
                        
                        // 发送状态
                        senderSocket.send(JSON.stringify({
                            type: 'sender-status',
                            connected: true,
                            timestamp: Date.now()
                        }));
                    }
                }, 2000);
            };
            
            senderSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    
                    if (data.type === 'receiver-connected') {
                        // 开始发送文件
                        sendFile();
                    }
                } catch (error) {
                    console.error('解析消息错误:', error);
                }
            };
            
            senderSocket.onclose = () => {
                console.log('发送方连接关闭');
            };
        }
        
        function sendFile() {
            if (!currentFile) return;
            
            senderProgress.classList.remove('hidden');
            
            // 发送文件信息
            senderSocket.send(JSON.stringify({
                type: 'file-metadata',
                metadata: {
                    name: currentFile.name,
                    size: currentFile.size,
                    type: currentFile.type
                }
            }));
            
            // 分块发送
            const CHUNK_SIZE = 64 * 1024;
            const reader = new FileReader();
            let offset = 0;
            let chunkIndex = 0;
            const totalChunks = Math.ceil(currentFile.size / CHUNK_SIZE);
            
            function readNext() {
                const slice = currentFile.slice(offset, offset + CHUNK_SIZE);
                reader.readAsArrayBuffer(slice);
            }
            
            reader.onload = (e) => {
                senderSocket.send(JSON.stringify({
                    type: 'file-chunk',
                    chunk: e.target.result,
                    index: chunkIndex,
                    total: totalChunks
                }));
                
                offset += e.target.result.byteLength;
                chunkIndex++;
                
                // 更新进度
                const progress = Math.round((offset / currentFile.size) * 100);
                senderProgressFill.style.width = progress + '%';
                
                if (offset < currentFile.size) {
                    setTimeout(readNext, 0);
                } else {
                    senderSocket.send(JSON.stringify({ type: 'transfer-complete' }));
                }
            };
            
            readNext();
        }
        
        // 接收文件
        codeInput.addEventListener('input', function() {
            this.value = this.value.toUpperCase();
        });
        
        connectBtn.addEventListener('click', async () => {
            const code = codeInput.value.trim();
            
            if (code.length !== 6) {
                showMessage('请输入6位取件码', 'warning');
                return;
            }
            
            connectBtn.disabled = true;
            connectBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 连接中...';
            
            try {
                const response = await fetch(API_BASE + '/room/' + code);
                const result = await response.json();
                
                if (!result.success) {
                    throw new Error(result.error);
                }
                
                // 连接 WebSocket
                connectAsReceiver(result.wsUrl);
                
                receiverStatusArea.classList.add('active');
                showMessage('连接成功！', 'info');
                
            } catch (error) {
                showMessage('连接失败: ' + error.message, 'warning');
                connectBtn.disabled = false;
                connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
            }
        });
        
        function connectAsReceiver(wsUrl) {
            receiverSocket = new WebSocket(wsUrl + '?role=receiver');
            let receivedChunks = [];
            let fileSize = 0;
            
            receiverSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    
                    switch (data.type) {
                        case 'file-metadata':
                            fileSize = data.metadata.size;
                            receivedChunks = [];
                            break;
                            
                        case 'file-chunk':
                            receivedChunks[data.index] = data.chunk;
                            const received = receivedChunks.filter(Boolean).reduce((a, b) => a + b.byteLength, 0);
                            const progress = Math.round((received / fileSize) * 100);
                            receiverProgressFill.style.width = progress + '%';
                            break;
                            
                        case 'transfer-complete':
                            // 合并文件
                            const blob = new Blob(receivedChunks);
                            const url = URL.createObjectURL(blob);
                            downloadBtn.href = url;
                            downloadBtn.download = 'received_file';
                            downloadBtn.classList.remove('hidden');
                            showMessage('文件接收完成！', 'info');
                            break;
                    }
                } catch (error) {
                    console.error('消息处理错误:', error);
                }
            };
            
            receiverSocket.onclose = () => {
                console.log('接收方连接关闭');
            };
        }
        
        // 取消操作
        cancelBtn.addEventListener('click', () => {
            if (senderSocket) senderSocket.close();
            senderStatusArea.classList.remove('active');
            generateBtn.disabled = false;
            generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
        });
        
        disconnectBtn.addEventListener('click', () => {
            if (receiverSocket) receiverSocket.close();
            receiverStatusArea.classList.remove('active');
            connectBtn.disabled = false;
            connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
        });
        
        // 页面关闭警告
        window.addEventListener('beforeunload', (e) => {
            if (senderSocket || receiverSocket) {
                e.preventDefault();
                e.returnValue = '文件传输中，确定离开？';
            }
        });
        
        // 健康检查
        fetch(API_BASE + '/health')
            .then(res => res.json())
            .then(data => console.log('服务状态:', data.status))
            .catch(err => console.warn('健康检查失败:', err));
    </script>
</body>
</html>`;