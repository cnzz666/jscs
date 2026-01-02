// 即时传输 Worker - 完整后端实现
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
      return new Response(getHTML(), {
        headers: {
          'Content-Type': 'text/html;charset=UTF-8',
          ...corsHeaders
        }
      });
    }
    
    // API 路由
    if (pathname.startsWith('/api/')) {
      return handleAPI(request, env, ctx);
    }
    
    // 404
    return new Response('Not Found', { status: 404 });
  },
};

// API 处理器
async function handleAPI(request, env, ctx) {
  const url = new URL(request.url);
  const pathname = url.pathname;
  
  // 创建房间
  if (pathname === '/api/create' && request.method === 'POST') {
    try {
      const { code } = await request.json();
      
      if (!code || code.length !== 6) {
        return Response.json({ error: '取件码必须是6位字符' }, { status: 400 });
      }
      
      // 生成房间ID
      const roomId = env.ROOMS.idFromName(code);
      const room = env.ROOMS.get(roomId);
      
      // 初始化房间
      const initResponse = await room.fetch('http://internal/init', {
        method: 'POST',
        body: JSON.stringify({ code, createdAt: Date.now() })
      });
      
      if (!initResponse.ok) {
        return Response.json({ error: '房间已存在' }, { status: 409 });
      }
      
      return Response.json({ 
        success: true, 
        code,
        wsUrl: `${new URL(request.url).origin.replace('http', 'ws')}/api/ws/${code}`,
        message: '房间创建成功'
      });
      
    } catch (error) {
      return Response.json({ error: '创建失败: ' + error.message }, { status: 500 });
    }
  }
  
  // 查询房间
  if (pathname.startsWith('/api/room/') && request.method === 'GET') {
    const code = pathname.split('/').pop();
    
    if (!code || code.length !== 6) {
      return Response.json({ error: '无效的取件码' }, { status: 400 });
    }
    
    try {
      const roomId = env.ROOMS.idFromName(code);
      const room = env.ROOMS.get(roomId);
      
      const infoResponse = await room.fetch('http://internal/info');
      
      if (!infoResponse.ok) {
        return Response.json({ exists: false }, { status: 404 });
      }
      
      const info = await infoResponse.json();
      return Response.json({
        exists: true,
        code: info.code,
        status: info.status,
        createdAt: info.createdAt,
        connections: info.connections,
        wsUrl: `${new URL(request.url).origin.replace('http', 'ws')}/api/ws/${code}`
      });
      
    } catch (error) {
      return Response.json({ exists: false, error: error.message }, { status: 404 });
    }
  }
  
  // WebSocket 连接
  if (pathname.startsWith('/api/ws/')) {
    const code = pathname.split('/').pop();
    const role = url.searchParams.get('role');
    
    if (!code || !role || !['sender', 'receiver'].includes(role)) {
      return new Response('Invalid request', { status: 400 });
    }
    
    // 获取房间
    const roomId = env.ROOMS.idFromName(code);
    const room = env.ROOMS.get(roomId);
    
    // 转发 WebSocket 请求
    return room.fetch(request);
  }
  
  // 健康检查
  if (pathname === '/api/health') {
    return Response.json({ 
      status: 'ok', 
      timestamp: Date.now(),
      service: 'Instant Transfer'
    });
  }
  
  return new Response('Not Found', { status: 404 });
}

// Durable Object 类 - 房间管理
export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.storage = state.storage;
    
    // 房间状态
    this.code = null;
    this.status = 'waiting'; // waiting, active, closed
    this.createdAt = null;
    
    // WebSocket 连接
    this.sender = null;
    this.receiver = null;
    
    // 心跳和状态
    this.lastHeartbeat = null;
    this.heartbeatInterval = null;
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    
    // WebSocket 连接
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleWebSocket(request);
    }
    
    // 内部 API
    if (url.pathname === '/internal/init') {
      return this.handleInit(request);
    } else if (url.pathname === '/internal/info') {
      return this.handleInfo();
    }
    
    return new Response('Not found', { status: 404 });
  }
  
  // 初始化房间
  async handleInit(request) {
    const existing = await this.storage.get('status');
    if (existing) {
      return new Response('Room already exists', { status: 409 });
    }
    
    const { code, createdAt } = await request.json();
    this.code = code;
    this.createdAt = createdAt;
    
    await this.storage.put('code', code);
    await this.storage.put('status', 'waiting');
    await this.storage.put('createdAt', createdAt);
    
    // 设置15分钟后自动清理
    await this.storage.setAlarm(Date.now() + 15 * 60 * 1000);
    
    return new Response('OK');
  }
  
  // 获取房间信息
  async handleInfo() {
    const code = await this.storage.get('code');
    const status = await this.storage.get('status');
    const createdAt = await this.storage.get('createdAt');
    
    if (!code) {
      return new Response('Room not found', { status: 404 });
    }
    
    const connections = {
      sender: !!this.sender,
      receiver: !!this.receiver
    };
    
    return Response.json({
      code,
      status: status || 'unknown',
      createdAt: parseInt(createdAt) || Date.now(),
      connections
    });
  }
  
  // 处理 WebSocket 连接
  async handleWebSocket(request) {
    const url = new URL(request.url);
    const role = url.searchParams.get('role');
    const clientId = url.searchParams.get('clientId') || `client_${Date.now()}`;
    
    // 创建 WebSocket 对
    const { 0: client, 1: server } = new WebSocketPair();
    
    server.accept();
    
    // 设置连接
    if (role === 'sender') {
      this.sender = server;
      this.sender.id = clientId;
      console.log(`[${this.code}] 发送方连接: ${clientId}`);
    } else if (role === 'receiver') {
      this.receiver = server;
      this.receiver.id = clientId;
      console.log(`[${this.code}] 接收方连接: ${clientId}`);
    } else {
      server.close(1008, 'Invalid role');
      return new Response(null, { status: 400 });
    }
    
    // 更新房间状态
    this.updateStatus();
    
    // 设置消息处理器
    server.addEventListener('message', (event) => {
      this.handleMessage(event.data, server, role);
    });
    
    // 连接关闭
    server.addEventListener('close', () => {
      console.log(`[${this.code}] ${role} 断开连接`);
      if (role === 'sender') {
        this.sender = null;
        // 通知接收方
        if (this.receiver) {
          this.receiver.send(JSON.stringify({
            type: 'sender-status',
            connected: false,
            timestamp: Date.now(),
            message: '发送方已断开'
          }));
        }
      } else if (role === 'receiver') {
        this.receiver = null;
        // 通知发送方
        if (this.sender) {
          this.sender.send(JSON.stringify({
            type: 'receiver-status',
            connected: false,
            timestamp: Date.now(),
            message: '接收方已断开'
          }));
        }
      }
      this.updateStatus();
    });
    
    server.addEventListener('error', (error) => {
      console.error(`[${this.code}] WebSocket error:`, error);
    });
    
    // 发送连接成功消息
    server.send(JSON.stringify({
      type: 'connected',
      role,
      clientId,
      roomCode: this.code,
      timestamp: Date.now()
    }));
    
    // 通知另一方
    this.notifyOtherParty(role);
    
    // 启动心跳检测（仅发送方）
    if (role === 'sender') {
      this.startHeartbeat();
    }
    
    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
  
  // 处理消息
  handleMessage(data, socket, role) {
    try {
      const message = JSON.parse(data);
      
      switch (message.type) {
        case 'heartbeat':
          this.lastHeartbeat = Date.now();
          break;
          
        case 'file-metadata':
          // 转发给接收方
          if (this.receiver) {
            this.receiver.send(JSON.stringify({
              type: 'file-metadata',
              metadata: message.metadata,
              timestamp: Date.now()
            }));
          }
          break;
          
        case 'file-chunk':
          // 转发给接收方（不存储）
          if (this.receiver) {
            this.receiver.send(JSON.stringify({
              type: 'file-chunk',
              chunk: message.chunk,
              index: message.index,
              total: message.total,
              timestamp: Date.now()
            }));
          }
          break;
          
        case 'transfer-complete':
          if (this.receiver) {
            this.receiver.send(JSON.stringify({
              type: 'transfer-complete',
              timestamp: Date.now()
            }));
          }
          break;
          
        case 'sender-status':
          // 转发发送方状态给接收方
          if (this.receiver && role === 'sender') {
            this.receiver.send(JSON.stringify({
              type: 'sender-status',
              connected: message.connected,
              timestamp: Date.now(),
              message: message.message || '发送方在线'
            }));
          }
          break;
          
        case 'receiver-status':
          // 转发接收方状态给发送方
          if (this.sender && role === 'receiver') {
            this.sender.send(JSON.stringify({
              type: 'receiver-status',
              connected: message.connected,
              timestamp: Date.now(),
              message: message.message || '接收方在线'
            }));
          }
          break;
      }
    } catch (error) {
      console.error('消息处理错误:', error);
    }
  }
  
  // 更新房间状态
  updateStatus() {
    if (this.sender && this.receiver) {
      this.status = 'active';
    } else if (this.sender) {
      this.status = 'waiting';
    } else {
      this.status = 'empty';
    }
    
    this.storage.put('status', this.status);
  }
  
  // 通知另一方
  notifyOtherParty(connectedRole) {
    if (connectedRole === 'sender' && this.receiver) {
      this.receiver.send(JSON.stringify({
        type: 'sender-connected',
        timestamp: Date.now(),
        message: '发送方已连接'
      }));
    } else if (connectedRole === 'receiver' && this.sender) {
      this.sender.send(JSON.stringify({
        type: 'receiver-connected',
        timestamp: Date.now(),
        message: '接收方已连接'
      }));
    }
  }
  
  // 启动心跳检测
  startHeartbeat() {
    this.lastHeartbeat = Date.now();
    
    // 每30秒检查一次心跳
    this.heartbeatInterval = setInterval(() => {
      const now = Date.now();
      
      if (this.sender && now - this.lastHeartbeat > 45000) {
        console.log(`[${this.code}] 发送方心跳超时`);
        this.sender.close(1001, '心跳超时');
        this.sender = null;
        this.updateStatus();
      }
    }, 30000);
  }
  
  // 清理房间
  async cleanup() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    
    if (this.sender) {
      this.sender.close(1000, '房间关闭');
    }
    
    if (this.receiver) {
      this.receiver.close(1000, '房间关闭');
    }
    
    await this.storage.deleteAll();
  }
  
  // 报警处理（房间过期）
  async alarm() {
    console.log(`房间 ${this.code} 已过期，正在清理`);
    await this.cleanup();
  }
}

// HTML 页面
function getHTML() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ 即时传输 | 极速文件快传</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <script src="https://cdn.jsdelivr.net/npm/feather-icons/dist/feather.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', 'SF Pro Display', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        
        :root {
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --secondary: #8b5cf6;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --light: #f8fafc;
            --dark: #1e293b;
            --gray: #64748b;
            --card-bg: rgba(255, 255, 255, 0.95);
            --shadow: 0 20px 60px rgba(0, 0, 0, 0.1);
            --transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
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
            margin: 0 auto;
        }
        
        .app-card {
            background: var(--card-bg);
            border-radius: 24px;
            padding: 40px 30px;
            box-shadow: var(--shadow);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
            text-align: center;
        }
        
        .logo {
            font-size: 3.5rem;
            margin-bottom: 10px;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            display: inline-block;
        }
        
        h1 {
            font-size: 2.2rem;
            color: var(--dark);
            margin-bottom: 10px;
            font-weight: 700;
        }
        
        .subtitle {
            color: var(--gray);
            margin-bottom: 30px;
            font-size: 1rem;
            line-height: 1.5;
        }
        
        .mode-selector {
            display: flex;
            background: #f1f5f9;
            border-radius: 12px;
            padding: 4px;
            margin-bottom: 30px;
        }
        
        .mode-btn {
            flex: 1;
            padding: 15px;
            border: none;
            background: transparent;
            border-radius: 10px;
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--gray);
            cursor: pointer;
            transition: var(--transition);
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
        }
        
        .mode-btn.active {
            background: white;
            color: var(--primary);
            box-shadow: 0 4px 12px rgba(99, 102, 241, 0.2);
        }
        
        .mode-btn i {
            font-size: 1.2rem;
        }
        
        .panel {
            display: none;
            animation: fadeIn 0.5s ease;
        }
        
        .panel.active {
            display: block;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .form-group {
            margin-bottom: 25px;
            text-align: left;
        }
        
        label {
            display: block;
            margin-bottom: 8px;
            color: var(--dark);
            font-weight: 600;
            font-size: 0.95rem;
        }
        
        .file-upload {
            border: 2px dashed #cbd5e1;
            border-radius: 12px;
            padding: 40px 20px;
            text-align: center;
            background: #f8fafc;
            cursor: pointer;
            transition: var(--transition);
            position: relative;
            overflow: hidden;
        }
        
        .file-upload:hover {
            border-color: var(--primary);
            background: #f0f9ff;
        }
        
        .file-upload i {
            font-size: 3rem;
            color: var(--primary);
            margin-bottom: 15px;
        }
        
        .file-upload-text {
            font-size: 1.1rem;
            color: var(--gray);
            margin-bottom: 5px;
        }
        
        .file-upload-subtext {
            font-size: 0.9rem;
            color: #94a3b8;
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
            border-left: 4px solid var(--primary);
            display: flex;
            align-items: center;
            gap: 15px;
            text-align: left;
        }
        
        .file-info i {
            color: var(--primary);
            font-size: 1.5rem;
        }
        
        .file-details h4 {
            color: var(--dark);
            margin-bottom: 5px;
        }
        
        .file-details p {
            color: var(--gray);
            font-size: 0.9rem;
        }
        
        .code-input {
            width: 100%;
            padding: 18px;
            border: 2px solid #e2e8f0;
            border-radius: 12px;
            font-size: 1.3rem;
            text-align: center;
            letter-spacing: 5px;
            font-weight: 700;
            font-family: 'Courier New', monospace;
            color: var(--dark);
            transition: var(--transition);
        }
        
        .code-input:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 4px rgba(99, 102, 241, 0.1);
        }
        
        .btn {
            width: 100%;
            padding: 18px;
            border: none;
            border-radius: 12px;
            font-size: 1.1rem;
            font-weight: 600;
            cursor: pointer;
            transition: var(--transition);
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
            margin-top: 20px;
        }
        
        .btn-primary {
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            color: white;
        }
        
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 30px rgba(99, 102, 241, 0.4);
        }
        
        .btn-success {
            background: linear-gradient(135deg, var(--success), #0da271);
            color: white;
        }
        
        .btn-success:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 30px rgba(16, 185, 129, 0.4);
        }
        
        .btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none !important;
            box-shadow: none !important;
        }
        
        .status-area {
            margin-top: 30px;
            padding: 25px;
            background: #f8fafc;
            border-radius: 16px;
            border-left: 4px solid var(--primary);
            display: none;
        }
        
        .status-area.active {
            display: block;
        }
        
        .code-display {
            background: linear-gradient(135deg, #f0f9ff, #e0f2fe);
            border: 3px dashed #bae6fd;
            border-radius: 16px;
            padding: 25px;
            margin: 20px 0;
        }
        
        .code-text {
            font-size: 2.8rem;
            font-weight: 800;
            letter-spacing: 15px;
            color: #0369a1;
            font-family: 'Courier New', monospace;
            text-align: center;
        }
        
        .status-info {
            background: white;
            border-radius: 12px;
            padding: 15px;
            margin: 15px 0;
            border-left: 4px solid var(--warning);
        }
        
        .status-info.active {
            border-color: var(--success);
        }
        
        .connection-status {
            display: flex;
            gap: 15px;
            margin: 20px 0;
        }
        
        .status-item {
            flex: 1;
            background: white;
            padding: 15px;
            border-radius: 12px;
            text-align: center;
            border: 2px solid #e2e8f0;
        }
        
        .status-item.active {
            border-color: var(--success);
            background: rgba(16, 185, 129, 0.05);
        }
        
        .status-item.inactive {
            border-color: var(--danger);
            background: rgba(239, 68, 68, 0.05);
        }
        
        .progress-container {
            margin: 25px 0;
        }
        
        .progress-bar {
            height: 10px;
            background: #e2e8f0;
            border-radius: 5px;
            overflow: hidden;
            margin-bottom: 10px;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--primary), var(--secondary));
            border-radius: 5px;
            width: 0%;
            transition: width 0.3s ease;
        }
        
        .progress-text {
            display: flex;
            justify-content: space-between;
            font-size: 0.9rem;
            color: var(--gray);
        }
        
        .alert {
            padding: 15px;
            border-radius: 12px;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 12px;
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
        
        .alert-success {
            background: #d1fae5;
            border: 2px solid #a7f3d0;
            color: #065f46;
        }
        
        .hidden {
            display: none;
        }
        
        .action-buttons {
            display: flex;
            gap: 15px;
            margin-top: 20px;
        }
        
        .action-buttons .btn {
            flex: 1;
            margin: 0;
        }
        
        .instructions {
            margin-top: 30px;
            padding: 20px;
            background: #f8fafc;
            border-radius: 16px;
            border-left: 4px solid var(--primary);
            text-align: left;
        }
        
        .instructions h3 {
            margin-bottom: 15px;
            color: var(--dark);
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .instructions ul {
            padding-left: 20px;
            color: var(--gray);
        }
        
        .instructions li {
            margin-bottom: 10px;
            line-height: 1.5;
        }
        
        @media (max-width: 480px) {
            .app-card {
                padding: 25px 20px;
            }
            
            h1 {
                font-size: 1.8rem;
            }
            
            .code-text {
                font-size: 2rem;
                letter-spacing: 10px;
            }
            
            .mode-btn {
                font-size: 1rem;
                padding: 12px;
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
            <p class="subtitle">极速安全的文件传输工具 | 点对点直传，保护隐私</p>
            
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
                        <div class="file-upload-text">点击或拖放文件到这里</div>
                        <div class="file-upload-subtext">最大支持 2GB 的文件</div>
                        <input type="file" id="fileInput">
                    </div>
                    <div class="file-info hidden" id="fileInfo">
                        <i class="fas fa-file"></i>
                        <div class="file-details">
                            <h4 id="fileName">未选择文件</h4>
                            <p id="fileSize">0 KB</p>
                        </div>
                    </div>
                </div>
                
                <button class="btn btn-primary" id="generateBtn">
                    <i class="fas fa-barcode"></i>
                    生成取件码
                </button>
                
                <div class="status-area" id="senderStatusArea">
                    <div class="alert alert-warning">
                        <i class="fas fa-exclamation-triangle"></i>
                        <div>
                            <strong>重要提示：</strong> 请不要关闭页面或刷新，否则传输会中断！
                        </div>
                    </div>
                    
                    <div class="code-display">
                        <div class="code-text" id="codeDisplay">ABC123</div>
                        <button class="btn btn-success" id="copyCodeBtn">
                            <i class="fas fa-copy"></i>
                            复制取件码
                        </button>
                    </div>
                    
                    <div class="connection-status">
                        <div class="status-item active" id="senderStatusItem">
                            <div>发送方</div>
                            <div class="status-value">在线</div>
                        </div>
                        <div class="status-item inactive" id="receiverStatusItem">
                            <div>接收方</div>
                            <div class="status-value">等待连接</div>
                        </div>
                    </div>
                    
                    <div class="status-info" id="senderStatusInfo">
                        <i class="fas fa-info-circle"></i>
                        等待接收方连接...
                    </div>
                    
                    <div class="progress-container hidden" id="senderProgress">
                        <div class="progress-bar">
                            <div class="progress-fill" id="senderProgressFill"></div>
                        </div>
                        <div class="progress-text">
                            <span>发送进度</span>
                            <span id="senderProgressPercent">0%</span>
                        </div>
                    </div>
                    
                    <div class="action-buttons">
                        <button class="btn btn-primary" id="startTransferBtn" disabled>
                            <i class="fas fa-play"></i>
                            开始传输
                        </button>
                        <button class="btn" id="cancelSenderBtn">
                            <i class="fas fa-times"></i>
                            取消
                        </button>
                    </div>
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
                        <div>
                            <strong>连接状态：</strong> 正在连接到发送方...
                        </div>
                    </div>
                    
                    <div class="connection-status">
                        <div class="status-item inactive" id="remoteSenderStatus">
                            <div>发送方</div>
                            <div class="status-value">离线</div>
                        </div>
                        <div class="status-item active" id="selfStatus">
                            <div>接收方</div>
                            <div class="status-value">在线</div>
                        </div>
                    </div>
                    
                    <div class="status-info" id="receiverStatusInfo">
                        <i class="fas fa-sync fa-spin"></i>
                        正在等待文件...
                    </div>
                    
                    <div class="progress-container">
                        <div class="progress-bar">
                            <div class="progress-fill" id="receiverProgressFill"></div>
                        </div>
                        <div class="progress-text">
                            <span>接收进度</span>
                            <span id="receiverProgressPercent">0%</span>
                        </div>
                    </div>
                    
                    <a class="btn btn-success hidden" id="downloadBtn" download>
                        <i class="fas fa-download"></i>
                        下载文件
                    </a>
                    
                    <button class="btn" id="cancelReceiverBtn">
                        <i class="fas fa-times"></i>
                        断开连接
                    </button>
                </div>
            </div>
            
            <div class="instructions">
                <h3><i class="fas fa-lightbulb"></i> 使用说明</h3>
                <ul>
                    <li>发送文件后，请将取件码告诉接收方</li>
                    <li>传输期间请勿关闭页面或刷新</li>
                    <li>文件直接在双方之间传输，服务器不存储</li>
                    <li>支持大文件传输，最高可达 2GB</li>
                </ul>
            </div>
        </div>
    </div>

    <script>
        // ===================== 配置 =====================
        const API_BASE = '/api';
        let currentRoomCode = null;
        let currentFile = null;
        let senderSocket = null;
        let receiverSocket = null;
        let heartbeatInterval = null;
        
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
        const copyCodeBtn = document.getElementById('copyCodeBtn');
        const senderStatusInfo = document.getElementById('senderStatusInfo');
        const senderProgress = document.getElementById('senderProgress');
        const senderProgressFill = document.getElementById('senderProgressFill');
        const senderProgressPercent = document.getElementById('senderProgressPercent');
        const startTransferBtn = document.getElementById('startTransferBtn');
        const cancelSenderBtn = document.getElementById('cancelSenderBtn');
        const senderStatusItem = document.getElementById('senderStatusItem');
        const receiverStatusItem = document.getElementById('receiverStatusItem');
        
        // 接收端元素
        const codeInput = document.getElementById('codeInput');
        const connectBtn = document.getElementById('connectBtn');
        const receiverStatusArea = document.getElementById('receiverStatusArea');
        const receiverStatusInfo = document.getElementById('receiverStatusInfo');
        const receiverProgressFill = document.getElementById('receiverProgressFill');
        const receiverProgressPercent = document.getElementById('receiverProgressPercent');
        const downloadBtn = document.getElementById('downloadBtn');
        const cancelReceiverBtn = document.getElementById('cancelReceiverBtn');
        const remoteSenderStatus = document.getElementById('remoteSenderStatus');
        const selfStatus = document.getElementById('selfStatus');
        
        // ===================== 工具函数 =====================
        
        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
        
        function showAlert(message, type = 'info') {
            // 创建临时提示
            const alert = document.createElement('div');
            alert.className = `alert alert-${type}`;
            alert.innerHTML = `
                <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'warning' ? 'exclamation-triangle' : 'info-circle'}"></i>
                <div>${message}</div>
            `;
            
            document.querySelector('.app-card').insertBefore(alert, document.querySelector('.instructions'));
            
            setTimeout(() => {
                alert.style.opacity = '0';
                alert.style.transition = 'opacity 0.3s';
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
            element.querySelector('.status-value').textContent = connected ? '在线' : '离线';
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
                fileInfo.classList.remove('hidden');
                generateBtn.disabled = false;
            }
        });
        
        // 拖放支持
        const fileUpload = document.getElementById('fileUpload');
        fileUpload.addEventListener('dragover', (e) => {
            e.preventDefault();
            fileUpload.style.borderColor = '#6366f1';
            fileUpload.style.background = '#f0f9ff';
        });
        
        fileUpload.addEventListener('dragleave', () => {
            fileUpload.style.borderColor = '#cbd5e1';
            fileUpload.style.background = '#f8fafc';
        });
        
        fileUpload.addEventListener('drop', (e) => {
            e.preventDefault();
            fileUpload.style.borderColor = '#cbd5e1';
            fileUpload.style.background = '#f8fafc';
            
            if (e.dataTransfer.files.length > 0) {
                fileInput.files = e.dataTransfer.files;
                fileInput.dispatchEvent(new Event('change'));
            }
        });
        
        // ===================== 发送端逻辑 =====================
        
        generateBtn.addEventListener('click', async () => {
            if (!currentFile) {
                showAlert('请先选择要发送的文件', 'warning');
                return;
            }
            
            generateBtn.disabled = true;
            generateBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 创建中...';
            
            currentRoomCode = generateRoomCode();
            
            try {
                // 创建房间
                const response = await fetch(`${API_BASE}/create`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ code: currentRoomCode })
                });
                
                if (!response.ok) {
                    const error = await response.json();
                    throw new Error(error.error || '创建失败');
                }
                
                const result = await response.json();
                
                // 显示取件码
                codeDisplay.textContent = currentRoomCode;
                senderStatusArea.classList.add('active');
                senderStatusInfo.innerHTML = '<i class="fas fa-info-circle"></i> 房间已创建，等待接收方连接...';
                
                // 连接 WebSocket
                connectAsSender(result.wsUrl);
                
                generateBtn.innerHTML = '<i class="fas fa-check"></i> 已创建';
                startTransferBtn.disabled = false;
                
                showAlert('房间创建成功！请将取件码告知接收方。', 'success');
                
            } catch (error) {
                console.error('创建房间错误:', error);
                showAlert(`创建失败: ${error.message}`, 'warning');
                generateBtn.disabled = false;
                generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
            }
        });
        
        function connectAsSender(wsUrl) {
            const url = new URL(wsUrl);
            url.searchParams.set('role', 'sender');
            url.searchParams.set('clientId', `sender_${Date.now()}`);
            
            senderSocket = new WebSocket(url.toString());
            
            senderSocket.onopen = () => {
                console.log('发送方 WebSocket 已连接');
                senderStatusInfo.innerHTML = '<i class="fas fa-check-circle"></i> 已连接，等待接收方...';
                updateConnectionStatus(senderStatusItem, true);
                
                // 开始心跳
                startHeartbeat();
            };
            
            senderSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    handleSenderMessage(data);
                } catch (error) {
                    console.error('消息解析错误:', error);
                }
            };
            
            senderSocket.onclose = () => {
                console.log('发送方 WebSocket 已断开');
                senderStatusInfo.innerHTML = '<i class="fas fa-times-circle"></i> 连接已断开';
                updateConnectionStatus(senderStatusItem, false);
                stopHeartbeat();
            };
            
            senderSocket.onerror = (error) => {
                console.error('WebSocket 错误:', error);
                showAlert('连接错误，请刷新页面重试', 'warning');
            };
        }
        
        function handleSenderMessage(data) {
            switch (data.type) {
                case 'receiver-connected':
                    senderStatusInfo.innerHTML = '<i class="fas fa-check-circle"></i> 接收方已连接，准备传输';
                    updateConnectionStatus(receiverStatusItem, true);
                    showAlert('接收方已连接！', 'success');
                    break;
                    
                case 'receiver-status':
                    updateConnectionStatus(receiverStatusItem, data.connected);
                    break;
            }
        }
        
        function startHeartbeat() {
            heartbeatInterval = setInterval(() => {
                if (senderSocket && senderSocket.readyState === WebSocket.OPEN) {
                    senderSocket.send(JSON.stringify({
                        type: 'heartbeat',
                        timestamp: Date.now()
                    }));
                    
                    // 发送状态更新
                    senderSocket.send(JSON.stringify({
                        type: 'sender-status',
                        connected: true,
                        timestamp: Date.now(),
                        message: '发送方在线'
                    }));
                }
            }, 2000);
        }
        
        function stopHeartbeat() {
            if (heartbeatInterval) {
                clearInterval(heartbeatInterval);
                heartbeatInterval = null;
            }
        }
        
        // 开始传输文件
        startTransferBtn.addEventListener('click', () => {
            if (!currentFile || !senderSocket) {
                showAlert('无法开始传输', 'warning');
                return;
            }
            
            startTransferBtn.disabled = true;
            senderProgress.classList.remove('hidden');
            
            // 发送文件元数据
            senderSocket.send(JSON.stringify({
                type: 'file-metadata',
                metadata: {
                    name: currentFile.name,
                    size: currentFile.size,
                    type: currentFile.type
                }
            }));
            
            // 分块发送文件
            const CHUNK_SIZE = 64 * 1024; // 64KB
            const totalChunks = Math.ceil(currentFile.size / CHUNK_SIZE);
            let currentChunk = 0;
            
            function sendNextChunk(start) {
                if (currentChunk >= totalChunks || senderSocket.readyState !== WebSocket.OPEN) {
                    // 传输完成
                    senderSocket.send(JSON.stringify({
                        type: 'transfer-complete'
                    }));
                    
                    senderProgressPercent.textContent = '100%';
                    senderProgressFill.style.width = '100%';
                    senderStatusInfo.innerHTML = '<i class="fas fa-check-circle"></i> 文件传输完成！';
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
                    
                    // 发送下一块
                    setTimeout(() => sendNextChunk(end), 0);
                };
                
                reader.readAsArrayBuffer(chunk);
            }
            
            sendNextChunk(0);
        });
        
        // 复制取件码
        copyCodeBtn.addEventListener('click', () => {
            navigator.clipboard.writeText(currentRoomCode)
                .then(() => {
                    copyCodeBtn.innerHTML = '<i class="fas fa-check"></i> 已复制';
                    setTimeout(() => {
                        copyCodeBtn.innerHTML = '<i class="fas fa-copy"></i> 复制取件码';
                    }, 2000);
                })
                .catch(() => {
                    showAlert('复制失败，请手动复制', 'warning');
                });
        });
        
        // 取消发送
        cancelSenderBtn.addEventListener('click', () => {
            if (senderSocket) {
                senderSocket.close(1000, '用户取消');
            }
            resetSender();
        });
        
        function resetSender() {
            senderStatusArea.classList.remove('active');
            fileInfo.classList.add('hidden');
            fileInput.value = '';
            currentFile = null;
            currentRoomCode = null;
            generateBtn.disabled = true;
            generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
            startTransferBtn.disabled = true;
            senderProgress.classList.add('hidden');
            senderProgressFill.style.width = '0%';
            senderProgressPercent.textContent = '0%';
        }
        
        // ===================== 接收端逻辑 =====================
        
        codeInput.addEventListener('input', function() {
            this.value = this.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
        });
        
        connectBtn.addEventListener('click', async () => {
            const code = codeInput.value.trim();
            
            if (code.length !== 6) {
                showAlert('请输入6位取件码', 'warning');
                return;
            }
            
            connectBtn.disabled = true;
            connectBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 连接中...';
            receiverStatusArea.classList.add('active');
            
            try {
                // 检查房间是否存在
                const response = await fetch(`${API_BASE}/room/${code}`);
                
                if (!response.ok) {
                    throw new Error('房间不存在或已过期');
                }
                
                const result = await response.json();
                
                if (!result.exists) {
                    throw new Error('房间不存在或已过期');
                }
                
                // 连接 WebSocket
                connectAsReceiver(result.wsUrl);
                
                connectBtn.innerHTML = '<i class="fas fa-check"></i> 已连接';
                showAlert('房间连接成功！', 'success');
                
            } catch (error) {
                console.error('连接房间错误:', error);
                showAlert(`连接失败: ${error.message}`, 'warning');
                connectBtn.disabled = false;
                connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
                receiverStatusArea.classList.remove('active');
            }
        });
        
        function connectAsReceiver(wsUrl) {
            const url = new URL(wsUrl);
            url.searchParams.set('role', 'receiver');
            url.searchParams.set('clientId', `receiver_${Date.now()}`);
            
            receiverSocket = new WebSocket(url.toString());
            let receivedChunks = [];
            let fileMetadata = null;
            
            receiverSocket.onopen = () => {
                console.log('接收方 WebSocket 已连接');
                receiverStatusInfo.innerHTML = '<i class="fas fa-check-circle"></i> 已连接，等待文件...';
                updateConnectionStatus(selfStatus, true);
            };
            
            receiverSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    handleReceiverMessage(data, receivedChunks);
                } catch (error) {
                    console.error('消息解析错误:', error);
                }
            };
            
            receiverSocket.onclose = () => {
                console.log('接收方 WebSocket 已断开');
                receiverStatusInfo.innerHTML = '<i class="fas fa-times-circle"></i> 连接已断开';
                updateConnectionStatus(selfStatus, false);
            };
            
            receiverSocket.onerror = (error) => {
                console.error('WebSocket 错误:', error);
                showAlert('连接错误，请重试', 'warning');
            };
        }
        
        function handleReceiverMessage(data, receivedChunks) {
            switch (data.type) {
                case 'sender-connected':
                    receiverStatusInfo.innerHTML = '<i class="fas fa-check-circle"></i> 发送方已连接';
                    updateConnectionStatus(remoteSenderStatus, true);
                    showAlert('发送方已连接！', 'success');
                    break;
                    
                case 'sender-status':
                    updateConnectionStatus(remoteSenderStatus, data.connected);
                    break;
                    
                case 'file-metadata':
                    receivedChunks.length = 0;
                    receivedChunks.metadata = data.metadata;
                    receiverStatusInfo.innerHTML = `<i class="fas fa-file"></i> 准备接收: ${data.metadata.name}`;
                    break;
                    
                case 'file-chunk':
                    receivedChunks[data.index] = data.chunk;
                    
                    const progress = Math.round(((data.index + 1) / data.total) * 100);
                    receiverProgressPercent.textContent = `${progress}%`;
                    receiverProgressFill.style.width = `${progress}%`;
                    
                    receiverStatusInfo.innerHTML = `<i class="fas fa-download"></i> 接收中: ${progress}%`;
                    break;
                    
                case 'transfer-complete':
                    // 合并所有数据块
                    const metadata = receivedChunks.metadata;
                    const blob = new Blob(receivedChunks);
                    
                    // 创建下载链接
                    const url = URL.createObjectURL(blob);
                    downloadBtn.href = url;
                    downloadBtn.download = metadata.name;
                    downloadBtn.innerHTML = `<i class="fas fa-download"></i> 下载 ${metadata.name}`;
                    downloadBtn.classList.remove('hidden');
                    
                    receiverStatusInfo.innerHTML = '<i class="fas fa-check-circle"></i> 文件接收完成！';
                    receiverProgressFill.style.background = '#10b981';
                    
                    showAlert('文件接收完成！点击下载按钮保存文件。', 'success');
                    break;
            }
        }
        
        // 取消接收
        cancelReceiverBtn.addEventListener('click', () => {
            if (receiverSocket) {
                receiverSocket.close(1000, '用户断开');
            }
            resetReceiver();
        });
        
        function resetReceiver() {
            receiverStatusArea.classList.remove('active');
            codeInput.value = '';
            downloadBtn.classList.add('hidden');
            receiverProgressFill.style.width = '0%';
            receiverProgressPercent.textContent = '0%';
            receiverProgressFill.style.background = 'linear-gradient(90deg, #6366f1, #8b5cf6)';
            connectBtn.disabled = false;
            connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
        }
        
        // ===================== 页面加载 =====================
        window.addEventListener('load', () => {
            // 测试 API 连接
            fetch(`${API_BASE}/health`)
                .then(res => res.json())
                .then(data => {
                    console.log('API 连接正常:', data);
                })
                .catch(error => {
                    console.warn('API 连接检查失败:', error);
                });
            
            // 页面关闭警告
            window.addEventListener('beforeunload', (e) => {
                if (senderSocket || receiverSocket) {
                    e.preventDefault();
                    e.returnValue = '文件传输正在进行中，确定要离开吗？';
                }
            });
        });
    </script>
</body>
</html>`;
}