// 即时传输 Worker - 完整实现
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
      const html = getHTML();
      return new Response(html, {
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
      await room.fetch('http://internal/init', {
        method: 'POST',
        body: JSON.stringify({ code, createdAt: Date.now() })
      });
      
      return Response.json({ 
        success: true, 
        code,
        wsUrl: `${url.origin.replace('http', 'ws')}/api/ws/${code}`,
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
      
      const info = await room.fetch('http://internal/info').then(r => r.json());
      
      return Response.json({
        exists: true,
        code: info.code,
        status: info.status,
        createdAt: info.createdAt,
        connections: info.connections,
        wsUrl: `${url.origin.replace('http', 'ws')}/api/ws/${code}`
      });
      
    } catch (error) {
      return Response.json({ exists: false }, { status: 404 });
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
    this.status = 'waiting';
    this.createdAt = null;
    
    // WebSocket 连接
    this.sender = null;
    this.receiver = null;
    
    // 心跳
    this.lastHeartbeat = null;
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
    
    return Response.json({
      code,
      status: status || 'unknown',
      createdAt: parseInt(createdAt) || Date.now(),
      connections: {
        sender: !!this.sender,
        receiver: !!this.receiver
      }
    });
  }
  
  // 处理 WebSocket 连接
  async handleWebSocket(request) {
    const url = new URL(request.url);
    const role = url.searchParams.get('role');
    const clientId = `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    // 创建 WebSocket 对
    const { 0: client, 1: server } = new WebSocketPair();
    
    server.accept();
    
    // 设置连接
    if (role === 'sender') {
      this.sender = server;
    } else if (role === 'receiver') {
      this.receiver = server;
    } else {
      server.close(1008, 'Invalid role');
      return new Response(null, { status: 400 });
    }
    
    // 发送连接成功消息
    server.send(JSON.stringify({
      type: 'connected',
      role,
      clientId,
      timestamp: Date.now()
    }));
    
    // 设置消息处理器
    server.addEventListener('message', (event) => {
      try {
        const data = JSON.parse(event.data);
        this.handleMessage(data, server, role);
      } catch (error) {
        console.error('消息处理错误:', error);
      }
    });
    
    // 连接关闭
    server.addEventListener('close', () => {
      if (role === 'sender') {
        this.sender = null;
        if (this.receiver) {
          this.receiver.send(JSON.stringify({
            type: 'sender-status',
            connected: false,
            timestamp: Date.now()
          }));
        }
      } else if (role === 'receiver') {
        this.receiver = null;
        if (this.sender) {
          this.sender.send(JSON.stringify({
            type: 'receiver-status',
            connected: false,
            timestamp: Date.now()
          }));
        }
      }
    });
    
    server.addEventListener('error', (error) => {
      console.error('WebSocket error:', error);
    });
    
    // 通知另一方
    if (role === 'sender' && this.receiver) {
      this.receiver.send(JSON.stringify({
        type: 'sender-connected',
        timestamp: Date.now()
      }));
    } else if (role === 'receiver' && this.sender) {
      this.sender.send(JSON.stringify({
        type: 'receiver-connected',
        timestamp: Date.now()
      }));
    }
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  // 处理消息
  handleMessage(data, socket, role) {
    switch (data.type) {
      case 'heartbeat':
        this.lastHeartbeat = Date.now();
        break;
        
      case 'file-metadata':
        if (this.receiver) {
          this.receiver.send(JSON.stringify({
            type: 'file-metadata',
            metadata: data.metadata,
            timestamp: Date.now()
          }));
        }
        break;
        
      case 'file-chunk':
        if (this.receiver) {
          this.receiver.send(JSON.stringify({
            type: 'file-chunk',
            chunk: data.chunk,
            index: data.index,
            total: data.total,
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
        if (this.receiver && role === 'sender') {
          this.receiver.send(JSON.stringify({
            type: 'sender-status',
            connected: data.connected,
            timestamp: Date.now()
          }));
        }
        break;
    }
  }
  
  // 报警处理（房间过期）
  async alarm() {
    console.log(`房间 ${this.code} 已过期，正在清理`);
    
    if (this.sender) {
      this.sender.close(1000, '房间过期');
    }
    
    if (this.receiver) {
      this.receiver.close(1000, '房间过期');
    }
    
    await this.storage.deleteAll();
  }
}

// HTML 页面
function getHTML() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ 即时传输</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', system-ui, sans-serif;
        }
        
        :root {
            --primary: #6366f1;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --light: #f8fafc;
            --dark: #1e293b;
            --gray: #64748b;
        }
        
        body {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        
        .container {
            width: 100%;
            max-width: 500px;
        }
        
        .app-card {
            background: white;
            border-radius: 20px;
            padding: 40px 30px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.1);
            text-align: center;
        }
        
        .logo {
            font-size: 3rem;
            color: var(--primary);
            margin-bottom: 15px;
        }
        
        h1 {
            font-size: 2rem;
            color: var(--dark);
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: var(--gray);
            margin-bottom: 30px;
            font-size: 0.95rem;
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
            color: var(--gray);
            cursor: pointer;
            transition: all 0.3s;
        }
        
        .mode-btn.active {
            background: white;
            color: var(--primary);
            box-shadow: 0 4px 12px rgba(99, 102, 241, 0.2);
        }
        
        .mode-btn i {
            margin-right: 8px;
        }
        
        .panel {
            display: none;
        }
        
        .panel.active {
            display: block;
            animation: fadeIn 0.3s ease;
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
            font-size: 0.9rem;
        }
        
        .file-upload {
            border: 2px dashed #cbd5e1;
            border-radius: 12px;
            padding: 40px 20px;
            text-align: center;
            background: #f8fafc;
            cursor: pointer;
            transition: all 0.3s;
        }
        
        .file-upload:hover {
            border-color: var(--primary);
            background: #f0f9ff;
        }
        
        .file-upload i {
            font-size: 2.5rem;
            color: var(--primary);
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
            border-left: 4px solid var(--primary);
            text-align: left;
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
            border-color: var(--primary);
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
            background: var(--primary);
            color: white;
        }
        
        .btn-primary:hover:not(:disabled) {
            background: #4f46e5;
            transform: translateY(-2px);
            box-shadow: 0 10px 20px rgba(99, 102, 241, 0.3);
        }
        
        .btn-success {
            background: var(--success);
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
            text-align: left;
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
            color: var(--primary);
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
            background: var(--primary);
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
            border-color: var(--success);
            background: rgba(16, 185, 129, 0.1);
        }
        
        .hidden {
            display: none;
        }
        
        .instructions {
            margin-top: 30px;
            padding: 20px;
            background: #f8fafc;
            border-radius: 16px;
            text-align: left;
            font-size: 0.9rem;
            color: var(--gray);
        }
        
        @media (max-width: 480px) {
            .app-card {
                padding: 25px 20px;
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
                    <div class="file-info hidden" id="fileInfo">
                        <div id="fileName">未选择文件</div>
                        <div id="fileSize" style="font-size: 0.9rem; color: var(--gray);">0 KB</div>
                    </div>
                </div>
                
                <button class="btn btn-primary" id="generateBtn">
                    <i class="fas fa-barcode"></i>
                    生成取件码
                </button>
                
                <div class="status-area" id="senderStatusArea">
                    <div class="alert alert-warning">
                        <i class="fas fa-exclamation-triangle"></i>
                        请不要关闭页面或刷新
                    </div>
                    
                    <div class="code-display" id="codeDisplay">ABC123</div>
                    
                    <div class="connection-status">
                        <div class="status-item active" id="senderStatusItem">
                            发送方：<span>在线</span>
                        </div>
                        <div class="status-item" id="receiverStatusItem">
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
                        <div class="status-item" id="remoteSenderStatus">
                            发送方：<span>检查中...</span>
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
                    
                    <button class="btn" id="cancelReceiverBtn">
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
        // 配置
        const API_BASE = '/api';
        let currentRoomCode = null;
        let currentFile = null;
        let senderSocket = null;
        let receiverSocket = null;
        
        // DOM 元素
        const senderModeBtn = document.getElementById('senderModeBtn');
        const receiverModeBtn = document.getElementById('receiverModeBtn');
        const senderPanel = document.getElementById('senderPanel');
        const receiverPanel = document.getElementById('receiverPanel');
        
        // 发送端
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
        
        // 接收端
        const codeInput = document.getElementById('codeInput');
        const connectBtn = document.getElementById('connectBtn');
        const receiverStatusArea = document.getElementById('receiverStatusArea');
        const receiverProgressFill = document.getElementById('receiverProgressFill');
        const downloadBtn = document.getElementById('downloadBtn');
        const cancelReceiverBtn = document.getElementById('cancelReceiverBtn');
        
        // 工具函数
        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
        
        function showMessage(message, type = 'info') {
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
        
        // 模式切换
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
        
        // 文件选择
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                currentFile = e.target.files[0];
                fileName.textContent = currentFile.name;
                fileSize.textContent = formatBytes(currentFile.size);
                fileInfo.classList.remove('hidden');
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
                const response = await fetch(API_BASE + '/create', {
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
                
                showMessage('房间创建成功！', 'success');
                
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
                const data = JSON.parse(event.data);
                
                switch (data.type) {
                    case 'receiver-connected':
                        // 开始发送文件
                        sendFile();
                        break;
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
                
                if (!result.exists) {
                    throw new Error('房间不存在');
                }
                
                // 连接 WebSocket
                connectAsReceiver(result.wsUrl);
                
                receiverStatusArea.classList.add('active');
                showMessage('连接成功！', 'success');
                
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
                        showMessage('文件接收完成！', 'success');
                        break;
                        
                    case 'sender-status':
                        // 更新发送方状态
                        document.querySelector('#remoteSenderStatus span').textContent = 
                            data.connected ? '在线' : '离线';
                        break;
                }
            };
            
            receiverSocket.onclose = () => {
                console.log('接收方连接关闭');
            };
        }
        
        // 取消操作
        cancelBtn.addEventListener('click', () => {
            if (senderSocket) senderSocket.close();
            senderStatusArea.classList.remove('hidden');
            generateBtn.disabled = false;
            generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
        });
        
        cancelReceiverBtn.addEventListener('click', () => {
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
}