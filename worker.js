// 即时传输 Worker - 修复版
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
    
    // 健康检查
    if (pathname === '/api/health') {
      return Response.json({ 
        status: 'ok', 
        timestamp: Date.now(),
        service: 'Instant Transfer'
      });
    }
    
    // WebSocket 连接
    if (pathname.startsWith('/api/ws/')) {
      return handleWebSocket(request);
    }
    
    // API 路由
    if (pathname.startsWith('/api/')) {
      return handleAPI(request);
    }
    
    return new Response('Not Found', { status: 404 });
  },
};

// 房间存储（内存中，重启会丢失，但简单）
let rooms = new Map();

// API 处理器
async function handleAPI(request) {
  const url = new URL(request.url);
  const pathname = url.pathname;
  
  // 创建房间
  if (pathname === '/api/create' && request.method === 'POST') {
    try {
      const { code } = await request.json();
      
      if (!code || code.length !== 6) {
        return Response.json({ error: '取件码必须是6位字符' }, { status: 400 });
      }
      
      // 检查房间是否已存在
      if (rooms.has(code)) {
        return Response.json({ error: '房间已存在' }, { status: 409 });
      }
      
      // 创建新房间
      const room = {
        code,
        createdAt: Date.now(),
        status: 'waiting',
        sender: null,
        receiver: null
      };
      
      rooms.set(code, room);
      
      // 30分钟后自动清理
      setTimeout(() => {
        if (rooms.has(code)) {
          const room = rooms.get(code);
          if (room.sender) room.sender.close(1000, '房间过期');
          if (room.receiver) room.receiver.close(1000, '房间过期');
          rooms.delete(code);
        }
      }, 30 * 60 * 1000);
      
      return Response.json({ 
        success: true, 
        code,
        wsUrl: `${url.origin.replace('http', 'ws')}/api/ws/${code}`,
        message: '房间创建成功'
      });
      
    } catch (error) {
      return Response.json({ error: '创建失败' }, { status: 500 });
    }
  }
  
  // 查询房间
  if (pathname.startsWith('/api/room/') && request.method === 'GET') {
    const code = pathname.split('/').pop();
    
    if (!code || code.length !== 6) {
      return Response.json({ error: '无效的取件码' }, { status: 400 });
    }
    
    const room = rooms.get(code);
    if (!room) {
      return Response.json({ exists: false }, { status: 404 });
    }
    
    return Response.json({
      exists: true,
      code: room.code,
      status: room.status,
      createdAt: room.createdAt,
      connections: {
        sender: !!room.sender,
        receiver: !!room.receiver
      },
      wsUrl: `${url.origin.replace('http', 'ws')}/api/ws/${code}`
    });
  }
  
  return new Response('Not Found', { status: 404 });
}

// WebSocket 处理器
async function handleWebSocket(request) {
  const url = new URL(request.url);
  const code = url.pathname.split('/').pop();
  const role = url.searchParams.get('role');
  
  if (!code || !role || !['sender', 'receiver'].includes(role)) {
    return new Response('Invalid request', { status: 400 });
  }
  
  const room = rooms.get(code);
  if (!room) {
    return new Response('Room not found', { status: 404 });
  }
  
  // 创建 WebSocket 对
  const { 0: client, 1: server } = new WebSocketPair();
  server.accept();
  
  // 存储连接
  if (role === 'sender') {
    room.sender = server;
  } else {
    room.receiver = server;
  }
  
  room.status = room.sender && room.receiver ? 'active' : 'waiting';
  
  // 发送连接成功消息
  server.send(JSON.stringify({
    type: 'connected',
    role,
    timestamp: Date.now()
  }));
  
  // 消息处理器
  server.addEventListener('message', (event) => {
    try {
      const data = JSON.parse(event.data);
      
      switch (data.type) {
        case 'file-metadata':
          if (room.receiver) {
            room.receiver.send(JSON.stringify({
              type: 'file-metadata',
              metadata: data.metadata
            }));
          }
          break;
          
        case 'file-chunk':
          if (room.receiver) {
            room.receiver.send(JSON.stringify({
              type: 'file-chunk',
              chunk: data.chunk,
              index: data.index,
              total: data.total
            }));
          }
          break;
          
        case 'transfer-complete':
          if (room.receiver) {
            room.receiver.send(JSON.stringify({
              type: 'transfer-complete'
            }));
          }
          break;
          
        case 'sender-status':
          if (room.receiver && role === 'sender') {
            room.receiver.send(JSON.stringify({
              type: 'sender-status',
              connected: data.connected,
              timestamp: Date.now()
            }));
          }
          break;
      }
    } catch (error) {
      console.error('消息处理错误:', error);
    }
  });
  
  // 连接关闭
  server.addEventListener('close', () => {
    if (role === 'sender') {
      room.sender = null;
      if (room.receiver) {
        room.receiver.send(JSON.stringify({
          type: 'sender-status',
          connected: false,
          timestamp: Date.now()
        }));
      }
    } else {
      room.receiver = null;
    }
    
    room.status = room.sender && room.receiver ? 'active' : 'waiting';
    
    // 如果双方都断开，清理房间
    if (!room.sender && !room.receiver) {
      setTimeout(() => {
        if (rooms.get(code) === room) {
          rooms.delete(code);
        }
      }, 60000);
    }
  });
  
  // 通知另一方
  if (role === 'sender' && room.receiver) {
    room.receiver.send(JSON.stringify({
      type: 'sender-connected',
      timestamp: Date.now()
    }));
  } else if (role === 'receiver' && room.sender) {
    room.sender.send(JSON.stringify({
      type: 'receiver-connected',
      timestamp: Date.now()
    }));
  }
  
  return new Response(null, { status: 101, webSocket: client });
}

// HTML 页面 - 完全重写，修复点击问题
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
            border-radius: 24px;
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
        }
        
        label {
            display: block;
            margin-bottom: 8px;
            color: var(--dark);
            font-weight: 600;
            text-align: left;
        }
        
        /* 修复文件选择区域 - 现在只有点击上传图标才有效 */
        .upload-area {
            border: 2px dashed #cbd5e1;
            border-radius: 12px;
            padding: 30px 20px;
            background: #f8fafc;
            cursor: pointer;
            transition: all 0.3s;
            position: relative;
        }
        
        .upload-area:hover {
            border-color: var(--primary);
            background: #f0f9ff;
        }
        
        .upload-icon {
            width: 60px;
            height: 60px;
            background: var(--primary);
            color: white;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 15px;
            font-size: 1.5rem;
        }
        
        .upload-text {
            color: var(--gray);
            margin-bottom: 5px;
        }
        
        .upload-subtext {
            font-size: 0.85rem;
            color: #94a3b8;
        }
        
        /* 文件选择按钮 - 只在图标上 */
        #fileSelectBtn {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 60px;
            height: 60px;
            opacity: 0;
            cursor: pointer;
            z-index: 2;
        }
        
        /* 隐藏默认的文件输入 */
        #fileInput {
            display: none;
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
            display: none !important;
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
                    <div class="upload-area" id="uploadArea">
                        <div class="upload-icon">
                            <i class="fas fa-cloud-upload-alt"></i>
                        </div>
                        <div class="upload-text">点击上方图标选择文件</div>
                        <div class="upload-subtext">支持所有类型文件</div>
                        <!-- 只在图标上触发文件选择 -->
                        <button class="btn" id="fileSelectBtn" style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 60px; height: 60px; opacity: 0;"></button>
                        <input type="file" id="fileInput">
                    </div>
                    <div class="file-info hidden" id="fileInfo">
                        <div id="fileName">未选择文件</div>
                        <div id="fileSize" style="font-size: 0.9rem; color: var(--gray);">0 KB</div>
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
        const fileSelectBtn = document.getElementById('fileSelectBtn');
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
        
        // 文件选择 - 现在只在点击图标按钮时触发
        fileSelectBtn.addEventListener('click', (e) => {
            e.stopPropagation(); // 阻止事件冒泡
            fileInput.click(); // 触发隐藏的文件输入
        });
        
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
                    throw new Error(result.error || '创建失败');
                }
                
                codeDisplay.textContent = currentRoomCode;
                senderStatusArea.classList.add('active');
                
                // 连接 WebSocket
                connectAsSender(result.wsUrl);
                
                showMessage('房间创建成功！请将取件码告知接收方', 'success');
                
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
                    
                    switch (data.type) {
                        case 'receiver-connected':
                            // 开始发送文件
                            sendFile();
                            break;
                    }
                } catch (error) {
                    console.error('消息解析错误:', error);
                }
            };
            
            senderSocket.onclose = () => {
                console.log('发送方连接关闭');
                showMessage('连接已断开', 'warning');
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
                if (senderSocket.readyState !== WebSocket.OPEN) {
                    showMessage('连接已断开，传输失败', 'warning');
                    return;
                }
                
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
                    showMessage('文件发送完成！', 'success');
                }
            };
            
            readNext();
        }
        
        // 接收文件
        codeInput.addEventListener('input', function() {
            this.value = this.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
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
                    throw new Error('房间不存在或已过期');
                }
                
                // 连接 WebSocket
                connectAsReceiver(result.wsUrl);
                
                receiverStatusArea.classList.add('active');
                showMessage('连接成功！等待文件...', 'success');
                
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
            let fileName = '';
            
            receiverSocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    
                    switch (data.type) {
                        case 'file-metadata':
                            fileSize = data.metadata.size;
                            fileName = data.metadata.name;
                            receivedChunks = [];
                            showMessage('开始接收文件: ' + fileName, 'info');
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
                            downloadBtn.download = fileName || '下载的文件';
                            downloadBtn.innerHTML = '<i class="fas fa-download"></i> 下载文件';
                            downloadBtn.classList.remove('hidden');
                            showMessage('文件接收完成！点击下载按钮保存', 'success');
                            break;
                            
                        case 'sender-status':
                            // 更新发送方状态
                            document.querySelector('#remoteSenderStatus span').textContent = 
                                data.connected ? '在线' : '离线';
                            break;
                            
                        case 'sender-connected':
                            showMessage('发送方已连接，开始传输文件...', 'success');
                            break;
                    }
                } catch (error) {
                    console.error('消息处理错误:', error);
                }
            };
            
            receiverSocket.onclose = () => {
                console.log('接收方连接关闭');
                showMessage('连接已断开', 'warning');
            };
        }
        
        // 取消操作
        cancelBtn.addEventListener('click', () => {
            if (senderSocket) {
                senderSocket.close(1000, '用户取消');
            }
            senderStatusArea.classList.remove('active');
            generateBtn.disabled = false;
            generateBtn.innerHTML = '<i class="fas fa-barcode"></i> 生成取件码';
        });
        
        cancelReceiverBtn.addEventListener('click', () => {
            if (receiverSocket) {
                receiverSocket.close(1000, '用户断开');
            }
            receiverStatusArea.classList.remove('active');
            connectBtn.disabled = false;
            connectBtn.innerHTML = '<i class="fas fa-plug"></i> 连接房间';
        });
        
        // 页面关闭警告
        window.addEventListener('beforeunload', (e) => {
            if (senderSocket || receiverSocket) {
                e.preventDefault();
                e.returnValue = '文件传输正在进行中，确定要离开吗？';
                return e.returnValue;
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