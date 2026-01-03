// ===================== 即时传输 Worker - 最终版本 =====================

// 全局房间存储
let rooms = new Map();

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
  const room = rooms.get(code);
  if (room) {
    if (room.sender) {
      try { room.sender.close(1000, '房间过期'); } catch (e) {}
    }
    if (room.receiver) {
      try { room.receiver.close(1000, '房间过期'); } catch (e) {}
    }
    if (room.heartbeatInterval) {
      clearInterval(room.heartbeatInterval);
    }
    rooms.delete(code);
  }
}

function updateRoomStatus(room) {
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

function broadcastToRoom(room, message, exclude = null) {
  const messageStr = typeof message === 'string' ? message : JSON.stringify(message);
  
  if (room.sender && room.sender !== exclude && room.sender.readyState === 1) {
    try { room.sender.send(messageStr); } catch (e) {}
  }
  
  if (room.receiver && room.receiver !== exclude && room.receiver.readyState === 1) {
    try { room.receiver.send(messageStr); } catch (e) {}
  }
}

function startHeartbeatCheck(room, ctx) {
  if (room.heartbeatInterval) {
    clearInterval(room.heartbeatInterval);
  }
  
  room.heartbeatInterval = setInterval(() => {
    if (!rooms.has(room.code)) {
      clearInterval(room.heartbeatInterval);
      return;
    }
    
    const now = Date.now();
    
    // 如果超过60秒没有心跳，断开发送方连接
    if (room.lastHeartbeat && now - room.lastHeartbeat > 60000) {
      if (room.sender) {
        try { room.sender.close(1001, '心跳超时'); } catch (e) {}
        room.sender = null;
        updateRoomStatus(room);
        
        broadcastToRoom(room, {
          type: 'error',
          message: '发送方心跳超时',
          timestamp: now
        });
      }
      clearInterval(room.heartbeatInterval);
    }
  }, 30000);
}

function handleWebSocketMessage(event, socket, role, room) {
  const timestamp = Date.now();
  
  try {
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
          
          if (room.senderReady && room.receiverReady && room.status !== 'transferring') {
            room.status = 'transferring';
            room.transferStartTime = timestamp;
            
            broadcastToRoom(room, {
              type: 'transfer-start',
              timestamp
            });
          }
          
          updateRoomStatus(room);
          break;
          
        case 'file-metadata':
          room.fileMetadata = data.metadata;
          room.transferStats = {
            bytesTransferred: 0,
            chunksSent: 0,
            chunksReceived: 0,
            startTime: timestamp
          };
          
          broadcastToRoom(room, {
            type: 'file-metadata',
            metadata: data.metadata,
            timestamp
          }, socket);
          break;
          
        case 'transfer-progress':
          if (room.receiver && role === 'sender') {
            room.receiver.send(JSON.stringify({
              type: 'transfer-progress',
              progress: data.progress,
              bytesTransferred: data.bytesTransferred,
              chunksSent: data.chunksSent,
              timestamp
            }));
          }
          break;
          
        case 'transfer-complete':
          room.transferStats.endTime = timestamp;
          room.status = 'completed';
          
          broadcastToRoom(room, {
            type: 'transfer-complete',
            timestamp,
            stats: {
              bytesTransferred: room.transferStats.bytesTransferred,
              duration: timestamp - room.transferStats.startTime
            }
          }, socket);
          break;
          
        case 'chunk-ack':
          if (room.sender && role === 'receiver') {
            room.sender.send(JSON.stringify({
              type: 'chunk-ack',
              timestamp
            }));
          }
          break;
      }
    } else if (event.data instanceof ArrayBuffer) {
      // 二进制数据直接转发
      if (role === 'sender' && room.receiver) {
        room.transferStats.bytesTransferred += event.data.byteLength;
        room.transferStats.chunksSent++;
        
        room.receiver.send(event.data);
      } else if (role === 'receiver' && room.sender) {
        room.transferStats.chunksReceived++;
        
        // 发送确认
        room.sender.send(JSON.stringify({
          type: 'chunk-ack',
          timestamp
        }));
      }
    }
  } catch (error) {
    console.error('处理消息错误:', error);
  }
}

function handleWebSocketClose(socket, role, room, code) {
  if (role === 'sender') {
    room.sender = null;
    room.senderReady = false;
    
    if (room.receiver) {
      room.receiver.send(JSON.stringify({
        type: 'sender-disconnected',
        timestamp: Date.now(),
        message: '发送方已断开连接'
      }));
    }
  } else {
    room.receiver = null;
    room.receiverReady = false;
    
    if (room.sender) {
      room.sender.send(JSON.stringify({
        type: 'receiver-disconnected',
        timestamp: Date.now(),
        message: '接收方已断开连接'
      }));
    }
  }
  
  updateRoomStatus(room);
  
  // 如果双方都断开，安排清理
  if (!room.sender && !room.receiver) {
    setTimeout(() => {
      if (rooms.get(code) === room && !room.sender && !room.receiver) {
        rooms.delete(code);
      }
    }, 5 * 60 * 1000);
  }
}

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
      }, { headers: corsHeaders });
    }
    
    // 创建房间
    if (pathname === '/api/room/create' && request.method === 'POST') {
      try {
        const data = await request.json();
        let code = data.code;
        
        if (!code) {
          code = generateRoomCode();
        } else {
          code = code.toUpperCase();
          if (code.length !== 6) {
            return Response.json({ 
              success: false, 
              error: '取件码必须是6位字符' 
            }, { status: 400, headers: corsHeaders });
          }
        }
        
        // 检查房间是否已存在
        if (rooms.has(code)) {
          const room = rooms.get(code);
          const now = Date.now();
          
          if (now - room.createdAt < 30 * 60 * 1000) {
            return Response.json({ 
              success: false, 
              error: '房间已存在' 
            }, { status: 409, headers: corsHeaders });
          } else {
            cleanupRoom(code);
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
          transferStats: null,
          lastHeartbeat: null,
          heartbeatInterval: null,
          transferStartTime: null
        };
        
        rooms.set(code, room);
        
        // 30分钟后自动清理
        ctx.waitUntil(new Promise(resolve => {
          setTimeout(() => {
            cleanupRoom(code);
            resolve();
          }, 30 * 60 * 1000);
        }));
        
        return Response.json({ 
          success: true, 
          code: code,
          wsUrl: (url.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + url.host + '/api/ws/' + code,
          message: '房间创建成功',
          createdAt: room.createdAt,
          expiresAt: room.createdAt + 30 * 60 * 1000
        }, { headers: corsHeaders });
        
      } catch (error) {
        console.error('创建房间错误:', error);
        return Response.json({ 
          success: false, 
          error: '服务器错误' 
        }, { status: 500, headers: corsHeaders });
      }
    }
    
    // 查询房间
    if (pathname.startsWith('/api/room/') && request.method === 'GET') {
      const code = pathname.split('/').pop().toUpperCase();
      
      if (!code || code.length !== 6) {
        return Response.json({ 
          success: false, 
          error: '无效的取件码格式' 
        }, { status: 400, headers: corsHeaders });
      }
      
      const room = rooms.get(code);
      if (!room) {
        return Response.json({ 
          success: false, 
          error: '房间不存在或已过期' 
        }, { status: 404, headers: corsHeaders });
      }
      
      // 检查是否过期
      if (Date.now() - room.createdAt > 30 * 60 * 1000) {
        cleanupRoom(code);
        return Response.json({ 
          success: false, 
          error: '房间已过期' 
        }, { status: 404, headers: corsHeaders });
      }
      
      return Response.json({
        success: true,
        code: room.code,
        status: room.status,
        createdAt: room.createdAt,
        expiresAt: room.createdAt + 30 * 60 * 1000,
        connections: {
          sender: !!room.sender,
          receiver: !!room.receiver
        },
        fileMetadata: room.fileMetadata,
        transferStats: room.transferStats,
        wsUrls: {
          sender: (url.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + url.host + '/api/ws/' + code + '?role=sender',
          receiver: (url.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + url.host + '/api/ws/' + code + '?role=receiver'
        }
      }, { headers: corsHeaders });
    }
    
    // 清理房间
    if (pathname === '/api/cleanup' && request.method === 'POST') {
      const now = Date.now();
      let cleaned = 0;
      
      for (const [code, room] of rooms.entries()) {
        if (now - room.createdAt > 30 * 60 * 1000) {
          cleanupRoom(code);
          cleaned++;
        }
      }
      
      return Response.json({
        success: true,
        cleaned: cleaned,
        remaining: rooms.size,
        timestamp: Date.now()
      }, { headers: corsHeaders });
    }
    
    // WebSocket 连接
    if (pathname.startsWith('/api/ws/')) {
      const code = pathname.split('/').pop().toUpperCase();
      const role = url.searchParams.get('role');
      
      if (!code || code.length !== 6) {
        return new Response('无效的房间代码', { status: 400 });
      }
      
      if (!role || (role !== 'sender' && role !== 'receiver')) {
        return new Response('无效的角色', { status: 400 });
      }
      
      const room = rooms.get(code);
      if (!room) {
        return new Response('房间不存在', { status: 404 });
      }
      
      if (Date.now() - room.createdAt > 30 * 60 * 1000) {
        cleanupRoom(code);
        return new Response('房间已过期', { status: 410 });
      }
      
      if (role === 'sender' && room.sender) {
        return new Response('发送方已连接', { status: 409 });
      }
      
      if (role === 'receiver' && room.receiver) {
        return new Response('接收方已连接', { status: 409 });
      }
      
      const { 0: client, 1: server } = new WebSocketPair();
      server.accept();
      
      if (role === 'sender') {
        room.sender = server;
        room.lastHeartbeat = Date.now();
        startHeartbeatCheck(room, ctx);
      } else {
        room.receiver = server;
      }
      
      updateRoomStatus(room);
      
      server.send(JSON.stringify({
        type: 'connected',
        success: true,
        role: role,
        timestamp: Date.now(),
        message: '连接成功',
        roomStatus: room.status
      }));
      
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
      
      server.addEventListener('message', (event) => {
        handleWebSocketMessage(event, server, role, room);
      });
      
      server.addEventListener('close', () => {
        handleWebSocketClose(server, role, room, code);
      });
      
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

// HTML 内容（完全使用字符串拼接，避免模板字符串）
const HTML_CONTENT = '<!DOCTYPE html>' +
'<html lang="zh-CN">' +
'<head>' +
'    <meta charset="UTF-8">' +
'    <meta name="viewport" content="width=device-width, initial-scale=1.0">' +
'    <title>即时文件传输</title>' +
'    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">' +
'    <style>' +
'        * { margin: 0; padding: 0; box-sizing: border-box; font-family: Arial, sans-serif; }' +
'        body { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; justify-content: center; align-items: center; padding: 20px; }' +
'        .container { width: 100%; max-width: 500px; }' +
'        .app-card { background: white; border-radius: 20px; padding: 30px; box-shadow: 0 20px 60px rgba(0,0,0,0.1); text-align: center; }' +
'        .logo { font-size: 3rem; color: #3b82f6; margin-bottom: 15px; }' +
'        h1 { font-size: 2rem; color: #1e293b; margin-bottom: 10px; }' +
'        .subtitle { color: #64748b; margin-bottom: 30px; }' +
'        .mode-selector { display: flex; background: #f1f5f9; border-radius: 12px; padding: 5px; margin-bottom: 30px; }' +
'        .mode-btn { flex: 1; padding: 15px; border: none; background: transparent; border-radius: 8px; font-size: 1rem; font-weight: 600; color: #64748b; cursor: pointer; transition: all 0.3s; }' +
'        .mode-btn.active { background: white; color: #3b82f6; box-shadow: 0 4px 12px rgba(59,130,246,0.2); }' +
'        .mode-btn i { margin-right: 8px; }' +
'        .panel { display: none; }' +
'        .panel.active { display: block; }' +
'        .form-group { margin-bottom: 25px; text-align: left; }' +
'        label { display: block; margin-bottom: 8px; color: #1e293b; font-weight: 600; }' +
'        .file-upload { border: 2px dashed #cbd5e1; border-radius: 12px; padding: 40px 20px; text-align: center; background: #f8fafc; cursor: pointer; transition: all 0.3s; position: relative; }' +
'        .file-upload:hover { border-color: #3b82f6; background: #f0f9ff; }' +
'        .file-upload i { font-size: 2.5rem; color: #3b82f6; margin-bottom: 10px; }' +
'        input[type="file"] { position: absolute; width: 100%; height: 100%; top: 0; left: 0; opacity: 0; cursor: pointer; }' +
'        .file-info { background: #f0f9ff; border-radius: 12px; padding: 15px; margin-top: 15px; border-left: 4px solid #3b82f6; display: none; }' +
'        .code-input { width: 100%; padding: 15px; border: 2px solid #e2e8f0; border-radius: 12px; font-size: 1.2rem; text-align: center; letter-spacing: 5px; font-family: "Courier New", monospace; }' +
'        .code-input:focus { outline: none; border-color: #3b82f6; }' +
'        .btn { width: 100%; padding: 16px; border: none; border-radius: 12px; font-size: 1rem; font-weight: 600; cursor: pointer; transition: all 0.3s; display: flex; align-items: center; justify-content: center; gap: 10px; }' +
'        .btn-primary { background: #3b82f6; color: white; }' +
'        .btn-primary:hover:not(:disabled) { background: #2563eb; transform: translateY(-2px); box-shadow: 0 10px 20px rgba(59,130,246,0.3); }' +
'        .btn-success { background: #10b981; color: white; }' +
'        .btn:disabled { opacity: 0.6; cursor: not-allowed; }' +
'        .status-area { margin-top: 30px; padding: 25px; background: #f8fafc; border-radius: 16px; display: none; }' +
'        .status-area.active { display: block; }' +
'        .alert { padding: 15px; border-radius: 12px; margin-bottom: 20px; }' +
'        .alert-warning { background: #fffbeb; border: 2px solid #fde68a; color: #92400e; }' +
'        .alert-info { background: #f0f9ff; border: 2px solid #bae6fd; color: #0369a1; }' +
'        .alert-success { background: #f0fdf4; border: 2px solid #10b981; color: #065f46; }' +
'        .code-display { font-size: 2.5rem; font-weight: 800; letter-spacing: 10px; color: #3b82f6; margin: 20px 0; font-family: "Courier New", monospace; }' +
'        .progress-bar { height: 10px; background: #e2e8f0; border-radius: 5px; margin: 15px 0; overflow: hidden; }' +
'        .progress-fill { height: 100%; background: #3b82f6; width: 0%; transition: width 0.3s; }' +
'        .connection-status { display: flex; gap: 15px; margin: 20px 0; }' +
'        .status-item { flex: 1; padding: 15px; border-radius: 10px; text-align: center; border: 2px solid #e2e8f0; }' +
'        .status-item.active { border-color: #10b981; background: rgba(16,185,129,0.1); }' +
'        .status-item.inactive { border-color: #ef4444; background: rgba(239,68,68,0.1); }' +
'        .hidden { display: none !important; }' +
'        .instructions { margin-top: 30px; padding: 20px; background: #f8fafc; border-radius: 16px; text-align: left; font-size: 0.9rem; color: #64748b; }' +
'        @media (max-width: 480px) { .app-card { padding: 20px; } .code-display { font-size: 2rem; letter-spacing: 8px; } }' +
'    </style>' +
'</head>' +
'<body>' +
'    <div class="container">' +
'        <div class="app-card">' +
'            <div class="logo">' +
'                <i class="fas fa-bolt"></i>' +
'            </div>' +
'            <h1>即时文件传输</h1>' +
'            <p class="subtitle">安全快速的P2P文件传输</p>' +
'            ' +
'            <div class="mode-selector">' +
'                <button class="mode-btn active" id="senderModeBtn">' +
'                    <i class="fas fa-cloud-upload-alt"></i>' +
'                    发送文件' +
'                </button>' +
'                <button class="mode-btn" id="receiverModeBtn">' +
'                    <i class="fas fa-cloud-download-alt"></i>' +
'                    接收文件' +
'                </button>' +
'            </div>' +
'            ' +
'            <!-- 发送端面板 -->' +
'            <div class="panel active" id="senderPanel">' +
'                <div class="form-group">' +
'                    <label>选择文件</label>' +
'                    <div class="file-upload" id="fileUpload">' +
'                        <i class="fas fa-cloud-upload-alt"></i>' +
'                        <div>点击或拖放文件</div>' +
'                        <input type="file" id="fileInput">' +
'                    </div>' +
'                    <div class="file-info" id="fileInfo">' +
'                        <div id="fileName">未选择文件</div>' +
'                        <div id="fileSize">0 KB</div>' +
'                    </div>' +
'                </div>' +
'                ' +
'                <button class="btn btn-primary" id="generateBtn" disabled>' +
'                    <i class="fas fa-barcode"></i>' +
'                    生成取件码' +
'                </button>' +
'                ' +
'                <div class="status-area" id="senderStatusArea">' +
'                    <div class="alert alert-warning">' +
'                        <i class="fas fa-exclamation-triangle"></i>' +
'                        请不要关闭页面或刷新' +
'                    </div>' +
'                    ' +
'                    <div class="code-display" id="codeDisplay">ABCDEF</div>' +
'                    ' +
'                    <div class="connection-status">' +
'                        <div class="status-item active" id="senderStatusItem">' +
'                            发送方：<span>在线</span>' +
'                        </div>' +
'                        <div class="status-item inactive" id="receiverStatusItem">' +
'                            接收方：<span>等待连接</span>' +
'                        </div>' +
'                    </div>' +
'                    ' +
'                    <div class="progress-bar hidden" id="senderProgress">' +
'                        <div class="progress-fill" id="senderProgressFill"></div>' +
'                    </div>' +
'                    ' +
'                    <button class="btn" id="cancelBtn">' +
'                        <i class="fas fa-times"></i>' +
'                        取消' +
'                    </button>' +
'                </div>' +
'            </div>' +
'            ' +
'            <!-- 接收端面板 -->' +
'            <div class="panel" id="receiverPanel">' +
'                <div class="form-group">' +
'                    <label>输入取件码</label>' +
'                    <input type="text" class="code-input" id="codeInput" placeholder="ABCDEF" maxlength="6">' +
'                </div>' +
'                ' +
'                <button class="btn btn-success" id="connectBtn">' +
'                    <i class="fas fa-plug"></i>' +
'                    连接房间' +
'                </button>' +
'                ' +
'                <div class="status-area" id="receiverStatusArea">' +
'                    <div class="alert alert-info">' +
'                        <i class="fas fa-info-circle"></i>' +
'                        正在连接到发送方...' +
'                    </div>' +
'                    ' +
'                    <div class="connection-status">' +
'                        <div class="status-item inactive" id="remoteSenderStatus">' +
'                            发送方：<span>离线</span>' +
'                        </div>' +
'                        <div class="status-item active" id="selfStatus">' +
'                            接收方：<span>连接中</span>' +
'                        </div>' +
'                    </div>' +
'                    ' +
'                    <div class="progress-bar">' +
'                        <div class="progress-fill" id="receiverProgressFill"></div>' +
'                    </div>' +
'                    ' +
'                    <a class="btn btn-success hidden" id="downloadBtn" download>' +
'                        <i class="fas fa-download"></i>' +
'                        下载文件' +
'                    </a>' +
'                    ' +
'                    <button class="btn" id="disconnectBtn">' +
'                        <i class="fas fa-times"></i>' +
'                        断开连接' +
'                    </button>' +
'                </div>' +
'            </div>' +
'            ' +
'            <div class="instructions">' +
'                <p><strong>使用说明：</strong></p>' +
'                <p>1. 发送方选择文件并生成取件码</p>' +
'                <p>2. 将取件码告知接收方</p>' +
'                <p>3. 接收方输入取件码连接房间</p>' +
'                <p>4. 传输期间请勿关闭页面</p>' +
'                <p>5. 文件直接传输，服务器不存储</p>' +
'            </div>' +
'        </div>' +
'    </div>' +
'    ' +
'    <script>' +
'        const API_BASE = "/api";' +
'        const CHUNK_SIZE = 64 * 1024;' +
'        ' +
'        let currentMode = "sender";' +
'        let currentRoomCode = null;' +
'        let currentFile = null;' +
'        let senderSocket = null;' +
'        let receiverSocket = null;' +
'        let fileChunks = [];' +
'        let fileMetadata = null;' +
'        let transferStartTime = null;' +
'        ' +
'        // DOM元素' +
'        const senderModeBtn = document.getElementById("senderModeBtn");' +
'        const receiverModeBtn = document.getElementById("receiverModeBtn");' +
'        const senderPanel = document.getElementById("senderPanel");' +
'        const receiverPanel = document.getElementById("receiverPanel");' +
'        const fileInput = document.getElementById("fileInput");' +
'        const fileInfo = document.getElementById("fileInfo");' +
'        const fileName = document.getElementById("fileName");' +
'        const fileSize = document.getElementById("fileSize");' +
'        const generateBtn = document.getElementById("generateBtn");' +
'        const senderStatusArea = document.getElementById("senderStatusArea");' +
'        const codeDisplay = document.getElementById("codeDisplay");' +
'        const senderProgress = document.getElementById("senderProgress");' +
'        const senderProgressFill = document.getElementById("senderProgressFill");' +
'        const cancelBtn = document.getElementById("cancelBtn");' +
'        const codeInput = document.getElementById("codeInput");' +
'        const connectBtn = document.getElementById("connectBtn");' +
'        const receiverStatusArea = document.getElementById("receiverStatusArea");' +
'        const receiverProgressFill = document.getElementById("receiverProgressFill");' +
'        const downloadBtn = document.getElementById("downloadBtn");' +
'        const disconnectBtn = document.getElementById("disconnectBtn");' +
'        ' +
'        // 工具函数' +
'        function formatBytes(bytes) {' +
'            if (bytes === 0) return "0 Bytes";' +
'            const k = 1024;' +
'            const sizes = ["Bytes", "KB", "MB", "GB"];' +
'            const i = Math.floor(Math.log(bytes) / Math.log(k));' +
'            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];' +
'        }' +
'        ' +
'        function showMessage(message, type) {' +
'            const alert = document.createElement("div");' +
'            alert.className = "alert alert-" + type;' +
'            alert.innerHTML = \'<i class="fas fa-info-circle"></i> \' + message;' +
'            ' +
'            document.querySelector(".app-card").insertBefore(alert, document.querySelector(".instructions"));' +
'            ' +
'            setTimeout(function() {' +
'                alert.style.opacity = "0";' +
'                setTimeout(function() { alert.remove(); }, 300);' +
'            }, 3000);' +
'        }' +
'        ' +
'        function generateRoomCode() {' +
'            const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";' +
'            let code = "";' +
'            for (let i = 0; i < 6; i++) {' +
'                code += chars.charAt(Math.floor(Math.random() * chars.length));' +
'            }' +
'            return code;' +
'        }' +
'        ' +
'        function updateConnectionStatus(element, connected) {' +
'            element.classList.remove("active", "inactive");' +
'            element.classList.add(connected ? "active" : "inactive");' +
'        }' +
'        ' +
'        // 模式切换' +
'        senderModeBtn.addEventListener("click", function() {' +
'            currentMode = "sender";' +
'            senderModeBtn.classList.add("active");' +
'            receiverModeBtn.classList.remove("active");' +
'            senderPanel.classList.add("active");' +
'            receiverPanel.classList.remove("active");' +
'        });' +
'        ' +
'        receiverModeBtn.addEventListener("click", function() {' +
'            currentMode = "receiver";' +
'            receiverModeBtn.classList.add("active");' +
'            senderModeBtn.classList.remove("active");' +
'            receiverPanel.classList.add("active");' +
'            senderPanel.classList.remove("active");' +
'        });' +
'        ' +
'        // 文件选择' +
'        fileInput.addEventListener("change", function(e) {' +
'            if (e.target.files.length > 0) {' +
'                currentFile = e.target.files[0];' +
'                fileName.textContent = currentFile.name;' +
'                fileSize.textContent = formatBytes(currentFile.size);' +
'                fileInfo.style.display = "block";' +
'                generateBtn.disabled = false;' +
'            }' +
'        });' +
'        ' +
'        // 创建房间' +
'        generateBtn.addEventListener("click", async function() {' +
'            if (!currentFile) {' +
'                showMessage("请先选择文件", "warning");' +
'                return;' +
'            }' +
'            ' +
'            generateBtn.disabled = true;' +
'            generateBtn.innerHTML = \'<i class="fas fa-spinner fa-spin"></i> 创建中...\';' +
'            ' +
'            currentRoomCode = generateRoomCode();' +
'            ' +
'            try {' +
'                const response = await fetch(API_BASE + "/room/create", {' +
'                    method: "POST",' +
'                    headers: { "Content-Type": "application/json" },' +
'                    body: JSON.stringify({ code: currentRoomCode })' +
'                });' +
'                ' +
'                const result = await response.json();' +
'                ' +
'                if (!result.success) {' +
'                    throw new Error(result.error);' +
'                }' +
'                ' +
'                codeDisplay.textContent = currentRoomCode;' +
'                senderStatusArea.classList.add("active");' +
'                ' +
'                // 连接WebSocket' +
'                connectAsSender(result.wsUrl);' +
'                ' +
'                showMessage("房间创建成功！", "info");' +
'                ' +
'            } catch (error) {' +
'                showMessage("创建失败: " + error.message, "warning");' +
'                generateBtn.disabled = false;' +
'                generateBtn.innerHTML = \'<i class="fas fa-barcode"></i> 生成取件码\';' +
'            }' +
'        });' +
'        ' +
'        // 发送方WebSocket连接' +
'        function connectAsSender(wsUrl) {' +
'            senderSocket = new WebSocket(wsUrl + "?role=sender");' +
'            ' +
'            senderSocket.onopen = function() {' +
'                console.log("发送方已连接");' +
'                updateConnectionStatus(document.getElementById("senderStatusItem"), true);' +
'                ' +
'                // 发送准备消息' +
'                senderSocket.send(JSON.stringify({ type: "ready" }));' +
'                ' +
'                // 启动心跳' +
'                setInterval(function() {' +
'                    if (senderSocket.readyState === WebSocket.OPEN) {' +
'                        senderSocket.send(JSON.stringify({ type: "heartbeat", timestamp: Date.now() }));' +
'                    }' +
'                }, 15000);' +
'            };' +
'            ' +
'            senderSocket.onmessage = function(event) {' +
'                try {' +
'                    if (typeof event.data === "string") {' +
'                        const data = JSON.parse(event.data);' +
'                        ' +
'                        if (data.type === "receiver-connected") {' +
'                            updateConnectionStatus(document.getElementById("receiverStatusItem"), true);' +
'                            showMessage("接收方已连接，开始传输", "success");' +
'                            sendFile();' +
'                        } else if (data.type === "receiver-disconnected") {' +
'                            updateConnectionStatus(document.getElementById("receiverStatusItem"), false);' +
'                        } else if (data.type === "chunk-ack") {' +
'                            // 块确认' +
'                        }' +
'                    }' +
'                } catch (error) {' +
'                    console.error("解析消息错误:", error);' +
'                }' +
'            };' +
'            ' +
'            senderSocket.onclose = function() {' +
'                console.log("发送方连接关闭");' +
'                updateConnectionStatus(document.getElementById("receiverStatusItem"), false);' +
'            };' +
'        }' +
'        ' +
'        // 发送文件' +
'        function sendFile() {' +
'            if (!currentFile) return;' +
'            ' +
'            senderProgress.classList.remove("hidden");' +
'            transferStartTime = Date.now();' +
'            ' +
'            // 发送文件元数据' +
'            senderSocket.send(JSON.stringify({' +
'                type: "file-metadata",' +
'                metadata: {' +
'                    name: currentFile.name,' +
'                    size: currentFile.size,' +
'                    type: currentFile.type,' +
'                    totalChunks: Math.ceil(currentFile.size / CHUNK_SIZE)' +
'                }' +
'            }));' +
'            ' +
'            // 分块发送' +
'            const totalChunks = Math.ceil(currentFile.size / CHUNK_SIZE);' +
'            let offset = 0;' +
'            let chunkIndex = 0;' +
'            ' +
'            function readNext() {' +
'                if (chunkIndex >= totalChunks) {' +
'                    // 发送完成' +
'                    senderSocket.send(JSON.stringify({ type: "transfer-complete" }));' +
'                    return;' +
'                }' +
'                ' +
'                const chunk = currentFile.slice(offset, offset + CHUNK_SIZE);' +
'                const reader = new FileReader();' +
'                ' +
'                reader.onload = function(e) {' +
'                    if (senderSocket.readyState === WebSocket.OPEN) {' +
'                        // 发送二进制数据' +
'                        senderSocket.send(e.target.result);' +
'                        ' +
'                        // 更新进度' +
'                        offset += e.target.result.byteLength;' +
'                        chunkIndex++;' +
'                        const progress = Math.round((offset / currentFile.size) * 100);' +
'                        senderProgressFill.style.width = progress + "%";' +
'                        ' +
'                        // 发送进度更新' +
'                        if (chunkIndex % 10 === 0 || chunkIndex === totalChunks) {' +
'                            senderSocket.send(JSON.stringify({' +
'                                type: "transfer-progress",' +
'                                progress: progress,' +
'                                bytesTransferred: offset,' +
'                                chunksSent: chunkIndex' +
'                            }));' +
'                        }' +
'                        ' +
'                        // 发送下一个块' +
'                        setTimeout(readNext, 0);' +
'                    }' +
'                };' +
'                ' +
'                reader.readAsArrayBuffer(chunk);' +
'            }' +
'            ' +
'            readNext();' +
'        }' +
'        ' +
'        // 接收方' +
'        codeInput.addEventListener("input", function() {' +
'            this.value = this.value.toUpperCase().replace(/[^A-Z0-9]/g, "");' +
'        });' +
'        ' +
'        connectBtn.addEventListener("click", async function() {' +
'            const code = codeInput.value.trim();' +
'            ' +
'            if (code.length !== 6) {' +
'                showMessage("请输入6位取件码", "warning");' +
'                return;' +
'            }' +
'            ' +
'            connectBtn.disabled = true;' +
'            connectBtn.innerHTML = \'<i class="fas fa-spinner fa-spin"></i> 连接中...\';' +
'            ' +
'            try {' +
'                const response = await fetch(API_BASE + "/room/" + code);' +
'                const result = await response.json();' +
'                ' +
'                if (!result.success) {' +
'                    throw new Error(result.error);' +
'                }' +
'                ' +
'                // 连接WebSocket' +
'                connectAsReceiver(result.wsUrls.receiver);' +
'                ' +
'                receiverStatusArea.classList.add("active");' +
'                showMessage("连接成功！", "info");' +
'                ' +
'            } catch (error) {' +
'                showMessage("连接失败: " + error.message, "warning");' +
'                connectBtn.disabled = false;' +
'                connectBtn.innerHTML = \'<i class="fas fa-plug"></i> 连接房间\';' +
'            }' +
'        });' +
'        ' +
'        // 接收方WebSocket连接' +
'        function connectAsReceiver(wsUrl) {' +
'            receiverSocket = new WebSocket(wsUrl);' +
'            ' +
'            receiverSocket.onopen = function() {' +
'                console.log("接收方已连接");' +
'                updateConnectionStatus(document.getElementById("selfStatus"), true);' +
'                ' +
'                // 发送准备消息' +
'                receiverSocket.send(JSON.stringify({ type: "ready" }));' +
'            };' +
'            ' +
'            receiverSocket.onmessage = function(event) {' +
'                try {' +
'                    if (typeof event.data === "string") {' +
'                        const data = JSON.parse(event.data);' +
'                        ' +
'                        if (data.type === "sender-connected") {' +
'                            updateConnectionStatus(document.getElementById("remoteSenderStatus"), true);' +
'                        } else if (data.type === "sender-disconnected") {' +
'                            updateConnectionStatus(document.getElementById("remoteSenderStatus"), false);' +
'                        } else if (data.type === "file-metadata") {' +
'                            fileMetadata = data.metadata;' +
'                            showMessage("开始接收文件: " + data.metadata.name, "info");' +
'                            fileChunks = new Array(data.metadata.totalChunks);' +
'                        } else if (data.type === "transfer-progress") {' +
'                            receiverProgressFill.style.width = data.progress + "%";' +
'                        } else if (data.type === "transfer-complete") {' +
'                            // 合并文件' +
'                            const blob = new Blob(fileChunks);' +
'                            const url = URL.createObjectURL(blob);' +
'                            downloadBtn.href = url;' +
'                            downloadBtn.download = fileMetadata ? fileMetadata.name : "file";' +
'                            downloadBtn.classList.remove("hidden");' +
'                            showMessage("文件接收完成！", "success");' +
'                        }' +
'                    } else if (event.data instanceof ArrayBuffer) {' +
'                        // 存储二进制数据' +
'                        for (let i = 0; i < fileChunks.length; i++) {' +
'                            if (!fileChunks[i]) {' +
'                                fileChunks[i] = event.data;' +
'                                break;' +
'                            }' +
'                        }' +
'                        ' +
'                        // 发送确认' +
'                        receiverSocket.send(JSON.stringify({ type: "chunk-ack" }));' +
'                    }' +
'                } catch (error) {' +
'                    console.error("接收消息错误:", error);' +
'                }' +
'            };' +
'            ' +
'            receiverSocket.onclose = function() {' +
'                console.log("接收方连接关闭");' +
'            };' +
'        }' +
'        ' +
'        // 取消操作' +
'        cancelBtn.addEventListener("click", function() {' +
'            if (senderSocket) senderSocket.close();' +
'            senderStatusArea.classList.remove("active");' +
'            generateBtn.disabled = false;' +
'            generateBtn.innerHTML = \'<i class="fas fa-barcode"></i> 生成取件码\';' +
'        });' +
'        ' +
'        disconnectBtn.addEventListener("click", function() {' +
'            if (receiverSocket) receiverSocket.close();' +
'            receiverStatusArea.classList.remove("active");' +
'            connectBtn.disabled = false;' +
'            connectBtn.innerHTML = \'<i class="fas fa-plug"></i> 连接房间\';' +
'        });' +
'        ' +
'        // 页面关闭警告' +
'        window.addEventListener("beforeunload", function(e) {' +
'            if (senderSocket || receiverSocket) {' +
'                e.preventDefault();' +
'                e.returnValue = "文件传输中，确定离开？";' +
'                return e.returnValue;' +
'            }' +
'        });' +
'        ' +
'        // 健康检查' +
'        fetch(API_BASE + "/health")' +
'            .then(function(res) { return res.json(); })' +
'            .then(function(data) { console.log("服务状态:", data.status); })' +
'            .catch(function(err) { console.warn("健康检查失败:", err); });' +
'    </script>' +
'</body>' +
'</html>';