const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

// 优先读取环境变量 UUID，未配置则使用默认值并去划线格式化
const RAW_UUID = process.env.UUID || 'de04edb5-1200-4113-a7f5-4f400afe8230';
const USER_ID = RAW_UUID.replace(/-/g, '').toLowerCase();

// 创建 HTTP 服务器
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('OK');
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end('<h1>Server is running smoothly.</h1>');
});

// 创建 WebSocket 服务实例
const wss = new WebSocketServer({ noServer: true });

// 监听 HTTP 协议 Upgrade 事件（处理 WebSocket 连接升级）
server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

// 处理 VLESS WebSocket 连接
wss.on('connection', (ws) => {
  let remoteSocket = null;

  ws.on('message', (chunk) => {
    // 建立 TCP 连接后直接转发数据包
    if (remoteSocket) {
      remoteSocket.write(chunk);
      return;
    }

    // 校验 VLESS 基础头部长度（最少 18 字节）
    if (chunk.length < 18) {
      ws.close();
      return;
    }

    // 提取客户端 UUID 并比对
    const clientUUID = chunk.slice(1, 17).toString('hex').toLowerCase();
    if (clientUUID !== USER_ID) {
      ws.close();
      return;
    }

    try {
      // 解析 VLESS 附加选项与目标信息
      const optCode = chunk[17];
      const port = chunk.readUInt16BE(18 + optCode);
      const ATYP = chunk[20 + optCode];
      let host = '';
      let offset = 21 + optCode;

      if (ATYP === 1) {
        // IPv4
        host = chunk.slice(offset, offset + 4).join('.');
        offset += 4;
      } else if (ATYP === 2) {
        // 域名
        const domainLen = chunk[offset];
        host = chunk.slice(offset + 1, offset + 1 + domainLen).toString();
        offset += 1 + domainLen;
      } else if (ATYP === 3) {
        // IPv6
        const ipv6Arr = [];
        for (let i = 0; i < 16; i += 2) {
          ipv6Arr.push(chunk.readUInt16BE(offset + i).toString(16));
        }
        host = ipv6Arr.join(':');
        offset += 16;
      }

      const rawData = chunk.slice(offset);

      // 与目标 IP/域名建立 TCP 连接
      remoteSocket = net.connect({ host, port }, () => {
        // 返回 VLESS 响应首部 (Version 0 + AddrLen 0)
        ws.send(Buffer.from([chunk[0], 0]));
        if (rawData.length > 0) {
          remoteSocket.write(rawData);
        }
      });

      // 目标服务器 -> WebSocket 数据转发
      remoteSocket.on('data', (data) => {
        if (ws.readyState === ws.OPEN) {
          ws.send(data);
        }
      });

      remoteSocket.on('error', () => cleanup());
      remoteSocket.on('close', () => ws.close());
    } catch (err) {
      cleanup();
      ws.close();
    }
  });

  const cleanup = () => {
    if (remoteSocket) {
      remoteSocket.destroy();
      remoteSocket = null;
    }
  };

  ws.on('close', cleanup);
  ws.on('error', cleanup);
});

// 导出 Serverless 入口函数以适配 Vercel
module.exports = (req, res) => {
  server.emit('request', req, res);
};

// 支持本地独立运行 (如 node api/index.js)
if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  server.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
  });
}
