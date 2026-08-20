import { WebSocket, createWebSocketStream } from 'ws';
import net from 'net';

// 从环境变量获取 UUID，若未配置则使用默认回退值
const USER_ID = (process.env.UUID || 'de04edb5-1200-4113-a7f5-4f400afe8230').toLowerCase().replace(/-/g, '');

export default function handler(req, res) {
  if (req.headers.upgrade?.toLowerCase() !== 'websocket') {
    res.status(200).send('Server is running smoothly.');
    return;
  }
  // 如果托管在支持直接处理 WebSocket 升级的环境中
  res.status(400).send('WebSocket upgrade required.');
}

// 核心 WebSocket 连接与 VLESS 协议解析
export function handleWebSocket(ws) {
  let remoteSocket = null;

  ws.on('message', (chunk) => {
    if (remoteSocket) {
      remoteSocket.write(chunk);
      return;
    }

    // 校验 VLESS 报文头部（前 17 字节包含 Version 和 UUID）
    if (chunk.length < 18) return ws.close();

    const clientUUID = chunk.slice(1, 17).toString('hex');
    if (clientUUID !== USER_ID) {
      return ws.close();
    }

    // 解析目标地址与端口
    const optCode = chunk[17];
    const port = chunk.readUInt16BE(18 + optCode);
    const ATYP = chunk[20 + optCode];
    let host = '';
    let offset = 21 + optCode;

    if (ATYP === 1) { // IPv4
      host = chunk.slice(offset, offset + 4).join('.');
      offset += 4;
    } else if (ATYP === 2) { // 域名
      const domainLen = chunk[offset];
      host = chunk.slice(offset + 1, offset + 1 + domainLen).toString();
      offset += 1 + domainLen;
    } else if (ATYP === 3) { // IPv6
      host = chunk.slice(offset, offset + 16).reduce((s, b, i) => s + (i % 2 === 0 && i ? ':' : '') + b.toString(16).padStart(2, '0'), '');
      offset += 16;
    }

    const rawData = chunk.slice(offset);

    // 建立 TCP 目标 socket 连接
    remoteSocket = net.connect({ host, port }, () => {
      // 响应 VLESS 建立成功 (Version 0 + AddrLen 0)
      ws.send(Buffer.from([chunk[0], 0]));
      if (rawData.length > 0) remoteSocket.write(rawData);
    });

    remoteSocket.on('data', (data) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(data);
    });

    remoteSocket.on('error', () => cleanup());
    remoteSocket.on('close', () => ws.close());
  });

  const cleanup = () => {
    if (remoteSocket) {
      remoteSocket.destroy();
      remoteSocket = null;
    }
  };

  ws.on('close', cleanup);
  ws.on('error', cleanup);
}
