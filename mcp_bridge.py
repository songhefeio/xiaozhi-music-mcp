"""
小智音乐 MCP 云桥接
连接 xiaozhi.me WebSocket 接入点 ↔ SSE MCP Server
部署在 Railway 上，7×24 小时运行，不需要开电脑
"""

import asyncio
import json
import logging
import os
import signal
import sys

import httpx
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("mcp-bridge")

# ========== 配置区 ==========
XIAOZHI_WS_URL = os.environ.get("XIAOZHI_WS_URL", "")
MCP_SSE_URL = os.environ.get("MCP_SSE_URL", "https://xiaozhi-music-mcp-production-d46c.up.railway.app/sse")
RECONNECT_DELAY = 5  # 断线重连间隔（秒）
# ========== 配置区结束 ==========


class McpSseClient:
    """通过 SSE + HTTP POST 与 MCP Server 通信"""

    def __init__(self, sse_url: str):
        self.sse_url = sse_url
        self.message_endpoint = None  # POST 消息的 URL
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.sse_task = None
        self.response_queue = asyncio.Queue()
        self._request_id_map = {}  # SSE 请求 ID → WebSocket 请求 ID

    async def connect(self):
        """连接 SSE 端点，获取 message endpoint"""
        logger.info(f"连接 MCP SSE: {self.sse_url}")

        # 启动 SSE 监听任务
        self.sse_task = asyncio.create_task(self._listen_sse())

        # 等待获取 message endpoint
        for _ in range(30):  # 最多等 30 秒
            if self.message_endpoint:
                break
            await asyncio.sleep(1)

        if not self.message_endpoint:
            raise RuntimeError("未能获取 MCP message endpoint")

        logger.info(f"MCP message endpoint: {self.message_endpoint}")

    async def _listen_sse(self):
        """监听 SSE 事件流"""
        try:
            async with self.http_client.stream("GET", self.sse_url) as resp:
                logger.info(f"SSE 连接状态: {resp.status_code}")
                event_type = None
                event_data = ""

                async for line in resp.aiter_lines():
                    if line.startswith("event:"):
                        event_type = line[6:].strip()
                    elif line.startswith("data:"):
                        event_data = line[5:].strip()
                    elif line == "":
                        # 空行表示事件结束
                        if event_type and event_data:
                            await self._handle_sse_event(event_type, event_data)
                        event_type = None
                        event_data = ""
        except Exception as e:
            logger.error(f"SSE 连接异常: {e}")

    async def _handle_sse_event(self, event_type: str, data: str):
        """处理 SSE 事件"""
        if event_type == "endpoint":
            # 服务器告知我们 POST 消息的 URL
            self.message_endpoint = data
            logger.info(f"收到 endpoint 事件: {data}")

        elif event_type == "message":
            # 服务器返回的 JSON-RPC 响应
            try:
                msg = json.loads(data)
                await self.response_queue.put(msg)
            except json.JSONDecodeError:
                logger.warning(f"无法解析 SSE 消息: {data}")

    async def send_request(self, request: dict) -> dict:
        """向 MCP Server 发送 JSON-RPC 请求"""
        if not self.message_endpoint:
            raise RuntimeError("MCP SSE 未连接（无 message endpoint）")

        logger.info(f"发送 MCP 请求: {request.get('method', '?')} (id={request.get('id', '?')})")

        resp = await self.http_client.post(
            self.message_endpoint,
            json=request,
            headers={"Content-Type": "application/json"},
        )

        if resp.status_code != 200 and resp.status_code != 202:
            logger.error(f"MCP POST 失败: {resp.status_code} {resp.text}")
            return {
                "jsonrpc": "2.0",
                "id": request.get("id"),
                "error": {"code": -32603, "message": f"MCP server error: {resp.status_code}"},
            }

        # 有些响应直接在 POST 返回中
        if resp.status_code == 200 and resp.text.strip():
            try:
                return resp.json()
            except json.JSONDecodeError:
                pass

        # 否则从 SSE 队列等待响应
        request_id = request.get("id")
        try:
            # 最多等 30 秒
            response = await asyncio.wait_for(self.response_queue.get(), timeout=30.0)
            return response
        except asyncio.TimeoutError:
            logger.error(f"MCP 请求超时 (id={request_id})")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": "MCP request timeout"},
            }

    async def close(self):
        if self.sse_task:
            self.sse_task.cancel()
        await self.http_client.aclose()


class XiaozhiBridge:
    """桥接 xiaozhi.me WebSocket 和 MCP SSE Server"""

    def __init__(self, ws_url: str, mcp_sse_url: str):
        self.ws_url = ws_url
        self.mcp_client = McpSseClient(mcp_sse_url)
        self.ws = None
        self._running = False

    async def run(self):
        """主循环：连接 WebSocket + MCP SSE，桥接消息"""
        self._running = True

        while self._running:
            try:
                # 1. 连接 MCP SSE
                await self.mcp_client.connect()

                # 2. 连接 xiaozhi WebSocket
                logger.info(f"连接 xiaozhi WebSocket: {self.ws_url[:50]}...")
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=30,
                    ping_timeout=10,
                ) as ws:
                    self.ws = ws
                    logger.info("✅ WebSocket 连接成功！桥接已启动")

                    # 3. 监听 WebSocket 消息并转发
                    async for message in ws:
                        await self._handle_ws_message(message)

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket 断开: {e}")
            except Exception as e:
                logger.error(f"桥接异常: {e}")
            finally:
                self.ws = None

            if self._running:
                logger.info(f"⏳ {RECONNECT_DELAY} 秒后重连...")
                await asyncio.sleep(RECONNECT_DELAY)
                # 重建 MCP SSE 客户端
                await self.mcp_client.close()
                self.mcp_client = McpSseClient(self.mcp_sse_url)

    async def _handle_ws_message(self, raw_message: str):
        """处理来自 xiaozhi.me 的 WebSocket 消息"""
        try:
            request = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.warning(f"无法解析 WebSocket 消息: {raw_message[:100]}")
            return

        method = request.get("method", "?")
        req_id = request.get("id")
        logger.info(f"◀ 收到 xiaozhi 请求: {method} (id={req_id})")

        # 处理 initialize 请求
        if method == "initialize":
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "serverInfo": {
                        "name": "xiaozhi-music-mcp",
                        "version": "1.0.0",
                    },
                },
            }
            await self._ws_send(response)
            return

        # 处理 ping 请求
        if method == "ping":
            response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
            await self._ws_send(response)
            return

        # 处理 notifications/initialized（通知，不需要响应）
        if method == "notifications/initialized":
            logger.info("收到 initialized 通知")
            return

        # 其他请求转发给 MCP Server
        try:
            response = await self.mcp_client.send_request(request)
            await self._ws_send(response)
        except Exception as e:
            logger.error(f"转发 MCP 请求失败: {e}")
            error_response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32603, "message": str(e)},
            }
            await self._ws_send(error_response)

    async def _ws_send(self, message: dict):
        """通过 WebSocket 发送消息"""
        if self.ws:
            text = json.dumps(message, ensure_ascii=False)
            logger.info(f"▶ 发送 xiaozhi 响应: {message.get('result', {}).get('tools', ['...']) if 'result' in message else message.get('error', '')}")
            await self.ws.send(text)

    async def stop(self):
        self._running = False
        await self.mcp_client.close()
        if self.ws:
            await self.ws.close()


async def main():
    if not XIAOZHI_WS_URL:
        logger.error("❌ 未设置 XIAOZHI_WS_URL 环境变量！")
        logger.error("请在 Railway 中设置环境变量 XIAOZHI_WS_URL=wss://api.xiaozhi.me/mcp/?token=...")
        sys.exit(1)

    bridge = XiaozhiBridge(XIAOZHI_WS_URL, MCP_SSE_URL)

    # 优雅退出
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(bridge.stop()))

    logger.info("🎵 小智音乐 MCP 云桥接启动")
    logger.info(f"  WebSocket: {XIAOZHI_WS_URL[:50]}...")
    logger.info(f"  MCP SSE:   {MCP_SSE_URL}")

    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
