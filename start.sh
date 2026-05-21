#!/bin/sh
# 启动 MCP Server + 云桥接
# 先启动 MCP Server，再启动桥接连接 xiaozhi.me

uvicorn music_mcp_server:app --host 0.0.0.0 --port "${PORT:-8080}" &
MCP_PID=$!

# 等待 MCP Server 启动
sleep 3

# 启动云桥接
python mcp_bridge.py &
BRIDGE_PID=$!

# 等待任意一个进程退出
wait -n $MCP_PID $BRIDGE_PID 2>/dev/null || wait $MCP_PID $BRIDGE_PID
