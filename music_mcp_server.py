"""
小智音乐 MCP Server - SSE 模式
基于 NeteaseCloudMusicApi，支持搜索、播放、歌词
部署到 Railway 后通过 SSE URL 接入小智Pro
"""

import json
import logging
from typing import Any

import httpx
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.routing import Mount, Route

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("music-mcp")

server = Server("xiaozhi-music")

# ========== 配置区 ==========
# 部署 NeteaseCloudMusicApi 到 Vercel 后，把地址填在这里
MUSIC_API_BASE = "https://netease-cloud-music-api-eta-ten-24.vercel.app"
DEFAULT_SEARCH_LIMIT = 10
# ========== 配置区结束 ==========

http_client = httpx.AsyncClient(timeout=30.0)


# ---------- 工具定义 ----------

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="search_music",
            description="搜索音乐歌曲，返回歌曲列表（歌名、歌手、ID）",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "搜索关键词，可以是歌名、歌手名或专辑名"
                    },
                    "limit": {
                        "type": "number",
                        "description": "返回结果数量，默认10",
                        "default": DEFAULT_SEARCH_LIMIT
                    }
                },
                "required": ["keyword"]
            }
        ),
        Tool(
            name="play_music",
            description="获取歌曲的播放链接，返回音频直链URL",
            inputSchema={
                "type": "object",
                "properties": {
                    "song_id": {
                        "type": "number",
                        "description": "歌曲ID，从search_music结果中获取"
                    }
                },
                "required": ["song_id"]
            }
        ),
        Tool(
            name="get_lyrics",
            description="获取歌曲的歌词，返回带时间戳的LRC格式歌词",
            inputSchema={
                "type": "object",
                "properties": {
                    "song_id": {
                        "type": "number",
                        "description": "歌曲ID，从search_music结果中获取"
                    }
                },
                "required": ["song_id"]
            }
        ),
        Tool(
            name="get_song_detail",
            description="获取歌曲详细信息，包括歌名、歌手、专辑、封面等",
            inputSchema={
                "type": "object",
                "properties": {
                    "song_id": {
                        "type": "number",
                        "description": "歌曲ID"
                    }
                },
                "required": ["song_id"]
            }
        ),
        Tool(
            name="search_and_play",
            description="一步搜索并播放：搜索歌曲，返回第一首歌的播放链接、歌词和详细信息",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "搜索关键词，歌名或歌手名"
                    }
                },
                "required": ["keyword"]
            }
        ),
    ]


# ---------- 工具实现 ----------

async def _search(keyword: str, limit: int = DEFAULT_SEARCH_LIMIT) -> list[dict]:
    """调用网易云音乐API搜索歌曲"""
    try:
        resp = await http_client.get(
            f"{MUSIC_API_BASE}/cloudsearch",
            params={"keywords": keyword, "limit": limit}
        )
        data = resp.json()
        songs = data.get("result", {}).get("songs", [])
        results = []
        for s in songs:
            artists = ", ".join(a["name"] for a in s.get("ar", []))
            results.append({
                "id": s["id"],
                "name": s["name"],
                "artist": artists,
                "album": s.get("al", {}).get("name", ""),
                "duration_ms": s.get("dt", 0),
            })
        return results
    except Exception as e:
        logger.error(f"搜索失败: {e}")
        return []


async def _get_url(song_id: int) -> str:
    """获取歌曲播放链接"""
    try:
        resp = await http_client.get(
            f"{MUSIC_API_BASE}/song/url/v1",
            params={"id": song_id, "level": "exhigh"}
        )
        data = resp.json()
        urls = data.get("data", [])
        if urls and urls[0].get("url"):
            return urls[0]["url"]
        return ""
    except Exception as e:
        logger.error(f"获取播放链接失败: {e}")
        return ""


async def _get_lyrics(song_id: int) -> str:
    """获取歌词"""
    try:
        resp = await http_client.get(
            f"{MUSIC_API_BASE}/lyric",
            params={"id": song_id}
        )
        data = resp.json()
        lrc = data.get("lrc", {}).get("lyric", "")
        return lrc if lrc else "未找到歌词"
    except Exception as e:
        logger.error(f"获取歌词失败: {e}")
        return "获取歌词失败"


async def _get_detail(song_id: int) -> dict:
    """获取歌曲详情"""
    try:
        resp = await http_client.get(
            f"{MUSIC_API_BASE}/song/detail",
            params={"ids": str(song_id)}
        )
        data = resp.json()
        songs = data.get("songs", [])
        if songs:
            s = songs[0]
            artists = ", ".join(a["name"] for a in s.get("ar", []))
            return {
                "id": s["id"],
                "name": s["name"],
                "artist": artists,
                "album": s.get("al", {}).get("name", ""),
                "cover": s.get("al", {}).get("picUrl", ""),
                "duration_ms": s.get("dt", 0),
            }
        return {}
    except Exception as e:
        logger.error(f"获取详情失败: {e}")
        return {}


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    if name == "search_music":
        keyword = arguments["keyword"]
        limit = arguments.get("limit", DEFAULT_SEARCH_LIMIT)
        results = await _search(keyword, limit)
        if not results:
            return [TextContent(type="text", text=f"未找到与\"{keyword}\"相关的歌曲")]
        text = f"搜索\"{keyword}\"的结果：\n"
        for i, s in enumerate(results, 1):
            duration_sec = s["duration_ms"] // 1000
            text += f"{i}. {s['name']} - {s['artist']} [{duration_sec//60}:{duration_sec%60:02d}] (ID:{s['id']})\n"
        return [TextContent(type="text", text=text)]

    elif name == "play_music":
        song_id = arguments["song_id"]
        url = await _get_url(song_id)
        if not url:
            return [TextContent(type="text", text="获取播放链接失败，可能是VIP歌曲或版权限制")]
        return [TextContent(type="text", text=url)]

    elif name == "get_lyrics":
        song_id = arguments["song_id"]
        lrc = await _get_lyrics(song_id)
        return [TextContent(type="text", text=lrc)]

    elif name == "get_song_detail":
        song_id = arguments["song_id"]
        detail = await _get_detail(song_id)
        if not detail:
            return [TextContent(type="text", text="未找到歌曲信息")]
        text = (
            f"歌曲：{detail['name']}\n"
            f"歌手：{detail['artist']}\n"
            f"专辑：{detail['album']}\n"
            f"封面：{detail['cover']}\n"
            f"时长：{detail['duration_ms']//1000}秒"
        )
        return [TextContent(type="text", text=text)]

    elif name == "search_and_play":
        keyword = arguments["keyword"]
        results = await _search(keyword, 5)
        if not results:
            return [TextContent(type="text", text=f"未找到与\"{keyword}\"相关的歌曲")]
        song = results[0]
        url = await _get_url(song["id"])
        lrc = await _get_lyrics(song["id"])
        if not url:
            return [TextContent(type="text", text=f"找到歌曲\"{song['name']} - {song['artist']}\"，但无法获取播放链接（可能需要VIP）")]
        duration_sec = song["duration_ms"] // 1000
        text = (
            f"正在播放：{song['name']} - {song['artist']}\n"
            f"时长：{duration_sec//60}:{duration_sec%60:02d}\n"
            f"播放链接：{url}\n"
            f"---歌词---\n{lrc}"
        )
        return [TextContent(type="text", text=text)]

    return [TextContent(type="text", text=f"未知工具：{name}")]


# ---------- SSE 服务启动 ----------

sse = SseServerTransport("/messages/")

async def handle_sse(request):
    async with sse.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await server.run(
            streams[0], streams[1], server.create_initialization_options()
        )

app = Starlette(
    routes=[
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ]
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
