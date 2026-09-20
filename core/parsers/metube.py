import asyncio
import re
from pathlib import Path
from typing import ClassVar
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from aiohttp import ClientError, ClientSession, ClientTimeout

from astrbot.api import logger

from ..config import PluginConfig
from ..download import Downloader, VideoInfo
from ..exception import DownloadException
from ..utils import generate_file_name
from .base import BaseParser, Platform, handle


class MetubeParser(BaseParser):
    """通过 Metube 服务下载 YouTube 视频的解析器

    流程：提交链接给 Metube -> 轮询任务状态 -> 从 Metube 取回视频文件。
    链接匹配与油管解析器相同，本解析器注册顺序在其之后，
    启用后会在 parser_map 中覆盖油管解析器，接管视频链接的解析。
    """

    # 轮询 Metube 任务状态的间隔（秒）
    POLL_INTERVAL: ClassVar[float] = 3.0

    # 从 YouTube 链接中提取视频 ID（Metube 的任务 ID 即视频 ID）
    VIDEO_ID_RE: ClassVar[re.Pattern[str]] = re.compile(
        r"(?:v=|youtu\.be/|shorts/|embed/|live/)([A-Za-z\d_-]{11})"
    )

    # 平台信息
    platform: ClassVar[Platform] = Platform(name="metube", display_name="油管(Metube)")

    def __init__(self, config: PluginConfig, downloader: Downloader):
        super().__init__(config, downloader)
        self.mycfg = config.parser.metube
        self.headers.update({"Referer": "https://www.youtube.com/"})
        self._api: ClientSession | None = None

    # ---------------- Metube API ----------------

    @property
    def api_base(self) -> str:
        """Metube 服务地址（去除尾部斜杠）。API 通常位于内网，不走代理"""
        return (self.mycfg.metube_url or "http://192.168.1.202:8081").rstrip("/")

    @property
    def api(self) -> ClientSession:
        """访问 Metube API 的独立会话，与代理无关"""
        if self._api is None or self._api.closed:
            self._api = ClientSession(
                timeout=ClientTimeout(total=self.cfg.common_timeout)
            )
        return self._api

    async def close_session(self) -> None:
        if self._api and not self._api.closed:
            await self._api.close()
            self._api = None
        await super().close_session()

    async def _request_json(self, method: str, path: str, **kwargs) -> dict:
        """请求 Metube API 并解析 JSON 响应"""
        async with self.api.request(
            method, f"{self.api_base}{path}", **kwargs
        ) as resp:
            if resp.status >= 400:
                detail = (await resp.text())[:200]
                raise ClientError(f"HTTP {resp.status} {resp.reason} {detail}")
            return await resp.json(content_type=None)

    @staticmethod
    def _is_target(item: dict, submit_url: str, video_id: str | None) -> bool:
        """判断 history 条目是否为目标任务"""
        return item.get("url") == submit_url or (
            bool(video_id) and item.get("id") == video_id
        )

    async def _find_history_entry(
        self, submit_url: str, video_id: str | None
    ) -> dict | None:
        """在 /history 中查找本视频的最新任务条目，找不到返回 None"""
        history = await self._request_json("GET", "/history")
        candidates = [
            item
            for group in ("done", "queue", "pending")
            for item in history.get(group) or []
            if self._is_target(item, submit_url, video_id)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda i: i.get("timestamp") or 0)

    async def _wait_finished(self, submit_url: str, video_id: str | None) -> dict:
        """轮询 /history 直到任务真正完成

        Metube 的 /add 不回传任务对象，用 视频ID/URL 匹配历史条目
        （done/queue 队列以规范化 URL 为键）。注意：
        status=finished 会为每个下载流各触发一次（如 .f133.mp4 纯视频流），
        条目从 queue 移入 done 才代表合并/后处理全部结束。
        """
        timeout = self.mycfg.wait_timeout or 600
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            history = await self._request_json("GET", "/history")
            live = next(
                (
                    item
                    for group in ("queue", "pending")
                    for item in history.get(group) or []
                    if self._is_target(item, submit_url, video_id)
                ),
                None,
            )
            if live is not None:
                if live.get("status") == "error":
                    raise DownloadException(
                        f"Metube 下载失败: "
                        f"{live.get('msg') or live.get('error') or '未知错误'}"
                    )
            else:
                # done 按 URL 键存储，新任务完成会覆盖旧条目，
                # 故此处条目必为最近一次尝试的最终状态
                entry = next(
                    (
                        item
                        for item in history.get("done") or []
                        if self._is_target(item, submit_url, video_id)
                    ),
                    None,
                )
                if entry is None:
                    raise DownloadException("Metube 任务不存在或已被删除")
                if entry.get("status") == "error":
                    raise DownloadException(
                        f"Metube 下载失败: "
                        f"{entry.get('msg') or entry.get('error') or '未知错误'}"
                    )
                if entry.get("filename"):
                    return entry
            if asyncio.get_running_loop().time() >= deadline:
                raise DownloadException(f"等待 Metube 下载超时({timeout}秒)")
            await asyncio.sleep(self.POLL_INTERVAL)

    async def _delete_download(self, download_id: str, where: str) -> None:
        """删除 Metube 下载记录（尽力而为）"""
        try:
            await self._request_json(
                "POST", "/delete", json={"ids": [download_id], "where": where}
            )
        except (ClientError, TimeoutError) as e:
            logger.warning(f"[metube] 删除记录 {download_id} 失败: {e}")

    async def _download_via_metube(self, url: str) -> Path:
        """提交链接给 Metube，等待完成后把视频取回到缓存目录"""
        submit_url = self._strip_timestamp_param(url)
        payload = {
            "url": submit_url,
            "download_type": "video",
            "quality": self.mycfg.video_quality or "720",
            "codec": self.mycfg.video_codec or "h264",
            "format": self.mycfg.video_format or "mp4",
            "auto_start": True,
        }
        try:
            resp = await self._request_json("POST", "/add", json=payload)
        except (ClientError, TimeoutError) as e:
            raise DownloadException(f"提交 Metube 下载失败, 服务不可达: {e}") from e
        if resp.get("status") != "ok":
            raise DownloadException(f"Metube 添加任务失败: {resp.get('msg') or resp}")
        video_id = (
            match.group(1) if (match := self.VIDEO_ID_RE.search(submit_url)) else None
        )
        logger.info(f"[metube] 已提交下载任务: {video_id or submit_url} | {url}")

        try:
            finished = await self._wait_finished(submit_url, video_id)
            filename = finished.get("filename")
            if not filename:
                raise DownloadException("Metube 未返回下载文件名")
            # Metube 完成文件的静态端点，流式下载落盘（受 source_max_size 限制）
            file_url = f"{self.api_base}/download/{quote(str(filename))}"
            file_name = generate_file_name(file_url, ".mp4")
            video_path = await self.downloader.streamd(
                file_url, file_name=file_name, proxy=None
            )
        except (DownloadException, ClientError, TimeoutError):
            # 取消仍在排队的任务，避免孤儿下载。
            # done/queue 队列以规范化 URL 为键，必须从条目取 url，不能用视频ID
            try:
                entry = await self._find_history_entry(submit_url, video_id)
            except (ClientError, TimeoutError):
                entry = None
            if entry and entry.get("url"):
                await self._delete_download(entry["url"], "queue")
            raise
        # 拉取成功后按需清理记录，DELETE_FILE_ON_TRASHCAN=true 时会同时删除服务端文件
        if self.mycfg.delete_after_fetch and finished.get("url"):
            await self._delete_download(finished["url"], "done")
        return video_path

    @staticmethod
    def _strip_timestamp_param(url: str) -> str:
        """去掉 t= 时间戳参数，避免 Metube 将其解析为裁剪起点"""
        try:
            parts = urlsplit(url)
            pairs = parse_qsl(parts.query, keep_blank_values=True)
            if not any(k == "t" for k, _ in pairs):
                return url
            query = urlencode([(k, v) for k, v in pairs if k != "t"])
            return urlunsplit(parts._replace(query=query))
        except ValueError:
            return url

    # ---------------- 解析 ----------------

    @handle("youtu", r"youtu\.be/[A-Za-z\d\._\?%&\+\-=/#]+")
    @handle(
        "youtube",
        r"youtube\.com/(?:watch|shorts)(?:/[A-Za-z\d_\-]+|\?v=[A-Za-z\d_\-]+)",
    )
    async def _parse_video(self, searched: re.Match[str]):
        return await self.parse_video(searched)

    async def parse_video(self, searched: re.Match[str]):
        # 从匹配对象中获取原始URL
        url = searched.group(0)

        # 元信息尽力获取，失败时完全交由 Metube 处理
        video_info = await self._fetch_video_info(url)
        author = (
            self.create_author(video_info.channel)
            if video_info and video_info.channel
            else None
        )

        # 与油管解析器保持一致：超长视频只发封面，不提交 Metube
        if video_info and video_info.duration > self.cfg.max_duration:
            return self.result(
                title=video_info.title,
                author=author,
                contents=self.create_image_contents([video_info.thumbnail]),
                timestamp=video_info.timestamp,
            )

        # 提交 Metube 并在后台等待下载完成
        video_task = asyncio.create_task(
            self._download_via_metube(url), name=f"metube | {url}"
        )
        contents = [
            self.create_video_content_by_task(
                video_task,
                cover_url=video_info.thumbnail if video_info else None,
                duration=video_info.duration if video_info else 0.0,
            )
        ]

        return self.result(
            title=video_info.title if video_info else None,
            author=author,
            contents=contents,
            timestamp=video_info.timestamp if video_info else None,
        )

    async def _fetch_video_info(self, url: str) -> VideoInfo | None:
        """尽力获取视频信息，失败不阻断 Metube 流程"""
        try:
            return await self.downloader.ytdlp_extract_info(
                url, headers=self.headers, proxy=self.proxy
            )
        except Exception as e:
            logger.warning(f"[metube] 获取视频信息失败, 将由 Metube 全权处理: {e}")
            return None
