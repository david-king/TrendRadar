# -*- coding: utf-8 -*-
"""
Minimal-intrusion custom sources adapter for TrendRadar.

提供：
- REST / RSS / HTML 三类数据源
- 统一输出结构：title, url, ts, rank, source, source_key, id
- 合并与去重（URL优先；可选择标题归一/模糊）
- 同步/异步友好：提供 fetch_custom_all_sync() 便于在 main.py 同步调用
"""

from __future__ import annotations
import asyncio
import time
import unicodedata
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx
from jsonpath_ng import parse as jp
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

import os, glob, yaml, pathlib
from typing import Tuple

# 可选：若不需要简繁转换，可删掉这段 try/except
try:
    from opencc import OpenCC
    _opencc = OpenCC('t2s')  # 把繁体转换为简体；需要反向可改 s2t
except Exception:
    _opencc = None

# 可选：模糊相似度（用于去重/模糊匹配）
try:
    from rapidfuzz import fuzz
except Exception:
    fuzz = None


# 环境变量优先，默认目录为 config/custom.d
CUSTOM_DIR_ENV = "TREND_CUSTOM_DIR"
DEFAULT_CUSTOM_DIR = "config/custom.d"


# ---------- 工具函数 ----------

def _load_yaml_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if isinstance(data, dict):
                return data
            # 允许文件直接是单个源对象（非 dict 时尝试包装）
            if isinstance(data, list):
                return {"custom_sources": data}
            return {}
    except Exception as e:
        print(f"[ERROR] 自定义源文件解析失败: {path}, 错误: {type(e).__name__} {e}")
        return {}

def _normalize_single_source(obj: dict, filename: str) -> Optional[dict]:
    """
    规范化一个数据源对象：
    - 必要字段: type, endpoint (rss/html 也需要 endpoint)
    - 自动补充 key（若缺失，按文件名+序号生成）
    - 支持 enabled: false
    """
    if not isinstance(obj, dict):
        return None
    if obj.get("enabled") is False:
        return None
    typ = (obj.get("type") or "").lower()
    if typ not in {"rest","rss","html"}:
        return None
    if not obj.get("endpoint"):
        return None
    # key
    key = obj.get("key")
    if not key:
        stem = pathlib.Path(filename).stem
        key = f"{typ}:{stem}"
        obj["key"] = key
    # name 兜底
    obj.setdefault("name", obj["key"])
    return obj

def load_custom_sources_from_dir(CONFIG: dict) -> Tuple[list, list]:
    """
    扫描目录，汇总全部自定义源。
    返回:
      sources: List[dict]      # 规范化后的源配置
      errors:  List[str]       # 简单错误信息（可用于日志）
    """
    custom_dir = os.getenv(CUSTOM_DIR_ENV, CONFIG.get("CUSTOM_DIR", DEFAULT_CUSTOM_DIR))
    patterns = [os.path.join(custom_dir, "*.yml"), os.path.join(custom_dir, "*.yaml")]
    files = sorted(f for p in patterns for f in glob.glob(p))
    sources, errors = [], []

    for fp in files:
        data = _load_yaml_file(fp)
        if "custom_sources" in data and isinstance(data["custom_sources"], list):
            items = data["custom_sources"]
        else:
            # 文件里直接是一个源对象
            items = [data]

        idx = 0
        for raw in items:
            idx += 1
            src = _normalize_single_source(raw, filename=os.path.basename(fp))
            if src is None:
                errors.append(f"skip_invalid:{fp}#{idx}")
                continue
            # 同 key 后来者覆盖（按文件名顺序）
            exist = {s["key"]: i for i, s in enumerate(sources)}
            if src["key"] in exist:
                sources[exist[src["key"]]] = src
            else:
                sources.append(src)
    return sources, errors

def get_all_custom_sources(CONFIG: dict) -> list:
    """
    统一汇总：目录中的源 + CONFIG['CUSTOM_SOURCES']（若存在）
    目录优先覆盖 CONFIG 内的同名 key。
    """
    dir_sources, _ = load_custom_sources_from_dir(CONFIG)
    cfg_sources = CONFIG.get("CUSTOM_SOURCES", [])
    merged = {s["key"]: s for s in cfg_sources if isinstance(s, dict) and s.get("key")}
    for s in dir_sources:
        merged[s["key"]] = s
    return list(merged.values())

def _now_ts() -> int:
    return int(time.time())

def normalize_text(s: str, use_opencc: bool = False) -> str:
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", s)  # 全/半角、兼容分解
    if use_opencc and _opencc:
        try:
            t = _opencc.convert(t)
        except Exception:
            pass
    return t.strip()

def parse_ts(v: Any) -> int:
    """
    支持：
      - int/float（秒）
      - ISO8601 字符串
      - 其它可被 dateutil 解析的时间字符串
    解析失败则返回当前时间
    """
    if v is None:
        return _now_ts()
    if isinstance(v, (int, float)):
        # 粗略判断是否是毫秒级
        return int(v // 1000) if v > 1e12 else int(v)
    if isinstance(v, str):
        try:
            return int(dtparser.parse(v).timestamp())
        except Exception:
            return _now_ts()
    return _now_ts()

def make_id(source_key: str, title: str, url: str) -> str:
    raw = f"{source_key}|{normalize_text(title)}|{url}".encode("utf-8", "ignore")
    return hashlib.md5(raw).hexdigest()

def _safe_get(d: Dict[str, Any], path: str, default=None):
    try:
        matches = jp(path).find(d)
        return matches[0].value if matches else default
    except Exception:
        return default


# ---------- 数据结构 ----------

@dataclass
class Item:
    title: str
    url: str
    ts: int
    rank: Optional[Any]
    source: str      # 展示名
    source_key: str  # 配置 key
    id: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "url": self.url,
            "ts": self.ts,
            "rank": self.rank,
            "source": self.source,
            "source_key": self.source_key,
            "id": self.id,
        }


# ---------- 基类与实现 ----------

class BaseSource:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.key = cfg.get("key") or cfg.get("name") or "custom"
        self.name = cfg.get("name", self.key)
        rl = cfg.get("rate_limit") or {}
        self.min_interval = 60.0 / rl.get("rpm", 0) if rl.get("rpm") else 0.0
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def _rate_limit(self):
        if self.min_interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()

    def _mk_item(self, title: str, url: str, ts: Any = None, rank: Any = None) -> Item:
        title = normalize_text(title, use_opencc=True)
        url = (url or "").strip()
        ts = parse_ts(ts)
        return Item(
            title=title,
            url=url,
            ts=ts,
            rank=rank,
            source=self.name,
            source_key=self.key,
            id=make_id(self.key, title, url),
        )

    async def fetch(self) -> List[Item]:
        raise NotImplementedError


class RestSource(BaseSource):
    async def fetch(self) -> List[Item]:
        await self._rate_limit()
        ep = self.cfg["endpoint"]
        method = self.cfg.get("method", "GET").upper()
        headers = self.cfg.get("headers", {})
        params = self.cfg.get("params", {})
        extract = self.cfg["extract"]  # {list, title, url, ts?, rank?}

        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.request(method, ep, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()

        items = []
        try:
            lst = [m.value for m in jp(extract["list"]).find(data)]
        except Exception:
            lst = []

        for it in lst:
            title = _safe_get(it, extract.get("title", ""), "")
            url = _safe_get(it, extract.get("url", ""), "")
            ts = _safe_get(it, extract.get("ts", ""), None)
            rank = _safe_get(it, extract.get("rank", ""), None)
            if not title or not url:
                continue
            items.append(self._mk_item(title, url, ts, rank))
        return items


class RssSource(BaseSource):
    async def fetch(self) -> List[Item]:
        await self._rate_limit()
        # feedparser 是同步库，用线程池跑即可；或直接同步调用（数据量小）
        feed = await asyncio.to_thread(feedparser.parse, self.cfg["endpoint"])
        items: List[Item] = []
        for e in feed.entries:
            title = e.get("title")
            url = e.get("link")
            if not title or not url:
                continue
            ts = 0
            if e.get("published_parsed"):
                ts = int(time.mktime(e.published_parsed))
            items.append(self._mk_item(title, url, ts))
        return items


class HtmlSource(BaseSource):
    async def fetch(self) -> List[Item]:
        await self._rate_limit()
        ep = self.cfg["endpoint"]
        hcfg = self.cfg.get("html", {})
        item_sel = hcfg["item"]                # 列表项 selector
        title_attr = hcfg.get("title_attr", "text")
        url_attr = hcfg.get("url_attr", "href")
        ts_selector = hcfg.get("ts_selector")
        ts_attr = hcfg.get("ts_attr", "datetime")

        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(ep)
            r.raise_for_status()
            html = r.text

        soup = BeautifulSoup(html, "html.parser")
        out: List[Item] = []
        for a in soup.select(item_sel):
            # 标题
            if title_attr == "text":
                title = a.get_text(strip=True)
            else:
                title = a.get(title_attr, "")

            # URL
            url = a.get(url_attr, "")
            if url and not url.startswith(("http://", "https://")):
                url = urljoin(ep, url)

            if not title or not url:
                continue

            # 时间（可选）
            ts_val = None
            if ts_selector:
                # 优先从 item 节点下查找时间
                tnode = a.select_one(ts_selector)
                # 若 item 下未找到，再从全局查找
                if not tnode:
                    tnode = soup.select_one(ts_selector)
                
                if tnode:
                    ts_val = tnode.get(ts_attr) or tnode.get_text(strip=True)

            out.append(self._mk_item(title, url, ts_val))
        return out


# ---------- 工厂与总调度 ----------

def _build_source(cfg: Dict[str, Any]) -> Optional[BaseSource]:
    typ = (cfg.get("type") or "").lower()
    if typ == "rest":
        return RestSource(cfg)
    if typ == "rss":
        return RssSource(cfg)
    if typ == "html":
        return HtmlSource(cfg)
    return None

async def fetch_custom_all(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    src_cfgs = config.get("custom_sources") or []
    tasks = []
    for sc in src_cfgs:
        s = _build_source(sc)
        if not s:
            continue
        tasks.append(s.fetch())

    results: List[List[Item]] = []
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)

    merged: List[Item] = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            # 获取出错的源 key
            key = src_cfgs[i].get("key", f"unknown_source_{i}")
            print(f"[ERROR] 自定义源抓取失败: key='{key}', 错误: {r}")
            continue
        merged.extend(r)

    return [x.to_dict() for x in merged]

def fetch_custom_all_sync(CONFIG: dict) -> List[Dict[str, Any]]:
    """
    读取目录与 CONFIG 的所有自定义源 -> 抓取 -> 返回统一结构的条目列表
    """
    all_sources = get_all_custom_sources(CONFIG)
    # 复用你已有的异步抓取逻辑
    # 如果你之前的 fetch_custom_all(config) 接收的是 {"custom_sources":[...]}，则：
    custom_cfg = {"custom_sources": all_sources}
    try:
        return asyncio.run(fetch_custom_all(custom_cfg))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(fetch_custom_all(custom_cfg))
        finally:
            loop.close()

# 将自定义源的 List[dict] → main.py 需要的 dict 结构
def custom_to_results(custom_items):
    """
    返回:
      c_results: { source_id: { title: { "ranks":[int...], "url": str, "mobileUrl": str } } }
      c_id_to_name: { source_id: display_name }
    说明:
      source_id 使用 'custom:source_key' 避免与内置平台冲突
      rank 若无则按出现顺序 1..n 填充
    """
    c_results, c_id_to_name = {}, {}
    for it in custom_items:
        sid = f"custom:{it.get('source_key','custom')}"
        sname = it.get('source') or sid
        c_id_to_name[sid] = sname
        c_results.setdefault(sid, {})
        title = (it.get('title') or '').strip()
        if not title:
            continue
        url = it.get('url') or ''
        rank = it.get('rank')
        entry = c_results[sid].setdefault(title, {"ranks": [], "url": url, "mobileUrl": ""})
        # 填 rank；如果没给 rank，则顺序自增
        entry["url"] = entry["url"] or url
        entry["ranks"].append(int(rank) if isinstance(rank, (int, float, str)) and str(rank).isdigit()
                              else len(entry["ranks"]) + 1)
    return c_results, c_id_to_name


# ---------- 合并与去重 ----------

def _title_key(title: str) -> str:
    return normalize_text(title, use_opencc=True)

def dedup_items(items: List[Dict[str, Any]],
                enable_fuzzy: bool = False,
                fuzzy_threshold: int = 90) -> List[Dict[str, Any]]:
    """
    去重策略：
      1) URL 完全一致 -> 视为同一条
      2) 标题归一后完全一致 -> 视为同一条
      3) （可选）标题模糊相似度 >= 阈值 -> 归并
    """
    seen_url = set()
    seen_title = {}
    out: List[Dict[str, Any]] = []

    for it in items:
        url = (it.get("url") or "").strip()
        if url and url in seen_url:
            continue

        tkey = _title_key(it.get("title") or "")
        if tkey and tkey in seen_title:
            continue

        if enable_fuzzy and fuzz:
            dup = False
            for tk in seen_title.keys():
                if fuzz.token_set_ratio(tkey, tk) >= fuzzy_threshold:
                    dup = True
                    break
            if dup:
                continue

        if url:
            seen_url.add(url)
        if tkey:
            seen_title[tkey] = 1
        out.append(it)
    return out


def merge_items(base_items: List[Dict[str, Any]],
                custom_items: List[Dict[str, Any]],
                enable_fuzzy: bool = False,
                fuzzy_threshold: int = 90) -> List[Dict[str, Any]]:
    return dedup_items(base_items + custom_items,
                       enable_fuzzy=enable_fuzzy,
                       fuzzy_threshold=fuzzy_threshold)
