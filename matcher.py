# -*- coding: utf-8 -*-
"""
高级匹配增强（独立文件）
- 规范化：NFKC，全/半角；可选简繁转换（OpenCC）
- 正则匹配：可把关键词写成正则
- 模糊匹配：RapidFuzz 相似度阈值
- 零依赖退化：没装 opencc/rapidfuzz 也能跑（走基础包含）
"""

from __future__ import annotations
import re, unicodedata, os, yaml
from typing import List, Dict

# 可选依赖：存在则用，不存在则自动降级
try:
    from opencc import OpenCC
    _opencc = OpenCC('t2s')  # 繁->简；如需简->繁改 's2t'
except Exception:
    _opencc = None

try:
    from rapidfuzz import fuzz
except Exception:
    fuzz = None


# ---------- 配置加载 ----------
def load_match_config(CONFIG: Dict) -> Dict:
    """
    优先使用 CONFIG['MATCH']；否则回退读取 config/config.yaml 的 match 段。
    支持环境变量关闭：ADV_MATCH=disable/off/false/0/none
    """
    # 环境变量关闭总开关
    sw = os.getenv("ADV_MATCH", "").strip().lower()
    if sw in {"disable", "disabled", "off", "false", "0", "none"}:
        return {"__disabled__": True}

    if isinstance(CONFIG, dict) and CONFIG.get("MATCH"):
        return CONFIG["MATCH"]

    # 回退：从主配置读取
    cfg_path = "config/config.yaml"
    if isinstance(CONFIG, dict) and CONFIG.get("CONFIG_PATH"):
        cfg_path = CONFIG["CONFIG_PATH"]

    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get("match", {}) or {}
    except Exception:
        return {}


# ---------- 文本规范化 ----------
def _norm(s: str, use_opencc=True, nfkc=True) -> str:
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", s) if nfkc else s
    if use_opencc and _opencc:
        try:
            t = _opencc.convert(t)
        except Exception:
            pass
    return t.strip()


# ---------- 基础与高级匹配 ----------
def _basic_match(title: str, any_words: List[str], must_words: List[str], not_words: List[str]) -> bool:
    return (all(m in title for m in (must_words or [])) and
            all(n not in title for n in (not_words or [])) and
            any(a in title for a in (any_words or [])))


def advanced_match(title: str,
                   any_words: List[str],
                   must_words: List[str],
                   not_words: List[str],
                   match_cfg: Dict) -> bool:
    """
    高级匹配主逻辑：规范化 + 正则(可选) + 模糊(可选) + 普通包含
    """
    norm = (match_cfg.get("normalize") or {})
    t = _norm(title, norm.get("opencc", True), norm.get("nfkc", True))

    # 过滤词
    for w in not_words or []:
        if _norm(w, norm.get("opencc", True), norm.get("nfkc", True)) in t:
            return False

    # 必须词
    for w in must_words or []:
        nw = _norm(w, norm.get("opencc", True), norm.get("nfkc", True))
        if not nw or nw not in t:
            return False

    # 正则
    if match_cfg.get("regex_enabled"):
        for w in any_words or []:
            try:
                if re.search(w, t):
                    return True
            except re.error:
                # 非法正则忽略
                pass

    # 模糊
    fcfg = (match_cfg.get("fuzzy") or {})
    if fcfg.get("enabled") and fuzz:
        thr = int(fcfg.get("threshold", 90))
        for w in any_words or []:
            nw = _norm(w, norm.get("opencc", True), norm.get("nfkc", True))
            if nw and fuzz.token_set_ratio(t, nw) >= thr:
                return True

    # 普通包含
    for w in any_words or []:
        nw = _norm(w, norm.get("opencc", True), norm.get("nfkc", True))
        if nw and nw in t:
            return True

    return False


def decide_match(title: str,
                 any_words: List[str],
                 must_words: List[str],
                 not_words: List[str],
                 match_cfg: Dict) -> bool:
    """
    对外统一入口：
    - 若 __disabled__ 或无可选库 → 回退 _basic_match
    - 否则用 advanced_match
    """
    if match_cfg.get("__disabled__"):
        return _basic_match(title, any_words, must_words, not_words)

    # 没装 opencc/rapidfuzz 也能走 advanced_match（只是相应能力降级）
    try:
        return advanced_match(title, any_words, must_words, not_words, match_cfg)
    except Exception:
        return _basic_match(title, any_words, must_words, not_words)
