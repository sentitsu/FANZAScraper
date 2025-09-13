# app/core/config.py

import os
from urllib.parse import urlparse, parse_qs, unquote, quote
from dotenv import load_dotenv
from typing import Tuple, Optional

# ---- load .env -------------------------------------------------------------
load_dotenv()

# ---- ENV / Defaults --------------------------------------------------------
AFFILIATE_ID: str = os.getenv("AFFILIATE_ID", "").strip()
AFFILIATE_REDIRECT: str = os.getenv("AFFILIATE_REDIRECT", "https://al.dmm.com").strip().rstrip("/")
# 任意: 外層に &ch=xxx を付けたい場合に使う（無ければ付けない）
AFFILIATE_CH: str = os.getenv("AFFILIATE_CH", "").strip()

# 例: "1280_720"
IFRAME_SIZE: str = os.getenv("FANZA_IFRAME_SIZE", "1280_720").strip()

# al.* の短縮ドメイン（これらは unwrap 対象）
REDIRECT_HOSTS = {"al.dmm.com", "al.fanza.co.jp", "al.dmm.co.jp"}


# ---- Size helpers ----------------------------------------------------------
def _parse_size(s: str) -> Tuple[int, int, float]:
    """
    '1280_720' → (1280, 720, 56.25)
    ratio は CSSの padding-top%（H/W*100）
    """
    try:
        w, h = map(int, s.split("_", 1))
    except Exception:
        w, h = 1280, 720
    return w, h, round(h / w * 100, 2)


FANZA_IFRAME_W, FANZA_IFRAME_H, FANZA_IFRAME_RATIO = _parse_size(IFRAME_SIZE)


# ---- Affiliate helpers -----------------------------------------------------
def _is_aff_redirect(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return any(host.endswith(h) for h in REDIRECT_HOSTS)


def _extract_lurl(url: str) -> Optional[str]:
    """al.* の lurl パラメータを取り出す（無ければ None）"""
    try:
        q = parse_qs(urlparse(url).query)
        vals = q.get("lurl", [])
        return unquote(vals[0]) if vals else None
    except Exception:
        return None


def _unwrap_aff_url(url: str, max_hops: int = 5) -> str:
    """
    al.* で多重ラップされている場合に中身（最終URL）を取り出す。
    例: al.dmm → al.fanza → video.dmm → ... を最大 max_hops 回まで剥がす。
    """
    inner = url
    hops = 0
    while _is_aff_redirect(inner) and hops < max_hops:
        nxt = _extract_lurl(inner)
        if not nxt:
            break
        inner = nxt
        hops += 1
    return inner


def make_aff_url(base: Optional[str]) -> str:
    """
    作品のベースURL（video.dmm.co.jp 等）からアフィURLを生成。
    - base が既に al.* の場合は unwrap（多重ラップ防止）
    - AFFILIATE_ID / AFFILIATE_REDIRECT が設定済なら 1回だけ包み直し
    - AFFILIATE_CH があれば &ch=... を外層に付与
    - どれか欠けている場合は base をそのまま返す
    """
    if not base:
        return ""

    # 既存の al.* ラップを剥がす
    final = _unwrap_aff_url(base)

    # 包み直し
    if AFFILIATE_ID and AFFILIATE_REDIRECT:
        aff = f"{AFFILIATE_REDIRECT}/?lurl={quote(final, safe='')}&af_id={AFFILIATE_ID}"
        if AFFILIATE_CH:
            aff += f"&ch={quote(AFFILIATE_CH, safe='')}"
        return aff

    # 未設定時は素のURLを返す（安全フォールバック）
    return base


__all__ = [
    "AFFILIATE_ID",
    "AFFILIATE_REDIRECT",
    "AFFILIATE_CH",
    "FANZA_IFRAME_W",
    "FANZA_IFRAME_H",
    "FANZA_IFRAME_RATIO",
    "make_aff_url",
]
