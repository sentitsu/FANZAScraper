# app/providers/fanza.py
import re, json, time, requests, os
from app.core import config
from app.core.config import make_aff_url
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from app.core.content_builder import ContentBuilder
from typing import Optional, Dict, Any, List
from jinja2 import Environment, FileSystemLoader, select_autoescape

API_ENDPOINT = "https://api.dmm.com/affiliate/v3/ItemList"

def get_player_size_from_env(default="1280_720"):
    s = os.getenv("FANZA_IFRAME_SIZE", default)
    try:
        w, h = [int(x) for x in s.split("_", 1)]
        return w, h, round(h / w * 100, 2)
    except Exception:
        return 1280, 720, round(720/1280*100, 2)

def parse_aspect_ratio(size_str: str) -> float | None:
    """
    '1280_720' のような文字列から高さ/幅*100 の割合を返す。
    例: 1280x720 → 56.25 (%)
    """
    try:
        w, h = size_str.split("_")
        return round((int(h) / int(w)) * 100, 2)
    except Exception:
        return None

def build_fanza_iframe_src(cid: str, affiliate_id: str, size: str = "560_360") -> str | None:
    """
    FANZA 公式の埋め込み用 iframe src を生成
    形式: https://www.dmm.co.jp/litevideo/-/part/=/affi_id=XXX/cid=YYY/size=WIDTH_HEIGHT/
    """
    if not cid or not affiliate_id:
        return None
    return f"https://www.dmm.co.jp/litevideo/-/part/=/affi_id={affiliate_id.strip()}/cid={str(cid).strip()}/size={ (size or '560_360').strip() }/"

def _guess_preview_mp4_urls(cid: str) -> list[str]:
    """
    CID から historical な freepv MP4 URL の候補を生成。
    例: jufe00582 -> .../freepv/j/juf/jufe00582/jufe00582mhb.mp4
    派生パターン（接尾辞や _/_w の有無）も網羅。
    """
    if not isinstance(cid, str) or not cid:
        return []
    m = re.match(r'^([a-z]+)', cid)
    if not m:
        return []
    alpha = m.group(1)              # 'jufe'
    a1 = alpha[:1]                  # 'j'
    a3 = alpha[:3]                  # 'juf'  (短い銘柄はそのまま)
    base = f"https://cc3001.dmm.co.jp/litevideo/freepv/{a1}/{a3}/{cid}/{cid}"

    # よく見かける接尾辞のバリエーション（順序＝優先度）
    tails = [
        "mhb.mp4", "dmb.mp4", "dm.mp4", "sm.mp4",
        "_mhb.mp4", "_dmb.mp4", "_dm.mp4", "_sm.mp4",
        "mhb_w.mp4", "dmb_w.mp4", "dm_w.mp4", "sm_w.mp4",
        "_mhb_w.mp4", "_dmb_w.mp4", "_dm_w.mp4", "_sm_w.mp4",
    ]
    return [base + t for t in tails]

def _resolve_litevideo_to_mp4(cid: str, timeout: float = 3.0) -> str | None:
    """
    freepv MP4 候補に HEAD を打ち、200 かつ video/mp4 っぽいものを返す。
    見つからなければ None。
    """
    for url in _guess_preview_mp4_urls(cid):
        try:
            r = requests.head(url, allow_redirects=True, timeout=timeout)
            if r.status_code == 200:
                ctype = (r.headers.get("Content-Type") or "").lower()
                if "video" in ctype and "mp4" in ctype:
                    return url
        except Exception:
            pass
    return None

def _extract_trailer_fields(it: dict) -> dict:
    """
    予告編系フィールドを抽出する。
    ポリシー:
      - 直リンク(.mp4/.m3u8)はテンプレには渡さない（規約配慮）
      - 公式 iframe の src があればそれを優先して渡す
    返却:
      trailer_embed, trailer_poster, trailer_url(None固定)
    """
    # まずは未初期化エラー対策として確実に初期化
    mp4 = None
    m3u8 = None
    iframe = None

    # よくあるキーの候補を拾う（あなたのデータ構造に合わせて増やしてOK）
    raw_url   = it.get("trailer_url") or it.get("sampleMovieURL") or it.get("trailer") or ""
    iframe    = (
        it.get("trailer_embed") or
        it.get("trailerEmbedURL") or
        it.get("sampleMovieEmbed") or
        it.get("embed_url") or
        None
    )
    poster = (
        it.get("trailer_poster") or
        it.get("image_large") or
        it.get("imageURL") or
        None
    )

    # raw_url が文字列で直リンクっぽいかを軽く判定（使わない方針だが監査用に拾っておく）
    if isinstance(raw_url, str) and raw_url:
        ul = raw_url.strip().lower()
        if ul.endswith(".mp4") or (".mp4?" in ul):
            mp4 = raw_url
        elif ul.endswith(".m3u8") or (".m3u8?" in ul):
            m3u8 = raw_url

    # 戻り値を一元的に組み立て
    # ※ 直リンクはテンプレに渡さないため trailer_url は常に None にしておく
    row = {
        "trailer_url": None,  # 直リンクは使わない
        "trailer_poster": poster,
        "trailer_embed": iframe or None,
    }

    # ログ用途（任意）：直リンクが来ていたら警告ログを出したい場合
    # if (mp4 or m3u8) and not iframe:
    #     logger.warning("Detected direct trailer link without iframe: mp4=%s m3u8=%s cid=%s", mp4, m3u8, it.get("cid"))

    return row


def build_content_html(row, content_builder: ContentBuilder | None = None, max_gallery: int = 12):
    title   = row.get("title","")
    maker   = row.get("maker","")
    actress = row.get("actress","")
    genres  = row.get("genres","")
    url     = row.get("URL","")
    jacket  = row.get("image_large","") or ""
    samples = [u for u in (row.get("sample_images","").split("|") if row.get("sample_images") else []) if u][:max_gallery]

    # テンプレ駆動（指定時）
    if content_builder:
        item = dict(row)
        item["_max_gallery"] = max_gallery
        return content_builder.render(item)

    # フォールバック（従来HTML）
    def img(u): return f'<img src="{u}" loading="lazy" decoding="async" alt="{title}">' if u else ""
    parts = []
    if jacket or samples:
        first_img = jacket or samples[0]
        parts.append(f'<figure class="lead-image">{img(first_img)}</figure>')
    meta = []
    if actress: meta.append(f"<strong>出演:</strong> {actress}")
    if maker:   meta.append(f"<strong>メーカー:</strong> {maker}")
    if genres:  meta.append(f"<strong>ジャンル:</strong> {genres}")
    if meta:
        parts.append("<p>" + "<br>".join(meta) + "</p>")
    if samples:
        items = "\n".join(f'<figure class="gallery__item">{img(u)}</figure>' for u in samples)
        parts.append(f'<div class="gallery">{items}</div>')
    if url:
        parts.append(f'<p><a href="{url}" rel="nofollow sponsored" target="_blank">公式ページはこちら</a></p>')
    return "\n".join(parts)

def fetch_items(api_id, affiliate_id, params, start=1, hits=100):
    q = {
        "api_id": api_id,
        "affiliate_id": affiliate_id,
        "output": "json",
        "site": params.get("site", "FANZA"),
        "service": params.get("service", "digital"),
        "floor": params.get("floor", "videoa"),
        "sort": params.get("sort", "date"),
        "hits": hits,
        "offset": start,
    }
    if kw := params.get("keyword"): q["keyword"] = kw
    if cid := params.get("cid"): q["cid"] = cid
    if gte := params.get("gte_date"): q["gte_date"] = gte
    if lte := params.get("lte_date"): q["lte_date"] = lte
    r = requests.get(API_ENDPOINT, params=q, timeout=30)
    r.raise_for_status()
    return r.json()

def _clean_query_keep_webp(u: str) -> str:
    try:
        sp = urlsplit(u)
        qs = dict(parse_qsl(sp.query, keep_blank_values=True))
        keep = {"f": "webp"} if qs.get("f","").lower()=="webp" else {}
        return urlunsplit((sp.scheme, sp.netloc, sp.path, urlencode(keep), sp.fragment))
    except Exception:
        return u

def _upgrade_dmm_size(u: str) -> str:
    if not u: return u
    if u.startswith("//"): u = "https:" + u
    u = re.sub(r"/ps\.jpg(\?.*)?$", "/pl.jpg", u, flags=re.I)
    u = re.sub(r"([a-z0-9]+)ps\.jpg$", r"\1pl.jpg", u, flags=re.I)
    if "awsimgsrc.dmm.co.jp" in u:
        m = re.search(r"/([^/]+)/\1-(\d+)\.(jpg|jpeg|png|webp)", u, flags=re.I)
        if m:
            cid = m.group(1)
            u = re.sub(rf"/{cid}/{cid}-(\d+)\.(?:jpg|jpeg|png|webp)",
                       rf"/{cid}/{cid}jp-\1.jpg", u, flags=re.I)
            u = _clean_query_keep_webp(u)
    if "now_printing" in u.lower() or "nowprinting" in u.lower():
        return ""
    return u

def _is_large_hint(u: str) -> bool:
    s = u.lower()
    return ("jp-" in s) or s.endswith("pl.jpg") or ("sample_l" in s)

def _is_now_printing_url_like(u: str) -> bool:
    if not u: return True
    s = u.lower()
    return ("now_printing" in s) or ("nowprinting" in s)

def _fast_placeholder_heuristic(u: str) -> bool:
    """URLだけで高速に判定（now_printing系）"""
    if not u:
        return True
    s = u.lower()
    return ("now_print" in s) or ("nowprinting" in s)

def _probe_is_placeholder(u: str, timeout: float = 8.0, verify: bool = True, use_network: bool = True) -> bool:
    """プレースホルダ判定
       1) URLヒューリスティック
       2) use_network=True のときだけ HEAD で軽確認（失敗=Falseにする）
    """
    if _fast_placeholder_heuristic(u):
        return True
    if (not use_network) or (not u):
        return False
    try:
        r = requests.head(u, allow_redirects=True, timeout=timeout, verify=verify)
        final = (r.url or "").lower()
        if "now_print" in final or "nowprinting" in final:
            return True
        cl = r.headers.get("Content-Length")
        if cl and cl.isdigit() and int(cl) < 15000:
            return True
    except Exception:
        # ネットワーク失敗を「プレースホルダ扱い」にしない
        return False
    return False

def _pick_best_feature(sample_urls: list[str]) -> str:
    if not sample_urls: return ""
    def score(u: str):
        s = u.lower()
        return (("jp-" in s) * 3) + (s.endswith("pl.jpg") * 2) + (re.search(r"-1\.jpg(\?|$)", s) is not None)
    return sorted(sample_urls, key=lambda x: (-score(x), x))[0]

def _extract_sample_images(it):
    urls = []
    def _collect(obj):
        if obj is None: return
        if isinstance(obj, str):
            u = _upgrade_dmm_size(obj.strip())
            import re as _re
            if u and _re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", u, _re.I):
                urls.append(u)
            return
        if isinstance(obj, dict):
            for k in ("image","src","url"):
                v = obj.get(k)
                if isinstance(v, str):
                    _collect(v)
            for v in obj.values():
                _collect(v)
            return
        if isinstance(obj, list):
            for v in obj: _collect(v)
            return
    for key in ("sampleImageURL","sampleImageURLS","sampleImage","sampleimage","iteminfo"):
        if key in it: _collect(it[key])

    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    uniq.sort(key=lambda x: (not _is_large_hint(x), x))
    return uniq

def normalize_item(it: Dict[str, Any]) -> Dict[str, Any]:
    cid   = it.get("content_id") or it.get("cid") or ""
    title = it.get("title", "")
    url   = it.get("URL") or it.get("affiliateURL") or ""
    date  = it.get("date", "")

    maker = ""
    if isinstance(it.get("maker"), dict):
        maker = it["maker"].get("name","")
    elif "iteminfo" in it and "maker" in it["iteminfo"]:
        m = it["iteminfo"]["maker"]
        if isinstance(m, list) and m: maker = m[0].get("name","")

    actresses = []
    ii = it.get("iteminfo") or {}
    for a in ii.get("actress", []) or []:
        if isinstance(a, dict) and a.get("name"):
            actresses.append(a["name"])
    actress_str = ", ".join(actresses)

    genres = []
    for g in ii.get("genre", []) or []:
        if isinstance(g, dict) and g.get("name"):
            genres.append(g["name"])
    genres_str = ", ".join(genres)

    image_large = ""
    img = it.get("imageURL")
    if isinstance(img, dict):
        image_large = img.get("large") or img.get("list") or ""
    if _is_now_printing_url_like(image_large):
        image_large = ""

    sample_images = _extract_sample_images(it)

    row = {
        "cid": cid,
        "title": title,
        "affiliateURL": it.get("affiliateURL") or it.get("affiliate_url") or "",
        "URL": url,
        "date": date,
        "maker": maker,
        "actress": actress_str,
        "genres": genres_str,
        "sample_images": "|".join(sample_images),
        "image_large": image_large,
    }

    row.update(_extract_trailer_fields(it))

    if not row.get("trailer_embed"):
        auto_src = build_fanza_iframe_src(
            row.get("cid"),
            config.AFFILIATE_ID,
            config.IFRAME_SIZE,
        )
        row["trailer_embed"] = auto_src

    
    # .env のサイズを反映
    row["player_width"]  = config.FANZA_IFRAME_W
    row["player_height"] = config.FANZA_IFRAME_H
    row["aspect_ratio"]  = config.FANZA_IFRAME_RATIO

    # 既存の size= を確実に .env 値へ
    if row.get("trailer_embed"):
        row["trailer_embed"] = re.sub(
            r"size=\d+_\d+",
            f"size={config.FANZA_IFRAME_W}_{config.FANZA_IFRAME_H}",
            row["trailer_embed"]
        )

    if not row.get("trailer_poster"):
        first_sample = (row.get("sample_images") or "").split("|")[0] if row.get("sample_images") else ""
        row["trailer_poster"] = row.get("image_large") or first_sample or None

    # ★ ここで aff_url を作って注入（ENVは config.make_aff_url が中で読む）
    base = row.get("affiliateURL") or row.get("URL") or ""
    row["aff_url"] = make_aff_url(base)

    return row

def sanitize_trailer_fields(item: dict) -> dict:
    """直リンク(.mp4/.m3u8)は使わない。iframe用のURLだけ許可。"""
    def is_direct(url: str) -> bool:
        u = (url or "").lower()
        return u.endswith(".mp4") or u.endswith(".m3u8") or (".mp4?" in u) or (".m3u8?" in u)

    # 直リンクは無効化
    if is_direct(item.get("trailer_url", "")):
        item["trailer_url"] = None

    # もし既に iframe 用の src が取れているならそれを採用
    if item.get("trailer_embed"):
        return item

    # ここで “公式の埋め込みURL” をセットする。
    # 例: アフィ管理画面/APIで得られる iframe src を item["trailer_embed"] に入れる。
    # 取得方法はあなたの環境に合わせて。
    # item["trailer_embed"] = build_fanza_embed_src(item)  # TODO: 実装

    return item
