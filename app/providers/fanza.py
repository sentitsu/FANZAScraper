# app/providers/fanza.py
import re, json, time, requests
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from app.core.content_builder import ContentBuilder
from typing import Optional, Dict, Any, List
from jinja2 import Environment, FileSystemLoader, select_autoescape

API_ENDPOINT = "https://api.dmm.com/affiliate/v3/ItemList"

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

def _extract_trailer_fields(it: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    DMM/FANZA レスポンスからトレーラー情報を抽出。
    1) 既知キー（sampleMovieURL 等）
    2) だめなら全体を走査して .mp4 / .m3u8 / YouTube を拾う
    """
    trailer_url: Optional[str] = None
    trailer_poster: Optional[str] = None
    trailer_youtube: Optional[str] = None
    trailer_embed: Optional[str] = None

    # --- 1) 代表キー（dict or str）
    smu = (
        it.get("sampleMovieURL") or it.get("sampleMovieUrl") or it.get("sample_movie_url")
        or (it.get("iteminfo", {}) or {}).get("sampleMovieURL")
        or (it.get("iteminfo", {}) or {}).get("sampleMovieUrl")
        or {}
    )
    if isinstance(smu, dict):
        # 解像度の高い順に
        for key in ("size_1080", "size_1280_720", "size_720_480", "size_720", "size_560_360", "size_476_306", "url", "mp4"):
            v = smu.get(key)
            if isinstance(v, str) and v.strip():
                trailer_url = v.strip()
                break
    elif isinstance(smu, str) and smu.strip():
        trailer_url = smu.strip()

    # --- 2) 追加の既知フィールド（環境差対応）
    for k in ("pr_movie", "prMovie", "preview", "trailer", "movie"):
        if trailer_url:
            break
        v = it.get(k)
        if isinstance(v, str) and v.strip() and v.strip().startswith("http"):
            trailer_url = v.strip()
            break
        if isinstance(v, dict):
            # よくある "url" キー
            cand = v.get("url") or v.get("src")
            if isinstance(cand, str) and cand.strip().startswith("http"):
                trailer_url = cand.strip()
                break

    # --- 3) 深掘り走査：アイテム全体からURL候補を拾ってスコアリング
    def _walk(x, acc: List[str]):
        if isinstance(x, str):
            sx = x.strip()
            lx = sx.lower()
            if sx.startswith("http") and (".mp4" in lx or ".m3u8" in lx or "youtube.com/" in lx or "youtu.be/" in lx):
                acc.append(sx)
        elif isinstance(x, dict):
            for vv in x.values():
                _walk(vv, acc)
        elif isinstance(x, list):
            for vv in x:
                _walk(vv, acc)

    if not trailer_url:
        cands: List[str] = []
        _walk(it, cands)
        def _score(u: str) -> int:
            s = 0
            ul = u.lower()
            if ".mp4" in ul: s += 100
            if ".m3u8" in ul: s += 80
            if "youtube.com/" in ul or "youtu.be/" in ul: s += 50
            # 粗い解像度ヒント
            for hint, pts in (("1080", 20), ("1280", 18), ("720", 15), ("560", 10), ("480", 8), ("360", 5)):
                if hint in ul: s += pts
            return s
        if cands:
            trailer_url = sorted(cands, key=_score, reverse=True)[0]

    # YouTube は別枠にも入れておく
    if trailer_url and ("youtube.com/" in trailer_url.lower() or "youtu.be/" in trailer_url.lower()):
        trailer_youtube = trailer_url
        # trailer_url は空に（本文側で二重判定しないため）
        trailer_url = None

    # --- 4) ポスター（大きめ優先）
    siu = it.get("sampleImageURL") or it.get("sampleImageUrl") or {}
    if isinstance(siu, dict):
        for key in ("large", "list", "small"):
            v = siu.get(key)
            if isinstance(v, list) and v:
                if isinstance(v[0], str) and v[0].strip():
                    trailer_poster = v[0].strip()
                    break
            if isinstance(v, str) and v.strip():
                trailer_poster = v.strip()
                break
    if not trailer_poster:
        for k in ("imageURL", "image_large", "image", "thumb", "jacket", "cover"):
            vv = it.get(k)
            if isinstance(vv, str) and vv.strip():
                trailer_poster = vv.strip()
                break

    cid = (it.get("content_id") or it.get("cid") or "").strip()
    if cid and trailer_url and "dmm.co.jp/litevideo/-/part" in trailer_url:
        mp4 = _resolve_litevideo_to_mp4(cid)
    if mp4:
        trailer_url = mp4

    return {
        "trailer_url": trailer_url,
        "trailer_poster": trailer_poster,
        "trailer_youtube": trailer_youtube,
        "trailer_embed": trailer_embed,
    }

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
        "URL": url,
        "date": date,
        "maker": maker,
        "actress": actress_str,
        "genres": genres_str,
        "sample_images": "|".join(sample_images),
        "image_large": image_large,
    }

    row.update(_extract_trailer_fields(it))

    if not row.get("trailer_poster"):
        first_sample = (row.get("sample_images") or "").split("|")[0] if row.get("sample_images") else ""
        row["trailer_poster"] = row.get("image_large") or first_sample or None

    return row
