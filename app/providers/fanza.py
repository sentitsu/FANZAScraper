# app/providers/fanza.py
import re, json, time, requests
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

API_ENDPOINT = "https://api.dmm.com/affiliate/v3/ItemList"

def build_content_html(row, max_gallery=12):
    title   = row.get("title","")
    maker   = row.get("maker","")
    actress = row.get("actress","")
    genres  = row.get("genres","")
    url     = row.get("URL","")
    jacket  = row.get("image_large","") or ""
    samples = [u for u in (row.get("sample_images","").split("|") if row.get("sample_images") else []) if u][:max_gallery]

    def img(u):
        return f'<img src="{u}" loading="lazy" decoding="async" alt="{title}">' if u else ""

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

def normalize_item(it):
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

    return {
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
