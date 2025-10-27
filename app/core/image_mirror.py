# app/core/image_mirror.py
import os, csv, io, hashlib, mimetypes, requests
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Optional, Any

MAP_CSV = os.getenv("IMAGE_MIRROR_MAP", "image_mirror_map.csv")
TIMEOUT = (5, 30)  # connect, read

def _load_map(path: str) -> Dict[str, Dict[str, str]]:
    m: Dict[str, Dict[str, str]] = {}
    if os.path.exists(path):
        with open(path, newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                m[row['src_url']] = row
    return m

def _append_map(path: str, rows: List[Dict[str, str]]):
    if not rows: return
    exists = os.path.exists(path)
    with open(path, 'a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['src_url','dest_url','sha1','bytes'])
        if not exists: w.writeheader()
        for r in rows: w.writerow(r)

def _coerce_url(u: Any) -> Optional[str]:
    """
    FANZAの poster が {large,list,small} の dict だったり、list/tuple が来ても
    必ず 'URLのstr' を返す。取れなければ None。
    """
    if not u:
        return None
    if isinstance(u, dict):
        return u.get("large") or u.get("list") or u.get("small") or None
    if isinstance(u, (list, tuple)):
        for v in u:
            s = _coerce_url(v)
            if s:
                return s
        return None
    s = str(u).strip()
    return s if s else None

def _split_samples(v: Any) -> List[str]:
    """'a|b|c' も ['a','b','c'] も同様に扱ってクリーンなリストへ。"""
    if not v:
        return []
    if isinstance(v, str):
        return [s for s in v.split("|") if s]
    if isinstance(v, (list, tuple)):
        return [str(s).strip() for s in v if str(s).strip()]
    return []

def _guess_ext(content_type: str, url_path: str) -> str:
    ext = mimetypes.guess_extension((content_type or '').split(';')[0].strip()) or ''
    if not ext:
        lp = url_path.lower()
        for cand in ('.jpg','.jpeg','.png','.webp','.gif','.avif'):
            if lp.endswith(cand): return cand
        return '.jpg'
    return '.jpg' if ext == '.jpe' else ext

def _download(url: str) -> Tuple[bytes, str]:
    headers = {'User-Agent':'Mozilla/5.0', 'Referer': ''}  # no-referrer 相当
    r = requests.get(url, headers=headers, timeout=TIMEOUT, stream=True)
    r.raise_for_status()
    data = r.content
    ctype = r.headers.get('Content-Type', 'image/jpeg').split(';')[0].strip()
    return data, ctype

def _resolve_media_id_from_url(wp_client, dest_url: str) -> Optional[int]:
    """
    dest_url（/wp-content/uploads/...）から attachment ID を推定。
    - まず basename で /wp/v2/media?search= を叩き、
      source_url 完全一致を最優先で照合。
    - 認証は App Password（環境変数 WP_URL, WP_USER, WP_APP_PASS）を使用。
    """
    if not dest_url:
        return None
    try:
        base = os.getenv("WP_URL", "").rstrip("/")
        user = os.getenv("WP_USER", "")
        app  = os.getenv("WP_APP_PASS", "")
        if not (base and user and app):
            return None

        fname = os.path.basename(urlparse(dest_url).path)
        r = requests.get(
            f"{base}/wp-json/wp/v2/media",
            params={"search": fname, "per_page": 20},
            auth=(user, app),
            timeout=20
        )
        r.raise_for_status()
        hits = r.json() or []
        # 1) 完全一致
        for m in hits:
            su = str(m.get("source_url") or "")
            if su.rstrip("/") == dest_url.rstrip("/"):
                return int(m.get("id")) if m.get("id") is not None else None
        # 2) basename 一致（派生サイズの可能性あり）
        for m in hits:
            su = str(m.get("source_url") or "")
            if su.lower().endswith("/" + fname.lower()):
                return int(m.get("id")) if m.get("id") is not None else None
    except Exception:
        pass
    return None

def _wp_upload_from_url(src_url: str, filename: str, wp_client) -> tuple[Optional[int], Optional[str]]:
    """
    アップロードの結果として (media_id, source_url) を返す。
    - 既存の wp_client 実装が返す型（dict/int/str）すべてを吸収
    - media_id が取れない時は REST で解決を試みる
    """
    media_id: Optional[int] = None
    source_url: Optional[str] = None

    # パスA: multipart 受けられるクライアント
    if hasattr(wp_client, "upload_media"):
        b, ctype = _download(src_url)
        files = {'file': (filename, io.BytesIO(b), ctype)}
        res = wp_client.upload_media(files, data={"title": filename})
        if isinstance(res, dict):
            media_id = res.get("id")
            source_url = res.get("source_url") or (res.get("guid", {}) or {}).get("rendered")
        return media_id, source_url

    # パスB: URL からのアップロード
    if hasattr(wp_client, "upload_media_from_url"):
        res = wp_client.upload_media_from_url(src_url, filename=filename)

        # 返り値が dict
        if isinstance(res, dict):
            media_id = res.get("id") or res.get("attachment_id")
            source_url = res.get("source_url")
            if not source_url:
                guid = res.get("guid")
                if isinstance(guid, dict):
                    source_url = guid.get("rendered") or guid.get("raw")
                elif isinstance(guid, str) and guid.startswith("http"):
                    source_url = guid
            # 足りなければ REST で補完
            if (media_id is not None) and not source_url:
                try:
                    data = getattr(wp_client, "_req")( "GET", f"/wp-json/wp/v2/media/{media_id}" )
                    source_url = (data or {}).get("source_url")
                except Exception:
                    pass
            return media_id, source_url

        # 返り値が int（ID）
        if isinstance(res, int):
            media_id = res

        # 返り値が str（URL or ID 文字列）
        elif isinstance(res, str):
            if res.isdigit():
                media_id = int(res)
            elif res.startswith("http"):
                source_url = res

        # 補完（ID だけ取れた / URL だけ取れたケース）
        if (media_id is not None) and not source_url:
            try:
                data = getattr(wp_client, "_req")( "GET", f"/wp-json/wp/v2/media/{media_id}" )
                source_url = (data or {}).get("source_url")
            except Exception:
                pass

        return media_id, source_url

    # どちらも無ければ諦め
    return None, None

def _mirror_one(url: str, wp_client, prefix: str,
                cache_map: Dict[str, Dict[str,str]],
                to_write: List[Dict[str,str]]) -> tuple[Optional[int], Optional[str]]:
    if not url:
        return None, None

    print(f"[mirror:in] src={url}")

    # CSVヒット：URLは取れるが ID は持っていない → ここで解決を試みる
    if url in cache_map:
        dest = cache_map[url]['dest_url']
        mid  = _resolve_media_id_from_url(wp_client, dest)
        return mid, dest

    try:
        b, ctype = _download(url)
        h = hashlib.sha1(b).hexdigest()
        path = urlparse(url).path
        ext = _guess_ext(ctype, path)
        fname = f"{prefix}-{h[:8]}{ext}"

        media_id, dest = _wp_upload_from_url(url, fname, wp_client)

        # ここで media_id が None でも、dest があれば REST 検索で補完
        if dest and media_id is None:
            media_id = _resolve_media_id_from_url(wp_client, dest)

        if dest:
            row = {'src_url':url,'dest_url':dest,'sha1':h,'bytes':str(len(b))}
            cache_map[url] = row
            to_write.append(row)
            print(f"[mirror] OK {url} -> {dest} id={media_id} bytes={len(b)}")
            return media_id, dest

        return None, None
    except Exception as e:
        print(f"[mirror] FAIL {url}: {e}")
        return None, None


def mirror_item_images(item: dict, wp_client, prefix: str) -> dict:
    cache = _load_map(MAP_CSV)
    pending: List[Dict[str, str]] = []

    # --- ポスター（アイキャッチ候補） ---
    # まずは normalize 済みの image_large（＝昇格済み）を優先
    poster_url = _coerce_url(item.get('image_large')) or _coerce_url(item.get('trailer_poster'))
    if poster_url:
        mid, new_poster = _mirror_one(poster_url, wp_client, f"{prefix}-poster", cache, pending)
        if new_poster:
            item['trailer_poster'] = new_poster
            item['image_large']    = new_poster
        # ここで mid が None でも、URL から最終補完
        if (mid is None) and new_poster:
            mid = _resolve_media_id_from_url(wp_client, new_poster)
        if mid:
            item['trailer_poster_id'] = int(mid)
            item['image_large_id']    = int(mid)

    # --- サンプル画像 ---
    samples_raw = _split_samples(item.get('sample_images'))
    samples = [_coerce_url(s) for s in samples_raw]
    out: List[str] = []
    sample_ids: List[int] = []
    idx = 1
    for u in samples:
        if not u:
            continue
        smid, nu = _mirror_one(u, wp_client, f"{prefix}-s{idx:02d}", cache, pending)
        out.append(nu or u)
        if smid:
            sample_ids.append(int(smid))
        idx += 1

    item['sample_images'] = '|'.join(out)
    if sample_ids:
        item['sample_image_ids'] = sample_ids

    _append_map(MAP_CSV, pending)
    return item
