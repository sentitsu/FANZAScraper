# app/providers/fanza_book.py

import requests
from typing import Dict, Any, List
from urllib.parse import urlencode

# 既存の config を流用（API_ID / AFFILIATE_ID など）
from app.core import config as CFG
from app.core.config import make_aff_url  # 動画側と同じ helper を再利用

API_ENDPOINT = "https://api.dmm.com/affiliate/v3/ItemList"

def _resolve_book_service_floor(params: Dict[str, Any]):
    """FANZA_BOOK 用の service/floor を 2 パターンに限定して解釈する。

    許可するのは次の 2 パターンのみ:
      - エロマンガ: service=ebook,  floor=comic           → site=FANZA / service=ebook / floor=comic
      - 同人      : service=doujin, floor=digital_doujin   → site=FANZA / service=doujin / floor=digital_doujin
    """
    raw_s = (params.get("service") or "").lower()
    raw_f = (params.get("floor") or "").lower()

    # --- 同人: doujin / digital_doujin ---
    if raw_s == "doujin" or raw_f == "digital_doujin":
        return "FANZA", "doujin", "digital_doujin"

    # --- エロマンガ: ebook / comic（デフォ） ---
    if raw_s in ("", "ebook") and raw_f in ("", "comic"):
        # floor 指定なしなら comic 扱い
        return "FANZA", "ebook", "comic"

    # 想定外の組み合わせは即エラーにして気付けるようにする
    raise ValueError(
        f"Unsupported service/floor for FANZA_BOOK: service={raw_s}, floor={raw_f}. "
        "Use either (service=ebook, floor=comic) or (service=doujin, floor=digital_doujin)."
    )


def fetch_items(api_id: str, affiliate_id: str, params: Dict[str, Any],
                start: int = 1, hits: int = 20) -> Dict[str, Any]:
    """FANZA の『エロマンガ / 同人』専用 ItemList ラッパー。"""
    site, service, floor = _resolve_book_service_floor(params)

    q = {
        "api_id": api_id or CFG.API_ID,
        "affiliate_id": affiliate_id or CFG.AFFILIATE_ID,
        "site": site,        # 常に FANZA
        "service": service,  # ebook or doujin
        "floor": floor,      # comic or digital_doujin
        "hits": hits,
        "offset": start,
        "sort": params.get("sort") or "date",
        "output": "json",
    }

    # 共通の絞り込みパラメータ
    for k in ("cid", "keyword", "article", "maker", "author", "genre", "gte_date", "lte_date"):
        v = params.get(k)
        if v:
            q[k] = v

    url = f"{API_ENDPOINT}?{urlencode(q)}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def _pick_sample_urls(prod: Dict[str, Any], max_count: int = 12) -> List[str]:
    """
    sample_l / sample_s からサンプル画像URLを取り出して、最大 max_count 件返す。

    - sampleImageURL / sampleImageURLS / sample のどれかを使う
    - sample_l / sample_s が list でも dict でも動くように防御的に処理
    - ネストした dict/list の中から文字列URLだけをフラットに回収
    """
    sample = (
        prod.get("sampleImageURL")
        or prod.get("sampleImageURLS")
        or prod.get("sample")
        or {}
    )

    # sample が dict 以外（list とか）のパターンもあるので防御
    if isinstance(sample, dict):
        arr_raw = sample.get("sample_l") or sample.get("sample_s") or []
    else:
        arr_raw = sample

    urls: List[str] = []

    def _collect(obj):
        """文字列URLだけを再帰的に集める小ヘルパー"""
        if obj is None:
            return
        if isinstance(obj, str):
            if obj.strip():
                urls.append(obj.strip())
            return
        if isinstance(obj, list):
            for v in obj:
                _collect(v)
            return
        if isinstance(obj, dict):
            for v in obj.values():
                _collect(v)
            return
        # それ以外の型は無視

    _collect(arr_raw)

    # 重複除去＋順序維持
    seen = set()
    out: List[str] = []
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)

    return out[:max_count]

def normalize_item(prod: Dict[str, Any]) -> Dict[str, Any]:
    """
    API レスポンス -> pipeline 共通スキーマに正規化。
    動画側の normalize_item と同じフィールド名に揃える。
    """
    cid   = prod.get("content_id") or prod.get("cid") or ""
    title = prod.get("title") or ""
    url   = make_aff_url(prod.get("URL") or "")
    # 書籍は date or volume_date が来ることがある
    date  = (prod.get("date") or prod.get("volume_date") or "").split(" ")[0]

    maker  = (prod.get("maker")  or [{}])[0].get("name") if prod.get("maker")  else ""
    label  = (prod.get("label")  or [{}])[0].get("name") if prod.get("label")  else ""
    series = (prod.get("series") or [{}])[0].get("name") if prod.get("series") else ""

    # 著者名 → actress フィールドに流しておく（既存のタグロジックを再利用するため）
    authors = [a.get("name") for a in (prod.get("author") or []) if a.get("name")]
    actress = ",".join(authors) if authors else ""

    genres_list = [g.get("name") for g in (prod.get("genre") or []) if g.get("name")]
    genres  = ",".join(genres_list) if genres_list else ""

    imageURL = prod.get("imageURL") or {}
    cover = imageURL.get("large") or imageURL.get("list") or ""

    max_gal = getattr(CFG, "MAX_GALLERY", 12)
    samples = _pick_sample_urls(prod, max_count=int(max_gal))

    row: Dict[str, Any] = {
        "cid": cid,
        "title": title,
        "URL": url,                 # pipeline は大文字 URL を見ている
        "date": date,
        "maker": maker,
        "label": label,
        "series": series,
        "actress": actress,         # 著者名をここに詰める
        "genres": genres,           # カンマ連結文字列
        "image_large": cover or "",
        "sample_images": "|".join(samples) if samples else "",
        # 書籍ではトレーラ系は使わないので空でOK
        "trailer_url": "",
        "trailer_youtube": "",
        "trailer_poster": "",
        "trailer_embed": "",
    }

    # 動画側と同様に aff_url も用意しておくとテンプレから使いやすい
    row["aff_url"] = make_aff_url(row["URL"])

    return row


def build_content_html(item: Dict[str, Any], content_builder=None, max_gallery: int = 12, **_):
    """
    pipeline 互換シグネチャで本文を生成。
    - ContentBuilder が渡されていればそれで描画
    - 無ければ簡易なフォールバック HTML
    """
    if content_builder is not None and hasattr(content_builder, "render"):
        return content_builder.render(item)

    # フォールバック（テンプレ未指定/欠落時）
    parts = [f"<h1>{item.get('title','')}</h1>"]
    if item.get("image_large"):
        parts.append(f"<p><img src=\"{item['image_large']}\" alt=\"cover\"></p>")
    if item.get("URL"):
        parts.append(
            f"<p><a href=\"{item['URL']}\" target=\"_blank\" rel=\"sponsored noopener\">公式ページ</a></p>"
        )

    # 簡易ギャラリー（最大 max_gallery 枚）
    sims = (item.get("sample_images") or "").split("|")
    sims = [s for s in sims if s][:max_gallery]
    if sims:
        parts.append("<div class='gallery'>")
        for s in sims:
            parts.append(f"<figure class='gallery__item'><img src=\"{s}\" alt=\"sample\"></figure>")
        parts.append("</div>")

    return "\n".join(parts)
