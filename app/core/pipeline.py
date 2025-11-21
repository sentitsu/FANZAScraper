# app/core/pipeline.py
import time, csv
import re
from typing import Dict, Any, List
from app.providers.fanza import fetch_items, normalize_item, build_content_html, \
    _is_now_printing_url_like, _probe_is_placeholder, _pick_best_feature, sanitize_trailer_fields, get_player_size_from_env
from app.core.wp_rest import WPClient
from app.core.filters import apply_filters
from app.util.logger import log_json
from importlib import import_module
from app.core.content_builder import ContentBuilder
import os as _os
from app.core.seo import build_seo_fields, build_wp_seo_meta
from app.core.csv_dedupe import load_skip_cids, append_ledger, load_skip_cids_in_dir
from app.core.image_mirror import mirror_item_images
from pathlib import Path
import traceback
from app.providers import fanza_book

from urllib.parse import urlparse
import os, mimetypes, requests, io

TAG_STOPWORDS = set(["ハイビジョン", "サンプル", "動画", "独占配信"])

_ZW_RE = re.compile(r"[\u200B\u200C\u200D\uFEFF]")  # ゼロ幅類

def _norm(s: str | None) -> str:
    if not s:
        return ""
    s = _ZW_RE.sub("", s)           # ゼロ幅除去
    s = s.replace("\u3000", " ")    # 全角スペース→半角
    return s.strip()

# 追加: 正規化済み stopwords
TAG_STOPWORDS_N = {_norm(x) for x in TAG_STOPWORDS}

def _is_stopword_term(term: str) -> bool:
    """完全一致 or 部分一致でノイズ語を弾く"""
    t = _norm(term)
    if not t:
        return True
    if t in TAG_STOPWORDS_N:
        return True
    # '独占配信中', 'ハイビジョン対応' などの派生も除外
    return any(sw in t for sw in TAG_STOPWORDS_N)

# ---- 追加: args からテンプレパスを確実に拾うユーティリティ
def _get_content_template_path(args) -> str | None:
    # argparse で --content-template を定義している想定
    # 名前が content_template / content_template_path どちらでも拾う
    return getattr(args, "content_template", None) or getattr(args, "content_template_path", None)


def get_wp_client_from_env():
    """
    環境変数から WP クライアントを作る:
      WP_BASE_URL, WP_USER, WP_APP_PASS を想定
    """
    base = _os.getenv('WP_URL')
    user = _os.getenv('WP_USER')
    ap   = _os.getenv('WP_APP_PASS')
    if not all([base,user,ap]):
        raise RuntimeError('WP creds missing: set WP_URL, WP_USER, WP_APP_PASS')
    return WPClient(base, user, ap)

# --- 女優・ジャンル等を分割する小関数とタグのストップワード ---
def _split_terms(s: str | None) -> list[str]:
    """
    空白/カンマ/読点/中黒/スラッシュ/パイプ(半角/全角) で分割して正規化。
    """
    parts = re.split(r"[、,\s/・\|｜]+", (s or ""))  # ← 全角｜(U+FF5C) を追加
    out = []
    for x in parts:
        n = _norm(x)
        if n:
            out.append(n)
    return out

def _is_newer_than(date_str: str, ymd: str | None) -> bool:
    from datetime import datetime
    if not date_str or not ymd:
        return False
    try:
        d_item = datetime.fromisoformat(date_str.split(" ")[0])
        d_thr  = datetime.fromisoformat(ymd)
        return d_item > d_thr
    except Exception:
        return False

def _filter_and_enhance(row: Dict[str, Any], args) -> Dict[str, Any] | None:

    # HEAD制御パラメータ
    use_head    = not getattr(args, "no_head_check", False)
    head_timeout = float(getattr(args, "head_timeout", 3.0))
    head_verify  = not getattr(args, "head_insecure", False)

    # 画像検証&代替
    if args.verify_images:
        ph = False
        if not row["image_large"]:
            ph = True
        elif _is_now_printing_url_like(row["image_large"]):
            ph = True
        else:
            ph = _probe_is_placeholder(
                row["image_large"],
                timeout=head_timeout,
                verify=head_verify,
                use_network=use_head
            )
        if ph:
            samples = [s for s in row["sample_images"].split("|") if s]
            if samples:
                cand = _pick_best_feature(samples)
                # サンプル側はURLヒューリスティック優先（必要ならここでHEADしてもOK）
                from app.providers.fanza import _fast_placeholder_heuristic
                if cand and not _fast_placeholder_heuristic(cand):
                    row["image_large"] = cand

    # 足切り
    samples_cnt = len([s for s in row["sample_images"].split("|") if s])

    if args.skip_placeholder:
        ph2 = (not row["image_large"]) or _is_now_printing_url_like(row["image_large"]) or \
              _probe_is_placeholder(
                  row["image_large"],
                  timeout=head_timeout,
                  verify=head_verify,
                  use_network=use_head
              )
        if ph2:
            return None

    if samples_cnt < args.min_samples:
        return None
    if _is_newer_than(row["date"], args.release_after):
        return None
    return row

def _init_wp(args) -> tuple[WPClient | None, list[int], list[int], str]:
    wp = None
    cat_ids, tag_ids = [], []
    status = "publish" if args.publish else "draft"
    if args.future_datetime:
        status = "future"
    if args.wp_post:
        if not (args.wp_url and args.wp_user and args.wp_app_pass):
            raise SystemExit("--wp-url/--wp-user/--wp-app-pass 必須（もしくは環境変数で設定）")
        wp = WPClient(args.wp_url, args.wp_user, args.wp_app_pass)
        cats = [s.strip() for s in (args.wp_categories or "").split(",") if s.strip()]
        tags = [s.strip() for s in (args.wp_tags or "").split(",") if s.strip()]
        if cats: cat_ids = wp.ensure_categories(cats)
        if tags: tag_ids = wp.ensure_tags(tags)
    return wp, cat_ids, tag_ids, status

def _load_hook(hook_path: str):
    """
    hook_path 例: "hooks.myhook:transform"
    """
    mod_name, func_name = hook_path.split(":")
    mod = import_module(mod_name)
    return getattr(mod, func_name)

def _init_content_builder(args) -> ContentBuilder | None:
    """
    CLI引数から ContentBuilder を初期化。
    いずれのテンプレ／フックも指定がなければ None を返し、レガシー本文生成にフォールバック。
    """
    has_any = any([
        getattr(args, "content_template", None),
        getattr(args, "content_md_template", None),
        getattr(args, "prepend_html", None),
        getattr(args, "append_html", None),
        getattr(args, "content_hook", None),
    ])
    if not has_any:
        return None

    hook_fn = _load_hook(args.content_hook) if getattr(args, "content_hook", None) else None
    return ContentBuilder(
        template_path=getattr(args, "content_template", None),
        md_template_path=getattr(args, "content_md_template", None),
        prepend_html=getattr(args, "prepend_html", None),
        append_html=getattr(args, "append_html", None),
        hook=hook_fn,
    )

# 置き換え: _search_media_by_filename を拡張
def _search_media_by_filename(wp, filename, per_page=5):
    try:
        return wp.search_media_by_filename(filename, per_page=per_page)
    except AttributeError:
        base = os.getenv("WP_URL", "").rstrip("/")
        user = os.getenv("WP_USER", "")
        app  = os.getenv("WP_APP_PASS", "")
        if not (base and user and app):
            return []

        # 1) slug（拡張子無し）で完全一致狙い
        name_noext = os.path.splitext(filename)[0]
        try:
            url = f"{base}/wp-json/wp/v2/media"
            r = requests.get(url, params={"slug": name_noext, "per_page": 1},
                             auth=(user, app), timeout=15)
            r.raise_for_status()
            d = r.json() or []
            if d:  # 見つかればそれを返す
                return d
        except Exception:
            pass

        # 2) それでも無ければ通常の search で候補を返す
        try:
            url = f"{base}/wp-json/wp/v2/media"
            r = requests.get(url, params={"search": filename, "per_page": per_page},
                             auth=(user, app), timeout=20)
            r.raise_for_status()
            return r.json() or []
        except Exception:
            return []


def _ensure_featured_media_mirrored(wp, url: str) -> int | None:
    parsed = urlparse(url)
    fname  = os.path.basename(parsed.path)

    hits = _search_media_by_filename(wp, fname, per_page=15) or []
    if not hits:
        return None

    # 1) source_url 完全一致
    for m in hits:
        su = str(m.get("source_url") or "")
        if su.rstrip("/").lower() == url.rstrip("/").lower():
            return m.get("id")

    # 2) basename の厳密一致（末尾一致ではなく“同名”のみ）
    for m in hits:
        su = str(m.get("source_url") or "")
        if os.path.basename(urlparse(su).path).lower() == fname.lower():
            return m.get("id")

    # 3) どれにも一致しないなら “見つからず” 扱い（誤命中を防ぐ）
    return None


def _ensure_featured_media_external(wp, url: str, row: dict) -> int | None:
    """
    外部URLをダウンロードして /media へアップロード（MIME/拡張子を尊重）。
    既に同名があれば流用。
    """
    parsed = urlparse(url)
    base_name = os.path.basename(parsed.path) or f"{row.get('cid','post')}"
    name_noext, ext = os.path.splitext(base_name)
    if not ext:
        ext = ".jpg"  # 仮の既定

    # 同名が既にあれば流用
    fname_try = f"{row.get('cid','post')}{ext.lower()}"
    hits = _search_media_by_filename(wp, fname_try, per_page=1) or []
    if hits:
        return hits[0]["id"]

    # ダウンロード
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.content

    # MIME 推定（レスポンス優先→拡張子）
    mime = resp.headers.get("Content-Type", "").split(";")[0].strip() or None
    if not mime:
        mime, _ = mimetypes.guess_type(base_name)
    if not mime:
        mime = "image/jpeg"

    # 拡張子を MIME に合わせて調整
    ext_map = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}
    want_ext = ext_map.get(mime, ext.lower())
    if not fname_try.lower().endswith(want_ext):
        fname_try = f"{row.get('cid','post')}{want_ext}"

    # 最終チェック：同名が増えていないか
    hits = _search_media_by_filename(wp, fname_try, per_page=1) or []
    if hits:
        return hits[0]["id"]

    # アップロード
    try:
        if hasattr(wp, "upload_media_bytes"):
            return wp.upload_media_bytes(data, filename=fname_try, mime=mime)
        # フォールバック：upload_media_from_bytes 的なものが無ければ URL 名を使って通常アップロードAPIを呼ぶ
        return wp.upload_media_from_bytes(data, filename=fname_try, mime=mime)  # ある場合
    except AttributeError:
        # 既存の URL アップローダしか無い場合でも、MIME/拡張子を整えた filename を渡す
        return wp.upload_media_from_url(url, fname_try)

def run_pipeline(args) -> Dict[str, Any]:
    # ====== どのProviderを使うかを決定 ======
    site_upper = (args.site or "FANZA").upper()
    is_books = site_upper in ("FANZA_BOOK", "FANZA-BOOK", "FANZA/EBOOK")
    if is_books:
        _fetch = fanza_book.fetch_items
        _normalize = fanza_book.normalize_item
        _build = fanza_book.build_content_html
    else:
        _fetch = fetch_items
        _normalize = normalize_item
        _build = build_content_html
        
    # ====== クエリパラメータ組み立て ======
    params = dict(
        site=args.site,
        service=args.service,
        floor=args.floor,
        keyword=args.keyword,
        cid=args.cid,
        gte_date=args.gte_date,
        lte_date=args.lte_date,
        sort=getattr(args, "sort", "date"),
    )

    # ====== WP 初期化 ======
    wp, cat_ids, tag_ids, status = _init_wp(args)

    # ====== 本文ビルダー初期化（指定がなければ None → レガシー生成） ======
    content_builder = _init_content_builder(args)
    max_gallery = getattr(args, "max_gallery", 12)
    no_content = getattr(args, "no_content", False)

    # ★ テンプレパスをここで確定（未指定なら None）。Windows でも正規化。
    content_template_path = _get_content_template_path(args)
    if content_template_path:
        content_template_path = str(Path(content_template_path))

    out_rows: List[Dict[str, Any]] = []
    offset, got = 1, 0
    # 新規件数の目標/カウンタ
    target_new: int = int(getattr(args, "target_new", 0) or 0)
    new_count: int = 0
    # 既存を更新しない運用
    no_update_existing: bool = bool(getattr(args, "no_update_existing", False))
    debug_dumped = False

    ledger_path = getattr(args, "ledger", None)

    # 起動時にスキップCID集合を準備
    skip_cids = set()
    # A) 互換: 既存の --skip-from-csv / --skip-csv-col が来ていたら尊重
    _from_csv = getattr(args, "skip_from_csv", None)
    if _from_csv:
        skip_cids |= load_skip_cids(_from_csv, colname=getattr(args, "skip_csv_col", "cid"))
    # B) 新規: --auto-skip-outputs が有効なら、outfile のフォルダ（無ければ ./out）を総なめ
    if getattr(args, "auto_skip_outputs", False):
        import os
        base_dir = None
        if getattr(args, "outfile", None):
            base_dir = os.path.dirname(args.outfile) or "."
        if not base_dir:
            base_dir = "out"
        skip_cids |= load_skip_cids_in_dir(base_dir, "*.csv")

    # ====== 取得ループ ======
    while got < args.max and (target_new == 0 or new_count < target_new):
        data = _fetch(args.api_id, args.affiliate_id, params, start=offset, hits=args.hits)
        result = data.get("result") or {}
        items = result.get("items") or []
        if not items:
            break

        # 最初の要素をダンプ（デバッグ用）
        if getattr(args, "debug", False) and not debug_dumped and items:
            import json
            with open("raw_first_item.json", "w", encoding="utf-8") as f:
                json.dump(items[0], f, ensure_ascii=False, indent=2)
            debug_dumped = True

        for it in items:
            # -- 標準化 → 事前フィルタ・補強 --
            row = _normalize(it)

            # === 画像ミラー（本文生成の直前に実施：直前で上書きされるのを防ぐ） ===
            if getattr(args, 'mirror_images', False):
                try:
                    wp2 = wp if ('wp' in locals() and wp) else get_wp_client_from_env()
                except Exception as e:
                    print(f"[mirror] WP client init skipped: {e}")
                    wp2 = None
                if wp2:
                    cid_for_name = (row.get('cid') or row.get('external_id') or f"post{int(time.time())}").lower()
                    try:
                        row = mirror_item_images(row, wp2, cid_for_name)
                        # 置換できたかをログで確認（先頭だけ）
                        print(f"[mirror] sample_images[:1] = { (row.get('sample_images') or '').split('|')[:1] }")
                        print(f"[mirror] image_large = { row.get('image_large') }")
                    except Exception as e:
                        print(f"[mirror] skip {cid_for_name}: {e}")

            # --- （動画のみ）プレイヤーサイズ注入 & iframeサイズ補正 ---
            if not is_books:
                W, H, ratio = get_player_size_from_env()
                row["player_width"] = W
                row["player_height"] = H
                row["aspect_ratio"] = ratio  # 例: 56.25
                if row.get("trailer_embed"):
                    row["trailer_embed"] = row["trailer_embed"].replace("size=1280_720", f"size={W}_{H}")

            # --- CSVベース重複スキップ（記事単位） ---
            cid = (row.get("cid") or "").strip()
            if cid and cid in skip_cids:
                continue

            # （動画のみ）トレーラーフィールドのサニタイズ
            if not is_books:
                row = sanitize_trailer_fields(row)

            row = _filter_and_enhance(row, args)
            if not row:
                continue
            
            # --- 表示用フィールド（テンプレで使う） ---
            row["genres_clean"]  = ",".join([g for g in _split_terms(row.get("genres")) if not _is_stopword_term(g)])
            row["maker_clean"]   = _norm(row.get("maker"))
            row["actress_clean"] = ",".join(_split_terms(row.get("actress")))

            # -- 後段フィルタ（キーワード・除外語など） --
            filtered = apply_filters([row], args)
            if not filtered:
                continue
            row = filtered[0]

            # -- 本文生成（テンプレ優先。未指定/欠落時は従来HTML）--
            if content_template_path:
                p = Path(content_template_path)
                if not p.exists():
                    log_json("error", where="content_build",
                             error=f"template_not_found: {p}", cid=row.get("cid"))
                    # テンプレが物理的に無い時だけ従来HTMLへ落とす
                    row["content"] = "<!-- tpl:missing -->" + \
                                     _build(row, content_builder=None, max_gallery=max_gallery)
                else:
                    # ① post.html.j2 を直接描画
                    try:
                        # ContentBuilderはrender_fileを持たない実装なので、
                        # build_content_htmlにContentBuilder(template_path=...)を渡して描画する
                        cb = ContentBuilder(template_path=str(p))
                        html = _build(row, content_builder=cb, max_gallery=max_gallery)
                        row["content"] = "<!-- tpl:post -->" + html
                    except Exception as e1:
                        # ここで落ちたら“原因を隠さない”ために安易に content.html.j2 へは落とさない
                        log_json("error", where="content_build",
                                 error=f"post_tpl_render_failed: {e1}", cid=row.get("cid"))
                        # 最低限の保険だけ（従来HTML）。コメントで判別できるようにする
                        try:
                            fallback = _build(row, content_builder=None, max_gallery=max_gallery)
                            row["content"] = "<!-- tpl:content(fallback) -->" + fallback
                        except Exception as e2:
                            log_json("error", where="content_build",
                                     error=f"legacy_build_failed: {e2}", cid=row.get("cid"))
                            row["content"] = ""
            else:
                # テンプレ未指定：従来HTML
                row["content"] = "<!-- tpl:content(default) -->" + \
                                 _build(row, content_builder=None, max_gallery=max_gallery)

            # -- CSV運用（WPなし）の“新規”定義：skip_cids に無い → 新規として採用
            #    out_rows に入れたら new_count を加算
            if not wp:
                out_rows.append(row)
                if target_new:
                    new_count += 1
                if ledger_path:
                    try:
                        import datetime as _dt
                        row_for_ledger = dict(row)
                        row_for_ledger["exported_at"] = _dt.datetime.utcnow().isoformat()
                        append_ledger(ledger_path, row_for_ledger)
                    except Exception as _e:
                        log_json("warn", where="append_ledger", error=str(_e), cid=row.get("cid"))
                continue  # WPなしはここで次のアイテムへ

            # -- 直投稿（任意） --
            if wp:
                # 事前に“既存かどうか”を判定（更新しない運用/新規カウントに使う）
                existing_pid = None
                try:
                    existing_pid = wp.find_post_id_by_external(cid) if cid else None
                except Exception:
                    existing_pid = None

                # 既存を更新しないフラグが立っていて、既存ヒット → スキップ
                if no_update_existing and existing_pid:
                    continue
                
                # --- SEOメタ生成 ---
                seo = build_seo_fields(row, site_name=_os.getenv("SITE_NAME") or "")
                meta_extra = {"provider": "FANZA", **build_wp_seo_meta(seo)}

                # ★ アイキャッチ（ジャケット）— ID最優先で確実に
                feat_id = row.get("image_large_id") or row.get("trailer_poster_id")
                if not feat_id:
                    def _pick_feat_url(r):
                        # image_large → trailer_poster → samples[0]
                        return r.get("image_large") or r.get("trailer_poster") \
                               or ( (r.get("sample_images") or "").split("|")[0] if (r.get("sample_images")) else None )
                    feat_url = _pick_feat_url(row)
                    if feat_url:
                        try:
                            if getattr(args, "mirror_images", False):
                                # ミラー運用：uploads 内から ID を逆引き（再アップしない）
                                feat_id = _ensure_featured_media_mirrored(wp, feat_url)
                            else:
                                # 非ミラー運用：外部から取得してアップロード
                                feat_id = _ensure_featured_media_external(wp, feat_url, row)
                        except Exception as e:
                            log_json("warn", where="featured_media", error=str(e), url=feat_url, cid=row.get("cid"))
                if feat_id and hasattr(wp, "_req"):
                    # ここを NameError 無しに修正（det/meta未定義の旧ログを全撤去）
                    try:
                        feat_id = int(feat_id)
                        meta = wp._req("GET", f"/wp-json/wp/v2/media/{feat_id}") or {}
                        details = meta.get("media_details") or {}
                        w = details.get("width"); h = details.get("height")
                        src = meta.get("source_url")
                    except Exception as e:
                        print(f"[dbg] featured_media meta err: {e}")

                # --- 作品ごとのカテゴリ（女優）とタグを算出 ---
                actresses = [_norm(x) for x in _split_terms(row.get("actress"))]
                if actresses:
                    # カテゴリは“女優名のカテゴリ”を1つだけ付ける（ナビ崩れ防止）
                    cat_ids_for_post = wp.ensure_categories([actresses[0]])
                else:
                    cat_ids_for_post = cat_ids or []

                base_tag_names = []
                for s in (args.wp_tags or "").split(","):
                    n = _norm(s)
                    if n and not _is_stopword_term(n):
                        base_tag_names.append(n)

                dyn_tag_names = []
                # 女優はタグとしても付与（テーマ側の横断検索に有利）
                dyn_tag_names += actresses
                # ジャンル（複数）からノイズ語を除外
                dyn_tag_names += [g for g in _split_terms(row.get("genres")) if not _is_stopword_term(g)]
                # 単一値系
                for key in ("maker", "label", "series"):
                    v = _norm(row.get(key))
                    if v and not _is_stopword_term(v):
                        dyn_tag_names.append(v)

                # 重複除去＋上限
                seen = set(); merged_tag_names = []
                for t in base_tag_names + dyn_tag_names:
                    if t and t not in seen:
                        seen.add(t); merged_tag_names.append(t)
                merged_tag_names = merged_tag_names[:15]

                tag_ids_for_post = wp.ensure_tags(merged_tag_names)

                # 投稿タイトル整形
                def _fmt_title(r):
                    cid    = (r.get("cid") or "").strip()
                    t      = (r.get("title") or "").strip()
                    # 著者名（actress_clean があれば優先）
                    author = (r.get("actress_clean") or r.get("actress") or "").strip()
                    # サークル名（maker_clean 優先）
                    circle = (r.get("maker_clean") or r.get("maker") or "").strip()

                    svc   = (getattr(args, "service", "") or "").lower()
                    floor = (getattr(args, "floor", "") or "").lower()

                    # 1) 電子書籍（エロ漫画）: FANZA_BOOK or digital/comic + ebook
                    is_ebook_service = svc in ("digital", "comic") and ("ebook" in floor)
                    if is_books or is_ebook_service:
                        if author:
                            return f"[{author}] {t}"
                        return t  # 著者が取れないときは素のタイトル

                    # 2) 同人: service=doujin or floor に doujin を含む
                    if svc == "doujin" or "doujin" in floor:
                        if circle:
                            return f"[{circle}] {t}"
                        # 一応、author があればそっちも使う
                        if author:
                            return f"[{author}] {t}"
                        return t

                    # 3) それ以外（動画など）は従来通り [品番] タイトル
                    return f"[{cid}] {t}" if cid else t

                if status == "future" and getattr(args, "future_datetime", None):
                    pid, link = wp.create_or_update_post(
                        title=_fmt_title(row),
                        content=row.get("content", ""),
                        status="future",
                        categories=cat_ids_for_post,
                        tags=tag_ids_for_post,
                        external_id=row.get("cid", ""),
                        meta_extra=meta_extra,
                        excerpt=seo.get("description"),
                        date=args.future_datetime,
                        featured_media=feat_id,
                    )
                else:
                    pid, link = wp.create_or_update_post(
                        title=_fmt_title(row),
                        content=row.get("content", ""),
                        status=status,
                        categories=cat_ids_for_post,
                        tags=tag_ids_for_post,
                        external_id=row.get("cid", ""),
                        meta_extra=meta_extra,
                        excerpt=seo.get("description"),
                        featured_media=feat_id,
                    )

                # “新規”だったらカウント（既存→更新のケースは加算しない）
                if target_new and pid and not existing_pid:
                    new_count += 1

                # NEW: WP投稿が成功した分だけ ledger に posted_at 付きで記帳
                try:
                    if ledger_path and pid:
                        import datetime as _dt
                        row_for_ledger = dict(row)
                        row_for_ledger["posted_at"] = _dt.datetime.utcnow().isoformat()
                        append_ledger(ledger_path, row_for_ledger)
                except Exception as _e:
                    log_json("warn", where="append_ledger", error=str(_e), cid=row.get("cid"))

                log_json("info", action="wp_posted", id=pid, link=link, cid=row.get("cid"))

        got += len(items)
        offset += len(items)
        total = result.get("total_count") or 0
        if offset > total:
            break
        time.sleep(args.sleep)

    # ====== CSV 出力（--outfile 指定時） ======
    if getattr(args, "outfile", None):
        # 既存のカラムを維持。必要なら fieldnames をプロジェクト都合で拡張可。
        BASE_FIELDS = [
            "cid", "title", "URL", "date",
            "maker", "actress", "genres",
            "sample_images", "image_large",
            "trailer_url",        # mp4 直リンク（あれば）
            "trailer_youtube",    # YouTube ID/URL（あれば）
            "trailer_poster",     # ポスター画像（あれば）
            "trailer_embed",      # 生iframe等（通常は空）
            "content",
            "aspect_ratio",
        ]
        all_keys = set()
        for r in out_rows:
            all_keys.update(r.keys())

        fieldnames = [c for c in BASE_FIELDS if c in all_keys] + \
                    [c for c in all_keys if c not in BASE_FIELDS]  # ← player_width 等も書ける

        with open(args.outfile, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(out_rows)

    return {
        "fetched": got,
        "kept": len(out_rows),
        "wp_posted": None,  # 各行はログ出力済み
        "outfile": getattr(args, "outfile", None),
        "status": "ok",
    }
