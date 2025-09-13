# app/core/pipeline.py
import time, csv
from typing import Dict, Any, List
from app.providers.fanza import fetch_items, normalize_item, build_content_html, \
    _is_now_printing_url_like, _probe_is_placeholder, _pick_best_feature
from app.core.wp_rest import WPClient
from app.core.filters import apply_filters
from app.util.logger import log_json
from importlib import import_module
from app.core.content_builder import ContentBuilder

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


def run_pipeline(args) -> Dict[str, Any]:
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

    # ====== 本文ビルダー初期化（指定が無ければ None → レガシー生成） ======
    content_builder = _init_content_builder(args)
    max_gallery = getattr(args, "max_gallery", 12)
    no_content = getattr(args, "no_content", False)

    out_rows: List[Dict[str, Any]] = []
    offset, got = 1, 0
    debug_dumped = False

    # ====== 取得ループ ======
    while got < args.max:
        data = fetch_items(args.api_id, args.affiliate_id, params, start=offset, hits=args.hits)
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
            row = normalize_item(it)
            row = _filter_and_enhance(row, args)
            if not row:
                continue

            # -- 後段フィルタ（キーワード・除外語など） --
            filtered = apply_filters([row], args)
            if not filtered:
                continue
            row = filtered[0]

            # -- 本文生成（必要なら）--
            if not no_content:
                # まずはテンプレ駆動（ContentBuilder 経由）
                try:
                    row["content"] = build_content_html(
                        row,
                        content_builder=content_builder,
                        max_gallery=max_gallery,
                    )
                except Exception:
                    row["content"] = ""
                # 空なら必ずフォールバック（黒/無表示を回避）
                if not row.get("content") or not row["content"].strip():
                    row["content"] = build_content_html(
                        row,
                        content_builder=None,   # ←テンプレ無視で従来HTML
                        max_gallery=max_gallery,
                    )

            # -- CSV バッファ --
            out_rows.append(row)

            # -- 直投稿（任意） --
            if wp:
                meta_extra = {"provider": "FANZA"}

                # ★ アイキャッチ（ジャケット）を明示設定
                feat_url = row.get("image_large") or row.get("trailer_poster")
                feat_id = None
                if feat_url:
                    try:
                        # 同一CIDで毎回同じ名前にしておくとライブラリで識別しやすい
                        fname = f'{row.get("cid","post")}.jpg'
                        feat_id = wp.upload_media_from_url(feat_url, fname)
                    except Exception as e:
                        log_json("warn", where="upload_media", error=str(e), url=feat_url, cid=row.get("cid"))


                if status == "future" and getattr(args, "future_datetime", None):
                    pid, link = wp.create_or_update_post(
                        title=row.get("title", ""),
                        content=row.get("content", ""),
                        status="future",
                        categories=cat_ids,
                        tags=tag_ids,
                        external_id=row.get("cid", ""),
                        meta_extra=meta_extra,
                        date=args.future_datetime,
                        featured_media=feat_id,
                    )
                else:
                    pid, link = wp.create_or_update_post(
                        title=row.get("title", ""),
                        content=row.get("content", ""),
                        status=status,
                        categories=cat_ids,
                        tags=tag_ids,
                        external_id=row.get("cid", ""),
                        meta_extra=meta_extra,
                        featured_media=feat_id,
                    )

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
        fieldnames = [
            "cid", "title", "URL", "date",
            "maker", "actress", "genres",
            "sample_images", "image_large",
            "trailer_url",        # mp4 直リンク（あれば）
            "trailer_youtube",    # YouTube ID/URL（あれば）
            "trailer_poster",     # ポスター画像（あれば）
            "trailer_embed",      # 生iframe等（通常は空）
            "content",
        ]
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