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
import os
from app.core.seo import build_seo_fields, build_wp_seo_meta
from app.core.csv_dedupe import load_skip_cids, append_ledger, load_skip_cids_in_dir

# --- 女優・ジャンル等を分割する小関数とタグのストップワード ---
def _split_terms(s: str | None) -> list[str]:
    """空白/カンマ/読点/中黒/スラッシュで分割してトリム。None→[]。"""
    return [x.strip() for x in re.split(r"[、,\s/・/]+", (s or "")) if x.strip()]

TAG_STOPWORDS = set(["ハイビジョン", "サンプル", "動画", "独占配信"])

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

            W, H, ratio = get_player_size_from_env()
            row["player_width"] = W
            row["player_height"] = H
            row["aspect_ratio"] = ratio  # 例: 56.25
            
            # 既存の iframe 埋め込みURL（例: item["trailer_embed"]）の size= を.envに合わせて置換
            if row.get("trailer_embed"):
                row["trailer_embed"] = row["trailer_embed"].replace("size=1280_720", f"size={W}_{H}")

            # NEW: CSVベースの重複スキップ（cid が既にCSVに存在していたら処理しない）
            cid = (row.get("cid") or "").strip()
            if cid and cid in skip_cids:
                continue

            row = sanitize_trailer_fields(row)
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
                seo = build_seo_fields(row, site_name=os.getenv("SITE_NAME"))
                meta_extra = {"provider": "FANZA", **build_wp_seo_meta(seo)}

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

                # --- 作品ごとのカテゴリ（女優）とタグを算出 ---
                actresses = _split_terms(row.get("actress"))
                if actresses:
                    # カテゴリは“女優名のカテゴリ”を1つだけ付ける（ナビ崩れ防止）
                    cat_ids_for_post = wp.ensure_categories([actresses[0]])
                else:
                    cat_ids_for_post = cat_ids or []

                base_tag_names = [s.strip() for s in (args.wp_tags or "").split(",") if s.strip()]

                dyn_tag_names = []
                # 女優はタグとしても付与（テーマ側の横断検索に有利）
                dyn_tag_names += actresses
                # ジャンル（複数）からノイズ語を除外
                dyn_tag_names += [g for g in _split_terms(row.get("genres")) if g not in TAG_STOPWORDS]
                # 単一値系
                for key in ("maker", "label", "series"):
                    v = (row.get(key) or "").strip()
                    if v:
                        dyn_tag_names.append(v)

                # 重複除去＋上限
                seen = set(); merged_tag_names = []
                for t in base_tag_names + dyn_tag_names:
                    if t and t not in seen:
                        seen.add(t); merged_tag_names.append(t)
                merged_tag_names = merged_tag_names[:15]

                tag_ids_for_post = wp.ensure_tags(merged_tag_names)

                # 品番つきタイトル（WP投稿用）
                def _fmt_title(r):
                    cid = (r.get("cid") or "").strip()
                    t   = (r.get("title") or "").strip()
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