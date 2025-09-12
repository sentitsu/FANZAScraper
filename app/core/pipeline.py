# app/core/pipeline.py
import time, csv
from typing import Dict, Any, List
from app.providers.fanza import fetch_items, normalize_item, build_content_html, \
    _is_now_printing_url_like, _probe_is_placeholder, _pick_best_feature
from app.core.wp_rest import WPClient
from app.core.filters import apply_filters
from app.util.logger import log_json

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
    # content生成
    if not args.no_content:
        row["content"] = build_content_html(row, max_gallery=args.max_gallery)

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

def run_pipeline(args) -> Dict[str, Any]:
    params = dict(site=args.site, service=args.service, floor=args.floor,
                  keyword=args.keyword, cid=args.cid,
                  gte_date=args.gte_date, lte_date=args.lte_date, 
                  sort=getattr(args, "sort", "date"))

    wp, cat_ids, tag_ids, status = _init_wp(args)

    out_rows: List[Dict[str, Any]] = []
    offset, got = 1, 0
    debug_dumped = False

    while got < args.max:
        data = fetch_items(args.api_id, args.affiliate_id, params, start=offset, hits=args.hits)
        result = data.get("result") or {}
        items  = result.get("items") or []
        if not items:
            break

        if args.debug and not debug_dumped and items:
            import json
            with open("raw_first_item.json", "w", encoding="utf-8") as f:
                json.dump(items[0], f, ensure_ascii=False, indent=2)
            debug_dumped = True

        for it in items:
            row = normalize_item(it)
            row = _filter_and_enhance(row, args)
            if not row:
                continue

            filtered = apply_filters([row], args)
            if not filtered:
                continue
            row = filtered[0]

            # CSV出力用バッファ
            out_rows.append(row)

            # 直投稿（任意）
            if wp:
                meta_extra = {"provider": "FANZA"}
                if status == "future" and args.future_datetime:
                    pid, link = wp.create_or_update_post(
                        title=row.get("title",""),
                        content=row.get("content",""),
                        status="future",
                        categories=cat_ids, tags=tag_ids,
                        external_id=row.get("cid",""),
                        meta_extra=meta_extra,
                        date=args.future_datetime,
                    )
                else:
                    pid, link = wp.create_or_update_post(
                        title=row.get("title",""),
                        content=row.get("content",""),
                        status=status,
                        categories=cat_ids, tags=tag_ids,
                        external_id=row.get("cid",""),
                        meta_extra=meta_extra,
                    )
                log_json("info", action="wp_posted", id=pid, link=link, cid=row.get("cid"))

        got += len(items)
        offset += len(items)
        total = result.get("total_count") or 0
        if offset > total:
            break
        time.sleep(args.sleep)

    # CSV 出力（--outfile が指定されていれば）
    if args.outfile:
        fieldnames = ["cid","title","URL","date","maker","actress","genres","sample_images","image_large","content"]
        with open(args.outfile, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(out_rows)

    return {
        "fetched": got,
        "kept": len(out_rows),
        "wp_posted": None,  # 各行はログ出力済み
        "outfile": args.outfile,
        "status": "ok",
    }
