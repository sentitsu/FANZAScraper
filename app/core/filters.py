# app/core/filters.py
import re

def _match_any(patterns, text):
    return any(re.search(p, text or "", re.I) for p in (patterns or []))

def _check_field(text, includes, excludes):
    if includes and not _match_any(includes, text):
        return False
    if excludes and _match_any(excludes, text):
        return False
    return True

def apply_filters(rows, args):
    """rows: normalize後の辞書配列（cid/title/maker/actress/genres/...）"""
    out = []
    for r in rows:
        if not _check_field(r.get("maker",""),   args.include_maker,   args.exclude_maker):   continue
        if not _check_field(r.get("actress",""), args.include_actress, args.exclude_actress): continue
        if not _check_field(r.get("genres",""),  args.include_genre,   args.exclude_genre):   continue
        if not _check_field(r.get("title",""),   args.include_title,   args.exclude_title):   continue

        cid = r.get("cid","")
        if args.include_cid_prefix and not _match_any(args.include_cid_prefix, cid): continue
        if args.exclude_cid_prefix and _match_any(args.exclude_cid_prefix, cid):     continue

        out.append(r)
    return out
