import csv, glob, os
from pathlib import Path

def load_skip_cids(patterns, colname="cid"):
    """patterns: 文字列 or カンマ区切り or グロブ(例: data/*.csv)
       colname : CIDが入っている列名（デフォ 'cid'）
    """
    if not patterns:
        return set()

    # パターンを列挙
    if isinstance(patterns, str):
        parts = []
        for p in patterns.split(","):
            parts.extend(glob.glob(p.strip()) or [p.strip()])
        paths = [p for p in parts if os.path.exists(p)]
    else:
        paths = []
        for p in patterns:
            paths.extend(glob.glob(p) or [p])
        paths = [p for p in paths if os.path.exists(p)]

    skip = set()
    for path in paths:
        try:
            # UTF-8(BOM含む)想定。必要なら cp932 等のフォールバック追加可
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if colname not in reader.fieldnames:
                    # "content_id" や "CID" などの別名がある場合のフォールバック
                    alt = next((c for c in reader.fieldnames if c.lower() == "cid" or "content_id" in c.lower()), None)
                    key = alt or colname
                else:
                    key = colname
                for row in reader:
                    v = (row.get(key) or "").strip()
                    if v:
                        skip.add(v)
        except Exception:
            # 壊れたCSVはスキップ（ログ出力は好みで）
            continue
    return skip


def append_ledger(path, row_dict, field_order=None):
    """処理済み行を台帳CSVに追記。pathが無ければヘッダ付きで新規作成。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    is_new = not os.path.exists(path)
    # フィールド順は固定化すると後々楽
    fields = field_order or [
        "cid","title","date","maker","actress","URL","image_large","sample_images","posted_at"
    ]
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if is_new:
            w.writeheader()
        w.writerow(row_dict)

# NEW: 出力ディレクトリ配下のCSVを自動スキャンしてCID集合を作る
def load_skip_cids_in_dir(out_dir: str, glob_pattern: str = "*.csv") -> set[str]:
    """
    out_dir 以下の CSV を総なめにして CID を集める。
    列名は優先的に 'cid'、無ければ 'content_id' 等のそれっぽい列を自動推定。
    """
    skip: set[str] = set()
    if not out_dir:
        return skip
    p = Path(out_dir)
    if not p.exists():
        return skip

    paths = sorted(p.glob(glob_pattern))
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                fns = [x or "" for x in reader.fieldnames]
                # 優先順に列名を推定
                key = None
                for cand in ("cid", "CID", "content_id", "ContentID", "contentId"):
                    if cand in fns:
                        key = cand
                        break
                if not key:
                    # 小文字化して 'cid' を探す
                    lowers = {c.lower(): c for c in fns}
                    if "cid" in lowers:
                        key = lowers["cid"]
                if not key:
                    # どうしても見つからなければこのCSVはスキップ
                    continue
                for row in reader:
                    v = (row.get(key) or "").strip()
                    if v:
                        skip.add(v)
        except Exception:
            continue
    return skip