# app/main.py
import os, argparse, json
from dotenv import load_dotenv
from app.core.pipeline import run_pipeline
from app.util.logger import log_json
from pathlib import Path
import sys

def build_args():
    ap = argparse.ArgumentParser(description="FANZA → WordPress 自動投稿（CSV/REST両対応）")
    # 取得系
    ap.add_argument("--api-id", default=os.getenv("API_ID"))
    ap.add_argument("--affiliate-id", default=os.getenv("AFFILIATE_ID"))
    
    ap.add_argument("--site", default="FANZA")
    ap.add_argument("--service", default="digital")
    ap.add_argument("--floor", default="videoa")
    ap.add_argument("--keyword", default=None)
    ap.add_argument("--cid", default=None)
    ap.add_argument("--gte-date", dest="gte_date", default=None)
    ap.add_argument("--lte-date", dest="lte_date", default=None)
    ap.add_argument("--hits", type=int, default=100)
    ap.add_argument("--max", type=int, default=500)
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--debug", action="store_true")

    ap.add_argument("--sort", choices=["date","-date","rank","-rank","price","-price"], default="date")

    # 足切り/整形
    ap.add_argument("--verify-images", action="store_true")
    ap.add_argument("--min-samples", type=int, default=1)
    ap.add_argument("--release-after", default=None)
    ap.add_argument("--skip-placeholder", action="store_true")
    ap.add_argument("--max-gallery", type=int, default=12)
    ap.add_argument("--no-content", action="store_true")

    ap.add_argument("--include-maker", action="append", default=[])
    ap.add_argument("--exclude-maker", action="append", default=[])
    ap.add_argument("--include-actress", action="append", default=[])
    ap.add_argument("--exclude-actress", action="append", default=[])
    ap.add_argument("--include-genre", action="append", default=[])
    ap.add_argument("--exclude-genre", action="append", default=[])
    ap.add_argument("--include-title", action="append", default=[])
    ap.add_argument("--exclude-title", action="append", default=[])
    ap.add_argument("--include-cid-prefix", action="append", default=[], help="品番の先頭にマッチ（例: ^SSIS|^ABW）")
    ap.add_argument("--exclude-cid-prefix", action="append", default=[])

    # HEAD判定の挙動制御
    ap.add_argument("--no-head-check", action="store_true", help="画像のHEAD確認を行わない（URLヒューリスティックのみ）")
    ap.add_argument("--head-timeout", type=float, default=3.0, help="画像HEADのタイムアウト秒（既定3s）")
    ap.add_argument("--head-insecure", action="store_true", help="HEAD時にSSL検証を無効化（verify=False）")

    # 出力
    ap.add_argument("--outfile", default="out/fanza_items.csv")

    # WordPress（REST直投稿）
    ap.add_argument("--wp-post", action="store_true")
    ap.add_argument("--wp-url", default=os.getenv("WP_URL"))
    ap.add_argument("--wp-user", default=os.getenv("WP_USER"))
    ap.add_argument("--wp-app-pass", default=os.getenv("WP_APP_PASS"))
    ap.add_argument("--wp-categories", default=os.getenv("WP_CATEGORIES", ""))
    ap.add_argument("--wp-tags", default=os.getenv("WP_TAGS", ""))
    ap.add_argument("--publish", action="store_true")           # 即時公開
    ap.add_argument("--future-datetime", default=None)          # 予約投稿 (ISO: 2025-09-11T21:00:00)
    return ap


def run():
    # PyInstaller対策: exe / スクリプト いずれでも「自身のフォルダの .env」を読む
    base_dir = Path(sys.argv[0]).parent
    load_dotenv(dotenv_path=base_dir / ".env")  # ← 追加

    args = build_args().parse_args()
    result = run_pipeline(args)
    log_json("info", **result)

if __name__ == "__main__":
    run()
