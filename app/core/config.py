# app/core/config.py
import os
from dotenv import load_dotenv
load_dotenv()

AFFILIATE_ID = os.getenv("AFFILIATE_ID", "").strip()
IFRAME_SIZE  = os.getenv("FANZA_IFRAME_SIZE", "1280_720").strip()

def _parse_size(s: str):
    try:
        w, h = map(int, s.split("_", 1))
    except Exception:
        w, h = 1280, 720
    return w, h, round(h / w * 100, 2)

FANZA_IFRAME_W, FANZA_IFRAME_H, FANZA_IFRAME_RATIO = _parse_size(IFRAME_SIZE)