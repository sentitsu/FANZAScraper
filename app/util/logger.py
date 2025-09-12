# app/util/logger.py
import json, sys, datetime

def log_json(level: str, **fields):
    doc = {"ts": datetime.datetime.utcnow().isoformat()+"Z", "level": level} | fields
    print(json.dumps(doc, ensure_ascii=False), file=sys.stdout, flush=True)
