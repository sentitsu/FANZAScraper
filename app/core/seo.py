import re, os

def _cut(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[:n].rstrip() + "…"

def build_seo_fields(item: dict, site_name: str | None = None) -> dict:
    title = (item.get("title") or "").strip()
    site  = site_name or os.getenv("SITE_NAME") or ""
    seo_title = _cut(f"{title} | {site}".strip(" |"), 32)

    parts = []
    if item.get("actress"): parts.append(item["actress"])
    if item.get("maker"):   parts.append(item["maker"])
    if item.get("genres"):  parts.append(item["genres"])
    seo_desc = _cut(" / ".join([p for p in parts if p]) or title, 120)

    kws = []
    if item.get("genres"):
        kws += [g.strip() for g in re.split(r"[、,\s/・]+", item["genres"]) if g.strip()]
    if item.get("actress"):
        kws += [a.strip() for a in re.split(r"[、,\s/・]+", item["actress"]) if a.strip()]
    seen, uniq = set(), []
    for k in kws:
        if k not in seen:
            seen.add(k); uniq.append(k)
    keywords = ", ".join(uniq[:10])

    return {"title": seo_title, "description": seo_desc,
            "keywords": keywords, "noindex": False, "nofollow": False}

def build_wp_seo_meta(seo: dict) -> dict:
    focus_kw = (seo["keywords"].split(",")[0] if seo["keywords"] else "").strip()
    robots   = ("noindex" if seo["noindex"] else "index") + "," + \
               ("nofollow" if seo["nofollow"] else "follow")
    return {
        # Yoast
        "_yoast_wpseo_title": seo["title"],
        "_yoast_wpseo_metadesc": seo["description"],
        "_yoast_wpseo_meta-robots-noindex": bool(seo["noindex"]),
        # All in One SEO
        "_aioseop_title": seo["title"],
        "_aioseop_description": seo["description"],
        "_aioseop_keywords": seo["keywords"],
        "aioseop_noindex":  bool(seo["noindex"]),
        "aioseop_nofollow": bool(seo["nofollow"]),
        # Rank Math
        "rank_math_title": seo["title"],
        "rank_math_description": seo["description"],
        "rank_math_focus_keyword": focus_kw,
        "rank_math_robots": robots,
        # Cocoon（候補）
        "_seo_title": seo["title"],
        "_seo_description": seo["description"],
        "_seo_keywords": seo["keywords"],
        "noindex":  bool(seo["noindex"]),
        "nofollow": bool(seo["nofollow"]),
    }
