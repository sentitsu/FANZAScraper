import re, os

def _cut(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[:n].rstrip() + "…"

# --- 追加: keywords 用ヘルパー -------------------------
def _cid_variants(cid: str) -> list[str]:
    """品番の表記ゆれ（ハイフン有無/ゼロ詰め有無）を揃えて返す"""
    s = (cid or "").strip().upper().replace(" ", "")
    if not s:
        return []
    m = re.match(r"([A-Z]+)[-_]?(0*)(\d+)", s)
    if not m:
        return [s]
    pre, zeros, num = m.groups()
    num_no_zero = str(int(num))  # 先頭0除去
    return [f"{pre}-{num_no_zero}", f"{pre}{num_no_zero}", f"{pre}-{num}", f"{pre}{num}", s]

def _dedup(seq):
    """順序維持で重複除去"""
    seen = set(); out = []
    for x in seq:
        if x and x not in seen:
            out.append(x); seen.add(x)
    return out


def build_seo_fields(row, site_name=None):
    cid   = (row.get("cid") or "").strip()
    title = (row.get("title") or "").strip()
    maker = (row.get("maker") or "").strip()
    actress = (row.get("actress") or "").replace("/", "、").strip()
    actresses = [a.strip() for a in actress.split("、") if a.strip()]

    # description（自然文・110〜120字）
    bits = []
    if cid: bits.append(f"[{cid}]")
    if title: bits.append(title)
    if maker: bits.append(f"（{maker}）")
    lead = "".join(bits)
    tail = "。"
    if actress: tail += f"出演：{actress}。"
    tail += "公式でサンプル動画と特典をチェック。"
    desc = _cut(lead + tail, 120)

    # keywords（使わないが保守用）
    # 女優名（複数可）＋品番バリエーション＋メーカー
    kw_list = []
    # 先頭を品番に（先頭しか見ない実装に合わせた保険）
    kw_list += _cid_variants(cid)
    kw_list += actresses
    if maker: kw_list.append(maker)
    keywords = ",".join(_dedup(kw_list))

    return {"title": title, "description": desc, "keywords": keywords, "noindex": False, "nofollow": False}

def build_wp_seo_meta(seo: dict, *, write_titles: bool = False, rankmath_use_external_id: bool = False) -> dict:
    """
    write_titles=False … 各SEOプラグインの「タイトル上書き」を送らない（推奨）
    rankmath_use_external_id=True … Rank Math にだけ [external_id] を含むテンプレを送る
                                    （write_titles=True のときだけ有効）
    """
    title   = (seo.get("title") or "").strip()
    desc    = (seo.get("description") or "").strip()
    kw      = (seo.get("keywords") or "").strip()
    focus_kw = (kw.split(",")[0] if kw else "").strip()

    noindex = bool(seo.get("noindex"))
    nofollow = bool(seo.get("nofollow"))
    robots = ("noindex" if noindex else "index") + "," + ("nofollow" if nofollow else "follow")

    meta = {
        # --- Rank Math（タイトルはデフォ送信しない） ---
        "rank_math_description": desc,
        "rank_math_focus_keyword": focus_kw,
        "rank_math_robots": robots,

        # --- Yoast ---
        "_yoast_wpseo_metadesc": desc,
        "_yoast_wpseo_meta-robots-noindex": noindex,

        # --- All in One SEO ---
        "_aioseop_description": desc,
        "_aioseop_keywords": kw,
        "aioseop_noindex":  noindex,
        "aioseop_nofollow": nofollow,

        # --- Cocoon（公式フィールドIDで保存する） ---
        "the_page_meta_description": desc,
        "the_page_meta_keywords": kw,      # ← ここが肝
        "the_page_noindex": noindex,
        "the_page_nofollow": nofollow,
        # 互換のため旧キーも残すなら下2行は任意
        "_seo_description": desc,
        "_seo_keywords": kw,
        "noindex":  noindex,
        "nofollow": nofollow,
    }

    if write_titles:
        # ここを有効にすると “プラグインで” タイトルを上書きする
        meta.update({
            "_yoast_wpseo_title": title,
            "_aioseop_title": title,
            "the_page_seo_title": title,      # CocoonのSEOタイトル
            "_seo_title": title,              # 互換（任意）
        })
        if rankmath_use_external_id:
            # WPメタ external_id（= CID）を Rank Math のテンプレで参照
            # 例: [CID] タイトル | サイト名
            meta["rank_math_title"] = "[%customfield(external_id)%] %title% %sep% %sitename%"
        else:
            meta["rank_math_title"] = title

    return meta
