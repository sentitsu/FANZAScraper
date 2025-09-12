import re

def transform(item, html: str) -> str:
    # タイトルの余計な記号を除去
    html = re.sub(r"【.*?】", "", html)

    # NGワードをマスク
    for w in ["過激表現A", "過激表現B"]:
        html = html.replace(w, "◯◯")

    # 作品カテゴリに応じて追記（例）
    if "単体" in (item.get("genres") or []):
        html += '<p class="note">編集部メモ：単体作品は初見さんにもおすすめ。</p>'

    return html