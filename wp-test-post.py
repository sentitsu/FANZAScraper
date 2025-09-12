import requests, base64

# WordPress設定
WP_URL = "https://javmix-av.com/wp-json/wp/v2/posts"
USERNAME = "botposter"
APP_PASSWORD = "H4gB uLiU nP68 a3v6 UZKZ eCeL"  # 発行して保管したやつ

# Basic Auth ヘッダ
token = base64.b64encode(f"{USERNAME}:{APP_PASSWORD}".encode()).decode("utf-8")
headers = {"Authorization": f"Basic {token}"}

# 記事データ
data = {
    "title": "テスト投稿",
    "content": "<p>本文HTMLここ</p>",
    "status": "draft",   # draft/publish
    "meta": {
        "external_id": "CID12345"  # MUプラグインで登録した外部ID
    }
}

# 投稿実行
res = requests.post(WP_URL, headers=headers, json=data)
print(res.status_code, res.json())