## 事前準備

### .env書き込み

以下を自分の情報に書き換えます：

API_ID=xxxxxxxxxxxxxxxx       ← DMMアフィリエイトAPIのID
AFFILIATE_ID=yyyyyyyyyyyyyy   ← DMMアフィリエイトID
WP_URL=https://your-site.com  ← WordPressサイトURL
WP_USER=wp_username           ← WordPressのユーザー名
WP_APP_PASS=xxxx xxxx xxxx xxxx   ← アプリケーションパスワード
WP_CATEGORIES=単体,専属        ← 投稿時に付与するカテゴリ（カンマ区切り）
WP_TAGS=デビュー,SSIS          ← 投稿時に付与するタグ（カンマ区切り）


.env は exeと同じフォルダに置いてください。

## GUI の使い方（fanza_scraper_desktop.exe）

fanza_scraper_desktop.exe をダブルクリックで起動

タブで条件を設定

基本：取得件数、ソート順、出力CSVの保存先など

フィルタ：メーカー/女優/ジャンル/タイトル/品番プレフィックスを include/exclude

WordPress：--wp-post にチェック → 即時公開か予約投稿を選択

高度：画像のHEADチェック（速度 vs 精度）を調整

実行ボタンを押すとログが流れ、完了するとCSVが出力されます。
WordPress投稿を有効にした場合は、そのままサイトに下書き/公開/予約されます。

### 各パラメーター詳細
取得系（APIに渡す条件）

--api-id [ID] / --affiliate-id [ID]
DMMアフィAPIのキー。環境変数 API_ID / AFFILIATE_ID でも可。

--site [FANZA]（FANZA） / --service [digital]（digital） / --floor [videoa]（videoa）
取得するカタログの種別。

--keyword "語句"
キーワード検索（APIの挙動に依存）。

--cid SSIS-123
品番でピンポイント取得。

--gte-date YYYY-MM-DD / --lte-date YYYY-MM-DD
発売日の下限／上限。

--sort {date,-date,rank,-rank,price,-price}（date）
APIのソート順。

--hits N（100）
1回のAPI取得件数（1〜100）。

--max N（500）
総取得上限。⚠pipeline側で「N件だけ処理」に調整済み。

--sleep 秒（0.7）
API呼び出しインターバル。

--debug
先頭1件の生JSONを raw_first_item.json に保存。

画像・本文・品質の整形／足切り

--verify-images
ジャケが弱い／NOW PRINTINGならサンプルから“良さげ1枚”に差し替え検討。

--skip-placeholder
プレースホルダ画像と判断した作品を除外。

--min-samples N（1）
サンプル画像が N 枚未満なら除外。

--release-after YYYY-MM-DD
この日付より新しい作品（=直近）は除外（新作を後回しにする運用向け）。

--max-gallery N（12）
本文に差し込むギャラリー最大枚数。

--no-content
CSVの content 列（本文HTML）を出力しない。

内容フィルタ（正規表現・部分一致）

include-* は「必須でヒット」、exclude-* は「当たれば除外」。複数回指定可（OR扱い）。大文字小文字無視。
PowerShellでは | を含むパターンはクォート必須。

--include-maker / --exclude-maker
例：--include-maker "S1|MOODYZ"

--include-actress / --exclude-actress
例：--include-actress "三上|葵"

--include-genre / --exclude-genre
例：--include-genre "単体|専属", --exclude-genre "企画|オムニバス|VR"

--include-title / --exclude-title
例：--include-title "デビュー|初撮り", --exclude-title "総集編|ダイジェスト"

--include-cid-prefix / --exclude-cid-prefix
品番先頭の正規表現。例：--include-cid-prefix "^SSIS|^IPX"

ネットワークHEAD判定の制御（画像プレースホルダ検出まわり）

--no-head-check
HEADリクエストを行わず、URLヒューリスティックだけで判定（高速・安全）。

--head-timeout 秒（3.0）
HEADを使う場合のタイムアウト。

--head-insecure
HEAD時に SSL 検証を無効化（verify=False）。回線事情でSSL握手に詰まる場合の回避。

出力

--outfile パス（out/fanza_items.csv）
CSV出力先。pipeline側で保存時にディレクトリ作成（安全化済み）。

WordPress 投稿（REST直投稿）

--wp-post
有効にすると各行を WordPress に下書き/公開/予約で投稿。

--wp-url / --wp-user / --wp-app-pass
接続先URL・ユーザー名・アプリケーションパスワード。
環境変数 WP_URL / WP_USER / WP_APP_PASS でもOK。

--wp-categories "A,B,C" / --wp-tags "x,y"
カンマ区切りの名称。存在しなければ作成してから付与。

--publish
ステータスを publish に（即時公開）。

--future-datetime 2025-09-11T21:00:00
予約日時（ISO）。指定があると publish より優先し、future で作成。