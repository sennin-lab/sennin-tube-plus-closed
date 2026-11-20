# CHANGELOG

## 追加: わかめtube の動画情報取得方法を移植 (app/wakame_sources.py)
- わかめtube(wkt-Plus / server/wakame.js)の取得先を追加: **AceThinker / MIN-Tube2 / RapidAPI / Freemake**。最後の手段として **Zernio(getlate)** を使う。
- インスタンス一覧は GitHub から取得し 1時間キャッシュ。取得失敗時は前回の一覧を使う。
- わかめ式の健康管理: **10分以内に5回タイムアウトしたインスタンスは30分ブロック**、成功で減点。Invidious にも同じ規則を適用。全滅時は諦めずに元の一覧を使う。
- 取得は並列レース化(わかめは順番に試すが、sennin では BATCH_SIZE 件ずつ並列にして速度を優先)。1ソースの待ち時間は上限 10 秒。
- わかめ系は Invidious / Piped の結果を2秒待ってから開始する。先に決まれば外部APIへは問い合わせない(通常時の外部負荷は増えない)。
- RapidAPI は環境変数 `RAPIDAPI_KEY_DEFAULT` / `RAPIDAPI_KEY_FALLBACK1〜3` で指定。未設定なら自動でスキップ。
- `?api=acethinker|min-tube2-api|rapidapi|freemake` で個別に指定可能。
- `/api/diag/{videoId}` に `wakame.streams` を追加(どの取得先が失敗しているか確認できる)。
- 未移植: わかめの「youtubei.js による動画情報取得」(Node 専用のため)。動画情報は従来の Invidious / Piped / Sia / Sennin から取得する。

## 修正: コメントが取得できない
- コメントは複数ソース(Invidious / Sennin / Sia)を並列に当たり、**コメントを含む最初の結果**を採用するように変更。以前は最初に完了したソースが空・失敗だと他を待たずに諦めていた。
- Sennin のコメント取得が動画APIの `comments` 欄しか見ていなかった問題を修正（`/api/comments?videoId=` も試す）。
- Sia のコメント API を候補に追加。
- コメントは描画後に `GET /api/comments/{videoId}` で非同期に取得（以前は4秒待ってから描画し、間に合わないと空になった）。
- コメント本文は `contentHtml` をサーバー側でエスケープ（外部由来のHTMLを表示しない）。

## 修正: ストリームが再生できない
- Invidious に `local=true` を付与し、ストリームURLを取得元インスタンス経由にする（googlevideo の直リンクは取得元IPに紐づき、別IPのブラウザで 403 になる）。相対URLは絶対URLに変換。
- 映像のみ(videoOnly)候補には最良の音声トラックを組み合わせる。音声のない候補は返さない。
- HLS 候補を正しく返す（Invidious の `hlsUrl`、Piped の `hls`）。形式ラベルは `application/x-mpegURL`。
- HLS マニフェストの `#EXT-X-MAP` / `#EXT-X-PART` 等の URI も中継経由に書き換え（以前は `KEY` と `MEDIA` のみ）。
- 中継 (`/proxy/piped-stream`) は共有 HTTP クライアントを使用し、Range 応答を圧縮しない。
- `GET /api/streams/{videoId}?refresh=1&proxy=1` を追加。プレイヤーは全候補が失敗したときに再取得する。
- プレイヤーは候補を順番に試し、失敗・無応答なら次の候補に切り替える。hls.js は 1.7.4 に固定。

## 高速化
- `fetch_with_inflight` が待機中もロックを握っていた問題を修正（全リクエストが直列化していた）。リトライは例外時のみ。
- キャッシュミス時に最速インスタンスを15台 ping してから取得していた処理を撤去（ネットワーク待ちなし）。
- Piped を情報取得とストリーム取得の両方から重複して叩いていた問題を、インフライト共有 + 5分キャッシュで解消。
- 動画情報とストリームが同じ Invidious レスポンスを使うように統一。
- 応答のgzip圧縮（HTML/JSON のみ。動画・画像は除外）。
- サムネイルに `Cache-Control: max-age=86400`。
- 教育用ソースの取得を10分キャッシュ。JSON API の読み取りタイムアウトを 18s → 10s。
- インスタンス一覧の取得に失敗したときもフォールバックを短時間キャッシュ（毎回リトライしない）。

## デザイン
- `base.html`: カラートークン（`:root`）を整理。ライト/ダーク（OS設定・`data-theme` 両対応）。フォーカスリング・トースト。
- `watch.html`: 再生ページを再設計。スマホは全幅プレイヤー＋左右16pxの余白、ドロップダウンは下部シート、コメント・説明カードの見直し、スケルトン表示。

## その他
- 未使用だった `app/stream.py` を削除（ルートがなく、存在しない関数を import していた）。
- `img/` ディレクトリが無くても起動するように作成。

## 高速化 (第2弾)
- `/watch?list=...` でプレイリスト取得を描画の待ち対象から外した。遅いプレイリストに最大6秒引きずられていた。
  - 計測(モック上流・プレイリスト応答3秒): 修正前 3.08秒 / 上流リクエスト17回 → 修正後 0.09秒 / 10回。
- `preconnect` を fonts.googleapis / fonts.gstatic / cdnjs に設定し、アイコンフォントなどの接続を先に確立。
- `uvloop` と `httptools` を requirements に追加（uvicorn が自動で利用し、イベントループとHTTP解析が速くなる）。
- 元のコードは `img/` が無いとローカルで起動できなかった（Dockerfileでは作られる）。起動時に作成するよう修正済み。

## 修正: 動画情報もストリームも取得できない (第3弾)
- Invidious インスタンス一覧(GitHub)の取得に失敗し、キャッシュも無いと「インスタンスなし」で動画情報・ストリームが同時に失敗していた。組み込みの一覧にフォールバックし、予備の一覧URLも試す。
- `local=true` の応答が得られなかった場合は、通常の動画URLで再試行する(情報・ストリーム共通)。
- `GET /api/video/{videoId}` を追加。サーバー描画で動画情報が欠けた場合、画面が取り直す。
- `GET /api/diag/{videoId}` を追加。各ソース(Invidious / Piped / Sia / Sennin)の成否・所要時間・エラーを一覧で返す。障害の切り分け用。
