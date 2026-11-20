"""
わかめtube (wkt-Plus / server/wakame.js) の動画ストリーム取得方法を移植したモジュール。

移植元と取得先:
  ・AceThinker : yes.json のインスタンス一覧 → {base}/api/dlapinewv2.php
  ・MIN-Tube2  : min-tube-api.json のインスタンス一覧 → {base}/api/video/{id}
  ・RapidAPI   : yt-api / ytstream-download の2ホスト × 環境変数の複数キー
  ・Freemake   : downloader.freemake.com/api/videoinfo/{id}
  ・Zernio     : getlate の youtube-live-downloader (他が全滅した時だけ使う最後の手段)

わかめtube から移植した運用ルール:
  ・インスタンス一覧は 1時間キャッシュし、取得失敗時は前回の一覧を使う
  ・10分以内に 5 回タイムアウトしたインスタンスは 30 分ブロックする (成功で減点)
  ・API ごとに同じ形式(統合 / 映像のみ + 音声)へ正規化する

わかめtube との違い(速度と信頼性のため):
  ・わかめは1インスタンスずつ順番に試すが、ここでは BATCH_SIZE 件ずつ並列に試し、
    最初に再生可能なものを採用する
  ・全体の待ち時間に上限(SOURCE_TIMEOUT)を設け、遅いインスタンスで待たされないようにする
  ・わかめの「youtubei.js による動画情報取得」は Node 専用のため移植していない
    (動画情報は従来どおり Invidious / Piped / Sia / Sennin から取る)
  ・Freemake の「計測(metrics)送信」は取得に必要ないため省略している

RapidAPI のキーは環境変数で指定する:
  RAPIDAPI_KEY_DEFAULT, RAPIDAPI_KEY_FALLBACK1, RAPIDAPI_KEY_FALLBACK2, RAPIDAPI_KEY_FALLBACK3
  (未設定なら RapidAPI は自動的にスキップされる)
"""

import asyncio
import logging
import os
import random
import re
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

# ── わかめtube と同じ定数 ───────────────────────────────────
LIST_CACHE_TTL = 60 * 60          # インスタンス一覧のキャッシュ (1時間)
FAIL_WINDOW = 10 * 60             # タイムアウト集計期間 (10分)
BLOCK_DURATION = 30 * 60          # ブロック期間 (30分)
MAX_FAILURES = 5                  # ブロックまでのタイムアウト回数

# ── sennin 向けの速度設定 ───────────────────────────────────
BATCH_SIZE = 4                    # 同時に試すインスタンス数
SOURCE_TIMEOUT = 10.0             # 1ソース(API群)の最大待ち時間(秒)
PER_REQUEST_TIMEOUT = httpx.Timeout(8.0, connect=3.0)

ACETHINKER_LIST_URL = (
    "https://raw.githubusercontent.com/toka-kun/Education/refs/heads/main/apis/AceThinker/yes.json"
)
MINTUBE_LIST_URL = (
    "https://raw.githubusercontent.com/Minotaur-ZAOU/test/refs/heads/main/min-tube-api.json"
)
RAPIDAPI_HOSTS = ("yt-api.p.rapidapi.com", "ytstream-download-youtube-videos.p.rapidapi.com")
RAPIDAPI_KEY_ENVS = (
    "RAPIDAPI_KEY_DEFAULT",
    "RAPIDAPI_KEY_FALLBACK1",
    "RAPIDAPI_KEY_FALLBACK2",
    "RAPIDAPI_KEY_FALLBACK3",
)
FREEMAKE_URL = "https://downloader.freemake.com/api/videoinfo/{vid}"
GETLATE_URL = "https://getlate.dev/api/tools/youtube-live-downloader"

# 指定(?stream_api=...)で選べる名前。画面のプルダウンの value と一致させること
API_NAMES = ("acethinker", "min-tube2-api", "rapidapi", "freemake", "zernio")

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
)

# ── 状態(プロセス内) ───────────────────────────────────────
_client: Optional[httpx.AsyncClient] = None
_list_cache: Dict[str, Tuple[List[str], float]] = {}
_health: Dict[str, Dict[str, float]] = {}   # instance -> {fails, first, blocked_until}


def _http() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=PER_REQUEST_TIMEOUT, follow_redirects=True)
    return _client


# ─────────────────────────────────────────────────────────
# インスタンスの健康管理 (わかめtube の recordTimeout / recordSuccess / isBlocked)
# ─────────────────────────────────────────────────────────
def is_blocked(instance: str) -> bool:
    h = _health.get(instance)
    return bool(h and h.get("blocked_until", 0) > time.time())


def record_success(instance: str) -> None:
    h = _health.get(instance)
    if h and h.get("fails", 0) > 0:
        h["fails"] = max(0, h["fails"] - 1)  # 成功のたびに1つ減点(0未満にはしない)


def record_timeout(instance: str) -> None:
    now = time.time()
    h = _health.get(instance)
    if h is None:
        _health[instance] = {"fails": 1, "first": now, "blocked_until": 0}
        return
    if now - h["first"] > FAIL_WINDOW:
        h["fails"] = 1
        h["first"] = now
    else:
        h["fails"] += 1
    if h["fails"] >= MAX_FAILURES:
        logger.warning("インスタンスを %d 分間ブロック: %s", BLOCK_DURATION // 60, instance)
        h["blocked_until"] = now + BLOCK_DURATION
        h["fails"] = 0


def healthy_instances(instances: List[str]) -> List[str]:
    """ブロック中のものを除いた一覧を返す。全部ブロック中なら、諦めずに元の一覧を返す。"""
    ok = [i for i in instances if not is_blocked(i)]
    return ok or list(instances)


# ─────────────────────────────────────────────────────────
# 共通ユーティリティ
# ─────────────────────────────────────────────────────────
async def _load_list(url: str) -> List[str]:
    """GitHub 上のインスタンス一覧を 1時間キャッシュして返す。失敗時は前回の値(無ければ空)。"""
    now = time.time()
    hit = _list_cache.get(url)
    if hit and now - hit[1] < LIST_CACHE_TTL:
        return hit[0]
    try:
        r = await _http().get(url, timeout=6)
        r.raise_for_status()
        data = r.json()
        items = [str(x).strip() for x in data if str(x).strip()] if isinstance(data, list) else []
        if items:
            _list_cache[url] = (items, now)
            return items
    except Exception as exc:
        logger.debug("インスタンス一覧の取得に失敗 %s: %s", url, exc)
    return hit[0] if hit else []


async def _race(
    factories: List[Callable[[], Awaitable[Any]]],
    accept: Callable[[Any], bool],
    timeout: float,
) -> Optional[Any]:
    """accept を満たす最初の結果を返す。勝者が出たら残りはキャンセルする。"""
    deadline = time.monotonic() + timeout
    tasks = [asyncio.ensure_future(f()) for f in factories]
    try:
        pending = set(tasks)
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break
            for t in done:
                if t.cancelled() or t.exception() is not None:
                    continue
                result = t.result()
                if accept(result):
                    return result
        return None
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _stream(url: str, name: str, fmt: str, audio_url: str) -> Dict[str, str]:
    """sennin の templates/watch.html が読む形式 (url / resolution / format / audioUrl) に揃える。"""
    if ".m3u8" in url:
        fmt = "application/x-mpegURL"
    return {
        "url": url,
        "resolution": str(name or "Unknown").strip() or "Unknown",
        "format": fmt,
        "audioUrl": audio_url or "",
    }


def _build(source: str, combined: list, video: list, audio: list) -> Optional[Dict[str, Any]]:
    """
    combined : [(url, 画質名, 拡張子)]  … 音声付き(統合)
    video    : [(url, 画質名, 拡張子)]  … 映像のみ (音声は別トラックと組み合わせる)
    audio    : [(url, 表示名, 拡張子, kbps)]
    """
    best_audio = max(audio, key=lambda a: a[3])[0] if audio else ""
    streams = [_stream(u, n, f"{ext}/mixed", "") for u, n, ext in combined]
    streams += [_stream(u, n, f"{ext}/videoOnly", best_audio) for u, n, ext in video]
    if not streams:
        return None
    return {
        "streamUrls": streams,
        "videoUrls": [s["url"] for s in streams],
        "formatStreams": [],
        "adaptiveFormats": [],
        "stream_api_used": source,
    }


def is_playable(result: Any) -> bool:
    """音声付き(mixed)か、音声URLが付いた映像があれば再生可能とみなす。"""
    if not isinstance(result, dict):
        return False
    return any(
        s.get("format", "").endswith("/mixed") or s.get("audioUrl")
        for s in (result.get("streamUrls") or [])
    )


# ─────────────────────────────────────────────────────────
# ① AceThinker (わかめ: getAceThinker)
# ─────────────────────────────────────────────────────────
async def _acethinker_one(instance: str, video_id: str) -> Optional[Dict[str, Any]]:
    # yes.json が ".../downloader" 付きでも無しでも動くようにそろえる
    base = re.sub(r"/downloader/?$", "", instance.rstrip("/")) + "/downloader"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ja,en;q=0.9",
        "referer": f"{base}/",
        "user-agent": DEFAULT_UA,
    }
    # url= はエンコードしない(わかめ・ブラウザと同じ書式)。video_id は呼び出し前に検証済み
    url = f"{base}/api/dlapinewv2.php?url=https://www.youtube.com/watch?v={video_id}"
    r = await _http().get(url, headers=headers, timeout=SOURCE_TIMEOUT)
    r.raise_for_status()
    res = (r.json() or {}).get("res_data") or {}
    formats = res.get("formats")
    if not formats:
        return None

    duration = res.get("duration")
    combined, video, audio = [], [], []
    for f in formats:
        if not isinstance(f, dict) or not f.get("url"):
            continue
        ext = f.get("ext") or "mp4"
        name = f.get("quality") or "Unknown"
        if f.get("acodec") != "none" and f.get("vcodec") != "none":
            combined.append((f["url"], name, ext))
        elif f.get("vcodec") == "none":
            kbps = 0.0
            if f.get("filesize") and duration:
                # わかめと同じ: filesize*8/duration を整数化してから /1000 で kbps
                kbps = round((f["filesize"] * 8) / duration) / 1000
            label = f"{kbps}kbps" if kbps else name
            audio.append((f["url"], label, ext, kbps))
        elif f.get("acodec") == "none":
            video.append((f["url"], name, ext))
    return _build("acethinker", combined, video, audio)


# ─────────────────────────────────────────────────────────
# ② MIN-Tube2 (わかめ: getMinTube2)
# ─────────────────────────────────────────────────────────
async def _mintube_one(instance: str, video_id: str) -> Optional[Dict[str, Any]]:
    r = await _http().get(f"{instance.rstrip('/')}/api/video/{video_id}", timeout=SOURCE_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or not data.get("stream_url"):
        return None

    audio_url = data.get("audioUrl") or ""
    combined = [(data["stream_url"], "Default", "mp4")]
    video = []
    hi = data.get("highstreamUrl")
    if hi and hi != data["stream_url"]:
        video.append((hi, "High Quality", "mp4"))  # わかめでは統合ではない映像扱い
    audio = [(audio_url, "medium", "m4a", 0.0)] if audio_url else []
    return _build("min-tube2-api", combined, video, audio)


# ─────────────────────────────────────────────────────────
# ③ RapidAPI (わかめ: getRapidApi)  ※キーは環境変数
# ─────────────────────────────────────────────────────────
def _rapid_keys() -> List[str]:
    return [k for k in (os.environ.get(name) for name in RAPIDAPI_KEY_ENVS) if k]


def _rapid_build(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    def ext_of(f: Dict[str, Any]) -> str:
        return str(f.get("mimeType") or "").split(";")[0].split("/")[-1] or "mp4"

    def quality_of(f: Dict[str, Any]) -> str:
        w, h = f.get("width"), f.get("height")
        fps = f.get("fps")
        if not w or not h:
            return "Unknown"
        return f"{min(int(w), int(h))}p{'' if fps is None else fps}"  # 例: 344p29

    adaptive = [f for f in (data.get("adaptiveFormats") or []) if f.get("url")]
    combined = [(f["url"], quality_of(f), ext_of(f)) for f in (data.get("formats") or []) if f.get("url")]
    video = [
        (f["url"], quality_of(f), ext_of(f))
        for f in adaptive if str(f.get("mimeType", "")).startswith("video/")
    ]
    audio = []
    for f in adaptive:
        if not str(f.get("mimeType", "")).startswith("audio/"):
            continue
        bitrate = f.get("bitrate") or 0
        kbps = bitrate / 1000 if bitrate else 0.0
        lang = (f.get("audioTrack") or {}).get("displayName")
        base_label = f"{kbps}kbps" if kbps else "Unknown"
        label = f"{lang} - {base_label}" if lang else base_label
        audio.append((f["url"], label, ext_of(f), kbps))
    return _build("rapidapi", combined, video, audio)


async def fetch_rapidapi(video_id: str) -> Optional[Dict[str, Any]]:
    keys = _rapid_keys()
    if not keys:
        return None  # キー未設定ならスキップ(わかめは例外にしていたが、ここでは他ソースを優先する)
    # わかめと同じく「キー → ホスト」の順に、エラーの種類によらず試す
    for key in keys:
        for host in RAPIDAPI_HOSTS:
            try:
                r = await _http().get(
                    f"https://{host}/dl",
                    params={
                        "id": video_id,
                        "cgeo": "JP",
                        "lang": "ja",
                        "fields": "formats[].*,adaptiveFormats[].*",
                    },
                    headers={"X-RapidAPI-Key": key, "X-RapidAPI-Host": host},
                    timeout=SOURCE_TIMEOUT,
                )
                r.raise_for_status()
                data = r.json() or {}
                if data.get("formats") or data.get("adaptiveFormats"):
                    result = _rapid_build(data)
                    if result:
                        return result
            except Exception as exc:
                logger.debug("RapidAPI %s 失敗: %s", host, exc)
    return None


# ─────────────────────────────────────────────────────────
# ④ Freemake (わかめ: getFreemake)
# ─────────────────────────────────────────────────────────
async def fetch_freemake(video_id: str) -> Optional[Dict[str, Any]]:
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://www.freemake.com",
        "Referer": "https://www.freemake.com/jp/free_video_downloader/",
        "User-Agent": DEFAULT_UA,
    }
    r = await _http().get(
        FREEMAKE_URL.format(vid=video_id),
        params={"atn": 1},
        headers=headers,
        timeout=SOURCE_TIMEOUT,
    )
    r.raise_for_status()
    qualities = (r.json() or {}).get("qualities") or []

    combined, video, audio = [], [], []
    for q in qualities:
        info = q.get("qualityInfo") or {}
        if not q.get("url") or not info:
            continue
        ext = str(info.get("format") or "mp4").lower()  # Freemake は "Mp4" のように返す
        name = info.get("qualityLabel") or "Unknown"
        abr = float(info.get("audioBitrate") or 0)
        if str(info.get("itag")) == "18":
            combined.append((q["url"], name, ext))
        elif abr == 0:
            video.append((q["url"], name, ext))
        else:
            audio.append((q["url"], f"{int(abr)}kbps" if abr else "Unknown", ext, abr))
    return _build("freemake", combined, video, audio)


# ─────────────────────────────────────────────────────────
# ⑤ Zernio / getlate (わかめ: getZernioStreamUrl) ※最後の手段
# ─────────────────────────────────────────────────────────
async def fetch_getlate(video_id: str) -> Optional[Dict[str, Any]]:
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    api_url = f"{GETLATE_URL}?url={quote(watch_url, safe='')}&formatId=1"
    try:
        r = await _http().get(
            api_url, timeout=3.0, follow_redirects=False
        )
    except Exception as exc:
        logger.debug("getlate 失敗: %s", exc)
        return None

    stream_url = r.headers.get("location") or ""
    if not stream_url and r.status_code < 300 and str(r.url) != api_url:
        stream_url = str(r.url)  # リダイレクトせず直接返した場合の最終URL
    if not stream_url:
        return None
    return {
        "streamUrls": [_stream(stream_url, "Default", "mp4/mixed", "")],
        "videoUrls": [stream_url],
        "formatStreams": [],
        "adaptiveFormats": [],
        "stream_api_used": "getlate",
    }


# ─────────────────────────────────────────────────────────
# インスタンス一覧を使う取得(AceThinker / MIN-Tube2 共通の並列試行)
# ─────────────────────────────────────────────────────────
async def _guarded(instance: str, one, video_id: str) -> Optional[Dict[str, Any]]:
    """1インスタンスの取得。失敗・タイムアウトを健康管理に記録し、例外は外へ出さない。"""
    try:
        result = await one(instance, video_id)
    except httpx.TimeoutException:
        record_timeout(instance)
        return None
    except Exception as exc:
        logger.debug("インスタンス失敗 %s: %s", instance, exc)
        return None
    # 再生できない結果(映像のみ・音声なし)は「成功」にも「勝者」にもしない
    if is_playable(result):
        record_success(instance)
        return result
    return None


async def _via_instances(list_url: str, one, video_id: str) -> Optional[Dict[str, Any]]:
    instances = [i for i in await _load_list(list_url) if i]
    pool = healthy_instances(instances)
    random.shuffle(pool)  # わかめと同じくランダムに分散させる

    deadline = time.monotonic() + SOURCE_TIMEOUT
    for start in range(0, len(pool), BATCH_SIZE):
        if time.monotonic() >= deadline:
            break
        batch = pool[start:start + BATCH_SIZE]
        result = await _race(
            [lambda inst=inst: _guarded(inst, one, video_id) for inst in batch],
            accept=lambda r: r is not None,
            timeout=max(0.1, deadline - time.monotonic()),
        )
        if result is not None:
            return result
    return None


async def _acethinker_all(video_id: str):
    return await _via_instances(ACETHINKER_LIST_URL, _acethinker_one, video_id)


async def _mintube_all(video_id: str):
    return await _via_instances(MINTUBE_LIST_URL, _mintube_one, video_id)


# ─────────────────────────────────────────────────────────
# 公開API
# ─────────────────────────────────────────────────────────
# 他の取得先(Invidious / Piped)が先に答えたら、わかめ系の外部APIには一切問い合わせない。
# そのため通常時の外部負荷は増えず、遅延分だけ「最後の砦」として効く。
WAKAME_START_DELAY = 2.0


async def fetch_wakame_streams_delayed(video_id: str, delay: float = WAKAME_START_DELAY) -> Optional[Dict[str, Any]]:
    """delay 秒待ってから fetch_wakame_streams を実行する。待ち中にキャンセルされれば何も送らない。"""
    await asyncio.sleep(delay)
    return await fetch_wakame_streams(video_id)


async def fetch_wakame_streams(video_id: str, api: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    わかめtube の取得方法でストリームを取る。
      api 指定あり : その取得先だけを使う (?api=acethinker 等)
      api 指定なし : 4 系統を並列に走らせ、最初に再生可能な結果を採用する。
                     どれも取れなければ最後に Zernio(getlate) を試す。
    失敗時は None を返す(例外は外へ出さない)。
    """
    try:
        if api:
            single = {
                "acethinker": lambda: _acethinker_all(video_id),
                "min-tube2-api": lambda: _mintube_all(video_id),
                "rapidapi": lambda: fetch_rapidapi(video_id),
                "freemake": lambda: fetch_freemake(video_id),
                "zernio": lambda: fetch_getlate(video_id),
            }.get(api)
            if single is None:
                return None
            result = await single()
            return result if is_playable(result) else None

        result = await _race(
            [
                lambda: _acethinker_all(video_id),
                lambda: _mintube_all(video_id),
                lambda: fetch_rapidapi(video_id),
                lambda: fetch_freemake(video_id),
            ],
            accept=is_playable,
            timeout=SOURCE_TIMEOUT,
        )
        # 遅延(2秒)+ 取得(最大10秒)+ getlate(3秒) を待てるように、外側の打ち切りより短くする
        if result is None:
            fallback = await fetch_getlate(video_id)
            if is_playable(fallback):
                result = fallback
        return result if is_playable(result) else None
    except Exception as exc:
        logger.debug("wakame sources 失敗 %s: %s", video_id, exc)
        return None
