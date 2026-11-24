#!/usr/bin/env python3
"""Clean Read-Only Telemetry & Process Lifecycle Controller for GO-TOKYO v99.0 (0%-Fee Lighter <-> Binance USDC-M)."""
import hashlib
import hmac
import http.client
import json
import os
import subprocess
import threading
import time
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config

API_KEY = "I4IImhi629VlyPD28pnIkveLZsAwH5yf0COTtmJQuYiGFICACdCS03HEXz2OCti4"
API_SEC = b"ng4mncCOkk5DvRJxPARevMGoDACQBEpzWBqh5XJ8TlEYCxR0TWpSp8vKVitg3U4D"
S3_BUCKET = "crypto-arb-bot-tokyo-bin-767579108352"
S3_KEY = "live-telemetry.json"
PAUSE_KEY = "pause-control.json"
AWS_REGION = "ap-northeast-1"

LIGHTER_ACCT_IDX = 770015
LIGHTER_BASE = "https://mainnet.zklighter.elliot.ai"
LIGHTER_GO_BIN = "/opt/crypto-arb-bot/lighter_go_bot"
LIGHTER_GO_URL = "https://github.com/Abishek17703/crypto-arb-bot/releases/download/lighter-go-latest/lighter_go_bot"
ARENA_GO_BIN = "/opt/crypto-arb-bot/arena_go"
TICK_GO_BIN = "/opt/crypto-arb-bot/tick_collector_go"
TICK_DIR = "/opt/crypto-arb-bot/ticks"
TICK_LOG_PATH = "/tmp/tick_collector.log"
TICK_MANIFEST_KEY = "ticks/manifest.json"
ARENA_JSON_PATH = "/tmp/forward_arena.json"
LIVE_ARENA_JSON_PATH = "/tmp/live_arena.json"
ASTER_ACCT_JSON_PATH = "/tmp/aster_acct.json"
ARENA_S3_KEY = "forward-arena.json"
ARENA_LOG_PATH = "/tmp/forward_arena.log"
PAUSE_FLAG_PATH = "/tmp/lighter_pause.flag"
LOG_PATH = "/tmp/lighter_tokyo.log"
HIST_PATHS = ("/opt/crypto-arb-bot/dn_trades_history.json", "/tmp/dn_trades_history.json")
START_EQUITY_USD = 397.7745
DN_EPOCH_MS = 1795276700000


class BinanceSession:
    def __init__(self, host):
        self.host = host
        self.conn = None
        self.connect()

    def connect(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self.conn = http.client.HTTPSConnection(self.host, 443, timeout=4)

    def request(self, method, path, params=None, signed=False):
        q = dict(params or {})
        if signed:
            q["timestamp"] = int(time.time() * 1000)
            q["recvWindow"] = 5000
        qs = urllib.parse.urlencode(q)
        if signed:
            sig = hmac.new(API_SEC, qs.encode(), hashlib.sha256).hexdigest()
            qs = f"{qs}&signature={sig}"
        full_path = f"{path}?{qs}" if qs else path
        headers = {"X-MBX-APIKEY": API_KEY, "Connection": "keep-alive"}
        for attempt in range(2):
            try:
                self.conn.request(method, full_path, headers=headers)
                resp = self.conn.getresponse()
                raw = resp.read().decode("utf-8", errors="replace")
                if resp.status == 200:
                    return json.loads(raw)
                return {"_error": resp.status, "msg": raw}
            except Exception as e:
                self.connect()
                if attempt == 1:
                    return {"_error": 500, "msg": str(e)}


def read_json_safe(path, default=None):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                return json.load(f)
    except Exception:
        pass
    return default


def write_flag(active):
    try:
        if active:
            with open(PAUSE_FLAG_PATH, "w", encoding="utf-8") as f:
                f.write("1")
        elif os.path.exists(PAUSE_FLAG_PATH):
            os.remove(PAUSE_FLAG_PATH)
    except Exception:
        pass


def trim_log_if_large(max_bytes=2 * 1024 * 1024):
    try:
        if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > max_bytes:
            with open(LOG_PATH, "rb") as f:
                f.seek(-512 * 1024, os.SEEK_END)
                tail = f.read()
            with open(LOG_PATH, "wb") as f:
                f.write(tail)
    except Exception:
        pass


def append_log(line):
    try:
        trim_log_if_large()
        with open(LOG_PATH, "a", encoding="utf-8", errors="replace") as f:
            f.write(line.rstrip("\n") + "\n")
    except Exception:
        pass


def read_log_tail(max_chars=8000):
    try:
        if not os.path.exists(LOG_PATH):
            return "NO_LOG"
        with open(LOG_PATH, "rb") as f:
            f.seek(0, os.SEEK_END)
            sz = f.tell()
            f.seek(max(0, sz - max_chars * 2), os.SEEK_SET)
            raw = f.read().decode("utf-8", errors="replace").replace("\x00", "")
        return raw[-max_chars:] if len(raw) > max_chars else raw
    except Exception:
        return "NO_LOG"


def fetch_lighter_fallback(acct_path="/tmp/lighter_acct.json", tr_path="/tmp/lighter_trades.json"):
    now = time.time()
    need_acct = not os.path.exists(acct_path) or (now - os.path.getmtime(acct_path) > 4.0)
    need_tr = not os.path.exists(tr_path) or (now - os.path.getmtime(tr_path) > 15.0)
    if need_acct:
        try:
            url = f"{LIGHTER_BASE}/api/v1/account?by=index&value={LIGHTER_ACCT_IDX}"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=3) as r:
                d = json.loads(r.read().decode("utf-8", errors="replace"))
            accts = d.get("accounts") or []
            if accts:
                with open(acct_path + ".tmp", "w", encoding="utf-8") as f:
                    json.dump(accts[0], f)
                os.replace(acct_path + ".tmp", acct_path)
        except Exception:
            pass
    if need_tr:
        try:
            url = f"{LIGHTER_BASE}/api/v1/trades?account_index={LIGHTER_ACCT_IDX}&limit=100&sort_by=timestamp"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=4) as r:
                d1 = json.loads(r.read().decode("utf-8", errors="replace"))
            trades = list(d1.get("trades") or [])
            cur = d1.get("next_cursor")
            if cur and trades:
                last_ts = int(trades[-1].get("timestamp") or 0)
                if 0 < last_ts < 100000000000:
                    last_ts *= 1000
                if last_ts >= DN_EPOCH_MS:
                    u2 = f"{url}&cursor={urllib.parse.quote(str(cur))}"
                    req2 = urllib.request.Request(u2, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req2, timeout=4) as r2:
                        d2 = json.loads(r2.read().decode("utf-8", errors="replace"))
                    seen = {int(x.get("trade_id") or 0) for x in trades}
                    for x in (d2.get("trades") or []):
                        tid = int(x.get("trade_id") or 0)
                        if tid and tid not in seen:
                            seen.add(tid)
                            trades.append(x)
            if trades:
                with open(tr_path + ".tmp", "w", encoding="utf-8") as f:
                    json.dump({"trades": trades}, f)
                os.replace(tr_path + ".tmp", tr_path)
        except Exception:
            pass


def read_dn_history():
    for p in HIST_PATHS:
        data = read_json_safe(p)
        if isinstance(data, list):
            return data[-200:]
    return []


def fetch_binance_income_and_reconstruct(fapi, lt_trades, disk_hist, cached_wallet, upnl):
    inc = fapi.request("GET", "/fapi/v1/income", {"startTime": DN_EPOCH_MS, "limit": 1000}, signed=True)
    if isinstance(inc, list):
        real_pnl, comm, fund, cnt = 0.0, 0.0, 0.0, 0
        for r in inc:
            tp = r.get("incomeType", "")
            try:
                v = float(r.get("income", 0.0))
            except Exception:
                v = 0.0
            if tp == "REALIZED_PNL":
                real_pnl += v
                cnt += 1
            elif tp == "COMMISSION":
                comm += v
            elif tp == "FUNDING_FEE":
                fund += v

        binance_audit = {
            "real": round(real_pnl, 4),
            "fee": round(comm, 4),
            "fund": round(fund, 4),
            "upnl": round(upnl, 4),
            "net": round(real_pnl + comm + fund + upnl, 4),
            "trades": cnt,
            "wallet": round(cached_wallet, 4),
        }
    else:
        if isinstance(inc, dict):
            append_log(f"[INC_ERR] {inc}")
        binance_audit = None
        inc = []

    merged = list(disk_hist or [])
    merged.sort(key=lambda r: int(r.get("closeTs") or r.get("openTs") or r.get("id") or 0), reverse=True)
    return binance_audit, merged[:200]


def is_go_bot_running():
    r = subprocess.run(["pgrep", "-f", "lighter_go_bot"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return r.returncode == 0


def ensure_arena_running():
    if not os.path.exists(ARENA_GO_BIN):
        return
    r = subprocess.run(["pgrep", "-f", "/opt/crypto-arb-bot/arena_go"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        try:
            with open(ARENA_LOG_PATH, "a", encoding="utf-8", errors="replace") as af:
                subprocess.Popen([ARENA_GO_BIN], stdout=af, stderr=subprocess.STDOUT)
        except Exception:
            pass


def ensure_tick_collector_running():
    if not os.path.exists(TICK_GO_BIN):
        return
    r = subprocess.run(["pgrep", "-f", "/opt/crypto-arb-bot/tick_collector_go"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        try:
            os.makedirs(TICK_DIR, exist_ok=True)
            with open(TICK_LOG_PATH, "a", encoding="utf-8", errors="replace") as tf:
                subprocess.Popen([TICK_GO_BIN], stdout=tf, stderr=subprocess.STDOUT)
        except Exception:
            pass


def sync_ticks_to_s3(s3, uploaded_files, lock):
    if not lock.acquire(blocking=False):
        return
    try:
        if not os.path.isdir(TICK_DIR):
            return
        files = sorted(
            f for f in os.listdir(TICK_DIR)
            if f.endswith(".ndjson.gz") and os.path.isfile(os.path.join(TICK_DIR, f))
        )
        chunks = []
        base_url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/ticks"
        for fname in files:
            fpath = os.path.join(TICK_DIR, fname)
            size = os.path.getsize(fpath)
            if size <= 20:
                continue
            if fname not in uploaded_files:
                with open(fpath, "rb") as fh:
                    s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=f"ticks/{fname}",
                        Body=fh.read(),
                        ContentType="application/gzip",
                    )
                uploaded_files.add(fname)
            chunks.append({
                "file": fname,
                "url": f"{base_url}/{fname}",
                "bytes": size,
            })
        status_obj = read_json_safe(os.path.join(TICK_DIR, "collector_status.json"), {})
        manifest = {
            "updatedAtMs": int(time.time() * 1000),
            "collectorStatus": status_obj,
            "chunkCount": len(chunks),
            "chunks": chunks[-576:],
        }
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=TICK_MANIFEST_KEY,
            Body=json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-store, no-cache, must-revalidate, max-age=0",
        )
    except Exception as e:
        append_log(f"[TICK_S3_ERR] {e}")
    finally:
        lock.release()


def stop_go_bot_gracefully():
    write_flag(True)
    deadline = time.time() + 12.0
    while time.time() < deadline:
        if not is_go_bot_running():
            append_log("[INSTANT-PAUSE] lighter_go_bot exited cleanly after flattening.")
            return
        time.sleep(0.2)
    subprocess.run(["pkill", "-9", "-f", "lighter_go_bot"], stderr=subprocess.DEVNULL)
    append_log("[INSTANT-PAUSE] lighter_go_bot force-stopped after 12.0s grace window.")


def ensure_s3_cors_and_presign(s3):
    try:
        s3.put_bucket_cors(
            Bucket=S3_BUCKET,
            CORSConfiguration={
                "CORSRules": [{
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["GET", "PUT", "POST", "HEAD"],
                    "AllowedOrigins": ["*"],
                    "MaxAgeSeconds": 86400,
                }]
            },
        )
    except Exception:
        pass
    try:
        put_url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": S3_BUCKET, "Key": PAUSE_KEY, "ContentType": "text/plain"},
            ExpiresIn=604800,
        )
    except Exception:
        put_url = ""
    try:
        post_data = s3.generate_presigned_post(
            Bucket=S3_BUCKET,
            Key=PAUSE_KEY,
            ExpiresIn=604800,
        )
    except Exception:
        post_data = None
    return put_url, post_data


def read_s3_pause_state(s3, default_val=True):
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=PAUSE_KEY)["Body"].read().decode("utf-8", errors="replace")
        obj = json.loads(raw)
        return bool(obj.get("paused", default_val)), int(obj.get("ts") or 0), str(obj.get("source") or "")
    except Exception:
        return default_val, 0, ""


def boot_go_bot(state):
    with state["boot_lock"]:
        if state["user_paused"]:
            write_flag(True)
            append_log("[PAUSED] lighter_go_bot halted by user.")
            return
        subprocess.run(["pkill", "-9", "-f", "lighter_go_bot"], stderr=subprocess.DEVNULL)
        write_flag(False)
        trim_log_if_large()
        try:
            with open(LOG_PATH, "a", encoding="utf-8", errors="replace") as lf:
                dl = subprocess.run(
                    f"curl -fsSL {LIGHTER_GO_URL} -o {LIGHTER_GO_BIN}.tmp && mv {LIGHTER_GO_BIN}.tmp {LIGHTER_GO_BIN} && chmod +x {LIGHTER_GO_BIN}",
                    shell=True,
                    stdout=lf,
                    stderr=lf,
                )
                lf.write(f"\n[TELEMETRY_BOOT] dl_rc={dl.returncode}\n")
                lf.flush()
                if dl.returncode == 0 and not state["user_paused"]:
                    env = dict(os.environ, GOGC="400")
                    subprocess.Popen(
                        ["sh", "-c", f"sudo -n nice -n -15 {LIGHTER_GO_BIN} || {LIGHTER_GO_BIN}"],
                        stdout=lf,
                        stderr=subprocess.STDOUT,
                        env=env,
                    )
                    time.sleep(1.5)
        except Exception as e:
            append_log(f"[TELEMETRY_BOOT_ERR] {e}")


def fetch_binance_snapshot(spot_api, fapi):
    raw_wallet_usd = None
    wb_r = spot_api.request("GET", "/sapi/v1/asset/wallet/balance", {"quoteAsset": "USDT"}, signed=True)
    if isinstance(wb_r, list):
        wb_sum = sum(float(w.get("balance", 0.0)) for w in wb_r if w.get("activate"))
        if wb_sum > 0:
            raw_wallet_usd = round(wb_sum, 4)

    balances = []
    binance_futures = None
    f_acc = fapi.request("GET", "/fapi/v2/account", signed=True)
    if isinstance(f_acc, dict) and "assets" in f_acc:
        binance_futures = {
            "marginBalance": round(float(f_acc.get("totalMarginBalance", 0.0)), 4),
            "availableBalance": round(float(f_acc.get("availableBalance", 0.0)), 4),
            "initialMargin": round(float(f_acc.get("totalInitialMargin", 0.0)), 4),
            "unrealizedProfit": round(float(f_acc.get("totalUnrealizedProfit", 0.0)), 4),
        }
        for fa in f_acc.get("assets", []):
            mb = float(fa.get("marginBalance", 0.0))
            av = float(fa.get("availableBalance", 0.0))
            im = float(fa.get("initialMargin", 0.0))
            ast = fa.get("asset", "")
            if mb > 0.01 or im > 0.0:
                balances.append({
                    "asset": f"{ast} (Perp)",
                    "free": f"{av:.4f}",
                    "locked": f"{im:.4f}",
                    "borrowed": "0.00",
                })

    sp = spot_api.request("GET", "/api/v3/account", signed=True)
    if isinstance(sp, dict) and "balances" in sp:
        for sb in sp.get("balances", []):
            ast = sb.get("asset", "")
            fr = float(sb.get("free", 0.0))
            lk = float(sb.get("locked", 0.0))
            if fr + lk >= 0.05 and ast in ("USDT", "USDC", "FDUSD", "USD1", "U", "BNB", "SOL", "ETH", "BTC"):
                balances.append({
                    "asset": f"{ast} (Spot)",
                    "free": f"{fr:.4f}",
                    "locked": f"{lk:.4f}",
                    "borrowed": "0.00",
                })

    cm = spot_api.request("GET", "/sapi/v1/margin/account", signed=True)
    if isinstance(cm, dict) and "userAssets" in cm:
        for ca in cm.get("userAssets", []):
            ast = ca.get("asset", "")
            fr = float(ca.get("free", 0.0))
            lk = float(ca.get("locked", 0.0))
            br = float(ca.get("borrowed", 0.0))
            if fr + lk + br > 0.01 and ast in ("USDT", "USDC", "FDUSD", "USD1", "U", "BNB", "SOL", "ETH", "BTC"):
                balances.append({
                    "asset": f"{ast} (5x Cross)",
                    "free": f"{fr:.4f}",
                    "locked": f"{lk:.4f}",
                    "borrowed": f"{br:.4f}",
                })

    return raw_wallet_usd, binance_futures, balances


def main():
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=f"https://s3.{AWS_REGION}.amazonaws.com",
        config=Config(signature_version="s3v4"),
    )
    spot_api = BinanceSession("api.binance.com")
    fapi = BinanceSession("fapi.binance.com")

    init_paused, init_sig_ts, init_sig_src = read_s3_pause_state(s3, default_val=True)
    state = {
        "user_paused": init_paused,
        "pause_sig_ts": init_sig_ts,
        "pause_sig_src": init_sig_src,
        "boot_lock": threading.Lock(),
    }
    write_flag(init_paused)
    if init_paused:
        stop_go_bot_gracefully()
        def _sweep_paused():
            try:
                with open(LOG_PATH, "a", encoding="utf-8", errors="replace") as lf:
                    subprocess.run([LIGHTER_GO_BIN], stdout=lf, stderr=subprocess.STDOUT, timeout=20)
            except Exception:
                pass
        threading.Thread(target=_sweep_paused, daemon=True).start()
    else:
        threading.Thread(target=boot_go_bot, args=(state,), daemon=True).start()

    pause_url, pause_post = ensure_s3_cors_and_presign(s3)
    last_presign = time.time()
    cached_wallet = 0.0
    cached_futures = {"marginBalance": 0.0, "availableBalance": 0.0, "initialMargin": 0.0, "unrealizedProfit": 0.0}
    cached_balances = []
    cached_audit = None
    cached_dn_hist = read_dn_history()
    last_bn_poll = 0.0
    last_lt_poll = 0.0
    last_inc_poll = 0.0
    last_s3_push = 0.0
    last_pause_poll = 0.0
    last_proc_chk = 0.0
    last_tick_sync = 0.0
    uploaded_tick_files = set()
    tick_sync_lock = threading.Lock()

    while True:
        now = time.time()
        try:
            if now - last_presign >= 21600.0:
                pause_url, pause_post = ensure_s3_cors_and_presign(s3)
                last_presign = now

            if now - last_pause_poll >= 1.0:
                last_pause_poll = now
                want_paused, sig_ts, sig_src = read_s3_pause_state(s3, default_val=state["user_paused"])
                if sig_ts > state["pause_sig_ts"]:
                    state["pause_sig_ts"] = sig_ts
                    state["pause_sig_src"] = sig_src
                    if want_paused:
                        write_flag(True)
                        append_log(f"[UI-PAUSE-SIGNAL ts={sig_ts} src={sig_src}] Pause & flatten confirmed.")
                if want_paused and not state["user_paused"]:
                    state["user_paused"] = True
                    threading.Thread(target=stop_go_bot_gracefully, daemon=True).start()
                elif not want_paused and state["user_paused"]:
                    state["user_paused"] = False
                    write_flag(False)
                    threading.Thread(target=boot_go_bot, args=(state,), daemon=True).start()

            if now - last_proc_chk >= 5.0:
                last_proc_chk = now
                ensure_arena_running()
                ensure_tick_collector_running()
                if not state["user_paused"] and os.path.exists(PAUSE_FLAG_PATH) and not is_go_bot_running():
                    state["user_paused"] = True
                    append_log("[TELEMETRY] Detected local pause flag exit from Go engine; keeping paused.")
                elif (
                    not state["user_paused"]
                    and not os.path.exists(PAUSE_FLAG_PATH)
                    and not is_go_bot_running()
                    and not state["boot_lock"].locked()
                ):
                    threading.Thread(target=boot_go_bot, args=(state,), daemon=True).start()

            if now - last_tick_sync >= 15.0:
                last_tick_sync = now
                threading.Thread(
                    target=sync_ticks_to_s3,
                    args=(s3, uploaded_tick_files, tick_sync_lock),
                    daemon=True,
                ).start()

            if now - last_bn_poll >= 2.0:
                last_bn_poll = now
                rw, bf, bals = fetch_binance_snapshot(spot_api, fapi)
                if rw is not None and rw > 0:
                    cached_wallet = rw
                if bf is not None:
                    cached_futures = bf
                if bals:
                    cached_balances = bals

            if now - last_lt_poll >= 3.0:
                last_lt_poll = now
                fetch_lighter_fallback()

            if now - last_inc_poll >= 15.0:
                last_inc_poll = now
                try:
                    lt_tr_obj = read_json_safe("/tmp/lighter_trades.json", {})
                    lt_tr_list = lt_tr_obj.get("trades", []) if isinstance(lt_tr_obj, dict) else []
                    aud, hist = fetch_binance_income_and_reconstruct(
                        fapi,
                        lt_tr_list,
                        read_dn_history(),
                        cached_wallet,
                        cached_futures.get("unrealizedProfit", 0.0),
                    )
                    if aud is not None:
                        cached_audit = aud
                    if hist is not None:
                        cached_dn_hist = hist
                except Exception as inc_err:
                    append_log(f"[INC_RECON_ERR] {inc_err}")

            if now - last_s3_push >= 1.0:
                last_s3_push = now
                paused = bool(state["user_paused"])
                status_msg = (
                    f"PAUSED BY USER — 100% FLAT (Binance Wallet ${cached_wallet:.4f})"
                    if paused
                    else f"GO-TOKYO v105.0 LIVE MICRO-ARENA | Binance Wallet ${cached_wallet:.4f} | Avail ${cached_futures.get('availableBalance', 0):.2f}"
                )
                lt_acct = read_json_safe("/tmp/lighter_acct.json")
                lt_trades_obj = read_json_safe("/tmp/lighter_trades.json", {})
                lt_trades = lt_trades_obj.get("trades", []) if isinstance(lt_trades_obj, dict) else []
                arena_obj = read_json_safe(ARENA_JSON_PATH)
                live_arena_obj = read_json_safe(LIVE_ARENA_JSON_PATH)
                aster_acct_obj = read_json_safe(ASTER_ACCT_JSON_PATH)

                payload = {
                    "ts": int(now * 1000),
                    "startEquityUsd": START_EQUITY_USD,
                    "totalLiveUsd": cached_wallet,
                    "rawWalletUsd": cached_wallet,
                    "netEquityGainTodayUsd": round(cached_wallet - START_EQUITY_USD, 4),
                    "binanceFutures": cached_futures,
                    "binanceAudit": cached_audit,
                    "balances": cached_balances,
                    "lighterAccount": lt_acct,
                    "lighterTrades": lt_trades,
                    "asterAccount": aster_acct_obj,
                    "dnTradesHistory": cached_dn_hist,
                    "lighterLog": read_log_tail(8000),
                    "forwardArena": arena_obj,
                    "liveArena": live_arena_obj,
                    "pauseControlUrl": pause_url,
                    "pauseControlPost": pause_post,
                    "hftTest": {
                        "status": status_msg,
                        "paused": paused,
                        "userPaused": paused,
                        "pauseSignalTs": state["pause_sig_ts"],
                        "pauseSource": state["pause_sig_src"],
                    },
                }
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=S3_KEY,
                    Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                    ContentType="application/json",
                    CacheControl="no-store, no-cache, must-revalidate, max-age=0",
                )
                if os.path.exists(ARENA_JSON_PATH):
                    try:
                        with open(ARENA_JSON_PATH, "rb") as af:
                            arena_bytes = af.read()
                        if len(arena_bytes) > 50:
                            s3.put_object(
                                Bucket=S3_BUCKET,
                                Key=ARENA_S3_KEY,
                                Body=arena_bytes,
                                ContentType="application/json",
                                CacheControl="no-store, no-cache, must-revalidate, max-age=0",
                            )
                    except Exception:
                        pass
        except Exception as e:
            print("Telemetry loop err:", e, flush=True)

        time.sleep(0.2)


if __name__ == "__main__":
    main()
