#!/usr/bin/env python3
"""1-3s remote S3 pause/resume controller & verifier for Tokyo Lighter HFT bot."""
import json
import sys
import time
import urllib.request

S3_TEL_URL = "https://crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com/live-telemetry.json"
LT_ACC_URL = "https://mainnet.zklighter.elliot.ai/api/v1/account?by=index&value=770015"


def fetch_json(url: str) -> dict:
    sep = "&" if "?" in url else "?"
    req = urllib.request.Request(
        f"{url}{sep}_t={int(time.time() * 1000)}",
        headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"},
    )
    with urllib.request.urlopen(req, timeout=6) as r:
        return json.loads(r.read().decode("utf-8"))


def acct_snapshot() -> tuple[float, list]:
    acc = fetch_json(LT_ACC_URL)["accounts"][0]
    eq = float(acc.get("total_asset_value", 0.0))
    open_items = [
        (p.get("symbol"), p.get("position"), p.get("open_order_count", 0))
        for p in acc.get("positions", [])
        if abs(float(p.get("position", 0.0))) > 1e-8 or int(p.get("open_order_count", 0)) > 0
    ]
    return eq, open_items


def send_flag(paused: bool) -> float:
    t0 = time.monotonic()
    tel = fetch_json(S3_TEL_URL)
    p_url = tel.get("pauseControlUrl", "")
    if not p_url:
        raise RuntimeError("Missing pauseControlUrl in live-telemetry.json")
    body = json.dumps({"paused": paused, "ts": int(time.time() * 1000)}).encode("utf-8")
    req = urllib.request.Request(p_url, data=body, headers={"Content-Type": "text/plain"}, method="PUT")
    with urllib.request.urlopen(req, timeout=6):
        pass
    return t0


def cmd_pause() -> int:
    t0 = send_flag(True)
    for _ in range(13):
        tel = fetch_json(S3_TEL_URL)
        user_paused = bool(tel.get("hftTest", {}).get("userPaused", False))
        log = tel.get("lighterLog", "")
        if user_paused or "[PAUSED]" in log or "[INSTANT-PAUSE]" in log:
            eq, open_items = acct_snapshot()
            dt = time.monotonic() - t0
            print(f"INSTANT_PAUSE_VERIFIED in {dt:.2f}s | userPaused={user_paused} | Eq=${eq:.2f} | Open={open_items}")
            return 0 if not open_items else 2
        time.sleep(0.8)
    eq, open_items = acct_snapshot()
    print(f"PAUSE_PUT_SENT_TIMEOUT_10S | Eq=${eq:.2f} | Open={open_items}")
    return 1


def cmd_resume() -> int:
    t0 = send_flag(False)
    for _ in range(10):
        tel = fetch_json(S3_TEL_URL)
        user_paused = bool(tel.get("hftTest", {}).get("userPaused", False))
        if not user_paused:
            eq, open_items = acct_snapshot()
            print(f"RESUME_VERIFIED in {time.monotonic() - t0:.2f}s | userPaused={user_paused} | Eq=${eq:.2f} | Open={open_items}")
            return 0
        time.sleep(0.8)
    print("RESUME_PUT_SENT_WAITING")
    return 1


def cmd_status() -> int:
    tel = fetch_json(S3_TEL_URL)
    eq, open_items = acct_snapshot()
    user_paused = bool(tel.get("hftTest", {}).get("userPaused", False))
    tel_ts = tel.get("ts") or tel.get("timestamp") or 0
    age_s = max(0.0, time.time() - tel_ts / 1000.0) if tel_ts else -1.0
    print(f"userPaused={user_paused} | Eq=${eq:.2f} | Open={open_items} | s3_age={age_s:.1f}s")
    return 0


def main() -> int:
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "pause").lower()
    if cmd == "pause":
        return cmd_pause()
    if cmd == "resume":
        return cmd_resume()
    if cmd == "status":
        return cmd_status()
    print(f"Usage: {sys.argv[0]} [pause|resume|status]")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
