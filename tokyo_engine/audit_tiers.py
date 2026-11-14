import sys
sys.path.insert(0, "/opt/crypto-arb-bot")
import telemetry_sync as ts

api = ts.BinanceMarginSession()
for sym in ["FDUSDUSDC", "FDUSDUSDT", "USD1USDT", "USDCUSDT"]:
    t = api.request("GET", "/sapi/v1/margin/isolatedMarginTier", {"symbol": sym}, signed=True)
    print("TIER", sym, t[:2] if isinstance(t, list) else t)
    mb_q = api.request("GET", "/sapi/v1/margin/maxBorrowable", {"asset": sym[-4:], "isolatedSymbol": sym}, signed=True)
    print("ISO_MB", sym, mb_q)

cm_all = api.request("GET", "/sapi/v1/margin/allPairs", {}, signed=True)
cm_syms = {p["symbol"] for p in cm_all} if isinstance(cm_all, list) else set()
for sym in ["FDUSDUSDC", "FDUSDUSDT", "USD1USDT", "USDCUSDT", "USD1USDC", "BNBU", "BTCU"]:
    print("IN_CM", sym, sym in cm_syms)

iso = api.request("GET", "/sapi/v1/margin/isolated/account", {}, signed=True)
for a in iso.get("assets", []):
    if a["symbol"] in ("FDUSDUSDC", "FDUSDUSDT", "USD1USDT", "USDCUSDT"):
        print("ISO_ACCT", a["symbol"], "marginRatio:", a.get("marginRatio"), "enabled:", a.get("enabled"), "baseNet:", a["baseAsset"]["netAsset"], "quoteNet:", a["quoteAsset"]["netAsset"])
