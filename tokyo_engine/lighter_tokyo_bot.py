#!/usr/bin/env python3
import asyncio, json, math, socket, time, urllib.request, websockets
import lighter

URL = "https://mainnet.zklighter.elliot.ai"
ACCT_IDX = 770015
KEY_IDX = 4
PRIV_KEY = "56491130b44362b137b0a3b46dc63d647865876f11ff1613c857f946ec8612f6e4f216d065f4695a"

MARKETS = {
    "ETH": {"id": 0, "sz_dec": 4, "px_dec": 2, "min_b": 0.0020},
    "SOL": {"id": 2, "sz_dec": 3, "px_dec": 3, "min_b": 0.055},
}
ID_TO_SYM = {v["id"]: k for k, v in MARKETS.items()}

CLIP_USD = 10.60
EDGE_BPS = 4.0
CLOSE_EDGE_BPS = 2.0
CANCEL_BPS = 1.5
STOP_EQ_USD = 14.50

bn_mid = {}
lt_bbo = {}
active_orders = {}
positions = {}
avg_entry = {}
last_ord_ts = {}
next_coi = int(time.time() * 100) % 10_000_000
running = True
client = None
lt_ws_conn = None
last_log = 0.0
eq_now = 14.98


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=6) as r:
        return json.loads(r.read())


async def cancel_all():
    try:
        await client.cancel_all_orders(
            time_in_force=client.CANCEL_ALL_TIF_IMMEDIATE,
            timestamp_ms=0,
            api_key_index=KEY_IDX,
        )
        active_orders.clear()
    except Exception as e:
        print(f"[CANCEL ALL ERR] {e}", flush=True)


async def sync_account_loop():
    global positions, avg_entry, eq_now, running
    while running:
        try:
            d = await asyncio.to_thread(fetch_json, f"{URL}/api/v1/account?by=index&value={ACCT_IDX}")
            acct = (d.get("accounts") or [{}])[0]
            eq_now = float(acct.get("total_asset_value") or acct.get("collateral") or 14.98)
            new_pos, new_avg = {}, {}
            for p in acct.get("positions", []):
                sym = ID_TO_SYM.get(p.get("market_id"))
                if sym:
                    sz = float(p.get("position") or 0.0)
                    sgn = p.get("sign", 1)
                    new_pos[sym] = sz * (1 if sgn >= 0 else -1) if sz > 0 else sz
                    new_avg[sym] = float(p.get("avg_entry_price") or 0.0)
            positions = new_pos
            avg_entry = new_avg
            if eq_now <= STOP_EQ_USD:
                print(f"[STOP-LOSS] Equity ${eq_now:.4f} <= ${STOP_EQ_USD:.2f}, halting!", flush=True)
                running = False
                await cancel_all()
                break
        except Exception as e:
            print(f"[SYNC ERR] {e}", flush=True)
        await asyncio.sleep(2.5)


async def cancel_side(sym, side):
    ao = active_orders.pop((sym, side), None)
    if not ao:
        return
    try:
        if lt_ws_conn is not None:
            tx_t, tx_i, _, err = client.sign_cancel_order(
                market_index=MARKETS[sym]["id"],
                order_index=ao["coi"],
                api_key_index=KEY_IDX,
            )
            if not err and tx_i:
                await lt_ws_conn.send(json.dumps({"type": "jsonapi/sendtx", "data": {"id": f"c_{ao['coi']}", "tx_type": int(tx_t), "tx_info": tx_i}}))
                return
        await client.cancel_order(
            market_index=MARKETS[sym]["id"],
            order_index=ao["coi"],
            api_key_index=KEY_IDX,
        )
    except Exception as e:
        print(f"[CANCEL {sym} {side} ERR] {e}", flush=True)


async def place_side(sym, side, px_f, mid_f):
    global next_coi
    m = MARKETS[sym]
    base_f = max(m["min_b"], CLIP_USD / px_f)
    base_int = int(math.ceil(base_f * (10 ** m["sz_dec"])))
    px_int = int(round(px_f * (10 ** m["px_dec"])))
    next_coi += 1
    coi = next_coi
    try:
        if lt_ws_conn is not None:
            tx_t, tx_i, _, err = client.sign_create_order(
                market_index=m["id"],
                client_order_index=coi,
                base_amount=base_int,
                price=px_int,
                is_ask=(side == "ASK"),
                order_type=client.ORDER_TYPE_LIMIT,
                time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
                order_expiry=client.DEFAULT_28_DAY_ORDER_EXPIRY,
                api_key_index=KEY_IDX,
            )
            if err:
                if "post only" not in str(err).lower():
                    print(f"[WS SIGN ERR] {sym} {side}: {err}", flush=True)
                return
            await lt_ws_conn.send(json.dumps({"type": "jsonapi/sendtx", "data": {"id": f"o_{coi}", "tx_type": int(tx_t), "tx_info": tx_i}}))
            active_orders[(sym, side)] = {"coi": coi, "px": px_f, "ref_mid": mid_f, "ts": time.time()}
            return
        _, _, err = await client.create_order(
            market_index=m["id"],
            client_order_index=coi,
            base_amount=base_int,
            price=px_int,
            is_ask=(side == "ASK"),
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
            order_expiry=client.DEFAULT_28_DAY_ORDER_EXPIRY,
            api_key_index=KEY_IDX,
        )
        if err:
            if "post only" not in str(err).lower():
                print(f"[ORD ERR] {sym} {side}: {err}", flush=True)
            return
        active_orders[(sym, side)] = {"coi": coi, "px": px_f, "ref_mid": mid_f, "ts": time.time()}
    except Exception as e:
        print(f"[ORD EXC] {sym} {side}: {e}", flush=True)


async def manage_coin(sym):
    global last_log
    if sym not in bn_mid or sym not in lt_bbo:
        return
    mid = bn_mid[sym]
    lt_b, lt_a = lt_bbo[sym]
    if lt_b <= 0 or lt_a <= 0:
        return
    pos_usd = positions.get(sym, 0.0) * mid

    for side in ("BID", "ASK"):
        ao = active_orders.get((sym, side))
        if ao:
            is_close = (side == "BID" and pos_usd <= -5.0) or (side == "ASK" and pos_usd >= 5.0)
            move_bps = abs(mid - ao["ref_mid"]) / ao["ref_mid"] * 10000.0
            age_s = time.time() - ao["ts"]
            too_close = (side == "BID" and ao["px"] >= lt_a) or (side == "ASK" and ao["px"] <= lt_b)
            if (not is_close and (move_bps >= CANCEL_BPS or age_s >= 45.0)) or too_close:
                await cancel_side(sym, side)

    allow_bid = pos_usd < 5.0
    allow_ask = pos_usd > -5.0
    ae = avg_entry.get(sym, 0.0)
    m = MARKETS[sym]
    tick = 10.0 ** (-m["px_dec"])

    want_bid = round(min(lt_b, mid * (1.0 - EDGE_BPS / 10000.0)), m["px_dec"])
    if pos_usd <= -5.0 and ae > 0:
        p_bid = round(min(lt_b, ae * (1.0 - CLOSE_EDGE_BPS / 10000.0)), m["px_dec"])
        want_bid = min(want_bid, p_bid) if want_bid > p_bid else p_bid

    want_ask = round(max(lt_a, mid * (1.0 + EDGE_BPS / 10000.0)), m["px_dec"])
    if pos_usd >= 5.0 and ae > 0:
        p_ask = round(max(lt_a, ae * (1.0 + CLOSE_EDGE_BPS / 10000.0)), m["px_dec"])
        want_ask = max(want_ask, p_ask) if want_ask < p_ask else p_ask

    if want_bid >= lt_a:
        want_bid = round(lt_a - tick, m["px_dec"])
    if want_ask <= lt_b:
        want_ask = round(lt_b + tick, m["px_dec"])

    now_t = time.time()
    if allow_bid and ((sym, "BID") not in active_orders) and (now_t - last_ord_ts.get((sym, "BID"), 0.0) >= 3.0):
        last_ord_ts[(sym, "BID")] = now_t
        await place_side(sym, "BID", want_bid, mid)
    if allow_ask and ((sym, "ASK") not in active_orders) and (now_t - last_ord_ts.get((sym, "ASK"), 0.0) >= 3.0):
        last_ord_ts[(sym, "ASK")] = now_t
        await place_side(sym, "ASK", want_ask, mid)

    if now_t - last_log >= 20.0:
        last_log = now_t
        print(f"[{time.strftime('%H:%M:%S')}] TOKYO Eq=${eq_now:.4f} | Pos={positions} | Active={len(active_orders)}", flush=True)


async def run_bn_ws():
    streams = "/".join(f"{s.lower()}usdt@bookTicker" for s in MARKETS)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    while running:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                while running:
                    d = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))["data"]
                    sym = d["s"].replace("USDT", "")
                    bn_mid[sym] = 0.5 * (float(d["b"]) + float(d["a"]))
                    await manage_coin(sym)
        except Exception as e:
            print(f"[BN WS] {e}", flush=True)
            await asyncio.sleep(1.0)


async def run_lt_ws():
    global lt_ws_conn
    url = "wss://mainnet.zklighter.elliot.ai/stream"
    while running:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                lt_ws_conn = ws
                try:
                    ws.transport.get_extra_info("socket").setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except Exception:
                    pass
                for m in MARKETS.values():
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{m['id']}"}))
                while running:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                    ch = msg.get("channel", "")
                    if ch.startswith("order_book:"):
                        sym = ID_TO_SYM.get(int(ch.split(":")[1]))
                        ob = msg.get("order_book", {})
                        bids, asks = ob.get("bids") or [], ob.get("asks") or []
                        if sym and bids and asks:
                            lt_bbo[sym] = (float(bids[0]["price"]), float(asks[0]["price"]))
        except Exception as e:
            lt_ws_conn = None
            print(f"[LT WS] {e}", flush=True)
            await asyncio.sleep(1.0)


async def main():
    global client, running
    client = lighter.SignerClient(url=URL, account_index=ACCT_IDX, api_private_keys={KEY_IDX: PRIV_KEY})
    err = client.check_client()
    print(f"TOKYO Lighter SignerClient check={err}", flush=True)
    if err is not None:
        return
    await cancel_all()
    print(f"TOKYO Lighter Bot LIVE | Acct #{ACCT_IDX} | Clip=${CLIP_USD} | Edge={EDGE_BPS}bps | Stop=${STOP_EQ_USD}", flush=True)
    try:
        await asyncio.gather(sync_account_loop(), run_bn_ws(), run_lt_ws())
    finally:
        running = False
        await cancel_all()
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
