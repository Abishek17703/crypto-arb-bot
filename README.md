# Crypto Arb Bot — GO-TOKYO v99.0 (KISS Core)

Single-workflow **0%-fee delta-neutral engine** trading Binance Futures (`USDC-M`, 0.00% Maker) vs Lighter Perps (0.00% fee) from AWS Tokyo (`ap-northeast-1`).

## Core Architecture
- [main.go](tokyo_engine/lighter_go/main.go): Single-slot `$40` KISS Go engine featuring:
  - Event-driven `<1.5ms` Binance Futures User Data WS (`listenKey` `ORDER_TRADE_UPDATE`) + Lighter WS.
  - Continuous 90s EWMA basis dislocation filter (`rawEdge >= 2.2bp` & `disloc >= 3.2bp`), `$250` BBO depth floor, and `500ms` jump anti-pickoff guard.
  - Verified 2-stage Lighter IOC hedge guard with instant Binance `UNHEDGED_ABORT` unwind if unhedged.
  - Patient 0%-fee GTX Maker convergence exit + `:58` hourly funding check (exits pre-`:00` only when net funding `< -1.5bp`).
- [telemetry_sync.py](tokyo_engine/telemetry_sync.py): Read-only AWS S3 telemetry publisher, exchange-audited trade reconstructor (`binanceAudit` + `dnTradesHistory`), and pause watchdog.
- [lighter_live_dashboard.html](tokyo_engine/lighter_live_dashboard.html): Live S3 + Lighter dashboard (mirrored in [mobile_app](mobile_app/app/src/main/assets/lighter_live_dashboard.html)).
- [deploy.yml](.github/workflows/deploy.yml): Single sequential CI pipeline (`build-and-verify` -> `deploy-tokyo` via SSM -> conditional `build-apk`).

## Controls
```bash
python3 tokyo_engine/instant_pause.py status
python3 tokyo_engine/instant_pause.py pause
python3 tokyo_engine/instant_pause.py resume
```
