use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

const S3_TEL_URL: &str =
    "https://crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com/live-telemetry.json";
const LT_ACC_URL: &str =
    "https://mainnet.zklighter.elliot.ai/api/v1/account?by=index&value=770015";
const GH_RUNS_URL: &str =
    "https://api.github.com/repos/Abishek17703/crypto-arb-bot/actions/runs?per_page=6";

#[derive(Parser)]
#[command(name = "carbon_ctl", about = "Native Rust controller for Tokyo Carbon Sniper v89")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Status,
    Strategies,
    Pause,
    Resume,
    WaitBuild {
        sha: String,
    },
    Order {
        sym: String,
        side: String,
        usd: f64,
        sl_bps: f64,
        tp_bps: f64,
        #[arg(default_value_t = 900)]
        max_hold_sec: i64,
        #[arg(default_value = "BN_MOM_SNIPE")]
        strat: String,
    },
    WaitScan {
        #[arg(default_value_t = 0)]
        min_seq: i64,
    },
    WaitClose {
        order_id: i64,
    },
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn parse_https_url(url: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| format!("Only https supported: {url}"))?;
    let (host_port, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    let (host, port) = match host_port.find(':') {
        Some(idx) => (
            host_port[..idx].to_string(),
            host_port[idx + 1..].parse::<u16>().unwrap_or(443),
        ),
        None => (host_port.to_string(), 443),
    };
    Ok((host, port, path.to_string()))
}

fn decode_chunked(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        let mut line_end = i;
        while line_end + 1 < raw.len() && !(raw[line_end] == b'\r' && raw[line_end + 1] == b'\n') {
            line_end += 1;
        }
        let size_str = std::str::from_utf8(&raw[i..line_end])
            .unwrap_or("0")
            .split(';')
            .next()
            .unwrap_or("0")
            .trim();
        let sz = usize::from_str_radix(size_str, 16).unwrap_or(0);
        i = (line_end + 2).min(raw.len());
        if sz == 0 {
            break;
        }
        let end = (i + sz).min(raw.len());
        out.extend_from_slice(&raw[i..end]);
        i = (end + 2).min(raw.len());
    }
    out
}

async fn http_req(
    method: &str,
    url: &str,
    body: Option<&[u8]>,
    content_type: Option<&str>,
) -> Result<(u16, String), String> {
    let (host, port, path) = parse_https_url(url)?;
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let connector = TlsConnector::from(cfg);
    let tcp = tokio::time::timeout(
        Duration::from_secs(8),
        TcpStream::connect((host.as_str(), port)),
    )
    .await
    .map_err(|_| format!("connect timeout {host}"))?
    .map_err(|e| e.to_string())?;
    let sni = ServerName::try_from(host.clone()).map_err(|e| e.to_string())?;
    let mut stream = connector
        .connect(sni, tcp)
        .await
        .map_err(|e| e.to_string())?;

    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: carbon-ctl/1.0\r\nCache-Control: no-cache\r\nAccept: application/json\r\nConnection: close\r\n"
    );
    if let Some(b) = body {
        let ct = content_type.unwrap_or("text/plain");
        req.push_str(&format!("Content-Type: {ct}\r\nContent-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    if let Some(b) = body {
        stream.write_all(b).await.map_err(|e| e.to_string())?;
    }
    stream.flush().await.ok();

    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut buf))
        .await
        .map_err(|_| "read timeout".to_string())?
        .map_err(|e| e.to_string())?;

    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response".to_string())?;
    let hdrs = String::from_utf8_lossy(&buf[..sep]).to_string();
    let status = hdrs
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let raw_body = &buf[sep + 4..];
    let body_bytes = if hdrs.to_lowercase().contains("transfer-encoding: chunked") {
        decode_chunked(raw_body)
    } else {
        raw_body.to_vec()
    };
    Ok((status, String::from_utf8_lossy(&body_bytes).to_string()))
}

async fn fetch_json(url: &str) -> Result<Value, String> {
    let sep = if url.contains('?') { '&' } else { '?' };
    let u = if url.contains("amazonaws.com") {
        format!("{url}{sep}_t={}", now_ms())
    } else {
        url.to_string()
    };
    let (st, body) = http_req("GET", &u, None, None).await?;
    if st >= 400 || st == 0 {
        return Err(format!("HTTP {st} from {url}"));
    }
    serde_json::from_str(&body).map_err(|e| format!("JSON err: {e}"))
}

async fn acct_snapshot() -> Result<(f64, Vec<(String, String, i64)>), String> {
    let v = fetch_json(LT_ACC_URL).await?;
    let a = v["accounts"]
        .get(0)
        .ok_or_else(|| "missing account".to_string())?;
    let eq = a["total_asset_value"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let mut open = Vec::new();
    if let Some(arr) = a["positions"].as_array() {
        for p in arr {
            let sz = p["position"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let oc = p["open_order_count"].as_i64().unwrap_or(0);
            if sz.abs() > 1e-8 || oc > 0 {
                open.push((
                    p["symbol"].as_str().unwrap_or("?").to_string(),
                    p["position"].as_str().unwrap_or("0").to_string(),
                    oc,
                ));
            }
        }
    }
    Ok((eq, open))
}

async fn put_pause_control(payload: Value) -> Result<(), String> {
    let tel = fetch_json(S3_TEL_URL).await?;
    let p_url = tel["pauseControlUrl"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Missing pauseControlUrl".to_string())?;
    let body = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    let (st, resp) = http_req("PUT", p_url, Some(&body), Some("text/plain")).await?;
    if st >= 300 || st == 0 {
        return Err(format!("PUT failed {st}: {resp}"));
    }
    Ok(())
}

fn parse_scan_seq(log: &str) -> (i64, i64) {
    let mut max_seq = 0;
    let mut max_score = 0;
    for line in log.lines().rev() {
        if let Some(idx) = line.find("[CARBON_SCAN seq=") {
            let rest = &line[idx + "[CARBON_SCAN seq=".len()..];
            if let Some(end) = rest.find(']') {
                max_seq = rest[..end].parse::<i64>().unwrap_or(0);
            }
            for part in line.split("sc=") {
                if let Some(sp) = part.split(|c: char| !c.is_ascii_digit()).next() {
                    if let Ok(sc) = sp.parse::<i64>() {
                        if sc > max_score && sc <= 10 {
                            max_score = sc;
                        }
                    }
                }
            }
            break;
        }
    }
    (max_seq, max_score)
}

fn sync_journal(log: &str) {
    use std::io::Write;
    let j_path = "strategy_journal.log";
    let existing = std::fs::read_to_string(j_path).unwrap_or_default();
    let mut seen: std::collections::HashSet<&str> = existing.lines().collect();
    if existing.is_empty() {
        let _ = std::fs::write(j_path, "# GO-TOKYO v91.0 Strategy Journal (Auto-Synced)\n");
    }
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(j_path) {
        let mut board_latest: Option<&str> = None;
        for l in log.lines() {
            if l.contains("[STRAT_BOARD") {
                board_latest = Some(l);
            }
            if (l.contains("[STRAT_OPEN") || l.contains("[STRAT_CLOSE")) && !seen.contains(l) {
                let _ = writeln!(f, "{l}");
                seen.insert(l);
            }
        }
        if let Some(bl) = board_latest {
            if !seen.contains(bl) {
                let _ = writeln!(f, "{bl}");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    let code = match cli.cmd {
        Cmd::Status => match tokio::join!(fetch_json(S3_TEL_URL), acct_snapshot()) {
            (Ok(tel), Ok((eq, open))) => {
                let paused = tel["hftTest"]["userPaused"].as_bool().unwrap_or(false);
                let age_s = (now_ms() - tel["ts"].as_i64().unwrap_or(0)) as f64 / 1000.0;
                println!("Eq=${eq:.4} | userPaused={paused} | s3_age={age_s:.1}s | Open={open:?}");
                let log = tel["lighterLog"].as_str().unwrap_or("");
                let tail: Vec<&str> = log.lines().rev().take(10).collect();
                for l in tail.into_iter().rev() {
                    println!("{l}");
                }
                0
            }
            (e1, e2) => {
                eprintln!("STATUS_ERR: {e1:?} {e2:?}");
                1
            }
        },
        Cmd::Strategies => match tokio::join!(fetch_json(S3_TEL_URL), acct_snapshot()) {
            (Ok(tel), Ok((eq, open))) => {
                let paused = tel["hftTest"]["userPaused"].as_bool().unwrap_or(false);
                let age_s = (now_ms() - tel["ts"].as_i64().unwrap_or(0)) as f64 / 1000.0;
                println!("Eq=${eq:.4} | userPaused={paused} | s3_age={age_s:.1}s | Open={open:?}");
                let log = tel["lighterLog"].as_str().unwrap_or("");
                sync_journal(log);
                let mut shown: Vec<&str> = log
                    .lines()
                    .filter(|l| {
                        l.contains("GO-TOKYO v")
                            || l.contains("[STRAT_BOARD")
                            || l.contains("[STRAT_SCAN")
                            || l.contains("[STRAT_OPEN")
                            || l.contains("[STRAT_CLOSE")
                    })
                    .collect();
                if shown.len() > 20 {
                    shown = shown[shown.len() - 20..].to_vec();
                }
                for l in shown {
                    println!("{l}");
                }
                0
            }
            (e1, e2) => {
                eprintln!("STRATEGIES_ERR: {e1:?} {e2:?}");
                1
            }
        },
        Cmd::Pause => {
            if let Err(e) = put_pause_control(json!({"paused": true, "ts": now_ms()})).await {
                eprintln!("PAUSE_ERR: {e}");
                std::process::exit(1);
            }
            let mut ok = false;
            for _ in 0..18 {
                if let Ok(tel) = fetch_json(S3_TEL_URL).await {
                    let paused = tel["hftTest"]["userPaused"].as_bool().unwrap_or(false);
                    if paused {
                        if let Ok((eq, open)) = acct_snapshot().await {
                            println!("PAUSED_OK | Eq=${eq:.4} | Open={open:?}");
                        } else {
                            println!("PAUSED_OK");
                        }
                        ok = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            if !ok {
                println!("PAUSE_SENT_TIMEOUT_18S");
            }
            0
        }
        Cmd::Resume => {
            if let Err(e) = put_pause_control(json!({"paused": false, "ts": now_ms()})).await {
                eprintln!("RESUME_ERR: {e}");
                std::process::exit(1);
            }
            let mut ok = false;
            for _ in 0..18 {
                if let Ok(tel) = fetch_json(S3_TEL_URL).await {
                    let paused = tel["hftTest"]["userPaused"].as_bool().unwrap_or(true);
                    if !paused {
                        if let Ok((eq, open)) = acct_snapshot().await {
                            println!("RESUMED_OK | Eq=${eq:.4} | Open={open:?}");
                        } else {
                            println!("RESUMED_OK");
                        }
                        ok = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            if !ok {
                println!("RESUME_SENT_WAITING");
            }
            0
        }
        Cmd::WaitBuild { sha } => {
            let mut rc = 1;
            for _ in 0..50 {
                if let Ok(v) = fetch_json(GH_RUNS_URL).await {
                    if let Some(arr) = v["workflow_runs"].as_array() {
                        if let Some(run) = arr.iter().find(|r| {
                            r["head_sha"].as_str().unwrap_or("").starts_with(&sha)
                                && r["name"].as_str().unwrap_or("").contains("Go")
                        }) {
                            let st = run["status"].as_str().unwrap_or("");
                            let conc = run["conclusion"].as_str().unwrap_or("");
                            if st == "completed" {
                                println!("BUILD_DONE status={st} conclusion={conc}");
                                rc = if conc == "success" { 0 } else { 1 };
                                break;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            rc
        }
        Cmd::Order {
            sym,
            side,
            usd,
            sl_bps,
            tp_bps,
            max_hold_sec,
            strat,
        } => {
            let oid = now_ms();
            let usd_clamped = usd.clamp(11.5, 240.0);
            let strat_up = strat.to_uppercase();
            let payload = json!({
                "paused": false,
                "ts": oid,
                "carbonOrder": {
                    "id": oid,
                    "sym": sym.to_uppercase(),
                    "side": side.to_uppercase(),
                    "usd": usd_clamped,
                    "slBps": sl_bps,
                    "tpBps": tp_bps,
                    "maxHoldSec": max_hold_sec,
                    "strat": strat_up
                }
            });
            match put_pause_control(payload).await {
                Ok(_) => {
                    println!(
                        "ORDER_SENT id={oid} sym={} side={} strat={} usd={usd_clamped:.2} sl={sl_bps:.1}bps tp={tp_bps:.1}bps hold={max_hold_sec}s",
                        sym.to_uppercase(), side.to_uppercase(), strat_up
                    );
                    0
                }
                Err(e) => {
                    eprintln!("ORDER_ERR: {e}");
                    1
                }
            }
        }
        Cmd::WaitScan { min_seq } => {
            let rc: i32;
            let mut ticks = 0;
            loop {
                ticks += 1;
                if let (Ok(tel), Ok((eq, open))) = tokio::join!(fetch_json(S3_TEL_URL), acct_snapshot()) {
                    let log = tel["lighterLog"].as_str().unwrap_or("");
                    let (seq, top_sc) = parse_scan_seq(log);
                    if seq > min_seq && (top_sc >= 7 || (min_seq == 0 && ticks >= 2) || ticks >= 225) {
                        println!("SCAN_READY Eq=${eq:.4} seq={seq} topScore={top_sc} Open={open:?}");
                        let matched: Vec<&str> = log
                            .lines()
                            .filter(|l| {
                                l.contains("[CARBON_SCAN")
                                    || l.contains("[STRAT_SCAN")
                                    || l.contains("[STRAT_BOARD")
                                    || l.contains("GO-TOKYO v")
                            })
                            .collect();
                        let start = matched.len().saturating_sub(6);
                        for l in &matched[start..] {
                            println!("{l}");
                        }
                        rc = 0;
                        break;
                    }
                }
                if ticks >= 225 {
                    rc = 1;
                    break;
                }
                tokio::time::sleep(Duration::from_secs(8)).await;
            }
            rc
        }
        Cmd::WaitClose { order_id } => {
            let mut saw_open = false;
            let mut loops = 0;
            let rc = 0;
            loop {
                loops += 1;
                if let (Ok(tel), Ok((eq, open))) = tokio::join!(fetch_json(S3_TEL_URL), acct_snapshot()) {
                    let log = tel["lighterLog"].as_str().unwrap_or("");
                    if !open.is_empty() {
                        saw_open = true;
                    }
                    let closed_tag = format!("[CARBON_CLOSED id={order_id}");
                    if log.contains(&closed_tag) || (saw_open && open.is_empty()) || loops >= 200 {
                        sync_journal(log);
                        println!("TRADE_DONE id={order_id} Eq=${eq:.4} Open={open:?}");
                        for l in log.lines() {
                            if l.contains(&format!("id={order_id}")) || l.contains("[CARBON_") || l.contains("[STRAT_") {
                                println!("{l}");
                            }
                        }
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            rc
        }
    };
    std::process::exit(code);
}
