# ─────────────────────────────────────────────
# Kalshi + Binance Signal Collector
# OBFUSCATED VERSION
#
# Runs both data collectors in a single async process with one shared DuckDB.
# Tables:
#   - price_ticks          : Binance websocket feed (WebSocket, ~50ms)
#   - orderbook_snapshots  : Kalshi orderbook polls  (REST, ~1s)
# ─────────────────────────────────────────────

import asyncio
import base64
import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import duckdb
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

# ── Environment ───────────────────────────────────────────────────────────────
load_dotenv()

# Shared DB — one file for everything
DB_PATH = 

# ── Kalshi Config ─────────────────────────────────────────────────────────────
API_URLS = {
    "prod": "https://api.elections.kalshi.com/trade-api/v2",
    "demo": "https://demo-api.kalshi.co/trade-api/v2",
}
KALSHI_ENV        = 
KEY_ID            = 
PRIVATE_KEY_PATH  = 
API_BASE          = 
TRACKED_SERIES    = ["KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M"]
POLL_INTERVAL_SEC = 1
DISCOVERY_TTL_SEC = 60
MAX_CONCURRENT    = 20

# ── Binance Config ────────────────────────────────────────────────────────────
SYMBOLS         = ["btcusdt", "ethusdt", "solusdt", "xrpusdt"]
BINANCE_WS_BASE = "wss://stream.binance.us:9443/stream"   # Binance.US (US users)
RECONNECT_DELAY = 5
FLUSH_INTERVAL  = 100
STREAM_NAMES    = "/".join(f"{s}@bookTicker" for s in SYMBOLS)
WS_URL          = f"{BINANCE_WS_BASE}?streams={STREAM_NAMES}"

# ── Startup Banner ────────────────────────────────────────────────────────────
print("=" * 60)
print("  Kalshi + Binance Signal Collector")
print("=" * 60)
print(f"  Shared DB     : {DB_PATH}")
print(f"  Kalshi env    : {KALSHI_ENV}  |  Key set: {'YES' if KEY_ID else 'NO'}")
print(f"  Binance feed  : {WS_URL}")
print("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def init_db(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)

    # Binance price ticks
    con.execute("""
        CREATE TABLE IF NOT EXISTS price_ticks (
            id            BIGINT PRIMARY KEY,
            received_at   TIMESTAMPTZ NOT NULL,
            event_time_ms BIGINT      NOT NULL,
            symbol        VARCHAR     NOT NULL,
            asset         VARCHAR     NOT NULL,
            best_bid      DOUBLE      NOT NULL,
            best_bid_qty  DOUBLE      NOT NULL,
            best_ask      DOUBLE      NOT NULL,
            best_ask_qty  DOUBLE      NOT NULL,
            mid           DOUBLE      NOT NULL,
            spread        DOUBLE      NOT NULL
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_price_asset_time
        ON price_ticks (asset, received_at)
    """)

    # Kalshi orderbook snapshots
    con.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id                   BIGINT PRIMARY KEY,
            fetched_at           TIMESTAMPTZ NOT NULL,
            market_ticker        VARCHAR     NOT NULL,
            asset                VARCHAR     NOT NULL,
            direction            VARCHAR     NOT NULL,
            yes_best_bid_dollars DOUBLE,
            yes_best_bid_qty     DOUBLE,
            no_best_bid_dollars  DOUBLE,
            no_best_bid_qty      DOUBLE,
            mid_dollars          DOUBLE,
            spread_dollars       DOUBLE,
            yes_bids_json        JSON,
            no_bids_json         JSON
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_orderbook_asset_time
        ON orderbook_snapshots (asset, fetched_at)
    """)

    print(f"[DB] Shared database ready: {db_path}")
    return con


# ══════════════════════════════════════════════════════════════════════════════
# KALSHI AUTH
# ══════════════════════════════════════════════════════════════════════════════
def load_private_key(path: str):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def make_headers(private_key, method: str, path_only: str) -> dict:
    if not private_key:
        return {"Content-Type": "application/json"}

    signing_path = (
        f"/trade-api/v2{path_only}"
        if not path_only.startswith("/trade-api/v2")
        else path_only
    )
    ts      = str(int(time.time() * 1000))
    message = (ts + method + signing_path).encode("utf-8")
    sig     = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type":            "application/json",
    }


# ══════════════════════════════════════════════════════════════════════════════
# KALSHI COLLECTOR
# ══════════════════════════════════════════════════════════════════════════════
def parse_kalshi_ticker(ticker: str) -> tuple[str, str]:
    for series in TRACKED_SERIES:
        if ticker.startswith(series):
            asset  = series.replace("KX", "").replace("15M", "")
            suffix = ticker.split("-")[-1]
            direction = (
                "UP"   if suffix == "00" else
                "DOWN" if suffix == "01" else
                f"STRIKE-{suffix}"
            )
            return asset, direction
    return "UNKNOWN", "NONE"


def parse_kalshi_snapshot(ticker: str, orderbook: dict) -> dict:
    asset, direction = parse_kalshi_ticker(ticker)
    yes_bids = orderbook.get("yes", [])
    no_bids  = orderbook.get("no",  [])
    y_best   = max(yes_bids, key=lambda x: x[0]) if yes_bids else [None, None]
    n_best   = max(no_bids,  key=lambda x: x[0]) if no_bids  else [None, None]
    mid      = (
        (float(y_best[0]) + (100 - float(n_best[0]))) / 2
        if y_best[0] and n_best[0] else None
    )
    return {
        "market_ticker":        ticker,
        "asset":                asset,
        "direction":            direction,
        "yes_best_bid_dollars": float(y_best[0]) / 100 if y_best[0] else None,
        "yes_best_bid_qty":     float(y_best[1])       if y_best[1] else None,
        "no_best_bid_dollars":  float(n_best[0]) / 100 if n_best[0] else None,
        "no_best_bid_qty":      float(n_best[1])       if n_best[1] else None,
        "mid_dollars":          mid / 100              if mid        else None,
        "spread_dollars":       (100 - float(y_best[0]) - float(n_best[0])) / 100
                                if y_best[0] and n_best[0] else None,
        "yes_bids_json":        json.dumps(yes_bids),
        "no_bids_json":         json.dumps(no_bids),
    }


async def get_active_markets(session: aiohttp.ClientSession, private_key, series: str) -> list:
    path    = f"/markets?series_ticker={series}&status=open&limit=100"
    headers = make_headers(private_key, "GET", "/markets")
    try:
        async with session.get(
            f"{API_BASE}{path}", headers=headers,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            resp.raise_for_status()
            return (await resp.json()).get("markets", [])
    except Exception as e:
        print(f"[KALSHI][ERROR] Discovery failed for {series}: {e}")
        return []


async def discover_markets(session: aiohttp.ClientSession, private_key) -> list:
    results = await asyncio.gather(
        *[get_active_markets(session, private_key, s) for s in TRACKED_SERIES]
    )
    return [m["ticker"] for res in results if isinstance(res, list) for m in res if m.get("ticker")]


async def fetch_orderbook(
    session: aiohttp.ClientSession,
    private_key,
    ticker: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, dict | None]:
    path    = f"/markets/{ticker}/orderbook"
    headers = make_headers(private_key, "GET", path)
    async with semaphore:
        try:
            async with session.get(
                f"{API_BASE}{path}", headers=headers,
                timeout=aiohttp.ClientTimeout(total=2),
            ) as resp:
                resp.raise_for_status()
                return ticker, (await resp.json()).get("orderbook", {})
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                print(f"[KALSHI][RATE LIMIT] {ticker} — backing off 2s")
                await asyncio.sleep(2)
            else:
                print(f"[KALSHI][HTTP {e.status}] {ticker}")
        except Exception as e:
            print(f"[KALSHI][ERROR] {ticker}: {e}")
    return ticker, None


async def kalshi_collector(con: duckdb.DuckDBPyConnection, private_key, stop_event: asyncio.Event):
    semaphore      = asyncio.Semaphore(MAX_CONCURRENT)
    active_tickers = []
    last_discovery = 0.0
    snap_id        = int(time.time() * 1e6) * 10  # offset so IDs don't clash with Binance

    print("[KALSHI] Collector started")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while not stop_event.is_set():
            loop_start = time.monotonic()

            if loop_start - last_discovery > DISCOVERY_TTL_SEC:
                active_tickers = await discover_markets(session, private_key)
                last_discovery = loop_start
                print(f"[KALSHI] {len(active_tickers)} active markets: {active_tickers}")

            if not active_tickers:
                print("[KALSHI][WARN] No active markets, retrying in 10s...")
                await asyncio.sleep(10)
                continue

            fetched_at = datetime.now(timezone.utc)
            results    = await asyncio.gather(*[
                fetch_orderbook(session, private_key, t, semaphore)
                for t in active_tickers
            ])

            rows = []
            for ticker, orderbook in results:
                if orderbook is None:
                    continue
                snap = parse_kalshi_snapshot(ticker, orderbook)
                snap_id += 1
                rows.append([
                    snap_id, fetched_at,
                    snap["market_ticker"], snap["asset"], snap["direction"],
                    snap["yes_best_bid_dollars"], snap["yes_best_bid_qty"],
                    snap["no_best_bid_dollars"],  snap["no_best_bid_qty"],
                    snap["mid_dollars"], snap["spread_dollars"],
                    snap["yes_bids_json"], snap["no_bids_json"],
                ])
                mid = snap["mid_dollars"]
                if mid:
                    print(f"[KALSHI] {snap['asset']:>3} {snap['direction']:>12} | mid=${mid:.4f}  spread=${snap['spread_dollars']:.4f}")

            if rows:
                con.executemany(
                    "INSERT INTO orderbook_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )

            elapsed   = time.monotonic() - loop_start
            sleep_for = max(0.0, POLL_INTERVAL_SEC - elapsed)
            print(f"[KALSHI] Round: {len(rows)} snapshots in {elapsed*1000:.0f}ms")
            if sleep_for:
                await asyncio.sleep(sleep_for)

    print("[KALSHI] Collector stopped.")


# ══════════════════════════════════════════════════════════════════════════════
# BINANCE COLLECTOR
# ══════════════════════════════════════════════════════════════════════════════
def parse_book_ticker(msg: dict, received_at: datetime) -> dict | None:
    data = msg.get("data", msg)
    if data.get("e") and data["e"] != "bookTicker":
        return None
    try:
        bid    = float(data["b"])
        ask    = float(data["a"])
        symbol = data["s"].upper()
        return {
            "received_at":   received_at,
            "event_time_ms": int(data.get("E", data.get("T", 0))),
            "symbol":        symbol,
            "asset":         symbol.replace("USDT", ""),
            "best_bid":      bid,
            "best_bid_qty":  float(data["B"]),
            "best_ask":      ask,
            "best_ask_qty":  float(data["A"]),
            "mid":           (bid + ask) / 2,
            "spread":        ask - bid,
        }
    except (KeyError, ValueError) as e:
        print(f"[BINANCE][PARSE ERROR] {e}")
        return None


async def binance_collector(con: duckdb.DuckDBPyConnection, stop_event: asyncio.Event):
    tick_id = int(time.time() * 1e6)
    buffer  = []
    total   = 0

    print("[BINANCE] Collector started, connecting...")

    while not stop_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_URL, heartbeat=20, receive_timeout=30
                ) as ws:
                    print(f"[BINANCE] Connected — streaming {len(SYMBOLS)} symbols")

                    async for msg in ws:
                        if stop_event.is_set():
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            received_at = datetime.now(timezone.utc)
                            tick        = parse_book_ticker(json.loads(msg.data), received_at)
                            if tick is None:
                                continue

                            tick_id += 1
                            buffer.append([
                                tick_id, tick["received_at"], tick["event_time_ms"],
                                tick["symbol"], tick["asset"],
                                tick["best_bid"], tick["best_bid_qty"],
                                tick["best_ask"], tick["best_ask_qty"],
                                tick["mid"], tick["spread"],
                            ])

                            if len(buffer) >= FLUSH_INTERVAL:
                                con.executemany(
                                    "INSERT INTO price_ticks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                    buffer,
                                )
                                total += len(buffer)
                                print(f"[BINANCE] Flushed {len(buffer)} ticks → {total:,} total | "
                                      f"{tick['asset']} mid=${tick['mid']:,.4f}")
                                buffer.clear()

                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            print(f"[BINANCE] WS closed/error — reconnecting")
                            break

        except aiohttp.ClientConnectorError as e:
            print(f"[BINANCE][CONNECT ERROR] {e}")
        except asyncio.TimeoutError:
            print("[BINANCE][TIMEOUT] No message in 30s — reconnecting")
        except Exception as e:
            print(f"[BINANCE][ERROR] {e}")

        if not stop_event.is_set():
            if buffer:
                con.executemany(
                    "INSERT INTO price_ticks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    buffer,
                )
                total += len(buffer)
                buffer.clear()
            print(f"[BINANCE] Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

    # Final flush
    if buffer:
        con.executemany(
            "INSERT INTO price_ticks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            buffer,
        )
    print(f"[BINANCE] Collector stopped. Total ticks: {total:,}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    if not KEY_ID or not Path(PRIVATE_KEY_PATH).exists():
        print("[WARN] Kalshi KEY_ID or PEM file missing — Kalshi auth will fail.")

    private_key = load_private_key(PRIVATE_KEY_PATH) if Path(PRIVATE_KEY_PATH).exists() else None
    con         = init_db(DB_PATH)
    stop_event  = asyncio.Event()

    def handle_stop():
        print("\n[SIGNAL] Shutdown — finishing current round...")
        stop_event.set()

    # Windows doesn't support add_signal_handler; use KeyboardInterrupt fallback
    if os.name != "nt":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_stop)

    try:
        # Both collectors run concurrently, sharing one DB connection
        await asyncio.gather(
            kalshi_collector(con, private_key, stop_event),
            binance_collector(con, stop_event),
        )
    except KeyboardInterrupt:
        handle_stop()
        await asyncio.sleep(1)
    finally:
        con.close()
        print("[DB] Connection closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted.")