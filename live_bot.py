# ─────────────────────────────────────────────
# BTC/ETH/SOL/XRP Prediction Market — 24/7 Live Trading Bot
# SIGNAL GENERATION OBFUSCATED VERSION
# ─────────────────────────────────────────────

import sys
print(sys.executable)

import os
import time
import base64
import json
import logging
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import requests
import duckdb
from dotenv import load_dotenv



# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
load_dotenv()

# Authentication
KEY_ID = 
PRIVATE_KEY_PATH = 
KALSHI_ENV = 
DB_PATH = 

# Discord Webhook (optional)
DISCORD_WEBHOOK_URL = 

# Kalshi API Base
API_URLS = {
    "prod": "https://api.elections.kalshi.com/trade-api/v2",
    "demo": "https://demo-api.kalshi.co/trade-api/v2"
}
API_BASE = API_URLS[KALSHI_ENV]

# Trading Parameters
TARGET_ASSETS = {"BTC", "ETH", "XRP", "SOL"} 
TRACKED_SERIES = ["KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M"]
POLL_INTERVAL_SEC = 1
ORDERBOOK_DEPTH = 10
CONTRACTS_PER_TRADE = 100 
DRY_RUN = True  # Paper trade


# Strategy Params - OBFUSCATED



# --- AUTHENTICATION HELPERS ---
def load_private_key(path: str):
    """Load RSA private key from PEM file."""
    if not path or not os.path.exists(path):
        logging.error(f"Private key file not found at: {path}")
        return None
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def make_headers(private_key, method: str, path_with_params: str) -> dict:
    """Kalshi V2 Signer."""
    path_only = path_with_params.split('?')[0]
    signing_path = f"/trade-api/v2{path_only}" if not path_only.startswith("/trade-api/v2") else path_only
    
    ts = str(int(time.time() * 1000))
    message = ts + method + signing_path
    
    sig = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256()
    )
    
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json"
    }

# --- DISCORD ALERTS ---
def send_discord_alert(message: str, color: int = 0x2196F3):
    """Sends a rich embed message to a Discord Webhook if configured."""
    if not DISCORD_WEBHOOK_URL:
        return
        
    payload = {
        "embeds": [{
            "description": message,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }]
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
    except Exception as e:
        logging.error(f"Failed to send Discord alert: {e}")

# --- KALSHI API CLIENT ---
class KalshiClient:
    def __init__(self, key_id, private_key):
        self.key_id = key_id
        self.private_key = private_key

    def get_active_markets(self, series_ticker: str):
        path = f"/markets?series_ticker={series_ticker}&status=open&limit=100"
        headers = make_headers(self.private_key, "GET", path)
        try:
            resp = requests.get(f"{API_BASE}{path}", headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json().get("markets", [])
        except Exception as e:
            logging.error(f"Market discovery failed for {series_ticker}: {e}")
            return []

    def get_market(self, ticker: str):
        path = f"/markets/{ticker}"
        headers = make_headers(self.private_key, "GET", path)
        try:
            resp = requests.get(f"{API_BASE}{path}", headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json().get("market", {})
        except Exception as e:
            logging.error(f"Market fetch failed for {ticker}: {e}")
            return None

    def fetch_orderbook(self, ticker: str):
        path = f"/markets/{ticker}/orderbook"
        headers = make_headers(self.private_key, "GET", path)
        try:
            resp = requests.get(f"{API_BASE}{path}", headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json().get("orderbook", {})
        except Exception as e:
            # Silence this error slightly as we poll frequently
            return None

    def place_order(self, ticker: str, side: str, count: int, price_cents: int, client_order_id: str):
        path = "/portfolio/orders"
        headers = make_headers(self.private_key, "POST", path)
        
        # Convert cents to fixed-point strings
        price_dollars = f"{(price_cents / 100):.4f}"
        
        payload = {
            "ticker": ticker,
            "side": side,  # "yes" or "no"
            "action": "buy",
            "client_order_id": client_order_id,
            "count": count,
            "time_in_force": "fill_or_kill", # Ensure we don't end up with resting orders
            # "buy_max_cost": price_cents * count # Optional
        }
        
        if side == "yes":
            payload["yes_price"] = price_cents
        elif side == "no":
            payload["no_price"] = price_cents
        
        try:
            resp = requests.post(f"{API_BASE}{path}", headers=headers, json=payload, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"Order failed: {e.response.text}")
            return None
        except Exception as e:
            logging.error(f"Order request error: {e}")
            return None

# --- DATABASE ENGINE ---
def init_db(db_path: str):
    """Create DuckDB with orderbook snapshot and trades tables."""
    con = duckdb.connect(db_path)

    # Orderbook snapshots with full depth (copied from ingest script)
    con.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id                  BIGINT PRIMARY KEY,
            fetched_at          TIMESTAMPTZ NOT NULL,
            market_ticker       VARCHAR     NOT NULL,
            asset               VARCHAR     NOT NULL,
            direction           VARCHAR     NOT NULL,
            yes_best_bid_dollars   DOUBLE,
            yes_best_bid_qty       DOUBLE,
            no_best_bid_dollars    DOUBLE,
            no_best_bid_qty        DOUBLE,
            mid_dollars         DOUBLE,
            spread_dollars      DOUBLE,
            yes_bids_json       JSON,
            no_bids_json        JSON
        )
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_orderbook_asset_time
        ON orderbook_snapshots (asset, fetched_at)
    """)

    # Trades log (New table specifically for the live bot)
    con.execute("""
        CREATE TABLE IF NOT EXISTS live_trades (
            id                  BIGINT PRIMARY KEY,
            traded_at           TIMESTAMPTZ NOT NULL,
            market_ticker       VARCHAR     NOT NULL,
            side                VARCHAR     NOT NULL,
            contracts           INTEGER     NOT NULL,
            limit_price_cents   INTEGER     NOT NULL,
            is_dry_run          BOOLEAN     NOT NULL,
            status              VARCHAR     NOT NULL,
            mid_at_entry        DOUBLE      NOT NULL,
            momentum_at_entry   DOUBLE      NOT NULL,
            client_order_id     VARCHAR     NOT NULL
        )
    """)
    
    return con

# --- STRATEGY STATE ---
class MarketState:
    def __init__(self, ticker, close_time_utc):
        self.ticker = ticker
        self.close_time_utc = close_time_utc
        self.history = [] # List of (timestamp, mid_price, yes_ask, no_ask)
        self.traded = False
        self.attempts = 0
        
        # Parse close time. Kalshi format e.g. "2024-03-01T15:00:00Z"
        try:
            self.close_dt = datetime.strptime(close_time_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            self.close_dt = None

    def update(self, orderbook):
        if not orderbook:
            return

        yes_bids = orderbook.get("yes", [])
        no_bids = orderbook.get("no", [])
        
        y_best = max(yes_bids, key=lambda x: x[0]) if yes_bids else None
        n_best = max(no_bids, key=lambda x: x[0]) if no_bids else None
        
        yes_bid_dollars = float(y_best[0])/100 if y_best else None
        no_bid_dollars = float(n_best[0])/100 if n_best else None
        
        # We need implied asks for entries
        yes_ask = (1 - no_bid_dollars) if no_bid_dollars is not None else None
        no_ask = (1 - yes_bid_dollars) if yes_bid_dollars is not None else None
        
        # Robust mid price calc (from backtest)
        two_sided = (yes_bid_dollars is not None) and (no_bid_dollars is not None)
        
        if two_sided:
            # Cross book check
            if yes_bid_dollars + no_bid_dollars > 1.0:
                logging.debug(f"{self.ticker} Orderbook crossed, skipping snapshot.")
                return 
            mid = (yes_bid_dollars + yes_ask) / 2
        elif yes_bid_dollars is not None:
             mid = yes_bid_dollars
        elif no_bid_dollars is not None:
             mid = 1 - no_bid_dollars
        else:
             return # No liquidity at all
            
        self.history.append((time.time(), mid, yes_ask, no_ask))
        
        # Keep only what we need for K_LAG
        if len(self.history) > K_LAG + 10:
            self.history.pop(0)

# --- ENGINE ---
def run_live(client: KalshiClient, db_con, dry_run: bool):
    active_markets = {} # ticker -> MarketState
    traded_chains = set() # Ensure 1 trade per chain ever
    pending_resolutions = {} # ticker -> trade info dict
    
    snapshot_id = int(time.time() * 1e6)
    trade_id = int(time.time() * 1e6)
    
    logging.info(f"Starting Live Bot. Target: {TARGET_ASSETS}. Dry Run: {dry_run}")
    
    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            
            # 1. Market Discovery (every iteration, to catch new ones early, limits permitting)
            # To save API calls, we could do this less frequently, but 15m chains move fast.
            # We poll each series
            current_tickers = set()
            for series in TRACKED_SERIES:
                markets = client.get_active_markets(series)
                for m in markets:
                    ticker = m.get("ticker")
                    if not ticker: continue
                    # Double check it belongs to a target asset (redundant but safe)
                    asset = ticker.split("15M")[0].replace("KX", "")
                    if asset not in TARGET_ASSETS: continue
                        
                    current_tickers.add(ticker)
                    
                    if ticker not in active_markets:
                        close_time = m.get("close_time", "")
                        active_markets[ticker] = MarketState(ticker, close_time)
                        # Also track chain id to prevent multiple entries if Kalshi uses multiple tickers per chain
                        # For UP/DOWN there are usually 2 tickers per chain (UP and DOWN), but Kalshi v2 treats the market itself as one ticker for binary options?
                        # Actually, in Kalshi v2, the `ticker` itself is the market.
                        logging.info(f"Tracking new market: {ticker}")

            # Cleanup expired markets
            to_remove = []
            for ticker, state in active_markets.items():
                if ticker not in current_tickers:
                    to_remove.append(ticker)
                elif state.close_dt:
                    if (state.close_dt - now_dt).total_seconds() < 0:
                        to_remove.append(ticker)
            for ticker in to_remove:
                del active_markets[ticker]
                logging.info(f"Removed expired market: {ticker}")
            
            # 2. Polling and Signals
            for ticker, state in active_markets.items():
                if state.traded or ticker in traded_chains:
                    continue # Already traded this chain
                    
                # Hourly Block Filter (Weekdays only)
                is_weekday = now_dt.weekday() <= 4 # 0=Mon, ..., 4=Fri
                if is_weekday and now_dt.hour in BLOCKED_HOURS:
                    # Skip signal processing during blocked hours
                    continue
                    
                # Time filters
                if state.close_dt:
                    time_to_close = (state.close_dt - now_dt).total_seconds()
                    chain_length = 15 * 60 # Assume 15 min chains
                    time_elapsed = chain_length - time_to_close
                    pct_through = time_elapsed / chain_length
                    
                    if pct_through < EXCLUSION_START_PCT:
                        continue # Too early in the chain
                        
                    if time_to_close < EXCLUSION_END_SEC:
                        continue # Too close to expiry
                
                # Fetch Book
                book = client.fetch_orderbook(ticker)
                if not book: continue
                
                state.update(book)
                
                # Format book to be saved to db
                asset = ticker.split("15M")[0].replace("KX", "")
                direction = "UP" if ticker.endswith("-00") else "DOWN" if ticker.endswith("-01") else "STRIKE-"+ticker.split('-')[-1]
                
                # Get best bids from book dictionary directly
                yes_bids = book.get("yes", [])
                no_bids = book.get("no", [])
                y_best = max(yes_bids, key=lambda x: x[0]) if yes_bids else None
                n_best = max(no_bids, key=lambda x: x[0]) if no_bids else None
                
                mid = state.history[-1][1] if state.history else None
                yes_bid_dollars = float(y_best[0])/100 if y_best else None
                no_bid_dollars = float(n_best[0])/100 if n_best else None
                spread = None
                if yes_bid_dollars is not None and no_bid_dollars is not None:
                     spread = 1.0 - yes_bid_dollars - no_bid_dollars
                
                snapshot_id += 1
                try:
                    db_con.execute("""
                        INSERT INTO orderbook_snapshots VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                    """, [
                        snapshot_id,
                        now_dt,
                        ticker,
                        asset,
                        direction,
                        yes_bid_dollars,
                        float(y_best[1]) if y_best else None,
                        no_bid_dollars,
                        float(n_best[1]) if n_best else None,
                        mid,
                        spread,
                        json.dumps(yes_bids),
                        json.dumps(no_bids),
                    ])
                except Exception as e:
                    logging.error(f"Failed to log orderbook to DB: {e}")
                
                # Check Signal
                if len(state.history) > K_LAG:
                     current_mid = state.history[-1][1]
                     yes_ask = state.history[-1][2]
                     no_ask = state.history[-1][3]
                     
                     lag_mid = state.history[-(K_LAG + 1)][1]
                     
                     momentum = current_mid - lag_mid
                     
                     signal = 0
                     entry_price_dollars = None
                     side = None
                     
                     # YES Signal
                     if (YES_ENTRY_ZONE[0] <= current_mid <= YES_ENTRY_ZONE[1]) and (momentum > MOMENTUM_THRESHOLD):
                         signal = 1
                         side = "yes"
                         entry_price_dollars = yes_ask if yes_ask is not None else (1 - current_mid) # Fallback
                     
                     # NO Signal
                     elif (NO_ENTRY_ZONE[0] <= current_mid <= NO_ENTRY_ZONE[1]) and (momentum < -MOMENTUM_THRESHOLD):
                         signal = -1
                         side = "no"
                         entry_price_dollars = no_ask if no_ask is not None else current_mid # Fallback
                         
                     # Execute
                     if signal != 0 and entry_price_dollars:
                         base_price_cents = int(round(entry_price_dollars * 100))
                         # Add 1 cent slippage to increase FOK fill probability
                         price_cents = min(99, max(1, base_price_cents + 1))
                         client_oid = f"{ticker}-{int(time.time())}"
                         
                         logging.info(f"🔥 SIGNAL: {ticker} | Side: {side.upper()} | Mid: {current_mid:.3f} | Mom: {momentum:.3f} | Ask: {base_price_cents}¢ (Limit: {price_cents}¢)")
                         
                         alert_msg = f"**🔥 SIGNAL DETECTED**\nMarket: `{ticker}`\nSide: **{side.upper()}**\nContracts: {CONTRACTS_PER_TRADE}\nLimit Price: {price_cents}¢\nMid: {current_mid:.3f} | Momentum: {momentum:.3f}"
                         
                         if not dry_run:
                             state.attempts += 1
                             logging.info(f"Placing Fill-or-Kill Order for {CONTRACTS_PER_TRADE} contracts. Attempt {state.attempts}/3")
                             order_resp = client.place_order(ticker, side, CONTRACTS_PER_TRADE, price_cents, client_oid)
                             
                             if order_resp:
                                 logging.info(f"Order Success: {order_resp}")
                                 state.traded = True
                                 traded_chains.add(ticker)
                                 # Send discord msg for actual trade
                                 send_discord_alert(alert_msg + "\n\n✅ order placed (live)", color=0x4CAF50)
                                 
                                 trade_id += 1
                                 db_con.execute("""
                                     INSERT INTO live_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                 """, [trade_id, now_dt, ticker, side, CONTRACTS_PER_TRADE, price_cents, dry_run, "success", current_mid, momentum, client_oid])
                                 
                                 pending_resolutions[ticker] = {
                                     "side": side, 
                                     "price_cents": price_cents, 
                                     "contracts": CONTRACTS_PER_TRADE, 
                                     "dry_run": dry_run,
                                     "close_dt": state.close_dt,
                                     "last_check": 0
                                 }
                                 
                             else:
                                 logging.warning("Order failed, will try next polling cycle if signal persists.")
                                 send_discord_alert(alert_msg + "\n\n❌ order failed", color=0xF44336)
                                 
                                 trade_id += 1
                                 db_con.execute("""
                                     INSERT INTO live_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                 """, [trade_id, now_dt, ticker, side, CONTRACTS_PER_TRADE, price_cents, dry_run, "failed", current_mid, momentum, client_oid])
                                 
                                 if state.attempts >= 3:
                                     logging.warning(f"Max attempts reached for {ticker}, will stop trying.")
                                     state.traded = True
                                     traded_chains.add(ticker)
                                 
                         else:
                             logging.info("DRY RUN: Order skipped.")
                             state.traded = True
                             traded_chains.add(ticker)
                             # Send discord msg for paper trade
                             send_discord_alert(alert_msg + "\n\n📝 paper trade (dry run)", color=0xFF9800)
                             
                             trade_id += 1
                             db_con.execute("""
                                 INSERT INTO live_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                             """, [trade_id, now_dt, ticker, side, CONTRACTS_PER_TRADE, price_cents, dry_run, "paper_trade", current_mid, momentum, client_oid])
                             
                             pending_resolutions[ticker] = {
                                 "side": side, 
                                 "price_cents": price_cents, 
                                 "contracts": CONTRACTS_PER_TRADE, 
                                 "dry_run": dry_run,
                                 "close_dt": state.close_dt,
                                 "last_check": 0
                             }
                             
            # 3. Check for settlements
            resolved_tickers = []
            for ticker, trade_info in pending_resolutions.items():
                close_dt = trade_info["close_dt"]
                # Only check after market close
                if close_dt and now_dt >= close_dt:
                    # Limit checks to once every 30s
                    if time.time() - trade_info.get("last_check", 0) > 30:
                        trade_info["last_check"] = time.time()
                        try:
                            market = client.get_market(ticker)
                            if market and market.get("status") in ["determined", "settled"]:
                                result = market.get("result", "")
                                
                                if result == trade_info["side"]:
                                    outcome_msg = "🏆 **WIN**"
                                    profit = (100 - trade_info["price_cents"]) * trade_info["contracts"]
                                    color = 0x4CAF50
                                elif result == "":
                                    continue
                                else:
                                    outcome_msg = "💀 **LOSS**"
                                    profit = -trade_info["price_cents"] * trade_info["contracts"]
                                    color = 0xF44336
                                    
                                mode_str = "[PAPER TRADE]" if trade_info["dry_run"] else "[LIVE TRADE]"
                                profit_dollars = profit / 100
                                
                                alert_msg = f"{outcome_msg}\nMarket: `{ticker}`\nMode: {mode_str}\nContracts: {trade_info['contracts']}\nEntry: {trade_info['price_cents']}¢\nResult: **{result.upper()}**\nP/L: **${profit_dollars:+.2f}**"
                                
                                logging.info(f"Resolution for {ticker}: {result.upper()} | P/L: ${profit_dollars:+.2f}")
                                send_discord_alert(alert_msg, color=color)
                                
                                resolved_tickers.append(ticker)
                        except Exception as e:
                            logging.error(f"Error checking settlement for {ticker}: {e}")
                            
            for ticker in resolved_tickers:
                del pending_resolutions[ticker]
                             
            time.sleep(POLL_INTERVAL_SEC)
            
        except KeyboardInterrupt:
            logging.info("Stopping Bot.")
            break
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Kalshi Live Bot')
    parser.add_argument('--live', action='store_true', help='Execute real trades (disables dry_run)')
    parser.add_argument('--contracts', type=int, default=5, help='Number of contracts to buy per trade')
    args = parser.parse_args()
    
    CONTRACTS_PER_TRADE = args.contracts
    is_dry_run = not args.live
    
    if not KEY_ID or not PRIVATE_KEY_PATH:
        logging.error("Missing private key environment variables.")
        exit(1)
        
    pk = load_private_key(PRIVATE_KEY_PATH)
    if not pk:
        exit(1)
        
    logging.info(f"Initializing DuckDB at {DB_PATH}...")
    db_con = init_db(DB_PATH)
        
    client = KalshiClient(KEY_ID, pk)
    run_live(client, db_con, is_dry_run)