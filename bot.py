import requests
import time
from datetime import datetime, timedelta
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Function to get IST datetime now
def now_ist():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

STRATEGIES = [
    {'name': 'Strategy_1', 'entry_diff': 0.30,  'exit_diff': 0.0},
    {'name': 'Strategy_2', 'entry_diff': 0.20,  'exit_diff': -0.20},
    {'name': 'Strategy_3', 'entry_diff': 0.20,  'exit_diff': -0.25},
    {'name': 'Strategy_4', 'entry_diff': 0.12,  'exit_diff': -0.24},
    {'name': 'Strategy_5', 'entry_diff': 0.25,  'exit_diff': -0.25, 'safety_timeout': 60 * 60}
]

MAX_BATCH_SIZE = 50
MAX_CALLS_PER_SECOND = 18
CALL_DELAY = 1.0 / MAX_CALLS_PER_SECOND

def upload_to_gdrive(local_file_path):
    try:
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile("mycreds.txt")
        if gauth.credentials is None:
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()
        gauth.SaveCredentialsFile("mycreds.txt")

        drive = GoogleDrive(gauth)

        file_name = local_file_path.split("/")[-1]
        file_list = drive.ListFile({'q': f"title='{file_name}' and trashed=false"}).GetList()

        if file_list:
            file = file_list[0]
            file.SetContentFile(local_file_path)
            file.Upload()
            print(f"Updated existing file on Google Drive: {file_name}")
        else:
            file = drive.CreateFile({'title': file_name})
            file.SetContentFile(local_file_path)
            file.Upload()
            print(f"Uploaded new file to Google Drive: {file_name}")

    except Exception as e:
        print(f"Error during Google Drive upload: {e}")

class ArbitrageBotStrategy:
    def __init__(self, entry_diff, exit_diff, strategy_name, safety_timeout=None):
        self.entry_diff = entry_diff
        self.exit_diff = exit_diff
        self.strategy_name = strategy_name
        self.positions = {}
        self.trade_log = []
        self.safety_timeout = safety_timeout or 30 * 60

class ArbitrageBot:
    def __init__(self):
        self.strategies = [ArbitrageBotStrategy(s.get('entry_diff'), s.get('exit_diff'), s.get('name'), s.get('safety_timeout')) for s in STRATEGIES]
        self.candidate_coins = []
        self.CANDIDATE_TRACK_TIME = 2 * 60  # seconds
        self.candidate_track_start = None
        self.last_full_scan_time = None
        self.SPOT_FEE_RATE = 0.001
        self.FUTURES_FEE_RATE = 0.0004
        self.TRADE_AMOUNT_USD = 100
        self.MIN_SCAN_DIFF_PERCENT = 0.10
        self.stop_event = threading.Event()

        self.spot_symbols = []
        self.futures_symbols = []

        self.spot_prices = {}
        self.futures_prices = {}

    def fetch_symbols(self):
        spot_info = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=5).json()
        futures_info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=5).json()
        self.spot_symbols = [s["symbol"] for s in spot_info["symbols"]
                             if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]
        self.futures_symbols = [s["symbol"] for s in futures_info["symbols"]
                               if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]

    def fetch_batch_prices(self, url, symbol_list, price_dict):
        params = {"symbols": f'["{"\",\"".join(symbol_list)}"]'}
        try:
            resp = requests.get(url, params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            for entry in data:
                sym = entry["symbol"]
                price_dict[sym] = {"bid": float(entry["bidPrice"]), "ask": float(entry["askPrice"])}
        except Exception as e:
            print(f"[{now_ist()}] Warning: Failed to fetch batch prices from {url} symbols count {len(symbol_list)}: {e}")

    def fetch_prices(self):
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        spot_url = "https://api.binance.com/api/v3/ticker/bookTicker"
        futures_url = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"

        spot_batches = list(chunks(self.spot_symbols, MAX_BATCH_SIZE))
        futures_batches = list(chunks(self.futures_symbols, MAX_BATCH_SIZE))

        with ThreadPoolExecutor(max_workers=10) as executor:
            for batch in spot_batches:
                executor.submit(self.fetch_batch_prices, spot_url, batch, self.spot_prices)
                time.sleep(CALL_DELAY)
            for batch in futures_batches:
                executor.submit(self.fetch_batch_prices, futures_url, batch, self.futures_prices)
                time.sleep(CALL_DELAY)

    def full_market_scan(self):
        now = now_ist()
        print(f"\n{now} - Running full market scan on live prices...")
        candidates = []
        for sym in self.futures_symbols:
            sp = self.spot_prices.get(sym)
            fp = self.futures_prices.get(sym)
            if not sp or not fp:
                continue
            diff_percent = (fp["bid"] - sp["ask"]) / sp["ask"] * 100
            if diff_percent >= self.MIN_SCAN_DIFF_PERCENT:
                candidates.append({
                    "symbol": sym,
                    "spot_price": sp["ask"],
                    "futures_price": fp["bid"],
                    "diff": diff_percent
                })
        if candidates:
            print(f"Candidates with futures premium ≥ {self.MIN_SCAN_DIFF_PERCENT}%:")
            for c in sorted(candidates, key=lambda x: x["diff"], reverse=True):
                print(f"  {c['symbol']} | Spot ask: {c['spot_price']:.5f}, Futures bid: {c['futures_price']:.5f}, Diff%: {c['diff']:.4f}")
            self.candidate_coins = candidates
            self.candidate_track_start = now
            self.last_full_scan_time = now
        else:
            print("No arbitrage candidates found.")
            self.candidate_coins = []
            # Set candidate tracking time but don't update last scan time to allow immediate rescans
            self.candidate_track_start = now

    def open_trade(self, strategy, coin):
        sym = coin["symbol"]
        spot_price = coin["spot_price"]
        fut_price = coin["futures_price"]
        ts = now_ist()
        spot_qty = self.TRADE_AMOUNT_USD / spot_price
        fut_qty = self.TRADE_AMOUNT_USD / fut_price
        strategy.positions[sym] = {
            "entry_time": ts,
            "entry_spot_price": spot_price,
            "entry_futures_price": fut_price,
            "spot_qty": spot_qty,
            "futures_qty": fut_qty
        }
        print(f"\n[{strategy.strategy_name}] {ts} | Opened trade on {sym}")
        print(f"  Spot buy @ {spot_price:.5f} (ask) qty: {spot_qty:.4f}")
        print(f"  Futures short @ {fut_price:.5f} (bid) qty: {fut_qty:.4f}")
        self.log_trade(strategy, sym, "ENTRY", ts, spot_price, fut_price, spot_qty, fut_qty, 0, 0, 0)

    def close_trade(self, strategy, sym):
        pos = strategy.positions[sym]
        ts = now_ist()
        sp_current = self.spot_prices.get(sym, {}).get("bid", pos["entry_spot_price"])
        fp_current = self.futures_prices.get(sym, {}).get("ask", pos["entry_futures_price"])
        spot_qty = pos["spot_qty"]
        fut_qty = pos["futures_qty"]
        spot_entry = pos["entry_spot_price"]
        fut_entry = pos["entry_futures_price"]
        spot_cost = spot_entry * spot_qty
        fut_cost = fut_entry * fut_qty
        spot_exit_val = sp_current * spot_qty
        fut_exit_val = fp_current * fut_qty
        spot_fee = (spot_cost + spot_exit_val) * self.SPOT_FEE_RATE
        fut_fee = (fut_cost + fut_exit_val) * self.FUTURES_FEE_RATE
        pnl_spot = spot_exit_val - spot_cost
        pnl_fut = fut_cost - fut_exit_val
        gross_pnl = pnl_spot + pnl_fut
        total_fees = spot_fee + fut_fee
        net_pnl = gross_pnl - total_fees
        print(f"\n[{strategy.strategy_name}] {ts} | Closed trade on {sym}")
        print(f"  Spot sell @ {sp_current:.5f} (bid), Futures cover @ {fp_current:.5f} (ask)")
        print(f"  Gross P&L: Spot: {pnl_spot:.4f} + Futures: {pnl_fut:.4f} = Total: {gross_pnl:.4f}")
        print(f"  Fees: Spot: {spot_fee:.4f}, Futures: {fut_fee:.4f}")
        print(f"  Net P&L after fees: {net_pnl:.4f}")
        self.log_trade(strategy, sym, "EXIT", ts, sp_current, fp_current, spot_qty, fut_qty, total_fees, total_fees, net_pnl,
                       entry_time=pos["entry_time"],
                       entry_spot_price=spot_entry,
                       entry_futures_price=fut_entry,
                       gross_pnl=gross_pnl,
                       spot_fee=spot_fee,
                       futures_fee=fut_fee)
        del strategy.positions[sym]
        self.save_logs(strategy)

    def log_trade(self, strategy, sym, action, ts, sp, fp, sp_qty, fp_qty, sp_fee, fp_fee, pnl,
                  entry_time=None, entry_spot_price=None, entry_futures_price=None, gross_pnl=None, spot_fee=None,
                  futures_fee=None):
        log_entry = {
            "symbol": sym,
            "action": action,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "spot_price": sp,
            "futures_price": fp,
            "spot_qty": sp_qty,
            "futures_qty": fp_qty,
            "spot_fee": sp_fee,
            "futures_fee": fp_fee,
            "pnl": pnl
        }
        if action == "EXIT":
            log_entry.update({
                "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S") if entry_time else "",
                "entry_spot_price": entry_spot_price,
                "entry_futures_price": entry_futures_price,
                "gross_pnl": gross_pnl,
                "spot_fee_detail": spot_fee,
                "futures_fee_detail": futures_fee,
                "net_pnl": pnl
            })
        else:
            log_entry.update({
                "entry_time": "",
                "entry_spot_price": None,
                "entry_futures_price": None,
                "gross_pnl": None,
                "spot_fee_detail": None,
                "futures_fee_detail": None,
                "net_pnl": None
            })
        strategy.trade_log.append(log_entry)

    def save_logs(self, strategy):
        df = pd.DataFrame(strategy.trade_log)
        column_order = ["symbol", "action", "entry_time", "timestamp", "entry_spot_price", "entry_futures_price",
                        "spot_price", "futures_price", "spot_qty", "futures_qty",
                        "gross_pnl", "spot_fee_detail", "futures_fee_detail", "spot_fee", "futures_fee", "pnl", "net_pnl"]
        df = df.reindex(columns=column_order)
        excel_name = f"trade_log_{strategy.strategy_name}.xlsx"
        csv_name = f"trade_log_{strategy.strategy_name}_backup.csv"
        df.to_excel(excel_name, index=False)
        df.to_csv(csv_name, index=False)
        print(f"[{strategy.strategy_name}] Trade logs saved.")
        upload_to_gdrive(excel_name)
        upload_to_gdrive(csv_name)

    def monitor_loop(self):
        premium_print_timer = now_ist()
        while not self.stop_event.is_set():
            now = now_ist()
            self.fetch_prices()

            if (now - premium_print_timer).total_seconds() >= 2:
                print(f"\n[{now}] Candidate premium differences:")
                for c in self.candidate_coins:
                    sp = self.spot_prices.get(c['symbol'])
                    fp = self.futures_prices.get(c['symbol'])
                    if sp and fp:
                        diff = (fp['bid'] - sp['ask']) / sp['ask'] * 100
                        print(f"  {c['symbol']} premium diff: {diff:.4f}%")
                premium_print_timer = now

            for strategy in self.strategies:
                if strategy.positions:
                    for sym in list(strategy.positions.keys()):
                        sp = self.spot_prices.get(sym)
                        fp = self.futures_prices.get(sym)
                        diff_pct = None
                        if sp and fp:
                            diff_pct = (fp["bid"] - sp["ask"]) / sp["ask"] * 100
                            entry_time = strategy.positions[sym]['entry_time']
                            if diff_pct is not None and diff_pct <= strategy.exit_diff:
                                print(f"[{strategy.strategy_name}] {now} | Premium ≤ {strategy.exit_diff}% on {sym}, exiting trade.")
                                self.close_trade(strategy, sym)
                            elif (now - entry_time).total_seconds() > strategy.safety_timeout:
                                print(f"[{strategy.strategy_name}] {now} | Safety timeout reached on {sym}, exiting trade.")
                                self.close_trade(strategy, sym)
                else:
                    if not self.candidate_coins:
                        # Full scan immediately if empty
                        self.full_market_scan()
                        continue
                    else:
                        elapsed = (now - self.candidate_track_start).total_seconds() if self.candidate_track_start else 0
                        # Enforce cooldown between full scans only if candidates exist
                        if self.last_full_scan_time:
                            diff_since_last_scan = (now - self.last_full_scan_time).total_seconds()
                        else:
                            diff_since_last_scan = None
                        if elapsed > self.CANDIDATE_TRACK_TIME and (diff_since_last_scan is None or diff_since_last_scan >= self.CANDIDATE_TRACK_TIME):
                            print(f"{now} Candidate tracking expired, rescanning full market")
                            self.full_market_scan()
                            continue
                        for c in self.candidate_coins:
                            sym = c["symbol"]
                            sp = self.spot_prices.get(sym)
                            fp = self.futures_prices.get(sym)
                            if not sp or not fp:
                                continue
                            diff_pct = (fp["bid"] - sp["ask"]) / sp["ask"] * 100
                            if len(strategy.positions) == 0 and diff_pct >= strategy.entry_diff:
                                print(f"[{strategy.strategy_name}] {now} | Opening trade on {sym}, premium {diff_pct:.4f}%")
                                self.open_trade(strategy, {
                                    "symbol": sym,
                                    "spot_price": sp["ask"],
                                    "futures_price": fp["bid"],
                                    "diff": diff_pct
                                })
                                break
            time.sleep(0.01)

    def run(self):
        print(f"Starting multi-strategy bot simulation... {now_ist()}")
        self.fetch_symbols()
        self.full_market_scan()
        try:
            self.monitor_loop()
        except KeyboardInterrupt:
            self.stop_event.set()
            print(f"Exiting and saving logs... {now_ist()}")
            for strategy in self.strategies:
                self.save_logs(strategy)


if __name__ == "__main__":
    bot = ArbitrageBot()
    bot.run()
