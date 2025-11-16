import requests
import time
from datetime import datetime, timedelta
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor

STRATEGIES = [
    {'name': 'Strategy_1', 'entry_diff': 0.30,  'exit_diff': 0.0},    # Entry: +0.3%, Exit: ≤0%
    {'name': 'Strategy_2', 'entry_diff': 0.20,  'exit_diff': -0.20},  # Entry: +0.2%, Exit: ≤-0.2%
    {'name': 'Strategy_3', 'entry_diff': 0.20,  'exit_diff': -0.25},  # Entry: +0.2%, Exit: ≤-0.25%
    {'name': 'Strategy_4', 'entry_diff': 0.12,  'exit_diff': -0.24},  # Entry: +0.12%, Exit: ≤-0.24%
    {'name': 'Strategy_5', 'entry_diff': 0.25,  'exit_diff': -0.25},  # NEW: Entry: +0.25%, Exit: ≤-0.25%
]

class ArbitrageBotStrategy:
    def __init__(self, entry_diff, exit_diff, strategy_name):
        self.entry_diff = entry_diff
        self.exit_diff = exit_diff
        self.strategy_name = strategy_name
        self.positions = {}
        self.trade_log = []

class ArbitrageBot:
    def __init__(self):
        self.strategies = [ArbitrageBotStrategy(s['entry_diff'], s['exit_diff'], s['name']) for s in STRATEGIES]
        self.candidate_coins = []
        self.CANDIDATE_TRACK_TIME = 2 * 60  # 2 minutes tracking window
        self.candidate_track_start = None
        self.SPOT_FEE_RATE = 0.001  # 0.1%
        self.FUTURES_FEE_RATE = 0.0004  # 0.04%
        self.TRADE_AMOUNT_USD = 100
        self.MIN_SCAN_DIFF_PERCENT = 0.10  # Scan threshold 0.1%
        self.stop_event = threading.Event()

        self.spot_symbols = []
        self.futures_symbols = []

        self.spot_prices = {}
        self.futures_prices = {}

        # Funding info: dict symbol -> dict with 'interval_hours', 'next_funding_time', 'last_funding_applied_time', 'last_funding_rate'
        self.funding_info = {}

    def fetch_symbols(self):
        spot_info = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=5).json()
        futures_info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=5).json()

        self.spot_symbols = [s["symbol"] for s in spot_info["symbols"]
                             if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]
        self.futures_symbols = [s["symbol"] for s in futures_info["symbols"]
                               if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING" and s["quoteAsset"] == "USDT"]

        # Initialize funding info for all futures symbols
        self.fetch_funding_info_all_symbols()

    def fetch_funding_info_all_symbols(self):
        # Fetch funding intervals and next funding time for each futures symbol
        url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            now_ts = datetime.utcnow()
            for entry in data:
                sym = entry["symbol"]
                if sym not in self.futures_symbols:
                    continue
                # Funding interval on Binance USDT futures is usually 8 hours (can vary; no direct API, so assuming 8h)
                # nextFundingTime is epoch milliseconds
                next_ft_ms = int(entry.get("nextFundingTime", 0))
                next_ft = datetime.utcfromtimestamp(next_ft_ms / 1000) if next_ft_ms > 0 else None
                # Funding rate
                funding_rate = float(entry.get("lastFundingRate", 0))
                # Record funding info
                if sym not in self.funding_info:
                    self.funding_info[sym] = {
                        "interval_hours": 8,  # default 8 hours (no official per-symbol API for interval)
                        "next_funding_time": next_ft,
                        "last_funding_applied_time": None,
                        "last_funding_rate": funding_rate
                    }
                else:
                    self.funding_info[sym]["next_funding_time"] = next_ft
                    self.funding_info[sym]["last_funding_rate"] = funding_rate
        except Exception as e:
            print(f"[{datetime.now()}] Warning: Failed to fetch funding info: {e}")

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
            print(f"[{datetime.now()}] Warning: Failed to fetch batch prices from {url} symbols count {len(symbol_list)}: {e}")

    def fetch_prices(self):
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        spot_url = "https://api.binance.com/api/v3/ticker/bookTicker"
        futures_url = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"

        with ThreadPoolExecutor(max_workers=5) as executor:
            spot_batches = list(chunks(self.spot_symbols, 50))
            futures_batches = list(chunks(self.futures_symbols, 50))

            spot_futures = [executor.submit(self.fetch_batch_prices, spot_url, batch, self.spot_prices) for batch in spot_batches]
            futures_futures = [executor.submit(self.fetch_batch_prices, futures_url, batch, self.futures_prices) for batch in futures_batches]

            for f in spot_futures:
                f.result()
                time.sleep(0.01)

            for f in futures_futures:
                f.result()
                time.sleep(0.01)

        # Also refresh funding info every minute approximately
        now = datetime.now()
        if not hasattr(self, "_last_funding_info_fetch") or (now - self._last_funding_info_fetch).seconds > 60:
            self.fetch_funding_info_all_symbols()
            self._last_funding_info_fetch = now

    def full_market_scan(self):
        now = datetime.now()
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
            self.last_candidate_update_time = now
        else:
            print("No arbitrage candidates found.")
            self.candidate_coins = []
            self.candidate_track_start = None
            self.last_candidate_update_time = None

    def apply_funding_fee_if_due(self, strategy, sym, position):
        # Apply funding fee if funding payout time has passed and not yet applied for this position

        info = self.funding_info.get(sym)
        if not info:
            return  # No funding info available, skip

        next_funding_time = info["next_funding_time"]
        last_applied = position.get("last_funding_applied_time")
        now = datetime.utcnow()

        if not next_funding_time:
            return  # Next funding time unknown, skip

        # If funding time is past or now, and not yet applied for that funding time
        # Also, do not apply multiple times for same funding payout
        funding_window_start = next_funding_time - timedelta(hours=info["interval_hours"])
        # Funding interval assumed 8 hours, adjustable if API changes

        # We compare and make sure funding applies only once per funding payout window
        if (now >= next_funding_time and (not last_applied or last_applied < funding_window_start)):
            # Calculate funding fee amount
            # funding fee = position size * funding rate (funding rate per funding interval)
            # position size = contracts * entry price (spot qty * entry spot price)
            # Direction matters: if funding rate positive, longs pay shorts; bot shorts futures and longs spot,
            # so bot will pay funding when rate > 0, receive if rate < 0 on futures leg.

            funding_rate = info["last_funding_rate"]
            spot_qty = position["spot_qty"]
            entry_spot_price = position["entry_spot_price"]

            # Position value approx in USD on spot side
            position_value = spot_qty * entry_spot_price

            # Funding fee paid on futures side
            funding_fee_amount = position_value * funding_rate

            # Funding fee reduces P&L (subtract if positive, add if negative funding fee)
            position["funding_fee_accumulated"] = position.get("funding_fee_accumulated", 0) + funding_fee_amount

            # Record last applied time for funding payout to avoid repeat
            position["last_funding_applied_time"] = now

            # Log funding fee event with timestamp and amount
            self.log_funding_fee(strategy, sym, datetime.utcnow(), funding_fee_amount, funding_rate, position_value)

            print(f"[{strategy.strategy_name}] {datetime.now()} | Applied funding fee on {sym}: {funding_fee_amount:.6f} USD @ rate {funding_rate:.8f}")

    def open_trade(self, strategy, coin):
        sym = coin["symbol"]
        spot_price = coin["spot_price"]
        fut_price = coin["futures_price"]
        ts = datetime.now()
        spot_qty = self.TRADE_AMOUNT_USD / spot_price
        fut_qty = self.TRADE_AMOUNT_USD / fut_price
        strategy.positions[sym] = {
            "entry_time": ts,
            "entry_spot_price": spot_price,
            "entry_futures_price": fut_price,
            "spot_qty": spot_qty,
            "futures_qty": fut_qty,
            "funding_fee_accumulated": 0,
            "last_funding_applied_time": None,
        }
        print(f"\n[{strategy.strategy_name}] {ts} | Opened trade on {sym}")
        print(f"  Spot buy @ {spot_price:.5f} (ask) qty: {spot_qty:.4f}")
        print(f"  Futures short @ {fut_price:.5f} (bid) qty: {fut_qty:.4f}")

        # Log entry as well
        self.log_trade(strategy, sym, "ENTRY", ts, spot_price, fut_price, spot_qty, fut_qty, 0, 0, 0)

    def close_trade(self, strategy, sym):
        pos = strategy.positions[sym]
        ts = datetime.now()

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

        # Include funding fees accumulated over the lifetime of the position
        funding_fee_accumulated = pos.get("funding_fee_accumulated", 0)

        total_fees = spot_fee + fut_fee
        net_pnl = gross_pnl - total_fees - funding_fee_accumulated

        print(f"\n[{strategy.strategy_name}] {ts} | Closed trade on {sym}")
        print(f"  Spot sell @ {sp_current:.5f} (bid), Futures cover @ {fp_current:.5f} (ask)")
        print(f"  Gross P&L: Spot: {pnl_spot:.4f} + Futures: {pnl_fut:.4f} = Total: {gross_pnl:.4f}")
        print(f"  Fees: Spot: {spot_fee:.4f}, Futures: {fut_fee:.4f}, Funding Fees: {funding_fee_accumulated:.4f}")
        print(f"  Net P&L after fees: {net_pnl:.4f}")

        self.log_trade(strategy, sym, "EXIT", ts, sp_current, fp_current, spot_qty, fut_qty, total_fees, total_fees, net_pnl,
                       entry_time=pos["entry_time"],
                       entry_spot_price=spot_entry,
                       entry_futures_price=fut_entry,
                       gross_pnl=gross_pnl,
                       spot_fee=spot_fee,
                       futures_fee=fut_fee,
                       funding_fee=funding_fee_accumulated)

        del strategy.positions[sym]

        self.save_logs(strategy)  # Save immediately after closing trade

        self.full_market_scan()

    def log_trade(self, strategy, sym, action, ts, sp, fp, sp_qty, fp_qty, sp_fee, fp_fee, pnl,
                  entry_time=None, entry_spot_price=None, entry_futures_price=None, gross_pnl=None, spot_fee=None,
                  futures_fee=None, funding_fee=None):
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
                "funding_fee": funding_fee,
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
                "funding_fee": None,
                "net_pnl": None
            })
        strategy.trade_log.append(log_entry)

    def log_funding_fee(self, strategy, sym, ts, funding_fee_amount, funding_rate, position_value):
        # Log funding fee as special row in trade log for tracking
        log_entry = {
            "symbol": sym,
            "action": "FUNDING_FEE",
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "spot_price": None,
            "futures_price": None,
            "spot_qty": None,
            "futures_qty": None,
            "spot_fee": None,
            "futures_fee": None,
            "pnl": None,
            "entry_time": "",
            "entry_spot_price": None,
            "entry_futures_price": None,
            "gross_pnl": None,
            "spot_fee_detail": None,
            "futures_fee_detail": None,
            "funding_fee": funding_fee_amount,
            "net_pnl": None,
            "funding_rate": funding_rate,
            "position_value": position_value
        }
        strategy.trade_log.append(log_entry)

    def save_logs(self, strategy):
        df = pd.DataFrame(strategy.trade_log)
        # Reorder columns for clarity, including extra funding_rate and position_value if exists
        column_order = ["symbol", "action", "entry_time", "timestamp", "entry_spot_price", "entry_futures_price",
                        "spot_price", "futures_price", "spot_qty", "futures_qty",
                        "gross_pnl", "spot_fee_detail", "futures_fee_detail", "spot_fee", "futures_fee", "funding_fee", "pnl", "net_pnl",
                        "funding_rate", "position_value"]
        df = df.reindex(columns=column_order)
        excel_name = f"trade_log_{strategy.strategy_name}.xlsx"
        csv_name = f"trade_log_{strategy.strategy_name}_backup.csv"
        df.to_excel(excel_name, index=False)
        df.to_csv(csv_name, index=False)
        print(f"[{strategy.strategy_name}] Trade logs saved.")

    def monitor_loop(self):
        while not self.stop_event.is_set():
            now = datetime.now()

            self.fetch_prices()

            for strategy in self.strategies:
                if strategy.positions:
                    for sym in list(strategy.positions.keys()):
                        sp = self.spot_prices.get(sym)
                        fp = self.futures_prices.get(sym)
                        if not sp or not fp:
                            continue

                        # Apply funding fee if funding payout time has passed
                        self.apply_funding_fee_if_due(strategy, sym, strategy.positions[sym])

                        # Adjusted exit premium calculation using futures_ask and spot_bid
                        diff_pct = (fp["ask"] - sp["bid"]) / sp["bid"] * 100
                        # Exit rules per strategy
                        if diff_pct is not None and diff_pct <= strategy.exit_diff:
                            print(f"[{strategy.strategy_name}] {now} | Premium ≤ {strategy.exit_diff}% on {sym}, exiting trade.")
                            self.close_trade(strategy, sym)
                else:
                    # Entry rule per strategy
                    if not self.candidate_coins:
                        self.full_market_scan()
                        continue
                    else:
                        elapsed = (now - self.candidate_track_start).total_seconds() if self.candidate_track_start else 0
                        if elapsed > self.CANDIDATE_TRACK_TIME:
                            print(f"{now} Candidate tracking expired, rescanning full market")
                            self.full_market_scan()
                            continue

                        # Only one open position at a time per strategy
                        for c in self.candidate_coins:
                            sym = c["symbol"]
                            sp = self.spot_prices.get(sym)
                            fp = self.futures_prices.get(sym)
                            if not sp or not fp:
                                continue
                            diff_pct = (fp["bid"] - sp["ask"]) / sp["ask"] * 100  # Entry premium unchanged
                            if len(strategy.positions) == 0 and diff_pct >= strategy.entry_diff:
                                print(f"[{strategy.strategy_name}] {now} | Opening trade on {sym}, premium {diff_pct:.4f}%")
                                self.open_trade(strategy, {
                                    "symbol": sym,
                                    "spot_price": sp["ask"],
                                    "futures_price": fp["bid"],
                                    "diff": diff_pct
                                })
                                break  # Only one open position per strategy at a time

            time.sleep(0.01)

    def run(self):
        print("Starting multi-strategy bot simulation...")
        self.fetch_symbols()
        self.full_market_scan()
        try:
            self.monitor_loop()
        except KeyboardInterrupt:
            self.stop_event.set()
            print("Exiting and saving logs...")
            for strategy in self.strategies:
                self.save_logs(strategy)

if __name__ == "__main__":
    bot = ArbitrageBot()
    bot.run()
