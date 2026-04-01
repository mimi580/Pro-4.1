"""
DERIV EXPIRYRANGE BOT  — Python Edition
=========================================
Symbol  : 1HZ10V  (Volatility 10 Index — 1-second feed)
Contract: EXPIRYRANGE  (Ends In: win if price stays within ±1.7 at expiry)
Duration: 2 minutes  (120 seconds)
Barriers: ±1.7

Signal model — 6-layer consensus vote system:

  LAYER 0 — ATR Regime Filter
    · ATR(20) / ATR(100) ratio classifies volatility
    · LOW  vol  → EXPIRYRANGE (price stays within barriers)
    · HIGH vol  → skip trade (too risky for Ends In)
    · MEDIUM    → proceed to other layers

  LAYER 1 — Bollinger Band Width
    · Narrow BB (consolidation) → vote TRADE
    · Wide BB   (expansion)     → vote SKIP

  LAYER 2 — RSI(14) Mean-Reversion Gate
    · RSI between 35–65 (neutral zone) → vote TRADE
    · RSI at extremes (<30 or >70)     → vote SKIP

  LAYER 3 — EMA Distance Filter
    · Price close to EMA midpoint (EMA7+EMA14)/2 → vote TRADE
    · Price far from EMA midpoint                 → vote SKIP

  LAYER 4 — Candle Body Strength
    · Small body (indecision/consolidation) → vote TRADE
    · Full body  (momentum)                 → vote SKIP

  LAYER 5 — Tick Streak Counter (1HZ10V specific)
    · Low directional streak (oscillating) → vote TRADE
    · High streak (trending hard)          → vote SKIP

  CONSENSUS
    · Minimum 3 TRADE votes required to place EXPIRYRANGE
    · Anything less → wait for better conditions

Martingale:
    1.5x multiplier — $0.35 → $0.53 → $0.79 → $1.19 → $1.78
    Resets after max_losses consecutive losses or on any win
"""

import asyncio
import json
import math
import os
import sys
import time
from collections import deque
from datetime import datetime

try:
    import websockets
    from websockets.exceptions import (
        ConnectionClosed, ConnectionClosedError, ConnectionClosedOK,
    )
except ImportError:
    sys.exit("websockets not installed — run: pip install websockets")


# ============================================================================
# CONFIGURATION
# ============================================================================

def _env(key: str, default):
    val = os.environ.get(key)
    if val is None:
        return default
    if isinstance(default, bool):
        return val.lower() in ("1", "true", "yes")
    if isinstance(default, float):
        return float(val)
    if isinstance(default, int):
        return int(val)
    return val


CONFIG = {
    # ── Deriv credentials ──────────────────────────────────────
    "api_token":        _env("DERIV_API_TOKEN", "REPLACE_WITH_YOUR_TOKEN"),
    "app_id":           _env("DERIV_APP_ID", 1089),

    # ── Contract parameters ────────────────────────────────────
    "symbol":           _env("SYMBOL",   "1HZ10V"),
    "contract_type":    "EXPIRYRANGE",
    "duration":         _env("DURATION", 2),        # minutes
    "duration_unit":    "m",
    "barrier":          _env("BARRIER",  1.7),      # ±1.7
    "currency":         "USD",

    # ── Technical indicator settings ──────────────────────────
    "ema_fast":         7,
    "ema_slow":         14,
    "atr_short":        20,
    "atr_long":         100,
    "bb_period":        20,
    "bb_stddev":        2.0,
    "rsi_period":       14,
    "streak_window":    10,
    "candle_interval":  60,     # 1-minute candles

    # ── Regime thresholds ──────────────────────────────────────
    "regime_low_ratio":  _env("REGIME_LOW",    0.80),
    "regime_high_ratio": _env("REGIME_HIGH",   1.20),
    "bb_narrow_thresh":  _env("BB_NARROW",     0.020),
    "bb_wide_thresh":    _env("BB_WIDE",       0.040),
    "rsi_mid_low":       _env("RSI_MID_LOW",   35.0),
    "rsi_mid_high":      _env("RSI_MID_HIGH",  65.0),
    "ema_dist_in":       _env("EMA_DIST_IN",   1.2),
    "body_ratio_in":     _env("BODY_IN",       0.50),
    "streak_in":         _env("STREAK_IN",     4),
    "min_votes":         _env("MIN_VOTES",     3),
    "min_ticks":         _env("MIN_TICKS",     120),

    # ── Risk / Martingale ──────────────────────────────────────
    "initial_stake":    _env("INITIAL_STAKE",   0.35),
    "martingale_mul":   _env("MARTINGALE_MUL",  1.5),
    "max_losses":       _env("MAX_LOSSES",       5),
    "target_profit":    _env("TARGET_PROFIT",    5.0),
    "stop_loss":        _env("STOP_LOSS",       15.0),

    # ── Resilience ─────────────────────────────────────────────
    "lock_timeout":         _env("LOCK_TIMEOUT",      130),  # 2 min + buffer
    "buy_recv_retries":     _env("BUY_RETRIES",          8),
    "reconnect_delay_min":  _env("RECONNECT_MIN",         2),
    "reconnect_delay_max":  _env("RECONNECT_MAX",        60),
    "ws_ping_interval":     _env("WS_PING",              30),
    "orphan_poll_attempts": _env("ORPHAN_ATTEMPTS",       4),
    "orphan_poll_interval": _env("ORPHAN_INTERVAL",       3),
}


# ============================================================================
# HELPERS
# ============================================================================

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _log(tag: str, msg: str):
    print(f"[{_ts()}] [{tag}] {msg}", flush=True)


# ============================================================================
# TECHNICAL INDICATORS
# ============================================================================

def calc_ema_series(prices: list, period: int) -> list:
    if len(prices) < period:
        return [None] * len(prices)
    alpha  = 2 / (period + 1)
    result = [None] * len(prices)
    result[period - 1] = sum(prices[:period]) / period
    for i in range(period, len(prices)):
        result[i] = alpha * prices[i] + (1 - alpha) * result[i - 1]
    return result

def ema_current(prices: list, period: int) -> float | None:
    if len(prices) < period:
        return None
    alpha = 2 / (period + 1)
    val   = sum(prices[:period]) / period
    for p in prices[period:]:
        val = alpha * p + (1 - alpha) * val
    return val

def calc_atr(prices: list, window: int) -> float:
    if len(prices) < 2:
        return 0.0
    moves = [abs(prices[i] - prices[i - 1])
             for i in range(max(1, len(prices) - window), len(prices))]
    return sum(moves) / len(moves) if moves else 0.0

def calc_sma(prices: list, period: int) -> float | None:
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period

def calc_stddev(prices: list, period: int) -> float | None:
    if len(prices) < period:
        return None
    window   = prices[-period:]
    mean     = sum(window) / period
    variance = sum((x - mean) ** 2 for x in window) / period
    return math.sqrt(variance)

def calc_bb_width(prices: list, period: int, num_std: float) -> float | None:
    sma = calc_sma(prices, period)
    std = calc_stddev(prices, period)
    if sma is None or std is None or sma == 0:
        return None
    return (2 * num_std * std) / sma

def calc_rsi(prices: list, period: int) -> float | None:
    if len(prices) < period + 1:
        return None
    gains, losses = [], []
    for i in range(len(prices) - period, len(prices)):
        diff = prices[i] - prices[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))

def calc_tick_streak(prices: list, window: int) -> int:
    if len(prices) < window + 1:
        return 0
    recent = prices[-(window + 1):]
    score  = 0
    for i in range(1, len(recent)):
        if recent[i] > recent[i - 1]:
            score += 1
        elif recent[i] < recent[i - 1]:
            score -= 1
    return score


# ============================================================================
# CANDLE BUILDER
# ============================================================================

class CandleBuilder:
    def __init__(self, interval_seconds: int = 60):
        self.interval       = interval_seconds
        self.candles: list  = []
        self._current: dict | None = None
        self._bucket_start: float | None = None

    def add_tick(self, price: float, epoch: float | None = None) -> bool:
        now    = epoch or time.time()
        bucket = int(now // self.interval) * self.interval

        if self._bucket_start is None:
            self._bucket_start = bucket
            self._current = {"open": price, "high": price,
                             "low": price, "close": price, "t": bucket}
            return False

        if bucket == self._bucket_start:
            c = self._current
            c["high"]  = max(c["high"], price)
            c["low"]   = min(c["low"],  price)
            c["close"] = price
            return False

        self.candles.append(dict(self._current))
        if len(self.candles) > 200:
            self.candles.pop(0)
        self._bucket_start = bucket
        self._current = {"open": price, "high": price,
                         "low": price, "close": price, "t": bucket}
        return True

    def last_completed(self) -> dict | None:
        return self.candles[-1] if self.candles else None

    def body_ratio(self, candle: dict) -> float:
        rng = candle["high"] - candle["low"]
        if rng == 0:
            return 0.0
        return abs(candle["close"] - candle["open"]) / rng


# ============================================================================
# SIGNAL ENGINE  — 6-layer vote system (EXPIRYRANGE only)
# ============================================================================

TRADE   = "TRADE"
SKIP    = "SKIP"
ABSTAIN = None


class SignalEngine:
    """
    Each layer votes TRADE (place EXPIRYRANGE), SKIP, or ABSTAIN.
    Final decision: >= min_votes TRADE votes required.
    """

    def __init__(self, cfg: dict):
        self.cfg        = cfg
        self.prices     = deque(maxlen=500)
        self.candles    = CandleBuilder(cfg["candle_interval"])
        self.tick_count = 0

    def add_tick(self, price: float, epoch: float | None = None):
        self.prices.append(price)
        self.tick_count += 1
        self.candles.add_tick(price, epoch)

    def is_ready(self) -> bool:
        return self.tick_count >= self.cfg["min_ticks"]

    # ── Layer 0 — ATR regime ─────────────────────────────────────────────────
    def _vote_atr_regime(self) -> str | None:
        cfg    = self.cfg
        prices = list(self.prices)
        atr_s  = calc_atr(prices, cfg["atr_short"])
        atr_l  = calc_atr(prices, cfg["atr_long"])
        if atr_l == 0:
            return ABSTAIN
        ratio = atr_s / atr_l
        if ratio < cfg["regime_low_ratio"]:
            return TRADE          # low volatility → price stays in range
        if ratio > cfg["regime_high_ratio"]:
            return SKIP           # high volatility → too risky for ends-in
        return ABSTAIN

    # ── Layer 1 — Bollinger Band width ───────────────────────────────────────
    def _vote_bb_width(self) -> str | None:
        cfg    = self.cfg
        prices = list(self.prices)
        bw     = calc_bb_width(prices, cfg["bb_period"], cfg["bb_stddev"])
        if bw is None:
            return ABSTAIN
        if bw < cfg["bb_narrow_thresh"]:
            return TRADE          # narrow = consolidation = ends in edge
        if bw > cfg["bb_wide_thresh"]:
            return SKIP           # wide = expansion = bad for ends in
        return ABSTAIN

    # ── Layer 2 — RSI mean-reversion gate ────────────────────────────────────
    def _vote_rsi(self) -> str | None:
        cfg    = self.cfg
        prices = list(self.prices)
        rsi    = calc_rsi(prices, cfg["rsi_period"])
        if rsi is None:
            return ABSTAIN
        if cfg["rsi_mid_low"] <= rsi <= cfg["rsi_mid_high"]:
            return TRADE          # neutral RSI = not at extremes
        return SKIP               # extended RSI = momentum risk

    # ── Layer 3 — EMA distance filter ────────────────────────────────────────
    def _vote_ema_distance(self) -> str | None:
        cfg    = self.cfg
        prices = list(self.prices)
        ema7   = ema_current(prices, cfg["ema_fast"])
        ema14  = ema_current(prices, cfg["ema_slow"])
        atr    = calc_atr(prices, cfg["atr_short"])
        if ema7 is None or ema14 is None or atr == 0:
            return ABSTAIN
        ema_mid = (ema7 + ema14) / 2
        dist    = abs(prices[-1] - ema_mid) / atr
        if dist < cfg["ema_dist_in"]:
            return TRADE          # price near centre = will likely stay in
        return SKIP               # price far from mean = risky

    # ── Layer 4 — Candle body strength ───────────────────────────────────────
    def _vote_candle_body(self) -> str | None:
        cfg    = self.cfg
        candle = self.candles.last_completed()
        if candle is None:
            return ABSTAIN
        br = self.candles.body_ratio(candle)
        if br < cfg["body_ratio_in"]:
            return TRADE          # small body = indecision/consolidation
        return SKIP               # full body = momentum

    # ── Layer 5 — Tick streak (1HZ10V specific) ──────────────────────────────
    def _vote_tick_streak(self) -> str | None:
        cfg    = self.cfg
        prices = list(self.prices)
        streak = abs(calc_tick_streak(prices, cfg["streak_window"]))
        if streak <= cfg["streak_in"]:
            return TRADE          # oscillating ticks = will stay in range
        return SKIP               # directional run = likely to break out

    # ── Consensus ─────────────────────────────────────────────────────────────
    def compute(self) -> dict:
        layers = {
            "atr_regime":   self._vote_atr_regime(),
            "bb_width":     self._vote_bb_width(),
            "rsi":          self._vote_rsi(),
            "ema_distance": self._vote_ema_distance(),
            "candle_body":  self._vote_candle_body(),
            "tick_streak":  self._vote_tick_streak(),
        }

        votes_trade = sum(1 for v in layers.values() if v == TRADE)
        votes_skip  = sum(1 for v in layers.values() if v == SKIP)
        total       = votes_trade + votes_skip
        min_v       = self.cfg["min_votes"]

        if votes_trade >= min_v and votes_trade > votes_skip:
            decision = "EXPIRYRANGE"
            reason   = f"TRADE ({votes_trade}/{total} votes for EXPIRYRANGE)"
        else:
            decision = None
            reason   = f"No trade (TRADE={votes_trade} SKIP={votes_skip} min={min_v})"

        return dict(
            decision=decision,
            votes_trade=votes_trade,
            votes_skip=votes_skip,
            total_votes=total,
            layers=layers,
            reason=reason,
        )


# ============================================================================
# MARTINGALE MANAGER
# ============================================================================

class MartingaleManager:
    def __init__(self, cfg: dict):
        self.initial_stake = cfg["initial_stake"]
        self.current_stake = cfg["initial_stake"]
        self.mul           = cfg["martingale_mul"]
        self.max_losses    = cfg["max_losses"]
        self.target_profit = cfg["target_profit"]
        self.stop_loss     = cfg["stop_loss"]
        self.loss_streak   = 0
        self.total_profit  = 0.0
        self.wins          = 0
        self.losses        = 0

    def get_stake(self) -> float:
        return round(self.current_stake, 2)

    def record_win(self, profit: float):
        self.wins         += 1
        self.total_profit += profit
        self.loss_streak   = 0
        self.current_stake = self.initial_stake
        _log("WIN",   f"+${profit:.2f} | stake reset → ${self.initial_stake:.2f}")
        self._print_stats()

    def record_loss(self, loss: float):
        self.losses       += 1
        self.total_profit += loss
        self.loss_streak  += 1
        _log("LOSS",  f"-${abs(loss):.2f} | streak={self.loss_streak}")
        if self.loss_streak >= self.max_losses:
            _log("MARTI", f"{self.max_losses} losses → reset to ${self.initial_stake:.2f}")
            self.current_stake = self.initial_stake
            self.loss_streak   = 0
        else:
            self.current_stake = round(self.current_stake * self.mul, 2)
            _log("MARTI", f"L{self.loss_streak} next stake ${self.current_stake:.2f}")
        self._print_stats()

    def can_trade(self) -> bool:
        if self.total_profit >= self.target_profit:
            _log("RISK", f"Target profit reached (${self.total_profit:.2f})")
            return False
        if self.total_profit <= -self.stop_loss:
            _log("RISK", f"Stop-loss hit (${self.total_profit:.2f})")
            return False
        return True

    def _print_stats(self):
        total = self.wins + self.losses
        wr    = (self.wins / total * 100) if total > 0 else 0.0
        print(f"\n{'='*58}")
        print(f"  {total} trades | W:{self.wins} L:{self.losses} | WR:{wr:.1f}%")
        print(f"  P&L ${self.total_profit:+.2f} | next stake ${self.current_stake:.2f}")
        print(f"{'='*58}\n")


# ============================================================================
# DERIV CLIENT  (send queue · receive inbox · orphan recovery)
# ============================================================================

class DerivClient:
    def __init__(self, cfg: dict):
        self.api_token = cfg["api_token"]
        self.app_id    = cfg["app_id"]
        self.symbol    = cfg["symbol"]
        self.cfg       = cfg
        self.endpoint  = (
            f"wss://ws.derivws.com/websockets/v3?app_id={self.app_id}"
        )
        self.ws                    = None
        self._send_queue: asyncio.Queue | None = None
        self._inbox:      asyncio.Queue | None = None
        self._send_task:  asyncio.Task  | None = None
        self._recv_task:  asyncio.Task  | None = None

    async def connect(self) -> bool:
        _log("WS", f"Connecting → {self.endpoint}")
        self.ws = await websockets.connect(
            self.endpoint,
            ping_interval=self.cfg["ws_ping_interval"],
            ping_timeout=20,
            close_timeout=10,
        )
        self._send_queue = asyncio.Queue()
        self._inbox      = asyncio.Queue()
        self._start_io()
        await self.send({"authorize": self.api_token})
        resp = await self.receive_type("authorize", timeout=15)
        if resp is None or "error" in resp:
            _log("AUTH", f"Failed: {(resp or {}).get('error', {}).get('message', 'timeout')}")
            return False
        auth = resp.get("authorize", {})
        _log("AUTH",
             f"OK | {auth.get('loginid','?')} | "
             f"Balance: ${auth.get('balance', 0):.2f} {auth.get('currency', '')}")
        return True

    def _start_io(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        self._send_task = asyncio.create_task(self._send_pump(), name="send_pump")
        self._recv_task = asyncio.create_task(self._recv_pump(), name="recv_pump")

    async def _send_pump(self):
        while True:
            data, fut = await self._send_queue.get()
            try:
                await self.ws.send(json.dumps(data))
                if fut and not fut.done():
                    fut.set_result(True)
            except Exception as exc:
                if fut and not fut.done():
                    fut.set_exception(exc)
            finally:
                self._send_queue.task_done()

    async def _recv_pump(self):
        try:
            async for raw in self.ws:
                try:
                    await self._inbox.put(json.loads(raw))
                except json.JSONDecodeError:
                    pass
        except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK):
            await self._inbox.put({"__disconnect__": True})
        except Exception as exc:
            _log("RECV", f"Error: {exc}")
            await self._inbox.put({"__disconnect__": True})

    async def close(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass

    async def send(self, data: dict):
        loop = asyncio.get_event_loop()
        fut  = loop.create_future()
        await self._send_queue.put((data, fut))
        await fut

    async def receive(self, timeout: float = 10) -> dict:
        try:
            return await asyncio.wait_for(self._inbox.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return {}

    async def receive_type(self, msg_type: str, timeout: float = 10) -> dict | None:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None
            try:
                msg = await asyncio.wait_for(self._inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            if "__disconnect__" in msg:
                await self._inbox.put(msg)
                return None
            if msg_type in msg or "error" in msg:
                return msg
            await self._inbox.put(msg)

    async def subscribe_ticks(self) -> bool:
        await self.send({"ticks": self.symbol, "subscribe": 1})
        resp = await self.receive_type("tick", timeout=10)
        if resp is None or "error" in resp:
            _log("TICK", f"Subscribe failed: {(resp or {}).get('error', {}).get('message', 'timeout')}")
            return False
        _log("TICK", f"Subscribed to {self.symbol}")
        return True

    async def place_trade(self, stake: float, barrier: float,
                          duration: int, duration_unit: str) -> str | None:
        proposal_req = {
            "proposal":       1,
            "amount":         stake,
            "basis":          "stake",
            "contract_type":  "EXPIRYRANGE",
            "currency":       self.cfg["currency"],
            "duration":       duration,
            "duration_unit":  duration_unit,
            "symbol":         self.symbol,
            "barrier":        f"+{barrier:.2f}",
            "barrier2":       f"-{barrier:.2f}",
        }
        await self.send(proposal_req)
        proposal = await self.receive_type("proposal", timeout=12)
        if proposal is None or "error" in proposal:
            _log("PROPOSAL", f"Error: {(proposal or {}).get('error', {}).get('message', 'timeout')}")
            return None
        proposal_id = proposal.get("proposal", {}).get("id")
        if not proposal_id:
            _log("PROPOSAL", "No proposal ID")
            return None

        buy_time    = time.time()
        contract_id = None
        await self.send({"buy": proposal_id, "price": stake})

        for attempt in range(self.cfg["buy_recv_retries"]):
            resp = await self.receive_type("buy", timeout=8)
            if resp is None:
                _log("BUY", f"No response (attempt {attempt + 1})")
                continue
            if "error" in resp:
                _log("BUY", f"Error: {resp['error'].get('message', '')}")
                return None
            contract_id = resp.get("buy", {}).get("contract_id")
            if contract_id:
                break

        if not contract_id:
            _log("BUY", "No contract_id — running orphan recovery")
            contract_id = await self._recover_orphan(stake, buy_time)
            if contract_id:
                _log("BUY", f"Orphan recovered → {contract_id}")
            else:
                _log("BUY", "Orphan recovery failed — unlocking")
                return None

        _log("TRADE",
             f"EXPIRYRANGE  ${stake:.2f}  ±{barrier}  "
             f"{duration}{duration_unit}  contract={contract_id}")

        try:
            await self.send({"proposal_open_contract": 1,
                             "contract_id": contract_id, "subscribe": 1})
        except Exception as exc:
            _log("TRADE", f"Subscribe to updates failed: {exc}")

        return contract_id

    async def _recover_orphan(self, stake: float, buy_time: float) -> str | None:
        for attempt in range(self.cfg["orphan_poll_attempts"]):
            await asyncio.sleep(self.cfg["orphan_poll_interval"])
            try:
                await self.send({"profit_table": 1, "description": 1,
                                 "sort": "DESC", "limit": 5})
                resp = await self.receive_type("profit_table", timeout=10)
                if not resp or "error" in resp:
                    continue
                for tx in resp.get("profit_table", {}).get("transactions", []):
                    if (abs(float(tx.get("buy_price", 0)) - stake) < 0.01 and
                            float(tx.get("purchase_time", 0)) >= buy_time - 5):
                        return str(tx.get("contract_id"))
            except Exception as exc:
                _log("ORPHAN", f"Poll {attempt + 1} error: {exc}")
        return None

    async def poll_contract(self, contract_id: str) -> dict | None:
        try:
            await self.send({"proposal_open_contract": 1,
                             "contract_id": contract_id})
            resp = await self.receive_type("proposal_open_contract", timeout=10)
            if resp and "proposal_open_contract" in resp:
                return resp["proposal_open_contract"]
        except Exception as exc:
            _log("POLL", f"Error: {exc}")
        return None


# ============================================================================
# MAIN BOT
# ============================================================================

class ExpiryRangeBot:
    def __init__(self, cfg: dict = CONFIG):
        self.cfg    = cfg
        self.client = DerivClient(cfg)
        self.signal = SignalEngine(cfg)
        self.risk   = MartingaleManager(cfg)

        self.tick_count     = 0
        self.last_eval_tick = 0

        self.current_contract:   dict | None  = None
        self.waiting_for_result: bool         = False
        self.lock_since:         float | None = None

        self._stop = False

    # ── Lock helpers ─────────────────────────────────────────────────────────

    def _unlock(self, reason: str = "manual"):
        if self.waiting_for_result:
            cid = (self.current_contract or {}).get("id", "?")
            _log("UNLOCK", f"Contract {cid} ({reason})")
        self.waiting_for_result = False
        self.current_contract   = None
        self.lock_since         = None

    def _check_lock_timeout(self):
        if not self.waiting_for_result or self.lock_since is None:
            return
        elapsed = time.monotonic() - self.lock_since
        if elapsed >= self.cfg["lock_timeout"]:
            _log("TIMEOUT", f"Locked {elapsed:.0f}s — auto-unlocking")
            self._unlock("timeout")

    # ── Console listener ─────────────────────────────────────────────────────

    async def _console(self):
        loop = asyncio.get_event_loop()
        _log("CMD", "Commands: [u]nlock  [s]tats  [q]uit")
        while not self._stop:
            try:
                cmd = (await loop.run_in_executor(None, input)).strip().lower()
                if cmd == "u":
                    self._unlock("user command")
                elif cmd == "s":
                    self.risk._print_stats()
                    print(f"  >> Ticks: {self.tick_count}  "
                          f"Ready: {self.signal.is_ready()}")
                elif cmd in ("q", "quit", "exit"):
                    _log("CMD", "Quit")
                    self._stop = True
                    break
            except (EOFError, KeyboardInterrupt):
                break

    # ── Tick handler ─────────────────────────────────────────────────────────

    async def on_tick(self, tick_data: dict):
        quote = tick_data.get("quote")
        if quote is None:
            return

        price = float(quote)
        epoch = float(tick_data.get("epoch", time.time()))

        self.tick_count += 1
        self.signal.add_tick(price, epoch)
        self._check_lock_timeout()

        if self.tick_count % 10 == 0:
            status = "WAITING" if self.waiting_for_result else "READY"
            warmup = (""
                      if self.signal.is_ready()
                      else f" [warmup {self.tick_count}/{self.cfg['min_ticks']}]")
            print(f"\r  #{self.tick_count}  p={price:.4f}  {status}{warmup}  {_ts()}",
                  end="", flush=True)

        if not self.waiting_for_result and self.signal.is_ready():
            if (self.tick_count - self.last_eval_tick) >= 5:
                self.last_eval_tick = self.tick_count
                print()
                await self._evaluate()

    # ── Signal evaluation and trade placement ────────────────────────────────

    async def _evaluate(self):
        if self.waiting_for_result:
            return

        result = self.signal.compute()

        print(f"\n{'='*58}")
        print(f"SIGNAL  #{self.tick_count}  {_ts()}")
        for layer, vote in result["layers"].items():
            marker = "→" if vote == TRADE else ("✗" if vote == SKIP else " ")
            print(f"  {marker} {layer:<16} {vote or 'ABSTAIN'}")
        print(f"  {'─'*40}")
        print(f"  TRADE={result['votes_trade']}  SKIP={result['votes_skip']}  "
              f"min={self.cfg['min_votes']}")
        print(f"  → {result['reason']}")
        print(f"{'='*58}")

        if result["decision"] is None:
            return
        if not self.risk.can_trade():
            return

        stake    = self.risk.get_stake()
        barrier  = self.cfg["barrier"]
        duration = self.cfg["duration"]
        unit     = self.cfg["duration_unit"]

        contract_id = await self.client.place_trade(stake, barrier, duration, unit)

        if contract_id:
            self.current_contract   = {
                "id":      contract_id,
                "stake":   stake,
                "barrier": barrier,
                "time":    datetime.now(),
            }
            self.waiting_for_result = True
            self.lock_since         = time.monotonic()
            _log("LOCK", f"Waiting for result on {contract_id}")
        else:
            _log("TRADE", "Placement failed — READY for next signal")

    # ── Settlement ───────────────────────────────────────────────────────────

    def _is_settled(self, data: dict) -> bool:
        if data.get("is_settled"):
            return True
        for key in ("status", "contract_status"):
            if data.get(key, "").lower() in ("sold", "won", "lost"):
                return True
        return False

    async def handle_settlement(self, contract_data: dict):
        cid = contract_data.get("contract_id")
        if not self.current_contract or cid != self.current_contract["id"]:
            return None
        if not self._is_settled(contract_data):
            return None

        profit = float(contract_data.get("profit", 0))
        status = contract_data.get("status", "unknown")

        print(f"\n{'='*58}")
        print(f"RESULT  contract={cid}")
        print(f"        status={status}  profit=${profit:.2f}")
        print(f"{'='*58}")

        if profit > 0:
            self.risk.record_win(profit)
        else:
            self.risk.record_loss(profit)

        self._unlock("settlement")
        return self.risk.can_trade()

    # ── Reconnect ────────────────────────────────────────────────────────────

    async def _reconnect(self) -> bool:
        delay   = self.cfg["reconnect_delay_min"]
        max_d   = self.cfg["reconnect_delay_max"]
        attempt = 0
        while not self._stop:
            attempt += 1
            _log("RECONNECT", f"Attempt {attempt} in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_d)
            await self.client.close()
            self.client = DerivClient(self.cfg)
            try:
                if not await self.client.connect():
                    continue
                if not await self.client.subscribe_ticks():
                    continue
                if self.waiting_for_result and self.current_contract:
                    cid  = self.current_contract["id"]
                    _log("RECONNECT", f"Re-attaching to {cid}")
                    data = await self.client.poll_contract(cid)
                    if data:
                        await self.handle_settlement(data)
                    await self.client.send({"proposal_open_contract": 1,
                                            "contract_id": cid, "subscribe": 1})
                _log("RECONNECT", "OK")
                return True
            except Exception as exc:
                _log("RECONNECT", f"Error: {exc}")
        return False

    # ── Main run loop ────────────────────────────────────────────────────────

    async def run(self):
        cfg = self.cfg
        print("\n" + "="*58)
        print("  DERIV EXPIRYRANGE BOT — Python Edition")
        print("="*58)
        print(f"  Symbol   : {cfg['symbol']}")
        print(f"  Contract : EXPIRYRANGE  (Ends In)")
        print(f"  Duration : {cfg['duration']}{cfg['duration_unit']}  (2 minutes)")
        print(f"  Barriers : ±{cfg['barrier']}")
        print(f"  Stake    : ${cfg['initial_stake']:.2f} "
              f"(x{cfg['martingale_mul']} mart, reset @{cfg['max_losses']} losses)")
        print(f"  Target   : +${cfg['target_profit']}  "
              f"Stop: -${cfg['stop_loss']}")
        print(f"  Min votes: {cfg['min_votes']}/6 layers")
        print(f"  Warmup   : {cfg['min_ticks']} ticks")
        print("="*58)
        print("  Signal layers:")
        print("    0  ATR regime       (LOW vol → trade)")
        print("    1  Bollinger width  (narrow → trade)")
        print("    2  RSI gate         (35-65 → trade)")
        print("    3  EMA distance     (near mid → trade)")
        print("    4  Candle body      (small → trade)")
        print("    5  Tick streak      (oscillating → trade)")
        print("="*58 + "\n")

        if cfg["api_token"] in ("REPLACE_WITH_YOUR_TOKEN", ""):
            _log("ERROR", "Set DERIV_API_TOKEN env var before running")
            return

        if not await self.client.connect():
            return
        if not await self.client.subscribe_ticks():
            return

        _log("BOT", f"Live — warming up ({cfg['min_ticks']} ticks needed)...")

        console_task = asyncio.create_task(self._console(), name="console")

        try:
            while not self._stop:
                response = await self.client.receive(timeout=60)

                if "__disconnect__" in response:
                    _log("WS", "Disconnected — reconnecting")
                    if not await self._reconnect():
                        break
                    continue

                if not response:
                    try:
                        await self.client.ws.ping()
                    except Exception:
                        _log("WS", "Ping failed — reconnecting")
                        if not await self._reconnect():
                            break
                    continue

                if "tick" in response:
                    await self.on_tick(response["tick"])

                if "proposal_open_contract" in response:
                    result = await self.handle_settlement(
                        response["proposal_open_contract"])
                    if result is False:
                        break

                if "buy" in response:
                    result = await self.handle_settlement(response["buy"])
                    if result is False:
                        break

                if "transaction" in response:
                    tx = response["transaction"]
                    if "contract_id" in tx:
                        result = await self.handle_settlement({
                            "contract_id": tx.get("contract_id"),
                            "profit":      tx.get("profit", 0),
                            "status":      tx.get("action", ""),
                            "is_settled":  True,
                        })
                        if result is False:
                            break

                if "profit_table" in response and self.current_contract:
                    for tx in response["profit_table"].get("transactions", []):
                        if tx.get("contract_id") == self.current_contract["id"]:
                            result = await self.handle_settlement({
                                "contract_id": tx["contract_id"],
                                "profit": (float(tx.get("sell_price", 0))
                                           - float(tx.get("buy_price", 0))),
                                "status":     "sold",
                                "is_settled": True,
                            })
                            if result is False:
                                break

        except KeyboardInterrupt:
            print("\n\nInterrupted")
        except Exception as exc:
            print(f"\nUnhandled error: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            console_task.cancel()
            await self.client.close()
            print("\nFINAL STATS")
            self.risk._print_stats()
            print(f"  Ticks processed: {self.tick_count}")
            print("Goodbye")


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main():
    bot = ExpiryRangeBot(CONFIG)
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
