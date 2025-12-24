# breakout_engine.py
import asyncio
import logging
from datetime import datetime
from math import floor
from typing import Optional, Tuple
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")


class BreakoutEngine:
    """
    Breakout Engine (Bull/Bear) — Parallel-safe

    ✅ Parallel + non-blocking ready:
      - Engine only touches brk_* keys (no collisions with Momentum)
      - run() is tick-fast (no candle aggregation here)
      - on_candle_close() is called by main.py candle_close_worker
      - Per-symbol atomic lock + daily cap via Redis (TradeControl helpers)
      - Side-level trade cap (bull/bear) via Redis reserve_side_trade
      - Rollback-safe reservations (if order fails)
      - Direction safety: bull -> BUY, bear -> SELL (hard-mapped)
      - Trade records never removed; marked CLOSED so PnL stays correct
      - Extensive logs for step-by-step debugging

    Required RedisManager methods (you said you want full Redis fix too):
      - reserve_side_trade(side, limit) -> bool
      - rollback_side_trade(side) -> bool
      - reserve_symbol_trade(symbol, max_trades=2, lock_ttl_sec=...) -> (bool, reason)
      - rollback_symbol_trade(symbol) -> bool
      - release_symbol_lock(symbol) -> bool
      - get_symbol_trade_count(symbol) -> int
    """

    EXIT_BUFFER_PCT = 0.0001
    TRIGGER_VALID_SECONDS = 6 * 60
    MAX_TRADES_PER_SYMBOL = 2

    # -----------------------------
    # TICK FAST-PATH (called each tick)
    # -----------------------------
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        Tick handler
          - Monitors OPEN
          - Monitors TRIGGER_WATCH for entry
          - Does NOT aggregate candles (main.py does that centrally)
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol") or ""
        if not symbol:
            return

        # Update ltp (used in scanner stats)
        stock["ltp"] = float(ltp or 0.0)

        brk_status = (stock.get("brk_status") or "WAITING").upper()

        # 1) Always monitor open trade (even if engine toggle off)
        if brk_status == "OPEN":
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2) Trigger watch -> enter if break happens (TTL + gates)
        if brk_status == "TRIGGER_WATCH":
            side = (stock.get("brk_side_latch") or "").lower()
            if side not in ("bull", "bear"):
                logger.warning(f"[BRK] {symbol} invalid side_latch; resetting.")
                BreakoutEngine._reset_waiting(stock)
                return

            # TTL check
            now_ts = int(datetime.now(IST).timestamp())
            set_ts = int(stock.get("brk_trigger_set_ts") or 0)
            if set_ts and (now_ts - set_ts) > BreakoutEngine.TRIGGER_VALID_SECONDS:
                logger.info(f"⏳ [BRK-EXPIRE] {symbol} {side.upper()} trigger expired (>6m). Reset.")
                BreakoutEngine._reset_waiting(stock)
                return

            # Engine toggle gates new entry only
            if not bool(state["engine_live"].get(side, True)):
                return

            # Trade window gate
            if not BreakoutEngine._within_trade_window(state["config"].get(side, {})):
                logger.info(f"🕒 [BRK-WINDOW] {symbol} {side.upper()} outside trade window; reset.")
                BreakoutEngine._reset_waiting(stock)
                return

            trig = float(stock.get("brk_trigger_px", 0.0) or 0.0)
            if trig <= 0:
                BreakoutEngine._reset_waiting(stock)
                return

            if side == "bull":
                if float(ltp) > trig:
                    logger.info(f"⚡ [BRK-TRIGGER] {symbol} BULL ltp {ltp:.2f} > {trig:.2f}")
                    await BreakoutEngine.open_trade(stock, float(ltp), state, "bull")
            else:
                if float(ltp) < trig:
                    logger.info(f"⚡ [BRK-TRIGGER] {symbol} BEAR ltp {ltp:.2f} < {trig:.2f}")
                    await BreakoutEngine.open_trade(stock, float(ltp), state, "bear")
            return

        # WAITING: nothing to do in tick path
        return

    # -----------------------------
    # CANDLE CLOSE QUALIFICATION (called by main.py)
    # -----------------------------
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        """
        Called once per minute per token by main.py candle_close_worker.
        This qualifies breakout triggers (PDH/PDL breaks + range gate + volume matrix).
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol") or ""
        if not symbol:
            return

        # Do not qualify if breakout already busy
        if (stock.get("brk_status") or "WAITING").upper() in ("OPEN", "TRIGGER_WATCH"):
            return

        # If symbol already exhausted daily cap (Redis)
        try:
            taken = await TradeControl.get_symbol_trade_count(symbol)
            if int(taken) >= BreakoutEngine.MAX_TRADES_PER_SYMBOL:
                return
        except Exception as e:
            logger.warning(f"[BRK] {symbol} trade_count check failed: {e}")

        pdh = float(stock.get("pdh", 0) or 0)
        pdl = float(stock.get("pdl", 0) or 0)
        if pdh <= 0 or pdl <= 0:
            return
        c_open = float(candle.get("open", 0))

        high = float(candle.get("high", 0) or 0)
        low = float(candle.get("low", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_vol = int(candle.get("volume", 0) or 0)
        if close <= 0 or high <= 0 or low <= 0:
            return

        now = datetime.now(IST)

        # --- BULL BREAKOUT ---
        if close > pdh and c_open < pdh:
            side = "bull"

            if not bool(state["engine_live"].get(side, True)):
                logger.debug(f"[BRK] {symbol} bull engine OFF; skip qualify")
                return

            if not BreakoutEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            if not BreakoutEngine._range_gate_ok(side, high, low, close, pdh=pdh, pdl=pdl):
                logger.info(f"❌ [BRK-REJECT] {symbol} BULL RangeGate fail")
                return

            ok, detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                logger.info(f"❌ [BRK-REJECT] {symbol} BULL | {detail}")
                return

            # Set trigger watch
            stock["brk_status"] = "TRIGGER_WATCH"
            stock["brk_side_latch"] = "bull"
            stock["brk_trigger_px"] = float(high)
            stock["brk_trigger_set_ts"] = int(now.timestamp())
            stock["brk_trigger_candle"] = dict(candle)

            # Scanner enrichment (engine-specific)
            stock["brk_scan_vol"] = int(c_vol)
            stock["brk_scan_reason"] = f"PDH break + Vol OK ({detail})"
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None

            logger.info(f"✅ [BRK-QUALIFIED] {symbol} BULL trigger@{high:.2f} {detail}")
            return

        # --- BEAR BREAKDOWN ---
        if close < pdl and c_open>pdl:
            side = "bear"

            if not bool(state["engine_live"].get(side, True)):
                logger.debug(f"[BRK] {symbol} bear engine OFF; skip qualify")
                return

            if not BreakoutEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            if not BreakoutEngine._range_gate_ok(side, high, low, close, pdh=pdh, pdl=pdl):
                logger.info(f"❌ [BRK-REJECT] {symbol} BEAR RangeGate fail")
                return

            ok, detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                logger.info(f"❌ [BRK-REJECT] {symbol} BEAR | {detail}")
                return

            stock["brk_status"] = "TRIGGER_WATCH"
            stock["brk_side_latch"] = "bear"
            stock["brk_trigger_px"] = float(low)
            stock["brk_trigger_set_ts"] = int(now.timestamp())
            stock["brk_trigger_candle"] = dict(candle)

            stock["brk_scan_vol"] = int(c_vol)
            stock["brk_scan_reason"] = f"PDL break + Vol OK ({detail})"
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None

            logger.info(f"✅ [BRK-QUALIFIED] {symbol} BEAR trigger@{low:.2f} {detail}")
            return

    # -----------------------------
    # VOLUME MATRIX
    # -----------------------------
    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict) -> Tuple[bool, str]:
        cfg = state["config"].get(side, {}) or {}
        matrix = cfg.get("volume_criteria", []) or []

        c_vol = int(candle.get("volume", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_val_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

        if not matrix:
            return True, "NoMatrix"

        tier_found = None
        for i, level in enumerate(matrix):
            try:
                min_sma_avg = float(level.get("min_sma_avg", 0) or 0)
            except Exception:
                min_sma_avg = 0.0

            if s_sma >= min_sma_avg:
                tier_found = (i, level)
            else:
                break

        if not tier_found:
            return False, f"SMA {s_sma:,.0f} too low"

        idx, level = tier_found
        required_vol = s_sma * float(level.get("sma_multiplier", 1.0) or 1.0)
        min_cr = float(level.get("min_vol_price_cr", 0) or 0)

        if c_vol >= required_vol and c_val_cr >= min_cr:
            return True, f"Tier{idx+1}Pass"
        return False, f"Tier{idx+1}Fail (Vol/Value)"

    # -----------------------------
    # OPEN TRADE (direction-safe + reservation-safe)
    # -----------------------------
    @staticmethod
    async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
        symbol = stock.get("symbol") or ""
        if not symbol:
            BreakoutEngine._reset_waiting(stock)
            return

        side_key = side_key.lower().strip()
        if side_key not in ("bull", "bear"):
            logger.error(f"[BRK] {symbol} invalid side_key in open_trade: {side_key}")
            BreakoutEngine._reset_waiting(stock)
            return

        cfg = state["config"].get(side_key, {}) or {}
        kite = state.get("kite")
        if not kite:
            logger.error(f"❌ [BRK] {symbol} kite session missing")
            BreakoutEngine._reset_waiting(stock)
            return

        if not bool(state["engine_live"].get(side_key, True)):
            logger.info(f"[BRK] {symbol} {side_key} engine OFF at entry; reset")
            BreakoutEngine._reset_waiting(stock)
            return

        if not BreakoutEngine._within_trade_window(cfg):
            logger.info(f"[BRK] {symbol} {side_key} outside window at entry; reset")
            BreakoutEngine._reset_waiting(stock)
            return

        # ✅ Direction hard-map (prevents wrong BUY/SELL)
        txn_type = kite.TRANSACTION_TYPE_BUY if side_key == "bull" else kite.TRANSACTION_TYPE_SELL

        # -------------------- reservations (atomic) --------------------
        side_limit = int(cfg.get("total_trades", 5) or 5)

        # 1) reserve side trade (bull/bear) with cap
        if not await TradeControl.reserve_side_trade(side_key, side_limit):
            logger.warning(f"🚫 [BRK-LIMIT] {symbol} side limit hit for {side_key}")
            BreakoutEngine._reset_waiting(stock)
            return

        # 2) reserve per-symbol lock + daily count (2/day, 2nd only after close)
        ok, reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=BreakoutEngine.MAX_TRADES_PER_SYMBOL)
        if not ok:
            logger.warning(f"🚫 [BRK-SYMBOL] {symbol} reserve failed: {reason}")
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)
            return

        # SL derived from trigger candle opposite extreme
        trig_candle = stock.get("brk_trigger_candle") or {}
        entry = float(ltp)

        if side_key == "bull":
            sl_px = float(trig_candle.get("low", 0) or 0)
        else:
            sl_px = float(trig_candle.get("high", 0) or 0)

        if sl_px <= 0:
            sl_px = round(entry * (0.995 if side_key == "bull" else 1.005), 2)

        # qty sizing
        risk_per_share = max(abs(entry - sl_px), entry * 0.005)
        risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
        qty = floor(risk_amount / risk_per_share)

        if qty <= 0:
            logger.warning(f"[BRK] {symbol} qty<=0 (risk calc). rollback reservations.")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)
            return

        # Targets / trail
        try:
            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
        except Exception:
            rr_val = 2.0

        if side_key == "bull":
            target = round(entry + (risk_per_share * rr_val), 2)
        else:
            target = round(entry - (risk_per_share * rr_val), 2)

        try:
            tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
        except Exception:
            tsl_ratio = 1.5

        trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

        # -------------------- place order --------------------
        try:
            logger.info(
                f"🧾 [BRK-ORDER] {symbol} {side_key.upper()} "
                f"txn={txn_type} qty={qty} entry={entry:.2f} sl={sl_px:.2f} tgt={target:.2f}"
            )

            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=txn_type,
                quantity=int(qty),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )

            trade = {
                "engine": "breakout",
                "side": side_key,
                "symbol": symbol,
                "qty": int(qty),
                "entry_price": float(entry),
                "sl_price": float(sl_px),
                "target_price": float(target),
                "order_id": order_id,
                "pnl": 0.0,
                "status": "OPEN",
                "entry_time": datetime.now(IST).strftime("%H:%M:%S"),
                "init_risk": float(risk_per_share),
                "trail_step": float(trail_step),
            }

            # Keep trade record forever (do not remove)
            state["trades"][side_key].append(trade)

            stock["brk_status"] = "OPEN"
            stock["brk_active_trade"] = trade
            stock["brk_side_latch"] = side_key

            # clear trigger state so we don't re-enter
            stock.pop("brk_trigger_px", None)
            stock["brk_trigger_set_ts"] = None

            # scanner fields reset
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None

            logger.info(f"🚀 [BRK-ENTRY] {symbol} {side_key.upper()} order={order_id} qty={qty}")

        except Exception as e:
            logger.error(f"❌ [BRK-ORDER-FAIL] {symbol}: {e}")
            # rollback reservations so attempts remain correct
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)

    # -----------------------------
    # MONITOR + EXIT
    # -----------------------------
    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("brk_active_trade")
        if not trade:
            return

        symbol = stock.get("symbol") or ""
        side_key = (stock.get("brk_side_latch") or "").lower()
        if side_key not in ("bull", "bear"):
            logger.warning(f"[BRK] {symbol} open trade invalid side_latch; forcing close")
            await BreakoutEngine.close_position(stock, state, "BAD_SIDE_LATCH")
            return

        is_bull = (side_key == "bull")
        entry = float(trade.get("entry_price", 0) or 0)
        qty = int(trade.get("qty", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)
        target = float(trade.get("target_price", 0) or 0)

        if entry <= 0 or qty <= 0:
            await BreakoutEngine.close_position(stock, state, "BAD_TRADE_STATE")
            return

        # Update PnL (kept for realized+unrealized)
        trade["pnl"] = round(((float(ltp) - entry) * qty) if is_bull else ((entry - float(ltp)) * qty), 2)

        b = BreakoutEngine.EXIT_BUFFER_PCT
        if is_bull:
            target_hit = float(ltp) >= (target * (1.0 - b))
            sl_hit = float(ltp) <= (sl * (1.0 + b))
        else:
            target_hit = float(ltp) <= (target * (1.0 + b))
            sl_hit = float(ltp) >= (sl * (1.0 - b))

        if target_hit:
            logger.info(f"🎯 [BRK-TARGET] {symbol} tgt={target:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await BreakoutEngine.close_position(stock, state, "TARGET")
            return

        if sl_hit:
            logger.info(f"🛑 [BRK-SL] {symbol} sl={sl:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await BreakoutEngine.close_position(stock, state, "SL")
            return

        # Step trailing SL
        new_sl = BreakoutEngine._step_trail_sl(trade, float(ltp), is_bull)
        if new_sl is not None:
            cur = float(trade.get("sl_price", 0) or 0)
            if is_bull and new_sl > cur:
                trade["sl_price"] = float(new_sl)
                logger.debug(f"[BRK-TRAIL] {symbol} SL -> {new_sl:.2f}")
            elif (not is_bull) and new_sl < cur:
                trade["sl_price"] = float(new_sl)
                logger.debug(f"[BRK-TRAIL] {symbol} SL -> {new_sl:.2f}")

        # Manual exit from UI
        if symbol in state.get("manual_exits", set()):
            logger.info(f"🖱️ [BRK-MANUAL] {symbol}")
            await BreakoutEngine.close_position(stock, state, "MANUAL")
            state["manual_exits"].remove(symbol)

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool) -> Optional[float]:
        entry = float(trade.get("entry_price", 0) or 0)
        init_risk = float(trade.get("init_risk", 0) or 0)
        step = float(trade.get("trail_step", 0) or 0)

        if entry <= 0 or init_risk <= 0 or step <= 0:
            return None

        profit = (ltp - entry) if is_bull else (entry - ltp)
        if profit <= 0:
            return None

        k = int(profit // step)
        if k < 1:
            return None

        desired = entry + ((k - 1) * step) if is_bull else entry - ((k - 1) * step)
        return round(desired, 2)

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        trade = stock.get("brk_active_trade")
        kite = state.get("kite")
        symbol = stock.get("symbol") or ""
        side_key = (stock.get("brk_side_latch") or "").lower()

        is_bull = (side_key == "bull")
        txn_type = None
        if kite:
            txn_type = kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY

        if trade and kite and symbol and txn_type:
            try:
                logger.info(f"🏁 [BRK-EXIT] {symbol} reason={reason} qty={trade.get('qty')} txn={txn_type}")
                exit_id = await asyncio.to_thread(
                    kite.place_order,
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=symbol,
                    transaction_type=txn_type,
                    quantity=int(trade["qty"]),
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET,
                )
                trade["exit_order_id"] = exit_id
            except Exception as e:
                logger.error(f"❌ [BRK-EXIT-FAIL] {symbol}: {e}")

        if trade:
            trade["status"] = "CLOSED"
            trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
            trade["exit_reason"] = reason

        # release open lock so second trade can occur
        if symbol:
            try:
                await TradeControl.release_symbol_lock(symbol)
            except Exception as e:
                logger.warning(f"[BRK] {symbol} release lock failed: {e}")

        BreakoutEngine._reset_waiting(stock)

    # -----------------------------
    # HELPERS
    # -----------------------------
    @staticmethod
    def _reset_waiting(stock: dict):
        stock["brk_status"] = "WAITING"
        stock["brk_active_trade"] = None

        stock.pop("brk_trigger_px", None)
        stock.pop("brk_side_latch", None)
        stock["brk_trigger_set_ts"] = None
        stock.pop("brk_trigger_candle", None)

        stock["brk_scan_seen_ts"] = None
        stock["brk_scan_seen_time"] = None
        stock["brk_scan_vol"] = 0
        stock["brk_scan_reason"] = None

    @staticmethod
    def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
        """
        cfg keys (fixed):
          trade_start: "HH:MM"
          trade_end:   "HH:MM"
        """
        from datetime import time as dtime

        try:
            now = now or datetime.now(IST)
            start_s = str(cfg.get("trade_start", "09:15"))
            end_s = str(cfg.get("trade_end", "15:10"))
            sh, sm = map(int, start_s.split(":"))
            eh, em = map(int, end_s.split(":"))
            start_t = dtime(sh, sm)
            end_t = dtime(eh, em)
            nt = now.time()
            return (nt >= start_t) and (nt <= end_t)
        except Exception:
            return True

    @staticmethod
    def _range_gate_ok(side: str, high: float, low: float, close: float, *, pdh: float, pdl: float) -> bool:
        """
        Same as your previous range-gate, but kept deterministic.
        """
        if close <= 0:
            return False

        range_pct = ((high - low) / close) * 100.0
        if range_pct <= 0.7:
            return True

        if side == "bull":
            if pdh <= 0:
                return False
            gap_pct = ((high - pdh) / pdh) * 100.0
            return gap_pct <= 0.5

        if side == "bear":
            if pdl <= 0:
                return False
            gap_pct = ((pdl - low) / pdl) * 100.0
            return gap_pct <= 0.5

        return False
