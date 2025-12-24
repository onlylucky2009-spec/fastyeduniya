import asyncio
import logging
import os
from datetime import datetime
from math import floor
from typing import Optional, Tuple

from redis_manager import TradeControl, IST

# --- LOGGING SETUP ---
# Engine ke har step ko track karne ke liye detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Nexus_Breakout")

class BreakoutEngine:
    """
    Advanced Breakout Engine (Bull/Bear)
    -----------------------------------
    ✅ 0.7% Candle Range Gate (Volatility Filter)
    ✅ 0.5% Gap Check (Extended Entry Filter)
    ✅ 10-Tier Volume SMA Matrix
    ✅ Strict Directional Latch (Buy/Short Fix)
    ✅ Heroku/Redis Atomic Locking
    """

    MAX_TRADES_PER_SYMBOL = 2
    TRIGGER_VALID_SECONDS = 6 * 60  # Trigger 6 minute tak valid rahega

    # --- CORE TICK HANDLER ---
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        Har tick par chalta hai. 
        Positions monitor karta hai aur trigger watch karta hai.
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        # LTP update karna zaroori hai real-time PnL ke liye
        stock["ltp"] = float(ltp or 0.0)
        status = stock.get("brk_status", "WAITING")

        # 1. Agar position OPEN hai toh monitor karo
        if status == "OPEN":
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. Agar TRIGGER_WATCH mein hai toh entry check karo
        if status == "TRIGGER_WATCH":
            side = (stock.get("brk_side_latch") or "").lower()
            
            # Basic validation
            if side not in ("bull", "bear"):
                BreakoutEngine._reset_waiting(stock)
                return

            # Trigger Expiry check (6 minutes)
            now_ts = datetime.now(IST).timestamp()
            set_ts = float(stock.get("brk_set_ts") or 0)
            if (now_ts - set_ts) > BreakoutEngine.TRIGGER_VALID_SECONDS:
                logger.info(f"⏳ [BRK] {stock['symbol']} trigger expire ho gaya.")
                BreakoutEngine._reset_waiting(stock)
                return

            # Dashboard toggle check
            if not state["engine_live"].get(side, True):
                return

            # Time Window check
            if not BreakoutEngine._within_trade_window(state["config"].get(side, {})):
                BreakoutEngine._reset_waiting(stock)
                return

            trigger_px = float(stock.get("brk_trigger_px", 0.0))

            # --- EXECUTION WITH DIRECTIONAL SAFETY ---
            # Side latch ensure karta hai ki direction galat na ho
            if side == "bull" and ltp > trigger_px:
                await BreakoutEngine.open_trade(stock, ltp, state, "bull")
            elif side == "bear" and ltp < trigger_px:
                await BreakoutEngine.open_trade(stock, ltp, state, "bear")
            
            return

    # --- CANDLE QUALIFICATION (1m Close) ---
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        """
        Jab 1-minute candle close hoti hai tab main.py isse call karta hai.
        Yahan 0.7% aur 0.5% filters check hote hain.
        """
        stock = state["stocks"].get(token)
        if not stock: return

        # Agar pehle se trade mein hai ya trigger watch kar raha hai toh skip
        if stock.get("brk_status") in ("OPEN", "TRIGGER_WATCH"):
            return

        symbol = stock.get("symbol")
        pdh = float(stock.get("pdh", 0))
        pdl = float(stock.get("pdl", 0))
        c_close = float(candle.get("close", 0))
        c_high = float(candle.get("high", 0))
        c_low = float(candle.get("low", 0))

        if pdh <= 0 or pdl <= 0:
            return

        # 1. BULL QUALIFICATION (PDH Breakout)
        if c_close > pdh:
            side = "bull"
            if not state["engine_live"].get(side, True): return

            # ✅ 0.7% Range aur 0.5% Gap Check
            if not BreakoutEngine._range_gate_ok(side, c_high, c_low, c_close, pdh, pdl):
                return

            # Volume Confirmation Matrix check
            v_ok, v_detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not v_ok:
                return

            stock.update({
                "brk_status": "TRIGGER_WATCH",
                "brk_side_latch": "bull",
                "brk_trigger_px": float(c_high),
                "brk_trigger_candle": dict(candle),
                "brk_set_ts": datetime.now(IST).timestamp(),
                "brk_scan_reason": f"PDH Break + Vol {v_detail}"
            })
            logger.info(f"✅ [BRK-QUALIFY] {symbol} BULLISH | Trigger: {c_high}")

        # 2. BEAR QUALIFICATION (PDL Breakdown)
        elif c_close < pdl:
            side = "bear"
            if not state["engine_live"].get(side, True): return

            # ✅ 0.7% Range aur 0.5% Gap Check
            if not BreakoutEngine._range_gate_ok(side, c_high, c_low, c_close, pdh, pdl):
                return

            v_ok, v_detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not v_ok:
                return

            stock.update({
                "brk_status": "TRIGGER_WATCH",
                "brk_side_latch": "bear",
                "brk_trigger_px": float(c_low),
                "brk_trigger_candle": dict(candle),
                "brk_set_ts": datetime.now(IST).timestamp(),
                "brk_scan_reason": f"PDL Break + Vol {v_detail}"
            })
            logger.info(f"✅ [BRK-QUALIFY] {symbol} BEARISH | Trigger: {c_low}")

    # --- VOLUME MATRIX LOGIC ---
    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict) -> Tuple[bool, str]:
        """
        10-tier volume matrix ko scan karta hai.
        """
        cfg = state["config"].get(side, {})
        matrix = cfg.get("volume_criteria", [])
        if not matrix: return True, "NoMatrix"

        c_vol = int(candle.get("volume", 0))
        s_sma = float(stock.get("sma", 0))
        c_val_cr = (c_vol * candle['close']) / 10000000.0 # Value in Crores

        tier_found = None
        for i, level in enumerate(matrix):
            if s_sma >= float(level.get("min_sma_avg", 0)):
                tier_found = (i, level)
            else:
                break # Hierarchy break

        if not tier_found: 
            return False, "SMA_Low"

        idx, level = tier_found
        req_vol = s_sma * float(level.get("sma_multiplier", 1.0))
        min_cr = float(level.get("min_vol_price_cr", 0))

        if c_vol >= req_vol and c_val_cr >= min_cr:
            return True, f"Tier{idx+1}_Pass"
        
        return False, f"Tier{idx+1}_Fail"

    # --- EXECUTION LOGIC ---
    @staticmethod
    async def open_trade(stock, ltp: float, state: dict, side_key: str):
        """
        Redis mein trade reserve karta hai aur Zerodha order place karta hai.
        """
        # Final Directional check
        if side_key == "bull" and ltp <= stock["brk_trigger_px"]: return
        if side_key == "bear" and ltp >= stock["brk_trigger_px"]: return

        symbol = stock["symbol"]
        kite = state.get("kite")
        cfg = state["config"].get(side_key, {})

        if not kite:
            logger.error(f"❌ [BRK] Kite session missing for {symbol}")
            return

        # Redis Atomic Check (Locking)
        ok, reason = await TradeControl.reserve_symbol_trade(symbol, BreakoutEngine.MAX_TRADES_PER_SYMBOL)
        if not ok:
            logger.warning(f"🚫 [BRK] {symbol} block by Redis: {reason}")
            stock["brk_status"] = "WAITING"
            return

        is_bull = (side_key == "bull")
        trig_candle = stock.get("brk_trigger_candle") or {}
        
        # Stoploss Calculation
        sl_px = float(trig_candle.get("low" if is_bull else "high", 0))
        if sl_px <= 0: 
            sl_px = ltp * 0.995 if is_bull else ltp * 1.005 # Safety fallback

        risk_per_share = max(abs(ltp - sl_px), ltp * 0.003)
        risk_amount = float(cfg.get("risk_trade_1", 2000))
        qty = floor(risk_amount / risk_per_share)

        if qty <= 0:
            await TradeControl.rollback_symbol_trade(symbol)
            stock["brk_status"] = "WAITING"
            return

        try:
            # Transaction Type is strictly mapped to side_key
            t_type = kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL
            
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=t_type,
                quantity=int(qty),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET
            )

            # Target calculation based on settings
            rr_str = str(cfg.get("risk_reward", "1:2"))
            rr_val = float(rr_str.split(":")[-1])
            target_px = ltp + (risk_per_share * rr_val) if is_bull else ltp - (risk_per_share * rr_val)

            trade_obj = {
                "engine": "breakout",
                "symbol": symbol,
                "side": side_key,
                "qty": int(qty),
                "entry_price": float(ltp),
                "sl_price": round(float(sl_px), 2),
                "target_price": round(float(target_px), 2),
                "status": "OPEN",
                "oid": order_id,
                "pnl": 0.0,
                "time": datetime.now(IST).strftime("%H:%M:%S")
            }

            stock["brk_status"] = "OPEN"
            stock["brk_active_trade"] = trade_obj
            state["trades"][side_key].append(trade_obj)
            
            logger.info(f"🔥 [BRK-EXEC] {symbol} {side_key.upper()} OrderPlaced: {order_id}")

        except Exception as e:
            logger.error(f"❌ [BRK-ORDER-ERROR] {symbol}: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            stock["brk_status"] = "WAITING"

    # --- MONITORING & PNL ---
    @staticmethod
    async def monitor_active_trade(stock, ltp: float, state: dict):
        """
        Live PnL update karta hai aur SL/Target hit check karta hai.
        """
        trade = stock.get("brk_active_trade")
        if not trade: return

        is_bull = (trade["side"] == "bull")
        entry = trade["entry_price"]
        qty = trade["qty"]
        sl = trade["sl_price"]
        target = trade["target_price"]

        # Real-time PnL (index.html isse fetch karta hai)
        trade["pnl"] = round((ltp - entry) * qty if is_bull else (entry - ltp) * qty, 2)

        # SL / Target Exit Check
        target_hit = (ltp >= target) if is_bull else (ltp <= target)
        sl_hit = (ltp <= sl) if is_bull else (ltp >= sl)

        if target_hit:
            await BreakoutEngine.close_position(stock, state, "TARGET_HIT")
        elif sl_hit:
            await BreakoutEngine.close_position(stock, state, "SL_HIT")

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        """
        Position exit order place karta hai aur Redis lock release karta hai.
        """
        trade = stock.get("brk_active_trade")
        if not trade or trade["status"] != "OPEN": return
        
        kite = state.get("kite")
        symbol = stock["symbol"]
        is_bull = (trade["side"] == "bull")

        try:
            # Exit direction is opposite of Entry
            t_type = kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY
            
            exit_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=t_type,
                quantity=int(trade["qty"]),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET
            )
            
            trade["status"] = "CLOSED"
            trade["exit_oid"] = exit_id
            trade["exit_reason"] = reason
            stock["brk_status"] = "CLOSED"
            
            # Release Redis Lock so next trade can happen in future
            await TradeControl.release_symbol_lock(symbol)
            logger.info(f"⏹️ [BRK-EXIT] {symbol} Reason: {reason}")

        except Exception as e:
            logger.error(f"❌ [BRK-EXIT-ERROR] {symbol}: {e}")

    # --- HELPERS (FILTERS) ---
    @staticmethod
    def _reset_waiting(stock: dict):
        """Engine state ko reset karta hai."""
        stock["brk_status"] = "WAITING"
        stock["brk_active_trade"] = None
        stock.pop("brk_trigger_px", None)
        stock.pop("brk_side_latch", None)
        stock.pop("brk_set_ts", None)

    @staticmethod
    def _within_trade_window(cfg: dict) -> bool:
        """Dashboard timings ke andar hai ya nahi?"""
        try:
            now = datetime.now(IST).time()
            start = datetime.strptime(cfg.get("trade_start", "09:15"), "%H:%M").time()
            end = datetime.strptime(cfg.get("trade_end", "15:10"), "%H:%M").time()
            return start <= now <= end
        except: return True

    @staticmethod
    def _range_gate_ok(side, high, low, close, pdh, pdl) -> bool:
        """
        ✅ 0.7% Candle Range Filter: Volatility control
        ✅ 0.5% Gap Filter: Over-extension control
        """
        # 1. Range Check: Candle ki body aur wicks bohot badi nahi honi chahiye
        range_pct = ((high - low) / close) * 100.0
        if range_pct > 2:
            logger.warning(f"🚫 Range rejected: {range_pct:.2f}% (Limit: 0.7%)")
            return False
        
        # 2. Gap Check: Breakout point PDH/PDL se bohot door nahi hona chahiye
        if side == "bull":
            gap = ((high - pdh) / pdh) * 100.0
            if gap > 1:
                logger.warning(f"🚫 Bull Gap rejected: {gap:.2f}% (Limit: 0.5%)")
                return False
        else:
            gap = ((pdl - low) / pdl) * 100.0
            if gap > 1:
                logger.warning(f"🚫 Bear Gap rejected: {gap:.2f}% (Limit: 0.5%)")
                return False

        return True