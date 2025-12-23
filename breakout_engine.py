import logging
import asyncio
from datetime import datetime
from math import floor
import pytz
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")

class BreakoutEngine:
    
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        Entry point for every price update.
        Handles state transitions: WAITING -> TRIGGER_WATCH -> OPEN -> CLOSED
        """
        stock = state["stocks"].get(token)
        if not stock: return

        # 1. MONITOR ACTIVE TRADES
        if stock['status'] == 'OPEN':
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. TRIGGER WATCH (Price confirmation)
        if stock['status'] == 'TRIGGER_WATCH':
            if stock['side_latch'] == 'BULL' and ltp >= stock['trigger_px']:
                logger.info(f"⚡ [TRIGGER] {stock['symbol']} hit {ltp} (Trigger: {stock['trigger_px']})")
                await BreakoutEngine.open_trade(token, stock, ltp, state)
            return

        # 3. 1-MINUTE CANDLE FORMATION
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock['candle'] and stock['candle']['bucket'] != bucket:
            # Bucket changed: Analyze previous minute
            asyncio.create_task(BreakoutEngine.analyze_candle_logic(token, stock['candle'], state))
            # Start new bucket
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        elif not stock['candle']:
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        else:
            c = stock['candle']
            c['high'] = max(c['high'], ltp)
            c['low'] = min(c['low'], ltp)
            c['close'] = ltp
            if stock['last_vol'] > 0:
                c['volume'] += max(0, vol - stock['last_vol'])
        
        stock['last_vol'] = vol

    @staticmethod
    async def analyze_candle_logic(token: int, candle: dict, state: dict):
        """Logic triggered at every minute close."""
        stock = state["stocks"][token]
        pdh = stock.get('pdh', 0)
        
        # Guard: Check if PDH exists and Bull Engine is ON
        if not state["engine_live"].get("bull") or pdh <= 0:
            return

        # LOGIC: Current Candle crossed PDH from below
        if candle['open'] < pdh and candle['close'] > pdh:
            logger.info(f"🔍 [SCAN] {stock['symbol']} Crossed PDH ({pdh}). Checking Vol Matrix...")
            
            is_qualified, detail = await BreakoutEngine.check_vol_matrix(stock, candle, 'bull', state)
            
            if is_qualified:
                logger.info(f"✅ [QUALIFIED] {stock['symbol']} | {detail}")
                stock['status'] = 'TRIGGER_WATCH'
                stock['side_latch'] = 'BULL'
                stock['trigger_px'] = round(candle['high'] * 1.0005, 2)
            else:
                logger.info(f"❌ [REJECTED] {stock['symbol']} | {detail}")

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        """Validates volume matrix received from Frontend."""
        matrix = state["config"][side].get('volume_criteria', [])
        c_vol = candle['volume']
        s_sma = stock.get('sma', 0) 
        c_val_cr = (c_vol * candle['close']) / 10000000.0

        if not matrix: return True, "Empty Matrix"

        tier_found = None
        for i, level in enumerate(matrix):
            if s_sma >= float(level.get('min_sma_avg', 0)):
                tier_found = (i, level)
            else: break

        if tier_found:
            idx, level = tier_found
            multiplier = float(level.get('sma_multiplier', 1.0))
            min_cr = float(level.get('min_vol_price_cr', 0))
            required_vol = s_sma * multiplier
            
            if c_vol >= required_vol and c_val_cr >= min_cr:
                return True, f"T{idx+1} Pass: {c_vol:,.0f} > {required_vol:,.0f}"
            return False, f"T{idx+1} Fail (Vol or Cr)"
        
        return False, f"SMA {s_sma:,.0f} too low"

    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict):
        """Places a real BUY order in Zerodha."""
        side = stock['side_latch'].lower()
        cfg = state["config"][side]
        kite = state.get("kite")

        if not kite:
            logger.error(f"❌ [AUTH ERROR] Kite instance missing for {stock['symbol']}")
            return

        # 1. Atomic Trade Limit Check
        daily_limit = int(cfg.get('total_trades', 5))
        if not await TradeControl.can_trade(side, daily_limit):
            logger.warning(f"🚫 [LIMIT] {stock['symbol']}: Daily limit reached.")
            stock['status'] = 'WAITING'
            return

        # 2. Risk & Qty Calculation
        sl_px = stock['candle']['low']
        risk_per_share = max(abs(ltp - sl_px), ltp * 0.002) # Min 0.2% risk floor
        total_risk_allowed = float(cfg.get('risk_trade_1', 2000))
        qty = floor(total_risk_allowed / risk_per_share)
        
        if qty <= 0:
            logger.error(f"⚠️ [SIZE ERROR] {stock['symbol']} Risk too high for risk limit.")
            stock['status'] = 'WAITING'
            return

        try:
            # 3. EXECUTE REAL ORDER (Regular, NSE, MIS, Market Buy)
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=stock['symbol'],
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=qty,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET
            )

            # 4. Success logic
            rr_val = float(cfg.get('risk_reward', "1:2").split(':')[-1])
            trade = {
                "symbol": stock['symbol'],
                "qty": qty,
                "entry_price": ltp,
                "sl_price": sl_px,
                "target_price": round(ltp + (risk_per_share * rr_val), 2),
                "order_id": order_id,
                "pnl": 0.0,
                "entry_time": datetime.now(IST).strftime("%H:%M:%S")
            }
            
            state["trades"][side].append(trade)
            stock['status'] = 'OPEN'
            stock['active_trade'] = trade
            logger.info(f"🚀 [REAL ENTRY] {stock['symbol']} Qty: {qty} | OrderID: {order_id}")

        except Exception as e:
            logger.error(f"❌ [KITE ERROR] Failed to place entry for {stock['symbol']}: {e}")
            stock['status'] = 'WAITING'

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        """Tracks live position for exit signals."""
        trade = stock.get('active_trade')
        if not trade: return
        
        side = stock['side_latch'].lower()
        cfg = state["config"][side]

        # Update PnL
        trade['pnl'] = round((ltp - trade['entry_price']) * trade['qty'], 2)

        # EXIT CHECK: Target
        if ltp >= trade['target_price']:
            logger.info(f"🎯 [TARGET] {stock['symbol']} reached {trade['target_price']}")
            await BreakoutEngine.close_position(stock, state, "TARGET")

        # EXIT CHECK: Stop Loss
        elif ltp <= trade['sl_price']:
            logger.info(f"🛑 [STOPLOSS] {stock['symbol']} hit {trade['sl_price']}")
            await BreakoutEngine.close_position(stock, state, "SL")

        # EXIT CHECK: Trailing SL
        else:
            tsl_ratio = float(cfg.get('trailing_sl', "1:1.5").split(':')[-1])
            new_sl = await BreakoutEngine.calculate_tsl(trade, ltp, tsl_ratio)
            if new_sl > trade['sl_price']:
                trade['sl_price'] = new_sl

        # EXIT CHECK: Manual exit via Dashboard
        if stock['symbol'] in state['manual_exits']:
            logger.info(f"🖱️ [MANUAL EXIT] {stock['symbol']}")
            await BreakoutEngine.close_position(stock, state, "MANUAL")
            state['manual_exits'].remove(stock['symbol'])

    @staticmethod
    async def calculate_tsl(trade: dict, ltp: float, ratio: float):
        entry = trade['entry_price']
        risk = entry - trade['sl_price']
        profit = ltp - entry
        if profit > (risk * ratio):
            return round(ltp - (risk * 0.8), 2)
        return trade['sl_price']

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        """Places a real SELL order to exit the position."""
        trade = stock.get('active_trade')
        kite = state.get("kite")
        
        if trade and kite:
            try:
                exit_id = await asyncio.to_thread(
                    kite.place_order,
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=stock['symbol'],
                    transaction_type=kite.TRANSACTION_TYPE_SELL,
                    quantity=trade['qty'],
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET
                )
                logger.info(f"🏁 [REAL EXIT] {stock['symbol']} Reason: {reason} | OrderID: {exit_id}")
            except Exception as e:
                logger.error(f"❌ [KITE EXIT ERROR] {stock['symbol']}: {e}")

        # Cleanup RAM state
        stock['status'] = 'WAITING'
        stock['active_trade'] = None