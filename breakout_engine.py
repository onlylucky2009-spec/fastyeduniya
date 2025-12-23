import logging
import asyncio
from datetime import datetime
from math import floor
import pytz
from redis_manager import TradeControl

# --- LOGGING SETUP ---
# Using a specific logger name to track engine flow in Heroku logs
logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")

class BreakoutEngine:
    
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        Main entry point. Processes every tick.
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        # 1. MONITOR ACTIVE TRADES
        if stock['status'] == 'OPEN':
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. TRIGGER WATCH
        if stock['status'] == 'TRIGGER_WATCH':
            if stock['side_latch'] == 'BULL' and ltp >= stock['trigger_px']:
                logger.info(f"⚡ [TRIGGER] {stock['symbol']} hit trigger price {stock['trigger_px']}. Opening trade...")
                await BreakoutEngine.open_trade(token, stock, ltp, state)
            return

        # 3. 1-MINUTE ASYNC CANDLE FORMATION
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock['candle'] and stock['candle']['bucket'] != bucket:
            # Process the closed candle
            asyncio.create_task(BreakoutEngine.analyze_candle_logic(token, stock['candle'], state))
            # Reset for new minute
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
        """
        Processes closed candle. Checks against Dashboard settings.
        """
        stock = state["stocks"][token]
        pdh = stock.get('pdh', 0)
        symbol = stock['symbol']
        
        # --- COMPATIBILITY & DASHBOARD CHECK ---
        is_bull_live = state["engine_live"].get("bull", False)
        
        if not is_bull_live:
            # Silent return to avoid log spam, engine is simply OFF
            return

        if pdh <= 0:
            logger.warning(f"⚠️ [SKIP] {symbol}: No PDH data found. Cannot check breakout.")
            return

        # LOGIC: Breakout of PDH
        # Open must be below PDH, Close must be above PDH
        if candle['open'] < pdh and candle['close'] > pdh:
            logger.info(f"🔍 [SCAN] {symbol} crossed PDH ({pdh}). Checking Volume Matrix...")
            
            # Check Volume against Frontend Dashboard Settings
            is_qualified, detail = await BreakoutEngine.check_vol_matrix(stock, candle, 'bull', state)
            
            if is_qualified:
                logger.info(f"✅ [QUALIFIED] {symbol}: {detail}")
                stock['status'] = 'TRIGGER_WATCH'
                stock['side_latch'] = 'BULL'
                stock['trigger_px'] = round(candle['high'] * 1.0005, 2)
            else:
                # DETAILED LOGGING: Tells exactly why the stock failed
                logger.info(f"❌ [REJECTED] {symbol}: {detail}")
        
        # Extra debug: If candle closed above PDH but didn't open below it (Gap Up case)
        elif candle['close'] > pdh:
             pass # Logic specifically requires crossing FROM below

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        """
        Validates volume against the 10-tier matrix received from Frontend.
        """
        matrix = state["config"][side].get('volume_criteria', [])
        c_vol = candle['volume']
        s_sma = stock.get('sma', 0) 
        c_val_cr = (c_vol * candle['close']) / 10000000.0 # Turnover in Crores
        symbol = stock['symbol']

        if not matrix:
            return True, "Volume Matrix empty in settings. Auto-passing."

        # Find applicable tier based on Stock's 20-Day SMA
        tier_found = None
        for i, level in enumerate(matrix):
            # Settings received from Frontend Dashboard
            min_sma_required = float(level.get('min_sma_avg', 0))
            
            if s_sma >= min_sma_required:
                tier_found = (i, level)
            else:
                # Matrix is usually sorted; if SMA is lower than this tier, stop searching
                break 

        if tier_found:
            idx, level = tier_found
            multiplier = float(level.get('sma_multiplier', 1.0))
            min_cr = float(level.get('min_vol_price_cr', 0))
            
            # Calculated Requirement
            required_vol = s_sma * multiplier
            
            # Step-by-Step Validation Logs
            if c_vol < required_vol:
                return False, f"Tier {idx+1} Fail: Candle Vol {c_vol:,.0f} < Required {required_vol:,.0f} (SMA {s_sma:,.0f} * Mult {multiplier})"
            
            if c_val_cr < min_cr:
                return False, f"Tier {idx+1} Fail: Turnover {c_val_cr:.2f}Cr < Min Required {min_cr}Cr"

            return True, f"Tier {idx+1} Pass: Vol {c_vol:,.0f} > {required_vol:,.0f} | Turnover {c_val_cr:.2f}Cr"
        
        return False, f"SMA {s_sma:,.0f} is below all defined Matrix Tiers."

    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict):
        """
        Executes trade using Dashboard risk settings.
        """
        side = stock['side_latch'].lower()
        cfg = state["config"][side]
        symbol = stock['symbol']
        
        # 1. Check Redis for daily limit
        daily_limit = int(cfg.get('total_trades', 5))
        if not await TradeControl.can_trade(side, daily_limit):
            logger.warning(f"🚫 [LIMIT] {symbol}: Daily limit of {daily_limit} trades reached for {side}.")
            stock['status'] = 'WAITING'
            return

        # 2. Calculate Risk based on Dashboard Settings
        sl_px = stock['candle']['low']
        risk_per_share = abs(ltp - sl_px)
        
        # Safety: Ensure risk isn't 0
        if risk_per_share <= 0: risk_per_share = ltp * 0.002 

        # Dashboard Setting: Risk per Trade
        risk_amount = float(cfg.get('risk_trade_1', 2000))
        qty = floor(risk_amount / risk_per_share)
        
        if qty <= 0:
            logger.error(f"❌ [SIZE ERROR] {symbol}: Risk/Share {risk_per_share:.2f} too high for ₹{risk_amount} risk limit.")
            stock['status'] = 'WAITING'
            return

        # Dashboard Setting: Risk Reward Ratio
        # e.g., "1:2" -> extracts 2
        try:
            rr_str = cfg.get('risk_reward', "1:2")
            rr_mult = float(rr_str.split(':')[-1])
        except:
            rr_mult = 2.0

        target_px = round(ltp + (risk_per_share * rr_mult), 2)

        # 3. Commit Trade to State
        trade = {
            "symbol": symbol,
            "qty": qty,
            "entry_price": ltp,
            "sl_price": sl_px,
            "target_price": target_px,
            "pnl": 0.0,
            "entry_time": datetime.now(IST).strftime("%H:%M:%S")
        }
        
        state["trades"][side].append(trade)
        stock['status'] = 'OPEN'
        stock['active_trade'] = trade
        
        logger.info(f"🚀 [ENTRY] {symbol} @ {ltp} | Qty: {qty} | SL: {sl_px} | Tgt: {target_px} (RR {rr_mult})")

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        """
        Real-time PnL and Exit monitoring.
        """
        trade = stock.get('active_trade')
        if not trade: return
        
        side = stock['side_latch'].lower()
        cfg = state["config"][side]

        # Update PnL
        trade['pnl'] = round((ltp - trade['entry_price']) * trade['qty'], 2)

        # 1. Target Hit
        if ltp >= trade['target_price']:
            logger.info(f"🎯 [TARGET] {stock['symbol']} hit {trade['target_price']}. PnL: +₹{trade['pnl']}")
            await BreakoutEngine.close_position(stock, state, "TARGET")

        # 2. Stop Loss Hit
        elif ltp <= trade['sl_price']:
            logger.info(f"🛑 [STOPLOSS] {stock['symbol']} hit {trade['sl_price']}. PnL: ₹{trade['pnl']}")
            await BreakoutEngine.close_position(stock, state, "SL")

        # 3. Trailing Stop Loss (TSL) from Dashboard
        else:
            tsl_cfg = cfg.get('trailing_sl', "1:1.5")
            try:
                tsl_ratio = float(tsl_cfg.split(':')[-1])
                new_sl = await BreakoutEngine.calculate_tsl(trade, ltp, tsl_ratio)
                if new_sl > trade['sl_price']:
                    trade['sl_price'] = new_sl
                    # Log TSL move occasionally, not every tick
            except: pass

        # 4. Manual Dashboard Exit
        if stock['symbol'] in state['manual_exits']:
            logger.info(f"🖱️ [MANUAL EXIT] User closed {stock['symbol']} via Dashboard.")
            await BreakoutEngine.close_position(stock, state, "MANUAL")
            state['manual_exits'].remove(stock['symbol'])

    @staticmethod
    async def calculate_tsl(trade: dict, ltp: float, ratio: float):
        entry = trade['entry_price']
        risk = entry - trade['sl_price']
        profit = ltp - entry
        if profit > (risk * ratio):
            return round(ltp - (risk * 0.8), 2) # Trail up but keep some room
        return trade['sl_price']

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        symbol = stock['symbol']
        stock['status'] = 'WAITING'
        stock['active_trade'] = None
        logger.info(f"🏁 [CLOSED] {symbol} | Reason: {reason}")