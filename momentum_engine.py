import logging
import asyncio
from datetime import datetime
from math import floor
import pytz
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

class MomentumEngine:
    
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """
        Main entry point for Momentum processing.
        """
        stock = state["stocks"].get(token)
        if not stock:
            return

        # 1. MONITOR ACTIVE MOMENTUM TRADES
        if stock['status'] == 'MOM_OPEN':
            await MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. TRIGGER WATCH
        if stock['status'] == 'MOM_TRIGGER_WATCH':
            if stock['side_latch'] == 'MOM_BULL' and ltp >= stock['trigger_px']:
                logger.info(f"🔥 [MOM-TRIGGER] {stock['symbol']} hit Bullish trigger {stock['trigger_px']}")
                await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bull')
            elif stock['side_latch'] == 'MOM_BEAR' and ltp <= stock['trigger_px']:
                logger.info(f"❄️ [MOM-TRIGGER] {stock['symbol']} hit Bearish trigger {stock['trigger_px']}")
                await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bear')
            return

        # 3. ASYNC CANDLE FORMATION
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock['candle'] and stock['candle']['bucket'] != bucket:
            # Process closed candle
            asyncio.create_task(MomentumEngine.analyze_momentum_logic(token, stock['candle'], state))
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
    async def analyze_momentum_logic(token: int, candle: dict, state: dict):
        """
        Checks velocity/momentum against Dashboard settings.
        """
        stock = state["stocks"][token]
        symbol = stock['symbol']
        body_size = abs(candle['close'] - candle['open'])
        body_pct = (body_size / candle['open']) * 100 if candle['open'] > 0 else 0

        # --- BULLISH MOMENTUM ---
        if state["engine_live"].get("mom_bull") and candle['close'] > candle['open']:
            if body_pct > 0.25:
                logger.info(f"⚡ [MOM-SCAN] {symbol}: Bullish Body {body_pct:.2f}% detected. Checking Volume...")
                is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bull', state)
                if is_qualified:
                    logger.info(f"✅ [MOM-QUALIFIED] {symbol}: {detail}")
                    stock['status'] = 'MOM_TRIGGER_WATCH'
                    stock['side_latch'] = 'MOM_BULL'
                    stock['trigger_px'] = round(candle['high'] + (body_size * 0.1), 2)
                else:
                    logger.info(f"❌ [MOM-REJECT] {symbol}: {detail}")
            else:
                # Failing particular condition: Body Size
                pass 

        # --- BEARISH MOMENTUM ---
        elif state["engine_live"].get("mom_bear") and candle['close'] < candle['open']:
            if body_pct > 0.25:
                logger.info(f"🔻 [MOM-SCAN] {symbol}: Bearish Body {body_pct:.2f}% detected. Checking Volume...")
                is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bear', state)
                if is_qualified:
                    logger.info(f"✅ [MOM-QUALIFIED] {symbol}: {detail}")
                    stock['status'] = 'MOM_TRIGGER_WATCH'
                    stock['side_latch'] = 'MOM_BEAR'
                    stock['trigger_px'] = round(candle['low'] - (body_size * 0.1), 2)
                else:
                    logger.info(f"❌ [MOM-REJECT] {symbol}: {detail}")

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        """
        Validates Volume against Dashboard Matrix Tiers.
        """
        matrix = state["config"][side].get('volume_criteria', [])
        c_vol = candle['volume']
        s_sma = stock.get('sma', 0)
        c_val_cr = (c_vol * candle['close']) / 10000000.0
        symbol = stock['symbol']

        if not matrix:
            return True, "No Volume Matrix defined (Auto-Pass)"

        tier_found = None
        for i, level in enumerate(matrix):
            min_sma_req = float(level.get('min_sma_avg', 0))
            if s_sma >= min_sma_req:
                tier_found = (i, level)
            else:
                break

        if tier_found:
            idx, level = tier_found
            multiplier = float(level.get('sma_multiplier', 1.0))
            min_cr = float(level.get('min_vol_price_cr', 0))
            target_vol = s_sma * multiplier
            
            if c_vol < target_vol:
                return False, f"Tier {idx+1} Fail: Vol {c_vol:,.0f} < Required {target_vol:,.0f} (SMA {s_sma:,.0f} * {multiplier})"
            
            if c_val_cr < min_cr:
                return False, f"Tier {idx+1} Fail: Value {c_val_cr:.2f}Cr < Min {min_cr}Cr"

            return True, f"Tier {idx+1} Pass: Vol {c_vol:,.0f} > {target_vol:,.0f} | Turnover {c_val_cr:.2f}Cr"
        
        return False, f"Stock SMA {s_sma:,.0f} too low for Matrix Tiers"

    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict, side_key: str):
        """
        Entry with Dashboard Risk Settings.
        """
        cfg = state["config"][side_key]
        symbol = stock['symbol']
        
        # 1. Redis Limit Check
        daily_limit = int(cfg.get('total_trades', 5))
        if not await TradeControl.can_trade(side_key, daily_limit):
            logger.warning(f"🚫 [MOM-LIMIT] {symbol}: Daily limit of {daily_limit} reached for {side_key}.")
            stock['status'] = 'WAITING'
            return

        # 2. Risk & Qty
        candle = stock['candle']
        is_bull = 'bull' in side_key
        sl_px = candle['low'] if is_bull else candle['high']
        risk_per_share = abs(ltp - sl_px)
        
        if risk_per_share <= 0: risk_per_share = ltp * 0.005 

        risk_amount = float(cfg.get('risk_trade_1', 2000))
        qty = floor(risk_amount / risk_per_share)
        
        if qty <= 0:
            logger.error(f"❌ [MOM-SIZE ERROR] {symbol}: Risk/Share {risk_per_share:.2f} too high for ₹{risk_amount} risk.")
            stock['status'] = 'WAITING'
            return

        # 3. Targets
        try:
            rr_str = cfg.get('risk_reward', "1:2")
            rr_mult = float(rr_str.split(':')[-1])
        except:
            rr_mult = 2.0

        target_px = round(ltp + (risk_per_share * rr_mult), 2) if is_bull else round(ltp - (risk_per_share * rr_mult), 2)

        trade = {
            "symbol": symbol,
            "qty": qty,
            "entry_price": ltp,
            "sl_price": sl_px,
            "target_price": target_px,
            "pnl": 0.0,
            "entry_time": datetime.now(IST).strftime("%H:%M:%S")
        }
        
        state["trades"][side_key].append(trade)
        stock['status'] = 'MOM_OPEN'
        stock['active_trade'] = trade
        
        logger.info(f"🚀 [MOM-ENTRY] {symbol} {side_key.upper()} @ {ltp} | Qty: {qty} | SL: {sl_px} | Tgt: {target_px}")

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        """
        Real-time monitoring for Momentum trades.
        """
        trade = stock.get('active_trade')
        if not trade: return
        
        side_key = stock['side_latch'].lower()
        cfg = state["config"][side_key]
        is_bull = 'bull' in side_key

        # Live PnL Calculation
        if is_bull:
            trade['pnl'] = round((ltp - trade['entry_price']) * trade['qty'], 2)
            target_hit = ltp >= trade['target_price']
            sl_hit = ltp <= trade['sl_price']
        else:
            trade['pnl'] = round((trade['entry_price'] - ltp) * trade['qty'], 2)
            target_hit = ltp <= trade['target_price']
            sl_hit = ltp >= trade['sl_price']

        # 1. Target
        if target_hit:
            logger.info(f"🎯 [MOM-TARGET] {stock['symbol']} hit {trade['target_price']}. PnL: +₹{trade['pnl']}")
            await MomentumEngine.close_position(stock, state, "TARGET")

        # 2. Stop Loss
        elif sl_hit:
            logger.info(f"🛑 [MOM-SL] {stock['symbol']} hit {trade['sl_price']}. PnL: ₹{trade['pnl']}")
            await MomentumEngine.close_position(stock, state, "SL")

        # 3. Trailing Stop Loss
        else:
            tsl_cfg = cfg.get('trailing_sl', "1:1.5")
            try:
                tsl_ratio = float(tsl_cfg.split(':')[-1])
                new_sl = await MomentumEngine.calculate_tsl(trade, ltp, tsl_ratio, is_bull)
                if is_bull and new_sl > trade['sl_price']: trade['sl_price'] = new_sl
                elif not is_bull and new_sl < trade['sl_price']: trade['sl_price'] = new_sl
            except: pass

        # 4. Manual Exit
        if stock['symbol'] in state['manual_exits']:
            logger.info(f"🖱️ [MOM-MANUAL] User closed {stock['symbol']} via Dashboard.")
            await MomentumEngine.close_position(stock, state, "MANUAL")
            state['manual_exits'].remove(stock['symbol'])

    @staticmethod
    async def calculate_tsl(trade: dict, ltp: float, ratio: float, is_bull: bool):
        entry, sl = trade['entry_price'], trade['sl_price']
        risk = abs(entry - sl)
        profit = (ltp - entry) if is_bull else (entry - ltp)
        if profit > (risk * ratio):
            return round(ltp - (risk * 0.9), 2) if is_bull else round(ltp + (risk * 0.9), 2)
        return sl

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        symbol = stock['symbol']
        stock['status'] = 'WAITING'
        stock['active_trade'] = None
        logger.info(f"🏁 [MOM-CLOSED] {symbol} | Reason: {reason}")