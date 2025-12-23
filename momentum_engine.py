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
        Aggregates ticks into 1-minute candles and monitors active momentum trades.
        """
        stock = state["stocks"].get(token)
        if not stock: return

        # 1. MONITOR ACTIVE MOMENTUM TRADES (PnL & Exits)
        if stock['status'] == 'MOM_OPEN':
            await MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. TRIGGER WATCH (Wait for price to breach the momentum candle trigger)
        if stock['status'] == 'MOM_TRIGGER_WATCH':
            # Bullish Momentum Trigger (Breach High)
            if stock['side_latch'] == 'MOM_BULL' and ltp >= stock['trigger_px']:
                logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} Bullish trigger hit @ {ltp}")
                await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bull')
            
            # Bearish Momentum Trigger (Breach Low)
            elif stock['side_latch'] == 'MOM_BEAR' and ltp <= stock['trigger_px']:
                logger.info(f"⚡ [MOM-TRIGGER] {stock['symbol']} Bearish trigger hit @ {ltp}")
                await MomentumEngine.open_trade(token, stock, ltp, state, 'mom_bear')
            return

        # 3. 1-MINUTE ASYNC CANDLE FORMATION
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock['candle'] and stock['candle']['bucket'] != bucket:
            # Candle closed: Process logic in background
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
            # Cumulative Volume Delta
            if stock['last_vol'] > 0:
                c['volume'] += max(0, vol - stock['last_vol'])
        
        stock['last_vol'] = vol

    @staticmethod
    async def analyze_momentum_logic(token: int, candle: dict, state: dict):
        """Checks for price velocity and volume surges against Dashboard settings."""
        stock = state["stocks"][token]
        symbol = stock['symbol']
        body_size = abs(candle['close'] - candle['open'])
        body_pct = (body_size / candle['open']) * 100 if candle['open'] > 0 else 0

        # --- BULLISH MOMENTUM ---
        if state["engine_live"].get("mom_bull") and candle['close'] > candle['open']:
            if body_pct > 0.25: # Requirement: Body > 0.25%
                is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bull', state)
                if is_qualified:
                    logger.info(f"✅ [MOM-QUALIFIED] {symbol} BULL Surge | Body: {body_pct:.2f}% | {detail}")
                    stock['status'] = 'MOM_TRIGGER_WATCH'
                    stock['side_latch'] = 'MOM_BULL'
                    stock['trigger_px'] = round(candle['high'] + (body_size * 0.1), 2)
                else:
                    logger.info(f"❌ [MOM-REJECT] {symbol} BULL | {detail}")

        # --- BEARISH MOMENTUM ---
        elif state["engine_live"].get("mom_bear") and candle['close'] < candle['open']:
            if body_pct > 0.25:
                is_qualified, detail = await MomentumEngine.check_vol_matrix(stock, candle, 'mom_bear', state)
                if is_qualified:
                    logger.info(f"✅ [MOM-QUALIFIED] {symbol} BEAR Crash | Body: {body_pct:.2f}% | {detail}")
                    stock['status'] = 'MOM_TRIGGER_WATCH'
                    stock['side_latch'] = 'MOM_BEAR'
                    stock['trigger_px'] = round(candle['low'] - (body_size * 0.1), 2)
                else:
                    logger.info(f"❌ [MOM-REJECT] {symbol} BEAR | {detail}")

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        """Asynchronous Volume tier check."""
        matrix = state["config"][side].get('volume_criteria', [])
        c_vol = candle['volume']
        s_sma = stock.get('sma', 0)
        c_val_cr = (c_vol * candle['close']) / 10000000.0

        if not matrix: return True, "No Matrix"

        tier_found = None
        for i, level in enumerate(matrix):
            if s_sma >= float(level.get('min_sma_avg', 0)): tier_found = (i, level)
            else: break

        if tier_found:
            idx, level = tier_found
            required_vol = s_sma * float(level.get('sma_multiplier', 1.0))
            min_cr = float(level.get('min_vol_price_cr', 0))
            if c_vol >= required_vol and c_val_cr >= min_cr:
                return True, f"Tier {idx+1} Pass"
            return False, f"Tier {idx+1} Fail (Vol/Value)"
        
        return False, f"SMA {s_sma:,.0f} too low"

    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict, side_key: str):
        """Places real BUY or SELL market orders in Zerodha."""
        cfg = state["config"][side_key]
        kite = state.get("kite")
        
        if not kite:
            logger.error(f"❌ [KITE ERROR] Session missing for {stock['symbol']}")
            return

        # 1. Trade Limit Check
        if not await TradeControl.can_trade(side_key, int(cfg.get('total_trades', 5))):
            logger.warning(f"🚫 [LIMIT] {stock['symbol']} limit reached for {side_key}")
            stock['status'] = 'WAITING'
            return

        # 2. Risk & Position Sizing
        is_bull = 'bull' in side_key
        sl_px = stock['candle']['low'] if is_bull else stock['candle']['high']
        risk_per_share = max(abs(ltp - sl_px), ltp * 0.005) # Min 0.5% risk floor
        
        risk_amount = float(cfg.get('risk_trade_1', 2000))
        qty = floor(risk_amount / risk_per_share)
        
        if qty <= 0:
            stock['status'] = 'WAITING'
            return

        try:
            # 3. EXECUTE REAL ORDER
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=stock['symbol'],
                transaction_type=kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL,
                quantity=qty,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET
            )

            # 4. Save to Live State
            rr_val = float(cfg.get('risk_reward', "1:2").split(':')[-1])
            trade = {
                "symbol": stock['symbol'],
                "qty": qty,
                "entry_price": ltp,
                "sl_price": sl_px,
                "target_price": round(ltp + (risk_per_share * rr_val) if is_bull else ltp - (risk_per_share * rr_val), 2),
                "order_id": order_id,
                "pnl": 0.0,
                "entry_time": datetime.now(IST).strftime("%H:%M:%S")
            }
            
            state["trades"][side_key].append(trade)
            stock['status'] = 'MOM_OPEN'
            stock['active_trade'] = trade
            logger.info(f"🚀 [MOM REAL ENTRY] {stock['symbol']} | Qty: {qty} | OrderID: {order_id}")

        except Exception as e:
            logger.error(f"❌ [KITE ORDER ERROR] {stock['symbol']}: {e}")
            stock['status'] = 'WAITING'

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        """Real-time monitoring of open momentum positions."""
        trade = stock.get('active_trade')
        if not trade: return
        
        side_key = stock['side_latch'].lower()
        cfg = state["config"][side_key]
        is_bull = 'bull' in side_key

        # Live PnL Update
        if is_bull:
            trade['pnl'] = round((ltp - trade['entry_price']) * trade['qty'], 2)
            target_hit = ltp >= trade['target_price']
            sl_hit = ltp <= trade['sl_price']
        else:
            trade['pnl'] = round((trade['entry_price'] - ltp) * trade['qty'], 2)
            target_hit = ltp <= trade['target_price']
            sl_hit = ltp >= trade['sl_price']

        # EXIT SIGNALS
        if target_hit:
            logger.info(f"🎯 [MOM-TARGET] {stock['symbol']} hit target {trade['target_price']}")
            await MomentumEngine.close_position(stock, state, "TARGET")
        
        elif sl_hit:
            logger.info(f"🛑 [MOM-STOPLOSS] {stock['symbol']} hit SL {trade['sl_price']}")
            await MomentumEngine.close_position(stock, state, "SL")
        
        # Trailing SL
        else:
            tsl_ratio = float(cfg.get('trailing_sl', "1:1.5").split(':')[-1])
            new_sl = await MomentumEngine.calculate_tsl(trade, ltp, tsl_ratio, is_bull)
            if is_bull and new_sl > trade['sl_price']: trade['sl_price'] = new_sl
            elif not is_bull and new_sl < trade['sl_price']: trade['sl_price'] = new_sl

        # Manual Dashboard Exit
        if stock['symbol'] in state['manual_exits']:
            logger.info(f"🖱️ [MOM-MANUAL EXIT] {stock['symbol']}")
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
        """Places real market order to exit the momentum position."""
        trade = stock.get('active_trade')
        kite = state.get("kite")
        is_bull = 'bull' in stock['side_latch'].lower()
        
        if trade and kite:
            try:
                # Place Exit Order (Opposite of entry)
                exit_id = await asyncio.to_thread(
                    kite.place_order,
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=stock['symbol'],
                    transaction_type=kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY,
                    quantity=trade['qty'],
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET
                )
                logger.info(f"🏁 [MOM REAL EXIT] {stock['symbol']} Reason: {reason} | OrderID: {exit_id}")
            except Exception as e:
                logger.error(f"❌ [KITE MOM EXIT ERROR] {stock['symbol']}: {e}")

        # Finalize State
        stock['status'] = 'WAITING'
        stock['active_trade'] = None