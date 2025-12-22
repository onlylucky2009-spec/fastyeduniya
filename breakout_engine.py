import logging
from datetime import datetime
from math import floor
import pytz

logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")


class BreakoutEngine:
    @staticmethod
    def run(token, ltp, vol, state):
        stock = state["stocks"].get(token)
        if not stock or stock['symbol'] in state["banned"]: return

        if stock['status'] == 'OPEN':
            BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        if stock['status'] == 'TRIGGER_WATCH':
            if stock['side_latch'] == 'BULL' and ltp > stock['trigger_px']:
                BreakoutEngine.open_trade("bull", stock, ltp, stock['stop_base'], state)
            elif stock['side_latch'] == 'BEAR' and ltp < stock['trigger_px']:
                BreakoutEngine.open_trade("bear", stock, ltp, stock['stop_base'], state)
            return

        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        if stock['candle'] and stock['candle']['bucket'] != bucket:
            BreakoutEngine.analyze_candle_logic(token, stock['candle'], state)
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        elif not stock['candle']:
            stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
        else:
            c = stock['candle']
            c['high'] = max(c['high'], ltp);
            c['low'] = min(c['low'], ltp);
            c['close'] = ltp
            if stock['last_vol'] > 0: c['volume'] += max(0, vol - stock['last_vol'])
        stock['last_vol'] = vol

    @staticmethod
    def analyze_candle_logic(token, candle, state):
        stock = state["stocks"][token]
        if stock['status'] != 'WAITING': return
        c_size = (candle['high'] - candle['low']) / candle['close']

        if state["engine_live"]["bull"] and candle['open'] < stock['pdh'] < candle['close']:
            if BreakoutEngine.is_vol_qualified(stock, candle, 'bull', state):
                if c_size > 0.007:
                    if (candle['high'] - stock['pdh']) / stock['pdh'] > 0.005: return
                    stock['stop_base'] = stock['pdh'] * 0.9998
                else:
                    stock['stop_base'] = candle['low']
                stock['status'], stock['side_latch'], stock['trigger_px'] = 'TRIGGER_WATCH', 'BULL', candle[
                                                                                                         'high'] * 1.0001

    @staticmethod
    def is_vol_qualified(stock, candle, side, state):
        matrix = state["config"][side].get('volume_criteria', [])
        if not matrix: return True
        c_vol, c_val_cr, s_sma = candle['volume'], (candle['volume'] * candle['close']) / 10000000.0, stock['sma']
        for level in matrix:
            try:
                if s_sma >= float(level.get('min_sma_avg', 0)) and c_vol >= (
                        s_sma * float(level.get('sma_multiplier', 1))) and c_val_cr >= float(
                        level.get('min_vol_price_cr', 0)):
                    return True
            except:
                continue
        return False

    @staticmethod
    def open_trade(side, stock, price, stop_base, state):
        cfg = state["config"][side]
        risk_key = f"risk_trade_{min(stock['trades'] + 1, 3)}"
        risk_amt = float(cfg.get(risk_key, 2000))
        risk_per_share = abs(price - stop_base) or price * 0.001
        qty = max(1, int(risk_amt / risk_per_share))
        rr = float(str(cfg.get('risk_reward', '1:2')).split(':')[1])
        tsl = float(str(cfg.get('trailing_sl', '1:1.5')).split(':')[1])
        state["trades"][side].append({
            "symbol": stock['symbol'], "entry_price": price, "ltp": price, "sl_price": stop_base,
            "target_price": price + (risk_per_share * rr) if side == 'bull' else price - (risk_per_share * rr),
            "qty": qty, "side": "BUY" if side == 'bull' else "SELL", "step": risk_per_share * tsl
        })
        stock['status'], stock['trades'] = 'OPEN', stock['trades'] + 1

    @staticmethod
    def monitor_active_trade(stock, ltp, state):
        for side in ["bull", "bear"]:
            for trade in state["trades"][side]:
                if trade['symbol'] == stock['symbol']:
                    trade['ltp'] = ltp
                    is_exit = False
                    if trade['side'] == 'BUY':
                        if ltp >= trade['target_price'] or ltp <= trade['sl_price']: is_exit = True
                    else:
                        if ltp <= trade['target_price'] or ltp >= trade['sl_price']: is_exit = True
                    if stock['symbol'] in state["manual_exits"]: is_exit = True
                    if is_exit:
                        state["trades"][side] = [t for t in state["trades"][side] if t['symbol'] != stock['symbol']]
                        stock['status'] = 'CLOSED'
                        state["manual_exits"].discard(stock['symbol'])
                        return
                    BreakoutEngine.apply_tsl(trade, ltp)

    @staticmethod
    def apply_tsl(trade, ltp):
        p = ltp - trade['entry_price'] if trade['side'] == 'BUY' else trade['entry_price'] - ltp
        if p >= trade['step']:
            new_sl = trade['entry_price'] + ((floor(p / trade['step']) - 1) * trade['step']) if trade[
                                                                                                    'side'] == 'BUY' else \
            trade['entry_price'] - ((floor(p / trade['step']) - 1) * trade['step'])
            if (trade['side'] == 'BUY' and new_sl > trade['sl_price']) or (
                    trade['side'] == 'SELL' and new_sl < trade['sl_price']):
                trade['sl_price'] = round(new_sl * 20) / 20