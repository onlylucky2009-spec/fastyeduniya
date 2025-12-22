import logging
from datetime import datetime, time as dt_time
from math import floor
import pytz

logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

class MomentumEngine:
    @staticmethod
    def run(token, ltp, vol, state):
        stock = state["stocks"].get(token)
        if not stock or stock['symbol'] in state["banned"]: return
        now = datetime.now(IST).time()

        if stock['status'] == 'OPEN_MOM':
            MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        if dt_time(9, 15) <= now < dt_time(9, 16):
            stock['hi'] = max(stock['hi'], ltp) if stock['hi'] > 0 else ltp
            stock['lo'] = min(stock['lo'], ltp) if stock['lo'] > 0 else ltp
            if not stock.get('candle_915'): stock['candle_915'] = {'volume': 0, 'close': ltp}
            stock['candle_915']['close'] = ltp
            if stock['last_vol'] > 0: stock['candle_915']['volume'] += max(0, vol - stock['last_vol'])
            stock['last_vol'] = vol
            return

        if stock['status'] == 'WAITING' and now >= dt_time(9, 16):
            if state["engine_live"]["mom_bull"] and ltp > (stock['hi'] * 1.0001):
                if MomentumEngine.is_vol_qualified(stock, 'mom_bull', state):
                    MomentumEngine.open_trade("mom_bull", stock, ltp, state)
            elif state["engine_live"]["mom_bear"] and ltp < (stock['lo'] * 0.9999):
                if MomentumEngine.is_vol_qualified(stock, 'mom_bear', state):
                    MomentumEngine.open_trade("mom_bear", stock, ltp, state)
        stock['last_vol'] = vol

    @staticmethod
    def is_vol_qualified(stock, side, state):
        c = stock.get('candle_915')
        if not c: return False
        matrix = state["config"][side].get('volume_criteria', [])
        if not matrix: return True
        c_vol, c_val_cr, s_sma = c['volume'], (c['volume'] * c['close']) / 10000000.0, stock.get('sma', 0)
        for level in matrix:
            try:
                if s_sma >= float(level.get('min_sma_avg', 0)) and c_vol >= (s_sma * float(level.get('sma_multiplier', 1))) and c_val_cr >= float(level.get('min_vol_price_cr', 0)):
                    return True
            except: continue
        return False

    @staticmethod
    def open_trade(side, stock, price, state):
        cfg = state["config"][side]
        sl_pct = float(cfg.get('stop_loss_pct', 0.5)) / 100.0
        risk_amt = float(cfg.get('risk_per_trade', 2000))
        qty = max(1, int(floor(risk_amt / (price * sl_pct))))
        rr = float(str(cfg.get('risk_reward', '1:2')).split(':')[1])
        state["trades"][side].append({
            "symbol": stock['symbol'], "entry_price": price, "ltp": price,
            "sl_price": price * (1 - sl_pct) if "bull" in side else price * (1 + sl_pct),
            "target_price": price * (1 + (sl_pct * rr)) if "bull" in side else price * (1 - (sl_pct * rr)),
            "qty": qty, "side": "BUY" if "bull" in side else "SELL", "pnl": 0.0
        })
        stock['status'] = 'OPEN_MOM'

    @staticmethod
    def monitor_active_trade(stock, ltp, state):
        for side in ["mom_bull", "mom_bear"]:
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