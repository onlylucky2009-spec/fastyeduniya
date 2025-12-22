import asyncio
import os
import logging
from typing import Dict, List
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Optimization for speed
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from kiteconnect import KiteConnect, KiteTicker
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine
from redis_manager import TradeControl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Nexus_Main")

# Initializing FastAPI with strict_slashes=False to prevent "Not Found" errors
app = FastAPI(strict_slashes=False)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

RAM_STATE = {
    "kite": None, "kws": None,
    "api_key": os.getenv("KITE_API_KEY", ""),
    "api_secret": os.getenv("KITE_API_SECRET", ""),
    "access_token": "",
    "stocks": {}, 
    "stocks_by_symbol": {},
    "trades": {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "config": {
        side: {
            "volume_criteria": [{"min_vol_price_cr": 0, "sma_multiplier": 1.0, "min_sma_avg": 0} for _ in range(10)],
            "total_trades": 5, "risk_trade_1": 2000, "risk_reward": "1:2", "trailing_sl": "1:1.5"
        } for side in ["bull", "bear", "mom_bull", "mom_bear"]
    },
    "engine_live": {side: False for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "pnl": {"total": 0.0, "bull": 0.0, "bear": 0.0, "mom_bull": 0.0, "mom_bear": 0.0},
    "data_connected": {"breakout": False, "momentum": False},
    "manual_exits": set(), "banned": set()
}

# --- TICK HANDLER ---
def on_ticks(ws, ticks):
    for tick in ticks:
        token = tick['instrument_token']
        if token in RAM_STATE["stocks"]:
            ltp = tick['last_price']
            vol = tick.get('volume_traded', 0)
            RAM_STATE["stocks"][token]['ltp'] = ltp
            BreakoutEngine.run(token, ltp, vol, RAM_STATE)
            MomentumEngine.run(token, ltp, vol, RAM_STATE)

# --- ROUTES ---

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    with open("index.html", "r") as f: return f.read()

@app.get("/api/kite/login")
@app.get("/api/kite/login/")
async def kite_login_redirect():
    # Priority 1: Check RAM_STATE (from Dashboard UI)
    # Priority 2: Check OS Environment (from Heroku Config)
    api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
    if not api_key: 
        return {"status": "error", "message": "API Key not found. Save in API Gateway first."}
    
    kite = KiteConnect(api_key=api_key)
    return RedirectResponse(url=kite.login_url())

@app.get("/login")
async def kite_callback(request_token: str = None):
    if not request_token:
        return {"status": "error", "message": "No request token provided by Zerodha"}
    
    try:
        api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
        api_secret = RAM_STATE["api_secret"] or os.getenv("KITE_API_SECRET")
        
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        
        RAM_STATE["access_token"] = data["access_token"]
        RAM_STATE["kite"] = kite
        RAM_STATE["kite"].set_access_token(RAM_STATE["access_token"])
        
        # Start WebSocket
        RAM_STATE["kws"] = KiteTicker(api_key, RAM_STATE["access_token"])
        RAM_STATE["kws"].on_ticks = on_ticks
        RAM_STATE["kws"].connect(threaded=True)
        
        return HTMLResponse("<h2>Authenticated!</h2><script>setTimeout(()=>window.close(),2000)</script>")
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stats")
@app.get("/api/stats/")
async def get_stats():
    return {
        "pnl": RAM_STATE["pnl"],
        "data_connected": RAM_STATE["data_connected"],
        "engine_status": {k: ("1" if v else "0") for k, v in RAM_STATE["engine_live"].items()}
    }

@app.get("/api/orders")
@app.get("/api/orders/")
async def get_orders():
    return RAM_STATE["trades"]

@app.get("/api/scanner")
@app.get("/api/scanner/")
async def get_scanner():
    signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
    for s in RAM_STATE["stocks"].values():
        if s.get('status') == 'TRIGGER_WATCH':
            side = s.get('side_latch', '').lower()
            if side in signals:
                signals[side].append({"symbol": s['symbol'], "price": s['trigger_px']})
    return signals

@app.post("/api/control")
@app.post("/api/control/")
async def control_center(data: dict):
    action = data.get("action")
    if action == "save_api":
        RAM_STATE["api_key"], RAM_STATE["api_secret"] = data["api_key"], data["api_secret"]
    elif action == "toggle_engine":
        RAM_STATE["engine_live"][data['side']] = data['enabled']
    elif action == "manual_exit":
        RAM_STATE["manual_exits"].add(data['symbol'])
    return {"status": "ok"}

@app.get("/api/settings/engine/{side}")
@app.get("/api/settings/engine/{side}/")
async def get_engine_params(side: str):
    return RAM_STATE["config"].get(side, {})

@app.post("/api/settings/engine/{side}")
@app.post("/api/settings/engine/{side}/")
async def save_engine_params(side: str, data: dict):
    RAM_STATE["config"][side].update(data)
    return {"status": "success"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)