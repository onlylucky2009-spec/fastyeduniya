import asyncio
import os
import logging
import threading
import json
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional, Set

# Core FastAPI & Server
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Kite Connect SDK
from kiteconnect import KiteConnect, KiteTicker

# --- CRITICAL FIX 1: TWISTED SIGNAL BYPASS (FOR HEROKU) ---
from twisted.internet import reactor
_original_run = reactor.run
def _patched_reactor_run(*args, **kwargs):
    kwargs['installSignalHandlers'] = False
    return _original_run(*args, **kwargs)
reactor.run = _patched_reactor_run

# High-Performance Event Loop
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# Custom Engine Modules & Managers
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Nexus_Async_Core")
IST = pytz.timezone("Asia/Kolkata")

# --- GLOBAL TICK COUNTER FOR HEARTBEAT ---
TICK_STATS = {"received": 0, "last_logged": datetime.now()}

# --- FASTAPI APP ---
app = FastAPI(strict_slashes=False)
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_methods=["*"], 
    allow_headers=["*"]
)

# --- STOCK UNIVERSE ---
STOCK_INDEX_MAPPING = {
    "RELIANCE": "NIFTY 50",
    "TCS": "NIFTY 50",
    "INFY": "NIFTY 50"
} 

# --- CONSOLIDATED RAM STATE ---
RAM_STATE = {
    "main_loop": None,
    "kite": None,
    "kws": None,
    "api_key": "",
    "api_secret": "",
    "access_token": "",
    "stocks": {}, 
    "trades": {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "config": {
        side: {
            "volume_criteria": [{"min_vol_price_cr": 0, "sma_multiplier": 1.0, "min_sma_avg": 0} for _ in range(10)],
            "total_trades": 10, 
            "risk_trade_1": 20, 
            "risk_reward": "1:2", 
            "trailing_sl": "1:1.5"
        } for side in ["bull", "bear", "mom_bull", "mom_bear"]
    },
    "engine_live": {side: False for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "pnl": {"total": 0.0, "bull": 0.0, "bear": 0.0, "mom_bull": 0.0, "mom_bear": 0.0},
    "data_connected": {"breakout": False, "momentum": False},
    "manual_exits": set()
}

# --- ASYNC BRIDGE (THREAD TO UVLOOP) ---
def on_ticks(ws, ticks):
    """
    Receives ticks from Kite's background thread and forwards them to Engines.
    Includes Heartbeat logging to verify data flow.
    """
    global TICK_STATS
    if not RAM_STATE["main_loop"]:
        return

    # Increment global counter
    TICK_STATS["received"] += len(ticks)
    
    # Heartbeat Log: Every 500 ticks
    if TICK_STATS["received"] >= 500:
        logger.info(f"💓 HEARTBEAT: Processed {TICK_STATS['received']} total ticks since last reset. Engines are active.")
        TICK_STATS["received"] = 0

    for tick in ticks:
        token = tick['instrument_token']
        if token in RAM_STATE["stocks"]:
            ltp = tick['last_price']
            vol = tick.get('volume_traded', 0)
            
            # Update LTP in RAM immediately
            RAM_STATE["stocks"][token]['ltp'] = ltp
            
            # Forward to Engines inside the main Async Loop
            try:
                asyncio.run_coroutine_threadsafe(
                    BreakoutEngine.run(token, ltp, vol, RAM_STATE),
                    RAM_STATE["main_loop"]
                )
                asyncio.run_coroutine_threadsafe(
                    MomentumEngine.run(token, ltp, vol, RAM_STATE),
                    RAM_STATE["main_loop"]
                )
            except Exception:
                pass

def on_connect(ws, response):
    logger.info("✅ TICKER: Handshake successful. Subscribing to tokens...")
    tokens = list(RAM_STATE["stocks"].keys())
    if tokens:
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"✅ TICKER: Subscribed to {len(tokens)} stocks.")
    RAM_STATE["data_connected"]["breakout"] = True
    RAM_STATE["data_connected"]["momentum"] = True

def on_error(ws, code, reason):
    logger.error(f"❌ TICKER ERROR: {code} - {reason}")

def on_close(ws, code, reason):
    logger.warning(f"⚠️ TICKER: Connection closed ({code}: {reason})")
    RAM_STATE["data_connected"]["breakout"] = False
    RAM_STATE["data_connected"]["momentum"] = False

# --- SYSTEM LIFECYCLE: STARTUP ---

@app.on_event("startup")
async def startup_event():
    logger.info("--- 🚀 NEXUS ASYNC ENGINE BOOTING ---")
    RAM_STATE["main_loop"] = asyncio.get_running_loop()
    
    # 1. Restore API Credentials from Redis or Heroku Env
    key_redis, sec_redis = await TradeControl.get_config()
    api_key = key_redis or os.getenv("KITE_API_KEY", "")
    api_secret = sec_redis or os.getenv("KITE_API_SECRET", "")
    
    if api_key and api_secret:
        RAM_STATE["api_key"] = str(api_key)
        RAM_STATE["api_secret"] = str(api_secret)
        logger.info(f"🔑 REDIS: API Credentials Restored ({RAM_STATE['api_key'][:4]}***)")

    # 2. Restore Session
    token = await TradeControl.get_access_token()
    if token and RAM_STATE["api_key"]:
        try:
            RAM_STATE["access_token"] = str(token)
            RAM_STATE["kite"] = KiteConnect(api_key=RAM_STATE["api_key"])
            RAM_STATE["kite"].set_access_token(RAM_STATE["access_token"])
            
            # 3. Map Instruments (Run blocking call in executor)
            logger.info("📡 KITE: Mapping NSE Instruments...")
            instruments = await asyncio.to_thread(RAM_STATE["kite"].instruments, "NSE")
            for instr in instruments:
                symbol = instr['tradingsymbol']
                if symbol in STOCK_INDEX_MAPPING:
                    t_id = instr['instrument_token']
                    RAM_STATE["stocks"][t_id] = {
                        'symbol': symbol, 'ltp': 0, 'status': 'WAITING', 'trades': 0,
                        'hi': 0, 'lo': 0, 'pdh': 0, 'pdl': 0, 'sma': 0, 'candle': None, 'last_vol': 0
                    }
            
            # 4. Fast-Boot: Hydrate Stock SMA/PDH from Redis Cache
            cached_data = await TradeControl.get_all_market_data()
            if cached_data:
                logger.info(f"⚡ CACHE: Hydrating {len(cached_data)} stocks from market cache.")
                for t_id_str, data in cached_data.items():
                    t_id = int(t_id_str)
                    if t_id in RAM_STATE["stocks"]:
                        RAM_STATE["stocks"][t_id].update(data)
            
            # 5. Start Ticker
            RAM_STATE["kws"] = KiteTicker(RAM_STATE["api_key"], RAM_STATE["access_token"])
            RAM_STATE["kws"].on_ticks = on_ticks
            RAM_STATE["kws"].on_connect = on_connect
            RAM_STATE["kws"].on_error = on_error
            RAM_STATE["kws"].on_close = on_close
            RAM_STATE["kws"].connect(threaded=True)
            logger.info("🛰️ SYSTEM: Ticker initialized in background thread (Signal-Safe).")

        except Exception as e:
            logger.error(f"❌ STARTUP ERROR: {e}")

# --- WEB & API ENDPOINTS ---

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    try:
        with open("index.html", "r") as f: return f.read()
    except: return "Dashboard index.html not found."

@app.get("/api/stats")
async def get_stats():
    """Provides real-time stats for the Dashboard UI."""
    total_pnl = 0.0
    engine_stats = {}
    for side in ["bull", "bear", "mom_bull", "mom_bear"]:
        side_pnl = sum(t.get('pnl', 0) for t in RAM_STATE["trades"][side])
        engine_stats[side] = side_pnl
        total_pnl += side_pnl
    
    RAM_STATE["pnl"]["total"] = total_pnl
    return {
        "pnl": {**RAM_STATE["pnl"], **engine_stats},
        "data_connected": RAM_STATE["data_connected"],
        "engine_status": {k: ("1" if v else "0") for k, v in RAM_STATE["engine_live"].items()}
    }

@app.get("/api/orders")
async def get_orders(): 
    return RAM_STATE["trades"]

@app.get("/api/scanner")
async def get_scanner():
    signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
    for t_id, s in RAM_STATE["stocks"].items():
        if s.get('status') in ['TRIGGER_WATCH', 'MOM_TRIGGER_WATCH']:
            side = s.get('side_latch', '').lower()
            if side in signals:
                signals[side].append({"symbol": s['symbol'], "price": s.get('trigger_px', 0)})
    return signals

@app.post("/api/control")
async def control_center(data: dict):
    action = data.get("action")
    if action == "save_api":
        key, secret = data.get("api_key"), data.get("api_secret")
        RAM_STATE["api_key"], RAM_STATE["api_secret"] = str(key), str(secret)
        await TradeControl.save_config(key, secret)
        logger.info("💾 CONTROL: API credentials persisted.")
        return {"status": "ok"}
    elif action == "toggle_engine":
        RAM_STATE["engine_live"][data['side']] = data['enabled']
        logger.info(f"⚙️ CONTROL: Engine {data['side']} set to {data['enabled']}")
        return {"status": "ok"}
    elif action == "manual_exit":
        RAM_STATE["manual_exits"].add(data['symbol'])
        return {"status": "ok"}
    return {"status": "error"}

@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    return RAM_STATE["config"].get(side, {})

@app.post("/api/settings/engine/{side}")
async def save_engine_settings(side: str, data: dict):
    if side in RAM_STATE["config"]:
        RAM_STATE["config"][side].update(data)
        logger.info(f"📝 SETTINGS: Volume matrix updated for {side}.")
    return {"status": "success"}

# --- AUTH FLOW ---

@app.get("/api/kite/login")
async def kite_login_redirect():
    api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
    if not api_key: return {"status": "error", "message": "Save API Key first."}
    return RedirectResponse(url=KiteConnect(api_key=api_key).login_url())

@app.get("/login")
async def kite_callback(request_token: str = None):
    if not request_token:
        return RedirectResponse(url="/")
    try:
        api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
        api_secret = RAM_STATE["api_secret"] or os.getenv("KITE_API_SECRET")
        if not api_key or not api_secret:
            k, s = await TradeControl.get_config()
            api_key, api_secret = api_key or k, api_secret or s

        kite = KiteConnect(api_key=str(api_key))
        data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=str(api_secret))
        
        token = data["access_token"]
        await TradeControl.save_access_token(token)
        RAM_STATE["access_token"] = token
        RAM_STATE["api_key"] = str(api_key)
        RAM_STATE["api_secret"] = str(api_secret)
        RAM_STATE["kite"] = kite
        RAM_STATE["kite"].set_access_token(token)
        
        logger.info("🔑 AUTH: Zerodha Session established.")
        return RedirectResponse(url="/")
    except Exception as e:
        logger.error(f"❌ AUTH ERROR: {e}")
        return {"status": "error", "message": str(e)}

# --- SERVER ENTRY POINT ---

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # CRITICAL: Use workers=1 for memory consistency
    uvicorn.run("main:app", host="0.0.0.0", port=port, loop="uvloop", workers=1)