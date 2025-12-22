import asyncio
import os
import logging
from datetime import datetime, timedelta
import pytz
from kiteconnect import KiteConnect
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [ASYNC-SYNC] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Sync_Command")
IST = pytz.timezone("Asia/Kolkata")

# --- STOCK UNIVERSE ---
# Yahan apni 500+ stocks ki list paste karein
STOCK_INDEX_MAPPING = {
'MINDTECK': 'NIFTY 500',
'NITIRAJ': 'NIFTY 500',
} 

async def fetch_and_sync_stock(kite, t_id, symbol, semaphore):
    """
    Ek stock ka historical data fetch karke SMA calculate karta hai.
    Formula: (Sum of last 5 days Volume) / 1875
    """
    async with semaphore:
        try:
            to_date = datetime.now(IST)
            from_date = to_date - timedelta(days=10) # 10 days for safety (weekends)
            
            # Historical data blocking call ko thread mein run karein
            records = await asyncio.to_thread(
                kite.historical_data, t_id, from_date, to_date, "day"
            )
            
            if len(records) >= 5:
                # Last 5 completed days (today's live candle exclude karne ke liye -6:-1)
                last_5_days = records[-6:-1]
                total_vol = sum(day['volume'] for day in last_5_days)
                
                # Formula: Sum / 1875 (Average Volume Per Minute over 5 days)
                avg_vol_per_minute = total_vol / 1875
                
                market_data = {
                    "sma": round(avg_vol_per_minute, 2),
                    "pdh": last_5_days[-1]['high'],
                    "pdl": last_5_days[-1]['low'],
                    "sync_time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Redis mein save karein (Async call)
                await TradeControl.save_market_data(str(t_id), market_data)
                return True
        except Exception as e:
            logger.debug(f"Error syncing {symbol}: {e}")
        
        # Rate limit respect (Zerodha allow 3 requests/sec)
        await asyncio.sleep(0.34)
        return False

async def run_sync():
    logger.info("🚀 Starting Superfast Async Market Data Sync...")
    
    # 1. Redis se config uthayein
    api_key, api_secret = await TradeControl.get_config()
    access_token = await TradeControl.get_access_token()
    
    if not api_key or not access_token:
        logger.error("❌ Sync Aborted: API Key ya Access Token Redis mein nahi mila.")
        return

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        
        logger.info("Fetching NSE instruments...")
        instruments = await asyncio.to_thread(kite.instruments, "NSE")
        
        target_stocks = []
        for instr in instruments:
            symbol = instr['tradingsymbol']
            if symbol in STOCK_INDEX_MAPPING:
                target_stocks.append((instr['instrument_token'], symbol))

        logger.info(f"Targeting {len(target_stocks)} stocks for Volume SMA (5-Day Strategy).")
        
        # 2. Concurrency Control (Max 3 parallel requests for Zerodha)
        sem = asyncio.Semaphore(3)
        tasks = [fetch_and_sync_stock(kite, t_id, sym, sem) for t_id, sym in target_stocks]
        
        results = await asyncio.gather(*tasks)
        
        # 3. Finalize
        await TradeControl.set_last_sync()
        success_count = sum(1 for r in results if r)
        logger.info(f"✅ SUCCESS: {success_count} stocks synced to Redis.")
        
    except Exception as e:
        logger.error(f"Critical Sync Failure: {e}")

if __name__ == "__main__":
    asyncio.run(run_sync())