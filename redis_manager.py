# import redis.asyncio as redis
# import os
# import time
# import json
# import logging
# import asyncio
# from typing import Tuple, Optional, Dict

# # --- LOGGING SETUP ---
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("Nexus_Redis")

# # --- CONNECTION CONFIGURATION ---
# REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# # Heroku Redis uses self-signed certificates. 
# # We configure the pool to handle 'rediss://' (secure) and 'redis://' (local).
# connection_kwargs = {
#     "decode_responses": True,
# }

# if REDIS_URL.startswith("rediss://"):
#     connection_kwargs["ssl_cert_reqs"] = None  # Mandatory for Heroku Redis SSL

# # Create a persistent Async Connection Pool
# pool = redis.ConnectionPool.from_url(REDIS_URL, **connection_kwargs)
# r = redis.Redis(connection_pool=pool)

# # --- ATOMIC LUA SCRIPT ---
# # This script ensures that checking the limit and incrementing the count 
# # happens as a single atomic transaction inside Redis.
# LUA_TRADE_LIMIT_CHECK = """
# local key = KEYS[1]
# local limit = tonumber(ARGV[1])
# local current = redis.call('get', key)

# if current and tonumber(current) >= limit then
#     return 0 -- Limit reached: REJECT
# else
#     local new_val = redis.call('incr', key)
#     if tonumber(new_val) == 1 then
#         redis.call('expire', key, 86400) -- Set 24h expiry on first trade of the day
#     end
#     return 1 -- Success: ALLOW
# end
# """

# class TradeControl:
    
#     # --- 1. ATOMIC TRADE LIMITING ---

#     @staticmethod
#     async def can_trade(strategy_side: str, limit: int) -> bool:
#         """
#         Checks daily limit and increments counter in one atomic step.
#         """
#         try:
#             date_str = time.strftime("%Y-%m-%d")
#             key = f"limit:{strategy_side}:{date_str}"
            
#             # Register and execute the Lua script asynchronously
#             script = r.register_script(LUA_TRADE_LIMIT_CHECK)
#             result = await script(keys=[key], args=[limit])
            
#             allow = bool(result)
#             if not allow:
#                 logger.warning(f"🚫 Trade Limit Reached for {strategy_side.upper()} ({limit})")
#             return allow
#         except Exception as e:
#             logger.error(f"Redis can_trade Error: {e}")
#             return False

#     @staticmethod
#     async def get_current_count(strategy_side: str) -> int:
#         """Fetches current trade count for the day."""
#         try:
#             date_str = time.strftime("%Y-%m-%d")
#             key = f"limit:{strategy_side}:{date_str}"
#             val = await r.get(key)
#             return int(val) if val else 0
#         except:
#             return 0

#     # --- 2. CONFIG PERSISTENCE (API KEYS) ---

#     @staticmethod
#     async def save_config(api_key: str, api_secret: str):
#         """Persists API credentials across Heroku restarts."""
#         try:
#             # Using asyncio.gather to write both keys concurrently
#             await asyncio.gather(
#                 r.set("nexus:api_key", api_key),
#                 r.set("nexus:api_secret", api_secret)
#             )
#             logger.info("✅ API Config saved to Redis.")
#         except Exception as e:
#             logger.error(f"Failed to save config: {e}")

#     @staticmethod
#     async def get_config() -> Tuple[Optional[str], Optional[str]]:
#         """Retrieves API credentials on startup."""
#         try:
#             res = await asyncio.gather(r.get("nexus:api_key"), r.get("nexus:api_secret"))
#             return res[0], res[1]
#         except Exception as e:
#             logger.error(f"Failed to get config: {e}")
#             return None, None

#     # --- 3. SESSION PERSISTENCE (ACCESS TOKENS) ---

#     @staticmethod
#     async def save_access_token(token: str):
#         """Saves the 24-hour Kite access token."""
#         try:
#             await r.set("nexus:access_token", token, ex=86400)
#             logger.info("✅ Access Token persisted for 24 hours.")
#         except Exception as e:
#             logger.error(f"Failed to save token: {e}")

#     @staticmethod
#     async def get_access_token() -> Optional[str]:
#         """Retrieves active session token."""
#         try:
#             return await r.get("nexus:access_token")
#         except:
#             return None

#     # --- 4. MARKET DATA PERSISTENCE (SMA/PDH CACHE) ---

#     @staticmethod
#     async def save_market_data(token_id: str, data: dict):
#         """Caches calculated SMA/PDH so we don't have to call Kite API on every restart."""
#         try:
#             await r.hset("nexus:market_cache", token_id, json.dumps(data))
#         except Exception as e:
#             logger.error(f"Failed to cache market data: {e}")

#     @staticmethod
#     async def get_all_market_data() -> Dict[str, dict]:
#         """Loads all stock parameters in one go on app boot."""
#         try:
#             cached = await r.hgetall("nexus:market_cache")
#             return {k: json.loads(v) for k, v in cached.items()}
#         except Exception as e:
#             logger.error(f"Failed to fetch market cache: {e}")
#             return {}

#     @staticmethod
#     async def set_last_sync():
#         """Marks that the morning sync command has finished."""
#         await r.set("nexus:sync_date", time.strftime("%Y-%m-%d"))

#     @staticmethod
#     async def get_last_sync_date() -> Optional[str]:
#         """Checks if we need to run a fresh morning sync."""
#         return await r.get("nexus:sync_date")

import redis.asyncio as redis
import os
import time
import json
import logging
import asyncio
from typing import Tuple, Optional, Dict

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Nexus_Redis")

# --- CONNECTION CONFIGURATION ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

connection_kwargs = {
    "decode_responses": True,
}

if REDIS_URL.startswith("rediss://"):
    connection_kwargs["ssl_cert_reqs"] = None 

# Create a persistent Async Connection Pool
pool = redis.ConnectionPool.from_url(REDIS_URL, **connection_kwargs)
r = redis.Redis(connection_pool=pool)

LUA_TRADE_LIMIT_CHECK = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local current = redis.call('get', key)

if current and tonumber(current) >= limit then
    return 0 
else
    local new_val = redis.call('incr', key)
    if tonumber(new_val) == 1 then
        redis.call('expire', key, 86400) 
    end
    return 1 
end
"""

class TradeControl:
    
    @staticmethod
    async def can_trade(strategy_side: str, limit: int) -> bool:
        try:
            date_str = time.strftime("%Y-%m-%d")
            key = f"limit:{strategy_side}:{date_str}"
            script = r.register_script(LUA_TRADE_LIMIT_CHECK)
            result = await script(keys=[key], args=[limit])
            return bool(result)
        except Exception as e:
            logger.error(f"Redis can_trade Error: {e}")
            return False

    @staticmethod
    async def get_current_count(strategy_side: str) -> int:
        try:
            date_str = time.strftime("%Y-%m-%d")
            key = f"limit:{strategy_side}:{date_str}"
            val = await r.get(key)
            return int(val) if val else 0
        except:
            return 0

    @staticmethod
    async def save_config(api_key: str, api_secret: str):
        try:
            # Ensure we are saving strings, not None or objects
            await asyncio.gather(
                r.set("nexus:api_key", str(api_key or "")),
                r.set("nexus:api_secret", str(api_secret or ""))
            )
            logger.info("✅ API Config saved to Redis.")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")

    @staticmethod
    async def get_config() -> Tuple[str, str]:
        """
        Retrieves API credentials. 
        FIX: Returns empty strings instead of None to prevent .encode() errors.
        """
        try:
            res = await asyncio.gather(r.get("nexus:api_key"), r.get("nexus:api_secret"))
            key = res[0] if res[0] is not None else ""
            secret = res[1] if res[1] is not None else ""
            return str(key), str(secret)
        except Exception as e:
            logger.error(f"Failed to get config: {e}")
            return "", ""

    @staticmethod
    async def save_access_token(token: str):
        try:
            await r.set("nexus:access_token", str(token), ex=86400)
            logger.info("✅ Access Token persisted.")
        except Exception as e:
            logger.error(f"Failed to save token: {e}")

    @staticmethod
    async def get_access_token() -> str:
        """FIX: Returns empty string instead of None."""
        try:
            val = await r.get("nexus:access_token")
            return str(val) if val is not None else ""
        except:
            return ""

    @staticmethod
    async def save_market_data(token_id: str, data: dict):
        try:
            await r.hset("nexus:market_cache", token_id, json.dumps(data))
        except Exception as e:
            logger.error(f"Failed to cache market data: {e}")

    @staticmethod
    async def get_all_market_data() -> Dict[str, dict]:
        try:
            cached = await r.hgetall("nexus:market_cache")
            return {k: json.loads(v) for k, v in cached.items()}
        except Exception as e:
            logger.error(f"Failed to fetch market cache: {e}")
            return {}

    @staticmethod
    async def set_last_sync():
        await r.set("nexus:sync_date", time.strftime("%Y-%m-%d"))

    @staticmethod
    async def get_last_sync_date() -> str:
        val = await r.get("nexus:sync_date")
        return str(val) if val else ""