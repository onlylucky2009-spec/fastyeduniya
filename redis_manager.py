import redis
import os
import time

# --- REDIS CONNECTION SETUP ---
# Heroku provides REDIS_URL environment variable automatically.
# decode_responses=True ensures we get Python strings instead of bytes.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
r = redis.from_url(REDIS_URL, decode_responses=True)

# --- LUA SCRIPT FOR ATOMIC TRADE LIMITING ---
# This script runs inside Redis to ensure 'Check-then-Increment' is atomic.
# This prevents race conditions where multiple signals hit at the same millisecond.
LUA_TRADE_LIMIT_CHECK = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local current = redis.call('get', key)

if current and tonumber(current) >= limit then
    return 0 -- Limit reached: Reject trade
else
    local new_val = redis.call('incr', key)
    -- If it's a new key, set expiry to 24 hours (86400 seconds)
    if tonumber(new_val) == 1 then
        redis.call('expire', key, 86400)
    end
    return 1 -- Success: Allow trade
end
"""

class TradeControl:
    @staticmethod
    def can_trade(strategy_side: str, limit: int):
        """
        Executes the Lua script to check daily limits atomically.
        """
        # Key format example: limit:bull:2025-12-22
        date_str = time.strftime("%Y-%m-%d")
        key = f"limit:{strategy_side}:{date_str}"
        
        try:
            # Register the script and execute
            lua_script = r.register_script(LUA_TRADE_LIMIT_CHECK)
            result = lua_script(keys=[key], args=[limit])
            return bool(result)
        except Exception as e:
            # If Redis connection fails, we block trade for safety
            print(f"Redis Error in can_trade: {e}")
            return False

    @staticmethod
    def get_current_count(strategy_side: str):
        """Returns the current number of trades taken today for a side."""
        date_str = time.strftime("%Y-%m-%d")
        key = f"limit:{strategy_side}:{date_str}"
        val = r.get(key)
        return int(val) if val else 0

    # --- CONFIG PERSISTENCE METHODS ---
    # These functions ensure your API Keys and Access Tokens survive Heroku restarts.

    @staticmethod
    def save_config(api_key, api_secret):
        """Saves API Credentials to Redis."""
        try:
            r.set("nexus:api_key", api_key)
            r.set("nexus:api_secret", api_secret)
            return True
        except Exception as e:
            print(f"Failed to save config to Redis: {e}")
            return False

    @staticmethod
    def get_config():
        """Retrieves API Credentials from Redis. Used on App Startup."""
        try:
            api_key = r.get("nexus:api_key")
            api_secret = r.get("nexus:api_secret")
            return api_key, api_secret
        except Exception as e:
            print(f"Failed to fetch config from Redis: {e}")
            return None, None

    @staticmethod
    def save_access_token(token):
        """Saves the Zerodha Access Token with a 24-hour expiry."""
        try:
            # Token usually expires at market close, but 24h is safe
            r.set("nexus:access_token", token, ex=86400)
            return True
        except Exception as e:
            print(f"Failed to save token to Redis: {e}")
            return False

    @staticmethod
    def get_access_token():
        """Retrieves the active Zerodha Access Token from Redis."""
        try:
            return r.get("nexus:access_token")
        except Exception as e:
            print(f"Failed to fetch token from Redis: {e}")
            return None