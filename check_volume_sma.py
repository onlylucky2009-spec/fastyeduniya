import redis
import json
import os

# --- Configuration ---
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
HASH_KEY = 'nexus:market_cache'
SMA_THRESHOLD = 100

def filter_high_volume_stocks():
    """
    Iterates through the Redis Hash 'nexus:market_cache' and 
    filters stocks where SMA > 1000.
    """
    try:
        # SSL fix for Heroku production environments
        r = redis.from_url(
            REDIS_URL, 
            decode_responses=True, 
            ssl_cert_reqs=None
        )
        
        # Verify connection
        r.ping()
        
        count = 0
        total_stocks = 0
        high_sma_stocks = []
        
        print(f"Connected to Redis. Analyzing Hash: {HASH_KEY}")
        print(f"Threshold: SMA > {SMA_THRESHOLD}")
        print("-" * 50)

        # Use hscan_iter to safely iterate over the Hash without blocking Redis
        # It yields (field, value) pairs
        for token, data_raw in r.hscan_iter(HASH_KEY):
            total_stocks += 1
            
            try:
                data = json.loads(data_raw)
                sma_value = data.get('sma', 0)
                symbol = data.get('symbol', 'Unknown')
                
                if sma_value > SMA_THRESHOLD:
                    count += 1
                    high_sma_stocks.append({
                        "symbol": symbol,
                        "sma": sma_value,
                        "token": token
                    })
                    
            except (json.JSONDecodeError, TypeError):
                continue

        # Output formatting
        if high_sma_stocks:
            # Sort by SMA value descending
            high_sma_stocks.sort(key=lambda x: x['sma'], reverse=True)
            
            print(f"{'SYMBOL':<15} | {'SMA':<12} | {'TOKEN':<12}")
            print("-" * 50)
            for stock in high_sma_stocks:
                print(f"{stock['symbol']:<15} | {stock['sma']:<12.2f} | {stock['token']:<12}")
        
        print("-" * 50)
        print(f"Scan Summary:")
        print(f"Total Stocks in Cache: {total_stocks}")
        print(f"Stocks with SMA > 1000: {count}")
        print("-" * 50)

        return count

    except redis.ConnectionError as e:
        print(f"Error: Could not connect to Redis. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    filter_high_volume_stocks()