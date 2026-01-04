import requests
import json
import os
from datetime import datetime
from pathlib import Path
import os


def fetch_cryptostocks_data(symbol, asset_type):
    """Fetch data from Alpaca API"""
    base_url_stocks = "https://data.alpaca.markets/v2/stocks/bars"
    base_url_crypto = "https://data.alpaca.markets/v1beta3/crypto/us/bars"

    params = {
        "timeframe": "1Day",
        "symbols": symbol,
        "start": "2015-01-01",
        "limit": 1000,
        "adjustment": "split",
        "feed": "sip",
        "sort": "asc",
    }
    params_crypto = {
        "timeframe": "1Day",
        "symbols": symbol,
        "start": "2015-01-01",
        "limit": 1000,
        "sort": "asc",
    }

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY"),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY")
    }
    
    print(f"Using API Key: {os.getenv('ALPACA_API_KEY')}")
    print(f"Using Secret Key: {os.getenv('ALPACA_SECRET_KEY')}")    
    print(f"Fetching data for {symbol}...")
    print(f"full query URL: {base_url_stocks if asset_type == 'STOCK' else base_url_crypto} with params {params if asset_type == 'STOCK' else params_crypto}")
    response = requests.get(base_url_stocks if asset_type == 'STOCK' else base_url_crypto, params=params if asset_type == 'STOCK' else params_crypto, headers=headers, timeout=30)
    response.raise_for_status()
    
    data = response.json() 
    
    if "Error Message" in data:
        raise ValueError(f"API Error: {data['Error Message']}")
    
    if "Note" in data:
        print(f"⚠️  API Rate Limit warning for {symbol}")
    
    if "next_page_token" in data:
        print(f"ℹ️  Next page token received for {symbol}: {data['next_page_token']}")

        next_page_bars = []
        next_page_token = data["next_page_token"]
        while next_page_token:
            print(f"Fetching next page for {symbol} with token: {next_page_token}")
            params["page_token"] = next_page_token
            params_crypto["page_token"] = next_page_token
            response = requests.get(base_url_stocks if asset_type == 'STOCK' else base_url_crypto, params=params if asset_type == 'STOCK' else params_crypto, headers=headers, timeout=30)
            response.raise_for_status()
            next_page_data = response.json()
            
            if "bars" in next_page_data and symbol in next_page_data["bars"]:
                next_page_bars.extend(next_page_data["bars"][symbol])
                print(f"Fetched {len(next_page_data['bars'][symbol])} additional bars for {symbol}")
            else:
                print(f"No bars found in next page for {symbol}")
                break
            
            next_page_token = next_page_data.get("next_page_token")
            print(f"Next page token is now: {next_page_token}")
    
        data["bars"][symbol].extend(next_page_bars)
        print(f"Total bars for {symbol} after pagination: {len(data['bars'][symbol])}")
        data["type"] = asset_type
    
    return data


def save_to_json(data, symbol, output_dir="/opt/raw_datasets/cryptostocks"):
    """Save API response to JSON file - grouped by ticker"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{symbol.replace('/', '_')}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    # Extract just this ticker's data from the response
    ticker_data = {
        "symbol": symbol,
        "type": data.get("type"),
        "bars": data.get("bars", {}).get(symbol, []),
        "next_page_token": data.get("next_page_token"),
        "fetched_at": datetime.now().isoformat()
    }
    
    with open(filepath, 'w') as f:
        json.dump(ticker_data, f, indent=2)
    
    print(f"✅ Saved {len(ticker_data['bars'])} bars to {filepath}")
    return filepath


def main():
    """Main function to fetch data for multiple symbols"""   
    symbols = [
        {
            "symbol": "BCH/USD",
            "type": "CRYPTO"
        },
        {
            "symbol": "BTC/USD",
            "type": "CRYPTO"
        },
        {
            "symbol": "ETH/USD",
            "type": "CRYPTO"
        },
        {
            "symbol": "SOL/USD",
            "type": "CRYPTO"
        },
        {
            "symbol": "AAPL",
            "type": "STOCK"
        },
        {
            "symbol": "MSFT",
            "type": "STOCK"
        },
        {
            "symbol": "GOOGL",
            "type": "STOCK"
        },
        {
            "symbol": "AMZN",
            "type": "STOCK"
        },
        {
            "symbol": "TSLA",
            "type": "STOCK"
        },
        {
            "symbol": "NVDA",
            "type": "STOCK"
        },
        {
            "symbol": "META",
            "type": "STOCK"
        },
        {
            "symbol": "AVGO",
            "type": "STOCK"
        },
        {
            "symbol": "BRK.B",
            "type": "STOCK"
        },
        {
            "symbol": "LLY",
            "type": "STOCK"
        },
        {
            "symbol": "V",
            "type": "STOCK"
        },
        {
            "symbol": "JNJ",
            "type": "STOCK"
        },
        {
            "symbol": "XOM",
            "type": "STOCK"
        },
        {
            "symbol": "JPM",
            "type": "STOCK"
        },
        {
            "symbol": "WMT",
            "type": "STOCK"
        }
    ]
    
    results = []
    success_count = 0
    
    print("="*60)
    print("ALPACA DATA INGESTION")
    print("="*60)

    for symbol_dict in symbols:
        symbol = symbol_dict["symbol"]
        try:
            # verify if symbol exists in raw datasets
            existing_files = list(Path("/opt/raw_datasets/cryptostocks").glob(f"{symbol.replace('/', '_')}_*.json"))
            if existing_files:
                print(f"ℹ️  Data for {symbol} already exists. Skipping fetch.")
                results.append({"symbol": symbol, "status": "skipped"})
                continue
            data = fetch_cryptostocks_data(symbol, symbol_dict["type"])
            
            # Check if bars exist for this symbol
            if "bars" in data and symbol in data["bars"] and len(data["bars"][symbol]) > 0:
                filepath = save_to_json(data, symbol)
                results.append({"symbol": symbol, "status": "success", "filepath": filepath})
                success_count += 1
            else:
                print(f"⚠️  No bars data for {symbol}")
                results.append({"symbol": symbol, "status": "no_data"})

            import time
            time.sleep(12)
                
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {str(e)}, Stacktrace: ", exc_info=True)
            results.append({"symbol": symbol, "status": "error", "error": str(e)})
    
    print("\n" + "="*60)
    print("INGESTION SUMMARY")
    print("="*60)
    for result in results:
        status_icon = "✅" if result["status"] == "success" else "❌"
        print(f"{status_icon} {result['symbol']}: {result['status']}")
    
    print(f"\n✅ Successfully fetched {success_count}/{len(symbols)} symbols")
    print("="*60)
    
    if success_count == 0 and all(r["status"] != "skipped" for r in results):
        raise ValueError("Failed to fetch any data from Alpaca")
    
    return results


if __name__ == "__main__":
    main()
