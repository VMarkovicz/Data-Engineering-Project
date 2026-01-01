import requests
import json
import os
from datetime import datetime
from pathlib import Path
import os


def fetch_cryptostocks_data(symbol):
    """Fetch data from Alpaca API"""
    base_url = "https://data.alpaca.markets/v2/stocks/bars"
    
    params = {
        "timeframe": "1Day",
        "symbols": symbol,
        "start": "2015-01-01",
        "limit": 1000,
        "adjustment": "raw",
        "feed": "sip",
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
    print(f"full query URL: {base_url} with params {params}")
    response = requests.get(base_url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    
    data = response.json()  # Changed from .text to .json()
    
    if "Error Message" in data:
        raise ValueError(f"API Error: {data['Error Message']}")
    
    if "Note" in data:
        print(f"⚠️  API Rate Limit warning for {symbol}")
    
    return data


def save_to_json(data, symbol, output_dir="/opt/raw_datasets/cryptostocks"):
    """Save API response to JSON file - grouped by ticker"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{symbol}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    # Extract just this ticker's data from the response
    ticker_data = {
        "symbol": symbol,
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
        "IBM",
        "AAPL",
        "MSFT",
        "GOOGL",
        "TSLA",
        "JPM",
        "XOM",
    ]
    
    results = []
    success_count = 0
    
    print("="*60)
    print("ALPACA DATA INGESTION")
    print("="*60)
    
    for symbol in symbols:
        try:
            # verify if symbol exists in raw datasets
            existing_files = list(Path("/opt/raw_datasets/cryptostocks").glob(f"{symbol}_*.json"))
            if existing_files:
                print(f"ℹ️  Data for {symbol} already exists. Skipping fetch.")
                results.append({"symbol": symbol, "status": "skipped"})
                continue
            data = fetch_cryptostocks_data(symbol)
            
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
            print(f"❌ Error fetching {symbol}: {str(e)}")
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
