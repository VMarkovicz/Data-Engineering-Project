from alpaca.data.live import StockDataStream

API_KEY = "PKEUUT6XOX2HFZEILUNVCWYDKI"
SECRET_KEY = "6FajSAMkrgb5brCe5LKuJH5oZRdEFiwP6qhaE6A7TC9V"

wss_client = StockDataStream(API_KEY, SECRET_KEY)

async def trade_handler(data):
    print(f"Trade recebido: {data}")

wss_client.subscribe_trades(trade_handler, "AAPL", "MSFT", "TSLA")

print("Conectando ao WebSocket...")
wss_client.run()
