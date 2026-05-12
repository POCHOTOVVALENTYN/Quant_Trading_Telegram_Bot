import ccxt.pro as ccxtpro
exchange = ccxtpro.binance({'options': {'defaultType': 'future'}})
print("Default API URLs:")
print(exchange.urls['api'])
print("\nDefault WS URLs:")
print(exchange.urls.get('ws', {}))
print("\nDemo URLs:")
print(exchange.urls.get('demo', {}))
