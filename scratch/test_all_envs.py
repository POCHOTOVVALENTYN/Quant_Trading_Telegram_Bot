import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("TEST_API_KEY_BINANCE", "").strip()
secret_key = os.getenv("TEST_SECRET_API_KEY_BINANCE", "").strip()

print(f"Testing with Key: {api_key[:4]}...{api_key[-4:]}\n")

environments = {
    "1. Old Testnet (Sandbox)": "https://testnet.binancefuture.com",
    "2. New Demo Trading": "https://demo-fapi.binance.com",
    "3. Mainnet (Live)": "https://fapi.binance.com"
}

endpoint = "/fapi/v2/balance" # /v2/balance is often more reliable for futures

for env_name, base_url in environments.items():
    print(f"--- Testing against {env_name} ({base_url}) ---")
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}&recvWindow=10000"
    
    try:
        signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        url = f"{base_url}{endpoint}?{query_string}&signature={signature}"
        headers = {"X-MBX-APIKEY": api_key}
        
        response = requests.get(url, headers=headers, timeout=5)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("SUCCESS: Key is valid for this environment!")
        else:
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Error: {e}")
    print("\n")
