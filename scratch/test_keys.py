import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("TEST_API_KEY_BINANCE", "").strip()
secret_key = os.getenv("TEST_SECRET_API_KEY_BINANCE", "").strip()

print(f"Testing with Key: {api_key[:4]}...{api_key[-4:]}")

base_url = "https://testnet.binancefuture.com"
endpoint = "/fapi/v1/balance"

timestamp = int(time.time() * 1000)
query_string = f"timestamp={timestamp}&recvWindow=10000"
signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

url = f"{base_url}{endpoint}?{query_string}&signature={signature}"
headers = {"X-MBX-APIKEY": api_key}

try:
    response = requests.get(url, headers=headers, timeout=10)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {e}")
