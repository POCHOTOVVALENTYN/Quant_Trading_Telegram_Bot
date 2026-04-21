import time
import os
import subprocess
from datetime import datetime

LOG_FILES = [
    "logs/market_data_live.log",
    "logs/engine_live.log",
    "logs/ml_worker_live.log",
    "logs/telegram_live.log"
]
MONITOR_LOG = "logs/monitoring_overnight.log"

def log_event(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(MONITOR_LOG, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

def check_services():
    log_event("--- Checking Services Status ---")
    ps = subprocess.check_output(["ps", "aux"]).decode()
    for service in ["market_data", "api.rest", "ml_worker", "api.telegram"]:
        if service in ps:
            log_event(f"✅ Service {service} is RUNNING")
        else:
            log_event(f"❌ Service {service} is DOWN")

def monitor():
    log_event("🚀 Starting Overnight Monitoring until 06:00 AM")
    
    while True:
        now = datetime.now()
        if now.hour == 6 and now.minute >= 0:
            log_event("🏁 Morning reached. Stopping monitor.")
            break
            
        check_services()
        
        # Check last 10 lines of each log for errors
        for log in LOG_FILES:
            if os.path.exists(log):
                try:
                    last_lines = subprocess.check_output(["tail", "-n", "10", log]).decode()
                    if "Error" in last_lines or "exception" in last_lines.lower():
                        log_event(f"⚠️ Potential issues in {log}")
                except:
                    pass
        
        log_event("--- Sleeping for 30 minutes ---")
        time.sleep(1800) # 30 mins

if __name__ == "__main__":
    monitor()
