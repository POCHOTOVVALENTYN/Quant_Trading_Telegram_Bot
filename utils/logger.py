import sys
from loguru import logger
import os

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure Loguru
logger.remove() # Remove default console logger

# Console logger for development/info
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

# File loggers per requirement (trade logs, error logs, signal logs, execution logs)
logger.add(os.path.join(log_dir, "trade.log"), filter=lambda record: "trade" in record["extra"], rotation="10 MB", compression="zip")
logger.add(os.path.join(log_dir, "error.log"), level="ERROR", rotation="10 MB")
logger.add(os.path.join(log_dir, "signal.log"), filter=lambda record: "signal" in record["extra"], rotation="10 MB")
logger.add(os.path.join(log_dir, "execution.log"), filter=lambda record: "execution" in record["extra"], rotation="10 MB")

# Utility functions to log specific events easily
def get_trade_logger():
    return logger.bind(trade=True)

def get_signal_logger():
    return logger.bind(signal=True)

def get_execution_logger():
    return logger.bind(execution=True)

# default export
app_logger = logger
