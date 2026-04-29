import logging
import logging.config
import os
import sys
import structlog
from datetime import datetime

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

def configure_logger():
    # Use environment variable to toggle between JSON and Console output
    # PRODUCTION (Docker) -> JSON
    # DEVELOPMENT (Local) -> Console (Pretty-print)
    log_format = os.environ.get("LOG_FORMAT", "json").lower()
    
    timestamper = structlog.processors.TimeStamper(fmt="iso")

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.StackInfoRenderer(),
        timestamper,
    ]

    if log_format == "json":
        # Production JSON format
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Development Console format
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.PrintLoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )

# Run configuration
configure_logger()

# Helper getters to maintain backward compatibility with current calls
def get_logger(name: str):
    return structlog.get_logger(name)

def get_trade_logger():
    return structlog.get_logger("trade").bind(service="trade")

def get_signal_logger():
    return structlog.get_logger("signal").bind(service="signal")

def get_execution_logger():
    return structlog.get_logger("execution").bind(service="execution")

def get_market_data_logger():
    return structlog.get_logger("market_data").bind(service="market_data")

def get_ml_logger():
    return structlog.get_logger("ml").bind(service="ml")

# Default export
app_logger = structlog.get_logger("app")

