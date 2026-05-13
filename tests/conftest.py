import os
import sys
from pathlib import Path

# CI / bare environments: pydantic requires TELEGRAM_BOT_TOKEN before Settings import
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-placeholder-token")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

