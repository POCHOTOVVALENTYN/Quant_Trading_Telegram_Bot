import re

file_path = "api/telegram/main.py"
with open(file_path, "r") as f:
    text = f.read()

helpers = """import httpx
from contextlib import asynccontextmanager

# Global client for preventing TIME_WAIT exhaustion
global_client = httpx.AsyncClient(timeout=httpx.Timeout(connect=2.5, read=8.0, write=5.0, pool=3.0))

@asynccontextmanager
async def get_http_client(*args, **kwargs):
    # Ignore kwargs like timeout here, let global client handle it
    yield global_client
"""

# Insert helpers after "import httpx"
if "get_http_client" not in text:
    text = text.replace("import httpx\n", helpers + "\n")

# Replace invocations
# We want to replace httpx.AsyncClient(something) inside `async with`
text = re.sub(
    r"httpx\.AsyncClient\([^)]*\)",
    r"get_http_client()",
    text
)

# And also empty calls: httpx.AsyncClient()
text = text.replace("httpx.AsyncClient()", "get_http_client()")

with open(file_path, "w") as f:
    f.write(text)
print("api/telegram/main.py patched successfully")
