with open("api/telegram/main.py", "r") as f: text = f.read()
import re
# The broken line is: global_client = get_http_client())
text = re.sub(r"global_client = get_http_client\(\)\)", "global_client = httpx.AsyncClient(timeout=httpx.Timeout(connect=2.5, read=8.0, write=5.0, pool=3.0))", text)
with open("api/telegram/main.py", "w") as f: f.write(text)
