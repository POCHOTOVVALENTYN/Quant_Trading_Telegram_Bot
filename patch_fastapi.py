import re

with open("api/rest/main.py", "r") as f:
    text = f.read()

helpers = """
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)):
    # Protect only if key is explicitly set in .env to something secure
    if settings.internal_api_key and settings.internal_api_key != "changeme_for_prod":
        if api_key != settings.internal_api_key:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API Key")
    return api_key

# We will apply this dependency to all routes by replacing @app. with @app. (adding Depends)
# But wait, we can just replace @app.get("/api/v1  with @app.get("/api/v1, dependencies=[Depends(verify_api_key)]
"""

if "verify_api_key" not in text:
    text = text.replace("from fastapi import FastAPI\n", "from fastapi import FastAPI\n" + helpers + "\n")

    # We need to inject dependencies=[Depends(verify_api_key)] to all @app.route("/api/v1...") lines
    text = re.sub(
        r'(@app\.(?:get|post|put|delete)\("/api/v1[^"]*")',
        r'\1, dependencies=[Depends(verify_api_key)]',
        text
    )

with open("api/rest/main.py", "w") as f:
    f.write(text)
print("api patched")
