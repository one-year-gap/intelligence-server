"""HTTP client retained from the local comparison experiment."""
import json
import os
import ssl
import urllib.error
import urllib.request

class APIError(Exception):
    pass

def client(key):
    try:
        import certifi
        context = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        context = ssl.create_default_context()
    def call(payload):
        req = urllib.request.Request('https://api.openai.com/v1/chat/completions',
            data=json.dumps(payload, ensure_ascii=False).encode(),
            headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=60, context=context) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                code = json.loads(exc.read()).get('error', {}).get('code')
            except Exception:
                code = 'unparsed'
            # Never print response bodies, request headers or credentials.
            raise APIError(f'HTTP {exc.code}; code={code}') from None
        except urllib.error.URLError:
            raise APIError('NETWORK_ERROR') from None
    return call


def client_from_env():
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise ValueError("OPENAI_API_KEY is required")
    return client(key)
