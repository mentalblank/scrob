from slowapi import Limiter
from slowapi.util import get_remote_address

# Single shared limiter instance imported by main.py (app setup)
# and by routers that need per-endpoint rate limits.
limiter = Limiter(key_func=get_remote_address)
