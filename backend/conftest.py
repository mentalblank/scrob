import os
import sys

# Modules import flat ("from models import ...") — same as the deployed
# entrypoint, which runs uvicorn with cwd=/app/backend.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
