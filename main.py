import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

if os.environ.get("VERCEL"):
    _db_path = "/tmp/pecan.db"
else:
    _db_path = os.path.join(_ROOT, "data", "pecan.db")

try:
    if not os.path.exists(_db_path):
        if not os.environ.get("VERCEL"):
            os.makedirs(os.path.join(_ROOT, "data"), exist_ok=True)
        from tools.database import get_all_alumni, init_database
        from tools.seed_data import generate_alumni
        from tools.vector_store import embed_alumni

        init_database()
        generate_alumni(250)
        embed_alumni(get_all_alumni() or [])
except Exception as exc:
    print(f"Auto-seed failed: {exc}")

from api import app
