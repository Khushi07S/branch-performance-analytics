# backend/db_fix_add_created_at.py
import sys, os, traceback
from datetime import datetime

# Ensure project root is on sys.path so package imports (backend.*) work.
this_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(this_file, os.pardir))   # backend/
project_root = os.path.abspath(os.path.join(project_root, os.pardir))# project root
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import Flask app factory and DB extension
from backend.app import create_app
from backend.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        # Add created_at column (safe: IF NOT EXISTS)
        sql = text("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now();
        """)
        db.session.execute(sql)

        # Optionally add updated_at column if you'd like it too
        sql2 = text("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITHOUT TIME ZONE;
        """)
        db.session.execute(sql2)

        db.session.commit()
        print("OK: created_at (and updated_at) column added or already present.")
    except Exception as exc:
        db.session.rollback()
        print("ERROR: Failed to add column(s). Traceback follows:")
        traceback.print_exc()
        raise
