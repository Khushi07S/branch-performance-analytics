# backend/branches.py
from flask import Blueprint, jsonify, current_app
from sqlalchemy import text
from .extensions import db

branches_bp = Blueprint("branches", __name__, url_prefix="/api/v1")

@branches_bp.route("/branches", methods=["GET"])
def list_branches():
    """
    Returns JSON: { "branches": ["BRANCH-1", "BRANCH-2", ...] }
    Admin/Manager can call this to populate the branch selector.
    """
    try:
        # Query distinct branch keys from persistent table
        sql = text("SELECT DISTINCT branches FROM branch_kpis_new ORDER BY branches")
        res = db.session.execute(sql)
        rows = [r[0] for r in res.fetchall() if r[0] is not None]
        return jsonify(branches=rows), 200
    except Exception as e:
        current_app.logger.exception("Failed to list branches")
        return jsonify(msg="Failed to fetch branches", error=str(e)), 500
