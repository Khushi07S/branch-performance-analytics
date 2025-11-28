from flask import Blueprint, jsonify, request
from backend.extensions import db
from backend.models import BranchKPIs

branches_bp = Blueprint("branches", __name__)

@branches_bp.route("/branches", methods=["GET"])
def get_all_branches():
    regions = request.args.get("region")

    query = BranchKPIs.query

    if regions:
        query = query.filter(BranchKPIs.region == regions)

    rows = query.all()

    # unique by branch_id
    seen = set()
    result = []
    for r in rows:
        if r.branches not in seen:
            seen.add(r.branches)
            result.append({
                "branch_id": r.branches,
                "name": r.branches,
                "state": r.state,
                "region": r.region,
            })

    return jsonify(result), 200
