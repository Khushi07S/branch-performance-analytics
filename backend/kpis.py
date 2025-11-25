# backend/kpis.py
"""
KPIs blueprint for branch_kpis_new table.
Provides:
 - GET  /api/v1/kpis/                          (health)
 - GET  /api/v1/kpis/overview/<branch_id>
 - GET  /api/v1/kpis/time-series/<branch_id>   (returns deposit & credit components; fallback to region aggregates)
 - GET  /api/v1/kpis/regions
 - GET  /api/v1/kpis/grid/<branch_id>
 - GET  /api/v1/kpis/branches-by-region
"""
from flask import Blueprint, jsonify, request, current_app
from .extensions import db
from sqlalchemy import text

kpis_bp = Blueprint("kpis", __name__)


@kpis_bp.route("/", methods=["GET"])
def index():
    return jsonify({"msg": "kpis blueprint alive"}), 200


@kpis_bp.route("/overview/<string:branch_id>", methods=["GET"])
def overview_by_branch(branch_id):
    try:
        sql = text("""
        SELECT *
        FROM branch_kpis_new
        WHERE branches = :branch_id
        ORDER BY year DESC, quarter DESC
        LIMIT 1
        """)
        row = db.session.execute(sql, {"branch_id": branch_id}).fetchone()
        if not row:
            return jsonify({"msg": "No data for branch"}), 404
        # robust conversion of Row -> dict
        try:
            data = dict(row._mapping)
        except Exception:
            try:
                data = dict(row)
            except Exception:
                keys = row.keys()
                data = {k: row[i] for i, k in enumerate(keys)}
        return jsonify(data), 200
    except Exception as e:
        current_app.logger.exception("overview_by_branch error")
        return jsonify({"msg": "Internal server error", "detail": str(e)}), 500


@kpis_bp.route("/time-series/<string:branch_id>", methods=["GET"])
def time_series(branch_id):
    """
    Returns time-series for the branch. Default metrics include:
      - totalaggregatedeposits
      - totalcurrentdeposits, totalsavingsdeposits, totaltermdeposits
      - totalaggregatecredit
      - totalretailcredit, totalagriculturecredit, totalbusinesscredit
      - totaltransactionscount

    If the branch has fewer than 2 rows, falls back to region-level aggregated series (if region exists).
    Accepts ?metrics=a,b,c&periods=N
    """
    try:
        periods = int(request.args.get("periods") or 1000)
        default_metrics = [
            "totalaggregatedeposits",
            "totalcurrentdeposits",
            "totalsavingsdeposits",
            "totaltermdeposits",
            "totalaggregatecredit",
            "totalretailcredit",
            "totalagriculturecredit",
            "totalbusinesscredit",
            "totaltransactionscount",
        ]
        metrics_param = request.args.get("metrics")
        if metrics_param:
            requested = [m.strip() for m in metrics_param.split(",") if m.strip()]
        else:
            requested = default_metrics

        allowed = set(default_metrics)
        metrics = [m for m in requested if m in allowed]
        if not metrics:
            metrics = default_metrics

        cols = ", ".join(metrics)

        sql = text(f"""
        SELECT year, quarter, {cols}, region
        FROM branch_kpis_new
        WHERE branches = :branch_id
        ORDER BY year, quarter
        LIMIT :limit
        """)
        rows = db.session.execute(sql, {"branch_id": branch_id, "limit": int(periods)}).fetchall()
        # If not enough rows (e.g., <2), attempt fallback to region-level timeseries
        if not rows or len(rows) < 2:
            # find branch region (latest)
            try:
                region_row = db.session.execute(text("""
                    SELECT region FROM branch_kpis_new WHERE branches = :branch_id ORDER BY year DESC, quarter DESC LIMIT 1
                """), {"branch_id": branch_id}).fetchone()
                region = None
                if region_row:
                    try:
                        region = dict(region_row._mapping).get("region")
                    except Exception:
                        region = region_row[0] if len(region_row) else None
                if region:
                    # aggregate by year/quarter for that region
                    agg_sql = text(f"""
                    SELECT year, quarter, {cols}
                    FROM (
                      SELECT year, quarter, {cols}, region
                      FROM branch_kpis_new
                      WHERE COALESCE(region,'') = :region
                    ) AS sub
                    GROUP BY year, quarter
                    ORDER BY year, quarter
                    LIMIT :limit
                    """)
                    rows = db.session.execute(agg_sql, {"region": region, "limit": int(periods)}).fetchall()
                    if not rows:
                        return jsonify({"msg": "No time-series data for branch or its region"}), 404
                else:
                    return jsonify({"msg": "No time-series data for branch"}), 404
            except Exception:
                current_app.logger.exception("time_series fallback error")
                return jsonify({"msg": "Internal server error", "detail": "fallback failed"}), 500

        labels = []
        series_data = {m: [] for m in metrics}
        for r in rows:
            try:
                rdict = dict(r._mapping)
            except Exception:
                try:
                    rdict = dict(r)
                except Exception:
                    keys = r.keys()
                    rdict = {k: r[i] for i, k in enumerate(keys)}
            labels.append(f"{rdict.get('year')}-Q{rdict.get('quarter')}")
            for m in metrics:
                val = rdict.get(m)
                series_data[m].append(val if val is not None else 0)

        datasets = []
        for m in metrics:
            datasets.append({"label": m, "data": series_data[m]})

        return jsonify({"labels": labels, "datasets": datasets}), 200

    except Exception as e:
        current_app.logger.exception("time_series error")
        return jsonify({"msg": "Internal server error", "detail": str(e)}), 500


@kpis_bp.route("/regions", methods=["GET"])
def regions_aggregate():
    try:
        year = request.args.get("year", type=int)
        where_clause = "WHERE 1=1"
        params = {}
        if year:
            where_clause += " AND year = :year"
            params["year"] = year

        sql = text(f"""
        SELECT COALESCE(region,'Unknown') AS region,
               COALESCE(state,'Unknown') AS state,
               SUM(COALESCE(totalaggregatedeposits,0)) AS deposits,
               SUM(COALESCE(totalaggregatecredit,0)) AS credit,
               SUM(COALESCE(totaltransactionscount,0)) AS transactions,
               COUNT(DISTINCT branches) AS branches
        FROM branch_kpis_new
        {where_clause}
        GROUP BY COALESCE(region,'Unknown'), COALESCE(state,'Unknown')
        ORDER BY deposits DESC
        """)
        rows = db.session.execute(sql, params).fetchall()
        result = []
        for r in rows:
            try:
                result.append(dict(r._mapping))
            except Exception:
                try:
                    result.append(dict(r))
                except Exception:
                    keys = r.keys()
                    result.append({k: r[i] for i, k in enumerate(keys)})
        return jsonify(result), 200
    except Exception as e:
        current_app.logger.exception("regions_aggregate error")
        return jsonify({"msg": "Internal server error", "detail": str(e)}), 500


@kpis_bp.route("/grid/<string:branch_id>", methods=["GET"])
def branch_grid(branch_id):
    try:
        sql = text("""
        SELECT year, quarter, state, city, region,
               totalcurrentdeposits, totalsavingsdeposits, totaltermdeposits, totalcasa,
               totalaggregatedeposits, totalretailcredit, totalagriculturecredit, totalbusinesscredit,
               totalaggregatecredit, totaltransactionscount
        FROM branch_kpis_new
        WHERE branches = :branch_id
        ORDER BY year DESC, quarter DESC
        """)
        rows = db.session.execute(sql, {"branch_id": branch_id}).fetchall()
        result = []
        for r in rows:
            try:
                result.append(dict(r._mapping))
            except Exception:
                try:
                    result.append(dict(r))
                except Exception:
                    keys = r.keys()
                    result.append({k: r[i] for i, k in enumerate(keys)})
        return jsonify(result), 200
    except Exception as e:
        current_app.logger.exception("branch_grid error")
        return jsonify({"msg": "Internal server error", "detail": str(e)}), 500


@kpis_bp.route("/branches-by-region", methods=["GET"])
def branches_by_region():
    """
    GET /api/v1/kpis/branches-by-region?region=North
    Returns distinct branch ids and a small preview (state, city).
    """
    try:
        region = request.args.get("region")
        if not region:
            return jsonify({"msg": "region query param required"}), 400

        sql = text("""
        SELECT DISTINCT branches AS branch_id, state, city
        FROM branch_kpis_new
        WHERE COALESCE(region,'') = :region
        ORDER BY branches
        """)
        rows = db.session.execute(sql, {"region": region}).fetchall()
        result = []
        for r in rows:
            try:
                result.append(dict(r._mapping))
            except Exception:
                try:
                    result.append(dict(r))
                except Exception:
                    keys = r.keys()
                    result.append({k: r[i] for i, k in enumerate(keys)})
        return jsonify(result), 200
    except Exception as e:
        current_app.logger.exception("branches_by_region error")
        return jsonify({"msg": "Internal server error", "detail": str(e)}), 500
