# backend: example Flask route
from flask import Blueprint, request, jsonify
from .extensions import db
from .models import User
from werkzeug.security import generate_password_hash
import datetime

admin_bp = Blueprint("admin", __name__)

@admin_bp.route("/auth/create-manager", methods=["POST"])
def create_manager():
    data = request.json or {}
    username = data.get("username")
    email = data.get("email")
    managed_branch_id = data.get("managed_branch_id")
    temp_password = data.get("temp_password")

    if not username:
        return jsonify(msg="username required"), 400

    # if backend should generate password server-side, uncomment next lines:
    # if not temp_password:
    #     temp_password = generate_random_password()  # implement securely

    # Hash the password for storage
    password_hash = generate_password_hash(temp_password)

    # Create the user (adapt to your model fields)
    u = User(username=username, email=email, role="manager", managed_branch_id=managed_branch_id)
    u.password_hash = password_hash
    u.needs_password_reset = True
    u.created_at = datetime.datetime.utcnow()
    db.session.add(u)
    db.session.commit()

    # Return created info including the temporary password (so admin can copy it)
    return jsonify({
        "username": u.username,
        "email": u.email,
        "managed_branch_id": u.managed_branch_id,
        "temp_password": temp_password
    }), 201
