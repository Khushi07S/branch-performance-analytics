# backend/admin.py
"""
Admin routes for branch performance analytics.
Includes manager registration endpoint.
"""

from flask import Blueprint, request, jsonify
from .extensions import db
from .models import User
from werkzeug.security import generate_password_hash
import datetime

admin_bp = Blueprint("admin", __name__)

@admin_bp.route("/create-manager", methods=["POST"])
def create_manager():
    """
    Create a new manager account.
    Expected JSON payload:
    {
        "name": "Manager Name",
        "email": "manager@bank.com",
        "username": "unique_username",
        "password": "temporary_password",
        "branchId": "BR001",
        "role": "manager"  # optional, defaults to manager
    }
    """
    try:
        data = request.json or {}
        
        # Extract required fields
        name = data.get("name", "").strip()
        email = data.get("email", "").strip()
        username = data.get("username", "").strip()
        password = data.get("password", "").strip()
        branch_id = data.get("branchId", "").strip()
        role = data.get("role", "manager").strip()
        
        # Validate required fields
        if not username:
            return jsonify({"msg": "Username is required"}), 400
        
        if not email:
            return jsonify({"msg": "Email is required"}), 400
            
        if not password:
            return jsonify({"msg": "Password is required"}), 400
        
        if not branch_id:
            return jsonify({"msg": "Branch ID is required"}), 400
        
        # Check for existing username
        existing_user = User.query.filter_by(username=username).first()
        if existing_user:
            return jsonify({"msg": f"Username '{username}' already exists"}), 409
        
        # Check for existing email
        existing_email = User.query.filter_by(email=email).first()
        if existing_email:
            return jsonify({"msg": f"Email '{email}' already registered"}), 409
        
        # Create new user
        new_user = User(
            username=username,
            email=email,
            role=role,
            managed_branch_id=branch_id,
            needs_password_reset=True,
            created_at=datetime.datetime.utcnow()
        )
        
        # Set password (this will use bcrypt hashing from your User model)
        new_user.set_password(password)
        
        # Add to database
        db.session.add(new_user)
        db.session.commit()
        
        # Return success response
        return jsonify({
            "msg": "Manager created successfully",
            "user": {
                "id": new_user.id,
                "username": new_user.username,
                "email": new_user.email,
                "role": new_user.role,
                "managed_branch_id": new_user.managed_branch_id,
                "needs_password_reset": new_user.needs_password_reset
            }
        }), 201
        
    except Exception as e:
        db.session.rollback()
        print(f"Error creating manager: {str(e)}")
        return jsonify({"msg": f"Failed to create manager: {str(e)}"}), 500


@admin_bp.route("/managers", methods=["GET"])
def get_managers():
    """Get all managers (optional - for listing purposes)"""
    try:
        managers = User.query.filter_by(role="manager").all()
        return jsonify({
            "managers": [m.to_dict() for m in managers]
        }), 200
    except Exception as e:
        return jsonify({"msg": f"Failed to fetch managers: {str(e)}"}), 500