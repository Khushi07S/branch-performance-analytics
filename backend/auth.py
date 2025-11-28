from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity, get_jwt
from .extensions import db, bcrypt
from .models import User
import requests

auth_bp = Blueprint('auth', __name__)

@auth_bp.route("/me", methods=["GET"])
@jwt_required()
def me():
    user_id = get_jwt_identity()
    user = User.query.get(user_id)
    if not user:
        return jsonify(msg="User not found"), 404
    return jsonify(username=user.username, role=user.role, managed_branch_id=user.managed_branch_id), 200
@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json() or {}
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify(msg="Missing username or password"), 400

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify(msg="Invalid credentials"), 401

    if not user.check_password(password):
        return jsonify(msg="Invalid credentials"), 401

    # If user must reset password, return token with reset flag
    if user.needs_password_reset:
        token = create_access_token(
            identity=str(user.id),              # FIXED
            additional_claims={'reset': True}
        )
        return jsonify(needs_reset=True, reset_token=token), 202

    # Standard JWT with role + branch
    access_token = create_access_token(
        identity=str(user.id),                 # FIXED
        additional_claims={
            'role': user.role,
            'branch': user.managed_branch_id
        }
    )

    return jsonify(
        access_token=access_token,
        user_role=user.role,
        needs_reset=False
    ), 200


@auth_bp.route('/google', methods=['POST'])
def google_login():
    """
    Google OAuth ID token login.
    """
    data = request.get_json() or {}
    id_token = data.get('id_token')
    if not id_token:
        return jsonify(msg="id_token required"), 400

    # Verify token with Google
    resp = requests.get("https://oauth2.googleapis.com/tokeninfo", params={'id_token': id_token})
    if resp.status_code != 200:
        return jsonify(msg="Invalid Google token"), 401

    info = resp.json()
    email = info.get('email')
    if not email:
        return jsonify(msg="Google token missing email"), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify(msg="User not registered. Admin must create this account."), 403

    # Force reset flow if needed
    if user.needs_password_reset:
        token = create_access_token(
            identity=str(user.id),             # FIXED
            additional_claims={'reset': True}
        )
        return jsonify(needs_reset=True, reset_token=token), 202

    token = create_access_token(
        identity=str(user.id),                 # FIXED
        additional_claims={
            'role': user.role,
            'branch': user.managed_branch_id
        }
    )

    return jsonify(access_token=token, user_role=user.role), 200


@auth_bp.route('/reset-password', methods=['POST'])
@jwt_required()
def reset_password():
    data = request.get_json() or {}
    new_password = data.get('new_password')

    if not new_password:
        return jsonify(msg="new_password is required"), 400

    # identity is stored as string → convert to int
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)

    if not user:
        return jsonify(msg="User not found"), 404

    # IMPORTANT: Do NOT check old password here
    # Because this flow is for "needs_password_reset == True"

    user.set_password(new_password)
    user.needs_password_reset = False   # 🔥 FIXED FLAG
    db.session.commit()

    return jsonify(msg="Password updated successfully"), 200
