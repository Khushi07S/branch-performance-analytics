import os
from backend.app import create_app
from backend.extensions import db
from backend.models import User

def create_initial_admin():
    app = create_app()
    with app.app_context():
        admin_username = os.environ.get('INITIAL_ADMIN_USERNAME')
        admin_email = os.environ.get('INITIAL_ADMIN_EMAIL')
        admin_password = os.environ.get('INITIAL_ADMIN_PASSWORD')

        if not (admin_username and admin_password and admin_email):
            print("Set INITIAL_ADMIN_USERNAME, INITIAL_ADMIN_EMAIL, INITIAL_ADMIN_PASSWORD in env.")
            return

        db.create_all()  # ensure tables exist in dev

        existing = User.query.filter_by(username=admin_username).first()
        if existing:
            print("Admin user already exists.")
            return

        admin = User(username=admin_username, email=admin_email, role='admin', managed_branch_id=None, needs_password_reset=False)
        admin.set_password(admin_password)
        db.session.add(admin)
        db.session.commit()
        print("Initial admin created.")

if __name__ == "__main__":
    create_initial_admin()
