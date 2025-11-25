import os
from backend.app import create_app, db
from backend.models import User

# --- Set the project root path for module imports ---
# NOTE: This is necessary because this script is executed outside the flask context
# os.chdir(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

# --- User Setup Logic ---

def setup_initial_users():
    """
    Initializes the Admin and Manager accounts by running within the application context.
    """
    print("--- Starting Database Initialization ---")
    
    # 1. Instantiate the app and push context
    app = create_app()
    
    with app.app_context():
        
        # 2. Clear existing users to guarantee a clean state
        try:
            db.session.query(User).delete()
            db.session.commit()
            print("Existing user data cleared successfully.")
        except Exception as e:
            # Handle case where table might not be initialized yet
            print(f"Warning: Could not clear users table. Assuming clean start or table needs creation. {e}")
            db.session.rollback()
            
        # 3. Insert Initial Users
        try:
            print("Inserting Admin and Manager accounts...")
            
            # --- Create Admin Account (Person A) ---
            admin_user = User(
                username='admin_boss', 
                email='admin@bharatxcorp.com', 
                role='admin', 
                managed_branch_id=None,
                needs_password_reset=False
            )
            admin_user.set_password('AdminSecurePass123')
            db.session.add(admin_user)
            
            # --- Create Manager Account (Person B) ---
            manager_user = User(
                username='manager_b', 
                email='manager@bharatxcorp.com', 
                role='manager', 
                managed_branch_id='BRANCH-1', 
                needs_password_reset=True
            )
            manager_user.set_password('TempPass123')
            db.session.add(manager_user)
            
            db.session.commit()
            print("SUCCESS: Admin and Manager users are now registered.")
            
        except Exception as e:
            db.session.rollback()
            print(f"FATAL ERROR: Failed to commit users. Database connection or schema issue remains: {e}")

if __name__ == '__main__':
    # You MUST be in the project root to run this script correctly
    setup_initial_users()