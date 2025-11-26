# backend/app.py
"""
Flask application factory + robust local runner for debugging.

Save this file and run:
    python -u -m backend.app
or
    python -u backend/app.py

The "-u" forces unbuffered output so prints appear immediately in Windows CMD.
"""

import os
import traceback
from flask import Flask, request
from .config import Config
from .extensions import db, jwt, bcrypt
from flask_cors import CORS
def create_app(config_class=Config):
    app = Flask(__name__, instance_relative_config=False)
    app.config.from_object(config_class)

    CORS(app, resources={r"/api/*": {"origins": ["http://localhost:5173", "http://127.0.0.1:5173"]}}, supports_credentials=True)

    # Initialize extensions
    db.init_app(app)
    jwt.init_app(app)
    bcrypt.init_app(app)

    # Helpful after_request CORS for your React frontend
    @app.after_request
    def after_request(response):
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:5173')
        response.headers.add('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        if request.method == 'OPTIONS':
            response.status_code = 200
        return response

    # Import blueprints locally to avoid circular imports
    try:
        from .auth import auth_bp
        from .kpis import kpis_bp
        from .admin import admin_bp
        from .branches import branches_bp 
        app.register_blueprint(auth_bp, url_prefix='/api/v1/auth')
        app.register_blueprint(kpis_bp, url_prefix='/api/v1/kpis')
        app.register_blueprint(admin_bp, url_prefix='/api/v1/admin')
        app.register_blueprint(branches_bp)
    except Exception as e:
        # Stash import error on app so it is visible in startup logs
        app.import_error = str(e)

    @app.route('/')
    def index():
        return "Branch Analytics Backend API is Running!"

    return app

    


# -------------------------
# Local runner (very explicit)
# -------------------------
if __name__ == '__main__':
    # Unbuffered prints (useful in Windows CMD)
    import sys
    try:
        print("\n=== backend.app startup ===")
        print(f"Python executable: {sys.executable}")
        print(f"Working directory: {os.getcwd()}")
        # Print relevant environment variables
        print("ENV VARS:")
        for var in ("DB_USER","DB_PASS","DB_HOST","DB_PORT","DB_NAME","JWT_SECRET_KEY","FLASK_ENV"):
            print(f"  {var} = {os.environ.get(var)}")
        app = create_app()
        print("create_app() returned a Flask app instance.")
        # Show if blueprints import had errors
        imp_err = getattr(app, "import_error", None)
        if imp_err:
            print("WARNING: blueprint import error captured:", imp_err)

        # Print the SQLALCHEMY URI (sanitized)
        uri = app.config.get('SQLALCHEMY_DATABASE_URI', '(not set)')
        print(f"SQLALCHEMY_DATABASE_URI: {uri}")

        # Ensure models are loaded (so db metadata is available)
        with app.app_context():
            try:
                # attempt to import models so any model-time errors show now
                from . import models  # noqa: F401
                print("models module imported successfully.")
            except Exception as me:
                print("ERROR importing models:")
                traceback.print_exc()

        host = os.environ.get('BACKEND_HOST', '127.0.0.1')
        port = int(os.environ.get('BACKEND_PORT', 5000))
        debug_env = os.environ.get('BACKEND_DEBUG', os.environ.get('FLASK_DEBUG', 'True'))
        debug = True if str(debug_env).lower() in ('true','1','yes') else False

        print(f"Starting Flask app on http://{host}:{port} (debug={debug})")
        # Bind explicitly and don't use threaded weirdness — this is local dev
        app.run(host=host, port=port, debug=debug)
    except Exception as e:
        print("FATAL: exception in backend.app __main__")
        traceback.print_exc()
        # Exit non-zero so any script calling this sees failure
        raise

# backend/app.py
# Add this to your existing app.py

from flask import Flask
from flask_cors import CORS
from .extensions import db, bcrypt, jwt
from .config import Config

# Import blueprints
from .auth import auth_bp
from .admin import admin_bp  # ADD THIS
from .branches import branches_bp
from .kpis import kpis_bp

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # Initialize extensions
    db.init_app(app)
    bcrypt.init_app(app)
    jwt.init_app(app)
    CORS(app)
    
    # Register blueprints
    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(admin_bp, url_prefix="/admin")  # ADD THIS
    app.register_blueprint(branches_bp, url_prefix="/branches")
    app.register_blueprint(kpis_bp, url_prefix="/kpis")
    
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)