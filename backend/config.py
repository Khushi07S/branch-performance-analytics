import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret'
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        f"postgresql://{os.environ.get('DB_USER','postgres')}:{os.environ.get('DB_PASS','postgres')}@{os.environ.get('DB_HOST','localhost')}:{os.environ.get('DB_PORT','5432')}/{os.environ.get('DB_NAME','branch_analytics_db')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY') or os.environ.get('SECRET_KEY') or 'dev-jwt-secret'
    JWT_TOKEN_LOCATION = ['headers']
    JWT_HEADER_NAME = 'Authorization'
    JWT_HEADER_TYPE = 'Bearer'

    # Google OAuth: You can set the client id for optional client-side validation. Not required for server.
    GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
