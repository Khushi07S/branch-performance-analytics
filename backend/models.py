from .extensions import db, bcrypt
from datetime import datetime

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=True)
    role = db.Column(db.String(20), nullable=False, default='manager')  # 'admin' or 'manager'
        # Just store the branch name / id, no DB-level FK
    managed_branch_id = db.Column(db.String(255), nullable=True)

    needs_password_reset = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password: str):
        self.password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

    def check_password(self, plain: str) -> bool:
        return bcrypt.check_password_hash(self.password_hash, plain)

    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'role': self.role,
            'managed_branch_id': self.managed_branch_id,
            'needs_password_reset': self.needs_password_reset
        }

class BranchKPIs(db.Model):
    __tablename__ = 'branch_kpis_new'
    branches = db.Column(db.String(255), primary_key=True)  # branch identifier
    year = db.Column(db.Integer, primary_key=True, default=0)
    quarter = db.Column(db.Integer, primary_key=True, default=0)

    # Example KPI columns (make sure these match your processed data)
    totalaggregatedeposits = db.Column(db.BigInteger, default=0)
    totalaggregatecredit = db.Column(db.BigInteger, default=0)
    totalcasa = db.Column(db.BigInteger, default=0)
    region = db.Column(db.String(120), nullable=True)
    state = db.Column(db.String(120), nullable=True)
    # Add other columns you need...

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
