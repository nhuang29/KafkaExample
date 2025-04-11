from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Loan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    loan_id = db.Column(db.Integer, unique=True, nullable=False)
    applicant_name = db.Column(db.String(100), nullable=False)
    loan_amount = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(100), nullable=False)