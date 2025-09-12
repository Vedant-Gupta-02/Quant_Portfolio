from sqlalchemy.orm import Session
from db_schema import Company
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg://vedant:sumoanjo@localhost:5432/quant_portfolio")

with Session(engine) as session:
    # Suppose you have the JSON dict from the portfolio
    company_ids = ['101', '102']

    companies = session.query(Company).filter(Company.id.in_(company_ids)).all()
    for c in companies:
        print(c.id, c.name)
