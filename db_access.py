import pandas as pd
from sqlalchemy import create_engine

# Use Postgres, not SQLite
uri = "postgresql+psycopg://vedant:sumoanjo@localhost:5432/quant_portfolio"
engine = create_engine(uri)

query = """
SELECT y.*
FROM yearly_portfolios y
"""

df_rules = pd.read_sql(query, engine)
print(df_rules.head())
