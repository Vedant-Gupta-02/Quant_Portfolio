from sqlalchemy import create_engine
from portfolio_creation import Backtester

# Connect to your database
engine = create_engine("postgresql+psycopg://vedant:sumoanjo@localhost/quant_portfolio")

# Initialize the backtester
bt = Backtester(engine)

# Run the backtest from 2000 to 2025
bt.run_backtest(start_year=2000, end_year=2025)
