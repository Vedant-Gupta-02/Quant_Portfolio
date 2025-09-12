# run_backtest.py
from sqlalchemy import create_engine
from backtester import PortfolioBacktester

engine = create_engine("postgresql+psycopg://vedant:sumoanjo@localhost:5432/quant_portfolio")

bt = PortfolioBacktester(engine)
bt.run_backtest(start_year=2000, end_year=2025)

