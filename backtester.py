# backtester.py
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from db_schema import YearlyPortfolio, RuleBacktestMetric, Price
from typing import Dict


class PortfolioBacktester:
    def __init__(self, engine, risk_free_rate: float = 0.02):
        self.engine = engine
        self.risk_free_rate = risk_free_rate

    def compute_portfolio_returns(self, weights: Dict[str, float], year: int) -> pd.Series:
        """Compute daily portfolio returns given company weights for one year."""
        with Session(self.engine) as session:
            q = session.query(Price).filter(
                Price.company_id.in_(list(weights.keys())),
                Price.date.between(f"{year}-01-01", f"{year}-12-31")
            )
            df = pd.read_sql(q.statement, self.engine)

        if df.empty:
            return pd.Series(dtype=float)

        df = df.pivot(index="date", columns="company_id", values="close").sort_index().ffill()
        daily_returns = df.pct_change().dropna()

        # Align weights with available companies
        valid_ids = [cid for cid in weights.keys() if cid in daily_returns.columns]
        w = np.array([weights[cid] for cid in valid_ids])
        w = w / w.sum()  # normalize
        port_returns = daily_returns[valid_ids].dot(w)

        return port_returns

    def compute_metrics(self, returns: pd.Series) -> Dict[str, float]:
        """Compute backtest metrics from daily returns."""
        if returns.empty:
            return dict(mean_return=None, median_return=None, volatility=None, sharpe=None)

        mean_ret = returns.mean() * 252
        median_ret = returns.median() * 252
        vol = returns.std() * np.sqrt(252)
        sharpe = (mean_ret - self.risk_free_rate) / vol if vol > 0 else None

        return dict(
            mean_return=float(mean_ret),
            median_return=float(median_ret),
            volatility=float(vol),
            sharpe=float(sharpe) if sharpe is not None else None
        )

    def run_backtest(self, start_year: int, end_year: int):
        """Run backtest for all portfolios and all weighting schemes."""
        with Session(self.engine) as session:
            portfolios = session.query(YearlyPortfolio).filter(
                YearlyPortfolio.year.between(start_year, end_year)
            ).all()

            for yp in portfolios:
                for scheme, weights in yp.weights.items():  # weights contains multiple schemes
                    returns = self.compute_portfolio_returns(weights, yp.year)
                    metrics = self.compute_metrics(returns)

                    rbm = RuleBacktestMetric(
                        rule_id=yp.rule_id,
                        year=yp.year,
                        scheme=scheme,
                        mean_return=metrics["mean_return"],
                        median_return=metrics["median_return"],
                        volatility=metrics["volatility"],
                        sharpe=metrics["sharpe"],
                    )
                    session.add(rbm)

            session.commit()
            print(f"✅ Backtest complete for {start_year}-{end_year}")
