# portfolio_weighting.py

import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from db_schema import Financial, Price, Metric  # <-- Import Metric
from typing import Literal
from datetime import date, timedelta

class PortfolioWeightingEngine:
    def __init__(self, engine):
        self.engine = engine

    def get_market_caps(self, company_ids, year):
        """
        FIX: Fetch market cap data from the end of the PREVIOUS year.
        """
        with Session(self.engine) as session:
            # Find the market_cap metric ID
            market_cap_metric = session.query(Metric).filter(Metric.name == 'market_cap').first()
            if not market_cap_metric:
                return pd.Series(1, index=company_ids) # Fallback

            # Query the financials table for the previous year-end's data
            q = session.query(Financial)
            q = q.filter(Financial.company_id.in_(company_ids))
            # <-- FIX: Use data from year-1, which is known at the start of 'year'
            q = q.filter(Financial.period_label.endswith(str(year - 1)))
            q = q.filter(Financial.metric_id == market_cap_metric.id)
            df = pd.read_sql(q.statement, self.engine)

        if df.empty:
            return pd.Series(1, index=company_ids)  # fallback to equal weight
        
        # Ensure company_id is the index
        df = df.set_index('company_id')
        # Reindex to ensure all requested company_ids are present, fill missing with 1
        return df['value'].reindex(company_ids, fill_value=1.0)


    def get_volatility(self, company_ids, year, lookback=252):
        """
        FIX: Compute annualized volatility from prices in the year PRIOR to measurement.
        """
        # <-- FIX: Calculate dates to avoid lookahead bias.
        end_date = date(year, 1, 1) - timedelta(days=1)
        start_date = end_date - timedelta(days=int(lookback * 1.5)) # Fetch a bit more data

        with Session(self.engine) as session:
            q = session.query(Price)
            q = q.filter(Price.company_id.in_(company_ids))
            q = q.filter(Price.date.between(start_date, end_date))
            df = pd.read_sql(q.statement, self.engine)

        if df.empty:
            return pd.Series(1, index=company_ids)

        vol_dict = {}
        for cid in company_ids:
            prices = df[df['company_id'] == cid].sort_values('date')['close'].tail(lookback)
            if len(prices) < 2:
                vol_dict[cid] = 0.2  # default 20%
            else:
                ret = prices.pct_change().dropna()
                vol_dict[cid] = ret.std() * np.sqrt(252)
        return pd.Series(vol_dict)

    def compute_weights(
        self,
        company_ids,
        year,
        scheme: Literal['equal', 'market_cap', 'inverse_vol', 'momentum'] = 'equal',
        lookback_vol: int = 252,
        momentum_period: int = 252
    ):
        """Compute portfolio weights based on selected scheme."""
        if not company_ids:
            return {}
        
        # Convert IDs to string for JSON compatibility in the final dict
        str_company_ids = [str(cid) for cid in company_ids]

        if scheme == 'equal':
            n = len(str_company_ids)
            return {cid: 1/n for cid in str_company_ids}
        
        elif scheme == 'market_cap':
            mcaps = self.get_market_caps(company_ids, year)
            weights = mcaps / mcaps.sum()
            return {str(k): v for k, v in weights.to_dict().items()}

        elif scheme == 'inverse_vol':
            vols = self.get_volatility(company_ids, year, lookback=lookback_vol)
            inv_vols = 1 / (vols + 1e-8) # Add small epsilon to avoid division by zero
            weights = inv_vols / inv_vols.sum()
            return {str(k): v for k, v in weights.to_dict().items()}

        elif scheme == 'momentum':
            # <-- FIX: Calculate dates to avoid lookahead bias
            end_date = date(year, 1, 1) - timedelta(days=1)
            start_date = end_date - timedelta(days=momentum_period)

            with Session(self.engine) as session:
                q = session.query(Price)
                q = q.filter(Price.company_id.in_(company_ids))
                q = q.filter(Price.date.between(start_date, end_date))
                df = pd.read_sql(q.statement, self.engine)

            mom_dict = {}
            for cid in company_ids:
                prices = df[df['company_id'] == cid].sort_values('date')['close']
                if len(prices) < 2:
                    mom_dict[cid] = 0.0
                else:
                    # Ensure we use the actual first and last price in the window
                    mom_dict[cid] = (prices.iloc[-1] / prices.iloc[0]) - 1
            
            mom_series = pd.Series(mom_dict)
            mom_series[mom_series < 0] = 0  # avoid negative weights
            
            if mom_series.sum() > 0:
                weights = mom_series / mom_series.sum()
            else:
                # Fallback to equal weight if all momentums are zero or negative
                weights = pd.Series(1/len(company_ids), index=company_ids)
            return {str(k): v for k, v in weights.to_dict().items()}
        else:
            raise ValueError(f"Unknown weighting scheme: {scheme}")

    # compute_all_weights and save_weights_to_db remain the same, as the logic
    # within compute_weights is what needed fixing. I've updated compute_weights
    # to return string keys for JSON compatibility.
    
    def compute_all_weights(
        self,
        company_ids,
        year,
        schemes: list[str] = ['equal', 'market_cap', 'inverse_vol', 'momentum'],
        lookback_vol: int = 252,
        momentum_period: int = 252
    ) -> dict[str, dict[str, float]]:
        """Compute weights for all schemes and return as a dict of dicts."""
        all_weights = {}
        for scheme in schemes:
            all_weights[scheme] = self.compute_weights(
                company_ids=company_ids,
                year=year,
                scheme=scheme,
                lookback_vol=lookback_vol,
                momentum_period=momentum_period
            )
        return all_weights

