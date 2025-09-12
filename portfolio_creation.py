# portfolio_creation.py

from sqlalchemy.orm import Session
from db_schema import Company, Financial, Rule, YearlyPortfolio, Metric
import json
from sqlalchemy import and_

# RENAMED for clarity from Backtester to PortfolioGenerator
class PortfolioGenerator:
    def __init__(self, engine):
        self.engine = engine

    def get_market_cap_for_sorting(self, session, company_ids: list[int], year: int):
        """
        FIX: Fetches market cap from the end of the *previous* year for sorting.
        """
        if not company_ids:
            return {}

        # Look for the 'market_cap' metric
        market_cap_metric = session.query(Metric).filter(Metric.name == 'market_cap').first()
        if not market_cap_metric:
            # Fallback if no market_cap metric is defined
            return {cid: 0 for cid in company_ids}

        # Query financials for the previous year
        # Data available at the start of 'year' is from the end of 'year - 1'
        financials = session.query(Financial).filter(
            Financial.company_id.in_(company_ids),
            Financial.metric_id == market_cap_metric.id,
            Financial.period_label.endswith(str(year - 1)) # Use previous year's data
        ).all()

        return {f.company_id: f.value for f in financials}


    def generate_portfolio(self, rule: Rule, year: int):
        """
        FIX: Rewritten to be more efficient and to save company IDs correctly.
        """
        filters = rule.rule_json.get("filters", [])
        if not filters:
            return []

        with Session(self.engine) as session:
            # Start with a query for all company IDs
            q = session.query(Company.id)

            # Dynamically build the query by joining and filtering for each rule
            for f in filters:
                # Alias is needed for each join to be unique
                financial_alias = Financial.__table__.alias()
                metric_alias = Metric.__table__.alias()

                # Define the join condition
                join_condition = and_(
                    Company.id == financial_alias.c.company_id,
                    financial_alias.c.metric_id == metric_alias.c.id,
                    metric_alias.c.name == f["name"],
                    # <-- FIX: Use previous year's data to avoid lookahead bias
                    financial_alias.c.period_label.endswith(str(year - 1))
                )

                # Define the filter condition on the value
                if f["sign"] == ">":
                    filter_condition = financial_alias.c.value > f["threshold"]
                elif f["sign"] == "<":
                    filter_condition = financial_alias.c.value < f["threshold"]
                elif f["sign"] == ">=":
                    filter_condition = financial_alias.c.value >= f["threshold"]
                elif f["sign"] == "<=":
                    filter_condition = financial_alias.c.value <= f["threshold"]
                else:
                    continue # Skip invalid sign

                # Apply the join and filter to the main query
                q = q.join(financial_alias, join_condition).filter(filter_condition)

            # Execute the query to get IDs of eligible companies
            eligible_company_ids = [result[0] for result in q.distinct().all()]

            # Fetch market caps for sorting the eligible companies
            market_caps = self.get_market_cap_for_sorting(session, eligible_company_ids, year)

            # Sort by market cap descending and take top 20
            eligible_company_ids.sort(key=lambda cid: market_caps.get(cid, 0), reverse=True)
            top_company_ids = eligible_company_ids[:20]

            if not top_company_ids:
                print(f"⚠️ No companies found for rule {rule.id} in year {year}")
                return []

            # <-- FIX: Store a placeholder dict of company IDs in the 'weights' column
            # This will be overwritten later by the weighting engine.
            portfolio_weights_placeholder = {str(cid): 0.0 for cid in top_company_ids}

            # Check if a portfolio already exists
            existing_portfolio = session.query(YearlyPortfolio).filter_by(rule_id=rule.id, year=year).one_or_none()

            if existing_portfolio:
                existing_portfolio.weights = portfolio_weights_placeholder
            else:
                new_portfolio = YearlyPortfolio(
                    rule_id=rule.id,
                    year=year,
                    weights=portfolio_weights_placeholder
                )
                session.add(new_portfolio)

            session.commit()
            return top_company_ids


    def run_backtest(self, start_year: int, end_year: int):
        """Run portfolio generation for all rules from start_year to end_year."""
        with Session(self.engine) as session:
            rules = session.query(Rule).all()

        for rule in rules:
            for year in range(start_year, end_year + 1):
                self.generate_portfolio(rule, year)
        print(f"✅ Portfolio generation complete for {start_year}-{end_year}")