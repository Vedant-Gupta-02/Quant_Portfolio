# run_portfolio_weighting.py

from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from db_schema import YearlyPortfolio
from portfolio_weighting import PortfolioWeightingEngine

# Connect to DB
engine = create_engine("postgresql+psycopg://vedant:sumoanjo@localhost:5432/quant_portfolio")
pwe = PortfolioWeightingEngine(engine)

with Session(engine) as session:
    # Fetch all yearly portfolios that need weighting
    portfolios = session.query(YearlyPortfolio).all()

    for yp in portfolios:
        # <-- FIX: Read company IDs from the keys of the placeholder dict
        # Ensure IDs are integers, as they are stored as string keys in JSON
        if not yp.weights:
            continue
        company_ids = [int(cid) for cid in yp.weights.keys()]

        if not company_ids:
            print(f"Skipping portfolio for rule {yp.rule_id}, year {yp.year} (no companies).")
            continue

        # <-- FIX: Compute weights for this portfolio for the given year
        all_calculated_weights = pwe.compute_all_weights(
            company_ids, year=yp.year
        )

        # <-- FIX: Update the 'weights' column in the YearlyPortfolio entry
        yp.weights = all_calculated_weights
        session.add(yp)
        print(f"Computed weights for rule {yp.rule_id}, year {yp.year}")

    session.commit()
    print("✅ All portfolios weighted successfully!")