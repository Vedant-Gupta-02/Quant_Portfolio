# run_evolution.py
from sqlalchemy import create_engine
from rule_engine import RuleEvolutionEngine

engine = create_engine("postgresql+psycopg://vedant:sumoanjo@localhost:5432/quant_portfolio")

evolver = RuleEvolutionEngine(engine)

# tweak one existing rule
evolver.tweak_rule()

# create one brand-new rule
evolver.create_random_rule()

# run a full evolution cycle
evolver.evolve(n_tweaks=5, n_random=5)
