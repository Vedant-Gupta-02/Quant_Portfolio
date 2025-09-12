import random
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from db_schema import Rule, Filter  # assumes both are in db_schema.py


class RuleEvolutionEngine:
    def __init__(self, engine):
        self.engine = engine

    def _fix_rule_id_sequence(self, session):
        """Fix Postgres sequence if it's out of sync with rules.id."""
        session.execute(text("""
            SELECT setval(
                pg_get_serial_sequence('rules', 'id'),
                COALESCE((SELECT MAX(id) FROM rules), 0) + 1,
                false
            )
        """))
        session.commit()
        print("🔄 Fixed rules.id sequence")

    def _safe_commit(self, session):
        """Commit with auto-repair for duplicate key errors."""
        try:
            session.commit()
        except IntegrityError as e:
            if "duplicate key value violates unique constraint" in str(e):
                session.rollback()
                self._fix_rule_id_sequence(session)
                session.commit()
            else:
                raise

    def tweak_rule(self, max_tweaks: int = 2):
        """Pick an existing rule, tweak it, and save a new version."""
        with Session(self.engine) as session:
            rules = session.query(Rule).all()
            if not rules:
                print("⚠️ No rules found in database to tweak.")
                return None

            original_rule = random.choice(rules)
            tweaked_json = original_rule.rule_json.copy()

            filters = tweaked_json.get("filters", [])
            if not filters:
                print("⚠️ Selected rule has no filters to tweak.")
                return None

            for _ in range(random.randint(1, max_tweaks)):
                f = random.choice(filters)

                tweak_type = random.choice(["threshold", "sign", "period"])
                if tweak_type == "threshold":
                    f["threshold"] = round(
                        float(f["threshold"]) * random.uniform(0.8, 1.2), 2
                    )
                elif tweak_type == "sign":
                    f["sign"] = random.choice([">", "<", ">=", "<="])
                elif tweak_type == "period":
                    f["period"] = random.choice(["1Y", "3Y", "5Y", "10Y"])

            new_rule = Rule(
                name=f"Tweaked {original_rule.name} {random.randint(1000,9999)}",
                rule_json=tweaked_json,
            )
            session.add(new_rule)
            self._safe_commit(session)

            print(f"🔧 Tweaked rule '{original_rule.name}' → '{new_rule.name}' (ID {new_rule.id})")
            return new_rule

    def create_random_rule(self, max_filters: int = 5):
        """Generate a brand-new random rule from scratch."""
        with Session(self.engine) as session:
            all_filters = session.query(Filter).all()
            if not all_filters:
                print("⚠️ No filters available in database.")
                return None

            num_filters = random.randint(1, max_filters)
            chosen_filters = random.sample(all_filters, num_filters)

            new_filters = []
            for f in chosen_filters:
                sign = random.choice([">", "<", ">=", "<="])

                if f.unit == "%" or "ROE" in f.name or "ROCE" in f.name:
                    threshold = random.randint(5, 30)
                elif "PE" in f.name or "PB" in f.name:
                    threshold = round(random.uniform(5, 40), 2)
                elif "Debt" in f.name:
                    threshold = random.randint(0, 10)
                elif "Dividend" in f.name:
                    threshold = round(random.uniform(0.5, 8.0), 2)
                else:
                    threshold = round(random.uniform(1, 100), 2)

                period = random.choice(["1Y", "3Y", "5Y", "10Y"])

                new_filters.append({
                    "id": f.id,
                    "name": f.name,
                    "sign": sign,
                    "threshold": threshold,
                    "period": period,
                    "consisPeriod": None,
                })

            rule_json = {
                "bt_period_start": "2000",
                "bt_period_end": "2025",
                "sign_mcap": ">=",
                "mcap_threshold": 500,
                "filters": new_filters,
            }

            new_rule = Rule(
                name=f"Random Rule {random.randint(1000,9999)}",
                rule_json=rule_json,
            )
            session.add(new_rule)
            self._safe_commit(session)

            print(f"✨ Created brand-new rule '{new_rule.name}' (ID {new_rule.id})")
            return new_rule

    def evolve(self, n_tweaks: int = 5, n_random: int = 5):
        """Run a full evolution step: tweak rules & create new ones."""
        results = []
        for _ in range(n_tweaks):
            r = self.tweak_rule()
            if r:
                results.append(r)
        for _ in range(n_random):
            r = self.create_random_rule()
            if r:
                results.append(r)
        print(f"✅ Evolution complete: {len(results)} new rules created.")
        return results
