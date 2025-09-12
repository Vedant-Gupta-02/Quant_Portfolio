"""
Quant_Portfolio — Database Layer
--------------------------------

This module defines the core database schema and loader utilities for your system.
It targets PostgreSQL (recommended) but also works with SQLite for quick tests.

Tables (first cut):
  - companies: master info about each listed company
  - metrics: catalogue of fundamental/technical metrics (e.g., ROE, PE)
  - financials: long-format store of metric values per period (yearly/quarterly/TTM)
  - prices: daily (or monthly) OHLCV + dividends
  - corporate_actions: splits/bonuses/special dividends etc.

Indexes & constraints are added for speed and integrity.

Utilities:
  - init_db(uri): create all tables
  - load_* helpers to ingest pandas DataFrames or CSVs efficiently
  - simple upsert routines keyed by natural unique constraints

You can extend this later with rule_definitions, backtests, and portfolio tables.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Iterable, Optional
from datetime import datetime
import json
from typing import List, Dict

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    JSON,
    TIMESTAMP,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session, Relationship

# ----------------------------
# Base & Enums
# ----------------------------

class Base(DeclarativeBase):
    pass

class MetricFreq(enum.StrEnum):
    yearly = "yearly"
    quarterly = "quarterly"
    ttm = "ttm"  # trailing 12 months

class PeriodType(enum.StrEnum):
    fiscal_year = "fiscal_year"
    fiscal_quarter = "fiscal_quarter"
    calendar_month = "calendar_month"

class CorporateActionType(enum.StrEnum):
    split = "split"
    bonus = "bonus"
    cash_dividend = "cash_dividend"
    rights = "rights"

# ----------------------------
# Tables
# ----------------------------

class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    isin: Mapped[Optional[str]] = mapped_column(String(24), unique=True)
    sector: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    industry: Mapped[Optional[str]] = mapped_column(String(96), index=True)
    country: Mapped[Optional[str]] = mapped_column(String(64), default="India")
    listing_date: Mapped[Optional[Date]] = mapped_column(Date)
    delisted_date: Mapped[Optional[Date]] = mapped_column(Date)
    metadata_json: Mapped[Optional[str]] = mapped_column(Text)  # optional freeform

    financials: Mapped[list[Financial]] = relationship(back_populates="company", cascade="all, delete-orphan")
    prices: Mapped[list[Price]] = relationship(back_populates="company", cascade="all, delete-orphan")
    actions: Mapped[list[CorporateAction]] = relationship(back_populates="company", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Company {self.ticker}>"

class Metric(Base):
    """Catalogue of metrics.

    Map your existing immutable metric IDs here (from your Model Set Context).
    Keep ids stable across runs to support rule storage/backtests.
    """
    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    unit: Mapped[Optional[str]] = mapped_column(String(32))  # %, x, ₹, etc.
    frequency: Mapped[MetricFreq] = mapped_column(Enum(MetricFreq), default=MetricFreq.yearly)

    def __repr__(self) -> str:
        return f"<Metric {self.id}:{self.name}>"

class Financial(Base):
    """Long-format store of ratio values.

    period_label examples:
      - FY2019, FY2020, Q1FY2024, TTM2024Q2, etc.
    period_start/end allow precise dating. Use None for TTM end-only if needed.
    """
    __tablename__ = "financials"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), index=True)
    metric_id: Mapped[int] = mapped_column(ForeignKey("metrics.id", ondelete="RESTRICT"), index=True)

    period_type: Mapped[PeriodType] = mapped_column(Enum(PeriodType))
    period_label: Mapped[str] = mapped_column(String(24), index=True)
    period_start: Mapped[Optional[Date]] = mapped_column(Date)
    period_end: Mapped[Optional[Date]] = mapped_column(Date)

    value: Mapped[float] = mapped_column(Float)
    source: Mapped[Optional[str]] = mapped_column(String(64))
    updated_at: Mapped[Optional[DateTime]] = mapped_column(DateTime)

    company: Mapped[Company] = relationship(back_populates="financials")
    metric: Mapped[Metric] = relationship()

    __table_args__ = (
        UniqueConstraint("company_id", "metric_id", "period_label", name="uq_fin_co_metric_periodlabel"),
        Index("ix_fin_co_metric_period", "company_id", "metric_id", "period_end"),
    )

class Price(Base):
    __tablename__ = "prices"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), index=True)
    date: Mapped[Date] = mapped_column(Date, index=True)

    open: Mapped[Optional[Numeric]] = mapped_column(Numeric(18, 6))
    high: Mapped[Optional[Numeric]] = mapped_column(Numeric(18, 6))
    low: Mapped[Optional[Numeric]] = mapped_column(Numeric(18, 6))
    close: Mapped[Numeric] = mapped_column(Numeric(18, 6))
    volume: Mapped[Optional[BigInteger]] = mapped_column(BigInteger)
    dividend: Mapped[Optional[Numeric]] = mapped_column(Numeric(18, 6), default=0)
    adj_close: Mapped[Optional[Numeric]] = mapped_column(Numeric(18, 6))

    company: Mapped[Company] = relationship(back_populates="prices")

    __table_args__ = (
        UniqueConstraint("company_id", "date", name="uq_price_company_date"),
        Index("ix_price_company_date", "company_id", "date"),
    )

class CorporateAction(Base):
    __tablename__ = "corporate_actions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), index=True)
    action_date: Mapped[Date] = mapped_column(Date, index=True)
    action_type: Mapped[CorporateActionType] = mapped_column(Enum(CorporateActionType))
    factor_numer: Mapped[Optional[Integer]] = mapped_column(Integer)  # e.g., split 1 -> numer=1
    factor_denom: Mapped[Optional[Integer]] = mapped_column(Integer)  #        split 5 -> denom=5
    cash_amount: Mapped[Optional[Numeric]] = mapped_column(Numeric(18, 6))  # for dividends
    notes: Mapped[Optional[str]] = mapped_column(Text)

    company: Mapped[Company] = relationship(back_populates="actions")

    __table_args__ = (
        Index("ix_action_company_date", "company_id", "action_date"),
    )
    

class Rule(Base):
    __tablename__ = "rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    rule_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, default=datetime.utcnow
    )
    yearly_portfolios: Mapped[list[YearlyPortfolio]] = relationship(back_populates="rule", cascade="all, delete-orphan")
    backtest_metrics: Mapped[list[RuleBacktestMetric]] = relationship(back_populates="rule", cascade="all, delete-orphan")


class Filter(Base):
    __tablename__ = "filters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(String(64), nullable=True)
    unit: Mapped[str] = mapped_column(String(32), nullable=True)


class YearlyPortfolio(Base):
    __tablename__ = "yearly_portfolios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("rules.id"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    weights: Mapped[dict[str, dict[str, float]]] = mapped_column(JSON, nullable=False)

    rule: Mapped["Rule"] = relationship(back_populates="yearly_portfolios")


class RuleBacktestMetric(Base):
    __tablename__ = "rule_backtest_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("rules.id"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    scheme: Mapped[str] = mapped_column(String, nullable=False)  # <-- NEW COLUMN

    mean_return: Mapped[float] = mapped_column(Float, nullable=True)
    median_return: Mapped[float] = mapped_column(Float, nullable=True)
    volatility: Mapped[float] = mapped_column(Float, nullable=True)
    sharpe: Mapped[float] = mapped_column(Float, nullable=True)

    rule: Mapped["Rule"] = relationship(back_populates="backtest_metrics")


# ----------------------------
# Engine + Session helpers
# ----------------------------

def init_db(uri: str) -> None:
    """Create all tables. Example URIs:
        - Postgres: "postgresql+psycopg://user:pass@localhost:5432/quant_portfolio"
        - SQLite  : "sqlite:///quant_portfolio.db"
    """
    engine = create_engine(uri, echo=False, future=True)
    Base.metadata.create_all(engine)


def get_session(uri: str) -> Session:
    engine = create_engine(uri, echo=False, future=True)
    return Session(engine)

# ----------------------------
# Loaders (pandas-friendly)
# ----------------------------

@dataclass
class UpsertResult:
    inserted: int
    updated: int


def upsert_companies(sess: Session, rows: Iterable[dict]) -> UpsertResult:
    """rows: iterable of dicts with keys:
      ticker (required), name (required), sector, industry, isin, country, listing_date
    Uses ticker as natural key.
    """
    inserted = updated = 0
    for r in rows:
        obj: Optional[Company] = sess.query(Company).filter_by(ticker=r["ticker"]).one_or_none()
        if obj is None:
            obj = Company(**r)
            sess.add(obj)
            inserted += 1
        else:
            for k, v in r.items():
                setattr(obj, k, v)
            updated += 1
    sess.commit()
    return UpsertResult(inserted, updated)


def upsert_metrics(sess: Session, rows: Iterable[dict]) -> UpsertResult:
    """rows: iterable of dicts with keys: id, name, description, unit, frequency"""
    inserted = updated = 0
    for r in rows:
        obj: Optional[Metric] = sess.get(Metric, r["id"]) or sess.query(Metric).filter_by(name=r["name"]).one_or_none()
        if obj is None:
            obj = Metric(**r)
            sess.add(obj)
            inserted += 1
        else:
            for k, v in r.items():
                setattr(obj, k, v)
            updated += 1
    sess.commit()
    return UpsertResult(inserted, updated)


def upsert_financials(sess: Session, rows: Iterable[dict], chunk_size: int = 10_000) -> UpsertResult:
    """rows keys: company_id|ticker, metric_id, period_type, period_label, period_start, period_end, value, source
    Prefers company_id; will look up by ticker if provided.
    Enforces uniqueness on (company_id, metric_id, period_label).
    """
    inserted = updated = 0
    buffer = []

    def flush(buf):
        nonlocal inserted, updated
        if not buf:
            return
        # fetch existing keys
        keys = [(b["company_id"], b["metric_id"], b["period_label"]) for b in buf]
        existing = {
            (f.company_id, f.metric_id, f.period_label): f
            for f in sess.query(Financial)
            .filter(Financial.company_id.in_([k[0] for k in keys]))
            .filter(Financial.metric_id.in_([k[1] for k in keys]))
            .filter(Financial.period_label.in_([k[2] for k in keys]))
        }
        for b in buf:
            key = (b["company_id"], b["metric_id"], b["period_label"])
            obj = existing.get(key)
            if obj is None:
                sess.add(Financial(**b))
                inserted += 1
            else:
                for k, v in b.items():
                    setattr(obj, k, v)
                updated += 1
        sess.commit()
        buf.clear()

    for r in rows:
        rec = dict(r)
        if "company_id" not in rec:
            # allow ticker mapping
            t = rec.pop("ticker")
            comp = sess.query(Company.id).filter_by(ticker=t).one()
            rec["company_id"] = comp[0]
        buffer.append(rec)
        if len(buffer) >= chunk_size:
            flush(buffer)
    flush(buffer)
    return UpsertResult(inserted, updated)


def upsert_prices(sess: Session, rows: Iterable[dict], chunk_size: int = 50_000) -> UpsertResult:
    """rows keys: company_id|ticker, date, open, high, low, close, volume, dividend, adj_close"""
    inserted = updated = 0
    buffer = []

    def flush(buf):
        nonlocal inserted, updated
        if not buf:
            return
        keys = [(b["company_id"], b["date"]) for b in buf]
        existing = {
            (p.company_id, p.date): p
            for p in sess.query(Price)
            .filter(Price.company_id.in_([k[0] for k in keys]))
            .filter(Price.date.in_([k[1] for k in keys]))
        }
        for b in buf:
            key = (b["company_id"], b["date"])
            obj = existing.get(key)
            if obj is None:
                sess.add(Price(**b))
                inserted += 1
            else:
                for k, v in b.items():
                    setattr(obj, k, v)
                updated += 1
        sess.commit()
        buf.clear()

    for r in rows:
        rec = dict(r)
        if "company_id" not in rec:
            t = rec.pop("ticker")
            comp = sess.query(Company.id).filter_by(ticker=t).one()
            rec["company_id"] = comp[0]
        buffer.append(rec)
        if len(buffer) >= chunk_size:
            flush(buffer)
    flush(buffer)
    return UpsertResult(inserted, updated)


def upsert_corporate_actions(sess: Session, rows: Iterable[dict]) -> UpsertResult:
    inserted = updated = 0
    for r in rows:
        # Unique by (company_id, action_date, action_type)
        if "company_id" not in r:
            t = r.pop("ticker")
            comp = sess.query(Company.id).filter_by(ticker=t).one()
            r["company_id"] = comp[0]
        obj = (
            sess.query(CorporateAction)
            .filter_by(company_id=r["company_id"], action_date=r["action_date"], action_type=r["action_type"]) 
            .one_or_none()
        )
        if obj is None:
            sess.add(CorporateAction(**r))
            inserted += 1
        else:
            for k, v in r.items():
                setattr(obj, k, v)
            updated += 1
    sess.commit()
    return UpsertResult(inserted, updated)

def upsert_rules(sess: Session, rules: List[Dict]) -> int:
    count = 0
    for r in rules:
        obj: Optional[Rule] = sess.query(Rule).filter_by(name=r["name"]).one_or_none()
        if obj:
            obj.rule_json = r["rule_json"]
        else:
            obj = Rule(**r)
            sess.add(obj)
        count += 1
    sess.commit()
    return count

def upsert_filters(sess: Session, filters: List[Dict]) -> int:
    count = 0
    for f in filters:
        obj: Optional[Filter] = sess.query(Filter).filter_by(name=f["name"]).one_or_none()
        if obj:
            obj.description = f.get("description", obj.description)
            obj.category = f.get("category", obj.category)
            obj.unit = f.get("unit", obj.unit)
            obj.metric_id = f.get("metric_id", obj.metric_id)
        else:
            obj = Filter(**f)
            sess.add(obj)
        count += 1
    sess.commit()
    return count


# ----------------------------
# Minimal CLI for quick starts
# ----------------------------

if __name__ == "__main__":
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="Init DB and/or load CSVs")
    parser.add_argument("--uri", required=True, help="SQLAlchemy URI e.g. postgresql+psycopg://user:pass@localhost/db")
    parser.add_argument("--init", action="store_true", help="Create tables")
    parser.add_argument("--companies_csv")
    parser.add_argument("--metrics_csv")
    parser.add_argument("--financials_csv")
    parser.add_argument("--prices_csv")
    parser.add_argument("--rules_csv")
    parser.add_argument("--filters_csv")

    args = parser.parse_args()

    if args.init:
        init_db(args.uri)
        print("✅ Tables created")

    with get_session(args.uri) as s:
        if args.companies_csv:
            df = pd.read_csv(args.companies_csv)
            res = upsert_companies(s, df.to_dict("records"))
            print("companies:", res)
        if args.metrics_csv:
            df = pd.read_csv(args.metrics_csv)
            res = upsert_metrics(s, df.to_dict("records"))
            print("metrics:", res)
        if args.financials_csv:
            df = pd.read_csv(args.financials_csv, parse_dates=["period_start", "period_end"])
            res = upsert_financials(s, df.to_dict("records"))
            print("financials:", res)
        if args.prices_csv:
            df = pd.read_csv(args.prices_csv, parse_dates=["date"])
            res = upsert_prices(s, df.to_dict("records"))
            print("prices:", res)
        if args.rules_csv:
            df = pd.read_csv(args.rules_csv)
            # assume rules.csv has columns: name, rule_json
            # parse JSON string into dict
            df["rule_json"] = df["rule_json"].apply(json.loads)
            res = upsert_rules(s, df.to_dict("records"))
            print("rules:", res)
        if args.filters_csv:
            df = pd.read_csv(args.filters_csv)
            res = upsert_filters(s, df.to_dict("records"))
            print("filters:", res)
            


            
"""
CSV Schemas (examples)
----------------------
companies.csv
  ticker,name,isin,sector,industry,country,listing_date
  TCS,Tata Consultancy Services,INE467B01029,IT,Software,India,2004-08-25

metrics.csv
  id,name,description,unit,frequency
  101,Average ROE,5Y average ROE,%,yearly

financials.csv (long format)
  ticker,metric_id,period_type,period_label,period_start,period_end,value,source
  TCS,101,fiscal_year,FY2020,2019-04-01,2020-03-31,27.5,CMIE

prices.csv
  ticker,date,open,high,low,close,volume,dividend,adj_close
  TCS,2024-01-02,3900,3950,3880,3940,123456,0,3940
"""
"""
python db_schema.py \
  --uri "postgresql+psycopg://USER:PASS@localhost:5432/quant_portfolio" \
  --companies_csv ./companies.csv \
  --metrics_csv ./metrics.csv \
  --financials_csv ./financials.csv \
  --prices_csv ./prices.csv
"""