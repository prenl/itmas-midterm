from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Country(Base):
    __tablename__ = "countries"

    id: Mapped[int] = mapped_column(primary_key=True)
    comtrade_code: Mapped[int | None] = mapped_column(index=True, unique=True)
    iso3: Mapped[str] = mapped_column(String(3), unique=True, index=True)
    iso2: Mapped[str | None] = mapped_column(String(2))
    name: Mapped[str] = mapped_column(Text)
    region: Mapped[str | None] = mapped_column(Text)
    income_group: Mapped[str | None] = mapped_column(Text)
    is_group: Mapped[bool] = mapped_column(Boolean, default=False)
    entry_effective_date: Mapped[datetime | None] = mapped_column(Date)
    entry_expired_date: Mapped[datetime | None] = mapped_column(Date)


class HSCode(Base):
    __tablename__ = "hs_codes"
    __table_args__ = (
        CheckConstraint("level IN (2, 4, 6)", name="ck_hs_codes_level"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(10), unique=True, index=True)
    level: Mapped[int] = mapped_column(SmallInteger)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("hs_codes.id"))
    description: Mapped[str] = mapped_column(Text)

    parent: Mapped["HSCode | None"] = relationship(remote_side=[id])


class TradeFlow(Base):
    __tablename__ = "trade_flows"
    __table_args__ = (
        CheckConstraint(
            "flow_type IN ('import', 'export', 're-import', 're-export')",
            name="ck_trade_flows_type",
        ),
        CheckConstraint("month BETWEEN 1 AND 12", name="ck_trade_flows_month"),
        CheckConstraint("frequency IN ('A', 'M')", name="ck_trade_flows_frequency"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reporter_country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), index=True)
    partner_country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), index=True)
    hs_code_id: Mapped[int] = mapped_column(ForeignKey("hs_codes.id"), index=True)
    flow_type: Mapped[str] = mapped_column(String(10))
    frequency: Mapped[str] = mapped_column(String(1))
    year: Mapped[int] = mapped_column(SmallInteger, index=True)
    month: Mapped[int | None] = mapped_column(SmallInteger)
    ref_period: Mapped[int | None] = mapped_column(index=True)
    classification_code: Mapped[str | None] = mapped_column(String(16), default="HS")
    customs_code: Mapped[str | None] = mapped_column(String(16))
    mode_of_transport_code: Mapped[str | None] = mapped_column(String(16))
    partner_2_country_id: Mapped[int | None] = mapped_column(ForeignKey("countries.id"))
    trade_value_usd: Mapped[float] = mapped_column(Numeric(20, 2))
    quantity: Mapped[float | None] = mapped_column(Numeric(20, 3))
    quantity_unit: Mapped[str | None] = mapped_column(String(32))
    alt_quantity: Mapped[float | None] = mapped_column(Numeric(20, 3))
    alt_quantity_unit: Mapped[str | None] = mapped_column(String(32))
    trade_weight_kg: Mapped[float | None] = mapped_column(Numeric(20, 3))
    gross_weight_kg: Mapped[float | None] = mapped_column(Numeric(20, 3))
    is_quantity_estimated: Mapped[bool | None] = mapped_column(Boolean)
    is_trade_weight_estimated: Mapped[bool | None] = mapped_column(Boolean)
    is_reported: Mapped[bool | None] = mapped_column(Boolean)
    is_aggregate: Mapped[bool | None] = mapped_column(Boolean)
    avg_unit_price_usd: Mapped[float | None] = mapped_column(Numeric(20, 4))
    source_system: Mapped[str | None] = mapped_column(Text)


class MacroIndicator(Base):
    __tablename__ = "macro_indicators"
    __table_args__ = (
        UniqueConstraint("country_id", "year", "month", name="uq_macro_country_period"),
        CheckConstraint("month BETWEEN 1 AND 12", name="ck_macro_month"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), index=True)
    year: Mapped[int] = mapped_column(SmallInteger)
    month: Mapped[int | None] = mapped_column(SmallInteger)
    gdp_usd: Mapped[float | None] = mapped_column(Numeric(20, 2))
    gdp_growth_pct: Mapped[float | None] = mapped_column(Numeric(8, 3))
    inflation_pct: Mapped[float | None] = mapped_column(Numeric(8, 3))
    exchange_rate_to_usd: Mapped[float | None] = mapped_column(Numeric(18, 6))
    unemployment_pct: Mapped[float | None] = mapped_column(Numeric(8, 3))
    trade_openness_pct: Mapped[float | None] = mapped_column(Numeric(8, 3))
    logistics_performance_index: Mapped[float | None] = mapped_column(Numeric(8, 3))
    tariff_rate_avg_pct: Mapped[float | None] = mapped_column(Numeric(8, 3))


class CountryPairFeature(Base):
    __tablename__ = "country_pair_features"
    __table_args__ = (
        UniqueConstraint(
            "origin_country_id",
            "destination_country_id",
            name="uq_country_pair_features",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin_country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), index=True)
    destination_country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), index=True)
    distance_km: Mapped[float | None] = mapped_column(Numeric(12, 2))
    shared_border: Mapped[bool] = mapped_column(Boolean, default=False)
    common_language: Mapped[bool] = mapped_column(Boolean, default=False)
    trade_agreement: Mapped[bool] = mapped_column(Boolean, default=False)
    sanctions_risk_score: Mapped[float | None] = mapped_column(Numeric(6, 3))


class RecommendationEvent(Base):
    __tablename__ = "recommendation_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), index=True)
    target_country_id: Mapped[int | None] = mapped_column(ForeignKey("countries.id"), index=True)
    target_hs_code_id: Mapped[int | None] = mapped_column(ForeignKey("hs_codes.id"), index=True)
    recommendation_type: Mapped[str] = mapped_column(String(32))
    model_name: Mapped[str] = mapped_column(Text)
    score: Mapped[float] = mapped_column(Numeric(12, 6))
    generated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    accepted: Mapped[bool | None] = mapped_column(Boolean)
