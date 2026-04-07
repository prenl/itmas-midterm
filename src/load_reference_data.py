from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models import Base, Country, HSCode, MacroIndicator, TradeFlow


def _add_countries(session: Session) -> dict[str, Country]:
    countries = {
        "KAZ": Country(
            comtrade_code=398,
            iso3="KAZ",
            iso2="KZ",
            name="Kazakhstan",
            region="Central Asia",
            income_group="UMI",
        ),
        "CHN": Country(
            comtrade_code=156,
            iso3="CHN",
            iso2="CN",
            name="China",
            region="East Asia",
            income_group="UMI",
        ),
        "DEU": Country(
            comtrade_code=276,
            iso3="DEU",
            iso2="DE",
            name="Germany",
            region="Europe",
            income_group="HI",
        ),
        "TUR": Country(
            comtrade_code=792,
            iso3="TUR",
            iso2="TR",
            name="Turkey",
            region="Europe / Asia",
            income_group="UMI",
        ),
        "UZB": Country(
            comtrade_code=860,
            iso3="UZB",
            iso2="UZ",
            name="Uzbekistan",
            region="Central Asia",
            income_group="LMI",
        ),
    }
    session.add_all(countries.values())
    session.flush()
    return countries


def _add_hs_codes(session: Session) -> dict[str, HSCode]:
    hs2_ores = HSCode(code="26", level=2, description="Ores, slag and ash")
    hs4_copper_ore = HSCode(code="2603", level=4, description="Copper ores and concentrates")

    hs2_copper = HSCode(code="74", level=2, description="Copper and articles thereof")
    hs4_refined_copper = HSCode(code="7403", level=4, description="Refined copper and copper alloys, unwrought")
    hs4_copper_wire = HSCode(code="7408", level=4, description="Copper wire")

    hs2_cereals = HSCode(code="10", level=2, description="Cereals")
    hs4_wheat = HSCode(code="1001", level=4, description="Wheat and meslin")

    hs2_milling = HSCode(code="11", level=2, description="Products of the milling industry")
    hs4_flour = HSCode(code="1101", level=4, description="Wheat or meslin flour")

    session.add_all(
        [
            hs2_ores,
            hs4_copper_ore,
            hs2_copper,
            hs4_refined_copper,
            hs4_copper_wire,
            hs2_cereals,
            hs4_wheat,
            hs2_milling,
            hs4_flour,
        ]
    )
    session.flush()

    hs4_copper_ore.parent_id = hs2_ores.id
    hs4_refined_copper.parent_id = hs2_copper.id
    hs4_copper_wire.parent_id = hs2_copper.id
    hs4_wheat.parent_id = hs2_cereals.id
    hs4_flour.parent_id = hs2_milling.id

    hs_codes = {
        "260300": HSCode(
            code="260300",
            level=6,
            parent_id=hs4_copper_ore.id,
            description="Copper ores and concentrates",
        ),
        "740311": HSCode(
            code="740311",
            level=6,
            parent_id=hs4_refined_copper.id,
            description="Refined copper cathodes and sections of cathodes",
        ),
        "740811": HSCode(
            code="740811",
            level=6,
            parent_id=hs4_copper_wire.id,
            description="Refined copper wire of which the maximum cross-sectional dimension exceeds 6 mm",
        ),
        "100199": HSCode(
            code="100199",
            level=6,
            parent_id=hs4_wheat.id,
            description="Other wheat and meslin",
        ),
        "110100": HSCode(
            code="110100",
            level=6,
            parent_id=hs4_flour.id,
            description="Wheat or meslin flour",
        ),
    }
    session.add_all(hs_codes.values())
    session.flush()
    return hs_codes


def _add_macro_indicators(session: Session, countries: dict[str, Country]) -> None:
    rows = [
        ("KAZ", 1, 261_000_000_000, 4.8, 8.7, 470.0, 4.9, 62.0, 2.8, 2.7),
        ("CHN", 2, 17_800_000_000_000, 5.0, 0.4, 7.12, 5.1, 37.0, 3.6, 3.4),
        ("DEU", 3, 4_500_000_000_000, 0.2, 2.4, 0.92, 3.2, 88.0, 4.1, 2.1),
        ("TUR", 4, 1_100_000_000_000, 3.3, 58.5, 32.0, 8.8, 67.0, 3.1, 4.8),
        ("UZB", 5, 115_000_000_000, 6.0, 9.8, 12_500.0, 5.7, 71.0, 2.7, 5.0),
    ]
    for iso3, row_id, gdp, growth, inflation, fx, unemployment, openness, lpi, tariff in rows:
        session.add(
            MacroIndicator(
                id=row_id,
                country_id=countries[iso3].id,
                year=2024,
                month=12,
                gdp_usd=gdp,
                gdp_growth_pct=growth,
                inflation_pct=inflation,
                exchange_rate_to_usd=fx,
                unemployment_pct=unemployment,
                trade_openness_pct=openness,
                logistics_performance_index=lpi,
                tariff_rate_avg_pct=tariff,
            )
        )


def _add_trade_flows(
    session: Session,
    countries: dict[str, Country],
    hs_codes: dict[str, HSCode],
) -> None:
    trade_rows = [
        (1, "KAZ", "CHN", "260300", 2024, 12, 900000, 3200, "tons", 3200000, 3230000, 281.25),
        (2, "KAZ", "DEU", "260300", 2024, 11, 650000, 2200, "tons", 2200000, 2225000, 295.45),
        (3, "KAZ", "UZB", "100199", 2024, 12, 550000, 1500, "tons", 1500000, 1514000, 366.67),
        (4, "KAZ", "TUR", "100199", 2024, 10, 410000, 1100, "tons", 1100000, 1112000, 372.73),
        (5, "CHN", "DEU", "740311", 2024, 12, 1500000, 1800, "tons", 1800000, 1812000, 833.33),
        (6, "CHN", "TUR", "740311", 2024, 12, 1100000, 1300, "tons", 1300000, 1310000, 846.15),
        (7, "CHN", "DEU", "740811", 2024, 11, 1800000, 1200, "tons", 1200000, 1213000, 1500.00),
        (8, "CHN", "UZB", "110100", 2024, 12, 420000, 900, "tons", 900000, 907000, 466.67),
        (9, "DEU", "TUR", "740811", 2024, 12, 1600000, 1000, "tons", 1000000, 1008000, 1600.00),
        (10, "DEU", "UZB", "110100", 2024, 11, 300000, 700, "tons", 700000, 706000, 428.57),
        (11, "TUR", "UZB", "110100", 2024, 12, 510000, 1000, "tons", 1000000, 1009000, 510.00),
        (12, "TUR", "DEU", "740811", 2024, 10, 950000, 650, "tons", 650000, 655000, 1461.54),
        (13, "UZB", "TUR", "100199", 2024, 12, 230000, 700, "tons", 700000, 705000, 328.57),
    ]

    for (
        row_id,
        reporter,
        partner,
        hs_code,
        year,
        month,
        trade_value,
        quantity,
        quantity_unit,
        trade_weight,
        gross_weight,
        unit_price,
    ) in trade_rows:
        session.add(
            TradeFlow(
                id=row_id,
                reporter_country_id=countries[reporter].id,
                partner_country_id=countries[partner].id,
                hs_code_id=hs_codes[hs_code].id,
                flow_type="export",
                frequency="A",
                year=year,
                month=month,
                ref_period=year * 10000 + 101,
                classification_code="HS",
                customs_code="C00",
                mode_of_transport_code="0",
                trade_value_usd=trade_value,
                quantity=quantity,
                quantity_unit=quantity_unit,
                alt_quantity=0,
                alt_quantity_unit="N/A",
                trade_weight_kg=trade_weight,
                gross_weight_kg=gross_weight,
                is_quantity_estimated=False,
                is_trade_weight_estimated=False,
                is_reported=True,
                is_aggregate=False,
                avg_unit_price_usd=unit_price,
                source_system="UN Comtrade demo seed",
            )
        )


def load_reference_data(database_url: str | None = None) -> None:
    effective_database_url = database_url or os.getenv(
        "TRADE_DATABASE_URL",
        "sqlite:///trade_system.db",
    )
    engine = create_engine(effective_database_url, echo=False)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        countries = _add_countries(session)
        hs_codes = _add_hs_codes(session)
        _add_macro_indicators(session, countries)
        _add_trade_flows(session, countries, hs_codes)
        session.commit()


if __name__ == "__main__":
    load_reference_data()
