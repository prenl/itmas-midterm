from __future__ import annotations

import argparse
import os
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from economic_complexity import build_export_matrix, compute_eci_pci
from load_reference_data import load_reference_data
from models import Country, HSCode, TradeFlow
from upgrade_recommender import recommend_export_upgrades


def _resolve_database_url() -> str:
    return os.getenv("TRADE_DATABASE_URL", "sqlite:///trade_system_demo.db")


def _remove_existing_sqlite_file(database_url: str) -> None:
    sqlite_prefix = "sqlite:///"
    if not database_url.startswith(sqlite_prefix):
        return

    db_path = Path(database_url.removeprefix(sqlite_prefix))
    if db_path.exists():
        db_path.unlink()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show export upgrade recommendations for a selected country.",
    )
    parser.add_argument(
        "--country",
        default="KAZ",
        help="ISO3 country code from the demo dataset: KAZ, CHN, DEU, TUR, UZB.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of recommendations to print.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    database_url = _resolve_database_url()
    os.environ["TRADE_DATABASE_URL"] = database_url

    _remove_existing_sqlite_file(database_url)
    load_reference_data(database_url)

    engine = create_engine(database_url, echo=False)
    with Session(engine) as session:
        countries = session.execute(select(Country).order_by(Country.id)).scalars().all()
        hs6_codes = session.execute(
            select(HSCode).where(HSCode.level == 6).order_by(HSCode.id)
        ).scalars().all()
        export_flows = session.execute(
            select(TradeFlow).where(TradeFlow.flow_type == "export").order_by(TradeFlow.id)
        ).scalars().all()

    country_by_iso3 = {country.iso3: country for country in countries}
    selected_country = country_by_iso3.get(args.country.upper())
    if selected_country is None:
        supported = ", ".join(sorted(country_by_iso3))
        raise SystemExit(
            f"Unknown country '{args.country}'. Supported demo countries: {supported}"
        )

    country_index_by_id = {country.id: index for index, country in enumerate(countries)}
    product_index_by_id = {product.id: index for index, product in enumerate(hs6_codes)}
    product_index_by_code = {product.code: index for index, product in enumerate(hs6_codes)}
    product_code_by_index = {index: product.code for index, product in enumerate(hs6_codes)}
    product_name_by_code = {product.code: product.description for product in hs6_codes}

    export_matrix = build_export_matrix(
        [
            (
                country_index_by_id[flow.reporter_country_id],
                product_index_by_id[flow.hs_code_id],
                float(flow.trade_value_usd),
            )
            for flow in export_flows
        ],
        num_countries=len(countries),
        num_products=len(hs6_codes),
    )
    complexity = compute_eci_pci(export_matrix)

    exporters_by_product: dict[str, set[str]] = {product.code: set() for product in hs6_codes}
    importers_by_product: dict[str, set[str]] = {product.code: set() for product in hs6_codes}
    selected_country_exports: dict[str, float] = {}
    country_name_by_id = {country.id: country.name for country in countries}
    product_code_by_id = {product.id: product.code for product in hs6_codes}

    for flow in export_flows:
        product_code = product_code_by_id[flow.hs_code_id]
        exporters_by_product[product_code].add(country_name_by_id[flow.reporter_country_id])
        importers_by_product[product_code].add(country_name_by_id[flow.partner_country_id])
        if flow.reporter_country_id == selected_country.id:
            selected_country_exports[product_code] = (
                selected_country_exports.get(product_code, 0.0) + float(flow.trade_value_usd)
            )

    selected_country_idx = country_index_by_id[selected_country.id]
    recommendations = recommend_export_upgrades(
        country_idx=selected_country_idx,
        country_name=selected_country.name,
        product_index_by_code=product_index_by_code,
        product_code_by_index=product_code_by_index,
        product_name_by_code=product_name_by_code,
        exporters_by_product=exporters_by_product,
        importers_by_product=importers_by_product,
        country_exports_by_product=selected_country_exports,
        complexity=complexity,
        top_k=args.top_k,
    )

    current_exports = sorted(
        selected_country_exports.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    print(f"Country: {selected_country.name} ({selected_country.iso3})")
    print(f"Country ECI: {round(float(complexity.eci[selected_country_idx].item()), 4)}")
    print("Current export base:")
    for product_code, trade_value in current_exports:
        print(
            f"- {product_code}: {product_name_by_code[product_code]} | "
            f"export USD {trade_value:,.0f}"
        )

    print("\nRecommended export upgrades:")
    if not recommendations:
        print("- No recommendations found in the current demo dataset.")
        return

    for index, recommendation in enumerate(recommendations, start=1):
        markets = ", ".join(recommendation.likely_markets)
        print(
            f"\n{index}. {recommendation.product_code} | {recommendation.product_description} | "
            f"opportunity score {recommendation.opportunity_score:.3f} | PCI {recommendation.pci:.3f}"
        )
        print(
            f"   Base product: {recommendation.based_on_code} | "
            f"{recommendation.based_on_description}"
        )
        print(f"   Potential markets in demo data: {markets}")


if __name__ == "__main__":
    main()
