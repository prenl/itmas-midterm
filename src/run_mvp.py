from __future__ import annotations

import argparse
from pathlib import Path

from mvp_data_pipeline import (
    build_recommendations,
    build_gdp_scenarios,
    ensure_output_dirs,
    fetch_world_bank_indicators,
    load_cepii_gravity,
    load_comtrade_exports_from_dir,
    load_upgrade_paths,
    render_charts,
    save_comtrade_api_keys,
    summarize_export_base,
    write_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Kazakhstan export-upgrade MVP pipeline.")
    parser.add_argument(
        "--comtrade-dir",
        default="data/comtrade",
        help="Directory containing one or more UN Comtrade CSV exports for Kazakhstan HS6 annual exports.",
    )
    parser.add_argument(
        "--gravity-file",
        default="Gravity_csv_V202211/Gravity_V202211.csv",
        help="Path to CEPII Gravity CSV.",
    )
    parser.add_argument(
        "--upgrade-paths",
        default="data/upgrade_paths.csv",
        help="Curated HS6 upgrade mapping CSV.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/mvp",
        help="Directory for generated CSV outputs and charts.",
    )
    parser.add_argument(
        "--comtrade-primary-key",
        default=None,
        help="Optional UN Comtrade API primary key. Stored only in the process environment.",
    )
    parser.add_argument(
        "--comtrade-secondary-key",
        default=None,
        help="Optional UN Comtrade API secondary key. Stored only in the process environment.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    save_comtrade_api_keys(args.comtrade_primary_key, args.comtrade_secondary_key)

    comtrade_dir = Path(args.comtrade_dir)
    gravity_file = Path(args.gravity_file)
    upgrade_paths_file = Path(args.upgrade_paths)
    output_dir = Path(args.output_dir)
    csv_dir, charts_dir = ensure_output_dirs(output_dir)

    trade_rows = load_comtrade_exports_from_dir(comtrade_dir)
    export_base = summarize_export_base(trade_rows)
    partners = {row.partner_iso3 for row in trade_rows if row.partner_iso3}
    world_bank = fetch_world_bank_indicators({"KAZ"} | partners)
    gravity_by_partner = load_cepii_gravity(gravity_file, reporter_iso3="KAZ", partners=partners)
    upgrade_paths = load_upgrade_paths(upgrade_paths_file)
    recommendations, partner_targets = build_recommendations(
        trade_rows=trade_rows,
        upgrade_paths=upgrade_paths,
        world_bank=world_bank,
        gravity_by_partner=gravity_by_partner,
    )

    write_csv(csv_dir / "kaz_export_base.csv", export_base)
    write_csv(csv_dir / "upgrade_recommendations.csv", recommendations)
    write_csv(csv_dir / "upgrade_partner_targets.csv", partner_targets)
    write_csv(csv_dir / "gdp_growth_scenarios.csv", build_gdp_scenarios(recommendations))
    render_charts(export_base, recommendations, charts_dir)

    total_exports = sum(float(row.trade_value_usd) for row in trade_rows)
    top_export = export_base[0] if export_base else None
    top_rec = recommendations[0] if recommendations else None

    print("Kazakhstan export-upgrade MVP")
    print(f"Comtrade rows loaded: {len(trade_rows)}")
    print(f"Distinct HS6 products: {len(export_base)}")
    print(f"Distinct export partners: {len(partners)}")
    print(f"Total export value in sample: USD {total_exports:,.0f}")
    if top_export is not None:
        print(
            f"Top current export: {top_export['cmd_code']} | {top_export['cmd_desc']} | "
            f"USD {float(top_export['trade_value_usd']):,.0f}"
        )
    if top_rec is not None:
        print(
            f"Top recommended upgrade: {top_rec['upgrade_hs6']} | {top_rec['upgrade_label']} | "
            f"estimated export uplift USD {float(top_rec['estimated_export_uplift_usd']):,.0f}"
        )
    print(f"CSV outputs: {csv_dir}")
    print(f"Charts: {charts_dir}")


if __name__ == "__main__":
    main()
