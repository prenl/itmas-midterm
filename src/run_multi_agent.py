from __future__ import annotations

import argparse
from pathlib import Path

from agents import MultiAgentCoordinator
from trade_pipeline import save_comtrade_api_keys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the final multi-agent export-upgrade pipeline."
    )
    parser.add_argument(
        "--target-country",
        default="KAZ",
        help="ISO3 code of the country for which recommendations are generated.",
    )
    parser.add_argument(
        "--regional-countries",
        default="",
        help="Optional comma-separated ISO3 list for regional comparison. If empty, all reporters found in the data are used.",
    )
    parser.add_argument(
        "--comtrade-dir",
        default="data/comtrade",
        help="Directory containing UN Comtrade HS6 trade files.",
    )
    parser.add_argument(
        "--gravity-file",
        default="Gravity_csv_V202211/Gravity_V202211.csv",
        help="Path to CEPII Gravity CSV.",
    )
    parser.add_argument(
        "--gravity-countries-file",
        default="Gravity_csv_V202211/Countries_V202211.csv",
        help="Path to CEPII countries reference CSV.",
    )
    parser.add_argument(
        "--upgrade-paths",
        default="data/upgrade_paths.csv",
        help="Curated HS6 upgrade mapping CSV.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/multi_agent_2024_hs6_underdeveloped",
        help="Directory for generated CSV outputs, charts, and coordinator summary.",
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
    target_country_iso3 = args.target_country.strip().upper()
    regional_countries = {
        code.strip().upper() for code in args.regional_countries.split(",") if code.strip()
    }
    if regional_countries and target_country_iso3 not in regional_countries:
        regional_countries.add(target_country_iso3)

    print("[run_multi_agent] Starting run")
    print(f"[run_multi_agent] target_country={target_country_iso3}")
    print(f"[run_multi_agent] comtrade_dir={args.comtrade_dir}")
    print(f"[run_multi_agent] gravity_file={args.gravity_file}")
    print(f"[run_multi_agent] output_dir={args.output_dir}")
    print(
        "[run_multi_agent] regional_countries="
        + (", ".join(sorted(regional_countries)) if regional_countries else "all reporters in dataset")
    )

    coordinator = MultiAgentCoordinator()
    state = coordinator.run(
        target_country_iso3=target_country_iso3,
        allowed_reporters=regional_countries,
        comtrade_dir=Path(args.comtrade_dir),
        gravity_file=Path(args.gravity_file),
        gravity_countries_file=Path(args.gravity_countries_file),
        upgrade_paths_file=Path(args.upgrade_paths),
        output_dir=Path(args.output_dir),
    )

    total_exports = sum(float(row.trade_value_usd) for row in state.trade_rows)
    top_export = state.export_base[0] if state.export_base else None
    top_rec = state.recommendations[0] if state.recommendations else None
    top_critic = state.critic_rows[0] if state.critic_rows else None

    print(f"{state.target_country_name} export-upgrade multi-agent system")
    print(f"Agents executed: 4 specialized agents + coordinator")
    print(f"Target country: {state.target_country_name} ({state.target_country_iso3})")
    print(
        f"Regional reporters considered: {len(state.regional_reporters)}"
        + (
            f" | user filter: {', '.join(sorted(regional_countries))}"
            if regional_countries
            else " | user filter: all reporters in dataset"
        )
    )
    print(f"Trade rows loaded: {len(state.trade_rows)}")
    print(f"Distinct HS6 exports: {len(state.export_base)}")
    print(f"Distinct export partners: {len(state.partners)}")
    print(f"Total export value in sample: USD {total_exports:,.0f}")
    print(f"Recommendations generated: {len(state.recommendations)}")
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
    if top_critic is not None:
        print(
            f"Critic assessment for top recommendation: {top_critic['critic_assessment']} | "
            f"flags: {top_critic['risk_flags']}"
        )
    print(f"Outputs: {args.output_dir}")


if __name__ == "__main__":
    main()
