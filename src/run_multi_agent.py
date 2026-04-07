from __future__ import annotations

import argparse
from pathlib import Path

from agents import MultiAgentCoordinator
from mvp_data_pipeline import save_comtrade_api_keys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Kazakhstan export-upgrade multi-agent MVP.")
    parser.add_argument(
        "--comtrade-dir",
        default="data/comtrade",
        help="Directory containing UN Comtrade CSV or Parquet exports for Kazakhstan HS6 annual exports.",
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
        default="outputs/multi_agent",
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

    coordinator = MultiAgentCoordinator()
    state = coordinator.run(
        comtrade_dir=Path(args.comtrade_dir),
        gravity_file=Path(args.gravity_file),
        upgrade_paths_file=Path(args.upgrade_paths),
        output_dir=Path(args.output_dir),
    )

    total_exports = sum(float(row.trade_value_usd) for row in state.trade_rows)
    top_export = state.export_base[0] if state.export_base else None
    top_rec = state.recommendations[0] if state.recommendations else None
    top_critic = state.critic_rows[0] if state.critic_rows else None

    print("Kazakhstan export-upgrade multi-agent MVP")
    print(f"Agents executed: 4 specialized agents + coordinator")
    print(f"Trade rows loaded: {len(state.trade_rows)}")
    print(f"Distinct HS6 exports: {len(state.export_base)}")
    print(f"Distinct export partners: {len(state.partners)}")
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
    if top_critic is not None:
        print(
            f"Critic assessment for top recommendation: {top_critic['critic_assessment']} | "
            f"flags: {top_critic['risk_flags']}"
        )
    print(f"Outputs: {args.output_dir}")


if __name__ == "__main__":
    main()
