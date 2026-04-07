from __future__ import annotations

from agents.types import MultiAgentState
from trade_pipeline import (
    enrich_trade_rows_with_country_reference,
    fetch_world_bank_indicators,
    load_cepii_country_reference,
    load_cepii_gravity,
    load_comtrade_trade_rows_from_dir,
    load_upgrade_paths,
    summarize_export_base,
)


class DataAcquisitionAgent:
    name = "data_acquisition_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        print(f"[agent:{self.name}] Loading CEPII country reference ...")
        iso3_by_numeric, name_by_iso3 = load_cepii_country_reference(state.gravity_countries_file)
        print(f"[agent:{self.name}] Loading Comtrade trade rows ...")
        state.trade_rows = load_comtrade_trade_rows_from_dir(state.comtrade_dir)
        print(f"[agent:{self.name}] Enriching country codes ...")
        state.trade_rows = enrich_trade_rows_with_country_reference(
            state.trade_rows,
            iso3_by_numeric=iso3_by_numeric,
            name_by_iso3=name_by_iso3,
        )
        if state.allowed_reporters:
            state.trade_rows = [
                row for row in state.trade_rows if row.reporter_iso3 in state.allowed_reporters
            ]
        state.regional_reporters = {row.reporter_iso3 for row in state.trade_rows if row.reporter_iso3}
        state.target_country_name = name_by_iso3.get(state.target_country_iso3, state.target_country_iso3)
        state.target_export_rows = [
            row
            for row in state.trade_rows
            if row.reporter_iso3 == state.target_country_iso3 and row.flow == "export"
        ]
        if not state.target_export_rows:
            raise ValueError(
                f"No export rows were found for target country {state.target_country_iso3} in {state.comtrade_dir}."
            )
        state.export_base = summarize_export_base(state.target_export_rows)
        state.partners = {
            row.partner_iso3
            for row in state.target_export_rows
            if row.partner_iso3 and row.partner_iso3 != "WLD"
        }
        print(
            f"[agent:{self.name}] Target exporters prepared: {len(state.target_export_rows)} export rows, "
            f"{len(state.export_base)} HS6 products, {len(state.partners)} partners"
        )
        print(f"[agent:{self.name}] Fetching World Bank data ...")
        state.world_bank = fetch_world_bank_indicators(
            {state.target_country_iso3} | state.partners | state.regional_reporters
        )
        print(f"[agent:{self.name}] Loading CEPII gravity pairs ...")
        state.gravity_by_partner = load_cepii_gravity(
            state.gravity_file,
            reporter_iso3=state.target_country_iso3,
            partners=(state.partners | state.regional_reporters) - {state.target_country_iso3, "WLD"},
        )
        print(f"[agent:{self.name}] Loading upgrade paths ...")
        state.upgrade_paths = load_upgrade_paths(state.upgrade_paths_file)
        state.coordinator_summary.append(
            f"{self.name}: loaded {len(state.trade_rows)} trade rows, "
            f"{len(state.export_base)} target-country HS6 exports, "
            f"{len(state.partners)} target export partners, and {len(state.regional_reporters)} regional reporters."
        )
        print(f"[agent:{self.name}] Completed")
        return state
