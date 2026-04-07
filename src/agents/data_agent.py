from __future__ import annotations

from agents.types import MultiAgentState
from trade_pipeline import (
    enrich_trade_rows_with_country_reference,
    fetch_world_bank_indicators,
    load_cepii_country_reference,
    load_cepii_gravity,
    load_comtrade_exports_from_dir,
    load_upgrade_paths,
    summarize_export_base,
)


class DataAcquisitionAgent:
    name = "data_acquisition_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        state.trade_rows = load_comtrade_exports_from_dir(state.comtrade_dir)
        iso3_by_numeric, name_by_iso3 = load_cepii_country_reference(state.gravity_countries_file)
        state.trade_rows = enrich_trade_rows_with_country_reference(
            state.trade_rows,
            iso3_by_numeric=iso3_by_numeric,
            name_by_iso3=name_by_iso3,
        )
        state.export_base = summarize_export_base(state.trade_rows)
        state.partners = {row.partner_iso3 for row in state.trade_rows if row.partner_iso3}
        state.world_bank = fetch_world_bank_indicators({"KAZ"} | state.partners)
        state.gravity_by_partner = load_cepii_gravity(
            state.gravity_file,
            reporter_iso3="KAZ",
            partners=state.partners,
        )
        state.upgrade_paths = load_upgrade_paths(state.upgrade_paths_file)
        state.coordinator_summary.append(
            f"{self.name}: loaded {len(state.trade_rows)} trade rows, "
            f"{len(state.export_base)} HS6 exports, and {len(state.partners)} partner countries."
        )
        return state
