from __future__ import annotations

from agents.types import MultiAgentState
from trade_pipeline import build_gdp_scenarios, build_recommendations


class OpportunityAnalysisAgent:
    name = "opportunity_analysis_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        print(f"[agent:{self.name}] Building recommendations ...")
        state.recommendations, state.partner_targets = build_recommendations(
            target_country_iso3=state.target_country_iso3,
            target_country_name=state.target_country_name or state.target_country_iso3,
            trade_rows=state.trade_rows,
            upgrade_paths=state.upgrade_paths,
            world_bank=state.world_bank,
            gravity_by_partner=state.gravity_by_partner,
        )
        print(f"[agent:{self.name}] Building GDP scenarios ...")
        state.gdp_scenarios = build_gdp_scenarios(state.recommendations)
        state.coordinator_summary.append(
            f"{self.name}: produced {len(state.recommendations)} upgrade recommendations and "
            f"{len(state.partner_targets)} partner-target rows."
        )
        print(
            f"[agent:{self.name}] Completed: {len(state.recommendations)} recommendations, "
            f"{len(state.partner_targets)} partner targets"
        )
        return state
