from __future__ import annotations

from agents.types import MultiAgentState
from trade_pipeline import build_gdp_scenarios, build_recommendations


class OpportunityAnalysisAgent:
    name = "opportunity_analysis_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        state.recommendations, state.partner_targets = build_recommendations(
            trade_rows=state.trade_rows,
            upgrade_paths=state.upgrade_paths,
            world_bank=state.world_bank,
            gravity_by_partner=state.gravity_by_partner,
        )
        state.gdp_scenarios = build_gdp_scenarios(state.recommendations)
        state.coordinator_summary.append(
            f"{self.name}: produced {len(state.recommendations)} upgrade recommendations and "
            f"{len(state.partner_targets)} partner-target rows."
        )
        return state
