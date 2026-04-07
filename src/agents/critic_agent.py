from __future__ import annotations

from agents.types import MultiAgentState


class CriticAgent:
    name = "critic_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        critic_rows: list[dict[str, object]] = []
        for recommendation in state.recommendations:
            risk_flags: list[str] = []
            partner_count = int(recommendation["base_partner_count"])
            gdp_uplift = float(recommendation["estimated_gdp_uplift_pct"] or 0.0)
            value_multiplier = float(recommendation["value_multiplier_assumption"])
            development_ratio = float(recommendation["current_upgrade_to_base_ratio"] or 0.0)

            if partner_count <= 1:
                risk_flags.append("narrow_partner_base")
            if value_multiplier >= 1.8:
                risk_flags.append("high_processing_jump")
            if gdp_uplift >= 0.05:
                risk_flags.append("requires_strong_execution")
            if recommendation["development_status"] == "underdeveloped_export" and development_ratio >= 0.20:
                risk_flags.append("already_partly_developed")

            assessment = "acceptable"
            if len(risk_flags) >= 3:
                assessment = "high_risk"
            elif len(risk_flags) >= 1:
                assessment = "cautious"

            critic_rows.append(
                {
                    "base_hs6": recommendation["base_hs6"],
                    "base_label": recommendation["base_label"],
                    "upgrade_hs6": recommendation["upgrade_hs6"],
                    "upgrade_label": recommendation["upgrade_label"],
                    "development_status": recommendation["development_status"],
                    "critic_assessment": assessment,
                    "risk_flags": ", ".join(risk_flags) if risk_flags else "none",
                    "critic_note": (
                        "Recommendation is consistent with the current analytical dataset."
                        if not risk_flags
                        else "Recommendation is plausible, but should be treated as scenario-based because of "
                        + ", ".join(risk_flags)
                        + "."
                    ),
                }
            )

        state.critic_rows = critic_rows
        state.coordinator_summary.append(
            f"{self.name}: reviewed {len(critic_rows)} recommendations and attached risk flags."
        )
        return state
