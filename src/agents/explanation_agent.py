from __future__ import annotations

from agents.types import MultiAgentState


class EconomicExplanationAgent:
    name = "economic_explanation_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        rows: list[dict[str, object]] = []
        for recommendation in state.recommendations:
            if recommendation["development_status"] == "underdeveloped_export":
                explanation = (
                    f"Kazakhstan already exports {recommendation['upgrade_label']} "
                    f"({recommendation['upgrade_hs6']}), but at a much smaller scale than "
                    f"{recommendation['base_label']} ({recommendation['base_hs6']}). "
                    f"This suggests an underdeveloped downstream export that could be expanded "
                    f"toward markets such as {recommendation['top_target_markets']}."
                )
            else:
                explanation = (
                    f"Kazakhstan already has an export base in {recommendation['base_label']} "
                    f"({recommendation['base_hs6']}). Moving into {recommendation['upgrade_label']} "
                    f"({recommendation['upgrade_hs6']}) represents a downstream upgrade with higher "
                    f"assumed value addition and access to current partner markets such as "
                    f"{recommendation['top_target_markets']}."
                )
            rows.append(
                {
                    "base_hs6": recommendation["base_hs6"],
                    "base_label": recommendation["base_label"],
                    "upgrade_hs6": recommendation["upgrade_hs6"],
                    "upgrade_label": recommendation["upgrade_label"],
                    "development_status": recommendation["development_status"],
                    "economic_explanation": explanation,
                }
            )
        state.explanation_rows = rows
        state.coordinator_summary.append(
            f"{self.name}: generated {len(rows)} explanation rows for the recommendation set."
        )
        return state
