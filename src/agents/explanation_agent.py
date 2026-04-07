from __future__ import annotations

from agents.types import MultiAgentState


class EconomicExplanationAgent:
    name = "economic_explanation_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        rows: list[dict[str, object]] = []
        for recommendation in state.recommendations:
            explanation = (
                f"Kazakhstan already has an export base in {recommendation['base_label']} "
                f"({recommendation['base_hs6']}). Moving into {recommendation['upgrade_label']} "
                f"({recommendation['upgrade_hs6']}) represents a downstream upgrade with higher "
                f"assumed value addition and access to current partner markets such as "
                f"{recommendation['top_target_markets']}."
            )
            rows.append(
                {
                    "upgrade_hs6": recommendation["upgrade_hs6"],
                    "upgrade_label": recommendation["upgrade_label"],
                    "economic_explanation": explanation,
                }
            )
        state.explanation_rows = rows
        state.coordinator_summary.append(
            f"{self.name}: generated {len(rows)} explanation rows for the recommendation set."
        )
        return state
