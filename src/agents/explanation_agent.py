from __future__ import annotations

from agents.types import MultiAgentState


class EconomicExplanationAgent:
    name = "economic_explanation_agent"

    def run(self, state: MultiAgentState) -> MultiAgentState:
        print(f"[agent:{self.name}] Generating explanations ...")
        rows: list[dict[str, object]] = []
        for recommendation in state.recommendations:
            if recommendation["development_status"] == "underdeveloped_export":
                explanation = (
                    f"{state.target_country_name} already exports {recommendation['upgrade_label']} "
                    f"({recommendation['upgrade_hs6']}), but at a much smaller scale than "
                    f"{recommendation['base_label']} ({recommendation['base_hs6']}). "
                    f"This suggests an underdeveloped downstream export that could be expanded "
                    f"toward markets such as {recommendation['top_target_markets']}. "
                    f"Regional support is visible through {recommendation['regional_exporter_count']} neighboring exporters "
                    f"and {recommendation['regional_importer_count']} regional import markets."
                )
            else:
                explanation = (
                    f"{state.target_country_name} already has an export base in {recommendation['base_label']} "
                    f"({recommendation['base_hs6']}). Moving into {recommendation['upgrade_label']} "
                    f"({recommendation['upgrade_hs6']}) represents a downstream upgrade with higher "
                    f"assumed value addition. The recommendation is reinforced by "
                    f"{recommendation['regional_exporter_count']} regional exporters and "
                    f"{recommendation['regional_importer_count']} regional import markets, including "
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
        print(f"[agent:{self.name}] Completed: {len(rows)} rows")
        return state
