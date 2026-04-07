from __future__ import annotations

from pathlib import Path

from agents.analysis_agent import OpportunityAnalysisAgent
from agents.critic_agent import CriticAgent
from agents.data_agent import DataAcquisitionAgent
from agents.explanation_agent import EconomicExplanationAgent
from agents.types import MultiAgentState
from mvp_data_pipeline import ensure_output_dirs, render_charts, write_csv


def _write_markdown_summary(state: MultiAgentState) -> None:
    top_recommendation = state.recommendations[0] if state.recommendations else None
    lines = [
        "# Multi-Agent Recommendation Summary",
        "",
        "## Agent Trace",
        "",
    ]
    for item in state.coordinator_summary:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Result Snapshot",
            "",
            f"- Trade rows loaded: {len(state.trade_rows)}",
            f"- Distinct HS6 exports: {len(state.export_base)}",
            f"- Distinct export partners: {len(state.partners)}",
            f"- Recommendations generated: {len(state.recommendations)}",
        ]
    )

    if top_recommendation is not None:
        lines.extend(
            [
                "",
                "## Top Recommendation",
                "",
                f"- Upgrade: {top_recommendation['upgrade_hs6']} | {top_recommendation['upgrade_label']}",
                f"- Base product: {top_recommendation['base_hs6']} | {top_recommendation['base_label']}",
                f"- Estimated export uplift: USD {float(top_recommendation['estimated_export_uplift_usd']):,.0f}",
                f"- Estimated GDP uplift: {float(top_recommendation['estimated_gdp_uplift_pct'] or 0.0):.6f}%",
                f"- Target markets: {top_recommendation['top_target_markets']}",
            ]
        )

    (state.output_dir / "multi_agent_summary.md").write_text("\n".join(lines), encoding="utf-8")


class MultiAgentCoordinator:
    def __init__(self) -> None:
        self.data_agent = DataAcquisitionAgent()
        self.analysis_agent = OpportunityAnalysisAgent()
        self.explanation_agent = EconomicExplanationAgent()
        self.critic_agent = CriticAgent()

    def run(
        self,
        *,
        comtrade_dir: Path,
        gravity_file: Path,
        upgrade_paths_file: Path,
        output_dir: Path,
    ) -> MultiAgentState:
        csv_dir, charts_dir = ensure_output_dirs(output_dir)
        state = MultiAgentState(
            comtrade_dir=comtrade_dir,
            gravity_file=gravity_file,
            upgrade_paths_file=upgrade_paths_file,
            output_dir=output_dir,
            csv_dir=csv_dir,
            charts_dir=charts_dir,
        )

        state = self.data_agent.run(state)
        state = self.analysis_agent.run(state)
        state = self.explanation_agent.run(state)
        state = self.critic_agent.run(state)

        write_csv(state.csv_dir / "kaz_export_base.csv", state.export_base)
        write_csv(state.csv_dir / "upgrade_recommendations.csv", state.recommendations)
        write_csv(state.csv_dir / "upgrade_partner_targets.csv", state.partner_targets)
        write_csv(state.csv_dir / "gdp_growth_scenarios.csv", state.gdp_scenarios)
        write_csv(state.csv_dir / "agent_explanations.csv", state.explanation_rows)
        write_csv(state.csv_dir / "critic_review.csv", state.critic_rows)
        render_charts(state.export_base, state.recommendations, state.charts_dir)
        _write_markdown_summary(state)
        return state
