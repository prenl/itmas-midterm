from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from trade_pipeline import TradeRow


@dataclass
class MultiAgentState:
    comtrade_dir: Path
    gravity_file: Path
    gravity_countries_file: Path
    upgrade_paths_file: Path
    output_dir: Path
    csv_dir: Path
    charts_dir: Path
    target_country_iso3: str
    allowed_reporters: set[str] = field(default_factory=set)
    target_country_name: str = ""
    trade_rows: list[TradeRow] = field(default_factory=list)
    target_export_rows: list[TradeRow] = field(default_factory=list)
    export_base: list[dict[str, object]] = field(default_factory=list)
    partners: set[str] = field(default_factory=set)
    regional_reporters: set[str] = field(default_factory=set)
    world_bank: dict[str, dict[int, dict[str, float | None]]] = field(default_factory=dict)
    gravity_by_partner: dict[str, dict[str, float | str]] = field(default_factory=dict)
    upgrade_paths: list[dict[str, str]] = field(default_factory=list)
    recommendations: list[dict[str, object]] = field(default_factory=list)
    partner_targets: list[dict[str, object]] = field(default_factory=list)
    gdp_scenarios: list[dict[str, object]] = field(default_factory=list)
    explanation_rows: list[dict[str, object]] = field(default_factory=list)
    critic_rows: list[dict[str, object]] = field(default_factory=list)
    coordinator_summary: list[str] = field(default_factory=list)
