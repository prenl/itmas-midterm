from __future__ import annotations

from dataclasses import dataclass

import torch

from economic_complexity import build_export_matrix, compute_eci_pci

try:
    from torch_geometric.data import HeteroData
except ImportError:  # pragma: no cover
    HeteroData = None


@dataclass
class TradeRecord:
    reporter_idx: int
    partner_idx: int
    hs6_idx: int
    trade_value_usd: float
    gdp_growth_pct: float
    inflation_pct: float


def build_trade_hetero_graph(records: list[TradeRecord]):
    if HeteroData is None:
        raise ImportError("Install torch-geometric to build the graph dataset.")
    if not records:
        raise ValueError("records must not be empty.")

    data = HeteroData()

    num_countries = max(max(r.reporter_idx, r.partner_idx) for r in records) + 1
    num_hs6 = max(r.hs6_idx for r in records) + 1

    export_matrix = build_export_matrix(
        [
            (rec.reporter_idx, rec.hs6_idx, rec.trade_value_usd)
            for rec in records
        ],
        num_countries=num_countries,
        num_products=num_hs6,
    )
    complexity = compute_eci_pci(export_matrix)

    country_trade_totals = export_matrix.sum(dim=1)
    hs6_trade_totals = export_matrix.sum(dim=0)

    country_growth_sum = torch.zeros(num_countries, dtype=torch.float32)
    country_inflation_sum = torch.zeros(num_countries, dtype=torch.float32)
    country_record_count = torch.zeros(num_countries, dtype=torch.float32)

    for rec in records:
        country_growth_sum[rec.reporter_idx] += float(rec.gdp_growth_pct)
        country_inflation_sum[rec.reporter_idx] += float(rec.inflation_pct)
        country_record_count[rec.reporter_idx] += 1.0

    safe_country_record_count = torch.where(
        country_record_count > 0,
        country_record_count,
        torch.ones_like(country_record_count),
    )
    avg_country_growth = country_growth_sum / safe_country_record_count
    avg_country_inflation = country_inflation_sum / safe_country_record_count

    data["country"].x = torch.stack(
        [
            complexity.eci,
            complexity.diversity,
            country_trade_totals,
            avg_country_growth,
            avg_country_inflation,
        ],
        dim=1,
    )
    data["hs6"].x = torch.stack(
        [
            complexity.pci,
            complexity.ubiquity,
            hs6_trade_totals,
        ],
        dim=1,
    )

    country_to_country = []
    country_to_hs6 = []
    edge_attr_country_country = []
    edge_attr_country_hs6 = []

    for rec in records:
        country_to_country.append([rec.reporter_idx, rec.partner_idx])
        country_to_hs6.append([rec.reporter_idx, rec.hs6_idx])

        edge_attr_country_country.append(
            [rec.trade_value_usd, rec.gdp_growth_pct, rec.inflation_pct]
        )
        edge_attr_country_hs6.append([rec.trade_value_usd])

    data["country", "exports_to", "country"].edge_index = (
        torch.tensor(country_to_country, dtype=torch.long).t().contiguous()
    )
    data["country", "exports_to", "country"].edge_attr = torch.tensor(
        edge_attr_country_country, dtype=torch.float
    )

    data["country", "trades_hs6", "hs6"].edge_index = (
        torch.tensor(country_to_hs6, dtype=torch.long).t().contiguous()
    )
    data["country", "trades_hs6", "hs6"].edge_attr = torch.tensor(
        edge_attr_country_hs6, dtype=torch.float
    )

    return data
