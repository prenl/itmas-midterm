from __future__ import annotations

from dataclasses import dataclass

import torch


@dataclass
class EconomicComplexityResult:
    export_matrix: torch.Tensor
    rca_matrix: torch.Tensor
    mcp_matrix: torch.Tensor
    diversity: torch.Tensor
    ubiquity: torch.Tensor
    eci: torch.Tensor
    pci: torch.Tensor


def build_export_matrix(
    country_product_values: list[tuple[int, int, float]],
    num_countries: int | None = None,
    num_products: int | None = None,
) -> torch.Tensor:
    if not country_product_values:
        raise ValueError("country_product_values must not be empty.")

    if num_countries is None:
        num_countries = max(country_idx for country_idx, _, _ in country_product_values) + 1
    if num_products is None:
        num_products = max(product_idx for _, product_idx, _ in country_product_values) + 1

    export_matrix = torch.zeros((num_countries, num_products), dtype=torch.float32)
    for country_idx, product_idx, trade_value in country_product_values:
        export_matrix[country_idx, product_idx] += float(trade_value)
    return export_matrix


def compute_rca_matrix(export_matrix: torch.Tensor) -> torch.Tensor:
    export_matrix = export_matrix.to(dtype=torch.float32)

    country_totals = export_matrix.sum(dim=1, keepdim=True)
    product_totals = export_matrix.sum(dim=0, keepdim=True)
    global_total = export_matrix.sum()

    if global_total <= 0:
        raise ValueError("Total exports must be positive to compute RCA.")

    safe_country_totals = torch.where(country_totals > 0, country_totals, torch.ones_like(country_totals))
    safe_product_totals = torch.where(product_totals > 0, product_totals, torch.ones_like(product_totals))

    country_share = export_matrix / safe_country_totals
    product_share = safe_product_totals / global_total
    rca = country_share / product_share

    no_exports_mask = country_totals.squeeze(1) <= 0
    no_product_exports_mask = product_totals.squeeze(0) <= 0
    if no_exports_mask.any():
        rca[no_exports_mask] = 0.0
    if no_product_exports_mask.any():
        rca[:, no_product_exports_mask] = 0.0

    return rca


def compute_mcp_matrix(export_matrix: torch.Tensor, rca_threshold: float = 1.0) -> torch.Tensor:
    rca_matrix = compute_rca_matrix(export_matrix)
    return (rca_matrix >= rca_threshold).to(dtype=torch.float32)


def _compute_complexity_index(
    relatedness_matrix: torch.Tensor,
    baseline: torch.Tensor,
) -> torch.Tensor:
    if relatedness_matrix.size(0) == 1:
        return torch.zeros(1, dtype=torch.float32)

    eigenvalues, eigenvectors = torch.linalg.eig(relatedness_matrix)
    eigenvalues = eigenvalues.real
    eigenvectors = eigenvectors.real

    order = torch.argsort(eigenvalues, descending=True)
    candidate_idx = None
    for idx in order.tolist()[1:]:
        eigenvector = eigenvectors[:, idx]
        if torch.std(eigenvector, unbiased=False) > 0:
            candidate_idx = idx
            break

    if candidate_idx is None:
        for idx in order.tolist():
            eigenvector = eigenvectors[:, idx]
            if torch.std(eigenvector, unbiased=False) > 0:
                candidate_idx = idx
                break

    if candidate_idx is None:
        return torch.zeros(relatedness_matrix.size(0), dtype=torch.float32)

    complexity = eigenvectors[:, candidate_idx]
    centered = complexity - complexity.mean()
    std = torch.std(centered, unbiased=False)
    if std > 0:
        complexity = centered / std
    else:
        complexity = torch.zeros_like(centered)

    baseline_centered = baseline - baseline.mean()
    denom = torch.linalg.norm(complexity) * torch.linalg.norm(baseline_centered)
    if denom > 0:
        corr_sign = torch.sign(torch.dot(complexity, baseline_centered) / denom)
        if corr_sign < 0:
            complexity = -complexity

    return complexity.to(dtype=torch.float32)


def compute_eci_pci(
    export_matrix: torch.Tensor,
    rca_threshold: float = 1.0,
) -> EconomicComplexityResult:
    export_matrix = export_matrix.to(dtype=torch.float32)
    rca_matrix = compute_rca_matrix(export_matrix)
    mcp_matrix = (rca_matrix >= rca_threshold).to(dtype=torch.float32)

    diversity = mcp_matrix.sum(dim=1)
    ubiquity = mcp_matrix.sum(dim=0)

    safe_diversity = torch.where(diversity > 0, diversity, torch.ones_like(diversity))
    safe_ubiquity = torch.where(ubiquity > 0, ubiquity, torch.ones_like(ubiquity))

    country_relatedness = (
        torch.diag(1.0 / safe_diversity)
        @ mcp_matrix
        @ torch.diag(1.0 / safe_ubiquity)
        @ mcp_matrix.T
    )
    product_relatedness = (
        torch.diag(1.0 / safe_ubiquity)
        @ mcp_matrix.T
        @ torch.diag(1.0 / safe_diversity)
        @ mcp_matrix
    )

    eci = _compute_complexity_index(country_relatedness, diversity)
    pci = _compute_complexity_index(product_relatedness, ubiquity)

    eci = torch.where(diversity > 0, eci, torch.zeros_like(eci))
    pci = torch.where(ubiquity > 0, pci, torch.zeros_like(pci))

    return EconomicComplexityResult(
        export_matrix=export_matrix,
        rca_matrix=rca_matrix,
        mcp_matrix=mcp_matrix,
        diversity=diversity,
        ubiquity=ubiquity,
        eci=eci,
        pci=pci,
    )
