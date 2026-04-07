from __future__ import annotations

from dataclasses import dataclass

from economic_complexity import EconomicComplexityResult


@dataclass
class UpgradeCandidate:
    product_code: str
    product_description: str
    based_on_code: str
    based_on_description: str
    pci: float
    rca: float
    opportunity_score: float
    likely_markets: list[str]
    rationale: str


PROCESSING_LINKS: dict[str, list[str]] = {
    "260300": ["740311", "740811"],
    "740311": ["740811"],
    "100199": ["110100"],
}


def recommend_export_upgrades(
    *,
    country_idx: int,
    country_name: str,
    product_index_by_code: dict[str, int],
    product_code_by_index: dict[int, str],
    product_name_by_code: dict[str, str],
    exporters_by_product: dict[str, set[str]],
    importers_by_product: dict[str, set[str]],
    country_exports_by_product: dict[str, float],
    complexity: EconomicComplexityResult,
    top_k: int = 5,
) -> list[UpgradeCandidate]:
    candidates: list[UpgradeCandidate] = []
    already_exported = set(country_exports_by_product)

    max_pci = float(complexity.pci.max().item()) if complexity.pci.numel() else 1.0
    min_pci = float(complexity.pci.min().item()) if complexity.pci.numel() else 0.0
    pci_range = max(max_pci - min_pci, 1e-6)
    max_source_exports = max(country_exports_by_product.values(), default=1.0)

    for source_code, export_value in country_exports_by_product.items():
        for candidate_code in PROCESSING_LINKS.get(source_code, []):
            if candidate_code in already_exported:
                continue

            product_idx = product_index_by_code.get(candidate_code)
            if product_idx is None:
                continue

            pci = float(complexity.pci[product_idx].item())
            rca = float(complexity.rca_matrix[country_idx, product_idx].item())
            external_exporters = sorted(exporters_by_product.get(candidate_code, set()) - {country_name})
            likely_markets = sorted(importers_by_product.get(candidate_code, set()) - {country_name})
            if not external_exporters or not likely_markets:
                continue

            source_strength = export_value / max_source_exports
            pci_strength = (pci - min_pci) / pci_range
            market_strength = min(len(likely_markets) / 3.0, 1.0)
            score = 0.45 * source_strength + 0.35 * pci_strength + 0.20 * market_strength

            candidates.append(
                UpgradeCandidate(
                    product_code=candidate_code,
                    product_description=product_name_by_code[candidate_code],
                    based_on_code=source_code,
                    based_on_description=product_name_by_code[source_code],
                    pci=pci,
                    rca=rca,
                    opportunity_score=score,
                    likely_markets=likely_markets,
                    rationale=(
                        f"{country_name} уже экспортирует {product_name_by_code[source_code]} "
                        f"({source_code}), а {', '.join(external_exporters)} уже экспортируют "
                        f"{product_name_by_code[candidate_code]} ({candidate_code})."
                    ),
                )
            )

    deduplicated: dict[str, UpgradeCandidate] = {}
    for candidate in candidates:
        current = deduplicated.get(candidate.product_code)
        if current is None or candidate.opportunity_score > current.opportunity_score:
            deduplicated[candidate.product_code] = candidate

    return sorted(
        deduplicated.values(),
        key=lambda item: (item.opportunity_score, item.pci, len(item.likely_markets)),
        reverse=True,
    )[:top_k]
