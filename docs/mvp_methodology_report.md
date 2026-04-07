# Kazakhstan Export Upgrade MVP: Methodology Report

## 1. Project Goal

The goal of the project is to demonstrate that a combined data stack based on:

- `UN Comtrade`
- `World Bank API`
- `CEPII Gravity / GeoDist`

can be used to identify plausible export upgrade opportunities for Kazakhstan at the `HS6` level.

In this MVP, the task is framed as follows:

- identify products that Kazakhstan already exports;
- map these exports to plausible downstream processing upgrades;
- estimate which partner markets are the most promising for those upgraded products;
- derive simple scenario-based estimates of possible export uplift and GDP growth uplift.

This is an MVP, not a full production forecasting system. Its role is to prove that the selected technology stack is capable of supporting the intended analytical workflow.

## 2. Research Idea

The economic logic behind the MVP is based on the concept of moving from raw or lower-stage products to more processed, higher-value products.

Examples:

- `copper ore -> refined copper cathodes -> copper wire`
- `wheat -> flour`
- `sunflower seeds -> crude sunflower oil -> refined sunflower oil`

Such transitions are interpreted as export upgrades or processing upgrades. The assumption is that if a country already has a strong export base in an upstream product, then it may be able to move into related downstream products with higher value added.

## 3. Data Sources

### 3.1 UN Comtrade

UN Comtrade is used as the main trade source.

Purpose:

- identify Kazakhstan’s export structure;
- measure export value by product and destination;
- determine the base products from which downstream upgrades can be proposed.

MVP scope:

- reporter: `Kazakhstan`
- flow: `exports`
- product classification: `HS6`
- frequency: `annual`
- years: `2020-2025`

Current implementation:

- the pipeline supports importing `CSV`, `parquet`, and `pq` files from `data/comtrade/`;
- this was chosen because the public Comtrade frontend uses a SPA-based access pattern and direct stable API routing was not fully finalized during MVP implementation;
- the architecture is prepared for future direct API integration.

### 3.2 World Bank API

World Bank data is used as the macroeconomic context layer.

Purpose:

- estimate current GDP scale of Kazakhstan and partner countries;
- use GDP growth and trade-related indicators as part of partner attractiveness and uplift scenarios.

Indicators used in the MVP:

- `NY.GDP.MKTP.CD` — GDP (current US$)
- `NY.GDP.MKTP.KD.ZG` — GDP growth (annual %)
- `FP.CPI.TOTL.ZG` — inflation (annual %)
- `NE.TRD.GNFS.ZS` — trade as % of GDP

Access mode:

- free public API
- no authorization required

### 3.3 CEPII Gravity / GeoDist

CEPII Gravity data is used as the bilateral country-pair feature layer.

Purpose:

- distance between Kazakhstan and partner countries;
- shared border;
- common official language;
- regional and bilateral trade-context information.

Local file used in the project:

- `Gravity_csv_V202211/Gravity_V202211.csv`

Typical fields used:

- `dist`
- `contig`
- `comlang_off`
- `rta_type`

## 4. Technology Stack

The MVP is implemented in Python.

Core libraries and tools:

- `Python 3.10`
- `SQLAlchemy` — ORM and database layer
- `PyTorch` — tensor and matrix computations
- `torch-geometric` — graph data structures and GNN components
- `matplotlib` — charts and visual outputs
- `pandas` — tabular processing and parquet support
- `pyarrow` — parquet reading

Environment:

- `conda` environment: `itmas_midterm`

## 4.1 Multi-Agent Extension

For the multi-agent systems course framing, the project now includes a lightweight multi-agent architecture on top of the analytical pipeline.

Implemented agents:

- `Data Acquisition Agent`
  Loads and validates UN Comtrade, World Bank, and CEPII inputs.

- `Opportunity Analysis Agent`
  Produces export-upgrade recommendations, target markets, and GDP scenarios.

- `Economic Explanation Agent`
  Generates concise economic explanations for each recommendation.

- `Critic Agent`
  Reviews recommendations, attaches risk flags, and labels them as acceptable or cautious.

- `Coordinator`
  Orchestrates all agents and composes the final output package.

This architecture allows the project to be framed not only as a trade analytics pipeline but also as a specialized multi-agent decision-support system.

## 5. Project Structure

Main implementation files:

- `src/economic_complexity.py`
  Calculates `RCA`, binary specialization matrix `Mcp`, `ECI`, and `PCI`.

- `src/graph_dataset.py`
  Builds a heterogeneous graph for country and product nodes.

- `src/gnn_recommender.py`
  Contains the graph attention model prototype.

- `src/upgrade_recommender.py`
  Encodes product-upgrade logic for downstream recommendations.

- `src/mvp_data_pipeline.py`
  Main MVP pipeline for data loading, joining, scoring, CSV export, and chart rendering.

- `src/run_mvp.py`
  Entry point for the Kazakhstan export-upgrade MVP run.

- `src/agents/`
  Multi-agent modules for acquisition, analysis, explanation, critique, and coordination.

- `src/run_multi_agent.py`
  Entry point for the multi-agent version of the MVP.

Supporting data files:

- `data/upgrade_paths.csv`
  Curated mapping between upstream and downstream `HS6` products.

- `data/comtrade/`
  Input directory for UN Comtrade exports.

Generated outputs:

- `outputs/mvp/csv/`
- `outputs/mvp/charts/`

## 6. Analytical Pipeline

The MVP workflow is organized into the following stages.

### 6.1 Trade Data Ingestion

Kazakhstan export records are loaded from UN Comtrade export files stored locally in:

- `data/comtrade/`

Supported formats:

- `.csv`
- `.parquet`
- `.pq`

The loader standardizes columns such as:

- year
- reporter ISO3
- partner ISO3
- partner name
- HS6 code
- product description
- trade value
- net weight
- quantity

### 6.2 Export Base Construction

The raw trade rows are aggregated into a Kazakhstan export base summary by `HS6` product.

For each product, the pipeline computes:

- total export value over `2020-2025`
- number of partner countries
- number of active years
- approximate average unit value

This summary is saved to:

- `outputs/mvp/csv/kaz_export_base.csv`

### 6.3 Product Upgrade Mapping

The project uses a curated table of plausible upgrade paths:

- `data/upgrade_paths.csv`

Each row describes:

- a base `HS6` product;
- an upgraded `HS6` product;
- product family;
- processing stage gap;
- assumed value multiplier.

This is a domain-driven layer added on top of trade data. It is important because pure trade data alone does not explicitly encode “processing upgrade” relationships.

### 6.4 Partner-Market Scoring

For each base product exported by Kazakhstan, the pipeline evaluates partner markets using:

- current bilateral trade presence from Comtrade;
- partner GDP from World Bank;
- partner GDP growth from World Bank;
- distance from CEPII;
- shared border from CEPII;
- common language from CEPII.

The result is a ranked list of likely target markets for each recommended upgraded product.

This output is saved to:

- `outputs/mvp/csv/upgrade_partner_targets.csv`

### 6.5 Upgrade Recommendation Scoring

Each upgrade candidate is assigned an `opportunity_score`.

The score combines:

- strength of Kazakhstan’s current export base in the upstream product;
- market attractiveness of current and nearby partners;
- assumed value increase from downstream processing;
- processing-stage distance between current and upgraded product.

This output is saved to:

- `outputs/mvp/csv/upgrade_recommendations.csv`

### 6.6 GDP Uplift Scenarios

The project does not produce a structural macroeconomic forecast. Instead, it produces scenario-based GDP uplift estimates.

The scenario logic is:

- start from estimated export uplift for a recommended product;
- translate that uplift into a small share of Kazakhstan GDP;
- generate three scenarios:
  - conservative
  - base
  - optimistic

This output is saved to:

- `outputs/mvp/csv/gdp_growth_scenarios.csv`

### 6.7 Multi-Agent Orchestration

In the multi-agent version of the system, the workflow is explicitly split into cooperating agents.

Execution order:

1. `Data Acquisition Agent`
2. `Opportunity Analysis Agent`
3. `Economic Explanation Agent`
4. `Critic Agent`
5. `Coordinator`

This makes the system suitable for discussion in a multi-agent systems context because:

- each agent has a specialized role;
- agents exchange structured intermediate state;
- the coordinator integrates agent outputs into the final artifact set.

## 7. Economic Complexity Layer

The project also includes an economic complexity module:

- `RCA` — Revealed Comparative Advantage
- `Mcp` — binary specialization matrix where `Mcp = 1` if `RCA >= 1`
- `ECI` — Economic Complexity Index for countries
- `PCI` — Product Complexity Index for products

Implementation:

- `src/economic_complexity.py`

Current role in the MVP:

- demonstrate that the project architecture supports complexity-based features;
- allow future integration of `PCI` and `ECI` directly into the final recommendation ranking.

This is important for the scientific contribution because it shows that the model is not limited to simple trade totals. It can be extended toward a more formal economic complexity framework.

## 8. Graph Neural Network Layer

The project also contains a prototype graph neural network layer.

Implementation:

- `src/graph_dataset.py`
- `src/gnn_recommender.py`

Graph structure:

- country nodes
- HS6 product nodes
- country-to-country trade edges
- country-to-product trade edges

Current role in the MVP:

- show that the system can be extended from rule-based and feature-based recommendations to graph-based learning;
- demonstrate technology readiness for a stronger experimental model in the next stage.

At the current MVP stage, the final recommendation CSV is still driven mainly by explicit data joins and scoring logic, rather than by a trained GNN ranking model.

## 9. Output Artifacts

### 9.1 Terminal Summary

The terminal summary provides a quick overview of:

- number of Comtrade rows loaded;
- number of distinct HS6 exports;
- number of export partners;
- total export value in the sample;
- top current export;
- top recommended upgrade.

### 9.2 CSV Outputs

The main analysis outputs are:

- `kaz_export_base.csv`
- `upgrade_recommendations.csv`
- `upgrade_partner_targets.csv`
- `gdp_growth_scenarios.csv`

The multi-agent version additionally generates:

- `agent_explanations.csv`
- `critic_review.csv`

These files are intended to be the main material for inspection, discussion, and inclusion in a scientific write-up.

### 9.3 Charts

The MVP currently generates the following charts:

- `top_exports.png`
  Kazakhstan’s main HS6 exports in the selected period.

- `estimated_export_uplift.png`
  Estimated export uplift for recommended upgraded products.

- `estimated_gdp_uplift_pct.png`
  Estimated GDP uplift percentage by recommended upgrade.

- `predicted_gdp_growth_scenarios.png`
  Scenario chart for conservative, base, and optimistic GDP growth uplift assumptions.

### 9.4 Multi-Agent Summary

The multi-agent runner also generates:

- `multi_agent_summary.md`

This file records:

- which agents ran;
- what each agent produced;
- the top recommendation;
- a compact coordinator-level summary for presentation.

## 10. Scientific Value of the MVP

The MVP demonstrates several important points for a scientific paper.

### 10.1 Integration of Heterogeneous Data Sources

The project successfully integrates:

- international trade data;
- macroeconomic indicators;
- bilateral gravity-style country-pair features.

This is a strong methodological basis for a recommendation system in trade analytics.

### 10.2 Product-Level Export Diversification Logic

The project does not only describe existing exports. It proposes a concrete mechanism for identifying likely next-stage products.

This gives the work practical relevance for:

- export diversification analysis;
- industrial upgrading analysis;
- policy-oriented trade recommendations.

### 10.3 Expandability Toward Advanced Models

The current MVP already includes:

- economic complexity computation;
- graph dataset construction;
- graph neural network prototype.

This means the work can naturally evolve into a stronger experimental system without changing the overall architecture.

## 11. Limitations

The MVP has several limitations that should be explicitly stated in the scientific work.

### 11.1 Curated Upgrade Paths

Upgrade paths are currently defined in:

- `data/upgrade_paths.csv`

This is useful for the MVP, but in a stronger version of the study the upgrade links should be derived from:

- input-output relations;
- product proximity;
- co-export patterns;
- expert validation.

### 11.2 Scenario-Based GDP Estimates

The GDP uplift logic is not a causal macroeconomic forecast.

It is a scenario approximation based on:

- export uplift assumptions;
- value multipliers;
- partial conversion of current trade base into upgraded exports.

This should be presented as a decision-support scenario, not as a guaranteed prediction.

### 11.3 Comtrade API Automation

The architecture supports future direct API ingestion, but the MVP currently relies on local exported trade files for robustness.

This does not weaken the analytical logic, but it should be mentioned as an implementation limitation of the current stage.

### 11.4 Limited Country Scope

The MVP is centered on:

- Kazakhstan as exporter

This is appropriate for a focused case study, but it is not yet a full global recommendation engine.

## 12. Reproducibility

The MVP can be reproduced by running:

```bash
conda run --no-capture-output -n itmas_midterm python src/run_mvp.py
```

Input assumptions:

- UN Comtrade exports are placed into `data/comtrade/`
- CEPII gravity file is located at `Gravity_csv_V202211/Gravity_V202211.csv`
- World Bank indicators are fetched online through the free API

## 13. Suggested Paper Framing

This project can be framed in a scientific paper as:

`A data-driven recommendation system for identifying export upgrading opportunities for Kazakhstan using UN Comtrade, World Bank, and CEPII Gravity data`

Possible emphasis areas:

- recommendation systems for trade analytics;
- export diversification and industrial upgrading;
- integration of graph-based methods and economic complexity;
- explainable decision-support tools for trade policy.

## 14. Next Steps

The strongest next research steps are:

1. replace the curated upgrade table with a data-driven product-proximity layer;
2. incorporate `PCI` and `ECI` directly into final ranking;
3. expand from Kazakhstan-only to a multi-country benchmark setting;
4. compare heuristic recommendations against graph-based ranking from the GNN layer;
5. validate recommendations against later-year trade outcomes.
6. replace rule-based explanation and critique with LLM-based agent reasoning while keeping the same multi-agent structure.

## 15. References and Official Sources

UN Comtrade:

- https://comtradeplus.un.org/
- https://comtradedeveloper.un.org/

World Bank API:

- https://api.worldbank.org/v2/
- https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation

CEPII Gravity:

- https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=8
