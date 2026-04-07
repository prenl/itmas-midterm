from __future__ import annotations

import csv
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import matplotlib.pyplot as plt
import pandas as pd


WORLD_BANK_BASE = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
WORLD_BANK_INDICATORS = {
    "gdp_current_usd": "NY.GDP.MKTP.CD",
    "gdp_growth_pct": "NY.GDP.MKTP.KD.ZG",
    "inflation_pct": "FP.CPI.TOTL.ZG",
    "trade_share_gdp_pct": "NE.TRD.GNFS.ZS",
}
TARGET_YEARS = {2020, 2021, 2022, 2023, 2024, 2025}
HEADER_ALIASES = {
    "year": ["year", "period", "refyear"],
    "reporter_iso3": ["reporterisocodeisoalpha3", "reporteriso", "reportercodeisoalpha3"],
    "reporter": ["reporterdesc", "reporter", "reportername"],
    "partner_iso3": ["partnerisocodeisoalpha3", "partneriso", "partnercodeisoalpha3"],
    "partner": ["partnerdesc", "partner", "partnername"],
    "cmd_code": ["cmdcode", "commoditycode", "hscode", "commoditycodes"],
    "cmd_desc": ["cmddesc", "commodity", "commoditydescription", "cmddescription"],
    "flow": ["flowdesc", "tradeflow", "flow"],
    "trade_value_usd": ["primaryvalue", "tradevalueusd", "tradevalue", "fobvalue"],
    "net_weight_kg": ["netwgt", "tradeweightinkg", "tradeweightkg", "weightkg"],
    "quantity": ["qty", "quantity"],
    "qty_unit": ["qtyunitabbr", "qtyunit", "quantityunit"],
}


@dataclass
class TradeRow:
    year: int
    reporter_iso3: str
    reporter: str
    partner_iso3: str
    partner: str
    cmd_code: str
    cmd_desc: str
    trade_value_usd: float
    net_weight_kg: float | None
    quantity: float | None
    qty_unit: str


def _normalize_header(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def _get_column_name(fieldnames: list[str], logical_name: str) -> str | None:
    normalized = {_normalize_header(name): name for name in fieldnames}
    for alias in HEADER_ALIASES[logical_name]:
        match = normalized.get(_normalize_header(alias))
        if match is not None:
            return match
    return None


def _to_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(raw: str | None) -> int | None:
    value = _to_float(raw)
    if value is None:
        return None
    return int(value)


def load_comtrade_exports_from_dir(comtrade_dir: Path) -> list[TradeRow]:
    if not comtrade_dir.exists():
        raise FileNotFoundError(
            f"Comtrade directory not found: {comtrade_dir}. "
            "Place one or more CSV exports there."
        )

    rows: list[TradeRow] = []
    files = sorted(comtrade_dir.glob("*.csv")) + sorted(comtrade_dir.glob("*.parquet")) + sorted(comtrade_dir.glob("*.pq"))
    if not files:
        raise FileNotFoundError(
            f"No CSV or Parquet files found in {comtrade_dir}. "
            "Export Kazakhstan HS6 annual exports from UN Comtrade and place the files there."
        )

    for csv_path in files:
        if csv_path.suffix.lower() in {".parquet", ".pq"}:
            rows.extend(_load_comtrade_exports_from_parquet(csv_path))
            continue

        with csv_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                continue

            columns = {
                logical_name: _get_column_name(reader.fieldnames, logical_name)
                for logical_name in HEADER_ALIASES
            }

            required = ["year", "partner", "cmd_code", "trade_value_usd"]
            missing = [name for name in required if columns[name] is None]
            if missing:
                raise ValueError(
                    f"{csv_path} is missing required columns for {missing}. "
                    "Use the raw UN Comtrade CSV export with detailed fields."
                )

            for record in reader:
                year = _to_int(record.get(columns["year"])) if columns["year"] else None
                if year not in TARGET_YEARS:
                    continue

                flow_text = ""
                if columns["flow"] is not None:
                    flow_text = (record.get(columns["flow"]) or "").strip().lower()
                if flow_text and "export" not in flow_text:
                    continue

                reporter_iso3 = ""
                if columns["reporter_iso3"] is not None:
                    reporter_iso3 = (record.get(columns["reporter_iso3"]) or "").strip().upper()
                if reporter_iso3 and reporter_iso3 != "KAZ":
                    continue

                partner_iso3 = ""
                if columns["partner_iso3"] is not None:
                    partner_iso3 = (record.get(columns["partner_iso3"]) or "").strip().upper()

                cmd_code = (record.get(columns["cmd_code"]) or "").strip()
                if not cmd_code or cmd_code.upper() == "TOTAL":
                    continue

                trade_value = _to_float(record.get(columns["trade_value_usd"]))
                if trade_value is None or trade_value <= 0:
                    continue

                rows.append(
                    TradeRow(
                        year=year,
                        reporter_iso3="KAZ",
                        reporter=(record.get(columns["reporter"]) or "Kazakhstan").strip()
                        if columns["reporter"] is not None
                        else "Kazakhstan",
                        partner_iso3=partner_iso3,
                        partner=(record.get(columns["partner"]) or "").strip(),
                        cmd_code=cmd_code,
                        cmd_desc=(record.get(columns["cmd_desc"]) or "").strip()
                        if columns["cmd_desc"] is not None
                        else "",
                        trade_value_usd=trade_value,
                        net_weight_kg=_to_float(record.get(columns["net_weight_kg"]))
                        if columns["net_weight_kg"] is not None
                        else None,
                        quantity=_to_float(record.get(columns["quantity"]))
                        if columns["quantity"] is not None
                        else None,
                        qty_unit=(record.get(columns["qty_unit"]) or "").strip()
                        if columns["qty_unit"] is not None
                        else "",
                    )
                )

    if not rows:
        raise ValueError("No Kazakhstan export rows for 2020-2025 were found in the supplied Comtrade CSV files.")
    return rows


def _load_comtrade_exports_from_parquet(parquet_path: Path) -> list[TradeRow]:
    frame = pd.read_parquet(parquet_path)
    if frame.empty:
        return []

    original_columns = list(frame.columns)
    normalized_to_original = {_normalize_header(column): column for column in original_columns}
    columns = {}
    for logical_name, aliases in HEADER_ALIASES.items():
        columns[logical_name] = None
        for alias in aliases:
            match = normalized_to_original.get(_normalize_header(alias))
            if match is not None:
                columns[logical_name] = match
                break

    required = ["year", "partner", "cmd_code", "trade_value_usd"]
    missing = [name for name in required if columns[name] is None]
    if missing:
        raise ValueError(
            f"{parquet_path} is missing required columns for {missing}. "
            "Use the raw UN Comtrade export with detailed fields."
        )

    rows: list[TradeRow] = []
    for record in frame.to_dict(orient="records"):
        year = _to_int(record.get(columns["year"])) if columns["year"] else None
        if year not in TARGET_YEARS:
            continue

        flow_text = ""
        if columns["flow"] is not None:
            flow_text = str(record.get(columns["flow"]) or "").strip().lower()
        if flow_text and "export" not in flow_text:
            continue

        reporter_iso3 = ""
        if columns["reporter_iso3"] is not None:
            reporter_iso3 = str(record.get(columns["reporter_iso3"]) or "").strip().upper()
        if reporter_iso3 and reporter_iso3 != "KAZ":
            continue

        partner_iso3 = ""
        if columns["partner_iso3"] is not None:
            partner_iso3 = str(record.get(columns["partner_iso3"]) or "").strip().upper()

        cmd_code = str(record.get(columns["cmd_code"]) or "").strip()
        if not cmd_code or cmd_code.upper() == "TOTAL":
            continue

        trade_value = _to_float(record.get(columns["trade_value_usd"]))
        if trade_value is None or trade_value <= 0:
            continue

        rows.append(
            TradeRow(
                year=year,
                reporter_iso3="KAZ",
                reporter=str(record.get(columns["reporter"]) or "Kazakhstan").strip()
                if columns["reporter"] is not None
                else "Kazakhstan",
                partner_iso3=partner_iso3,
                partner=str(record.get(columns["partner"]) or "").strip(),
                cmd_code=cmd_code,
                cmd_desc=str(record.get(columns["cmd_desc"]) or "").strip()
                if columns["cmd_desc"] is not None
                else "",
                trade_value_usd=trade_value,
                net_weight_kg=_to_float(record.get(columns["net_weight_kg"]))
                if columns["net_weight_kg"] is not None
                else None,
                quantity=_to_float(record.get(columns["quantity"]))
                if columns["quantity"] is not None
                else None,
                qty_unit=str(record.get(columns["qty_unit"]) or "").strip()
                if columns["qty_unit"] is not None
                else "",
            )
        )

    return rows


def load_upgrade_paths(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_cepii_gravity(gravity_path: Path, reporter_iso3: str, partners: set[str]) -> dict[str, dict[str, float | str]]:
    gravity_by_partner: dict[str, dict[str, float | str]] = {}
    latest_year_by_partner: dict[str, int] = {}

    with gravity_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["iso3_o"] != reporter_iso3:
                continue
            partner_iso3 = row["iso3_d"]
            if partner_iso3 not in partners:
                continue

            year_text = row.get("year", "")
            if not year_text.isdigit():
                continue
            year = int(year_text)
            if year > 2025:
                continue

            previous = latest_year_by_partner.get(partner_iso3, -1)
            if year < previous:
                continue

            latest_year_by_partner[partner_iso3] = year
            gravity_by_partner[partner_iso3] = {
                "dist_km": _to_float(row.get("dist")),
                "contig": _to_int(row.get("contig")) or 0,
                "comlang_off": _to_int(row.get("comlang_off")) or 0,
                "rta_type": row.get("rta_type", ""),
                "year": year,
            }

    return gravity_by_partner


def fetch_world_bank_indicators(country_iso3_list: set[str]) -> dict[str, dict[int, dict[str, float | None]]]:
    result: dict[str, dict[int, dict[str, float | None]]] = {}
    for country_iso3 in sorted(country_iso3_list):
        per_year: dict[int, dict[str, float | None]] = defaultdict(dict)
        for feature_name, indicator in WORLD_BANK_INDICATORS.items():
            params = urlencode({"format": "json", "per_page": "100"})
            url = WORLD_BANK_BASE.format(country=country_iso3, indicator=indicator) + "?" + params
            with urlopen(url) as response:
                payload = json.loads(response.read().decode("utf-8"))
            observations = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
            for item in observations:
                date = item.get("date")
                if not date or not date.isdigit():
                    continue
                year = int(date)
                if year not in TARGET_YEARS:
                    continue
                per_year[year][feature_name] = item.get("value")
        result[country_iso3] = dict(per_year)
    return result


def summarize_export_base(trade_rows: list[TradeRow]) -> list[dict[str, object]]:
    by_code: dict[str, dict[str, object]] = {}
    for row in trade_rows:
        item = by_code.setdefault(
            row.cmd_code,
            {
                "cmd_code": row.cmd_code,
                "cmd_desc": row.cmd_desc or row.cmd_code,
                "trade_value_usd": 0.0,
                "net_weight_kg": 0.0,
                "years": set(),
                "partners": set(),
            },
        )
        item["trade_value_usd"] = float(item["trade_value_usd"]) + row.trade_value_usd
        item["years"].add(row.year)
        if row.partner:
            item["partners"].add(row.partner)
        if row.net_weight_kg is not None:
            item["net_weight_kg"] = float(item["net_weight_kg"]) + row.net_weight_kg

    summary = []
    for item in by_code.values():
        trade_value = float(item["trade_value_usd"])
        net_weight = float(item["net_weight_kg"])
        summary.append(
            {
                "cmd_code": item["cmd_code"],
                "cmd_desc": item["cmd_desc"],
                "trade_value_usd": trade_value,
                "partner_count": len(item["partners"]),
                "year_count": len(item["years"]),
                "avg_unit_value_usd_per_kg": trade_value / net_weight if net_weight > 0 else None,
            }
        )

    return sorted(summary, key=lambda row: float(row["trade_value_usd"]), reverse=True)


def build_recommendations(
    trade_rows: list[TradeRow],
    upgrade_paths: list[dict[str, str]],
    world_bank: dict[str, dict[int, dict[str, float | None]]],
    gravity_by_partner: dict[str, dict[str, float | str]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    latest_kaz_gdp = None
    for year in sorted(TARGET_YEARS, reverse=True):
        latest_kaz_gdp = world_bank.get("KAZ", {}).get(year, {}).get("gdp_current_usd")
        if latest_kaz_gdp:
            break

    base_summary = summarize_export_base(trade_rows)
    summary_by_code = {row["cmd_code"]: row for row in base_summary}

    partner_rows_by_code: dict[str, list[TradeRow]] = defaultdict(list)
    for row in trade_rows:
        partner_rows_by_code[row.cmd_code].append(row)

    recs: list[dict[str, object]] = []
    partner_csv_rows: list[dict[str, object]] = []
    for path in upgrade_paths:
        base_code = path["base_hs6"]
        upgrade_code = path["upgrade_hs6"]
        base = summary_by_code.get(base_code)
        if base is None:
            continue

        if upgrade_code in summary_by_code:
            continue

        stage_gap = float(path["stage_gap"])
        value_multiplier = float(path["value_multiplier"])
        base_trade_value = float(base["trade_value_usd"])
        source_rows = partner_rows_by_code[base_code]
        partner_scores = []
        aggregated_partner_rows: dict[str, dict[str, object]] = {}
        for row in source_rows:
            partner_iso3 = row.partner_iso3
            if not partner_iso3:
                continue
            item = aggregated_partner_rows.setdefault(
                partner_iso3,
                {
                    "partner_iso3": partner_iso3,
                    "partner_name": row.partner,
                    "base_trade_value_usd": 0.0,
                },
            )
            item["base_trade_value_usd"] = float(item["base_trade_value_usd"]) + row.trade_value_usd

        for partner_iso3, aggregated in aggregated_partner_rows.items():
            yearly_features = world_bank.get(partner_iso3, {})
            gdp = None
            gdp_growth = None
            for year in sorted(TARGET_YEARS, reverse=True):
                if gdp is None:
                    gdp = yearly_features.get(year, {}).get("gdp_current_usd")
                if gdp_growth is None:
                    gdp_growth = yearly_features.get(year, {}).get("gdp_growth_pct")
                if gdp is not None and gdp_growth is not None:
                    break

            gravity = gravity_by_partner.get(partner_iso3, {})
            dist_km = gravity.get("dist_km") or 5000.0
            contig = gravity.get("contig") or 0
            lang = gravity.get("comlang_off") or 0
            score = (
                math.log10(max(float(gdp or 1.0), 1.0)) * 0.55
                + float(gdp_growth or 0.0) * 0.15
                + (1.0 if contig else 0.0) * 0.15
                + (1.0 if lang else 0.0) * 0.05
                + (1.0 / math.log10(max(float(dist_km), 10.0))) * 0.10
            )
            partner_scores.append(
                {
                    "upgrade_hs6": upgrade_code,
                    "partner_iso3": partner_iso3,
                    "partner_name": str(aggregated["partner_name"]),
                    "partner_score": score,
                    "partner_gdp_usd": gdp,
                    "partner_gdp_growth_pct": gdp_growth,
                    "distance_km": dist_km,
                    "shared_border": contig,
                    "common_language": lang,
                    "base_trade_value_usd": aggregated["base_trade_value_usd"],
                }
            )

        partner_scores.sort(key=lambda item: item["partner_score"], reverse=True)
        top_partners = partner_scores[:5]
        average_partner_score = sum(item["partner_score"] for item in top_partners) / len(top_partners) if top_partners else 0.0

        convertible_share = 0.12 if stage_gap <= 1 else 0.08
        export_uplift_usd = base_trade_value * convertible_share * (value_multiplier - 1.0)
        gdp_uplift_pct = (export_uplift_usd / latest_kaz_gdp * 100.0) if latest_kaz_gdp else None
        opportunity_score = (
            math.log10(max(base_trade_value, 1.0)) * 0.42
            + average_partner_score * 0.28
            + value_multiplier * 0.20
            + (2.5 - stage_gap) * 0.10
        )

        recs.append(
            {
                "base_hs6": base_code,
                "base_label": path["base_label"],
                "upgrade_hs6": upgrade_code,
                "upgrade_label": path["upgrade_label"],
                "processing_family": path["processing_family"],
                "base_export_value_usd_2020_2025": round(base_trade_value, 2),
                "base_partner_count": base["partner_count"],
                "base_avg_unit_value_usd_per_kg": base["avg_unit_value_usd_per_kg"],
                "value_multiplier_assumption": value_multiplier,
                "convertible_share_assumption": convertible_share,
                "estimated_export_uplift_usd": round(export_uplift_usd, 2),
                "estimated_gdp_uplift_pct": round(gdp_uplift_pct, 6) if gdp_uplift_pct is not None else None,
                "opportunity_score": round(opportunity_score, 6),
                "top_target_markets": ", ".join(item["partner_name"] for item in top_partners),
                "short_conclusion": (
                    f"Moving from {base_code} to {upgrade_code} looks realistic given "
                    f"Kazakhstan's existing export base and current partner mix."
                ),
            }
        )

        for partner_row in top_partners:
            partner_csv_rows.append(
                {
                    "upgrade_hs6": upgrade_code,
                    "upgrade_label": path["upgrade_label"],
                    **partner_row,
                }
            )

    recs.sort(key=lambda row: float(row["opportunity_score"]), reverse=True)
    partner_csv_rows.sort(key=lambda row: (row["upgrade_hs6"], -float(row["partner_score"])))
    return recs, partner_csv_rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _wrap_label(code: str, label: str, max_words: int = 4) -> str:
    words = label.split()
    lines = [code]
    for index in range(0, len(words), max_words):
        lines.append(" ".join(words[index:index + max_words]))
    return "\n".join(lines)


def build_gdp_scenarios(recommendations: list[dict[str, object]]) -> list[dict[str, object]]:
    scenarios: list[dict[str, object]] = []
    for recommendation in recommendations:
        base_pct = float(recommendation["estimated_gdp_uplift_pct"] or 0.0)
        for scenario_name, multiplier in [("conservative", 0.6), ("base", 1.0), ("optimistic", 1.5)]:
            scenarios.append(
                {
                    "upgrade_hs6": recommendation["upgrade_hs6"],
                    "upgrade_label": recommendation["upgrade_label"],
                    "scenario": scenario_name,
                    "predicted_gdp_growth_uplift_pct": round(base_pct * multiplier, 6),
                }
            )
    return scenarios


def render_charts(
    export_base_rows: list[dict[str, object]],
    recommendations: list[dict[str, object]],
    charts_dir: Path,
) -> None:
    charts_dir.mkdir(parents=True, exist_ok=True)

    top_exports = export_base_rows[:10]
    if top_exports:
        plt.figure(figsize=(11, 6))
        labels = [_wrap_label(str(row["cmd_code"]), str(row["cmd_desc"])) for row in top_exports]
        values = [float(row["trade_value_usd"]) / 1_000_000 for row in top_exports]
        plt.bar(labels, values, color="#1f77b4")
        plt.title("Kazakhstan top HS6 exports, 2020-2025")
        plt.ylabel("USD million")
        plt.xlabel("HS6 product")
        plt.xticks(rotation=0, fontsize=8)
        plt.tight_layout()
        plt.savefig(charts_dir / "top_exports.png", dpi=160)
        plt.close()

    top_recs = recommendations[:8]
    if top_recs:
        plt.figure(figsize=(11, 6))
        labels = [_wrap_label(str(row["upgrade_hs6"]), str(row["upgrade_label"])) for row in top_recs]
        values = [float(row["estimated_export_uplift_usd"]) / 1_000_000 for row in top_recs]
        plt.bar(labels, values, color="#ff7f0e")
        plt.title("Estimated export uplift by recommended HS6 upgrade")
        plt.ylabel("USD million")
        plt.xlabel("Recommended upgrade")
        plt.xticks(rotation=0, fontsize=8)
        plt.tight_layout()
        plt.savefig(charts_dir / "estimated_export_uplift.png", dpi=160)
        plt.close()

        plt.figure(figsize=(11, 6))
        labels = [_wrap_label(str(row["upgrade_hs6"]), str(row["upgrade_label"])) for row in top_recs]
        values = [float(row["estimated_gdp_uplift_pct"] or 0.0) for row in top_recs]
        plt.bar(labels, values, color="#2ca02c")
        plt.title("Estimated Kazakhstan GDP uplift by recommended HS6 upgrade")
        plt.ylabel("GDP uplift, %")
        plt.xlabel("Recommended upgrade")
        plt.xticks(rotation=0, fontsize=8)
        plt.tight_layout()
        plt.savefig(charts_dir / "estimated_gdp_uplift_pct.png", dpi=160)
        plt.close()

        gdp_scenarios = build_gdp_scenarios(top_recs)
        scenario_names = ["conservative", "base", "optimistic"]
        x_positions = list(range(len(top_recs)))
        width = 0.24
        plt.figure(figsize=(13, 7))
        for offset, scenario_name in enumerate(scenario_names):
            scenario_values = [
                next(
                    item["predicted_gdp_growth_uplift_pct"]
                    for item in gdp_scenarios
                    if item["upgrade_hs6"] == recommendation["upgrade_hs6"] and item["scenario"] == scenario_name
                )
                for recommendation in top_recs
            ]
            shifted = [x + (offset - 1) * width for x in x_positions]
            plt.bar(shifted, scenario_values, width=width, label=scenario_name.title())

        plt.xticks(
            x_positions,
            [_wrap_label(str(row["upgrade_hs6"]), str(row["upgrade_label"])) for row in top_recs],
            fontsize=8,
        )
        plt.ylabel("Predicted GDP growth uplift, %")
        plt.xlabel("Recommended upgrade")
        plt.title("Predicted GDP growth uplift under conservative, base, and optimistic scenarios")
        plt.legend()
        plt.tight_layout()
        plt.savefig(charts_dir / "predicted_gdp_growth_scenarios.png", dpi=160)
        plt.close()


def ensure_output_dirs(base_dir: Path) -> tuple[Path, Path]:
    csv_dir = base_dir / "csv"
    charts_dir = base_dir / "charts"
    csv_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)
    return csv_dir, charts_dir


def save_comtrade_api_keys(primary_key: str | None, secondary_key: str | None) -> None:
    if primary_key:
        os.environ["UN_COMTRADE_PRIMARY_KEY"] = primary_key
    if secondary_key:
        os.environ["UN_COMTRADE_SECONDARY_KEY"] = secondary_key
