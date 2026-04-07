from __future__ import annotations

import csv
import gzip
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd


WORLD_BANK_BASE = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
WORLD_BANK_INDICATORS = {
    "gdp_current_usd": "NY.GDP.MKTP.CD",
    "gdp_growth_pct": "NY.GDP.MKTP.KD.ZG",
    "inflation_pct": "FP.CPI.TOTL.ZG",
    "trade_share_gdp_pct": "NE.TRD.GNFS.ZS",
}
TARGET_YEARS = {2020, 2021, 2022, 2023, 2024, 2025}
WORLD_BANK_BATCH_SIZE = 40
TOP_EXPORT_CHART_COUNT = 15
TOP_RECOMMENDATION_CHART_COUNT = 15
HS6_REFERENCE_LABELS = {
    "100199": "Other wheat and meslin",
    "110100": "Wheat or meslin flour",
    "260300": "Copper ores and concentrates",
    "270900": "Crude petroleum oils and oils from bituminous minerals",
    "271019": "Medium oils and petroleum preparations, nes",
    "271121": "Natural gas in gaseous state",
    "281820": "Aluminium oxide",
    "284410": "Uranium enriched in U235 and related compounds",
    "710691": "Silver, unwrought",
    "720241": "Ferro-chromium containing more than 4 percent carbon",
    "740311": "Refined copper cathodes and sections of cathodes",
    "740319": "Refined copper, unwrought, other",
    "760110": "Unwrought aluminium, not alloyed",
    "790111": "Zinc, not alloyed, containing at least 99.99 percent zinc",
    "880240": "Aeroplanes and other aircraft exceeding 15000 kg",
}
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
    flow: str


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


def _is_hs6_code(code: str) -> bool:
    code = code.strip()
    return len(code) == 6 and code.isdigit()


def _normalize_flow(raw: str | None) -> str | None:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    if text in {"x", "2", "e", "export", "exports"}:
        return "export"
    if text in {"m", "1", "i", "import", "imports"}:
        return "import"
    return None


def load_comtrade_trade_rows_from_dir(comtrade_dir: Path) -> list[TradeRow]:
    if not comtrade_dir.exists():
        raise FileNotFoundError(
            f"Comtrade directory not found: {comtrade_dir}. "
            "Place one or more CSV exports there."
        )

    rows: list[TradeRow] = []
    files = (
        sorted(comtrade_dir.glob("*.csv"))
        + sorted(comtrade_dir.glob("*.parquet"))
        + sorted(comtrade_dir.glob("*.pq"))
        + sorted(comtrade_dir.glob("*.txt.gz"))
        + sorted(comtrade_dir.glob("*.gz"))
    )
    if not files:
        raise FileNotFoundError(
            f"No CSV or Parquet files found in {comtrade_dir}. "
            "Export UN Comtrade trade files and place them there."
        )

    print(f"[trade_pipeline] Comtrade files discovered: {len(files)} in {comtrade_dir}")

    for csv_path in files:
        before_count = len(rows)
        print(f"[trade_pipeline] Reading {csv_path.name} ...")
        if _looks_like_gzip_text(csv_path):
            rows.extend(_load_comtrade_trade_rows_from_gzip_tsv(csv_path))
            print(
                f"[trade_pipeline] Finished {csv_path.name}: +{len(rows) - before_count} rows "
                f"(cumulative {len(rows)})"
            )
            continue

        if csv_path.suffix.lower() in {".parquet", ".pq"}:
            rows.extend(_load_comtrade_trade_rows_from_parquet(csv_path))
            print(
                f"[trade_pipeline] Finished {csv_path.name}: +{len(rows) - before_count} rows "
                f"(cumulative {len(rows)})"
            )
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

                flow = _normalize_flow(record.get(columns["flow"])) if columns["flow"] is not None else "export"
                if flow is None:
                    continue

                reporter_iso3 = ""
                if columns["reporter_iso3"] is not None:
                    reporter_iso3 = (record.get(columns["reporter_iso3"]) or "").strip().upper()

                partner_iso3 = ""
                if columns["partner_iso3"] is not None:
                    partner_iso3 = (record.get(columns["partner_iso3"]) or "").strip().upper()

                cmd_code = (record.get(columns["cmd_code"]) or "").strip()
                if not cmd_code or cmd_code.upper() == "TOTAL" or not _is_hs6_code(cmd_code):
                    continue

                trade_value = _to_float(record.get(columns["trade_value_usd"]))
                if trade_value is None or trade_value <= 0:
                    continue

                rows.append(
                    TradeRow(
                        year=year,
                        reporter_iso3=reporter_iso3,
                        reporter=(record.get(columns["reporter"]) or reporter_iso3 or "Unknown reporter").strip()
                        if columns["reporter"] is not None
                        else (reporter_iso3 or "Unknown reporter"),
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
                        flow=flow,
                    )
                )

        print(
            f"[trade_pipeline] Finished {csv_path.name}: +{len(rows) - before_count} rows "
            f"(cumulative {len(rows)})"
        )

    if not rows:
        raise ValueError("No HS6 trade rows for 2020-2025 were found in the supplied Comtrade files.")
    print(f"[trade_pipeline] Total loaded trade rows: {len(rows)}")
    return rows


def _looks_like_gzip_text(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        with path.open("rb") as handle:
            return handle.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def _load_comtrade_trade_rows_from_gzip_tsv(path: Path) -> list[TradeRow]:
    rows: list[TradeRow] = []
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            return rows

        for record in reader:
            year = _to_int(record.get("refYear") or record.get("period"))
            if year not in TARGET_YEARS:
                continue

            reporter_code = str(record.get("reporterCode") or "").strip()

            flow = _normalize_flow(record.get("flowCode"))
            if flow is None:
                continue

            classification_code = str(record.get("classificationCode") or "").strip().upper()
            if classification_code and not classification_code.startswith("H"):
                continue

            cmd_code = str(record.get("cmdCode") or "").strip()
            if not cmd_code or cmd_code.upper() == "TOTAL" or not _is_hs6_code(cmd_code):
                continue

            is_aggregate = str(record.get("isAggregate") or "").strip()
            if is_aggregate == "1":
                continue

            trade_value = _to_float(record.get("primaryValue") or record.get("FOBValue") or record.get("CIFValue"))
            if trade_value is None or trade_value <= 0:
                continue

            partner_iso3 = ""
            partner_code = str(record.get("partnerCode") or "").strip()
            if partner_code == "0":
                partner_iso3 = "WLD"

            rows.append(
                TradeRow(
                    year=year,
                    reporter_iso3=reporter_code,
                    reporter=reporter_code or "Unknown reporter",
                    partner_iso3=partner_iso3,
                    partner=partner_code or "Unknown partner",
                    cmd_code=cmd_code,
                    cmd_desc=cmd_code,
                    trade_value_usd=trade_value,
                    net_weight_kg=_to_float(record.get("netWgt")),
                    quantity=_to_float(record.get("qty")),
                    qty_unit=str(record.get("qtyUnitCode") or "").strip(),
                    flow=flow,
                )
            )
    return rows


def load_cepii_country_reference(countries_path: Path) -> tuple[dict[str, str], dict[str, str]]:
    iso3_by_numeric: dict[str, str] = {}
    name_by_iso3: dict[str, str] = {}
    with countries_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            iso3 = (row.get("iso3") or "").strip().upper()
            iso3num = (row.get("iso3num") or "").strip()
            country_name = (row.get("country") or row.get("countrylong") or iso3).strip()
            if iso3 and iso3num and iso3num not in iso3_by_numeric:
                iso3_by_numeric[iso3num] = iso3
            if iso3 and iso3 not in name_by_iso3:
                name_by_iso3[iso3] = country_name
    return iso3_by_numeric, name_by_iso3


def _load_comtrade_trade_rows_from_parquet(parquet_path: Path) -> list[TradeRow]:
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

        flow = _normalize_flow(record.get(columns["flow"])) if columns["flow"] is not None else "export"
        if flow is None:
            continue

        reporter_iso3 = ""
        if columns["reporter_iso3"] is not None:
            reporter_iso3 = str(record.get(columns["reporter_iso3"]) or "").strip().upper()

        partner_iso3 = ""
        if columns["partner_iso3"] is not None:
            partner_iso3 = str(record.get(columns["partner_iso3"]) or "").strip().upper()

        cmd_code = str(record.get(columns["cmd_code"]) or "").strip()
        if not cmd_code or cmd_code.upper() == "TOTAL" or not _is_hs6_code(cmd_code):
            continue

        trade_value = _to_float(record.get(columns["trade_value_usd"]))
        if trade_value is None or trade_value <= 0:
            continue

        rows.append(
            TradeRow(
                year=year,
                reporter_iso3=reporter_iso3,
                reporter=str(record.get(columns["reporter"]) or reporter_iso3 or "Unknown reporter").strip()
                if columns["reporter"] is not None
                else (reporter_iso3 or "Unknown reporter"),
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
                flow=flow,
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
    result: dict[str, dict[int, dict[str, float | None]]] = {
        country_iso3: {} for country_iso3 in sorted(country_iso3_list)
    }
    country_codes = sorted(country_iso3_list)
    print(
        f"[trade_pipeline] Fetching World Bank indicators for {len(country_codes)} countries: "
        f"{', '.join(country_codes)}"
    )
    for feature_name, indicator in WORLD_BANK_INDICATORS.items():
        print(f"[trade_pipeline] Indicator {feature_name} ({indicator})")
        for start in range(0, len(country_codes), WORLD_BANK_BATCH_SIZE):
            batch = country_codes[start:start + WORLD_BANK_BATCH_SIZE]
            batch_key = ";".join(batch)
            params = urlencode({"format": "json", "per_page": "20000"})
            url = WORLD_BANK_BASE.format(country=batch_key, indicator=indicator) + "?" + params
            print(
                f"[trade_pipeline]   batch {start // WORLD_BANK_BATCH_SIZE + 1}: "
                f"{', '.join(batch)}"
            )
            with urlopen(url) as response:
                payload = json.loads(response.read().decode("utf-8"))
            observations = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
            if observations is None:
                observations = []
            for item in observations:
                country_iso3 = item.get("countryiso3code")
                if not country_iso3 or country_iso3 not in result:
                    continue
                date = item.get("date")
                if not date or not date.isdigit():
                    continue
                year = int(date)
                if year not in TARGET_YEARS:
                    continue
                country_year = result[country_iso3].setdefault(year, {})
                country_year[feature_name] = item.get("value")
        print(f"[trade_pipeline] Completed indicator {feature_name}")
    print("[trade_pipeline] World Bank fetch completed")
    return result


def enrich_trade_rows_with_country_reference(
    trade_rows: list[TradeRow],
    *,
    iso3_by_numeric: dict[str, str],
    name_by_iso3: dict[str, str],
) -> list[TradeRow]:
    enriched: list[TradeRow] = []
    for row in trade_rows:
        reporter_code = row.reporter_iso3.strip()
        partner_code = row.partner.strip()
        resolved_reporter_iso3 = row.reporter_iso3
        resolved_reporter_name = row.reporter
        resolved_partner_iso3 = row.partner_iso3
        resolved_partner_name = row.partner

        if reporter_code in iso3_by_numeric:
            resolved_reporter_iso3 = iso3_by_numeric[reporter_code]
            resolved_reporter_name = name_by_iso3.get(resolved_reporter_iso3, resolved_reporter_iso3)
        elif len(reporter_code) == 3 and reporter_code.isalpha():
            resolved_reporter_iso3 = reporter_code.upper()
            resolved_reporter_name = name_by_iso3.get(resolved_reporter_iso3, row.reporter or resolved_reporter_iso3)

        if partner_code in iso3_by_numeric:
            resolved_partner_iso3 = iso3_by_numeric[partner_code]
            resolved_partner_name = name_by_iso3.get(resolved_partner_iso3, resolved_partner_iso3)
        elif partner_code == "0":
            resolved_partner_iso3 = "WLD"
            resolved_partner_name = "World"
        elif len(row.partner_iso3) == 3 and row.partner_iso3.isalpha():
            resolved_partner_iso3 = row.partner_iso3.upper()
            resolved_partner_name = name_by_iso3.get(resolved_partner_iso3, row.partner or resolved_partner_iso3)

        enriched.append(
            TradeRow(
                year=row.year,
                reporter_iso3=resolved_reporter_iso3,
                reporter=resolved_reporter_name,
                partner_iso3=resolved_partner_iso3,
                partner=resolved_partner_name,
                cmd_code=row.cmd_code,
                cmd_desc=row.cmd_desc,
                trade_value_usd=row.trade_value_usd,
                net_weight_kg=row.net_weight_kg,
                quantity=row.quantity,
                qty_unit=row.qty_unit,
                flow=row.flow,
            )
        )
    return enriched


def summarize_export_base(trade_rows: list[TradeRow]) -> list[dict[str, object]]:
    by_code: dict[str, dict[str, object]] = {}
    for row in trade_rows:
        if row.flow != "export":
            continue
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
                "cmd_desc": HS6_REFERENCE_LABELS.get(str(item["cmd_code"]), item["cmd_desc"]),
                "trade_value_usd": trade_value,
                "partner_count": len(item["partners"]),
                "year_count": len(item["years"]),
                "avg_unit_value_usd_per_kg": trade_value / net_weight if net_weight > 0 else None,
            }
        )

    return sorted(summary, key=lambda row: float(row["trade_value_usd"]), reverse=True)


def build_recommendations(
    *,
    target_country_iso3: str,
    target_country_name: str,
    trade_rows: list[TradeRow],
    upgrade_paths: list[dict[str, str]],
    world_bank: dict[str, dict[int, dict[str, float | None]]],
    gravity_by_partner: dict[str, dict[str, float | str]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    latest_target_gdp = None
    for year in sorted(TARGET_YEARS, reverse=True):
        latest_target_gdp = world_bank.get(target_country_iso3, {}).get(year, {}).get("gdp_current_usd")
        if latest_target_gdp:
            break

    target_export_rows = [
        row for row in trade_rows if row.reporter_iso3 == target_country_iso3 and row.flow == "export"
    ]
    base_summary = summarize_export_base(target_export_rows)
    summary_by_code = {row["cmd_code"]: row for row in base_summary}

    target_partner_rows_by_code: dict[str, list[TradeRow]] = defaultdict(list)
    regional_upgrade_exports: dict[str, list[TradeRow]] = defaultdict(list)
    regional_upgrade_imports: dict[str, list[TradeRow]] = defaultdict(list)
    for row in trade_rows:
        if row.reporter_iso3 == target_country_iso3 and row.flow == "export":
            target_partner_rows_by_code[row.cmd_code].append(row)
        elif row.reporter_iso3 != target_country_iso3 and row.flow == "export":
            regional_upgrade_exports[row.cmd_code].append(row)
        elif row.reporter_iso3 != target_country_iso3 and row.flow == "import":
            regional_upgrade_imports[row.cmd_code].append(row)

    recs: list[dict[str, object]] = []
    partner_csv_rows: list[dict[str, object]] = []
    for path in upgrade_paths:
        base_code = path["base_hs6"]
        upgrade_code = path["upgrade_hs6"]
        base = summary_by_code.get(base_code)
        if base is None:
            continue

        existing_upgrade = summary_by_code.get(upgrade_code)

        stage_gap = float(path["stage_gap"])
        value_multiplier = float(path["value_multiplier"])
        base_trade_value = float(base["trade_value_usd"])
        existing_upgrade_value = float(existing_upgrade["trade_value_usd"]) if existing_upgrade is not None else 0.0
        development_ratio = existing_upgrade_value / base_trade_value if base_trade_value > 0 else 0.0

        # Skip only when the downstream product is already strongly established.
        if existing_upgrade is not None and development_ratio >= 0.35:
            continue

        source_rows = target_partner_rows_by_code[base_code]
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

        regional_export_rows = regional_upgrade_exports.get(upgrade_code, [])
        regional_import_rows = regional_upgrade_imports.get(upgrade_code, [])
        regional_exporters = {row.reporter_iso3 for row in regional_export_rows if row.reporter_iso3}
        regional_importers = {row.reporter_iso3 for row in regional_import_rows if row.reporter_iso3}
        regional_export_value = sum(row.trade_value_usd for row in regional_export_rows)
        regional_import_value = sum(row.trade_value_usd for row in regional_import_rows)

        for row in regional_import_rows:
            market_iso3 = row.reporter_iso3
            if not market_iso3:
                continue
            item = aggregated_partner_rows.setdefault(
                market_iso3,
                {
                    "partner_iso3": market_iso3,
                    "partner_name": row.reporter,
                    "base_trade_value_usd": 0.0,
                    "regional_import_value_usd": 0.0,
                },
            )
            item["regional_import_value_usd"] = float(item.get("regional_import_value_usd", 0.0)) + row.trade_value_usd

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
            import_demand_value = float(aggregated.get("regional_import_value_usd", 0.0) or 0.0)
            score = (
                math.log10(max(float(gdp or 1.0), 1.0)) * 0.55
                + float(gdp_growth or 0.0) * 0.15
                + (1.0 if contig else 0.0) * 0.15
                + (1.0 if lang else 0.0) * 0.05
                + (1.0 / math.log10(max(float(dist_km), 10.0))) * 0.10
                + math.log10(max(import_demand_value, 1.0)) * 0.12
            )
            partner_scores.append(
                {
                    "upgrade_hs6": upgrade_code,
                    "partner_iso3": partner_iso3,
                    "partner_name": str(aggregated["partner_name"]),
                    "partner_score": score,
                    "partner_gdp_usd": gdp,
                    "partner_gdp_growth_pct": gdp_growth,
                    "regional_import_value_usd": round(import_demand_value, 2),
                    "distance_km": dist_km,
                    "shared_border": contig,
                    "common_language": lang,
                    "base_trade_value_usd": aggregated["base_trade_value_usd"],
                }
            )

        partner_scores.sort(key=lambda item: item["partner_score"], reverse=True)
        top_partners = partner_scores[:5]
        average_partner_score = sum(item["partner_score"] for item in top_partners) / len(top_partners) if top_partners else 0.0

        if existing_upgrade is None:
            development_status = "new_export_opportunity"
            convertible_share = 0.12 if stage_gap <= 1 else 0.08
        else:
            development_status = "underdeveloped_export"
            convertible_share = 0.08 if stage_gap <= 1 else 0.05

        export_uplift_usd = base_trade_value * convertible_share * (value_multiplier - 1.0)
        gdp_uplift_pct = (export_uplift_usd / latest_target_gdp * 100.0) if latest_target_gdp else None
        opportunity_score = (
            math.log10(max(base_trade_value, 1.0)) * 0.42
            + average_partner_score * 0.28
            + value_multiplier * 0.20
            + (2.5 - stage_gap) * 0.10
            + (1.0 - min(development_ratio, 1.0)) * 0.12
            + math.log10(max(regional_export_value, 1.0)) * 0.07
            + math.log10(max(regional_import_value, 1.0)) * 0.09
            + min(len(regional_exporters), 10) * 0.03
            + min(len(regional_importers), 10) * 0.04
        )

        recs.append(
            {
                "base_hs6": base_code,
                "base_label": path["base_label"],
                "upgrade_hs6": upgrade_code,
                "upgrade_label": path["upgrade_label"],
                "development_status": development_status,
                "processing_family": path["processing_family"],
                "base_export_value_usd_2020_2025": round(base_trade_value, 2),
                "current_upgrade_export_value_usd_2020_2025": round(existing_upgrade_value, 2),
                "current_upgrade_to_base_ratio": round(development_ratio, 6),
                "base_partner_count": base["partner_count"],
                "base_avg_unit_value_usd_per_kg": base["avg_unit_value_usd_per_kg"],
                "value_multiplier_assumption": value_multiplier,
                "convertible_share_assumption": convertible_share,
                "regional_exporter_count": len(regional_exporters),
                "regional_export_value_usd": round(regional_export_value, 2),
                "regional_importer_count": len(regional_importers),
                "regional_import_value_usd": round(regional_import_value, 2),
                "estimated_export_uplift_usd": round(export_uplift_usd, 2),
                "estimated_gdp_uplift_pct": round(gdp_uplift_pct, 6) if gdp_uplift_pct is not None else None,
                "opportunity_score": round(opportunity_score, 6),
                "top_target_markets": ", ".join(item["partner_name"] for item in top_partners),
                "short_conclusion": (
                    f"Moving from {base_code} to {upgrade_code} looks realistic given "
                    f"{target_country_name}'s export base, regional capability, and nearby demand."
                    if existing_upgrade is None
                    else f"{upgrade_code} is already exported, but still looks underdeveloped relative to {base_code}."
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
    *,
    target_country_name: str = "Kazakhstan",
) -> None:
    import matplotlib.pyplot as plt

    charts_dir.mkdir(parents=True, exist_ok=True)

    top_exports = export_base_rows[:TOP_EXPORT_CHART_COUNT]
    if top_exports:
        plt.figure(figsize=(14, 8))
        labels = [_wrap_label(str(row["cmd_code"]), str(row["cmd_desc"])) for row in top_exports]
        values = [float(row["trade_value_usd"]) / 1_000_000 for row in top_exports]
        plt.bar(labels, values, color="#1f77b4")
        plt.title(f"{target_country_name} top HS6 exports")
        plt.ylabel("USD million")
        plt.xlabel("HS6 product")
        plt.xticks(rotation=35, ha="right", fontsize=8)
        plt.tight_layout()
        plt.savefig(charts_dir / "top_exports.png", dpi=160)
        plt.close()

    top_recs = recommendations[:TOP_RECOMMENDATION_CHART_COUNT]
    if top_recs:
        plt.figure(figsize=(14, 8))
        labels = [_wrap_label(str(row["upgrade_hs6"]), str(row["upgrade_label"])) for row in top_recs]
        values = [float(row["estimated_export_uplift_usd"]) / 1_000_000 for row in top_recs]
        plt.bar(labels, values, color="#ff7f0e")
        plt.title("Estimated export uplift by recommended HS6 upgrade")
        plt.ylabel("USD million")
        plt.xlabel("Recommended upgrade")
        plt.xticks(rotation=35, ha="right", fontsize=8)
        plt.tight_layout()
        plt.savefig(charts_dir / "estimated_export_uplift.png", dpi=160)
        plt.close()

        plt.figure(figsize=(14, 8))
        labels = [_wrap_label(str(row["upgrade_hs6"]), str(row["upgrade_label"])) for row in top_recs]
        values = [float(row["estimated_gdp_uplift_pct"] or 0.0) for row in top_recs]
        plt.bar(labels, values, color="#2ca02c")
        plt.title(f"Estimated {target_country_name} GDP uplift by recommended HS6 upgrade")
        plt.ylabel("GDP uplift, %")
        plt.xlabel("Recommended upgrade")
        plt.xticks(rotation=35, ha="right", fontsize=8)
        plt.tight_layout()
        plt.savefig(charts_dir / "estimated_gdp_uplift_pct.png", dpi=160)
        plt.close()

        gdp_scenarios = build_gdp_scenarios(top_recs)
        scenario_names = ["conservative", "base", "optimistic"]
        x_positions = list(range(len(top_recs)))
        width = 0.24
        plt.figure(figsize=(15, 8))
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
            rotation=35,
            ha="right",
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
