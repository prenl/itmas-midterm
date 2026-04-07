CREATE TABLE countries (
    id SERIAL PRIMARY KEY,
    comtrade_code INTEGER UNIQUE,
    iso3 CHAR(3) UNIQUE NOT NULL,
    name TEXT NOT NULL,
    iso2 CHAR(2),
    region TEXT,
    income_group TEXT,
    is_group BOOLEAN DEFAULT FALSE,
    entry_effective_date DATE,
    entry_expired_date DATE
);

CREATE TABLE hs_codes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    level SMALLINT NOT NULL CHECK (level IN (2, 4, 6)),
    parent_id INTEGER REFERENCES hs_codes(id),
    description TEXT NOT NULL
);

CREATE TABLE trade_flows (
    id BIGSERIAL PRIMARY KEY,
    reporter_country_id INTEGER NOT NULL REFERENCES countries(id),
    partner_country_id INTEGER NOT NULL REFERENCES countries(id),
    hs_code_id INTEGER NOT NULL REFERENCES hs_codes(id),
    flow_type VARCHAR(10) NOT NULL CHECK (flow_type IN ('import', 'export', 're-import', 're-export')),
    frequency CHAR(1) NOT NULL CHECK (frequency IN ('A', 'M')),
    year SMALLINT NOT NULL,
    month SMALLINT CHECK (month BETWEEN 1 AND 12),
    ref_period INTEGER,
    classification_code VARCHAR(16) DEFAULT 'HS',
    customs_code VARCHAR(16),
    mode_of_transport_code VARCHAR(16),
    partner_2_country_id INTEGER REFERENCES countries(id),
    trade_value_usd NUMERIC(20, 2) NOT NULL,
    quantity NUMERIC(20, 3),
    quantity_unit VARCHAR(32),
    alt_quantity NUMERIC(20, 3),
    alt_quantity_unit VARCHAR(32),
    trade_weight_kg NUMERIC(20, 3),
    gross_weight_kg NUMERIC(20, 3),
    is_quantity_estimated BOOLEAN,
    is_trade_weight_estimated BOOLEAN,
    is_reported BOOLEAN,
    is_aggregate BOOLEAN,
    avg_unit_price_usd NUMERIC(20, 4),
    source_system TEXT
);

CREATE INDEX idx_trade_flows_main
    ON trade_flows (reporter_country_id, partner_country_id, hs_code_id, year, flow_type);

CREATE TABLE macro_indicators (
    id BIGSERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL REFERENCES countries(id),
    year SMALLINT NOT NULL,
    month SMALLINT CHECK (month BETWEEN 1 AND 12),
    gdp_usd NUMERIC(20, 2),
    gdp_growth_pct NUMERIC(8, 3),
    inflation_pct NUMERIC(8, 3),
    exchange_rate_to_usd NUMERIC(18, 6),
    unemployment_pct NUMERIC(8, 3),
    trade_openness_pct NUMERIC(8, 3),
    logistics_performance_index NUMERIC(8, 3),
    tariff_rate_avg_pct NUMERIC(8, 3),
    UNIQUE (country_id, year, month)
);

CREATE TABLE country_pair_features (
    id BIGSERIAL PRIMARY KEY,
    origin_country_id INTEGER NOT NULL REFERENCES countries(id),
    destination_country_id INTEGER NOT NULL REFERENCES countries(id),
    distance_km NUMERIC(12, 2),
    shared_border BOOLEAN DEFAULT FALSE,
    common_language BOOLEAN DEFAULT FALSE,
    trade_agreement BOOLEAN DEFAULT FALSE,
    sanctions_risk_score NUMERIC(6, 3),
    UNIQUE (origin_country_id, destination_country_id)
);

CREATE TABLE recommendation_events (
    id BIGSERIAL PRIMARY KEY,
    source_country_id INTEGER NOT NULL REFERENCES countries(id),
    target_country_id INTEGER REFERENCES countries(id),
    target_hs_code_id INTEGER REFERENCES hs_codes(id),
    recommendation_type VARCHAR(32) NOT NULL,
    model_name TEXT NOT NULL,
    score NUMERIC(12, 6) NOT NULL,
    generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    accepted BOOLEAN
);
