-- =============================================================================
-- schema.sql  —  Growify Digital · Task 3: SQL Schema & Query Layer
--
-- Database : SQLite (PostgreSQL-compatible syntax throughout)
-- Pattern  : Star schema — fact_campaigns & fact_sales as fact tables;
--            dim_campaign, dim_date as dimension tables.
--
-- This file is the SINGLE SOURCE OF TRUTH for both Power BI and the AI tool.
-- =============================================================================

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;   -- write-ahead log: better read concurrency

-- =============================================================================
-- DIMENSION: dim_date
-- All time-based filtering and grouping flows through this table.
-- Created by Python; defined here for documentation and Power BI reference.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date        TEXT    PRIMARY KEY,  -- ISO 8601  YYYY-MM-DD
    week        INTEGER NOT NULL,     -- ISO week number 1–53
    month       INTEGER NOT NULL,     -- 1–12
    quarter     INTEGER NOT NULL,     -- 1–4
    year        INTEGER NOT NULL,
    month_name  TEXT    NOT NULL,     -- 'January' … 'December'
    day_of_week TEXT                  -- 'Monday' … 'Sunday'
);

-- Why these indexes?
-- Power BI date slicers and AI tool date-range WHERE clauses hit year+month most.
CREATE INDEX IF NOT EXISTS idx_dim_date_ym  ON dim_date(year, month);
CREATE INDEX IF NOT EXISTS idx_dim_date_yq  ON dim_date(year, quarter);

-- =============================================================================
-- DIMENSION: dim_campaign
-- One row per unique campaign — attributes only, no metrics.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_id   TEXT  PRIMARY KEY,
    campaign_name TEXT  NOT NULL,
    platform      TEXT  NOT NULL,  -- Google / Meta / TikTok / LinkedIn …
    channel       TEXT  NOT NULL,  -- Paid Search / Display / Social / Email …
    region        TEXT,            -- EMEA / APAC / NA / LATAM …
    country       TEXT,
    brand_name    TEXT,
    status        TEXT,            -- Active / Paused / Ended
    start_date    TEXT,
    end_date      TEXT,
    budget        REAL,
    ad_group      TEXT,
    campaign_type TEXT,
    objective     TEXT
);

-- Why: platform and region are the two most-queried breakdown dimensions
-- in Power BI channel breakdown page and AI tool regional queries.
CREATE INDEX IF NOT EXISTS idx_dc_platform   ON dim_campaign(platform);
CREATE INDEX IF NOT EXISTS idx_dc_region     ON dim_campaign(region);
CREATE INDEX IF NOT EXISTS idx_dc_brand      ON dim_campaign(brand_name);
CREATE INDEX IF NOT EXISTS idx_dc_country    ON dim_campaign(country);

-- =============================================================================
-- FACT: fact_campaigns
-- One row per campaign per day — all numeric metrics live here.
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_campaigns (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id           TEXT    NOT NULL REFERENCES dim_campaign(campaign_id),
    date                  TEXT    NOT NULL REFERENCES dim_date(date),

    -- Volume metrics (recalculated by Python; source of truth)
    impressions           REAL    DEFAULT 0,
    clicks                REAL    DEFAULT 0,
    conversions           REAL    DEFAULT 0,
    spend                 REAL    DEFAULT 0,
    revenue               REAL    DEFAULT 0,

    -- Derived metrics (recalculated from source cols, not from export)
    ctr                   REAL,   -- clicks / impressions
    cpc                   REAL,   -- spend / clicks
    cpm                   REAL,   -- (spend / impressions) * 1000
    roi                   REAL,   -- (revenue - spend) / spend
    roas                  REAL,   -- revenue / spend

    -- Data quality flags set by Python cleaner
    ctr_wrong_flag        INTEGER DEFAULT 0,
    cpc_wrong_flag        INTEGER DEFAULT 0,
    cpm_wrong_flag        INTEGER DEFAULT 0,
    roi_wrong_flag        INTEGER DEFAULT 0,
    spend_outlier_flag    INTEGER DEFAULT 0,
    clicks_outlier_flag   INTEGER DEFAULT 0
);

-- Why: date and campaign_id appear in every join and WHERE clause.
-- Composite index eliminates table scans for the most common pattern.
CREATE INDEX IF NOT EXISTS idx_fc_date         ON fact_campaigns(date);
CREATE INDEX IF NOT EXISTS idx_fc_campaign_id  ON fact_campaigns(campaign_id);
CREATE INDEX IF NOT EXISTS idx_fc_date_camp    ON fact_campaigns(date, campaign_id);

-- =============================================================================
-- FACT: fact_sales  (Shopify orders)
-- One row per order line. Joins to dim_campaign via campaign_id for attribution.
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id         TEXT  PRIMARY KEY,
    order_date       TEXT  REFERENCES dim_date(date),
    campaign_id      TEXT  REFERENCES dim_campaign(campaign_id),

    -- Product
    product_type     TEXT,
    product_category TEXT,
    quantity         INTEGER DEFAULT 1,
    unit_price       REAL,
    total_price      REAL,
    total_tax        REAL    DEFAULT 0,
    total_discounts  REAL    DEFAULT 0,
    refund_amount    REAL    DEFAULT 0,
    net_revenue      REAL,   -- total_price - refund_amount
    aov              REAL,   -- total_price / quantity
    shipping_price   REAL    DEFAULT 0,

    -- Customer / Geo
    region           TEXT,
    country          TEXT,
    city             TEXT,
    customer_segment TEXT,
    customer_type    TEXT,
    payment_method   TEXT,

    -- Attribution
    channel          TEXT,
    source           TEXT,
    brand_name       TEXT,
    currency         TEXT  DEFAULT 'USD',
    status           TEXT,
    fulfillment_status TEXT,
    financial_status   TEXT,
    discount_code      TEXT
);

-- Why: order_date for date-range queries; region/country for geo drilldown;
-- campaign_id for attribution JOIN between Shopify and campaign tables.
CREATE INDEX IF NOT EXISTS idx_fs_order_date   ON fact_sales(order_date);
CREATE INDEX IF NOT EXISTS idx_fs_region       ON fact_sales(region);
CREATE INDEX IF NOT EXISTS idx_fs_country      ON fact_sales(country);
CREATE INDEX IF NOT EXISTS idx_fs_campaign_id  ON fact_sales(campaign_id);
CREATE INDEX IF NOT EXISTS idx_fs_channel      ON fact_sales(channel);

-- =============================================================================
-- VIEW: vw_powerbi_performance
-- PURPOSE: Primary data source for Power BI dashboard.
-- Aggregated by date + campaign + platform + channel + region.
-- Import this view directly via the SQLite ODBC connector.
-- =============================================================================
CREATE VIEW IF NOT EXISTS vw_powerbi_performance AS
SELECT
    -- ── Time dimensions (from dim_date) ──────────────────────────────────
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.date,

    -- ── Campaign dimensions (from dim_campaign) ───────────────────────────
    c.platform,
    c.channel,
    c.region,
    c.country,
    c.brand_name,
    c.campaign_id,
    c.campaign_name,
    c.status,

    -- ── Aggregated volume metrics ─────────────────────────────────────────
    SUM(f.impressions)  AS total_impressions,
    SUM(f.clicks)       AS total_clicks,
    SUM(f.conversions)  AS total_conversions,
    SUM(f.spend)        AS total_spend,
    SUM(f.revenue)      AS total_revenue,

    -- ── Recalculated rate metrics (safe division via CASE) ────────────────
    -- CTR: clicks / impressions
    CASE WHEN SUM(f.impressions) > 0
         THEN ROUND(SUM(f.clicks) * 1.0 / SUM(f.impressions), 6)
         ELSE 0 END AS ctr,

    -- CPC: spend / clicks
    CASE WHEN SUM(f.clicks) > 0
         THEN ROUND(SUM(f.spend) / SUM(f.clicks), 4)
         ELSE 0 END AS cpc,

    -- CPM: (spend / impressions) * 1000
    CASE WHEN SUM(f.impressions) > 0
         THEN ROUND(SUM(f.spend) / SUM(f.impressions) * 1000, 4)
         ELSE 0 END AS cpm,

    -- ROI: (revenue - spend) / spend
    CASE WHEN SUM(f.spend) > 0
         THEN ROUND((SUM(f.revenue) - SUM(f.spend)) / SUM(f.spend), 4)
         ELSE 0 END AS roi,

    -- ROAS: revenue / spend
    CASE WHEN SUM(f.spend) > 0
         THEN ROUND(SUM(f.revenue) / SUM(f.spend), 4)
         ELSE 0 END AS roas,

    -- Conversion rate: conversions / clicks
    CASE WHEN SUM(f.clicks) > 0
         THEN ROUND(SUM(f.conversions) * 1.0 / SUM(f.clicks), 4)
         ELSE 0 END AS conversion_rate

FROM fact_campaigns  f
JOIN dim_campaign    c ON f.campaign_id = c.campaign_id
JOIN dim_date        d ON f.date        = d.date
GROUP BY
    d.year, d.quarter, d.month, d.month_name, d.date,
    c.platform, c.channel, c.region, c.country,
    c.brand_name, c.campaign_id, c.campaign_name, c.status;


-- =============================================================================
-- VIEW: vw_ai_campaign_summary
-- PURPOSE: Primary data source for the AI insight tool.
-- The AI tool uses this for most natural-language queries.
-- Flexible: the Python layer adds WHERE clauses on top at runtime.
-- =============================================================================
CREATE VIEW IF NOT EXISTS vw_ai_campaign_summary AS
SELECT
    d.year,
    d.month,
    d.month_name,
    d.quarter,
    c.platform,
    c.channel,
    c.region,
    c.country,
    c.brand_name,
    c.campaign_id,
    c.campaign_name,
    c.status,
    SUM(f.impressions)  AS impressions,
    SUM(f.clicks)       AS clicks,
    SUM(f.conversions)  AS conversions,
    SUM(f.spend)        AS spend,
    SUM(f.revenue)      AS revenue,
    ROUND(SUM(f.clicks)      * 1.0 / NULLIF(SUM(f.impressions), 0), 4) AS ctr,
    ROUND(SUM(f.spend)             / NULLIF(SUM(f.clicks),       0), 2) AS cpc,
    ROUND(SUM(f.revenue)           / NULLIF(SUM(f.spend),        0), 2) AS roas,
    ROUND((SUM(f.revenue) - SUM(f.spend)) / NULLIF(SUM(f.spend), 0), 4) AS roi,
    ROUND(SUM(f.spend)       / NULLIF(SUM(f.impressions), 0) * 1000, 2) AS cpm,
    ROUND(SUM(f.conversions) * 1.0 / NULLIF(SUM(f.clicks),      0), 4) AS conversion_rate
FROM fact_campaigns f
JOIN dim_campaign   c ON f.campaign_id = c.campaign_id
JOIN dim_date       d ON f.date        = d.date
GROUP BY
    d.year, d.month, d.month_name, d.quarter,
    c.platform, c.channel, c.region, c.country,
    c.brand_name, c.campaign_id, c.campaign_name, c.status;


-- =============================================================================
-- VIEW: vw_top_campaigns_roas
-- PURPOSE: Executive summary Page 1 — top 5 campaigns table in Power BI.
-- =============================================================================
CREATE VIEW IF NOT EXISTS vw_top_campaigns_roas AS
SELECT
    c.campaign_id,
    c.campaign_name,
    c.platform,
    c.channel,
    c.region,
    SUM(f.spend)       AS total_spend,
    SUM(f.revenue)     AS total_revenue,
    SUM(f.conversions) AS total_conversions,
    ROUND(SUM(f.revenue) / NULLIF(SUM(f.spend), 0), 2) AS roas,
    ROUND(SUM(f.spend)   / NULLIF(SUM(f.clicks), 0), 2) AS cpc,
    ROUND(SUM(f.clicks) * 1.0 / NULLIF(SUM(f.impressions), 0), 4) AS ctr
FROM fact_campaigns f
JOIN dim_campaign   c ON f.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.platform, c.channel, c.region
ORDER BY roas DESC;


-- =============================================================================
-- VIEW: vw_country_performance
-- PURPOSE: Page 2 region matrix + country drilldown in Power BI.
--          Also answers AI tool "country-wise performance" questions.
-- =============================================================================
CREATE VIEW IF NOT EXISTS vw_country_performance AS
SELECT
    c.country,
    c.region,
    SUM(f.spend)       AS total_spend,
    SUM(f.revenue)     AS total_revenue,
    SUM(f.conversions) AS total_conversions,
    SUM(f.clicks)      AS total_clicks,
    ROUND(SUM(f.revenue) / NULLIF(SUM(f.spend),        0), 2) AS roas,
    ROUND(SUM(f.clicks) * 1.0 / NULLIF(SUM(f.impressions), 0), 4) AS ctr,
    ROUND(SUM(f.spend)   / NULLIF(SUM(f.clicks),       0), 2) AS cpc,
    ROUND((SUM(f.revenue)-SUM(f.spend)) / NULLIF(SUM(f.spend),0), 4) AS roi
FROM fact_campaigns f
JOIN dim_campaign   c ON f.campaign_id = c.campaign_id
GROUP BY c.country, c.region
ORDER BY total_spend DESC;


-- =============================================================================
-- VIEW: vw_mom_spend
-- PURPOSE: Month-over-month spend change — DAX measure cross-check
--          and direct answer for AI tool MoM questions.
-- =============================================================================
CREATE VIEW IF NOT EXISTS vw_mom_spend AS
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.spend)                                                   AS monthly_spend,
    LAG(SUM(f.spend)) OVER (ORDER BY d.year, d.month)             AS prev_month_spend,
    ROUND(
        (SUM(f.spend) - LAG(SUM(f.spend)) OVER (ORDER BY d.year, d.month))
        / NULLIF(LAG(SUM(f.spend)) OVER (ORDER BY d.year, d.month), 0) * 100,
        2
    )                                                              AS mom_pct_change
FROM fact_campaigns f
JOIN dim_date d ON f.date = d.date
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- =============================================================================
-- VIEW: vw_platform_channel_breakdown
-- PURPOSE: Power BI Page 2 — platform bar chart + channel donut.
-- =============================================================================
CREATE VIEW IF NOT EXISTS vw_platform_channel_breakdown AS
SELECT
    c.platform,
    c.channel,
    SUM(f.spend)       AS total_spend,
    SUM(f.revenue)     AS total_revenue,
    SUM(f.conversions) AS total_conversions,
    SUM(f.clicks)      AS total_clicks,
    SUM(f.impressions) AS total_impressions,
    ROUND(SUM(f.revenue) / NULLIF(SUM(f.spend),        0), 2) AS roas,
    ROUND(SUM(f.spend)   / NULLIF(SUM(f.clicks),       0), 2) AS cpc,
    ROUND(SUM(f.clicks) * 1.0 / NULLIF(SUM(f.impressions), 0), 4) AS ctr,
    ROUND(SUM(f.conversions) * 1.0 / NULLIF(SUM(f.clicks), 0), 4) AS conversion_rate
FROM fact_campaigns f
JOIN dim_campaign   c ON f.campaign_id = c.campaign_id
GROUP BY c.platform, c.channel
ORDER BY total_spend DESC;


-- =============================================================================
-- INDEX RATIONALE  (for reviewer)
-- =============================================================================
/*
  idx_fc_date          → date is in every WHERE clause (date-range filters)
  idx_fc_campaign_id   → JOIN to dim_campaign on every query
  idx_fc_date_camp     → composite eliminates table scan for date+campaign joins
  idx_dc_platform      → #1 breakdown dimension in BI and AI tool
  idx_dc_region        → regional drilldowns, country-wise views
  idx_dc_brand         → cross-page slicer on brand_name
  idx_dc_country       → country map visual + geo filters
  idx_fs_order_date    → Shopify date-range queries
  idx_fs_region        → Shopify regional breakdown
  idx_fs_campaign_id   → JOIN between Shopify and campaign for attribution
  idx_fs_channel       → channel breakdown on Shopify sales
*/
