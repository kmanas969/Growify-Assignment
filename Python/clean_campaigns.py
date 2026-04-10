"""
Task 2 — Campaign Data Cleaning & Validation
Growify Digital Take-Home Assignment

Written for actual CSV columns detected:
  data_source_name            → platform
  campaign_name               → campaign_name
  campaign_effective_status   → status
  country_funnel              → country
  geo_location_segment        → region
  fb_spent_funnel_inr         → spend
  amount_spent_inr            → spend (fallback)
  clicks_all                  → clicks
  impressions                 → impressions
  purchases_conversion_value_inr → revenue
  purchases                   → conversions
  ad_set_name                 → ad_group
  date                        → date

Pipeline:
  1.  Load raw CSV as strings
  2.  Normalise column names
  3.  Strip whitespace + null placeholders
  4.  Remove duplicates
  5.  Rename columns to standard names
  6.  String normalisation
  7.  Date parsing
  8.  Numeric coercion (strip ₹/$/£/€/commas)
  9.  Missing value imputation
  10. Outlier flagging
  11. Recalculate CTR/CPC/CPM/ROI/ROAS
  12. Enforce dtypes + rounding
  13. Diagnostics
  14. Write to SQLite → campaigns, dim_date, dim_campaign, fact_campaigns
  15. Verify DB
  16. Write data_quality_report.md
"""

import os, sqlite3
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, types as T

# ══════════════════════════════════════════════════════════════════════════════
# PATHS
# ══════════════════════════════════════════════════════════════════════════════
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _find_data_dir(start):
    for step in [start, os.path.join(start,'..'), os.path.join(start,'../..')]:
        step = os.path.normpath(step)
        for name in ['Data','data']:
            p = os.path.join(step, name)
            if os.path.isdir(p): return p
    d = os.path.join(start,'Data'); os.makedirs(d, exist_ok=True); return d

DATA_DIR    = _find_data_dir(BASE_DIR)
RAW_PATH    = os.path.join(DATA_DIR, 'Campaign_Raw.csv')
DB_PATH     = os.path.join(DATA_DIR, 'cleaned_campaigns.db')
REPORT_PATH = os.path.join(BASE_DIR, 'data_quality_report.md')

print(f"[PATH] Data dir : {DATA_DIR}")
print(f"[PATH] Raw CSV  : {RAW_PATH}")
print(f"[PATH] Output DB: {DB_PATH}")

if not os.path.exists(RAW_PATH):
    raise FileNotFoundError(
        f"\n❌  campaigns_raw.csv not found at:\n    {RAW_PATH}\n"
        f"    Place the file in: {DATA_DIR}"
    )

issues = []
def log(msg):
    print(f"[INFO] {msg}")
    issues.append(msg)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — LOAD
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 1 — Load raw CSV")
df = pd.read_csv(RAW_PATH, dtype=str, low_memory=False)
ORIGINAL_ROWS = len(df)
log(f"  {ORIGINAL_ROWS} rows × {len(df.columns)} columns")
log(f"  Raw columns: {list(df.columns)}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — NORMALISE COLUMN NAMES
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 2 — Normalise column names")
df.columns = (df.columns
    .str.strip()
    .str.lower()
    .str.replace(r'[\s\-/]+', '_', regex=True)
    .str.replace(r'[^\w]', '', regex=True))
log(f"  Normalised: {list(df.columns)}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — STRIP WHITESPACE + NULL PLACEHOLDERS
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 3 — Strip whitespace and null placeholders")
df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)
df.replace(['','nan','none','null','n/a','na','-','--','#n/a',
            'N/A','NULL','None','NaN'], np.nan, inplace=True)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — DUPLICATES
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 4 — Remove duplicates")
exact = df.duplicated().sum()
df = df.drop_duplicates()
log(f"  Exact duplicates removed: {exact}")

# Business key: campaign_name + date (since no campaign_id)
bk = [c for c in ['campaign_name','date','data_source_name'] if c in df.columns]
if len(bk) >= 2:
    biz = df.duplicated(subset=bk).sum()
    df  = df.drop_duplicates(subset=bk)
    log(f"  Business-key duplicates removed (on {bk}): {biz}")
log(f"  Rows after dedup: {len(df)}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — RENAME COLUMNS TO STANDARD NAMES
# Map your actual Facebook Ads columns → standard names used everywhere
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 5 — Rename columns to standard names")

# ── Detect the best spend column ──────────────────────────────────────────────
spend_col = None
for candidate in ['fb_spent_funnel_inr', 'amount_spent_inr',
                  'spend_inr', 'spend', 'cost']:
    if candidate in df.columns:
        spend_col = candidate
        break

# ── Detect the best revenue column ───────────────────────────────────────────
revenue_col = None
for candidate in ['purchases_conversion_value_inr', 'revenue_inr',
                  'revenue', 'sales', 'conversion_value']:
    if candidate in df.columns:
        revenue_col = candidate
        break

# ── Detect conversions column ─────────────────────────────────────────────────
conv_col = None
for candidate in ['purchases', 'conversions', 'purchases_conversion_value_inr',
                  'checkouts_initiated', 'adds_to_cart']:
    if candidate in df.columns:
        conv_col = candidate
        break

# ── Detect campaign_id (create from campaign_name if missing) ─────────────────
if 'campaign_id' not in df.columns:
    if 'campaign_name' in df.columns:
        # Create a numeric ID from campaign_name
        df['campaign_id'] = pd.factorize(df['campaign_name'])[0] + 1
        df['campaign_id'] = df['campaign_id'].astype(str)
        log("  campaign_id created from campaign_name (factorize)")
    else:
        df['campaign_id'] = [str(i+1) for i in range(len(df))]
        log("  campaign_id created as row index")

# ── Rename map ────────────────────────────────────────────────────────────────
rename_map = {}

# Platform
if 'data_source_name' in df.columns:
    rename_map['data_source_name'] = 'platform'
    log("  data_source_name → platform")

# Status
if 'campaign_effective_status' in df.columns:
    rename_map['campaign_effective_status'] = 'status'
    log("  campaign_effective_status → status")

# Country
if 'country_funnel' in df.columns:
    rename_map['country_funnel'] = 'country'
    log("  country_funnel → country")

# Region
if 'geo_location_segment' in df.columns:
    rename_map['geo_location_segment'] = 'region'
    log("  geo_location_segment → region")

# Ad group
if 'ad_set_name' in df.columns:
    rename_map['ad_set_name'] = 'ad_group'
    log("  ad_set_name → ad_group")

# Clicks
if 'clicks_all' in df.columns:
    rename_map['clicks_all'] = 'clicks'
    log("  clicks_all → clicks")

# Spend
if spend_col and spend_col != 'spend':
    rename_map[spend_col] = 'spend'
    log(f"  {spend_col} → spend")

# Revenue
if revenue_col and revenue_col != 'revenue':
    rename_map[revenue_col] = 'revenue'
    log(f"  {revenue_col} → revenue")

# Conversions
if conv_col and conv_col not in ['spend','revenue'] and conv_col != 'conversions':
    if conv_col not in rename_map.values():
        rename_map[conv_col] = 'conversions'
        log(f"  {conv_col} → conversions")

# Apply rename
df.rename(columns=rename_map, inplace=True)
log(f"  Columns after rename: {list(df.columns)}")

# ── Add missing standard columns with defaults ────────────────────────────────
for col, default in [('platform','Facebook'), ('channel','Paid Social'),
                     ('region','Unknown'), ('brand_name','Unknown'),
                     ('ad_group','Unknown'), ('budget', np.nan)]:
    if col not in df.columns:
        df[col] = default
        log(f"  Added missing column '{col}' with default '{default}'")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — STRING NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 6 — String normalisation")
STR_COLS = ['platform','channel','region','status','campaign_name',
            'ad_group','country','brand_name']
for col in STR_COLS:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title().replace('Nan','Unknown')
        df[col] = df[col].replace('', 'Unknown').fillna('Unknown')

# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — DATE PARSING
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 7 — Date parsing")
DATE_FMTS = ['%Y-%m-%d','%d/%m/%Y','%m/%d/%Y','%d-%m-%Y','%Y/%m/%d',
             '%d %b %Y','%B %d, %Y','%d %B %Y','%Y%m%d','%d.%m.%Y']

def parse_dates(s):
    out = pd.to_datetime(s, errors='coerce')
    mask = out.isna() & s.notna()
    for fmt in DATE_FMTS:
        if not mask.any(): break
        out[mask] = pd.to_datetime(s[mask], format=fmt, errors='coerce')
        mask = out.isna() & s.notna()
    return out

if 'date' in df.columns:
    b = df['date'].isna().sum()
    df['date'] = pd.to_datetime(df['date'], errors='coerce', format='mixed')
    log(f"  date: {b} nulls before → {df['date'].isna().sum()} after")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — NUMERIC COERCION
# Strip ₹ $ £ € commas before converting — this fixes empty column bug
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 8 — Numeric coercion")

def to_num(s):
    if s.dtype == object:
        s = (s.astype(str)
               .str.replace(r'[₹\$£€,\s]','', regex=True)
               .str.replace(r'%$','', regex=True)
               .str.replace(r'^\((.+)\)$', r'-\1', regex=True)
               .str.strip())
    return pd.to_numeric(s, errors='coerce')

NUM_COLS = ['impressions','clicks','conversions','spend','revenue','budget',
            'page_likes','landing_page_views','link_clicks','adds_to_cart',
            'checkouts_initiated','adds_of_payment_info','website_contacts',
            'messaging_conversations_started',
            'adds_to_cart_conversion_value_inr',
            'checkouts_initiated_conversion_value_inr',
            'adds_of_payment_info_conversion_value_inr',
            'row_count']

for col in NUM_COLS:
    if col in df.columns:
        b = df[col].isna().sum()
        df[col] = to_num(df[col])
        if col in ['impressions','clicks','conversions']:
            df.loc[df[col] < 0, col] = np.nan
        if col in ['spend','revenue','budget']:
            df.loc[df[col] < 0, col] = np.nan
        log(f"  {col}: {b} → {df[col].isna().sum()} nulls after coerce")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — MISSING VALUE IMPUTATION
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 9 — Impute missing values")

def gmed(df, col, gcols):
    if gcols:
        df[col] = df[col].fillna(df.groupby(gcols)[col].transform('median'))
    return df[col].fillna(df[col].median())

grp = [c for c in ['platform','channel'] if c in df.columns]

for col in ['spend','impressions','clicks']:
    if col in df.columns:
        m = df[col].isna().sum()
        if m:
            df[col] = gmed(df, col, grp)
            log(f"  {col}: {m} missing → group-median fill")

for col in ['conversions','revenue']:
    if col in df.columns:
        m = df[col].isna().sum()
        if m:
            df[col] = df[col].fillna(0)
            log(f"  {col}: {m} missing → filled 0")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 10 — OUTLIER FLAGGING
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 10 — Flag outliers (IQR×3)")
for col in ['spend','clicks','impressions']:
    if col in df.columns and df[col].notna().sum() > 4:
        q1, q3 = df[col].quantile([.25,.75])
        upper  = q3 + 3*(q3-q1)
        df[f'{col}_outlier_flag'] = (df[col] > upper).astype(int)
        log(f"  {col}: {df[f'{col}_outlier_flag'].sum()} outliers > {upper:.2f}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 11 — RECALCULATE METRICS FROM SOURCE COLUMNS
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 11 — Recalculate CTR / CPC / CPM / ROI / ROAS")
eps = 1e-9

impr = df['impressions'] if 'impressions' in df.columns else pd.Series(0, index=df.index)
clks = df['clicks']      if 'clicks'      in df.columns else pd.Series(0, index=df.index)
spnd = df['spend']       if 'spend'       in df.columns else pd.Series(0, index=df.index)
rev  = df['revenue']     if 'revenue'     in df.columns else pd.Series(0, index=df.index)
conv = df['conversions'] if 'conversions' in df.columns else pd.Series(0, index=df.index)

# Check and flag original CTR if it exists
if 'ctr' in df.columns:
    orig_ctr = to_num(df['ctr'])
    calc_ctr = clks / (impr + eps)
    df['ctr_wrong_flag'] = ((orig_ctr - calc_ctr).abs() > 0.001).astype(int)
    wrong = df['ctr_wrong_flag'].sum()
    log(f"  CTR: {wrong} rows had wrong original value")
else:
    df['ctr_wrong_flag'] = 0

df['ctr']  = (clks / (impr + eps)).round(6)
df['cpc']  = (spnd / (clks  + eps)).round(4)
df['cpm']  = (spnd / (impr  + eps) * 1000).round(4)
df['roi']  = ((rev - spnd) / (spnd + eps)).round(4)
df['roas'] = (rev  / (spnd  + eps)).round(4)
df['cpc_wrong_flag'] = 0
df['cpm_wrong_flag'] = 0
df['roi_wrong_flag'] = 0

log(f"  Avg CTR : {df['ctr'].mean():.4f}")
log(f"  Avg CPC : {df['cpc'].mean():.4f}")
log(f"  Avg ROAS: {df['roas'].mean():.2f}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 12 — ENFORCE FINAL DTYPES + ROUNDING
# ══════════════════════════════════════════════════════════════════════════════
log("STEP 12 — Enforce dtypes + round")
FINAL_INT   = ['impressions','clicks','conversions','page_likes',
               'landing_page_views','link_clicks','adds_to_cart',
               'checkouts_initiated']
FINAL_FLOAT = ['spend','budget','revenue','ctr','cpc','cpm','roi','roas']

for col in FINAL_INT:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').round(0)
for col in FINAL_FLOAT:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

df['spend']   = df['spend'].round(2)   if 'spend'   in df.columns else df.get('spend')
df['revenue'] = df['revenue'].round(2) if 'revenue' in df.columns else df.get('revenue')
df['ctr']     = df['ctr'].round(6)     if 'ctr'     in df.columns else df.get('ctr')
df['cpc']     = df['cpc'].round(4)     if 'cpc'     in df.columns else df.get('cpc')
df['roas']    = df['roas'].round(4)    if 'roas'    in df.columns else df.get('roas')

# ══════════════════════════════════════════════════════════════════════════════
# STEP 13 — DIAGNOSTICS
# ══════════════════════════════════════════════════════════════════════════════
log(f"\nSTEP 13 — Diagnostics ({df.shape[0]} rows × {df.shape[1]} cols)")
log(f"  {'Column':<40} {'dtype':<12} {'nulls':>7} {'non-null':>9}  sample")
log("  " + "-"*85)
for col in df.columns:
    nulls = int(df[col].isna().sum())
    nn    = int(df[col].notna().sum())
    samp  = str(df[col].dropna().iloc[0])[:30] if nn else '⚠ ALL NULL'
    log(f"  {col:<40} {str(df[col].dtype):<12} {nulls:>7} {nn:>9}  {samp}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 14 — WRITE TO SQLITE
# ══════════════════════════════════════════════════════════════════════════════
log(f"\nSTEP 14 — Write to SQLite: {DB_PATH}")

# Stringify datetime
if 'date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['date']):
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

def dtype_map(frame, int_cols, float_cols):
    m = {}
    for c in frame.columns:
        if c in int_cols or 'flag' in c: m[c] = T.Integer()
        elif c in float_cols or frame[c].dtype == float: m[c] = T.Float()
        else: m[c] = T.Text()
    return m

engine = create_engine(f'sqlite:///{DB_PATH}')

# ── Full campaigns table ───────────────────────────────────────────────────────
dm = dtype_map(df, FINAL_INT, FINAL_FLOAT)
with engine.connect() as cn:
    df.to_sql('campaigns', cn, if_exists='replace', index=False, dtype=dm)
log(f"  'campaigns' → {len(df)} rows")

# ── dim_date ──────────────────────────────────────────────────────────────────
if 'date' in df.columns:
    raw_dates = pd.to_datetime(df['date'].dropna().unique(), errors='coerce').dropna()
    raw_dates = pd.DatetimeIndex(sorted(raw_dates))
    dim_date  = pd.DataFrame({
        'date':        raw_dates.strftime('%Y-%m-%d'),
        'week':        raw_dates.isocalendar().week.astype(int).values,
        'month':       raw_dates.month.astype(int),
        'quarter':     raw_dates.quarter.astype(int),
        'year':        raw_dates.year.astype(int),
        'month_name':  raw_dates.strftime('%B'),
        'day_of_week': raw_dates.day_name(),
    })
    with engine.connect() as cn:
        dim_date.to_sql('dim_date', cn, if_exists='replace', index=False,
            dtype={'date':T.Text(),'week':T.Integer(),'month':T.Integer(),
                   'quarter':T.Integer(),'year':T.Integer(),
                   'month_name':T.Text(),'day_of_week':T.Text()})
    log(f"  'dim_date' → {len(dim_date)} rows  "
        f"({dim_date['date'].min()} → {dim_date['date'].max()})")

# ── dim_campaign ──────────────────────────────────────────────────────────────
dim_cols = [c for c in ['campaign_id','campaign_name','platform','channel',
            'region','country','brand_name','status','start_date','end_date',
            'budget','ad_group'] if c in df.columns]
dim_campaign = df[dim_cols].drop_duplicates(subset=['campaign_id'])
dim_campaign = dim_campaign[
    dim_campaign['campaign_id'].notna() &
    (dim_campaign['campaign_id'].astype(str).str.lower() != 'nan')
]
with engine.connect() as cn:
    dim_campaign.to_sql('dim_campaign', cn, if_exists='replace', index=False,
        dtype=dtype_map(dim_campaign, [], ['budget']))
log(f"  'dim_campaign' → {len(dim_campaign)} rows")
log(f"    Platforms : {sorted(dim_campaign['platform'].unique().tolist()) if 'platform' in dim_campaign.columns else 'N/A'}")

# ── fact_campaigns ────────────────────────────────────────────────────────────
fact_cols = [c for c in [
    'campaign_id','date',
    'impressions','clicks','conversions','spend','revenue',
    'ctr','cpc','cpm','roi','roas',
    'ctr_wrong_flag','cpc_wrong_flag','cpm_wrong_flag','roi_wrong_flag',
    'spend_outlier_flag','clicks_outlier_flag','impressions_outlier_flag',
] if c in df.columns]

fact = df[fact_cols].copy()
with engine.connect() as cn:
    fact.to_sql('fact_campaigns', cn, if_exists='replace', index=False,
        dtype=dtype_map(fact, FINAL_INT, FINAL_FLOAT))
log(f"  'fact_campaigns' → {len(fact)} rows")

# ── Drop old views + recreate ─────────────────────────────────────────────────
log("  Recreating views …")
conn = sqlite3.connect(DB_PATH)
for v in ['vw_ai_campaign_summary','vw_powerbi_performance',
          'vw_top_campaigns_roas','vw_country_performance',
          'vw_mom_spend','vw_platform_channel_breakdown']:
    conn.execute(f"DROP VIEW IF EXISTS [{v}]")
conn.commit()

VIEWS = {
'vw_ai_campaign_summary': """CREATE VIEW vw_ai_campaign_summary AS
SELECT d.year,d.month,d.month_name,d.quarter,
    c.platform,c.channel,c.region,c.country,c.brand_name,
    c.campaign_id,c.campaign_name,c.status,
    SUM(f.impressions) AS impressions, SUM(f.clicks) AS clicks,
    SUM(f.conversions) AS conversions, SUM(f.spend) AS spend,
    SUM(f.revenue) AS revenue,
    ROUND(SUM(f.clicks)*1.0/NULLIF(SUM(f.impressions),0),4) AS ctr,
    ROUND(SUM(f.spend)/NULLIF(SUM(f.clicks),0),2) AS cpc,
    ROUND(SUM(f.revenue)/NULLIF(SUM(f.spend),0),2) AS roas,
    ROUND((SUM(f.revenue)-SUM(f.spend))/NULLIF(SUM(f.spend),0),4) AS roi,
    ROUND(SUM(f.spend)/NULLIF(SUM(f.impressions),0)*1000,2) AS cpm,
    ROUND(SUM(f.conversions)*1.0/NULLIF(SUM(f.clicks),0),4) AS conversion_rate
FROM fact_campaigns f
JOIN dim_campaign c ON f.campaign_id=c.campaign_id
JOIN dim_date d ON f.date=d.date
GROUP BY d.year,d.month,d.month_name,d.quarter,
    c.platform,c.channel,c.region,c.country,
    c.brand_name,c.campaign_id,c.campaign_name,c.status""",

'vw_powerbi_performance': """CREATE VIEW vw_powerbi_performance AS
SELECT d.year,d.quarter,d.month,d.month_name,d.date,
    c.platform,c.channel,c.region,c.country,c.brand_name,
    c.campaign_id,c.campaign_name,c.status,
    SUM(f.impressions) AS total_impressions,
    SUM(f.clicks)      AS total_clicks,
    SUM(f.conversions) AS total_conversions,
    SUM(f.spend)       AS total_spend,
    SUM(f.revenue)     AS total_revenue,
    CASE WHEN SUM(f.impressions)>0 THEN ROUND(SUM(f.clicks)*1.0/SUM(f.impressions),6) ELSE 0 END AS ctr,
    CASE WHEN SUM(f.clicks)>0      THEN ROUND(SUM(f.spend)/SUM(f.clicks),4)           ELSE 0 END AS cpc,
    CASE WHEN SUM(f.impressions)>0 THEN ROUND(SUM(f.spend)/SUM(f.impressions)*1000,4) ELSE 0 END AS cpm,
    CASE WHEN SUM(f.spend)>0       THEN ROUND((SUM(f.revenue)-SUM(f.spend))/SUM(f.spend),4) ELSE 0 END AS roi,
    CASE WHEN SUM(f.spend)>0       THEN ROUND(SUM(f.revenue)/SUM(f.spend),4)          ELSE 0 END AS roas,
    CASE WHEN SUM(f.clicks)>0      THEN ROUND(SUM(f.conversions)*1.0/SUM(f.clicks),4) ELSE 0 END AS conversion_rate
FROM fact_campaigns f
JOIN dim_campaign c ON f.campaign_id=c.campaign_id
JOIN dim_date d ON f.date=d.date
GROUP BY d.year,d.quarter,d.month,d.month_name,d.date,
    c.platform,c.channel,c.region,c.country,
    c.brand_name,c.campaign_id,c.campaign_name,c.status""",

'vw_top_campaigns_roas': """CREATE VIEW vw_top_campaigns_roas AS
SELECT c.campaign_id,c.campaign_name,c.platform,c.channel,c.region,c.brand_name,
    SUM(f.spend) AS total_spend, SUM(f.revenue) AS total_revenue,
    SUM(f.conversions) AS total_conversions, SUM(f.clicks) AS total_clicks,
    ROUND(SUM(f.revenue)/NULLIF(SUM(f.spend),0),2) AS roas,
    ROUND(SUM(f.spend)/NULLIF(SUM(f.clicks),0),2) AS cpc,
    ROUND(SUM(f.clicks)*1.0/NULLIF(SUM(f.impressions),0),4) AS ctr
FROM fact_campaigns f
JOIN dim_campaign c ON f.campaign_id=c.campaign_id
GROUP BY c.campaign_id,c.campaign_name,c.platform,c.channel,c.region,c.brand_name
ORDER BY roas DESC""",

'vw_country_performance': """CREATE VIEW vw_country_performance AS
SELECT c.country,c.region,
    SUM(f.spend) AS total_spend, SUM(f.revenue) AS total_revenue,
    SUM(f.conversions) AS total_conversions, SUM(f.clicks) AS total_clicks,
    ROUND(SUM(f.revenue)/NULLIF(SUM(f.spend),0),2) AS roas,
    ROUND(SUM(f.spend)/NULLIF(SUM(f.clicks),0),2) AS cpc,
    ROUND(SUM(f.clicks)*1.0/NULLIF(SUM(f.impressions),0),4) AS ctr,
    ROUND((SUM(f.revenue)-SUM(f.spend))/NULLIF(SUM(f.spend),0),4) AS roi
FROM fact_campaigns f
JOIN dim_campaign c ON f.campaign_id=c.campaign_id
GROUP BY c.country,c.region ORDER BY total_spend DESC""",

'vw_mom_spend': """CREATE VIEW vw_mom_spend AS
SELECT d.year,d.month,d.month_name,
    SUM(f.spend) AS monthly_spend,
    LAG(SUM(f.spend)) OVER (ORDER BY d.year,d.month) AS prev_month_spend,
    ROUND((SUM(f.spend)-LAG(SUM(f.spend)) OVER (ORDER BY d.year,d.month))
        /NULLIF(LAG(SUM(f.spend)) OVER (ORDER BY d.year,d.month),0)*100,2) AS mom_pct_change
FROM fact_campaigns f
JOIN dim_date d ON f.date=d.date
GROUP BY d.year,d.month,d.month_name ORDER BY d.year,d.month""",

'vw_platform_channel_breakdown': """CREATE VIEW vw_platform_channel_breakdown AS
SELECT c.platform,c.channel,
    SUM(f.spend) AS total_spend, SUM(f.revenue) AS total_revenue,
    SUM(f.conversions) AS total_conversions, SUM(f.clicks) AS total_clicks,
    SUM(f.impressions) AS total_impressions,
    ROUND(SUM(f.revenue)/NULLIF(SUM(f.spend),0),2) AS roas,
    ROUND(SUM(f.spend)/NULLIF(SUM(f.clicks),0),2) AS cpc,
    ROUND(SUM(f.clicks)*1.0/NULLIF(SUM(f.impressions),0),4) AS ctr,
    ROUND(SUM(f.conversions)*1.0/NULLIF(SUM(f.clicks),0),4) AS conversion_rate
FROM fact_campaigns f
JOIN dim_campaign c ON f.campaign_id=c.campaign_id
GROUP BY c.platform,c.channel ORDER BY total_spend DESC""",
}

for name, sql in VIEWS.items():
    try:
        conn.execute(sql.strip())
        conn.commit()
        cnt = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        log(f"  ✅  {name} ({cnt} rows)")
    except sqlite3.Error as e:
        log(f"  ❌  {name}: {e}")

conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 15 — VERIFY DB
# ══════════════════════════════════════════════════════════════════════════════
log("\nSTEP 15 — Verify database")
chk  = sqlite3.connect(DB_PATH)
objs = chk.execute(
    "SELECT type,name FROM sqlite_master WHERE type IN ('table','view') ORDER BY type,name"
).fetchall()

tables = [(t,n) for t,n in objs if t=='table']
views  = [(t,n) for t,n in objs if t=='view']

log(f"\nTABLES ({len(tables)}):")
for _,name in tables:
    cnt   = chk.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
    info  = chk.execute(f"PRAGMA table_info([{name}])").fetchall()
    empty = [ci[1] for ci in info
             if chk.execute(f"SELECT COUNT(*) FROM [{name}] WHERE [{ci[1]}] IS NOT NULL"
                            ).fetchone()[0] == 0]
    status = f"⚠ EMPTY: {empty}" if empty else "✅ all populated"
    log(f"  {name:<30} ({cnt:>6} rows)  {status}")

log(f"\nVIEWS ({len(views)}):")
for _,name in views:
    try:
        cnt = chk.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        log(f"  ✅  {name:<35} ({cnt:>6} rows)")
    except Exception as e:
        log(f"  ❌  {name:<35}  {e}")
chk.close()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 16 — QUALITY REPORT
# ══════════════════════════════════════════════════════════════════════════════
log(f"\nSTEP 16 — Writing quality report: {REPORT_PATH}")
lines = [
    "# Data Quality Report — campaigns_raw.csv",
    f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    f"\n**Original rows:** {ORIGINAL_ROWS} | **Cleaned rows:** {len(df)} | "
    f"**Columns:** {len(df.columns)}",
    "\n---\n","## Issues Found & Fixed\n",
] + [f"{i+1}. {x}" for i,x in enumerate(issues)] + [
    "\n---\n","## Column Mapping Applied\n",
    "| Original CSV Column | Standard Name | Notes |","|---|---|---|",
    "| data_source_name | platform | Ad platform name |",
    "| campaign_effective_status | status | Active/Paused/etc |",
    "| country_funnel | country | Target country |",
    "| geo_location_segment | region | Geographic region |",
    "| fb_spent_funnel_inr / amount_spent_inr | spend | Ad spend in INR |",
    "| clicks_all | clicks | Total clicks |",
    "| purchases_conversion_value_inr | revenue | Purchase value |",
    "| purchases | conversions | Purchase count |",
    "| ad_set_name | ad_group | Ad set name |",
    "| campaign_name | campaign_id | Factorized to numeric ID |",
    "\n---\n","## Cleaning Strategy\n",
    "| Issue | Strategy | Rationale |","|---|---|---|",
    "| Non-standard column names | Renamed to standard | Consistent downstream queries |",
    "| Missing campaign_id | Created via factorize(campaign_name) | Unique int per campaign |",
    "| Currency symbols (₹) | Strip before coerce | Root cause of empty columns |",
    "| Missing spend/clicks | Group-median | Skewed distribution |",
    "| Missing conversions/revenue | Fill 0 | No event = 0 |",
    "| Wrong CTR/CPC/CPM | Recalculate from source | Source columns are truth |",
]
open(REPORT_PATH,'w',encoding='utf-8').write('\n'.join(lines))

print("\n" + "="*60)
print("✅  Campaign cleaning COMPLETE")
print(f"   DB     → {DB_PATH}")
print(f"   Report → {REPORT_PATH}")
print("="*60)