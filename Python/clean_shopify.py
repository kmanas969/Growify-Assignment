"""
clean_Shopify.py  —  Growify Digital Take-Home Assignment
==========================================================
Reads  : Raw_Shopify_Sales.csv
Writes : data/cleaned_campaigns.db  (appends Shopify tables alongside
         the existing campaign tables produced by clean_campaigns.py)

Tables created / replaced in the DB
────────────────────────────────────
  dim_date          – calendar dimension (merged with campaign dates)
  dim_product       – product dimension
  fact_shopify      – grain: one row per order-date-product
  vw_shopify_summary          – brand × month revenue summary
  vw_shopify_country_revenue  – country revenue breakdown
  vw_shopify_channel_mix      – sales channel breakdown

Power BI connection: same DSN  GrowifyCampaigns → data/cleaned_campaigns.db
"""

import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
RAW_CSV = Path("Raw_Shopify_Sales.csv")          # adjust path if needed
DB_PATH = Path("D:\Growify_assignment\Data\cleaned_campaigns.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

print("=" * 60)
print("  clean_Shopify.py  —  Shopify Sales ETL")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────
# STEP 1 — LOAD
# ─────────────────────────────────────────────────────────────────
print("\n[1/7]  Loading raw CSV …")
# Use the full path where the file is actually saved
df = pd.read_csv("D:/Growify_assignment/Data/Raw_Shopify_Sales.csv", dtype=str)       # load everything as str first
print(f"       Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")


# ─────────────────────────────────────────────────────────────────
# STEP 2 — NORMALISE STRING NOISE
# ─────────────────────────────────────────────────────────────────
print("[2/7]  Normalising strings …")

def clean_str(s: pd.Series) -> pd.Series:
    """Strip whitespace, title-case, map sentinel NANs → NaN."""
    s = s.str.strip()
    s = s.str.replace(r"(?i)^\s*nan\s*$", "", regex=True)  # 'NAN',' nan' → ''
    s = s.replace("", np.nan)
    return s

# Apply to all object columns
for col in df.select_dtypes("object").columns:
    df[col] = clean_str(df[col])

# Normalise free-text lookup columns
def normalise_label(s: pd.Series) -> pd.Series:
    return s.str.strip().str.title()

df["Data Source name"]   = normalise_label(df["Data Source name"])
df["Country Funnel"]     = normalise_label(df["Country Funnel"])
df["Billing Country"]    = normalise_label(df["Billing Country"])
df["Shipping Country"]   = normalise_label(df["Shipping Country"])
df["Customer Sale Type"] = normalise_label(df["Customer Sale Type"])
df["Sales Channel"]      = normalise_label(df["Sales Channel"])
df["Geo Location Segment"] = normalise_label(df["Geo Location Segment"])

# Standardise brand names → Brand A / Brand B
def map_brand(s: pd.Series) -> pd.Series:
    return s.str.upper().str.strip().map(
        {"BRAND A": "Brand A", "BRAND B": "Brand B"}
    )

df["brand_name"] = map_brand(df["Data Source name"])

# Standardise customer type
df["Customer Sale Type"] = df["Customer Sale Type"].replace(
    {"First-Time": "First-time", "Returning": "Returning"}
)


# ─────────────────────────────────────────────────────────────────
# STEP 3 — PARSE / FIX DATES
# ─────────────────────────────────────────────────────────────────
print("[3/7]  Parsing dates …")

def parse_date_col(s: pd.Series) -> pd.Series:
    """Try dd-mm-yyyy, then ISO 8601, return datetime."""
    out = pd.to_datetime(s, format="%d-%m-%Y", errors="coerce")
    mask_fail = out.isna() & s.notna()
    # fallback: ISO-style timestamps (Order Created At etc.)
    out[mask_fail] = pd.to_datetime(
        s[mask_fail].str.lower().str.strip(), errors="coerce", utc=True
    ).dt.tz_localize(None)
    return out

df["date"]               = parse_date_col(df["Date"])
df["transaction_ts"]     = pd.to_datetime(df["Transaction Timestamp"].str.lower().str.strip(),
                                           errors="coerce", utc=True).dt.tz_localize(None)
df["order_created_at"]   = pd.to_datetime(df["Order Created At"].str.lower().str.strip(),
                                           errors="coerce", utc=True).dt.tz_localize(None)
df["order_updated_at"]   = pd.to_datetime(df["Order Updated At"].str.lower().str.strip(),
                                           errors="coerce", utc=True).dt.tz_localize(None)

# Derive date from timestamp if Date column is null
mask_no_date = df["date"].isna() & df["order_created_at"].notna()
df.loc[mask_no_date, "date"] = df.loc[mask_no_date, "order_created_at"].dt.normalize()

print(f"       Date range: {df['date'].min().date()} → {df['date'].max().date()}")


# ─────────────────────────────────────────────────────────────────
# STEP 4 — COERCE NUMERIC COLUMNS
# ─────────────────────────────────────────────────────────────────
print("[4/7]  Coercing numeric columns …")

NUM_COLS = {
    "Order ID":                   "order_id",
    "Product ID":                 "product_id",
    "Gross Sales (INR)":          "gross_sales_inr",
    "Net Sales (INR)":            "net_sales_inr",
    "Total Sales (INR)":          "total_sales_inr",
    "Orders":                     "orders",
    "Returns (INR)":              "returns_inr",
    "Return Rate":                "return_rate",
    "Items Sold":                 "items_sold",
    "Items Returned":             "items_returned",
    "Average Order Value (INR)":  "avg_order_value_inr",
    "New Customer Orders":        "new_customer_orders",
    "Returning Customer Orders":  "returning_customer_orders",
    "Average Items Per Order":    "avg_items_per_order",
    "Discounts (INR)":            "discounts_inr",
    "Customer ID":                "customer_id",
    "Row Count":                  "row_count",
}

for raw_col, clean_col in NUM_COLS.items():
    df[clean_col] = pd.to_numeric(df[raw_col], errors="coerce")

# Floor negative sales/returns to 0 (data entry artefacts)
for col in ["gross_sales_inr", "net_sales_inr", "total_sales_inr", "returns_inr"]:
    neg_count = (df[col] < 0).sum()
    if neg_count:
        print(f"       ⚠  {neg_count} negative values clamped to 0 in {col}")
    df[col] = df[col].clip(lower=0)

# Fill implicit zeros
for col in ["gross_sales_inr", "net_sales_inr", "total_sales_inr",
            "orders", "returns_inr", "items_sold", "discounts_inr"]:
    df[col] = df[col].fillna(0)


# ─────────────────────────────────────────────────────────────────
# STEP 5 — DROP UNUSABLE ROWS
# ─────────────────────────────────────────────────────────────────
print("[5/7]  Dropping unusable rows …")
before = len(df)

# Must have a date and a brand
df = df.dropna(subset=["date", "brand_name"])

# Remove exact duplicates (same order_id + date + product_id)
df = df.drop_duplicates(
    subset=["order_id", "date", "product_id"], keep="first"
)

print(f"       Dropped {before - len(df):,} rows  →  {len(df):,} clean rows remain")


# ─────────────────────────────────────────────────────────────────
# STEP 6 — BUILD DIMENSION & FACT TABLES
# ─────────────────────────────────────────────────────────────────
print("[6/7]  Building dimension and fact tables …")

# ── dim_date ──────────────────────────────────────────────────────
all_dates = df["date"].dt.normalize().dropna().unique()
dim_date = pd.DataFrame({"date": pd.to_datetime(sorted(all_dates))})
dim_date["date_str"]    = dim_date["date"].dt.strftime("%Y-%m-%d")
dim_date["year"]        = dim_date["date"].dt.year
dim_date["quarter"]     = dim_date["date"].dt.quarter
dim_date["month"]       = dim_date["date"].dt.month
dim_date["month_name"]  = dim_date["date"].dt.strftime("%b")
dim_date["week"]        = dim_date["date"].dt.isocalendar().week.astype(int)
dim_date["day_of_week"] = dim_date["date"].dt.day_name()
dim_date["is_weekend"]  = dim_date["date"].dt.dayofweek >= 5
dim_date["date"]        = dim_date["date_str"]   # store as text for SQLite

# ── dim_product ───────────────────────────────────────────────────
dim_product = (
    df[["product_id", "Product Title", "Product Type", "Product Tags", "Variant Title"]]
    .dropna(subset=["product_id"])
    .rename(columns={
        "Product Title": "product_title",
        "Product Type":  "product_type",
        "Product Tags":  "product_tags",
        "Variant Title": "variant_title",
    })
    .drop_duplicates(subset=["product_id"])
    .reset_index(drop=True)
)

# ── fact_shopify ───────────────────────────────────────────────────
FACT_COLS = [
    "date", "brand_name", "order_id", "product_id",
    "Country Funnel", "Geo Location Segment",
    "Billing Country", "Billing Province", "Billing City",
    "Shipping Country", "Sales Channel", "Customer Sale Type", "customer_id",
    "gross_sales_inr", "net_sales_inr", "total_sales_inr",
    "orders", "returns_inr", "return_rate",
    "items_sold", "items_returned",
    "avg_order_value_inr", "avg_items_per_order",
    "new_customer_orders", "returning_customer_orders",
    "discounts_inr",
    "transaction_ts", "order_created_at",
]

fact = (
    df[FACT_COLS]
    .rename(columns={
        "Country Funnel":    "country",
        "Geo Location Segment": "region",
        "Billing Country":   "billing_country",
        "Billing Province":  "billing_province",
        "Billing City":      "billing_city",
        "Shipping Country":  "shipping_country",
        "Sales Channel":     "sales_channel",
        "Customer Sale Type":"customer_type",
    })
    .copy()
)

# Store timestamps as ISO strings
for ts_col in ["transaction_ts", "order_created_at"]:
    fact[ts_col] = fact[ts_col].astype(str).replace("NaT", np.nan)

fact["date"] = pd.to_datetime(fact["date"]).dt.strftime("%Y-%m-%d")

print(f"       fact_shopify:  {len(fact):,} rows")
print(f"       dim_date:      {len(dim_date):,} rows")
print(f"       dim_product:   {len(dim_product):,} rows")


# ── Aggregated views (materialised as tables for Power BI) ────────

# vw_shopify_summary — brand × year-month
fact["_date"] = pd.to_datetime(fact["date"])
summary = (
    fact.groupby(["brand_name",
                  fact["_date"].dt.year.rename("year"),
                  fact["_date"].dt.month.rename("month"),
                  fact["_date"].dt.strftime("%b").rename("month_name")])
    .agg(
        gross_sales_inr   = ("gross_sales_inr",  "sum"),
        net_sales_inr     = ("net_sales_inr",     "sum"),
        total_sales_inr   = ("total_sales_inr",   "sum"),
        total_orders      = ("orders",            "sum"),
        total_returns_inr = ("returns_inr",       "sum"),
        total_discounts   = ("discounts_inr",     "sum"),
        total_items_sold  = ("items_sold",        "sum"),
    )
    .reset_index()
)
summary["net_revenue_inr"] = summary["total_sales_inr"] - summary["total_returns_inr"]

# vw_shopify_country_revenue
country_rev = (
    fact.groupby(["country", "brand_name"])
    .agg(
        total_sales_inr   = ("total_sales_inr",  "sum"),
        total_orders      = ("orders",           "sum"),
        total_returns_inr = ("returns_inr",      "sum"),
        total_items_sold  = ("items_sold",       "sum"),
    )
    .reset_index()
)
country_rev["avg_order_value"] = np.where(
    country_rev["total_orders"] > 0,
    country_rev["total_sales_inr"] / country_rev["total_orders"],
    0,
)

# vw_shopify_channel_mix
channel_mix = (
    fact.groupby(["sales_channel", "brand_name"])
    .agg(
        total_sales_inr   = ("total_sales_inr",  "sum"),
        total_orders      = ("orders",           "sum"),
        total_discounts   = ("discounts_inr",    "sum"),
        new_customers     = ("new_customer_orders","sum"),
        returning_customers = ("returning_customer_orders","sum"),
    )
    .reset_index()
)

# Remove the helper column before writing
fact = fact.drop(columns=["_date"])


# ─────────────────────────────────────────────────────────────────
# STEP 7 — WRITE TO SQLITE
# ─────────────────────────────────────────────────────────────────
print("[7/7]  Writing to SQLite …")
print(f"       DB path: {DB_PATH.resolve()}")

con = sqlite3.connect(DB_PATH)

# dim_date: merge with existing rows if table already exists
try:
    existing_dates = pd.read_sql("SELECT date FROM dim_date", con)
    dim_date = dim_date[~dim_date["date"].isin(existing_dates["date"])]
    dim_date.to_sql("dim_date", con, if_exists="append", index=False)
    print(f"       dim_date: appended {len(dim_date):,} new date rows")
except Exception:
    dim_date.to_sql("dim_date", con, if_exists="replace", index=False)
    print(f"       dim_date: created with {len(dim_date):,} rows")

# All other tables — replace
TABLES = {
    "dim_product":               dim_product,
    "fact_shopify":              fact,
    "vw_shopify_summary":        summary,
    "vw_shopify_country_revenue": country_rev,
    "vw_shopify_channel_mix":    channel_mix,
}

for tbl, data in TABLES.items():
    data.to_sql(tbl, con, if_exists="replace", index=False)
    print(f"       {tbl:<35} {len(data):>6,} rows  ✓")

con.close()

print()
print("✅  Done!  All Shopify tables written to:", DB_PATH.resolve())
print()
print("Power BI — add these objects in the Navigator after connecting:")
print("  ✅  dim_product")
print("  ✅  fact_shopify")
print("  ✅  vw_shopify_summary")
print("  ✅  vw_shopify_country_revenue")
print("  ✅  vw_shopify_channel_mix")
print()
print("Suggested relationships in Model view:")
print("  dim_date.date            ──1:M──  fact_shopify.date")
print("  dim_product.product_id   ──1:M──  fact_shopify.product_id")