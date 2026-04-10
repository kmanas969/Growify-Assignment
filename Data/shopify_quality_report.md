# Data Quality Report — Shopify Raw Data

Generated: 2026-04-09 20:26:36

**Original rows:** 5680 | **Cleaned rows:** 2173 | **Columns:** 38

---

## Issues Found & Fixed

1. STEP 1 — Load raw Shopify CSV
2.   5680 rows × 38 columns
3.   columns: ['Data Source name', 'Date', 'Currency', 'Sales Channel', 'Transaction Timestamp', 'Order Created At', 'Order Updated At', 'Order ID', 'Order Name', 'Country Funnel', 'Geo Location Segment', 'Billing Country', 'Billing Province', 'Billing City', 'Order Tags', 'Product ID', 'Product Title', 'Product Tags', 'Product Type', 'Variant Title', 'Gross Sales (INR)', 'Net Sales (INR)', 'Total Sales (INR)', 'Orders', 'Returns (INR)', 'Return Rate', 'Items Sold', 'Items Returned', 'Average Order Value (INR)', 'New Customer Orders', 'Returning Customer Orders', 'Average Items Per Order', 'Discounts (INR)', 'Row Count', 'SKU', 'Customer Sale Type', 'Customer ID', 'Shipping Country']
4. STEP 2 — Normalise column names
5.   → ['data_source_name', 'date', 'currency', 'sales_channel', 'transaction_timestamp', 'order_created_at', 'order_updated_at', 'order_id', 'order_name', 'country_funnel', 'geo_location_segment', 'billing_country', 'billing_province', 'billing_city', 'order_tags', 'product_id', 'product_title', 'product_tags', 'product_type', 'variant_title', 'gross_sales_inr', 'net_sales_inr', 'total_sales_inr', 'orders', 'returns_inr', 'return_rate', 'items_sold', 'items_returned', 'average_order_value_inr', 'new_customer_orders', 'returning_customer_orders', 'average_items_per_order', 'discounts_inr', 'row_count', 'sku', 'customer_sale_type', 'customer_id', 'shipping_country']
6. STEP 3 — Strip whitespace and null placeholders
7. STEP 4 — Remove duplicates
8.   exact dups: 16
9.   duplicate order_id: 3491
10.   rows after dedup: 2173
11. STEP 5 — String normalisation
12.   string cols: ['data_source_name', 'currency', 'sales_channel', 'order_name', 'country_funnel', 'billing_country', 'billing_province', 'billing_city', 'product_title', 'product_type', 'variant_title', 'shipping_country']
13. STEP 6 — Date parsing
14.   date cols: ['data_source_name', 'date', 'transaction_timestamp', 'order_created_at', 'order_updated_at', 'geo_location_segment', 'return_rate']
15.   data_source_name: 0 → 2173 nulls
16.   date: 160 → 1325 nulls
17.   transaction_timestamp: 183 → 298 nulls
18.   order_created_at: 160 → 276 nulls
19.   order_updated_at: 168 → 294 nulls
20.   geo_location_segment: 166 → 2173 nulls
21.   return_rate: 1457 → 2173 nulls
22. STEP 7 — Numeric coercion (stripping currency symbols)
23.   auto-detected numeric: order_id
24.   auto-detected numeric: product_id
25.   auto-detected numeric: gross_sales_inr
26.   auto-detected numeric: net_sales_inr
27.   auto-detected numeric: total_sales_inr
28.   auto-detected numeric: orders
29.   auto-detected numeric: returns_inr
30.   auto-detected numeric: items_sold
31.   auto-detected numeric: items_returned
32.   auto-detected numeric: average_order_value_inr
33.   auto-detected numeric: new_customer_orders
34.   auto-detected numeric: returning_customer_orders
35.   auto-detected numeric: average_items_per_order
36.   auto-detected numeric: discounts_inr
37.   auto-detected numeric: row_count
38.   auto-detected numeric: customer_id
39.   FLT  order_id: 1 → 1 nulls
40.   FLT  product_id: 1502 → 1502 nulls
41.   FLT  gross_sales_inr: 175 → 175 nulls
42.   FLT  net_sales_inr: 163 → 163 nulls
43.   FLT  total_sales_inr: 178 → 178 nulls
44.   FLT  orders: 180 → 180 nulls
45.   FLT  returns_inr: 175 → 175 nulls
46.   FLT  items_sold: 238 → 238 nulls
47.   FLT  items_returned: 2067 → 2067 nulls
48.   FLT  average_order_value_inr: 1656 → 1656 nulls
49.   FLT  new_customer_orders: 706 → 706 nulls
50.   FLT  returning_customer_orders: 1573 → 1573 nulls
51.   FLT  average_items_per_order: 1658 → 1658 nulls
52.   FLT  discounts_inr: 172 → 172 nulls
53.   FLT  row_count: 163 → 163 nulls
54.   FLT  customer_id: 174 → 174 nulls
55. STEP 8 — Impute missing values
56. STEP 9 — Flag outliers
57. STEP 10 — Derived metrics
58. STEP 11 — Enforce dtypes + round
59. 
STEP 12 — Diagnostics  (2173 rows × 38 cols)
60.   Column                              dtype          nulls  non-null  sample
61.   ---------------------------------------------------------------------------
62.   data_source_name                    datetime64[s]    2173         0  ⚠ ALL NULL
63.   date                                datetime64[us]    1325       848  2026-08-01 00:00:00
64.   currency                            str                0      2173  Inr
65.   sales_channel                       str                0      2173  Online Store
66.   transaction_timestamp               datetime64[us, UTC]     298      1875  2026-01-08 11:39:42+00:00
67.   order_created_at                    datetime64[us, UTC]     276      1897  2026-01-08 11:39:43+00:00
68.   order_updated_at                    datetime64[us, UTC]     294      1879  2026-01-08 11:39:44+00:00
69.   order_id                            float64            1      2172  -6318230000000.0
70.   order_name                          str                0      2173  Unknown
71.   country_funnel                      str                0      2173  United Arab Emirates
72.   geo_location_segment                datetime64[s]    2173         0  ⚠ ALL NULL
73.   billing_country                     str                0      2173  Unknown
74.   billing_province                    str                0      2173  Dubai
75.   billing_city                        str                0      2173  Dubai
76.   order_tags                          str              724      1449  NAN
77.   product_id                          float64         1502       671  8725100000000.0
78.   product_title                       str                0      2173  Unknown
79.   product_tags                        str             1373       800   nan
80.   product_type                        str                0      2173  Unknown
81.   variant_title                       str                0      2173  Unknown
82.   gross_sales_inr                     float64          175      1998  0.0
83.   net_sales_inr                       float64          163      2010  0.0
84.   total_sales_inr                     float64          178      1995  0.0
85.   orders                              float64          180      1993  0.0
86.   returns_inr                         float64          175      1998  0.0
87.   return_rate                         datetime64[s]    2173         0  ⚠ ALL NULL
88.   items_sold                          float64          238      1935  0.0
89.   items_returned                      float64         2067       106  0.0
90.   average_order_value_inr             float64         1656       517  3940.975346
91.   new_customer_orders                 float64          706      1467  0.0
92.   returning_customer_orders           float64         1573       600  0.0
93.   average_items_per_order             float64         1658       515  1.0
94.   discounts_inr                       float64          172      2001  0.0
95.   row_count                           float64          163      2010  1.0
96.   sku                                 str             1625       548  CHERIEKAFTAN-S & EASYIV-S
97.   customer_sale_type                  str              154      2019  First-time
98.   customer_id                         float64          174      1999  9052170000000.0
99.   shipping_country                    str                0      2173  Unknown
100. 
STEP 13 — Write to SQLite: D:\Growify_assignment\Data\cleaned_shopify.db
101.   'shopify_orders' → 2173 rows
102.   'dim_date' → 36 rows (from 'date')
103.   'fact_sales' → 2173 rows, 4 columns
104. 
STEP 13 verify —
105.   dim_date (36 rows) — ✅ all populated
106.   fact_sales (2173 rows) — ✅ all populated
107.   shopify_orders (2173 rows) — ⚠ EMPTY: ['data_source_name', 'geo_location_segment', 'return_rate']
108. 
STEP 14 — Write report: D:\Growify_assignment\Python\shopify_quality_report.md

---

## Cleaning Strategy

| Issue | Strategy | Rationale |
|---|---|---|
| Exact duplicates | Drop keep-first | Pipeline noise |
| Duplicate order_id | Drop keep-first | Same order twice |
| Mixed/timezone date formats | Multi-format + utc=True | Shopify ISO 8601 exports |
| Currency symbols | Strip $£€ before coerce | Root cause of empty columns |
| Missing prices | Group-median by product_type | Category-level pattern |
| Missing quantity | Fill 1 | Minimum plausible |
| Missing tax/discount/refund | Fill 0 | No record = no amount |
| Negative prices | Set NaN | Physically impossible |
| Outliers | Flag column, keep row | May be bulk orders |
| SQLite dtype mismatch | Explicit SQLAlchemy dtype map | Power BI reads correctly |

---

## Derived Columns

| Column | Formula |
|---|---|
| net_revenue | revenue − refund_amount |
| aov | total_price / quantity |
| gross_margin | (revenue − cogs) / revenue |

---

## Tables Written

| Table | Description |
|---|---|
| shopify_orders | Full cleaned dataset |
| dim_date | Date dimension |
| fact_sales | Fact table for star schema |