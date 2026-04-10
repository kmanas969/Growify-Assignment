# Data Quality Report — campaigns_raw.csv

Generated: 2026-04-09 20:08:54

**Original rows:** 10028 | **Cleaned rows:** 9924 | **Columns:** 27

---

## Issues Found & Fixed

1. STEP 1 — Load raw CSV
2.   10028 rows × 26 columns
3.   columns: ['Data Source name', 'Date', 'Campaign Name', 'Campaign Effective Status', 'Ad Set Name', 'Ad Name', 'Country Funnel', 'Geo Location Segment', 'FB Spent Funnel (INR)', 'Amount Spent (INR)', 'Clicks (all)', 'Impressions', 'Page Likes', 'Landing Page Views', 'Link Clicks', 'Adds to Cart', 'Checkouts Initiated', 'Adds of Payment Info', 'Purchases', 'Purchases Conversion Value (INR)', 'Website Contacts', 'Messaging Conversations Started', 'Adds to Cart Conversion Value (INR)', 'Checkouts Initiated Conversion Value (INR)', 'Adds of Payment Info Conversion Value (INR)', 'Row Count']
4. STEP 2 — Normalise column names
5.   → ['data_source_name', 'date', 'campaign_name', 'campaign_effective_status', 'ad_set_name', 'ad_name', 'country_funnel', 'geo_location_segment', 'fb_spent_funnel_inr', 'amount_spent_inr', 'clicks_all', 'impressions', 'page_likes', 'landing_page_views', 'link_clicks', 'adds_to_cart', 'checkouts_initiated', 'adds_of_payment_info', 'purchases', 'purchases_conversion_value_inr', 'website_contacts', 'messaging_conversations_started', 'adds_to_cart_conversion_value_inr', 'checkouts_initiated_conversion_value_inr', 'adds_of_payment_info_conversion_value_inr', 'row_count']
6. STEP 3 — Strip whitespace and replace null placeholders
7. STEP 4 — Remove duplicates
8.   exact dups removed: 104
9.   rows after dedup: 9924
10. STEP 5 — String normalisation
11.   string cols: ['data_source_name', 'campaign_name', 'campaign_effective_status', 'ad_set_name', 'ad_name', 'country_funnel', 'geo_location_segment']
12. STEP 6 — Date parsing
13.   date cols: ['date', 'messaging_conversations_started']
14.   date: 629 nulls before → 6104 after
15.   messaging_conversations_started: 657 nulls before → 9924 after
16. STEP 7 — Numeric coercion (stripping currency symbols)
17.   auto-detected numeric: fb_spent_funnel_inr
18.   auto-detected numeric: amount_spent_inr
19.   auto-detected numeric: clicks_all
20.   auto-detected numeric: page_likes
21.   auto-detected numeric: landing_page_views
22.   auto-detected numeric: adds_to_cart
23.   auto-detected numeric: checkouts_initiated
24.   auto-detected numeric: adds_of_payment_info
25.   auto-detected numeric: purchases
26.   auto-detected numeric: purchases_conversion_value_inr
27.   auto-detected numeric: website_contacts
28.   auto-detected numeric: adds_to_cart_conversion_value_inr
29.   auto-detected numeric: checkouts_initiated_conversion_value_inr
30.   auto-detected numeric: adds_of_payment_info_conversion_value_inr
31.   auto-detected numeric: row_count
32.   INT  impressions: 663 → 1092 nulls
33.   INT  link_clicks: 666 → 1028 nulls
34.   FLT  fb_spent_funnel_inr: 736 → 736 nulls
35.   FLT  amount_spent_inr: 660 → 660 nulls
36.   FLT  clicks_all: 658 → 658 nulls
37.   FLT  page_likes: 899 → 899 nulls
38.   FLT  landing_page_views: 660 → 660 nulls
39.   FLT  adds_to_cart: 647 → 647 nulls
40.   FLT  checkouts_initiated: 1031 → 1031 nulls
41.   FLT  adds_of_payment_info: 1065 → 1065 nulls
42.   FLT  purchases: 669 → 669 nulls
43.   FLT  purchases_conversion_value_inr: 663 → 663 nulls
44.   FLT  website_contacts: 2538 → 2538 nulls
45.   FLT  adds_to_cart_conversion_value_inr: 658 → 658 nulls
46.   FLT  checkouts_initiated_conversion_value_inr: 1608 → 1608 nulls
47.   FLT  adds_of_payment_info_conversion_value_inr: 1084 → 1084 nulls
48.   FLT  row_count: 671 → 671 nulls
49. STEP 8 — Impute missing values
50.   impressions: 1092 → group-median
51. STEP 9 — Flag outliers
52.   impressions: 1417 outliers > 6942.75
53. STEP 10 — Recalculate CTR / CPC / CPM / ROI / ROAS
54. STEP 11 — Enforce dtypes + round
55. 
STEP 12 — Diagnostics  (9924 rows × 27 cols)
56.   Column                           dtype          nulls  non-null  sample
57.   ----------------------------------------------------------------------
58.   data_source_name                 str                0      9924  Brand A
59.   date                             datetime64[us]    6104      3820  2026-02-01 00:00:00
60.   campaign_name                    str                0      9924  Unknown
61.   campaign_effective_status        str                0      9924  Paused
62.   ad_set_name                      str                0      9924  Tof | Lal 3-5% | Sales | Wom
63.   ad_name                          str                0      9924  Custom | Video 2 | Eoss | 15
64.   country_funnel                   str                0      9924  India
65.   geo_location_segment             str                0      9924  India
66.   fb_spent_funnel_inr              float64          736      9188  0.0
67.   amount_spent_inr                 float64          660      9264  0.0
68.   clicks_all                       float64          658      9266  0.0
69.   impressions                      float64            0      9924  448.0
70.   page_likes                       float64          899      9025  0.0
71.   landing_page_views               float64          660      9264  0.0
72.   link_clicks                      float64         1028      8896  -0.0
73.   adds_to_cart                     float64          647      9277  0.0
74.   checkouts_initiated              float64         1031      8893  -0.0
75.   adds_of_payment_info             float64         1065      8859  0.0
76.   purchases                        float64          669      9255  0.0
77.   purchases_conversion_value_inr   float64          663      9261  0.0
78.   website_contacts                 float64         2538      7386  0.0
79.   messaging_conversations_started  datetime64[s]    9924         0  ⚠ ALL NULL
80.   adds_to_cart_conversion_value_inr float64          658      9266  0.0
81.   checkouts_initiated_conversion_value_inr float64         1608      8316  0.0
82.   adds_of_payment_info_conversion_value_inr float64         1084      8840  0.0
83.   row_count                        float64          671      9253  1.0
84.   impressions_outlier_flag         int64              0      9924  0
85. 
STEP 13 — Write to SQLite: D:\Growify_assignment\Data\cleaned_campaigns.db
86.   'campaigns' → 9924 rows
87.   'dim_date' → 36 rows
88.   'fact_campaigns' → 9924 rows
89.   schema.sql partial (objects may exist): no such column: campaign_id
90. 
STEP 13 verify —
91.   campaigns (9924 rows) — ⚠ EMPTY: ['messaging_conversations_started']
92.   dim_campaign (0 rows) — ⚠ EMPTY: ['campaign_id', 'campaign_name', 'platform', 'channel', 'region', 'country', 'brand_name', 'status', 'start_date', 'end_date', 'budget', 'ad_group', 'campaign_type', 'objective']
93.   dim_date (36 rows) — ✅ all columns populated
94.   fact_campaigns (9924 rows) — ✅ all columns populated
95. 
STEP 14 — Write quality report: D:\Growify_assignment\Python\data_quality_report.md

---

## Cleaning Strategy

| Issue | Strategy | Rationale |
|---|---|---|
| Exact duplicates | Drop keep-first | Pipeline noise |
| Business-key dups | Drop keep-first | Same event twice |
| Mixed date formats | Multi-format fallback parser | Robust, no data loss |
| Inverted date ranges | Swap start↔end | Logical constraint |
| Currency symbols in numbers | Strip $£€ before coerce | Root cause of empty columns |
| Missing spend/impressions/clicks | Group-median by platform+channel | Skewed distribution |
| Missing conversions/revenue | Fill 0 | No event = 0 |
| Missing strings | Fill 'Unknown' | Preserve row |
| Negative volumes | Set NaN | Physically impossible |
| Wrong CTR/CPC/CPM/ROI | Recalculate + flag | Source columns are truth |
| Mixed-case strings | Title-case + strip | Consistent grouping |
| Outlier spend/clicks | Flag column keep row | May be real spend |
| SQLite dtype mismatch | Explicit SQLAlchemy dtype map | Power BI reads numbers correctly |