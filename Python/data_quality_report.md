# Data Quality Report — campaigns_raw.csv

Generated: 2026-04-10 11:31:05

**Original rows:** 10028 | **Cleaned rows:** 3602 | **Columns:** 42

---

## Issues Found & Fixed

1. STEP 1 — Load raw CSV
2.   10028 rows × 26 columns
3.   Raw columns: ['Data Source name', 'Date', 'Campaign Name', 'Campaign Effective Status', 'Ad Set Name', 'Ad Name', 'Country Funnel', 'Geo Location Segment', 'FB Spent Funnel (INR)', 'Amount Spent (INR)', 'Clicks (all)', 'Impressions', 'Page Likes', 'Landing Page Views', 'Link Clicks', 'Adds to Cart', 'Checkouts Initiated', 'Adds of Payment Info', 'Purchases', 'Purchases Conversion Value (INR)', 'Website Contacts', 'Messaging Conversations Started', 'Adds to Cart Conversion Value (INR)', 'Checkouts Initiated Conversion Value (INR)', 'Adds of Payment Info Conversion Value (INR)', 'Row Count']
4. STEP 2 — Normalise column names
5.   Normalised: ['data_source_name', 'date', 'campaign_name', 'campaign_effective_status', 'ad_set_name', 'ad_name', 'country_funnel', 'geo_location_segment', 'fb_spent_funnel_inr', 'amount_spent_inr', 'clicks_all', 'impressions', 'page_likes', 'landing_page_views', 'link_clicks', 'adds_to_cart', 'checkouts_initiated', 'adds_of_payment_info', 'purchases', 'purchases_conversion_value_inr', 'website_contacts', 'messaging_conversations_started', 'adds_to_cart_conversion_value_inr', 'checkouts_initiated_conversion_value_inr', 'adds_of_payment_info_conversion_value_inr', 'row_count']
6. STEP 3 — Strip whitespace and null placeholders
7. STEP 4 — Remove duplicates
8.   Exact duplicates removed: 104
9.   Business-key duplicates removed (on ['campaign_name', 'date', 'data_source_name']): 6322
10.   Rows after dedup: 3602
11. STEP 5 — Rename columns to standard names
12.   campaign_id created from campaign_name (factorize)
13.   data_source_name → platform
14.   campaign_effective_status → status
15.   country_funnel → country
16.   geo_location_segment → region
17.   ad_set_name → ad_group
18.   clicks_all → clicks
19.   fb_spent_funnel_inr → spend
20.   purchases_conversion_value_inr → revenue
21.   purchases → conversions
22.   Columns after rename: ['platform', 'date', 'campaign_name', 'status', 'ad_group', 'ad_name', 'country', 'region', 'spend', 'amount_spent_inr', 'clicks', 'impressions', 'page_likes', 'landing_page_views', 'link_clicks', 'adds_to_cart', 'checkouts_initiated', 'adds_of_payment_info', 'conversions', 'revenue', 'website_contacts', 'messaging_conversations_started', 'adds_to_cart_conversion_value_inr', 'checkouts_initiated_conversion_value_inr', 'adds_of_payment_info_conversion_value_inr', 'row_count', 'campaign_id']
23.   Added missing column 'channel' with default 'Paid Social'
24.   Added missing column 'brand_name' with default 'Unknown'
25.   Added missing column 'budget' with default 'nan'
26. STEP 6 — String normalisation
27. STEP 7 — Date parsing
28.   date: 103 nulls before → 127 after
29. STEP 8 — Numeric coercion
30.   impressions: 229 → 375 nulls after coerce
31.   clicks: 211 → 331 nulls after coerce
32.   conversions: 221 → 231 nulls after coerce
33.   spend: 273 → 407 nulls after coerce
34.   revenue: 236 → 247 nulls after coerce
35.   budget: 3602 → 3602 nulls after coerce
36.   page_likes: 331 → 331 nulls after coerce
37.   landing_page_views: 241 → 241 nulls after coerce
38.   link_clicks: 229 → 229 nulls after coerce
39.   adds_to_cart: 241 → 241 nulls after coerce
40.   checkouts_initiated: 417 → 417 nulls after coerce
41.   adds_of_payment_info: 463 → 463 nulls after coerce
42.   website_contacts: 1149 → 1149 nulls after coerce
43.   messaging_conversations_started: 245 → 245 nulls after coerce
44.   adds_to_cart_conversion_value_inr: 237 → 237 nulls after coerce
45.   checkouts_initiated_conversion_value_inr: 651 → 651 nulls after coerce
46.   adds_of_payment_info_conversion_value_inr: 454 → 454 nulls after coerce
47.   row_count: 239 → 239 nulls after coerce
48. STEP 9 — Impute missing values
49.   spend: 407 missing → group-median fill
50.   impressions: 375 missing → group-median fill
51.   clicks: 331 missing → group-median fill
52.   conversions: 231 missing → filled 0
53.   revenue: 247 missing → filled 0
54. STEP 10 — Flag outliers (IQR×3)
55.   spend: 326 outliers > 3514.16
56.   clicks: 390 outliers > 277.00
57.   impressions: 467 outliers > 5671.00
58. STEP 11 — Recalculate CTR / CPC / CPM / ROI / ROAS
59.   Avg CTR : 135202666.4758
60.   Avg CPC : 29980724725.9583
61.   Avg ROAS: 3928473070572.62
62. STEP 12 — Enforce dtypes + round
63. 
STEP 13 — Diagnostics (3602 rows × 42 cols)
64.   Column                                   dtype          nulls  non-null  sample
65.   -------------------------------------------------------------------------------------
66.   platform                                 str                0      3602  Brand A
67.   date                                     datetime64[us]     127      3475  2026-02-01 00:00:00
68.   campaign_name                            str                0      3602  Unknown
69.   status                                   str                0      3602  Paused
70.   ad_group                                 str                0      3602  Tof | Lal 3-5% | Sales | Women
71.   ad_name                                  str              222      3380  Custom | Video 2 | EOSS | 15th
72.   country                                  str                0      3602  India
73.   region                                   str                0      3602  India
74.   spend                                    float64            0      3602  0.0
75.   amount_spent_inr                         str              229      3373  0.0
76.   clicks                                   float64            0      3602  0.0
77.   impressions                              float64            0      3602  781.0
78.   page_likes                               float64          331      3271  0.0
79.   landing_page_views                       float64          241      3361  0.0
80.   link_clicks                              float64          229      3373  -0.0
81.   adds_to_cart                             float64          241      3361  0.0
82.   checkouts_initiated                      float64          417      3185  -0.0
83.   adds_of_payment_info                     float64          463      3139  0.0
84.   conversions                              float64            0      3602  0.0
85.   revenue                                  float64            0      3602  0.0
86.   website_contacts                         float64         1149      2453  0.0
87.   messaging_conversations_started          float64          245      3357  0.0
88.   adds_to_cart_conversion_value_inr        float64          237      3365  0.0
89.   checkouts_initiated_conversion_value_inr float64          651      2951  0.0
90.   adds_of_payment_info_conversion_value_inr float64          454      3148  0.0
91.   row_count                                float64          239      3363  1.0
92.   campaign_id                              str                0      3602  0
93.   channel                                  str                0      3602  Paid Social
94.   brand_name                               str                0      3602  Unknown
95.   budget                                   float64         3602         0  ⚠ ALL NULL
96.   spend_outlier_flag                       int64              0      3602  0
97.   clicks_outlier_flag                      int64              0      3602  0
98.   impressions_outlier_flag                 int64              0      3602  0
99.   ctr_wrong_flag                           int64              0      3602  0
100.   ctr                                      float64            0      3602  0.0
101.   cpc                                      float64            0      3602  0.0
102.   cpm                                      float64            0      3602  0.0
103.   roi                                      float64            0      3602  0.0
104.   roas                                     float64            0      3602  0.0
105.   cpc_wrong_flag                           int64              0      3602  0
106.   cpm_wrong_flag                           int64              0      3602  0
107.   roi_wrong_flag                           int64              0      3602  0
108. 
STEP 14 — Write to SQLite: D:\Growify_assignment\Data\cleaned_campaigns.db
109.   'campaigns' → 3602 rows
110.   'dim_date' → 87 rows  (2026-01-01 → 2026-12-03)
111.   'dim_campaign' → 114 rows
112.     Platforms : ['Brand A', 'Brand B', 'Unknown']
113.   'fact_campaigns' → 3602 rows
114.   Recreating views …
115.   ✅  vw_ai_campaign_summary (608 rows)
116.   ✅  vw_powerbi_performance (2073 rows)
117.   ✅  vw_top_campaigns_roas (114 rows)
118.   ✅  vw_country_performance (14 rows)
119.   ✅  vw_mom_spend (12 rows)
120.   ✅  vw_platform_channel_breakdown (3 rows)
121. 
STEP 15 — Verify database
122. 
TABLES (9):
123.   campaigns                      (  3602 rows)  ⚠ EMPTY: ['budget']
124.   dim_campaign                   (   114 rows)  ⚠ EMPTY: ['budget']
125.   dim_date                       (    87 rows)  ✅ all populated
126.   dim_product                    (   317 rows)  ✅ all populated
127.   fact_campaigns                 (  3602 rows)  ✅ all populated
128.   fact_shopify                   (  3849 rows)  ✅ all populated
129.   vw_shopify_channel_mix         (     6 rows)  ✅ all populated
130.   vw_shopify_country_revenue     (    32 rows)  ✅ all populated
131.   vw_shopify_summary             (     7 rows)  ✅ all populated
132. 
VIEWS (6):
133.   ✅  vw_ai_campaign_summary              (   608 rows)
134.   ✅  vw_country_performance              (    14 rows)
135.   ✅  vw_mom_spend                        (    12 rows)
136.   ✅  vw_platform_channel_breakdown       (     3 rows)
137.   ✅  vw_powerbi_performance              (  2073 rows)
138.   ✅  vw_top_campaigns_roas               (   114 rows)
139. 
STEP 16 — Writing quality report: D:\Growify_assignment\python\data_quality_report.md

---

## Column Mapping Applied

| Original CSV Column | Standard Name | Notes |
|---|---|---|
| data_source_name | platform | Ad platform name |
| campaign_effective_status | status | Active/Paused/etc |
| country_funnel | country | Target country |
| geo_location_segment | region | Geographic region |
| fb_spent_funnel_inr / amount_spent_inr | spend | Ad spend in INR |
| clicks_all | clicks | Total clicks |
| purchases_conversion_value_inr | revenue | Purchase value |
| purchases | conversions | Purchase count |
| ad_set_name | ad_group | Ad set name |
| campaign_name | campaign_id | Factorized to numeric ID |

---

## Cleaning Strategy

| Issue | Strategy | Rationale |
|---|---|---|
| Non-standard column names | Renamed to standard | Consistent downstream queries |
| Missing campaign_id | Created via factorize(campaign_name) | Unique int per campaign |
| Currency symbols (₹) | Strip before coerce | Root cause of empty columns |
| Missing spend/clicks | Group-median | Skewed distribution |
| Missing conversions/revenue | Fill 0 | No event = 0 |
| Wrong CTR/CPC/CPM | Recalculate from source | Source columns are truth |