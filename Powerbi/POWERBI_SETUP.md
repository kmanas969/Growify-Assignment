# Power BI Setup Guide — Task 4
## Growify Digital Take-Home Assignment

> **Requirement:** Connect Power BI directly to SQLite.
> Do NOT import any CSV files.

---

## Step 1 — Install SQLite ODBC Driver

1. Download the 64-bit SQLite ODBC driver:
   https://www.ch-werner.de/sqliteodbc/
   → file: `sqliteodbc_w64.exe`

2. Run the installer (accept defaults)

3. Open **ODBC Data Sources (64-bit)**
   (Windows search → "ODBC Data Sources (64-bit)")

4. Click **Add** → select **SQLite3 ODBC Driver** → Finish

5. Set:
   - **Data Source Name:** `GrowifyCampaigns`
   - **Database Name:** Browse to `data/cleaned_campaigns.db`
   - Click **OK**

---

## Step 2 — Connect Power BI to SQLite

1. Open **Power BI Desktop**

2. **Home** → **Get Data** → **ODBC**

3. Select DSN: `GrowifyCampaigns` → **OK**

4. In the Navigator, select these objects:
   - ✅ `dim_date`
   - ✅ `dim_campaign`
   - ✅ `fact_campaigns`
   - ✅ `vw_powerbi_performance`
   - ✅ `vw_top_campaigns_roas`
   - ✅ `vw_country_performance`
   - ✅ `vw_mom_spend`
   - ✅ `vw_platform_channel_breakdown`

5. Click **Load** (not Transform — data is already clean)

---

## Step 3 — Data Model (Star Schema)

In the **Model** view, set these relationships:

```
dim_date.date          ──1:M──  fact_campaigns.date
dim_campaign.campaign_id ──1:M──  fact_campaigns.campaign_id
dim_date.date          ──1:M──  vw_powerbi_performance.date
dim_campaign.campaign_id ──1:M──  vw_powerbi_performance.campaign_id
```

Right-click `dim_date` → **Mark as date table** → select `date` column

---

## Step 4 — Create a Measures Table

1. **Home** → **Enter Data** → name it `_Measures` → **Load**
2. Delete the default column
3. Right-click `_Measures` → **New Measure** for each measure below

---

## Step 5 — All DAX Measures

Paste each one as a new measure in the `_Measures` table.

```dax
// ═══════════════════════════════════════════════════════════
// CORE VOLUME METRICS
// ═══════════════════════════════════════════════════════════

Total Spend =
SUM(fact_campaigns[spend])

Total Revenue =
SUM(fact_campaigns[revenue])

Total Conversions =
SUM(fact_campaigns[conversions])

Total Clicks =
SUM(fact_campaigns[clicks])

Total Impressions =
SUM(fact_campaigns[impressions])

// ═══════════════════════════════════════════════════════════
// RATE METRICS — safe division via DIVIDE()
// ═══════════════════════════════════════════════════════════

CTR % =
DIVIDE(
    SUM(fact_campaigns[clicks]),
    SUM(fact_campaigns[impressions]),
    0
) * 100

CPC =
DIVIDE(
    SUM(fact_campaigns[spend]),
    SUM(fact_campaigns[clicks]),
    0
)

CPM =
DIVIDE(
    SUM(fact_campaigns[spend]),
    SUM(fact_campaigns[impressions]),
    0
) * 1000

ROAS =
DIVIDE(
    SUM(fact_campaigns[revenue]),
    SUM(fact_campaigns[spend]),
    0
)

ROI =
DIVIDE(
    SUM(fact_campaigns[revenue]) - SUM(fact_campaigns[spend]),
    SUM(fact_campaigns[spend]),
    0
)

Conversion Rate % =
DIVIDE(
    SUM(fact_campaigns[conversions]),
    SUM(fact_campaigns[clicks]),
    0
) * 100

// ═══════════════════════════════════════════════════════════
// COUNTRY-WISE PERFORMANCE
// ═══════════════════════════════════════════════════════════

Country Spend Rank =
RANKX(
    ALL(dim_campaign[country]),
    [Total Spend],
    ,
    DESC,
    DENSE
)

Country ROAS =
DIVIDE(
    CALCULATE(SUM(fact_campaigns[revenue])),
    CALCULATE(SUM(fact_campaigns[spend])),
    0
)

// ═══════════════════════════════════════════════════════════
// MONTH-OVER-MONTH SPEND CHANGE
// ═══════════════════════════════════════════════════════════

Prev Month Spend =
CALCULATE(
    [Total Spend],
    DATEADD(dim_date[date], -1, MONTH)
)

MoM Spend Change % =
DIVIDE(
    [Total Spend] - [Prev Month Spend],
    [Prev Month Spend],
    0
) * 100

MoM Spend Change $ =
[Total Spend] - [Prev Month Spend]

// ═══════════════════════════════════════════════════════════
// LABELS  (for KPI card subtitles)
// ═══════════════════════════════════════════════════════════

Total Sales =
SUM(fact_campaigns[revenue])

ROAS Label =
FORMAT([ROAS], "0.00") & "x"

CTR Label =
FORMAT([CTR %], "0.00") & "%"

Spend Label =
"$" & FORMAT([Total Spend], "#,##0")

MoM Label =
IF([MoM Spend Change %] >= 0,
   "▲ " & FORMAT([MoM Spend Change %], "0.0") & "%",
   "▼ " & FORMAT(ABS([MoM Spend Change %]), "0.0") & "%")
```

---

## Step 6 — Build the 3-Page Dashboard

### Page 1 — Executive Summary

| Visual | Type | Fields | Notes |
|---|---|---|---|
| Total Spend | KPI Card | [Total Spend] | Subtitle: MoM Label |
| Total Revenue | KPI Card | [Total Sales] | |
| ROAS | KPI Card | [ROAS] | Subtitle: ROAS Label |
| Total Conversions | KPI Card | [Total Conversions] | |
| CTR % | KPI Card | [CTR %] | Subtitle: CTR Label |
| Spend vs Conversions | Line Chart | X: dim_date[date] grouped by month, Y1: [Total Spend], Y2: [Total Conversions] | Dual axis |
| Top 5 Campaigns | Table | campaign_name, platform, [Total Spend], [Total Revenue], [ROAS], [Total Conversions] | Sort by ROAS |

**Slicers on this page (sync to all pages):**
- Date range slider: `dim_date[date]`
- Brand Name: `dim_campaign[brand_name]` (multi-select dropdown)

---

### Page 2 — Channel Breakdown

| Visual | Type | Fields | Notes |
|---|---|---|---|
| Spend by Platform | Clustered Bar | Axis: platform, Values: [Total Spend], [Total Revenue] | Sort by spend |
| Channel Mix | Donut | Legend: channel, Values: [Total Spend] | |
| Region Matrix | Matrix | Rows: region, Columns: month_name, Values: [Total Spend], [ROAS] | Conditional formatting on ROAS |
| CPC by Channel | Bar | Axis: channel, Values: [CPC] | Sort ascending (lower = better) |
| Platform KPIs | Card row | [CPC], [CPM], [Conversion Rate %] | Filtered by platform slicer |

---

### Page 3 — Audience Insights

| Visual | Type | Fields | Notes |
|---|---|---|---|
| Spend vs Conversions | Scatter | X: [Total Spend], Y: [Total Conversions], Size: [Total Revenue], Legend: platform | Bubble chart |
| Conversions by Country | Bar | Axis: country, Values: [Total Conversions] | Sort DESC |
| Conversion Rate by Region | Bar | Axis: region, Values: [Conversion Rate %] | |
| Map | Map/Filled Map | Location: country, Size: [Total Spend] | |

---

## Step 7 — Cross-page Slicers

1. Add slicers on Page 1:
   - Date slicer: `dim_date[date]` → format as "Between"
   - Brand Name: `dim_campaign[brand_name]` → Multi-select dropdown

2. **View** → **Sync Slicers** → enable both slicers on all 3 pages

---

## Step 8 — Drill-through

1. On Page 3, right-click blank area → **Add drillthrough**
2. Add `dim_campaign[campaign_name]` as drillthrough field
3. Now users can right-click any country bar → **Drillthrough** → see campaigns for that country

---

## Step 9 — Export

1. **File** → **Export** → **Export to PDF** (select "All Pages")
2. Save `.pbix` file
3. Submit both:
   - `powerbi/growify_dashboard.pbix`
   - `powerbi/growify_dashboard.pdf`

---

## Troubleshooting

| Problem | Fix |
|---|---|
| ODBC driver not showing | Install 64-bit version, restart Power BI |
| No data in visuals | Check that clean_campaigns.py ran successfully |
| Relationships not working | Ensure dim_date is marked as Date Table |
| MoM measures return blank | dim_date must be marked as Date Table first |
| Map visual not loading | Enable "Map and filled map visuals" in Options → Security |
