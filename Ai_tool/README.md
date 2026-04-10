# AI Insight Tool — README
## Task 5 · Growify Digital Take-Home Assignment

---

## Architecture

```
User Question
     │
     ▼ Step 1 — Text-to-SQL
  LLM receives: question + schema context + conversation history
  LLM outputs:  a targeted SQLite SQL query
     │
     ▼ Step 2 — SQL Execution
  Python runs query against cleaned_campaigns.db
  Returns result rows (max 30 sent to LLM)
     │
     ▼ Step 3 — Answer Generation
  LLM receives: question + result rows as JSON
  LLM outputs:  plain-English business insight
     │
     ▼
  Streamlit displays: answer + SQL expander + raw data expander
```

**Key principle:** The LLM never sees the full table.
It generates SQL → only result rows are passed back. Keeps tokens low and answers accurate.

---

## Setup

### 1. Install dependencies (from project root)
```bash
pip install -r requirements.txt
```

### 2. Get a free API key
- **Anthropic Claude** (recommended): https://console.anthropic.com
- **OpenAI GPT-4o** (alternative): https://platform.openai.com

### 3. Set your API key
```bash
cd ai_tool
cp .env.example .env
# Edit .env and paste your key
```

### 4. Make sure databases exist
```bash
cd ../python
python clean_campaigns.py
python clean_shopify.py
```

### 5. Run
```bash
cd ../ai_tool
streamlit run app.py
```
Opens at: **http://localhost:8501**

---

## 10 Example Questions

| # | Question | What it tests |
|---|---|---|
| 1 | **"Which campaign had the worst CPC in March?"** | Month filter + metric ranking |
| 2 | **"Summarise UK region performance"** | Regional string filter + multi-metric summary |
| 3 | **"What is total spend by platform?"** | Simple aggregation + grouping |
| 4 | **"Which channel has the highest ROAS?"** | Channel ranking by calculated metric |
| 5 | **"Show me the top 5 campaigns by conversions"** | Ranking + LIMIT |
| 6 | **"What was the month-over-month spend change?"** | MoM view + LAG function |
| 7 | **"Which region had the lowest ROI?"** | Ascending sort + ROI metric |
| 8 | **"How did Meta perform compared to Google last quarter?"** | Platform comparison + quarter filter |
| 9 | **"What is the average CTR across all channels?"** | Simple average + grouping |
| 10 | **"Which country had the most conversions overall?"** | Country-level ranking |

---

## Follow-up Example (Conversation Memory)

```
User:  Which campaign had the worst CPC in March?
AI:    Campaign "Spring_Boost_UK" on Meta had the worst CPC of $5.82 in March.

User:  What was its ROAS?
AI:    "Spring_Boost_UK" had a ROAS of 1.24x in March — below the platform average.
       [correctly resolves "its" via conversation history]

User:  How did it compare to Google campaigns that month?
AI:    Google campaigns averaged a ROAS of 2.87x in March vs Meta's 1.24x...
```

---

## Design Decisions

| Decision | Rationale |
|---|---|
| Text-to-SQL (LLM generates SQL) | No hardcoded query templates; handles any question |
| Schema context in system prompt | LLM knows exact column names → fewer hallucinations |
| Result rows capped at 30 for LLM | Keeps prompt small; prevents token limit errors |
| Conversation memory (last 6 turns) | Enables natural follow-up questions |
| LOWER() for string comparisons | Handles 'Google' / 'google' / 'GOOGLE' |
| Views as primary data source | Pre-aggregated; AI never queries full fact table |
| Streamlit + expanders | Clean UX; SQL visible for transparency |
