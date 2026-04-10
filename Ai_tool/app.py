"""
Task 5 — AI Insight Tool  (Text-to-SQL + LLM)
Growify Digital Take-Home Assignment

Supports 4 LLM providers:
  ✅ Groq         (FREE, fastest, no card — RECOMMENDED)
  ✅ Gemini       (Free quota, may hit limits)
  ✅ Anthropic    (Free $5 credits)
  ✅ OpenAI       (Paid)

Run:  cd ai_tool && streamlit run app.py
"""

import os, re, json, sqlite3
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE PATH
# ══════════════════════════════════════════════════════════════════════════════
BASE = os.path.dirname(os.path.abspath(__file__))

def _find_db(name):
    for step in [BASE, os.path.join(BASE,'..'), os.path.join(BASE,'../..')]:
        for folder in ['Data','data','']:
            p = os.path.normpath(os.path.join(step, folder, name))
            if os.path.exists(p):
                return p
    return None

CAMPAIGNS_DB = _find_db('cleaned_campaigns.db')
# Hardcode fallback if auto-detect fails:
# CAMPAIGNS_DB = r'D:\Growify_assignment\Data\cleaned_campaigns.db'

# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA CONTEXT
# ══════════════════════════════════════════════════════════════════════════════
SCHEMA = """
SQLite database: cleaned_campaigns.db

== EXACT COLUMN NAMES PER VIEW — use these exactly ==

TABLE: fact_campaigns
  campaign_id, date, impressions, clicks, conversions,
  spend, revenue, ctr, cpc, cpm, roi, roas

TABLE: dim_campaign
  campaign_id, campaign_name, platform, channel, region,
  country, brand_name, status, budget, ad_group

TABLE: dim_date
  date, week, month, quarter, year, month_name, day_of_week

VIEW: vw_ai_campaign_summary  <- USE FOR MOST QUESTIONS
  columns: year, month, month_name, quarter,
           platform, channel, region, country, brand_name,
           campaign_id, campaign_name, status,
           impressions, clicks, conversions, spend, revenue,
           ctr, cpc, roas, roi, cpm, conversion_rate

VIEW: vw_powerbi_performance  <- USE FOR DATE/TREND QUESTIONS
  columns: year, quarter, month, month_name, date,
           platform, channel, region, country, brand_name,
           campaign_id, campaign_name, status,
           total_impressions, total_clicks, total_conversions,
           total_spend, total_revenue,
           ctr, cpc, cpm, roi, roas, conversion_rate

VIEW: vw_top_campaigns_roas  <- USE FOR CAMPAIGN RANKING
  columns: campaign_id, campaign_name, platform, channel, region, brand_name,
           total_spend, total_revenue, total_conversions, total_clicks,
           roas, cpc, ctr

VIEW: vw_country_performance  <- USE FOR COUNTRY/REGION QUESTIONS
  columns: country, region,
           total_spend, total_revenue, total_conversions, total_clicks,
           roas, cpc, ctr, roi

VIEW: vw_mom_spend  <- USE FOR MONTH-OVER-MONTH QUESTIONS
  columns: year, month, month_name,
           monthly_spend, prev_month_spend, mom_pct_change

VIEW: vw_platform_channel_breakdown  <- USE FOR PLATFORM/CHANNEL BREAKDOWN
  columns: platform, channel,
           total_spend, total_revenue, total_conversions,
           total_clicks, total_impressions,
           roas, cpc, ctr, conversion_rate

== CRITICAL: COLUMN NAME RULES ==
vw_ai_campaign_summary    -> spend, revenue, clicks (NO total_ prefix)
vw_powerbi_performance    -> total_spend, total_revenue, total_clicks (WITH total_ prefix)
vw_top_campaigns_roas     -> total_spend, total_revenue (WITH total_ prefix)
vw_country_performance    -> total_spend, total_revenue (WITH total_ prefix)
vw_platform_channel_breakdown -> total_spend, total_revenue (WITH total_ prefix)

== OTHER RULES ==
- Strings are Title Case: 'Facebook', 'Google', 'Active'
- Date format: YYYY-MM-DD
- month_name is full English: 'January', 'March', 'December'
- worst CPC = highest value = ORDER BY cpc DESC
- best CPC  = lowest value  = ORDER BY cpc ASC
- worst ROI = lowest value  = ORDER BY roi ASC
"""

SQL_SYSTEM = f"""You are a SQLite expert for a marketing analytics database.
Convert the user question into a single valid SQLite SQL query.
{SCHEMA}
STRICT RULES:
1. Return ONLY raw SQL — no markdown, no explanation, no code fences
2. Prefer views (vw_*) over raw tables for aggregation
3. Always LIMIT 50 unless asked for all
4. Use LOWER() for string filters: WHERE LOWER(platform)='facebook'
5. Month filter: WHERE LOWER(month_name)='march'
6. Never SELECT * — name columns explicitly
"""

ANSWER_SYSTEM = """You are a senior marketing analyst at Growify Digital.
You receive: (1) user question (2) SQL result rows as JSON.
Write a concise business-friendly answer:
- Lead with the most important finding
- Bullet points for multi-metric answers
- Use ₹/$ for money, 2 decimal places for percentages
- Max 200 words unless full breakdown requested
- Never mention SQL or technical details
"""

# ══════════════════════════════════════════════════════════════════════════════
# LLM PROVIDERS
# ══════════════════════════════════════════════════════════════════════════════
def call_groq(system, messages, max_tokens=800):
    from groq import Groq
    key = os.getenv("GROQ_API_KEY","")
    if not key:
        st.error("GROQ_API_KEY not set in .env\nGet free key: https://console.groq.com")
        st.stop()
    client = Groq(api_key=key)
    r = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role":"system","content":system}]+messages,
        max_tokens=max_tokens
    )
    return r.choices[0].message.content.strip()

def call_gemini(system, messages, max_tokens=800):
    import google.generativeai as genai
    key = os.getenv("GEMINI_API_KEY","")
    if not key:
        st.error("GEMINI_API_KEY not set in .env\nGet free key: https://aistudio.google.com")
        st.stop()
    genai.configure(api_key=key)
    model = genai.GenerativeModel(model_name="gemini-2.0-flash", system_instruction=system)
    history = [{"role":"user" if m["role"]=="user" else "model","parts":[m["content"]]}
               for m in messages[:-1]]
    chat = model.start_chat(history=history)
    return chat.send_message(messages[-1]["content"]).text.strip()

def call_anthropic(system, messages, max_tokens=800):
    import anthropic
    key = os.getenv("ANTHROPIC_API_KEY","")
    if not key:
        st.error("ANTHROPIC_API_KEY not set in .env\nGet key: https://console.anthropic.com")
        st.stop()
    r = anthropic.Anthropic(api_key=key).messages.create(
        model="claude-sonnet-4-20250514", max_tokens=max_tokens,
        system=system, messages=messages
    )
    return r.content[0].text.strip()

def call_openai(system, messages, max_tokens=800):
    from openai import OpenAI
    key = os.getenv("OPENAI_API_KEY","")
    if not key:
        st.error("OPENAI_API_KEY not set in .env")
        st.stop()
    r = OpenAI(api_key=key).chat.completions.create(
        model="gpt-4o",
        messages=[{"role":"system","content":system}]+messages,
        max_tokens=max_tokens
    )
    return r.choices[0].message.content.strip()

def call_llm(system, messages, provider, max_tokens=800):
    if provider == "Groq (Free ⚡ Recommended)": return call_groq(system, messages, max_tokens)
    elif provider == "Gemini (Free)":            return call_gemini(system, messages, max_tokens)
    elif provider == "Anthropic (Claude)":       return call_anthropic(system, messages, max_tokens)
    else:                                        return call_openai(system, messages, max_tokens)

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def history_msgs(history):
    msgs = []
    for t in history[-6:]:
        msgs.append({"role":"user",      "content": t["question"]})
        msgs.append({"role":"assistant", "content": t["answer"]})
    return msgs

def generate_sql(question, history, provider):
    msgs = history_msgs(history)
    msgs.append({"role":"user","content":f"Question: {question}"})
    raw = call_llm(SQL_SYSTEM, msgs, provider, max_tokens=400)
    return re.sub(r'```(?:sql)?|```','', raw).strip()

def run_query(sql):
    if not CAMPAIGNS_DB:
        raise RuntimeError("cleaned_campaigns.db not found.\nRun clean_campaigns.py first.")
    conn = sqlite3.connect(CAMPAIGNS_DB)
    conn.row_factory = sqlite3.Row
    try:
        cur  = conn.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    except sqlite3.Error as e:
        raise RuntimeError(f"SQL Error: {e}\n\nQuery:\n{sql}")
    finally:
        conn.close()

def generate_answer(question, sql, rows, history, provider):
    msgs = history_msgs(history)
    msgs.append({"role":"user","content":(
        f"Question: {question}\n\nSQL:\n{sql}\n\n"
        f"Result ({len(rows)} rows):\n{json.dumps(rows[:30],indent=2,default=str)}"
    )})
    return call_llm(ANSWER_SYSTEM, msgs, provider, max_tokens=600)

# ══════════════════════════════════════════════════════════════════════════════
# STREAMLIT UI
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Growify AI Insights", page_icon="📊", layout="wide")

if "history"  not in st.session_state: st.session_state.history  = []
if "provider" not in st.session_state: st.session_state.provider = "Groq (Free ⚡ Recommended)"

PROVIDERS = ["Groq (Free ⚡ Recommended)","Gemini (Free)","Anthropic (Claude)","OpenAI (GPT-4o)"]

with st.sidebar:
    st.title("📊 Growify AI")
    st.caption("Marketing Insight Tool")
    st.divider()

    st.subheader("🤖 LLM Provider")
    provider = st.radio("Select provider:", PROVIDERS,
                        index=PROVIDERS.index(st.session_state.provider))
    st.session_state.provider = provider

    # Key status + instructions per provider
    if provider == "Groq (Free ⚡ Recommended)":
        st.success("✅ Completely FREE — no credit card!")
        st.info("1. Go to https://console.groq.com\n"
                "2. Sign up with Gmail\n"
                "3. API Keys → Create API Key\n"
                "4. Add to .env:\n   GROQ_API_KEY=gsk_xxx")
        key_ok = bool(os.getenv("GROQ_API_KEY"))

    elif provider == "Gemini (Free)":
        st.warning("⚠️ Has rate limits — may get 429 error")
        st.info("1. Go to https://aistudio.google.com\n"
                "2. Get API Key → Create\n"
                "3. Add to .env:\n   GEMINI_API_KEY=AIzaSy_xxx")
        key_ok = bool(os.getenv("GEMINI_API_KEY"))

    elif provider == "Anthropic (Claude)":
        st.info("$5 free credits\n"
                "1. https://console.anthropic.com\n"
                "2. Add to .env:\n   ANTHROPIC_API_KEY=sk-ant-xxx")
        key_ok = bool(os.getenv("ANTHROPIC_API_KEY"))

    else:
        st.warning("Paid plan required")
        key_ok = bool(os.getenv("OPENAI_API_KEY"))

    if key_ok:
        st.success("🔑 API key detected ✅")
    else:
        st.error("🔑 API key NOT found in .env")

    st.divider()
    st.subheader("🗄️ Database")
    db_ok = CAMPAIGNS_DB and os.path.exists(CAMPAIGNS_DB)
    if db_ok:
        st.success(f"✅ Connected ({os.path.getsize(CAMPAIGNS_DB)//1024} KB)")
        st.caption(CAMPAIGNS_DB)
    else:
        st.error("❌ DB not found\nRun clean_campaigns.py first")

    st.divider()
    st.subheader("💡 Try These")
    EXAMPLES = [
        "What is total spend by platform?",
        "Which campaign had the highest ROAS?",
        "Show top 5 campaigns by conversions",
        "Which country had the most clicks?",
        "What was month-over-month spend change?",
        "Which channel has the best CTR?",
        "What is average CPC by platform?",
        "Show total revenue vs spend",
        "Which region had the lowest ROI?",
        "Compare campaigns by status",
    ]
    for ex in EXAMPLES:
        if st.button(ex, use_container_width=True, key=f"btn_{ex[:12]}"):
            st.session_state["_q"] = ex

    st.divider()
    if st.button("🗑️ Clear conversation", use_container_width=True):
        st.session_state.history = []
        st.rerun()

# ── MAIN ──────────────────────────────────────────────────────────────────────
st.title("📊 Growify AI Insight Tool")
st.caption("Ask natural-language questions → answered from your SQL database")

if not db_ok:
    st.error("⚠️  Database not found. Run `python clean_campaigns.py` first.")
    st.stop()

# Replay history
for turn in st.session_state.history:
    with st.chat_message("user"):
        st.write(turn["question"])
    with st.chat_message("assistant"):
        st.write(turn["answer"])
        with st.expander("🔍 SQL generated"):
            st.code(turn["sql"], language="sql")
        if turn.get("rows"):
            with st.expander(f"📋 Raw data ({turn['row_count']} rows)"):
                st.dataframe(pd.DataFrame(turn["rows"]), use_container_width=True)

# Question input
prefill  = st.session_state.pop("_q", None)
question = st.chat_input("Ask about your campaign performance …")
if prefill and not question:
    question = prefill

if question:
    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("🤖 Generating SQL …"):
            try:
                sql = generate_sql(question, st.session_state.history,
                                   st.session_state.provider)
            except Exception as e:
                st.error(f"SQL generation failed: {e}"); st.stop()

        with st.spinner("⚙️ Running query …"):
            try:
                rows = run_query(sql)
            except RuntimeError as e:
                st.error(str(e)); st.stop()

        with st.spinner("✍️ Generating insight …"):
            answer = generate_answer(question, sql, rows,
                                     st.session_state.history,
                                     st.session_state.provider)

        st.write(answer)

        c1, c2 = st.columns(2)
        with c1:
            with st.expander("🔍 SQL generated"):
                st.code(sql, language="sql")
        with c2:
            if rows:
                with st.expander(f"📋 Raw data ({len(rows)} rows)"):
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
            else:
                st.info("Query returned 0 rows.")

        st.session_state.history.append({
            "question":  question,
            "sql":       sql,
            "answer":    answer,
            "rows":      rows[:10],
            "row_count": len(rows),
        })
