"""
Quick database verification script.
Run: python verify_db.py
"""
import sqlite3
import os

# ── Find the database ──────────────────────────────────────────────────────────
candidates = [
    '../Data/cleaned_campaigns.db',
    '../data/cleaned_campaigns.db',
    'Data/cleaned_campaigns.db',
    'data/cleaned_campaigns.db',
    'cleaned_campaigns.db',
]

db_path = None
for p in candidates:
    if os.path.exists(p):
        db_path = p
        break

if not db_path:
    print("❌  cleaned_campaigns.db not found!")
    print("    Run clean_campaigns.py first.")
    exit(1)

print(f"✅  Found DB: {db_path}")
print(f"    Size: {os.path.getsize(db_path) // 1024} KB\n")

conn = sqlite3.connect(db_path)

# ── List all tables and views ──────────────────────────────────────────────────
objects = conn.execute(
    "SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name"
).fetchall()

print("=" * 50)
print("TABLES & VIEWS IN DATABASE")
print("=" * 50)
for name, typ in objects:
    print(f"  [{typ.upper()}]  {name}")

print()

# ── Row counts + empty column check ───────────────────────────────────────────
print("=" * 50)
print("ROW COUNTS + EMPTY COLUMN CHECK")
print("=" * 50)
for name, typ in objects:
    if typ != 'table':
        continue
    count = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
    cols  = conn.execute(f"PRAGMA table_info([{name}])").fetchall()
    empty = []
    for col in cols:
        col_name = col[1]
        non_null = conn.execute(
            f"SELECT COUNT(*) FROM [{name}] WHERE [{col_name}] IS NOT NULL"
        ).fetchone()[0]
        if non_null == 0:
            empty.append(col_name)

    status = "✅  all columns populated" if not empty else f"⚠️  EMPTY COLS: {empty}"
    print(f"\n  {name} — {count} rows — {status}")

    # Show sample row
    sample = conn.execute(f"SELECT * FROM [{name}] LIMIT 1").fetchone()
    if sample:
        col_names = [c[1] for c in cols]
        print(f"  Sample row:")
        for cn, val in zip(col_names, sample):
            print(f"    {cn:<30} = {val}")

conn.close()
print("\n" + "=" * 50)
print("Verification complete.")
