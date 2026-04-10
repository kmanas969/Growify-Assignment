"""
Applies schema.sql to cleaned_campaigns.db
Run: python run_schema.py
"""
import sqlite3
import os

# ── Find the database ──────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_CANDIDATES = [
    os.path.join(BASE_DIR, '..', 'Data', 'cleaned_campaigns.db'),
    os.path.join(BASE_DIR, '..', 'data', 'cleaned_campaigns.db'),
    os.path.join(BASE_DIR, 'Data', 'cleaned_campaigns.db'),
]

SQL_CANDIDATES = [
    os.path.join(BASE_DIR, '..', 'sql', 'schema.sql'),
    os.path.join(BASE_DIR, 'sql', 'schema.sql'),
    os.path.join(BASE_DIR, 'schema.sql'),
]

db_path  = next((p for p in DB_CANDIDATES  if os.path.exists(p)), None)
sql_path = next((p for p in SQL_CANDIDATES if os.path.exists(p)), None)

# ── Guard checks ───────────────────────────────────────────────────────────────
if not db_path:
    print("❌  cleaned_campaigns.db not found!")
    print("    Run clean_campaigns.py first.")
    exit(1)

if not sql_path:
    print("❌  schema.sql not found!")
    print("    Make sure it exists in the sql/ folder.")
    exit(1)

print(f"✅  DB     : {db_path}")
print(f"✅  Schema : {sql_path}")
print()

# ── Apply schema.sql ───────────────────────────────────────────────────────────
conn = sqlite3.connect(db_path)

with open(sql_path, 'r', encoding='utf-8') as f:
    sql_script = f.read()

# Split into individual statements and run one by one
# This gives better error messages than executescript()
statements = [s.strip() for s in sql_script.split(';') if s.strip()]

success = 0
skipped = 0
errors  = 0

for stmt in statements:
    # Skip comments-only blocks
    lines = [l for l in stmt.splitlines() if not l.strip().startswith('--')]
    clean = '\n'.join(lines).strip()
    if not clean:
        continue
    try:
        conn.execute(clean)
        success += 1
    except sqlite3.OperationalError as e:
        msg = str(e)
        # "already exists" is fine — schema uses IF NOT EXISTS but just in case
        if 'already exists' in msg:
            skipped += 1
        else:
            print(f"  ⚠️  Error: {msg}")
            print(f"     Statement: {clean[:80]}...")
            errors += 1

conn.commit()

# ── Show what's now in the DB ──────────────────────────────────────────────────
print(f"Schema applied — {success} OK, {skipped} skipped (already exist), {errors} errors")
print()
print("=" * 50)
print("OBJECTS NOW IN DATABASE")
print("=" * 50)

objects = conn.execute(
    "SELECT type, name FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name"
).fetchall()

tables = [(t,n) for t,n in objects if t == 'table']
views  = [(t,n) for t,n in objects if t == 'view']

print(f"\nTABLES ({len(tables)}):")
for _, name in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
    print(f"  ✅  {name}  ({count} rows)")

print(f"\nVIEWS ({len(views)}):")
for _, name in views:
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        print(f"  ✅  {name}  ({count} rows)")
    except Exception as e:
        print(f"  ⚠️  {name}  — {e}")

conn.close()
print("\n✅  Done!")