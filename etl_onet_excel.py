# etl_onet_excel.py
"""
ETL for O*NET (Excel) -> SQLite.
Usage:
    pip install pandas openpyxl
    python etl_onet_excel.py --data_dir ./db_30_0_excel --out_db ./onet_dw.db --out_dir ./output

# This script:
#  - Reads O*NET excel files (Occupation Data, Skills, Knowledge, Abilities, Task Statements, Task Ratings)
#  - Cleans column names
#  - Builds dims and facts
#  - Writes to SQLite (onet_dw.db)
#  - Runs validation checks and sample SQL queries
# """

# import argparse
# from pathlib import Path
# import pandas as pd
# import sqlite3
# import sys

# def clean_cols(df):
#     df = df.copy()
#     df.columns = [str(c).strip().lower().replace(' ', '_').replace('*','').replace('/','_').replace('-','_') for c in df.columns]
#     return df

# def to_numeric_cols(df, cols):
#     for c in cols:
#         if c in df.columns:
#             df[c] = pd.to_numeric(df[c], errors='coerce')
#     return df

# def read_excel(path, nrows=None):
#     df = pd.read_excel(path, sheet_name=0, nrows=nrows, dtype=object)
#     df = clean_cols(df)
#     df = df.rename(columns={c: c.replace('o*net_soc_code','onet_soc_code') for c in df.columns})
#     return df

# def build_dim_fact_from_ratings(path, key_element_col='element_id', element_name_col='element_name', nrows=None):
#     df = read_excel(path, nrows=nrows)
#     num_cols = [c for c in ['data_value','n','standard_error','lower_ci_bound','upper_ci_bound'] if c in df.columns]
#     df = to_numeric_cols(df, num_cols)
#     dim = df[[c for c in [key_element_col, element_name_col] if c in df.columns]].drop_duplicates().reset_index(drop=True)
#     fact_cols = [c for c in ['onet_soc_code', key_element_col, 'scale_id', 'scale_name', 'category',
#                               'data_value','n','standard_error','lower_ci_bound','upper_ci_bound',
#                               'recommend_suppress','not_relevant','date','domain_source'] if c in df.columns]
#     fact = df[fact_cols].copy()
#     return dim, fact

# def find_file(data_dir, keywords):
#     """
#     Find the first Excel file in data_dir whose name contains all keywords.
#     Keywords are case-insensitive.
#     """
#     for f in data_dir.glob("*.xlsx"):
#         name = f.name.lower()
#         if all(k in name for k in keywords):
#             return f
#     return None

# def main(data_dir, out_db, out_dir, sample_rows=None):
#     data_dir = Path(data_dir)
#     out_db = Path(out_db)
#     out_dir = Path(out_dir)
#     out_dir.mkdir(parents=True, exist_ok=True)

#     # --- 1) Occupations ---
#     occ_path = find_file(data_dir, ["occupation"])
#     if not occ_path or not occ_path.exists():
#         print("ERROR: Could not find Occupation file in", data_dir); sys.exit(1)
#     print("Using Occupation file:", occ_path.name)
#     occ = read_excel(occ_path, nrows=sample_rows)
#     occ_cols = [c for c in ['onet_soc_code','title','description'] if c in occ.columns]
#     dim_occupations = occ[occ_cols].drop_duplicates().reset_index(drop=True)

#     # --- 2) Skills ---
#     skills_path = find_file(data_dir, ["skill"])
#     if skills_path:
#         print("Using Skills file:", skills_path.name)
#         dim_skills, fact_skills = build_dim_fact_from_ratings(skills_path, nrows=sample_rows)
#     else:
#         print("WARNING: Skills file not found")
#         dim_skills, fact_skills = pd.DataFrame(), pd.DataFrame()

#     # --- 3) Knowledge ---
#     knowledge_path = find_file(data_dir, ["knowledge"])
#     if knowledge_path:
#         print("Using Knowledge file:", knowledge_path.name)
#         dim_knowledge, fact_knowledge = build_dim_fact_from_ratings(knowledge_path, nrows=sample_rows)
#     else:
#         print("WARNING: Knowledge file not found")
#         dim_knowledge, fact_knowledge = pd.DataFrame(), pd.DataFrame()

#     # --- 4) Abilities ---
#     abilities_path = find_file(data_dir, ["abilit"])
#     if abilities_path:
#         print("Using Abilities file:", abilities_path.name)
#         dim_abilities, fact_abilities = build_dim_fact_from_ratings(abilities_path, nrows=sample_rows)
#     else:
#         print("WARNING: Abilities file not found")
#         dim_abilities, fact_abilities = pd.DataFrame(), pd.DataFrame()

#     # --- 5) Tasks ---
#     ts_path = find_file(data_dir, ["task","statement"])
#     dim_tasks = pd.DataFrame()
#     if ts_path:
#         print("Using Task Statements file:", ts_path.name)
#         ts = read_excel(ts_path, nrows=sample_rows)
#         task_cols = [c for c in ['task_id','task','task_type','incumbents_responding','date','domain_source'] if c in ts.columns]
#         dim_tasks = ts[task_cols].drop_duplicates().reset_index(drop=True)

#     # --- 6) Task Ratings ---
#     tr_path = find_file(data_dir, ["task","rating"])
#     fact_task_ratings = None
#     if tr_path:
#         print("Using Task Ratings file:", tr_path.name)
#         tr = read_excel(tr_path, nrows=sample_rows)
#         num_cols = [c for c in ['data_value','n','standard_error','lower_ci_bound','upper_ci_bound'] if c in tr.columns]
#         tr = to_numeric_cols(tr, num_cols)
#         tr_cols = [c for c in ['onet_soc_code','task_id','task','scale_id','scale_name','category','data_value','n','standard_error','lower_ci_bound','upper_ci_bound','recommend_suppress','not_relevant','date','domain_source'] if c in tr.columns]
#         fact_task_ratings = tr[tr_cols].copy()

#     # --- 7) Write to SQLite ---
#     conn = sqlite3.connect(str(out_db))
#     dim_occupations.to_sql("dim_occupations", conn, if_exists='replace', index=False)
#     if not dim_skills.empty:
#         dim_skills.to_sql("dim_skills", conn, if_exists='replace', index=False)
#     if not dim_knowledge.empty:
#         dim_knowledge.to_sql("dim_knowledge", conn, if_exists='replace', index=False)
#     if not dim_abilities.empty:   # <-- FIXED HERE
#         dim_abilities.to_sql("dim_abilities", conn, if_exists='replace', index=False)
#     if not dim_tasks.empty:
#         dim_tasks.to_sql("dim_tasks", conn, if_exists='replace', index=False)
#     if not fact_skills.empty:
#         fact_skills.to_sql("fact_skills", conn, if_exists='replace', index=False)
#     if not fact_knowledge.empty:
#         fact_knowledge.to_sql("fact_knowledge", conn, if_exists='replace', index=False)
#     if not fact_abilities.empty:
#         fact_abilities.to_sql("fact_abilities", conn, if_exists='replace', index=False)
#     if fact_task_ratings is not None:
#         fact_task_ratings.to_sql("fact_task_ratings", conn, if_exists='replace', index=False)

#     # Create indexes
#     cur = conn.cursor()
#     cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_occ_code ON dim_occupations(onet_soc_code)")
#     cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_skills_occ ON fact_skills(onet_soc_code)")
#     cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_skills_elem ON fact_skills(element_id)")  # fixed
#     cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_skills_elem ON dim_skills(element_id)")    # fixed
#     conn.commit()

#     # --- 8) Validation checks ---
#     validation = []
#     def rowcount(table):
#         try:
#             return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
#         except Exception:
#             return None

#     tables = ["dim_occupations","dim_skills","dim_knowledge","dim_abilities","fact_skills","fact_knowledge","fact_abilities"]
#     if not dim_tasks.empty:
#         tables.append("dim_tasks")
#     if fact_task_ratings is not None:
#         tables.append("fact_task_ratings")

#     for t in tables:
#         validation.append({"table":t,"rowcount":rowcount(t)})

#     for ft in ["fact_skills","fact_knowledge","fact_abilities"]:
#         try:
#             total = conn.execute(f"SELECT COUNT(*) FROM {ft}").fetchone()[0]
#             nulls = conn.execute(f"SELECT COUNT(*) FROM {ft} WHERE data_value IS NULL OR TRIM(COALESCE(data_value,''))=''").fetchone()[0]
#             pct_null = round(100.0 * nulls / total, 2) if total else None
#             validation.append({"table":ft, "pct_null_data_value":pct_null})
#         except Exception:
#             validation.append({"table":ft, "pct_null_data_value":"error"})

#     for ft in ["fact_skills","fact_knowledge","fact_abilities"]:
#         try:
#             rows = conn.execute(f"""
#                 SELECT DISTINCT f.onet_soc_code FROM {ft} f
#                 LEFT JOIN dim_occupations d ON f.onet_soc_code = d.onet_soc_code
#                 WHERE d.onet_soc_code IS NULL LIMIT 5
#             """).fetchall()
#             missing = [r[0] for r in rows]
#             validation.append({"table":ft,"fk_missing_sample_onet": missing})
#         except Exception:
#             validation.append({"table":ft,"fk_missing_sample_onet":"error"})

#     import json
#     val_df = pd.DataFrame(validation)
#     val_df.to_csv(out_dir / "validation_report.csv", index=False)

#     # --- 9) Example SQL queries ---
#     def run_query(sql, name):
#         df = pd.read_sql_query(sql, conn)
#         df.to_csv(out_dir / name, index=False)
#         print("Saved", name, "rows:", len(df))
#         return df

#     q1 = """
#     SELECT o.onet_soc_code, o.title,
#            AVG(f.data_value) AS avg_skill_importance,
#            COUNT(*) AS skill_count
#     FROM fact_skills f
#     JOIN dim_occupations o ON f.onet_soc_code = o.onet_soc_code
#     WHERE f.scale_id LIKE '%IM%'
#     GROUP BY o.onet_soc_code, o.title
#     ORDER BY avg_skill_importance DESC
#     LIMIT 10;
#     """
#     q2 = """
#     SELECT d.element_name,
#        AVG(f.data_value) AS avg_knowledge_importance,
#        COUNT(*) AS occupation_count
#     FROM fact_knowledge f
#     JOIN dim_knowledge d ON f.element_id = d.element_id
#     WHERE f.scale_id LIKE '%IM%'
#     GROUP BY d.element_name
#     ORDER BY avg_knowledge_importance DESC
#     LIMIT 10;
#     """
#     q3 = """
#     SELECT d.element_name,
#        AVG(f.data_value) AS avg_ability_level,
#        COUNT(*) AS occupation_count
#     FROM fact_abilities f
#     JOIN dim_abilities d ON f.element_id = d.element_id
#     WHERE f.scale_id LIKE '%LV%'
#     GROUP BY d.element_name
#     ORDER BY avg_ability_level DESC
#     LIMIT 10;

#     """
#     run_query(q1, "top10_occupations_by_avg_skill_importance.csv")
#     run_query(q2, "top_skills_for_15-1252.00.csv")
#     run_query(q3, "avg_skill_by_soc_major.csv")

#     conn.close()
#     print("ETL complete. DB:", out_db, "Reports in:", out_dir)

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--data_dir", default="./db_30_0_excel", help="Path to folder containing O*NET Excel files")
#     parser.add_argument("--out_db", default="./onet_dw.db")
#     parser.add_argument("--out_dir", default="./output")
#     parser.add_argument("--sample_rows", type=int, default=None, help="If set, read only first N rows from each file (quick).")
#     args = parser.parse_args()
#     main(args.data_dir, args.out_db, args.out_dir, sample_rows=args.sample_rows)


import pandas as pd
import sqlite3
import os

# ---------- CONFIG ----------
INPUT_FILE = "db_30_0_excel/Occupation Data.xlsx"  # path to one of your Excel files
DB_FILE = "onet_dw.db"   # SQLite output DB

# ---------- CONNECT ----------
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# ---------- DROP OLD TABLES ----------
tables = ["dim_occupations", "dim_skills", "dim_knowledge", "dim_abilities",
          "dim_tasks", "fact_skills", "fact_knowledge", "fact_abilities", "fact_task_ratings"]

for t in tables:
    cur.execute(f"DROP TABLE IF EXISTS {t}")
conn.commit()

# ---------- CREATE SCHEMA ----------
cur.executescript("""
CREATE TABLE dim_occupations (
    onet_soc_code TEXT PRIMARY KEY,
    title TEXT,
    description TEXT
);

CREATE TABLE dim_skills (
    element_id TEXT PRIMARY KEY,
    element_name TEXT
);

CREATE TABLE dim_knowledge (
    element_id TEXT PRIMARY KEY,
    element_name TEXT
);

CREATE TABLE dim_abilities (
    element_id TEXT PRIMARY KEY,
    element_name TEXT
);

CREATE TABLE dim_tasks (
    task_id TEXT PRIMARY KEY,
    task TEXT,
    task_type TEXT,
    incumbents_responding INTEGER
);

CREATE TABLE fact_skills (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    onet_soc_code TEXT,
    element_id TEXT,
    scale_id TEXT,
    data_value REAL,
    n INTEGER,
    standard_error REAL,
    date TEXT,
    domain_source TEXT
);

CREATE TABLE fact_knowledge (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    onet_soc_code TEXT,
    element_id TEXT,
    scale_id TEXT,
    data_value REAL,
    n INTEGER,
    standard_error REAL,
    date TEXT,
    domain_source TEXT
);

CREATE TABLE fact_abilities (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    onet_soc_code TEXT,
    element_id TEXT,
    scale_id TEXT,
    data_value REAL,
    n INTEGER,
    standard_error REAL,
    date TEXT,
    domain_source TEXT
);

CREATE TABLE fact_task_ratings (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    onet_soc_code TEXT,
    task_id TEXT,
    scale_id TEXT,
    data_value REAL,
    n INTEGER,
    standard_error REAL,
    date TEXT,
    domain_source TEXT
);
""")
conn.commit()

print("✅ Schema created.")

# ---------- LOAD DIMENSIONS ----------
# Adjust file paths to your Excel files from the unzipped O*NET Excel package

# Occupations
occupations = pd.read_excel("db_30_0_excel/Occupation Data.xlsx")
occupations = occupations.rename(columns={
    "O*NET-SOC Code": "onet_soc_code",
    "Title": "title",
    "Description": "description"
})[["onet_soc_code", "title", "description"]]
occupations.to_sql("dim_occupations", conn, if_exists="append", index=False)
print(f"✅ dim_occupations loaded: {len(occupations)} rows")

# Skills (dimension only)
skills = pd.read_excel("db_30_0_excel/Skills.xlsx")
skills_dim = skills.rename(columns={
    "Element ID": "element_id",
    "Element Name": "element_name"
})[["element_id", "element_name"]].drop_duplicates()
skills_dim.to_sql("dim_skills", conn, if_exists="append", index=False)
print(f"✅ dim_skills loaded: {len(skills_dim)} rows")

# Knowledge
knowledge = pd.read_excel("db_30_0_excel/Knowledge.xlsx")
knowledge_dim = knowledge.rename(columns={
    "Element ID": "element_id",
    "Element Name": "element_name"
})[["element_id", "element_name"]].drop_duplicates()
knowledge_dim.to_sql("dim_knowledge", conn, if_exists="append", index=False)
print(f"✅ dim_knowledge loaded: {len(knowledge_dim)} rows")

# Abilities
abilities = pd.read_excel("db_30_0_excel/Abilities.xlsx")
abilities_dim = abilities.rename(columns={
    "Element ID": "element_id",
    "Element Name": "element_name"
})[["element_id", "element_name"]].drop_duplicates()
abilities_dim.to_sql("dim_abilities", conn, if_exists="append", index=False)
print(f"✅ dim_abilities loaded: {len(abilities_dim)} rows")

# Tasks
tasks = pd.read_excel("db_30_0_excel/Task Statements.xlsx")
tasks_dim = tasks.rename(columns={
    "Task ID": "task_id",
    "Task": "task",
    "Task Type": "task_type",
    "Incumbents Responding": "incumbents_responding"
})[["task_id", "task", "task_type", "incumbents_responding"]]
tasks_dim.to_sql("dim_tasks", conn, if_exists="append", index=False)
print(f"✅ dim_tasks loaded: {len(tasks_dim)} rows")

# ---------- LOAD FACTS ----------
def load_fact(file, table, is_task=False):
    df = pd.read_excel(file)
    df = df.rename(columns=lambda x: x.strip().lower())

    if is_task:
        df = df.rename(columns={"o*net-soc code": "onet_soc_code", "task id": "task_id"})
        df = df[["onet_soc_code", "task_id", "scale id", "data value", "n", "standard error", "date", "domain source"]]
        df.columns = ["onet_soc_code", "task_id", "scale_id", "data_value", "n", "standard_error", "date", "domain_source"]
    else:
        df = df.rename(columns={"o*net-soc code": "onet_soc_code", "element id": "element_id"})
        df = df[["onet_soc_code", "element_id", "scale id", "data value", "n", "standard error", "date", "domain source"]]
        df.columns = ["onet_soc_code", "element_id", "scale_id", "data_value", "n", "standard_error", "date", "domain_source"]

    df.to_sql(table, conn, if_exists="append", index=False)
    print(f"✅ {table} loaded: {len(df)} rows")

# Load facts
load_fact("db_30_0_excel/Skills.xlsx", "fact_skills")
load_fact("db_30_0_excel/Knowledge.xlsx", "fact_knowledge")
load_fact("db_30_0_excel/Abilities.xlsx", "fact_abilities")
load_fact("db_30_0_excel/Task Ratings.xlsx", "fact_task_ratings", is_task=True)

print("ETL complete. Data loaded into onet_dw.db")
conn.close()
