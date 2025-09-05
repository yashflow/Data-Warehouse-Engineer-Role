
# import sqlite3
# import pandas as pd

# conn = sqlite3.connect("onet_dw.db")

# print(pd.read_sql("SELECT COUNT(*) FROM dim_occupations", conn))
# print(pd.read_sql("SELECT COUNT(*) FROM fact_skills", conn))
# print(pd.read_sql("SELECT * FROM fact_skills LIMIT 5", conn))

# conn.close()



import sqlite3
import pandas as pd

# --- CONFIG ---
DB_PATH = "onet_dw.db"
REPORT_PATH = "validation_report.csv"

# --- CONNECT ---
conn = sqlite3.connect(DB_PATH)

report_rows = []

def add_report(table, check, result):
    report_rows.append({"table": table, "check": check, "result": result})

# --- 1. Row counts ---
tables = [
    "dim_occupations", "dim_skills", "dim_knowledge", "dim_abilities", "dim_tasks",
    "fact_skills", "fact_knowledge", "fact_abilities", "fact_task_ratings"
]

for tbl in tables:
    try:
        cnt = pd.read_sql_query(f"SELECT COUNT(*) AS c FROM {tbl}", conn)["c"].iloc[0]
        add_report(tbl, "row_count", cnt)
    except Exception as e:
        add_report(tbl, "row_count", f"ERROR: {e}")

# --- 2. % nulls in data_value for fact tables ---
fact_tables = ["fact_skills", "fact_knowledge", "fact_abilities", "fact_task_ratings"]

for tbl in fact_tables:
    try:
        q = f"""
        SELECT 
            SUM(CASE WHEN data_value IS NULL THEN 1 ELSE 0 END)*1.0/COUNT(*)*100 AS pct_null
        FROM {tbl}
        """
        pct_null = pd.read_sql_query(q, conn)["pct_null"].iloc[0]
        add_report(tbl, "% nulls in data_value", round(pct_null, 2))
    except Exception as e:
        add_report(tbl, "% nulls in data_value", f"ERROR: {e}")

# --- 3. FK sample checks (fact vs dim_occupations) ---
for tbl in fact_tables:
    try:
        q = f"""
        SELECT DISTINCT f.onet_soc_code
        FROM {tbl} f
        LEFT JOIN dim_occupations d ON f.onet_soc_code = d.onet_soc_code
        WHERE d.onet_soc_code IS NULL
        LIMIT 5
        """
        missing = pd.read_sql_query(q, conn)
        if missing.empty:
            add_report(tbl, "FK check (onet_soc_code)", "OK")
        else:
            add_report(tbl, "FK check (onet_soc_code)", f"MISMATCH: {missing['onet_soc_code'].tolist()}")
    except Exception as e:
        add_report(tbl, "FK check (onet_soc_code)", f"ERROR: {e}")

# --- 4. Numeric stats (min/max/mean for data_value) ---
for tbl in fact_tables:
    try:
        q = f"""
        SELECT MIN(data_value) as min_val,
               MAX(data_value) as max_val,
               AVG(data_value) as mean_val
        FROM {tbl}
        """
        stats = pd.read_sql_query(q, conn).iloc[0].to_dict()
        add_report(tbl, "numeric_stats", stats)
    except Exception as e:
        add_report(tbl, "numeric_stats", f"ERROR: {e}")

# --- 5. Duplicate PK checks in dimensions ---
pk_dims = {
    "dim_skills": "element_id",
    "dim_knowledge": "element_id",
    "dim_abilities": "element_id",
    "dim_tasks": "task_id",
    "dim_occupations": "onet_soc_code"
}

for tbl, pk in pk_dims.items():
    try:
        q = f"""
        SELECT {pk}, COUNT(*) as cnt
        FROM {tbl}
        GROUP BY {pk}
        HAVING cnt > 1
        LIMIT 5
        """
        dups = pd.read_sql_query(q, conn)
        if dups.empty:
            add_report(tbl, f"duplicate_{pk}", "OK")
        else:
            add_report(tbl, f"duplicate_{pk}", f"DUPLICATES: {dups[pk].tolist()}")
    except Exception as e:
        add_report(tbl, f"duplicate_{pk}", f"ERROR: {e}")

# --- SAVE REPORT ---
df_report = pd.DataFrame(report_rows)
df_report.to_csv(REPORT_PATH, index=False)
print(f"Validation report written to {REPORT_PATH}")
