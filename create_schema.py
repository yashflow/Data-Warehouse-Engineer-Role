import sqlite3

# Connect (creates file if it doesn’t exist)
conn = sqlite3.connect("onet_dw.db")
cursor = conn.cursor()

# Drop old tables (safe re-run)
tables = ["fact_task_ratings", "fact_abilities", "fact_knowledge", "fact_skills",
          "dim_tasks", "dim_abilities", "dim_knowledge", "dim_skills", "dim_occupations"]
for t in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {t}")

# Create schema
schema_sql = """
-- Dimension Tables
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

-- Fact Tables
CREATE TABLE fact_skills (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    onet_soc_code TEXT,
    element_id TEXT,
    scale_id TEXT,
    data_value REAL,
    n INTEGER,
    standard_error REAL,
    date TEXT,
    domain_source TEXT,
    FOREIGN KEY (onet_soc_code) REFERENCES dim_occupations(onet_soc_code),
    FOREIGN KEY (element_id) REFERENCES dim_skills(element_id)
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
    domain_source TEXT,
    FOREIGN KEY (onet_soc_code) REFERENCES dim_occupations(onet_soc_code),
    FOREIGN KEY (element_id) REFERENCES dim_knowledge(element_id)
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
    domain_source TEXT,
    FOREIGN KEY (onet_soc_code) REFERENCES dim_occupations(onet_soc_code),
    FOREIGN KEY (element_id) REFERENCES dim_abilities(element_id)
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
    domain_source TEXT,
    FOREIGN KEY (onet_soc_code) REFERENCES dim_occupations(onet_soc_code),
    FOREIGN KEY (task_id) REFERENCES dim_tasks(task_id)
);
"""

cursor.executescript(schema_sql)

conn.commit()
conn.close()

print("✅ Schema created in onet_dw.db")
