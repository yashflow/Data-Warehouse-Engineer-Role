


import pandas as pd
import sqlite3
import os

# ---------- CONFIG ----------
INPUT_FILE = "db_30_0_excel/Occupation Data.xlsx"  
DB_FILE = "onet_dw.db"   

# ---------- CONNECT ----------
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()


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
