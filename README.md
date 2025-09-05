# Data-Warehouse-Engineer-Role

##  O*NET → Tulsa For You and Me: Data Warehouse Prototype
A lightweight local data warehouse built from O*NET occupation-level data (skills, knowledge, abilities, tasks).  
This README documents dataset, schema, ETL logic, validation, and sample queries.

---

## 1. Dataset Used & Rationale
- **Dataset Source:** O*NET occupation-level Excel files  
- **Rationale:** O*NET provides standardized occupation descriptors (skills, knowledge, abilities, tasks).  
  A local warehouse enables structured reporting and analysis of workforce skills and training needs.  

---

## 2. Schema Overview (Star Schema)

###  Dimension Tables
<table>
<thead><tr><th>Table</th><th>Key Columns</th></tr></thead>
<tbody>
<tr><td>dim_occupations</td><td><code>onet_soc_code</code>, title, description</td></tr>
<tr><td>dim_skills</td><td><code>element_id</code>, element_name, category</td></tr>
<tr><td>dim_knowledge</td><td><code>element_id</code>, element_name, category</td></tr>
<tr><td>dim_abilities</td><td><code>element_id</code>, element_name, category</td></tr>
<tr><td>dim_tasks</td><td><code>task_id</code>, task</td></tr>
</tbody>
</table>

###  Fact Tables
<table>
<thead><tr><th>Table</th><th>Measures / Keys</th></tr></thead>
<tbody>
<tr><td>fact_skills</td><td>onet_soc_code, element_id, scale_id, data_value, n, standard_error, date, domain_source</td></tr>
<tr><td>fact_knowledge</td><td>onet_soc_code, element_id, scale_id, data_value, ...</td></tr>
<tr><td>fact_abilities</td><td>onet_soc_code, element_id, scale_id, data_value, ...</td></tr>
<tr><td>fact_task_ratings</td><td>onet_soc_code, task_id, scale_id, data_value, ...</td></tr>
</tbody>
</table>

---

## 3. ETL Logic & Validation

###  ETL Process
- **Extract** → Read Excel files using `pandas.read_excel()`  
- **Transform** → Normalize column names, map fields to schema, coerce numeric values, deduplicate dims  
- **Load** → Write cleaned data into SQLite (`onet_dw.db`) using `DataFrame.to_sql()`  

###  Validation Checks
-  Row counts — all tables must have > 0 rows  
-  % nulls in `data_value` — flag if > 50%  
-  Foreign Key check — fact `onet_soc_code` must exist in `dim_occupations`  
-  Numeric statistics — min / max / mean for fact tables  
-  Duplicate PKs — ensure unique `element_id` / `task_id`  

 **Action if failures appear:**
- FK mismatches → strip spaces / normalize codes  
- Suppressed rows (`recommend_suppress='Y'`) → drop or mark  
- Unexpected numeric ranges → check `scale_id` / `scale_name`  

---

## 4. How to Run the Pipeline

###  Prerequisites
- Python **3.9+**  
- Libraries: `pandas`, `openpyxl`  

###  Commands
```bash
pip install pandas openpyxl

# Run ETL
python etl_onet_excel.py --data_dir ./db_30_0_excel --out_db ./onet_dw.db --out_dir ./output

# Run Queries
python sql_queries.py
