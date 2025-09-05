import sqlite3
import pandas as pd

conn = sqlite3.connect("onet_dw.db")

query = """ SELECT 
    o.title AS occupation,
    s.element_name AS skill,
    f.data_value AS importance
FROM fact_skills f
JOIN dim_skills s ON f.element_id = s.element_id
JOIN dim_occupations o ON f.onet_soc_code = o.onet_soc_code
WHERE f.scale_id = 'IM' -- IM = Importance
ORDER BY f.data_value DESC
LIMIT 10;
"""
"""
SELECT 
    o.title AS occupation,
    ROUND(AVG(f.data_value),2) AS avg_knowledge_importance
FROM fact_knowledge f
JOIN dim_knowledge k ON f.element_id = k.element_id
JOIN dim_occupations o ON f.onet_soc_code = o.onet_soc_code
WHERE f.scale_id = 'IM' -- Importance
GROUP BY o.title
ORDER BY avg_knowledge_importance DESC
LIMIT 10;
"""
"""

SELECT 
    o.title AS occupation,
    a.element_name AS ability,
    f.data_value AS importance
FROM fact_abilities f
JOIN dim_abilities a ON f.element_id = a.element_id
JOIN dim_occupations o ON f.onet_soc_code = o.onet_soc_code
WHERE f.scale_id = 'IM'
  AND o.title LIKE '%Software Developers%'
ORDER BY f.data_value DESC
LIMIT 10;

 """  


df = pd.read_sql(query, conn)
print(df)

conn.close()



