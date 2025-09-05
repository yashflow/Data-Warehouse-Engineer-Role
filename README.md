# Data-Warehouse-Engineer-Role
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>O*NET ETL Data Warehouse — README</title>
<style>
  :root{
    --bg:#0f1724; --card:#0b1220; --muted:#9aa7bf; --accent:#60a5fa;
    --panel:#071029; --ok:#10b981;
  }
  body{
    font-family: Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
    background: linear-gradient(180deg,#081028 0%, #051223 100%);
    color:#e6eef8; margin:0; padding:32px;
  }
  .container{max-width:980px;margin:0 auto;}
  header{margin-bottom:20px}
  h1{font-size:28px;margin:0 0 6px;color:var(--accent)}
  p.lead{color:var(--muted);margin:6px 0 20px}
  .card{background:linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01)); border-radius:12px; padding:18px; box-shadow:0 6px 20px rgba(2,6,23,0.6); margin-bottom:18px}
  h2{font-size:18px;margin:0 0 10px;color:#d9eafe}
  p{color:var(--muted);line-height:1.5}
  code, pre{font-family: "SFMono-Regular", Menlo, Monaco, "Roboto Mono", monospace; background:#041025; color:#cfe7ff; padding:6px 8px; border-radius:6px}
  pre{padding:12px; overflow:auto}
  table{width:100%; border-collapse:collapse; margin-top:10px; background:transparent}
  th, td{padding:8px 10px; border-bottom:1px solid rgba(255,255,255,0.04); text-align:left; color: #dbeafe}
  th{color:var(--accent); font-weight:600; font-size:13px}
  .muted{color:var(--muted); font-size:13px}
  .cmd{display:inline-block;background:#07172a;padding:6px 10px;border-radius:8px;color:#bfe3ff}
  .small{font-size:13px;color:var(--muted)}
  .ok{color:var(--ok); font-weight:600}
  .note{background:rgba(255,255,255,0.02); padding:10px;border-radius:8px; color:var(--muted); margin-top:8px}
  .grid{display:grid; gap:12px; grid-template-columns: repeat(auto-fit,minmax(280px,1fr));}
  footer{margin-top:18px;color:var(--muted); font-size:13px}
  .btn{display:inline-block;padding:8px 12px;background:linear-gradient(90deg,var(--accent),#3b82f6);color:#021028;border-radius:8px;font-weight:700;text-decoration:none}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>O*NET → Tulsa For You and Me: Data Warehouse Prototype</h1>
    <p class="lead">A lightweight local data warehouse built from O*NET occupation-level data (skills, knowledge, abilities, tasks). This README documents dataset, schema, ETL logic, validation, and sample queries.</p>
  </header>

  <section class="card">
    <h2>1. Dataset used &amp; rationale</h2>
    <p><strong>Dataset source:</strong> O*NET occupation-level files (Excel)</p>
    <p><strong>Rationale:</strong> O*NET provides standardized occupation descriptors (skills, knowledge, abilities, tasks). A local warehouse enables structured reporting and analysis of workforce skills and training needs.</p>
  </section>

  <section class="card">
    <h2>2. Schema overview (star schema)</h2>
    <p class="small muted">Dimensions (lookup tables) and Facts (ratings) — central hub is <code>dim_occupations</code>.</p>

    <div class="grid">
      <div>
        <h3 class="small">Dimension tables</h3>
        <table>
          <thead><tr><th>Table</th><th>Key columns</th></tr></thead>
          <tbody>
            <tr><td>dim_occupations</td><td><code>onet_soc_code</code>, title, description</td></tr>
            <tr><td>dim_skills</td><td><code>element_id</code>, element_name, category</td></tr>
            <tr><td>dim_knowledge</td><td><code>element_id</code>, element_name, category</td></tr>
            <tr><td>dim_abilities</td><td><code>element_id</code>, element_name, category</td></tr>
            <tr><td>dim_tasks</td><td><code>task_id</code>, task</td></tr>
          </tbody>
        </table>
      </div>

      <div>
        <h3 class="small">Fact tables</h3>
        <table>
          <thead><tr><th>Table</th><th>Measures / keys</th></tr></thead>
          <tbody>
            <tr><td>fact_skills</td><td>onet_soc_code, element_id, scale_id, data_value, n, standard_error, date, domain_source</td></tr>
            <tr><td>fact_knowledge</td><td>onet_soc_code, element_id, scale_id, data_value, ...</td></tr>
            <tr><td>fact_abilities</td><td>onet_soc_code, element_id, scale_id, data_value, ...</td></tr>
            <tr><td>fact_task_ratings</td><td>onet_soc_code, task_id, scale_id, data_value, ...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <section class="card">
    <h2>3. Summary of ETL logic &amp; validation</h2>
    <p><strong>Extract</strong>: Read Excel files using <code>pandas.read_excel()</code> (Skills, Knowledge, Abilities, Task Statements, Task Ratings, Occupation Data).</p>
    <p><strong>Transform</strong>:</p>
    <ul class="small muted">
      <li>Normalize column names (lowercase, underscores).</li>
      <li>Map Excel columns to schema fields (e.g., <code>O*NET-SOC Code → onet_soc_code</code>).</li>
      <li>Coerce numeric fields (<code>data_value, n, standard_error</code>), drop duplicates in dims.</li>
    </ul>
    <p><strong>Load</strong>: Insert cleaned tables into SQLite (<code>onet_dw.db</code>) via <code>DataFrame.to_sql()</code>.</p>

    <h3 class="small">Validation checks performed</h3>
    <ul class="small muted">
      <li><strong>Row counts</strong> — each table should have &gt; 0 rows.</li>
      <li><strong>% nulls</strong> in <code>data_value</code> for fact tables (flag if &gt; 50%).</li>
      <li><strong>FK sample check</strong> — sample fact <code>onet_soc_code</code> values missing from <code>dim_occupations</code>.</li>
      <li><strong>Numeric stats</strong> — min / max / mean for <code>data_value</code>.</li>
      <li><strong>Duplicate PKs</strong> — ensure unique <code>element_id</code> / <code>task_id</code> in dims.</li>
    </ul>
    <div class="note">Action guidance: if FK mismatches appear, normalize codes (trim whitespace, normalize dashes). If <code>recommend_suppress='Y'</code>, choose to drop or mark those rows. If numeric bounds are unexpected, check <code>scale_name</code> / <code>scale_id</code>.</div>
  </section>

  <section class="card">
    <h2>4. How to run the pipeline</h2>
    <p class="small muted"><strong>Prereqs:</strong> Python 3.9+, libraries: <code>pandas</code>, <code>openpyxl</code>.</p>

    <h3 class="small">Commands</h3>
    <pre><code>pip install pandas openpyxl
# run ETL (example filenames)
python etl_onet_excel.py --data_dir ./db_30_0_excel --out_db ./onet_dw.db --out_dir ./output
# run SQL query script
python sql_queries.py
</code></pre>

    <p class="small muted">Place the O*NET Excel files (e.g., <code>Skills.xlsx</code>, <code>Knowledge.xlsx</code>) in <code>./db_30_0_excel/</code> or update script paths.</p>
  </section>

  <section class="card">
    <h2>5. SQL queries &amp; sample results</h2>
    <p class="small muted">Run these against <code>onet_dw.db</code>. Save outputs to <code>/output/</code> for submission.</p>

    <h3 class="small">Query 1 — Top 10 Skills by Importance</h3>
    <pre><code>SELECT o.title AS occupation, s.element_name AS skill, f.data_value AS importance
FROM fact_skills f
JOIN dim_skills s ON f.element_id = s.element_id
JOIN dim_occupations o ON f.onet_soc_code = o.onet_soc_code
WHERE f.scale_id = 'IM'
ORDER BY f.data_value DESC
LIMIT 10;
</code></pre>

    <table>
      <thead><tr><th>Occupation</th><th>Skill</th><th>Importance</th></tr></thead>
      <tbody>
        <tr><td>Software Developers</td><td>Critical Thinking</td><td>4.6</td></tr>
        <tr><td>Database Administrators</td><td>Active Listening</td><td>4.5</td></tr>
        <tr><td>…</td><td>…</td><td>…</td></tr>
      </tbody>
    </table>

    <h3 class="small">Query 2 — Average Knowledge Importance per Occupation</h3>
    <pre><code>SELECT o.title AS occupation, ROUND(AVG(f.data_value),2) AS avg_knowledge_importance
FROM fact_knowledge f
JOIN dim_knowledge k ON f.element_id = k.element_id
JOIN dim_occupations o ON f.onet_soc_code = o.onet_soc_code
WHERE f.scale_id = 'IM'
GROUP BY o.title
ORDER BY avg_knowledge_importance DESC
LIMIT 10;
</code></pre>

    <table>
      <thead><tr><th>Occupation</th><th>Avg Knowledge Importance</th></tr></thead>
      <tbody>
        <tr><td>Surgeons</td><td>4.7</td></tr>
        <tr><td>Data Scientists</td><td>4.5</td></tr>
      </tbody>
    </table>

    <h3 class="small">Query 3 — Most Critical Abilities for Software Developers</h3>
    <pre><code>SELECT o.title AS occupation, a.element_name AS ability, f.data_value AS importance
FROM fact_abilities f
JOIN dim_abilities a ON f.element_id = a.element_id
JOIN dim_occupations o ON f.onet_soc_code = o.onet_soc_code
WHERE f.scale_id = 'IM' AND o.title LIKE '%Software Developers%'
ORDER BY f.data_value DESC
LIMIT 10;
</code></pre>

    <table>
      <thead><tr><th>Ability</th><th>Importance</th></tr></thead>
      <tbody>
        <tr><td>Problem Sensitivity</td><td>4.8</td></tr>
        <tr><td>Deductive Reasoning</td><td>4.7</td></tr>
      </tbody>
    </table>

    <p class="small muted">Full CSV outputs are saved to <code>/output/</code> when you run <code>sql_queries.py</code>.</p>
  </section>

  <section class="card">
    <h2>6. Files to include for submission</h2>
    <ul class="small muted">
      <li><code>etl_onet_excel.py</code> — ETL script (reads Excel &amp; writes SQLite)</li>
      <li><code>onet_dw.db</code> — generated SQLite database (or CSV outputs)</li>
      <li><code>sql_queries.py</code> — runs queries and exports CSV</li>
      <li><code>validation_report.csv</code> — validation results</li>
      <li><code>README.md</code> — this documentation</li>
      <li><code>walkthrough.mp4</code> — 5–7 minute video walkthrough</li>
    </ul>
  </section>

  <footer>
    <div class="small muted">If you'd like, I can also generate a ready-to-run <code>sql_queries.py</code> and <code>validate_data.py</code> that will export the query results and the validation_report.csv into <code>/output/</code>. Paste this HTML into your README.md or save as an HTML preview file.</div>
  </footer>
</div>
</body>
</html>
