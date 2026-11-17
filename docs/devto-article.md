---
title: Building a Serverless OLAP Cube with DuckDB, DuckDB-WASM and GitHub Actions (no AWS, $0)
published: true
description: A star schema, a GROUP BY CUBE and slice/dice/drill-down in the browser, built and deployed automatically to GitHub Pages.
tags: olap, duckdb, dataengineering, githubactions
---

**OLAP (Online Analytical Processing)** is the kind of workload behind dashboards and BI tools: you aggregate large fact tables along *dimensions* (time, geography, product, …) and then **slice, dice, drill down, roll up and pivot** to explore the numbers.

People usually reach for a warehouse like Snowflake, ClickHouse, Druid or Pinot for this, which means a cluster, a bill and some ops. For small and medium datasets there is a much lighter option: **DuckDB**, an in-process columnar OLAP database that also compiles to **WebAssembly**, so it can run inside the browser.

In this post I build a complete OLAP solution that:

1. Lives in a **public GitHub repository**.
2. Is **built, tested and deployed automatically** with GitHub Actions.
3. Is **published on a public SaaS** (GitHub Pages). No AWS, no servers, $0.

🔗 **Live demo:** https://ikerasierrar.github.io/olap-sales-cube-duckdb/
📦 **Repository:** https://github.com/IkerASierraR/olap-sales-cube-duckdb

---

## Architecture

```
 git push ─► GitHub Actions ───────────────────────────────► GitHub Pages (static)
              1. pip install duckdb                            index.html
              2. etl/build_cube.py  → star schema + CUBE        data/*.parquet (ZSTD)
              3. pytest  → data-quality gates                         │
              4. upload-pages-artifact + deploy-pages                 ▼
                                                          Browser runs DuckDB-WASM:
                                                          slice · dice · drill · pivot
```

The idea: **compute in CI, serve static files, query on the client.** The heavy lifting (generating the star schema and pre-aggregating the cube) runs in a GitHub Actions runner. The output is plain Parquet files. The browser downloads them and DuckDB-WASM answers every OLAP query locally in milliseconds.

## 1. The data model: a star schema

The dataset is a synthetic retail business across 12 LATAM stores, 50 products and three sales channels, covering 2023–2025.

| Table            | Grain                         | Rows    |
|------------------|-------------------------------|---------|
| `fact_sales`     | one sale line                 | 500,000 |
| `dim_date`       | day                           | 1,096   |
| `dim_store`      | store → country → region      | 12      |
| `dim_product`    | product → brand → category    | 50      |
| `agg_sales_cube` | `CUBE(year, quarter, region, category, channel)` | 2,400 |

Each dimension carries a **hierarchy**, which is what makes drill-down and roll-up possible:

- **Time:** Year → Quarter → Month
- **Geography:** Region → Country → Store
- **Product:** Category → Brand → Product

## 2. ETL with DuckDB (Python)

`etl/build_cube.py` creates everything with plain SQL. A fixed seed and a single thread make the dataset reproducible on every CI run:

```python
con = duckdb.connect()
con.execute("SET threads = 1")
con.execute("SELECT setseed(0.42)")

con.execute(f"""
    CREATE TABLE fact_sales AS
    WITH base AS (
        SELECT i AS sale_id,
               DATE '2023-01-01' + CAST(floor(random() * 1096) AS INTEGER) AS date_key,
               1 + CAST(floor(random() * 50) AS INTEGER) AS product_id,
               1 + CAST(floor(random() * 12) AS INTEGER) AS store_id,
               ['Online','In-store','Marketplace'][1 + CAST(floor(random()*3) AS INTEGER)] AS channel,
               1 + CAST(floor(random() * 5) AS INTEGER) AS quantity,
               round(random() * 0.30, 2) AS discount
        FROM range({rows}) t(i)
    )
    SELECT b.*, round(p.list_price * b.quantity * (1 - b.discount), 2) AS revenue,
                round(p.list_price * 0.60 * b.quantity, 2)              AS cost
    FROM base b JOIN dim_product p USING (product_id)
""")
```

### The OLAP cube: `GROUP BY CUBE`

This is the MOLAP-style part. `CUBE` computes the aggregates for **every combination** of the listed dimensions (2⁵ = 32 grouping sets) in one pass. `GROUPING()` returns a bitmask that tells you which dimensions were rolled up in each row:

```sql
CREATE TABLE agg_sales_cube AS
SELECT d.year, d.quarter, s.region, p.category, f.channel,
       GROUPING(d.year, d.quarter, s.region, p.category, f.channel) AS grouping_id,
       count(*) AS orders, sum(f.quantity) AS units,
       round(sum(f.revenue), 2) AS revenue,
       round(sum(f.revenue - f.cost), 2) AS profit
FROM fact_sales f
JOIN dim_date d USING (date_key)
JOIN dim_store s USING (store_id)
JOIN dim_product p USING (product_id)
GROUP BY CUBE (d.year, d.quarter, s.region, p.category, f.channel);
```

`grouping_id = 31` is the grand total, `30` is "by channel only", `11` is "year × region", and so on.

Every table is then exported as ZSTD-compressed Parquet. The 500K-row fact table ends up at about **5 MB**, and the whole build takes around **1.3 seconds**:

```python
con.execute(f"COPY {t} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
```

## 3. Data-quality gates (pytest)

Nothing gets deployed if the data is wrong. DuckDB can query Parquet files directly, so the tests stay short:

```python
def test_cube_grand_total_matches_fact():
    fact = q("SELECT round(sum(revenue), 2) FROM '$D/fact_sales.parquet'")[0][0]
    cube = q("SELECT revenue FROM '$D/agg_sales_cube.parquet' WHERE grouping_id = 31")[0][0]
    assert abs(fact - cube) < 1.0

def test_no_orphan_keys():   # referential integrity of the star schema
    ...
```

## 4. OLAP in the browser with DuckDB-WASM

The front end is a single `index.html` with no build step. It boots DuckDB-WASM from a CDN, registers the Parquet files over HTTP and creates a denormalized `sales` table:

```js
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
const workerUrl = URL.createObjectURL(
  new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), new Worker(workerUrl));
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
const conn = await db.connect();

await db.registerFileURL("fact_sales.parquet", url, duckdb.DuckDBDataProtocol.HTTP, false);
await conn.query(`CREATE TABLE fact_sales AS SELECT * FROM read_parquet('fact_sales.parquet')`);
```

On top of that, a small metadata layer describes **dimensions, hierarchies and measures**, and the UI generates SQL from it:

```js
const DIMS = {
  year:    { expr: "CAST(year AS VARCHAR)",       next: "quarter" },
  quarter: { expr: "year || '-Q' || quarter",     next: "month"   },
  region:  { expr: "region",                      next: "country" },
  // ...
};
const MEASURES = {
  revenue: { expr: "sum(revenue)" },
  margin:  { expr: "100 * sum(revenue - cost) / sum(revenue)" },
  // ...
};
```

That covers the classic OLAP operations:

| Operation      | How it works in the app |
|----------------|-------------------------|
| **Slice**      | Pick a single year → `WHERE year = 2024` |
| **Dice**       | Combine year + channel + drill filters |
| **Drill-down** | Click a row (e.g. *Andean*) → adds a filter and moves to the next level (*Country*) |
| **Roll-up**    | Remove a filter chip → goes back up the hierarchy |
| **Pivot**      | Choose a *Columns* dimension → rows × columns cross-tab |

The generated SQL is shown under the table, and there's a **SQL console** for running your own queries, for example a `ROLLUP` with subtotals:

```sql
SELECT coalesce(region, '(all)') AS region, coalesce(country, '(all)') AS country,
       round(sum(revenue), 0) AS revenue
FROM sales
GROUP BY ROLLUP (region, country)
ORDER BY region NULLS LAST, country NULLS LAST;
```

Aggregations over all 500K rows come back in tens of milliseconds, entirely in the browser.

## 5. Deploy through automation: GitHub Actions → GitHub Pages

`.github/workflows/deploy.yml` runs on every push to `main`, on demand, and **every Monday** so the cube gets refreshed:

```yaml
on:
  push: { branches: [main] }
  schedule: [{ cron: "0 6 * * 1" }]
  workflow_dispatch:

permissions: { contents: read, pages: write, id-token: write }

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12", cache: pip }
      - run: pip install -r requirements.txt
      - run: python etl/build_cube.py --rows 500000 --out site/data
      - run: pytest -q
      - uses: actions/configure-pages@v5
      - uses: actions/upload-pages-artifact@v3
        with: { path: site }
  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment: github-pages
    steps:
      - id: deployment
        uses: actions/deploy-pages@v4
```

The generated Parquet files are **not committed**. They are build artifacts, so the repo only holds code. To deploy your own copy: fork the repo, go to **Settings → Pages → Source: GitHub Actions**, and push.

## When does this approach make sense?

✅ Good fit:
- Datasets from MBs up to a few hundred MB (Parquet)
- Public or internal dashboards that refresh on a schedule
- Demos, teaching OLAP concepts, embedded analytics, prototypes

⚠️ Use a server-side engine (ClickHouse, Druid, Pinot, Snowflake, MotherDuck) when:
- Data is in the GBs/TBs or needs real-time ingestion
- Rows must stay private (anything served statically is downloadable)
- You need many concurrent users querying fresh data

## Conclusion

With **DuckDB in CI** and **DuckDB-WASM in the browser** you get a real OLAP stack: star schema, `CUBE`/`ROLLUP`, slice/dice/drill/pivot. It is reproducible, tested, deployed automatically, and it costs nothing to run.

Try the [live demo](https://ikerasierrar.github.io/olap-sales-cube-duckdb/), read the [code](https://github.com/IkerASierraR/olap-sales-cube-duckdb), and feel free to fork it. ⭐
