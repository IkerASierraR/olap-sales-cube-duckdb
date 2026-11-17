# OLAP Sales Cube — DuckDB + DuckDB-WASM, serverless

A complete **Online Analytical Processing (OLAP)** solution that costs $0 to run:

* **ETL / cube build:** Python + [DuckDB](https://duckdb.org) generate a retail **star schema**
  (500K-row fact table + date/store/product dimensions) and a pre-aggregated
  **`GROUP BY CUBE`** table, exported as ZSTD Parquet.
* **Query engine:** [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview) runs inside the browser,
  so slice, dice, drill-down, roll-up and pivot queries run on the client in milliseconds.
* **Automation:** GitHub Actions rebuilds the cube, runs data-quality tests and deploys on every push
  (plus a weekly scheduled rebuild).
* **Hosting:** GitHub Pages (public SaaS). **No AWS.**

**Live demo:** https://ikerasierrar.github.io/olap-sales-cube-duckdb/

**Article (dev.to):** https://dev.to/ikers/building-a-serverless-olap-cube-with-duckdb-duckdb-wasm-and-github-actions-no-aws-0-4ogj

## Architecture

```
 push to main ──► GitHub Actions ──────────────────────────────► GitHub Pages
                  │ 1. pip install duckdb                          │ index.html
                  │ 2. etl/build_cube.py  (star schema + CUBE)     │ data/*.parquet
                  │ 3. pytest (data-quality checks)                ▼
                  └ 4. upload-pages-artifact / deploy-pages     Browser: DuckDB-WASM
                                                                 (OLAP queries run locally)
```

### Star schema

| Table            | Grain                          | Rows    |
|------------------|--------------------------------|---------|
| `fact_sales`     | one sale line                  | 500,000 |
| `dim_date`       | day (2023-01-01 → 2025-12-31)  | 1,096   |
| `dim_store`      | store (12 LATAM cities)        | 12      |
| `dim_product`    | product (5 categories×5 brands)| 50      |
| `agg_sales_cube` | `CUBE(year, quarter, region, category, channel)` | 2,400 |

Hierarchies used for drill-down / roll-up:
`Year → Quarter → Month`, `Region → Country → Store`, `Category → Brand → Product`.

## Run locally

```bash
pip install -r requirements.txt
python etl/build_cube.py          # writes site/data/*.parquet
pytest -q                         # data-quality tests
python -m http.server -d site 8000
# open http://localhost:8000
```

## Deploy your own copy

1. Fork this repository.
2. In **Settings → Pages**, set *Source* to **GitHub Actions**.
3. Push to `main` (or run the workflow manually). Done.

## License

MIT
