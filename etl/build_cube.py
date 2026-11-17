"""
ETL for the OLAP demo.

Generates a deterministic retail star schema with DuckDB, builds a
pre-aggregated OLAP cube (GROUP BY CUBE) and exports everything as
ZSTD-compressed Parquet files into site/data so the static front-end
(DuckDB-WASM) can query them directly from the browser.

Usage:
    python etl/build_cube.py [--rows 500000] [--out site/data]
"""
import argparse
import json
import pathlib
import time
from datetime import datetime, timezone

import duckdb


def build(rows: int, out: pathlib.Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    # Single thread + fixed seed => reproducible dataset on every CI run
    con.execute("SET threads = 1")
    con.execute("SELECT setseed(0.42)")

    t0 = time.time()

    # ---------- Dimensions ----------
    con.execute("""
        CREATE TABLE dim_date AS
        SELECT CAST(d AS DATE)            AS date_key,
               year(d)                    AS year,
               quarter(d)                 AS quarter,
               month(d)                   AS month,
               monthname(d)               AS month_name,
               dayname(d)                 AS day_name,
               isodow(d) IN (6, 7)        AS is_weekend
        FROM range(DATE '2023-01-01', DATE '2026-01-01', INTERVAL 1 DAY) t(d)
    """)

    con.execute("""
        CREATE TABLE dim_store AS
        SELECT * FROM (VALUES
            (1,  'Bogotá Centro',     'Bogotá',        'Colombia',  'Andean'),
            (2,  'Medellín Poblado',  'Medellín',      'Colombia',  'Andean'),
            (3,  'Quito Norte',       'Quito',         'Ecuador',   'Andean'),
            (4,  'Lima Miraflores',   'Lima',          'Peru',      'Andean'),
            (5,  'CDMX Reforma',      'Mexico City',   'Mexico',    'North'),
            (6,  'Monterrey Valle',   'Monterrey',     'Mexico',    'North'),
            (7,  'Guadalajara Sur',   'Guadalajara',   'Mexico',    'North'),
            (8,  'Santiago Las Condes','Santiago',     'Chile',     'Southern Cone'),
            (9,  'Buenos Aires Palermo','Buenos Aires','Argentina', 'Southern Cone'),
            (10, 'Montevideo Pocitos','Montevideo',    'Uruguay',   'Southern Cone'),
            (11, 'São Paulo Paulista','São Paulo',     'Brazil',    'Brazil'),
            (12, 'Rio Copacabana',    'Rio de Janeiro','Brazil',    'Brazil')
        ) t(store_id, store_name, city, country, region)
    """)

    con.execute("""
        CREATE TABLE dim_product AS
        SELECT i AS product_id,
               ['Electronics','Home','Sports','Books','Fashion'][1 + (i % 5)]       AS category,
               ['Andes','Pacifica','Aurora','Sierra','Nova'][1 + ((i // 5) % 5)]    AS brand,
               ['Electronics','Home','Sports','Books','Fashion'][1 + (i % 5)]
                   || ' item ' || lpad(CAST(i AS VARCHAR), 3, '0')                  AS product_name,
               round(5 + random() * 495, 2)                                         AS list_price
        FROM range(1, 51) t(i)
    """)

    # ---------- Fact table ----------
    con.execute(f"""
        CREATE TABLE fact_sales AS
        WITH base AS (
            SELECT i AS sale_id,
                   DATE '2023-01-01' + CAST(floor(random() * 1096) AS INTEGER)              AS date_key,
                   1 + CAST(floor(random() * 50) AS INTEGER)                                AS product_id,
                   1 + CAST(floor(random() * 12) AS INTEGER)                                AS store_id,
                   ['Online','In-store','Marketplace'][1 + CAST(floor(random() * 3) AS INTEGER)] AS channel,
                   1 + CAST(floor(random() * 5) AS INTEGER)                                 AS quantity,
                   round(random() * 0.30, 2)                                                AS discount
            FROM range({rows}) t(i)
        )
        SELECT b.sale_id, b.date_key, b.product_id, b.store_id, b.channel,
               b.quantity, b.discount,
               round(p.list_price * b.quantity * (1 - b.discount), 2) AS revenue,
               round(p.list_price * 0.60 * b.quantity, 2)              AS cost
        FROM base b JOIN dim_product p USING (product_id)
        ORDER BY b.date_key, b.store_id
    """)

    # ---------- Pre-aggregated OLAP cube (MOLAP-style) ----------
    con.execute("""
        CREATE TABLE agg_sales_cube AS
        SELECT d.year, d.quarter, s.region, p.category, f.channel,
               GROUPING(d.year, d.quarter, s.region, p.category, f.channel) AS grouping_id,
               count(*)                       AS orders,
               sum(f.quantity)                AS units,
               round(sum(f.revenue), 2)       AS revenue,
               round(sum(f.revenue - f.cost), 2) AS profit
        FROM fact_sales f
        JOIN dim_date d    USING (date_key)
        JOIN dim_store s   USING (store_id)
        JOIN dim_product p USING (product_id)
        GROUP BY CUBE (d.year, d.quarter, s.region, p.category, f.channel)
    """)

    tables = ["dim_date", "dim_store", "dim_product", "fact_sales", "agg_sales_cube"]
    manifest = {"built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "duckdb_version": duckdb.__version__, "tables": {}}
    for t in tables:
        path = out / f"{t}.parquet"
        con.execute(f"COPY {t} TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        manifest["tables"][t] = {"rows": n, "bytes": path.stat().st_size}

    manifest["build_seconds"] = round(time.time() - t0, 2)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--out", default="site/data")
    args = ap.parse_args()
    m = build(args.rows, pathlib.Path(args.out))
    print(json.dumps(m, indent=2))
