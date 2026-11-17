"""Data-quality checks run in CI before deploying."""
import json
import pathlib

import duckdb

DATA = pathlib.Path(__file__).resolve().parents[1] / "site" / "data"


def q(sql):
    return duckdb.sql(sql.replace("$D", DATA.as_posix())).fetchall()


def test_manifest_exists():
    m = json.loads((DATA / "manifest.json").read_text(encoding="utf-8"))
    assert m["tables"]["fact_sales"]["rows"] > 0


def test_no_orphan_keys():
    orphans = q("""
        SELECT count(*) FROM '$D/fact_sales.parquet' f
        LEFT JOIN '$D/dim_product.parquet' p USING (product_id)
        LEFT JOIN '$D/dim_store.parquet'   s USING (store_id)
        LEFT JOIN '$D/dim_date.parquet'    d USING (date_key)
        WHERE p.product_id IS NULL OR s.store_id IS NULL OR d.date_key IS NULL
    """)[0][0]
    assert orphans == 0


def test_cube_grand_total_matches_fact():
    fact = q("SELECT round(sum(revenue), 2) FROM '$D/fact_sales.parquet'")[0][0]
    cube = q("SELECT revenue FROM '$D/agg_sales_cube.parquet' WHERE grouping_id = 31")[0][0]
    assert abs(fact - cube) < 1.0


def test_measures_are_positive():
    bad = q("SELECT count(*) FROM '$D/fact_sales.parquet' WHERE revenue <= 0 OR quantity <= 0")[0][0]
    assert bad == 0
