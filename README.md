```markdown
# Retail Marketing Lakehouse (Databricks + ADLS + PySpark + dbx)

An end-to-end, production-style project implementing a Lakehouse with Bronze/Silver/Gold layers, SCD2 customer dimension, and marketing KPIs. Ready to clone and run.

## Stack
- Azure Databricks, ADLS Gen2, Delta Lake, PySpark, SQL, dbx

## Quickstart
1. Run `sql/00_catalogs_schemas.sql` ... `sql/30_gold_tables.sql`.
2. Generate sample data: `python tools/generate_sample_data.py`.
3. `dbx deploy --environment dev` then launch the three jobs in order.
4. Explore: `SELECT * FROM retail.gold.mart_campaign_kpis;`

## Project Layout
(see repo tree in this README)

## Highlights
- Auto Loader ingestion to Bronze
- SCD2 `dim_customer_scd2` via Delta MERGE
- Gold star schema + campaign KPIs
- CI with GitHub Actions, basic tests

## Notes
- Replace `<storageacct>` and paths for your environment.
- Unity Catalog recommended; Hive metastore compatible with minor name changes.
```
