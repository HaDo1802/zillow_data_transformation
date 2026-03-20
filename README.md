# Zillow Real Estate Analytics (dbt + Supabase)

Analytics engineering project for Zillow listing history using a bronze-silver-gold architecture, dbt Core, and GitHub Actions CI/CD.

![Tech Stack](data_model_material/tech_stack.png)

## What This Project Demonstrates

- Contract-driven modeling: raw ingestion contract is explicit and versioned in SQL + loader code.
- Grain-first data modeling: each model has a clear business grain before metrics are added.
- Idempotent ingestion and transformation: duplicate-safe raw loads and deterministic downstream builds.
- State-aware deployment: only changed dbt graph nodes are promoted to production.
- Operational discipline: CI isolation schemas, automated cleanup, and scheduled incremental refresh.

## Repository Layout

```text
real_estate_transformation/
├── scripts/
│   ├── python/
│   │   └── load_bronze.py      # 2 Load files into raw table
│   └── sql/
│       └── bronze_setup.sql    # 1. Use it for your raw table set up first
├── zillow_transformation/      # 3. All transformation models using dbt
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/
│   │   │   ├── staging/stg_zillow_property_master.sql
│   │   │   ├── intermediate/int_zillow_property_history.sql
│   │   │   ├── intermediate/int_zillow_property_latest.sql
│   │   │   └── schema.yml
│   │   └── gold/
│   │       ├── dimensions/dim_property.sql
│   │       ├── dimensions/dim_date.sql
│   │       ├── facts/fact_property_snapshot.sql
│   │       ├── marts/mart_market_summary.sql
│   │       ├── marts/mart_property_current.sql
│   │       └── schema.yml
└── .github/workflows/
    ├── ci.yml
    └── cd.yml
```

## Data Architecture and Modeling Principles

## 1) Bronze (Raw Contract)

Raw table: `raw.raw_property_master_data`  
Setup: `scripts/sql/bronze_setup.sql`

Design choices:
- Keep raw fields as text to preserve source fidelity and avoid early coercion errors.
- Derive `snapshot_date` at ingest time from `extracted_at` (day-level, `YYYY-MM-DD`) in `load_bronze.py`.
- Enforce idempotency with a business-level uniqueness rule:
  - `unique (zpid, snapshot_date, price, listingstatus)`
- Loader uses `ON CONFLICT ON CONSTRAINT raw_property_master_data_uk DO NOTHING`.

This gives safe re-runs, backfills, and resilience to duplicated file deliveries.

## 2) Silver (Standardization + History)

Silver models standardize schema, cast types, and apply light business normalization.

- `stg_zillow_property_master`: source-aligned typed staging contract.
- `int_zillow_property_history`: historical continuity at event-level grain.
- `int_zillow_property_latest`: latest-record projection per property.

Principle: silver is where technical quality is enforced; gold should not re-clean dirty source behavior.

## 3) Gold (Business Serving Layer)

Gold models provide analytics-ready dimensions, facts, and marts.

- Dimensions: `dim_property`, `dim_date`
- Fact: `fact_property_snapshot` (time-aware metrics)
- Marts: `mart_market_summary`, `mart_property_current`

Principle: conformed dimensions + stable fact grain for repeatable BI semantics.

![Star Schema](data_model_material/Star_schema.png)

## Modeling Brainstorm and Design Decisions

This is the decision path used to shape the gold model, not just the final SQL structure.

1. Business process first
- Core process: track how each listing changes over time (price, status, activity).
- Modeling implication: prioritize time-variant fact design over a single current-state table. Even though the api data is not providing ideal data for time-series analysis, mainly due to my small volume of free quote, I still want to mimic the real-world production-grade handling strategy. 

2. Grain before columns
- Chosen fact grain: one row per `property_id` per `snapshot_date`.
- Why: this supports trend analysis, avoids accidental double counting, and makes testable uniqueness explicit.

3. Dimension vs fact boundary
- Dimensions store descriptive context with lower volatility (`dim_property`, `dim_date`). I chose SCD1 for dim_propery because the I wanted to focus on the point-in-time pricing analysis over the time-series analysis now.
- Facts store measurable and changing values (`fact_property_snapshot`).
- Why: keeps joins stable and avoids repeatedly storing high-cardinality descriptive text in fact tables. I am thinking of "what-if" situation when my data is scaling to millions.

4. Keep both history and current-serving marts
- Historical truth: `fact_property_snapshot`.
- Consumer convenience: latest-serving marts (`mart_property_current`) and aggregate marts (`mart_market_summary`).
- Why: BI users need both point-in-time analysis and fast current-state dashboards.


## Tagging Strategy (Execution Semantics)

Tags area added for better control. All models have at least 2 tags: frequency and layer

- Silver folders default to: `["daily", "silver"]`
- Gold dimensions: `["one-time", "gold", "dimension"]`
- Gold facts: `["daily", "gold", "fact"]`
- Gold marts: `["daily", "gold", "mart"]`

This enables precise selectors like:
- `dbt run --select tag:daily`
- `dbt run --select tag:one-time`

- Silver folders default to: `["daily", "silver"]`
- Gold dimensions: `["one-time", "gold", "dimension"]`
- Gold facts: `["daily", "gold", "fact"]`
- Gold marts: `["daily", "gold", "mart"]`

Model-level tags are also explicitly declared in `schema.yml` for readability and governance.

This supports clean selectors like:
- `dbt run --select tag:daily`
- `dbt run --select tag:one-time`

## CI/CD Design

Workflows:
- `.github/workflows/ci.yml`
- `.github/workflows/cd.yml`

Branch target:
- `master`

Path filters:
- Pipelines run only for dbt/ingestion/workflow-relevant files, reducing bombarded noisy run.

### CI (`pull_request -> master`)

Goals: fast feedback + isolated validation, never touches prod target.

1. `lint` job
- `dbt deps`
- `dbt compile --target ci`

2. `test` job (depends on lint)
- Download latest prod manifest artifact (`dbt-manifest-prod`, continue-on-error for bootstrap)
- `dbt seed --target ci`
- `dbt run --select state:modified+ --state ../prod-manifest --defer --target ci --full-refresh`
- `dbt test --select state:modified+ --state ../prod-manifest --target ci`
- Always drop ephemeral schemas:
  - `ci_${CI_RUN_ID}_silver`
  - `ci_${CI_RUN_ID}_gold`

### CD

Two production jobs with `environment: production`:

1. `deploy` (`push -> master` and `workflow_dispatch`)
- `dbt deps`
- `dbt seed --target prod`
- `dbt run --select state:modified+ --state ../prod-manifest --defer --target prod`
- `dbt test --select state:modified+ --state ../prod-manifest --target prod`
- Upload `target/manifest.json` as artifact `dbt-manifest-prod` (30 days)

2. `daily_refresh` (`schedule: 0 7 * * *`)
- `python scripts/python/load_bronze.py --all --prefix "${SUPABASE_RAW_PREFIX}"`
- `dbt deps`
- `dbt run --select tag:daily --target prod`
- `dbt test --select tag:daily --target prod`
- `dbt source freshness --target prod`
- Upload fresh `dbt-manifest-prod`

Important:
- No `--full-refresh` in daily refresh.
- Python ingestion runs from repo root.
- All dbt commands run in `zillow_transformation/`.

## Local Run Shortcuts

```bash
cd zillow_transformation
dbt deps
dbt seed --target dev
dbt run --target dev
dbt test --target dev
```

Daily-tag local check:

```bash
dbt run --select tag:daily --target dev
dbt test --select tag:daily --target dev
```
