# 🏡 Zillow Real Estate Analytics — dbt Star Schema Project

This project implements a **modern, production-style data transformation pipeline** for real estate analytics using historical Zillow property listing data. Raw snapshot-based listing data is transformed into a **well-structured star schema** using **dbt**, enabling time-aware analysis of property prices, listing behavior, and market dynamics.

The project closely follows **industry best practices** in dimensional modeling, Slowly Changing Dimensions (SCD Type 2), incremental fact tables, data quality testing, and documentation-driven development.

---

## 🗂 Project Structure

```
real_estate_transformation/
├── zillow_transformation/
│   ├── models/
│   │   ├── staging/                    # Source-aligned cleaned models
│   │   │   └── stg_properties.sql
│   │   ├── mart/
│   │   │   ├── dim/                    # Dimension tables
│   │   │   │   ├── dim_property.sql
│   │   │   │   ├── dim_location.sql
│   │   │   │   └── dim_date.sql
│   │   │   └── fact/                   # Fact tables
│   │   │       └── fact_property_listing.sql
│   │   ├── schema.yml                  # Tests & documentation
│   ├── snapshots/
│   │   └── snap_property.sql           # SCD Type 2 snapshot
│   ├── dbt_project.yml
│   └── packages.yml
├── data_model/                         # Schema diagrams & definitions
│   └── star_schema.txt
└── README.md
```

---

## ⚙️ Technology Stack

- **Data Source**: Zillow API — historical property listing data
- **Programming Language**: SQL + Jinja (dbt)
- **Data Warehouse**: PostgreSQL
- **Transformation Tool**: dbt Core (v1.10+)
- **Modeling Approach**: Kimball Star Schema
- **Change Tracking**: dbt Snapshots (SCD Type 2)

> This project intentionally uses PostgreSQL to demonstrate that dbt is effective beyond cloud-native warehouses.

---

## 🧱 Data Architecture

### 1️⃣ Data Source

Property listing data is ingested from the Zillow API and stored as a **history table**. For details about scraping and ingestion process, please refer to my repo about [zillow_data_extract](https://github.com/HaDo1802/zillow_data_extract).

Each row represents the full state of a property listing at the time of extraction.

Captured attributes include:
- Zillow property identifier
- Snapshot date
- Price and Zestimate values
- Bedrooms, bathrooms, living area, lot size
- Property type and listing status
- Address and geospatial information
- Listing activity flags (open house, images, 3D model)
- Metadata timestamps

This raw table is **append-only** and preserves the full change history.

---

### 2️⃣ Staging Layer

**Model: `stg_properties`**

Purpose:
- Standardize column names (`snake_case`)
- Normalize data types
- Apply light cleansing rules
- Preserve original grain

Grain:
> One row per property per snapshot date, defined by each time the pipeline is triggered.

The staging layer acts as a **contract** between raw ingestion and business modeling.

---

## 🌐 Data Modeling Approach

### Step 1: Business Process

The core business process is **tracking real estate listings over time**.

Each time Zillow data is scraped, the state of a property listing is captured.  
Changes in price, listing status, or property attributes represent meaningful business events.

---

### Step 2: Define the Grain

The grain of the primary fact table is defined as:

> **One row per property per snapshot date**

This grain ensures:
- Accurate historical analysis
- Time-based trend evaluation
- No loss of detail from the source

---

### Step 3: Identify Dimensions

Dimensions provide descriptive context around each listing snapshot.

#### `dim_property`
Describes relatively stable property characteristics:
- bedrooms
- bathrooms
- living_area
- lot_area
- property_type

Implemented as **SCD Type 2** to preserve historical changes.

---

#### `dim_location`
Describes where the property is located:
- city
- state
- zip_code
- vegas_district
- latitude / longitude

Used for geographic slicing and aggregation.

---

#### `dim_date`
Standardized calendar dimension supporting:
- calendar hierarchy (day, month, quarter, year)
- weekend flags
- fiscal attributes

Used for all time-based joins.

---

### Step 4: Facts for Measurement

#### `fact_property_listing`

Stores **time-variant, measurable metrics** for each snapshot:

Metrics:
- price
- zestimate
- rent_zestimate
- days_on_zillow

Flags:
- has_image
- has_video
- has_3d_model
- is_open_house
- is_fsba

Foreign keys connect each record to:
- property dimension
- location dimension
- snapshot date

---

## ⭐ Star Schema Overview

The model follows a classic **star schema** design.

| Fact Table Column | Dimension Table | Description |
|------------------|-----------------|-------------|
| property_id | dim_property | Property attributes (SCD Type 2) |
| location_id | dim_location | Geographic context |
| snapshot_date_id | dim_date | Time of snapshot |

This structure supports efficient filtering, aggregation, and historical analysis.
![Star Schema](data_model_visualization/Star_schema.png)

---

## 🔄 Slowly Changing Dimensions (SCD)

### Why SCD Type 2?

Property characteristics can change over time:
- renovations
- reclassification
- corrections in listing data

Using SCD Type 2 allows:
- Full historical accuracy
- Point-in-time reporting
- No data overwrites

### Implementation

The `snap_property` snapshot:
- Tracks changes in property attributes
- Creates new records on change
- Maintains `valid_from` / `valid_to` ranges
- Flags current records

---

## 🧪 Data Quality Framework

Data quality is enforced using dbt tests:

- `not_null` on primary keys
- `unique` constraints on dimension keys
- `relationships` tests for foreign keys
- Accepted values tests for categorical fields
- Snapshot consistency checks

---

## 📈 Analytics Use Cases

- Property price trends over time
- Market activity by location
- Listing lifecycle analysis
- Snapshot-based point-in-time reporting
- Property attribute change tracking


---

## 🚀 Why This Project Matters

This project demonstrates:
- Real-world dimensional modeling
- Correct use of SCD Type 2
- Incremental fact table design
- Professional dbt project structure
- Warehouse-agnostic transformation logic

It is designed to be **portfolio-ready** and easily extendable to cloud data warehouses.

---
