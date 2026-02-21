import os
import re

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Las Vegas Real Estate Analytics", layout="wide")

REQUIRED_COLUMNS = [
    "price",
    "bedrooms",
    "bathrooms",
    "livingarea",
    "propertytype",
    "listingstatus",
    "vegas_district",
    "latitude",
    "longitude",
]

DEFAULT_SQL = """select
    f.property_id,
    f.snapshot_date,
    d.vegas_district,
    d.property_type,
    f.price,
    f.days_on_zillow
from gold.fact_property_latest f
join gold.dim_property d
  on d.property_id = f.property_id
order by f.snapshot_date desc
limit 100;
"""


def _db_conn_kwargs() -> dict:
    return {
        "host": os.getenv("SUPABASE_DB_HOST"),
        "port": os.getenv("SUPABASE_DB_PORT", "5432"),
        "dbname": os.getenv("SUPABASE_DB_NAME", "postgres"),
        "user": os.getenv("SUPABASE_DB_USER"),
        "password": os.getenv("SUPABASE_DB_PASSWORD"),
        "sslmode": os.getenv("SUPABASE_DB_SSLMODE", "require"),
    }


def _validate_db_env() -> None:
    conn_kwargs = _db_conn_kwargs()
    missing = [k for k in ("host", "user", "password") if not conn_kwargs.get(k)]
    if missing:
        raise RuntimeError(
            "Missing Supabase DB env vars: "
            + ", ".join(f"SUPABASE_DB_{m.upper()}" for m in missing)
        )


def _run_sql(query: str) -> pd.DataFrame:
    _validate_db_env()
    with psycopg2.connect(**_db_conn_kwargs()) as conn:
        return pd.read_sql_query(query, conn)


def load_dashboard_df() -> pd.DataFrame:
    query = """
        select
            f.price,
            f.bedrooms,
            f.bathrooms,
            f.living_area as livingarea,
            d.property_type as propertytype,
            f.listing_status as listingstatus,
            d.vegas_district,
            d.latitude,
            d.longitude,
            f.snapshot_date
        from gold.fact_property_latest f
        inner join gold.dim_property d
            on d.property_id = f.property_id
        left join gold.dim_date dd
            on dd.date_day = f.snapshot_date
    """
    return _run_sql(query)


def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    numeric_cols = [
        "price",
        "bedrooms",
        "bathrooms",
        "livingarea",
        "latitude",
        "longitude",
    ]
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data["propertytype"] = data["propertytype"].astype("string").fillna("Unknown")
    data["listingstatus"] = data["listingstatus"].astype("string").fillna("Unknown")
    data["vegas_district"] = data["vegas_district"].astype("string").fillna("Unknown")

    data["price_per_sqft"] = np.where(data["livingarea"] > 0, data["price"] / data["livingarea"], np.nan)
    return data


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("## Filter Panel")
    st.sidebar.caption("Refine listings by location, product type, and price profile.")
    st.sidebar.divider()

    districts = sorted(df["vegas_district"].dropna().unique().tolist())
    property_types = sorted(df["propertytype"].dropna().unique().tolist())
    listing_statuses = sorted(df["listingstatus"].dropna().unique().tolist())

    min_bed = int(np.nanmin(df["bedrooms"])) if df["bedrooms"].notna().any() else 0
    max_bed = int(np.nanmax(df["bedrooms"])) if df["bedrooms"].notna().any() else 10
    min_price = float(np.nanmin(df["price"])) if df["price"].notna().any() else 0.0
    max_price = float(np.nanmax(df["price"])) if df["price"].notna().any() else 1.0

    with st.sidebar.expander("Location & Inventory", expanded=True):
        selected_districts = st.multiselect(
            "District",
            options=districts,
            default=districts,
            help="Choose one or more Vegas districts.",
        )
        selected_property_types = st.multiselect(
            "Property Type",
            options=property_types,
            default=property_types,
        )
        selected_statuses = st.multiselect(
            "Listing Status",
            options=listing_statuses,
            default=listing_statuses,
        )

    with st.sidebar.expander("Price & Size", expanded=True):
        bedrooms_range = st.slider(
            "Bedrooms",
            min_value=min_bed,
            max_value=max_bed,
            value=(min_bed, max_bed),
        )
        price_range = st.slider(
            "Price Range ($)",
            min_value=min_price,
            max_value=max_price,
            value=(min_price, max_price),
            format="$%.0f",
        )

    st.sidebar.caption(f"Rows in source dataset: {len(df):,}")

    mask = (
        df["vegas_district"].isin(selected_districts)
        & df["propertytype"].isin(selected_property_types)
        & df["listingstatus"].isin(selected_statuses)
        & df["bedrooms"].between(bedrooms_range[0], bedrooms_range[1], inclusive="both")
        & df["price"].between(price_range[0], price_range[1], inclusive="both")
    )
    filtered = df.loc[mask].copy()
    st.sidebar.caption(f"Rows after filters: {len(filtered):,}")
    return filtered


def add_summary_metrics(df: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Average Price", f"${df['price'].mean():,.0f}" if len(df) else "N/A")
    c2.metric("Median Price", f"${df['price'].median():,.0f}" if len(df) else "N/A")
    c3.metric("Average Price / Sqft", f"${df['price_per_sqft'].mean():,.0f}" if df["price_per_sqft"].notna().any() else "N/A")
    c4.metric("Total Listings", f"{len(df):,}")


def add_regression_line(fig: go.Figure, x: pd.Series, y: pd.Series, name: str = "Trend") -> go.Figure:
    clean = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(clean) < 2:
        return fig

    slope, intercept = np.polyfit(clean["x"], clean["y"], 1)
    x_line = np.linspace(clean["x"].min(), clean["x"].max(), 100)
    y_line = slope * x_line + intercept
    fig.add_trace(
        go.Scatter(
            x=x_line,
            y=y_line,
            mode="lines",
            name=name,
            line=dict(color="black", width=2, dash="dash"),
        )
    )
    return fig


def render_dashboard_page() -> None:
    st.title("Las Vegas Real Estate Investment Dashboard")
    st.caption("This dashboard provides insights for Las Vegas investment and pricing analysis")

    try:
        raw_df = load_dashboard_df()
    except Exception as exc:
        st.error(f"Failed to load data from Supabase: {exc}")
        st.stop()

    if raw_df.empty:
        st.warning("No rows returned from Supabase gold schema.")
        st.stop()

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        st.error(f"Missing required columns: {', '.join(missing_cols)}")
        st.stop()

    df = prepare_df(raw_df)
    filtered_df = apply_filters(df)

    if filtered_df.empty:
        st.warning("No rows match the current filter selection. Adjust sidebar filters.")
        st.stop()

    add_summary_metrics(filtered_df)

    st.subheader("Market Overview")
    district_price = (
        filtered_df.groupby("vegas_district", dropna=False)["price"]
        .mean()
        .reset_index()
        .sort_values("price", ascending=False)
    )
    fig_price = px.bar(
        district_price,
        x="vegas_district",
        y="price",
        title="Average Listing Price by Vegas District",
        labels={"vegas_district": "District", "price": "Average Price"},
    )
    st.plotly_chart(fig_price, use_container_width=True)
    st.caption("Insight: focus acquisition search in districts with lower average prices and sufficient inventory depth.")

    supply_dist = (
        filtered_df.groupby(["vegas_district", "propertytype"], dropna=False)
        .size()
        .reset_index(name="listing_count")
    )
    fig_supply = px.bar(
        supply_dist,
        x="vegas_district",
        y="listing_count",
        color="propertytype",
        barmode="stack",
        title="Property Type Distribution by District",
        labels={"vegas_district": "District", "listing_count": "Listing Count", "propertytype": "Property Type"},
    )
    st.plotly_chart(fig_supply, use_container_width=True)
    st.caption("Insight: prioritize districts with balanced supply, not just high volume in one property type.")

    st.subheader("Price Structure")
    fig_living_price = px.scatter(
        filtered_df,
        x="livingarea",
        y="price",
        color="vegas_district",
        title="Living Area vs Price",
        labels={"livingarea": "Living Area (sqft)", "price": "Price"},
        opacity=0.7,
    )
    fig_living_price = add_regression_line(fig_living_price, filtered_df["livingarea"], filtered_df["price"], name="Regression")
    st.plotly_chart(fig_living_price, use_container_width=True)
    st.caption("Insight: listings far below the trend line can indicate relative value for the same size segment.")

    fig_box_bed = px.box(
        filtered_df,
        x=filtered_df["bedrooms"].astype("Int64").astype("string"),
        y="price",
        points="outliers",
        title="Price Distribution by Bedroom Count",
        labels={"x": "Bedrooms", "price": "Price"},
    )
    st.plotly_chart(fig_box_bed, use_container_width=True)
    st.caption("Insight: bedroom tiers with wider spread need stricter comp selection before pricing decisions.")

    st.subheader("Recommendations")
    corr_cols = ["price", "bedrooms", "bathrooms", "livingarea", "price_per_sqft"]
    corr_matrix = filtered_df[corr_cols].corr(numeric_only=True)
    fig_corr = px.imshow(
        corr_matrix,
        text_auto=True,
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
        title="Correlation Heatmap of Core Price Drivers",
    )
    st.plotly_chart(fig_corr, use_container_width=True)
    candidate_cols = [
        "vegas_district",
        "propertytype",
        "price",
        "livingarea",
        "bedrooms",
        "bathrooms",
        "price_per_sqft",
        "listingstatus",
    ]
    candidates = (
        filtered_df[candidate_cols]
        .dropna(subset=["price_per_sqft"])
        .query("livingarea >= 800")
        .nsmallest(15, "price_per_sqft")
    )
    st.subheader("Recommended Watchlist: Lowest Price per Sqft (Living Area >= 800 sqft)")
    st.dataframe(candidates, use_container_width=True)

    st.markdown(
        """
**Recommendation Summary**
1. Prioritize districts with lower average price and diversified supply mix.
2. In each district, compare target listings against the living-area trend line.
3. Use low `price_per_sqft` watchlist as shortlist, then validate with street-level comps.
"""
    )


def render_relationships_page() -> None:
    st.title("Table Relationships")
    st.caption("Visualized from `data_model_material/schema_build.txt`")

    st.subheader("Relationship Matrix")
    relationships = pd.DataFrame(
        [
            {
                "from_table": "fct_property_listing",
                "from_key": "property_id",
                "to_table": "dim_property",
                "to_key": "property_id",
                "cardinality": "Many-to-One",
                "description": "Property context",
            },
            {
                "from_table": "fct_property_listing",
                "from_key": "snapshot_date_id",
                "to_table": "dim_date",
                "to_key": "date_id",
                "cardinality": "Many-to-One",
                "description": "Snapshot calendar",
            },
            {
                "from_table": "fct_property_listing",
                "from_key": "location_id",
                "to_table": "dim_location",
                "to_key": "location_id",
                "cardinality": "Many-to-One",
                "description": "Location context",
            },
            {
                "from_table": "fct_property_listing",
                "from_key": "date_price_changed_id",
                "to_table": "dim_date",
                "to_key": "date_id",
                "cardinality": "Many-to-One (nullable)",
                "description": "Price-change date context",
            },
        ]
    )
    st.dataframe(relationships, use_container_width=True)

    st.subheader("Star Schema Diagram")
    fig_rel = go.Figure()
    fig_rel.update_xaxes(visible=False, range=[0, 1])
    fig_rel.update_yaxes(visible=False, range=[0, 1])

    fig_rel.add_shape(type="rect", x0=0.34, y0=0.34, x1=0.66, y1=0.66, line=dict(color="#1f4e79", width=2), fillcolor="#d9e8f5")
    fig_rel.add_annotation(
        x=0.50,
        y=0.50,
        showarrow=False,
        align="left",
        text=(
            "<b>fct_property_listing</b><br>"
            "PK: property_listing_id<br>"
            "FK: property_id<br>"
            "FK: snapshot_date_id<br>"
            "FK: location_id<br>"
            "FK: date_price_changed_id (nullable)<br>"
            "price, coming_soon, contingent_type"
        ),
    )

    dim_boxes = [
        (0.05, 0.38, 0.29, 0.62, "#e7f2e2", "<b>dim_property</b><br>PK: property_id<br>zillow_property_id<br>bedrooms, bathrooms<br>lot_size, property_type"),
        (0.71, 0.38, 0.95, 0.62, "#fff1db", "<b>dim_location</b><br>PK: location_id<br>country, state<br>city, zip_code"),
        (0.38, 0.72, 0.62, 0.95, "#f5e5f5", "<b>dim_date</b><br>PK: date_id<br>day_of_week, day_of_month<br>cal_month, cal_quarter<br>cal_year, is_weekend"),
    ]
    for x0, y0, x1, y1, color, label in dim_boxes:
        fig_rel.add_shape(type="rect", x0=x0, y0=y0, x1=x1, y1=y1, line=dict(color="#555", width=1.5), fillcolor=color)
        fig_rel.add_annotation(x=(x0 + x1) / 2, y=(y0 + y1) / 2, showarrow=False, align="left", text=label)

    fig_rel.add_annotation(x=0.29, y=0.50, ax=0.34, ay=0.50, xref="x", yref="y", axref="x", ayref="y", showarrow=True, arrowhead=3)
    fig_rel.add_annotation(x=0.71, y=0.50, ax=0.66, ay=0.50, xref="x", yref="y", axref="x", ayref="y", showarrow=True, arrowhead=3)
    fig_rel.add_annotation(x=0.50, y=0.72, ax=0.50, ay=0.66, xref="x", yref="y", axref="x", ayref="y", showarrow=True, arrowhead=3)
    fig_rel.add_annotation(
        x=0.57,
        y=0.72,
        ax=0.58,
        ay=0.66,
        xref="x",
        yref="y",
        axref="x",
        ayref="y",
        showarrow=True,
        arrowhead=2,
    )

    fig_rel.update_layout(height=560, margin=dict(l=10, r=10, t=20, b=10))
    st.plotly_chart(fig_rel, use_container_width=True)

    st.subheader("Example Join Query (schema_build design)")
    st.code(
        """select
    f.property_listing_id,
    f.price,
    p.property_type,
    p.bedrooms,
    l.city,
    l.zip_code,
    dd.cal_year,
    dd.cal_month
from fct_property_listing f
join dim_property p
  on p.property_id = f.property_id
join dim_location l
  on l.location_id = f.location_id
join dim_date dd
  on dd.date_id = f.snapshot_date_id
limit 100;""",
        language="sql",
    )


def _validate_read_only_query(query: str) -> tuple[bool, str]:
    cleaned = query.strip()
    if not cleaned:
        return False, "Query is empty."

    compact = cleaned.rstrip(";").strip()
    if ";" in compact:
        return False, "Only one SQL statement is allowed."

    if not re.match(r"^(select|with)\b", compact, flags=re.IGNORECASE):
        return False, "Only SELECT/WITH read queries are allowed."

    forbidden = re.compile(
        r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|comment|vacuum|analyze|call|do|copy)\b",
        flags=re.IGNORECASE,
    )
    if forbidden.search(compact):
        return False, "Query contains non-read-only keywords."
    return True, ""


def render_query_page() -> None:
    st.title("Custom SQL Query")
    st.caption("Run your own read-only SQL against Supabase")

    st.info("Allowed query type: single `SELECT` or `WITH ... SELECT` statement.")
    query = st.text_area("SQL", value=DEFAULT_SQL, height=240)
    run_query = st.button("Run Query", type="primary")

    if run_query:
        ok, error = _validate_read_only_query(query)
        if not ok:
            st.error(error)
            return

        try:
            df = _run_sql(query)
        except Exception as exc:
            st.error(f"Query failed: {exc}")
            return

        st.success(f"Returned {len(df):,} row(s)")
        st.dataframe(df, use_container_width=True)
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download Result CSV",
            data=csv_bytes,
            file_name="query_result.csv",
            mime="text/csv",
        )


page = st.sidebar.radio(
    "Navigation",
    options=["Dashboard", "Table Relationships Info", "Make Your Own SQL Query"],
)

if page == "Dashboard":
    render_dashboard_page()
elif page == "Table Relationships Info":
    render_relationships_page()
else:
    render_query_page()
