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

st.set_page_config(
    page_title="Las Vegas Real Estate Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
    .metric-label { font-size: 13px; }
    h1 { font-size: 1.6rem; font-weight: 600; }
    h2 { font-size: 1.1rem; font-weight: 500;
         border-bottom: 1px solid #e0e0e0; padding-bottom: 6px; }
    section[data-testid="stSidebar"] .stButton button {
        width: 100%;
        border-radius: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

DEFAULT_SQL = """select
    property_id,
    snapshot_date,
    vegas_district,
    property_type,
    price,
    days_on_zillow
from gold.mart_property_current
order by snapshot_date desc
limit 100;
"""

PROPERTY_TYPE_COUNT_COLUMNS = [
    "single_family_count",
    "condo_count",
    "townhouse_count",
    "multi_family_count",
]

PROPERTY_TYPE_LABELS = {
    "single_family_count": "SINGLE_FAMILY",
    "condo_count": "CONDO",
    "townhouse_count": "TOWNHOUSE",
    "multi_family_count": "MULTI_FAMILY",
}

PLOTLY_CONFIG = {
    "displayModeBar": True,
    "modeBarButtonsToRemove": [
        "zoom2d",
        "pan2d",
        "select2d",
        "lasso2d",
        "zoomIn2d",
        "zoomOut2d",
        "autoScale2d",
        "resetScale2d",
    ],
}


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


@st.cache_resource
def get_connection():
    _validate_db_env()
    conn = psycopg2.connect(**_db_conn_kwargs())
    conn.autocommit = True
    return conn


def _run_sql(query: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        get_connection.clear()
        conn = get_connection()
        return pd.read_sql_query(query, conn)


@st.cache_data(ttl=3600)
def load_market_summary() -> pd.DataFrame:
    query = """
        select * from gold.mart_market_summary
        order by snapshot_date desc
    """
    return _run_sql(query)


@st.cache_data(ttl=3600)
def load_property_watchlist() -> pd.DataFrame:
    query = """
        select
            property_id,
            snapshot_date,
            street_address,
            city,
            vegas_district,
            property_type,
            price,
            living_area,
            bedrooms,
            bathrooms,
            price_per_sqft,
            days_on_zillow,
            listing_status
        from gold.mart_property_current
        where price is not null
          and living_area is not null
        order by price_per_sqft asc
    """
    return _run_sql(query)


def apply_dashboard_filters(market_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    st.sidebar.markdown("## Filter panel")
    st.sidebar.caption("Refine dashboard metrics by district and snapshot date.")
    st.sidebar.divider()

    districts = sorted(market_df["vegas_district"].dropna().unique().tolist())
    selected_districts = st.sidebar.multiselect(
        "District",
        options=districts,
        default=districts,
        help="Choose one or more Vegas districts.",
    )

    min_date = market_df["snapshot_date"].min().date()
    max_date = market_df["snapshot_date"].max().date()
    selected_dates = st.sidebar.slider(
        "Snapshot date range",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
    )

    filtered = market_df.loc[
        market_df["vegas_district"].isin(selected_districts)
        & market_df["snapshot_date"].dt.date.between(
            selected_dates[0], selected_dates[1]
        )
    ].copy()

    return filtered, selected_districts


@st.cache_data(show_spinner=False)
def _cached_regression_line(x_values: tuple[float, ...], y_values: tuple[float, ...]):
    clean = pd.DataFrame({"x": x_values, "y": y_values}).dropna()
    if len(clean) < 2:
        return None, None

    slope, intercept = np.polyfit(clean["x"], clean["y"], 1)
    x_line = np.linspace(clean["x"].min(), clean["x"].max(), 100)
    y_line = slope * x_line + intercept
    return x_line, y_line


def add_regression_line(
    fig: go.Figure, x: pd.Series, y: pd.Series, name: str = "Trend"
) -> go.Figure:
    x_line, y_line = _cached_regression_line(tuple(x.tolist()), tuple(y.tolist()))
    if x_line is None or y_line is None:
        return fig

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
    st.caption(
        "This dashboard provides insights for Las Vegas investment and pricing analysis"
    )

    try:
        market_df = load_market_summary()
        watchlist_df = load_property_watchlist()
    except Exception as exc:
        st.error(f"Failed to load data from Supabase: {exc}")
        st.stop()

    if market_df.empty:
        st.warning("No rows returned from `gold.mart_market_summary`.")
        st.stop()

    market_df = market_df.copy()
    watchlist_df = watchlist_df.copy()
    market_df["snapshot_date"] = pd.to_datetime(market_df["snapshot_date"])
    watchlist_df["snapshot_date"] = pd.to_datetime(watchlist_df["snapshot_date"])

    districts = sorted(market_df["vegas_district"].dropna().unique().tolist())
    if "selected_districts" not in st.session_state:
        st.session_state["selected_districts"] = districts
    else:
        st.session_state["selected_districts"] = [
            district
            for district in st.session_state["selected_districts"]
            if district in districts
        ] or districts

    st.sidebar.title("Filters")
    st.sidebar.markdown("#### District")
    st.sidebar.caption("Choose one or more submarkets.")
    district_action_col1, district_action_col2 = st.sidebar.columns(2)
    if district_action_col1.button("Select all", use_container_width=True):
        st.session_state["selected_districts"] = districts
    if district_action_col2.button("Clear", use_container_width=True):
        st.session_state["selected_districts"] = []

    selected_districts = st.sidebar.multiselect(
        "District",
        options=districts,
        default=st.session_state["selected_districts"],
        key="selected_districts",
        label_visibility="collapsed",
        placeholder="Select district(s)",
    )
    latest_snapshot_date = market_df["snapshot_date"].max()
    filtered_market_df = market_df.loc[
        market_df["vegas_district"].isin(selected_districts)
    ].copy()
    filtered_market_df = filtered_market_df.loc[
        filtered_market_df["snapshot_date"] == latest_snapshot_date
    ].copy()

    properties_shown = (
        int(filtered_market_df["listing_count"].sum())
        if not filtered_market_df.empty
        else 0
    )
    st.sidebar.caption(f"Properties shown: {properties_shown:,}")
    st.sidebar.caption(f"Last updated: {latest_snapshot_date.date()}")

    if filtered_market_df.empty:
        st.warning(
            "No rows match the current filter selection. Adjust sidebar filters."
        )
        st.stop()

    latest_kpi_df = market_df.loc[
        market_df["vegas_district"].isin(selected_districts)
        & (market_df["snapshot_date"] == latest_snapshot_date)
    ].copy()

    if latest_kpi_df.empty:
        st.warning(
            "No rows are available for the latest snapshot in the selected date range."
        )
        st.stop()

    property_current_df = watchlist_df.loc[
        watchlist_df["vegas_district"].isin(selected_districts)
    ].copy()

    if property_current_df.empty:
        st.warning("No data matches current filters.")
        st.stop()

    # st.markdown("## KPI metrics")
    total_listings = int(latest_kpi_df["listing_count"].sum())
    median_price = latest_kpi_df["median_price"].median()
    avg_price_per_sqft = latest_kpi_df["avg_price_per_sqft"].mean()
    avg_days_on_market = latest_kpi_df["avg_days_on_zillow"].mean()

    c1, c2, c3, c4 = st.columns(4, gap="medium")
    c1.metric("Total listings", f"{total_listings:,}")
    c2.metric(
        "Median price", f"${median_price:,.0f}" if pd.notna(median_price) else "N/A"
    )
    c3.metric(
        "Avg price / sqft",
        f"${avg_price_per_sqft:,.0f}" if pd.notna(avg_price_per_sqft) else "N/A",
    )
    c4.metric(
        "Avg days on market",
        f"{avg_days_on_market:,.0f} days" if pd.notna(avg_days_on_market) else "N/A",
    )

    st.divider()

    st.markdown("## Market overview")
    latest_chart_df = filtered_market_df.loc[
        filtered_market_df["snapshot_date"] == filtered_market_df["snapshot_date"].max()
    ].copy()
    district_price = (
        filtered_market_df.groupby("vegas_district", as_index=False)["avg_price"]
        .mean()
        .sort_values("avg_price", ascending=False)
    )

    overview_col1, overview_col2 = st.columns(2)
    fig_price = px.bar(
        district_price,
        x="vegas_district",
        y="avg_price",
        color="vegas_district",
        title="Average price by district",
        labels={"vegas_district": "District", "avg_price": "Average price"},
        template="plotly_white",
        height=380,
    )
    fig_price.update_layout(
        showlegend=False, title_font_size=14, title_font_color="#1a1a1a"
    )
    fig_price.update_yaxes(tickprefix="$", tickformat=",.0f")
    overview_col1.plotly_chart(
        fig_price, use_container_width=True, config=PLOTLY_CONFIG
    )
    overview_col1.caption(
        "Districts with higher average prices usually reflect stronger demand and tighter inventory."
    )

    status_breakdown = latest_chart_df[
        ["vegas_district", "for_sale_count", "pending_count", "sold_count"]
    ].melt(
        id_vars="vegas_district",
        value_vars=["for_sale_count", "pending_count", "sold_count"],
        var_name="listing_status",
        value_name="listing_count",
    )
    status_breakdown["listing_status"] = status_breakdown["listing_status"].map(
        {
            "for_sale_count": "FOR_SALE",
            "pending_count": "PENDING",
            "sold_count": "SOLD",
        }
    )
    fig_status = px.bar(
        status_breakdown,
        x="vegas_district",
        y="listing_count",
        color="listing_status",
        barmode="stack",
        title="Listing status breakdown by district",
        labels={
            "vegas_district": "District",
            "listing_count": "Listing count",
            "listing_status": "Status",
        },
        template="plotly_white",
        height=380,
        color_discrete_map={
            "FOR_SALE": "#4C9BE8",
            "PENDING": "#F4A623",
            "SOLD": "#2ECC71",
        },
    )
    fig_status.update_layout(title_font_size=14, title_font_color="#1a1a1a")
    overview_col2.plotly_chart(
        fig_status, use_container_width=True, config=PLOTLY_CONFIG
    )
    overview_col2.caption("Pending rate indicates near-term demand pressure.")

    st.markdown("## Price structure")
    structure_col1, structure_col2 = st.columns(2)
    fig_scatter = px.scatter(
        property_current_df,
        x="living_area",
        y="price",
        color="vegas_district",
        opacity=0.6,
        trendline="ols",
        size_max=8,
        title="Price vs living area",
        labels={
            "living_area": "Living area (sqft)",
            "price": "Price ($)",
            "vegas_district": "District",
        },
        template="plotly_white",
        height=380,
    )
    fig_scatter.update_layout(title_font_size=14, title_font_color="#1a1a1a")
    fig_scatter.update_yaxes(tickprefix="$", tickformat=",.0f")
    structure_col1.plotly_chart(
        fig_scatter, use_container_width=True, config=PLOTLY_CONFIG
    )
    structure_col1.caption("Points below the trend line may indicate relative value.")

    fig_days = px.box(
        property_current_df,
        x="vegas_district",
        y="days_on_zillow",
        points="outliers",
        title="Days on market by district",
        labels={"vegas_district": "District", "days_on_zillow": "Days on market"},
        template="plotly_white",
        height=380,
    )
    fig_days.update_layout(title_font_size=14, title_font_color="#1a1a1a")
    structure_col2.plotly_chart(
        fig_days, use_container_width=True, config=PLOTLY_CONFIG
    )
    structure_col2.caption("Lower median = faster-moving market.")

    st.divider()

    st.markdown("## Median price by district and bedroom count")
    heatmap_df = property_current_df.copy()
    heatmap_df["bedroom_bucket"] = heatmap_df["bedrooms"].apply(
        lambda value: (
            "5+"
            if pd.notna(value) and value >= 5
            else str(int(value)) if pd.notna(value) else None
        )
    )
    heatmap_df = heatmap_df.dropna(subset=["bedroom_bucket"])
    bedroom_order = ["1", "2", "3", "4", "5+"]
    heatmap_pivot = heatmap_df.pivot_table(
        index="vegas_district",
        columns="bedroom_bucket",
        values="price",
        aggfunc="median",
    ).reindex(columns=bedroom_order)
    if heatmap_pivot.empty:
        st.warning("No data matches current filters.")
        st.stop()
    heatmap_text = heatmap_pivot.applymap(
        lambda value: f"${value:,.0f}" if pd.notna(value) else ""
    )
    heatmap_height = max(420, min(620, 120 + (len(heatmap_pivot.index) * 42)))
    fig_heatmap = px.imshow(
        heatmap_pivot,
        color_continuous_scale="Blues",
        text_auto=False,
        labels={"x": "Bedrooms", "y": "District", "color": "Median price"},
        title="Median price by district and bedroom count",
        template="plotly_white",
        height=heatmap_height,
        aspect="auto",
    )
    fig_heatmap.update_traces(text=heatmap_text.values, texttemplate="%{text}")
    fig_heatmap.update_layout(
        title_font_size=14,
        title_font_color="#1a1a1a",
        margin=dict(l=12, r=12, t=54, b=12),
        coloraxis_colorbar=dict(thickness=14, len=0.78),
    )
    fig_heatmap.update_xaxes(side="top", tickangle=0)
    fig_heatmap.update_yaxes(automargin=True)
    fig_heatmap.update_coloraxes(colorbar_tickprefix="$", colorbar_tickformat=",.0f")
    st.plotly_chart(fig_heatmap, use_container_width=True, config=PLOTLY_CONFIG)
    st.caption("Empty cells indicate no listings for that combination.")

    st.markdown("## Value watchlist — lowest price per sqft (living area >= 800 sqft)")
    filtered_watchlist = (
        property_current_df.loc[
            (property_current_df["living_area"] >= 800)
            & (property_current_df["price"] >= 10000)
            & property_current_df["price_per_sqft"].notna()
        ]
        .sort_values("price_per_sqft", ascending=True)
        .head(25)
    )
    if filtered_watchlist.empty:
        st.warning("No data matches current filters.")
        st.stop()

    display_watchlist = filtered_watchlist[
        [
            "street_address",
            "vegas_district",
            "property_type",
            "bedrooms",
            "bathrooms",
            "living_area",
            "price",
            "price_per_sqft",
            "days_on_zillow",
            "listing_status",
        ]
    ].copy()
    display_watchlist["price"] = display_watchlist["price"].map(
        lambda value: f"${value:,.0f}" if pd.notna(value) else "N/A"
    )
    display_watchlist["price_per_sqft"] = display_watchlist["price_per_sqft"].map(
        lambda value: f"${value:,.0f}/sqft" if pd.notna(value) else "N/A"
    )
    display_watchlist["living_area"] = display_watchlist["living_area"].map(
        lambda value: f"{value:,.0f} sqft" if pd.notna(value) else "N/A"
    )
    display_watchlist["days_on_zillow"] = display_watchlist["days_on_zillow"].map(
        lambda value: f"{value:,.0f} days" if pd.notna(value) else "N/A"
    )
    st.dataframe(display_watchlist, use_container_width=True, hide_index=True)


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

    fig_rel.add_shape(
        type="rect",
        x0=0.34,
        y0=0.34,
        x1=0.66,
        y1=0.66,
        line=dict(color="#1f4e79", width=2),
        fillcolor="#d9e8f5",
    )
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
        (
            0.05,
            0.38,
            0.29,
            0.62,
            "#e7f2e2",
            "<b>dim_property</b><br>PK: property_id<br>zillow_property_id<br>bedrooms, bathrooms<br>lot_size, property_type",
        ),
        (
            0.71,
            0.38,
            0.95,
            0.62,
            "#fff1db",
            "<b>dim_location</b><br>PK: location_id<br>country, state<br>city, zip_code",
        ),
        (
            0.38,
            0.72,
            0.62,
            0.95,
            "#f5e5f5",
            "<b>dim_date</b><br>PK: date_id<br>day_of_week, day_of_month<br>cal_month, cal_quarter<br>cal_year, is_weekend",
        ),
    ]
    for x0, y0, x1, y1, color, label in dim_boxes:
        fig_rel.add_shape(
            type="rect",
            x0=x0,
            y0=y0,
            x1=x1,
            y1=y1,
            line=dict(color="#555", width=1.5),
            fillcolor=color,
        )
        fig_rel.add_annotation(
            x=(x0 + x1) / 2, y=(y0 + y1) / 2, showarrow=False, align="left", text=label
        )

    fig_rel.add_annotation(
        x=0.29,
        y=0.50,
        ax=0.34,
        ay=0.50,
        xref="x",
        yref="y",
        axref="x",
        ayref="y",
        showarrow=True,
        arrowhead=3,
    )
    fig_rel.add_annotation(
        x=0.71,
        y=0.50,
        ax=0.66,
        ay=0.50,
        xref="x",
        yref="y",
        axref="x",
        ayref="y",
        showarrow=True,
        arrowhead=3,
    )
    fig_rel.add_annotation(
        x=0.50,
        y=0.72,
        ax=0.50,
        ay=0.66,
        xref="x",
        yref="y",
        axref="x",
        ayref="y",
        showarrow=True,
        arrowhead=3,
    )
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
