# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "altair==6.0.0",
#     "marimo>=0.19.4",
#     "polars==1.37.1",
#     "pyarrow==23.0.0",
#     "pyodide-http==0.2.2",
#     "requests==2.32.5",
# ]
# ///

import marimo

__generated_with = "0.19.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import io
    from datetime import date, datetime, time, timezone
    import pyarrow.parquet as pq
    return alt, date, io, mo, pl, pq


@app.cell
def _(io, pl, pq):
    # As of Jan 2026, Polars WASM can't directly read Parquet over the network, so we use requests after patching
    url = "https://data.jack-kelly.com/home-energy-data/solar-pv/year=2026/month=1/00000000.parquet"

    # 1. Use standard Python/WASM networking to get the file bytes
    import pyodide_http

    pyodide_http.patch_all()
    import requests


    # 1. Essential: Patch requests to work in the browser
    pyodide_http.patch_all()

    # 2. Download the file into memory
    response = requests.get(url)

    if response.status_code == 200:
        # 3. Read into a PyArrow Table (This works in WASM!)
        # We can't read into Polars because Polars WASM has disabled Parquet entirely as of Jan 2026.
        table = pq.read_table(io.BytesIO(response.content))

        # 4. Convert PyArrow Table to Polars DataFrame
        df = pl.from_arrow(table)
    else:
        print(f"Failed to download: {response.status_code}")
    return (df,)


@app.cell
def _(date, df, mo):
    # Date picker
    dates = df["period_end_time"].dt.date().unique().sort(descending=True)

    query_params = mo.query_params()
    initial_date_str = query_params.get("date", str(dates[0]))

    try:
        initial_date = date.fromisoformat(initial_date_str)
    except ValueError:
        print("Failed to parse {initial_date_str} as a date")
        initial_date = dates[0]


    def try_to_set_date_param(d: date):
        # This fails in molab
        try:
            query_params.set("date", str(d))
        except:
            pass


    selected_date = mo.ui.date.from_series(
        series=dates.rename("Date to plot:"),
        value=initial_date,
        on_change=lambda val: try_to_set_date_param(val),
    )
    return (selected_date,)


@app.cell
def _(df, mo):
    # Pick the inverter(s)
    unique_inverters = df["serial_number"].unique().sort().to_list()
    selected_inverters = mo.ui.multiselect(options=unique_inverters, value=unique_inverters, label="Inverters to plot:")
    return (selected_inverters,)


@app.cell
def _(alt, df, mo, pl, selected_date, selected_inverters):
    data_to_plot = (
        df.filter(
            pl.col("period_end_time").dt.date() == selected_date.value,
            pl.col("serial_number").is_in(selected_inverters.value),
        )
        .with_columns((pl.col("joules_produced") / pl.col("period_duration").dt.total_seconds()).alias("watts"))
        .drop(["period_duration"])
    )

    # Altair doesn't recognise `zoneinfo.ZoneInfo(key='UTC')` as UTC.
    # I've submitted a PR to fix this: https://github.com/vega/altair/pull/3944
    # And we can't use `astimezone` in WASM because Polars tries to load a library that isn't available.
    x_axis_max_datetime = data_to_plot.select(pl.col("period_end_time").max().dt.replace_time_zone(None)).item()
    MIN_HOUR = 17
    if x_axis_max_datetime.hour < MIN_HOUR:
        x_axis_max_datetime = x_axis_max_datetime.replace(hour=MIN_HOUR)


    chart = (
        alt.Chart(data_to_plot)
        .mark_line(point=True, strokeWidth=2, strokeOpacity=0.7)
        .encode(
            x=alt.X(
                "period_end_time:T", title="Time", axis=alt.Axis(format="%H:%M", tickCount=alt.TimeInterval("hour"))
            ).scale(domainMax=x_axis_max_datetime),
            y=alt.Y("watts:Q", title="Power (Watts)", axis=alt.Axis(tickMinStep=50)).scale(domain=(0, 220)),
            color=alt.Color("serial_number:N", title="Micro-inverter Serial Number"),
            tooltip=[
                alt.Tooltip("period_end_time:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
                alt.Tooltip("serial_number:N", title="Serial Number"),
                alt.Tooltip("watts:Q", title="Power (Watts)", format=".2f"),
                alt.Tooltip("joules_produced:Q", title="Joules Produced"),
            ],
        )
        .configure_axis(
            grid=False,
            domain=False,
            ticks=True,
            labelFontSize=12,  # Increases font size (default is ~10)
            labelPadding=10,  # Adds space between axis and labels
            titleFontSize=14,  # Increases the "Timestamp" title size
            titlePadding=15,  # Adds space between label and title
        )
        .configure_view(strokeWidth=0)  # Removes the outer frame/box
        .properties(title="Power Output (Watts) of Micro-inverters over Time", height=600, width="container")
        .interactive()
    )

    mo.vstack([selected_date, selected_inverters, chart])
    return


if __name__ == "__main__":
    app.run()
