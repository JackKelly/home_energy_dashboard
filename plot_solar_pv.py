# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "altair==6.0.0",
#     "marimo>=0.19.4",
#     "polars==1.37.1",
#     "pyarrow==23.0.0",
#     "pydantic-ai==1.44.0",
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
    from enum import StrEnum, auto
    from typing import NamedTuple
    return NamedTuple, StrEnum, alt, auto, date, io, mo, pl, pq


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
def _(NamedTuple, StrEnum, auto, mo):
    # Pick inverters


    class Azimuth(StrEnum):
        SE = auto()
        SW = auto()
        NW = auto()


    class Inverter(NamedTuple):
        id: int
        serial_number: str
        azimuth: Azimuth
        desc: str
        color: str

        def __repr__(self) -> str:
            return f"{self.azimuth.upper()} ({self.desc})"


    inverters = [
        # South east:
        Inverter(1, "482202080061", Azimuth.SE, "top NE", "#4682B4"),
        Inverter(2, "482202080196", Azimuth.SE, "bottom SW", "#6495ED"),
        Inverter(3, "482202080253", Azimuth.SE, "bottom middle", "#1E90FF"),
        Inverter(4, "482202079929", Azimuth.SE, "bottom NE", "#87CEFA"),
        # South west:
        Inverter(5, "482202079973", Azimuth.SW, "top middle landscape", "#2E8B57"),
        Inverter(6, "482202080024", Azimuth.SW, "bottom SE", "#008000"),
        Inverter(7, "482202079731", Azimuth.SW, "bottom middle", "#3CB371"),
        Inverter(8, "482202079726", Azimuth.SW, "bottom NW", "#90EE90"),
        # North west:
        Inverter(9, "482202079737", Azimuth.NW, "upper NE?", "#FFA07A"),
        Inverter(10, "482202080303", Azimuth.NW, "lower SW?", "#FF6347"),
    ]

    selected_inverters = mo.ui.multiselect(options=inverters, value=inverters, label="Inverters to plot:")
    return inverters, selected_inverters


@app.cell
def _():
    return


@app.cell
def _(alt, df, inverters, mo, pl, selected_date, selected_inverters):
    data_to_plot = (
        df.filter(
            pl.col("period_end_time").dt.date() == selected_date.value,
            pl.col("serial_number").is_in([inverter.serial_number for inverter in selected_inverters.value]),
        )
        .with_columns((pl.col("joules_produced") / pl.col("period_duration").dt.total_seconds()).alias("watts"))
        .drop(["period_duration"])
        .join(
            pl.DataFrame(inverters)
            .cast({"serial_number": pl.Categorical})
            .hstack(pl.Series(name="label", values=[str(inverter) for inverter in inverters]).to_frame()),
            on="serial_number",
        )
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
        .mark_line(
            point=True,
            strokeWidth=2,
            strokeOpacity=0.7,
            interpolate="monotone",
        )
        .encode(
            x=alt.X(
                "period_end_time:T", title="Time", axis=alt.Axis(format="%H:%M", tickCount=alt.TimeInterval("hour"))
            ).scale(domainMax=x_axis_max_datetime),
            y=alt.Y("watts:Q", title="Power (Watts)", axis=alt.Axis(tickMinStep=50)).scale(domain=(0, 220)),
            color=alt.Color(
                "label:N",
                title="Inverter",
                scale=alt.Scale(
                    domain=[str(inverter) for inverter in selected_inverters.value],
                    range=[inverter.color for inverter in selected_inverters.value],
                ),
            ),
            tooltip=[
                alt.Tooltip("period_end_time:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
                alt.Tooltip("label:N", title="Label"),
                alt.Tooltip("watts:Q", title="Power (Watts)", format=".2f"),
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


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
