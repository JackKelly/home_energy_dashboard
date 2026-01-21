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
    import io
    from datetime import date, timedelta
    from enum import StrEnum, auto
    from typing import Final, NamedTuple

    import altair as alt
    import marimo as mo
    import polars as pl
    import pyarrow.parquet as pq
    import pyodide_http

    pyodide_http.patch_all()

    import requests

    return (
        Final,
        NamedTuple,
        StrEnum,
        alt,
        auto,
        date,
        io,
        mo,
        pl,
        pq,
        requests,
        timedelta,
    )


@app.cell
def _(Final):
    PARQUET_URL: Final[str] = (
        "https://data.jack-kelly.com/home-energy-data/solar-pv/year=2026/month=1/00000000.parquet"
    )
    return (PARQUET_URL,)


@app.cell
def _(mo):
    # Load new data regularly
    refresh = mo.ui.refresh(default_interval="5m")
    return (refresh,)


@app.cell
def _(PARQUET_URL: "Final[str]", io, pl, pq, refresh, requests):
    # Just referencing `refresh` will cause this cell to refresh if refresh is shown in the UI.
    refresh

    # As of Jan 2026, Polars WASM can't directly read Parquet over the network, so we use requests after patching
    response = requests.get(PARQUET_URL)
    response.raise_for_status()

    table = pq.read_table(io.BytesIO(response.content))
    df = pl.from_arrow(table)
    return (df,)


@app.cell
def _(date, df, mo):
    # Date picker
    dates = df["period_end_time"].dt.date().unique().sort(descending=True)
    latest_available_date = dates[0]

    # Create a state to hold the current date
    get_date_state, set_date_state = mo.state(latest_available_date)

    # Read date from app URL
    query_params = mo.query_params()
    if query_params_date_str := query_params.get("date"):
        try:
            query_params_date = date.fromisoformat(query_params_date_str)
        except ValueError:
            print(f"Failed to parse {query_params_date_str=} as a date.")
        else:
            if dates[-1] <= query_params_date <= dates[0]:
                set_date_state(query_params_date)
            else:
                print(f"{query_params_date=} is out of range.")
    return (
        dates,
        get_date_state,
        latest_available_date,
        query_params,
        set_date_state,
    )


@app.cell
def _(date, get_date_state, query_params, set_date_state, timedelta):
    # Helper functions for the date picker UI elements
    def set_date(new_date: date):
        set_date_state(new_date)
        try:
            query_params.set("date", str(new_date))  # This fails in molab
        except:
            print("Failed to set date in query_params")

    def shift_day(delta):
        new_date = get_date_state() + timedelta(days=delta)
        set_date(new_date)

    return set_date, shift_day


@app.cell
def _(dates, get_date_state, mo, set_date):
    date_picker = mo.ui.date.from_series(
        series=dates,
        value=get_date_state(),
        label="Select Date",
        on_change=lambda val: set_date(val),
    )
    return (date_picker,)


@app.cell
def _(dates, get_date_state, mo, shift_day):
    prev_day_button = mo.ui.button(
        label="Previous Day",
        on_click=lambda _: shift_day(-1),
        disabled=get_date_state() <= dates[-1],
    )
    return (prev_day_button,)


@app.cell
def _(get_date_state, latest_available_date, mo, shift_day):
    next_day_button = mo.ui.button(
        label="Next Day",
        on_click=lambda _: shift_day(1),
        disabled=get_date_state() >= latest_available_date,
    )
    return (next_day_button,)


@app.cell
def _(date, get_date_state, mo, set_date):
    today_button = mo.ui.button(
        label="Today",
        on_click=lambda _: set_date(date.today()),
        disabled=get_date_state() == date.today(),
    )
    return (today_button,)


@app.cell
def _(NamedTuple, StrEnum, auto, mo, pl):
    # Pick inverters

    class Azimuth(StrEnum):
        SE = auto()
        SW = auto()
        NW = auto()

    class Inverter(NamedTuple):
        id: int  # My own ID. Just to help keep the inverters in a semantic order.
        serial_number: str
        azimuth: Azimuth
        description: str
        color: str

        def __repr__(self) -> str:
            return f"{self.azimuth.upper()} ({self.description})"

    all_inverters = [
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

    multiselect_inverters = mo.ui.multiselect(
        options=all_inverters, value=all_inverters, label="Inverters to plot:"
    )

    all_inverters_df = (
        pl.DataFrame(all_inverters)
        .cast({"serial_number": pl.Categorical})
        .hstack(
            pl.Series(
                name="label", values=[str(inverter) for inverter in all_inverters]
            ).to_frame()
        )
    )
    return all_inverters_df, multiselect_inverters


@app.cell
def _(
    all_inverters_df,
    alt,
    date,
    date_picker,
    df,
    get_date_state,
    latest_available_date,
    mo,
    multiselect_inverters,
    next_day_button,
    pl,
    prev_day_button,
    refresh,
    today_button,
):
    selected_inverters = sorted(si for si in multiselect_inverters.value)

    data_to_plot = (
        df.filter(
            pl.col("period_end_time").dt.date() == get_date_state(),
            pl.col("serial_number").is_in(
                [inverter.serial_number for inverter in selected_inverters]
            ),
        )
        .with_columns(
            (
                pl.col("joules_produced") / pl.col("period_duration").dt.total_seconds()
            ).alias("watts")
        )
        .drop(["period_duration"])  # Altair doesn't like the timedelta type.
        .join(all_inverters_df, on="serial_number")
    )

    # Altair doesn't recognise `zoneinfo.ZoneInfo(key='UTC')` as UTC.
    # My PR to fix this has been merged: https://github.com/vega/altair/pull/3944
    # TODO(Jack): When Altair is next released, we can get rid of `replace_time_zone(None)`.
    # And we can't use `astimezone` in WASM because Polars tries to load a library that isn't available.
    x_axis_max_datetime = data_to_plot.select(
        pl.col("period_end_time").max().dt.replace_time_zone(None)
    ).item()
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
                "period_end_time:T",
                title=f"{get_date_state()}",
                axis=alt.Axis(format="%H:%M", tickCount=alt.TimeInterval("hour")),
            ).scale(domainMax=x_axis_max_datetime),
            y=alt.Y(
                "watts:Q", title="Power (Watts)", axis=alt.Axis(tickMinStep=50)
            ).scale(domain=(0, 220)),
            color=alt.Color(
                "label:N",
                title="Inverter",
                scale=alt.Scale(
                    domain=[str(inverter) for inverter in selected_inverters],
                    range=[inverter.color for inverter in selected_inverters],
                ),
            ),
            tooltip=[
                alt.Tooltip(
                    "period_end_time:T", title="Time", format="%Y-%m-%d %H:%M:%S"
                ),
                alt.Tooltip("label:N", title="Label"),
                alt.Tooltip("watts:Q", title="Watts", format=".2f"),
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
        .properties(
            title=f"Power output (Watts) of our 10 micro-inverters for {get_date_state()}",
            height=600,
            width="container",
        )
        .interactive()
    )

    date_selector_ui_elements = [prev_day_button, date_picker, next_day_button]

    # Only show the "today" button if today is in the data:
    if latest_available_date == date.today():
        date_selector_ui_elements.append(today_button)

    top_row = mo.hstack(
        [
            mo.hstack(date_selector_ui_elements, justify="start"),
        ]
        # Only refresh if we're showing today:
        + ([refresh] if get_date_state() == date.today() else [])
        + [
            multiselect_inverters,
        ],
    )

    mo.vstack([top_row, chart])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
