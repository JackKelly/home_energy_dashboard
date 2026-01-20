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
    from datetime import date, datetime, time, timezone, timedelta
    import pyarrow.parquet as pq
    from enum import StrEnum, auto
    from typing import NamedTuple, Final
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
def _(Final, io, pl, pq, requests):
    url: Final[str] = "https://data.jack-kelly.com/home-energy-data/solar-pv/year=2026/month=1/00000000.parquet"

    # As of Jan 2026, Polars WASM can't directly read Parquet over the network, so we use requests after patching
    response = requests.get(url)
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

    query_params = mo.query_params()
    if query_params_date_str := query_params.get("date"):
        try:
            query_params_date = date.fromisoformat(query_params_date_str)
        except ValueError:
            print(f"Failed to parse {query_params_date_str=} as a date. Using latest date instead.")
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
def _(get_date_state, latest_available_date, mo, set_date):
    today_button = mo.ui.button(
        label="Today",
        on_click=lambda _: set_date(latest_available_date),
        disabled=get_date_state() == latest_available_date,
    )
    return (today_button,)


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

    selected_inverters_multiselect = mo.ui.multiselect(options=inverters, value=inverters, label="Inverters to plot:")
    return inverters, selected_inverters_multiselect


@app.cell
def _(
    alt,
    date_picker,
    df,
    get_date_state,
    inverters,
    mo,
    next_day_button,
    pl,
    prev_day_button,
    selected_inverters_multiselect,
    today_button,
):
    selected_inverters = sorted(si for si in selected_inverters_multiselect.value)

    data_to_plot = (
        df.filter(
            pl.col("period_end_time").dt.date() == get_date_state(),
            pl.col("serial_number").is_in([inverter.serial_number for inverter in selected_inverters]),
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
                "period_end_time:T",
                title=f"{get_date_state()}",
                axis=alt.Axis(format="%H:%M", tickCount=alt.TimeInterval("hour")),
            ).scale(domainMax=x_axis_max_datetime),
            y=alt.Y("watts:Q", title="Power (Watts)", axis=alt.Axis(tickMinStep=50)).scale(domain=(0, 220)),
            color=alt.Color(
                "label:N",
                title="Inverter",
                scale=alt.Scale(
                    domain=[str(inverter) for inverter in selected_inverters],
                    range=[inverter.color for inverter in selected_inverters],
                ),
            ),
            tooltip=[
                alt.Tooltip("period_end_time:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
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
            title=f"Power output (Watts) of our 10 micro-inverters for {get_date_state()}", height=600, width="container"
        )
        .interactive()
    )

    mo.vstack(
        [
            mo.hstack(
                [
                    mo.hstack([prev_day_button, date_picker, next_day_button, today_button], justify="start"),
                    selected_inverters_multiselect,
                ]
            ),
            chart,
        ]
    )
    return


if __name__ == "__main__":
    app.run()
