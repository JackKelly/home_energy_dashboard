# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "altair==6.0.0",
#     "marimo>=0.19.7",
#     "polars==1.38.0",
#     "pyarrow==23.0.0",
#     "pyodide-http==0.2.2",
#     "requests==2.32.5",
#     "urlpath==2.0.0",
# ]
# ///

import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


@app.cell
def _():
    import io
    from datetime import date, datetime, time, timedelta
    from enum import StrEnum, auto
    from typing import Final, NamedTuple

    import altair as alt
    import marimo as mo
    import polars as pl
    import pyarrow.parquet as pq
    import pyodide_http
    from urlpath import URL

    pyodide_http.patch_all()

    import requests
    from requests.exceptions import HTTPError
    return (
        Final,
        HTTPError,
        NamedTuple,
        StrEnum,
        URL,
        alt,
        auto,
        date,
        datetime,
        io,
        mo,
        pl,
        pq,
        requests,
        time,
        timedelta,
    )


@app.cell
def _(Final, URL, date, io, pl, pq, requests):
    def load_parquet_for_month(d: date) -> pl.DataFrame:
        base_parquet_url: Final[URL] = URL("https://data.jack-kelly.com/home-energy-data/solar-pv/")
        parquet_url = base_parquet_url / f"year={d.year}" / f"month={d.month}" / "00000000.parquet"
        print("Attempting to load", parquet_url, "...")
        # As of Jan 2026, Polars WASM can't directly read Parquet over the network, so we use requests after `pyodide_http.patch_all()`
        response = requests.get(parquet_url)
        response.raise_for_status()
        table = pq.read_table(io.BytesIO(response.content))
        df = pl.from_arrow(table)
        print("Successfully loaded", parquet_url)
        return df
    return (load_parquet_for_month,)


@app.cell
def _(Final, HTTPError, date, datetime, load_parquet_for_month, pl):
    def load_archive() -> pl.DataFrame:
        start_date: Final[datetime] = date(2026, 1, 1)
        months = pl.date_range(start=start_date, end=date.today(), interval="1mo", eager=True)
        dfs = [pl.DataFrame()]
        for month in months[:-1]:
            try:
                dfs.append(load_parquet_for_month(month))
            except HTTPError as e:
                if e.response.status_code == 404:
                    print("Skipping missing parquet:", e.request.url)
                else:
                    raise
        return pl.concat(dfs, how="vertical")


    archive_df = load_archive()
    return (archive_df,)


@app.cell
def _(mo):
    # Load new data regularly
    refresh = mo.ui.refresh(default_interval="5m")
    return (refresh,)


@app.cell
def _(archive_df, date, load_parquet_for_month, pl, refresh):
    # Just referencing `refresh` will cause this cell to refresh if refresh is shown in the UI.
    refresh

    _df_of_this_months_data = load_parquet_for_month(date.today())
    df = pl.concat([archive_df, _df_of_this_months_data], how="vertical")
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

    multiselect_inverters = mo.ui.multiselect(options=all_inverters, value=all_inverters, label="Inverters to plot:")

    all_inverters_df = (
        pl.DataFrame(all_inverters)
        .cast({"serial_number": pl.Categorical})
        .hstack(pl.Series(name="label", values=[str(inverter) for inverter in all_inverters]).to_frame())
    )
    return all_inverters_df, multiselect_inverters


@app.cell
def _(
    all_inverters_df,
    alt,
    date,
    date_picker,
    datetime,
    df,
    get_date_state,
    latest_available_date,
    mo,
    multiselect_inverters,
    next_day_button,
    pl,
    prev_day_button,
    refresh,
    time,
    today_button,
):
    selected_inverters = sorted(si for si in multiselect_inverters.value)

    data_to_plot = (
        df.filter(
            pl.col("period_end_time").dt.date() == get_date_state(),
            pl.col("serial_number").is_in([inverter.serial_number for inverter in selected_inverters]),
        )
        .with_columns((pl.col("joules_produced") / pl.col("period_duration").dt.total_seconds()).alias("watts"))
        .drop(["period_duration"])  # Altair doesn't like the timedelta type.
        .join(all_inverters_df, on="serial_number")
    )

    midnight = datetime.combine(get_date_state(), time(hour=0))

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
            ).scale(
                domainMin=midnight.replace(hour=7, minute=30),
                domainMax=midnight.replace(hour=17, minute=0),
            ),
            y=alt.Y("watts:Q", title="Power (Watts)", axis=alt.Axis(tickMinStep=50)).scale(
                domain=(0, 250)
            ),  # Our inverters' max continuous output is 290 VA.
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
