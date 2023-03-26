from dash import Dash, Input, Output, State
import dash_bootstrap_components as dbc

from app_utilities import get_candle_data, load_screener_data, get_symbol_info
from app_graphing import create_figure
from screener import Screener

from indicators import make_indicators

from pandas import read_hdf
from pathlib import Path
from plotly.graph_objects import Figure
from app_layout import layout


app = Dash(name="Screener", suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP])


app.layout = layout()


@app.callback(
    Output("store-hdf-path", "data"),
    Input("store-config", "data"),
)
def load_hdf_path(config: dict) -> None:
    if not config:
        return ""
    return str(Path(config.get("config").get("paths").get("hdf_path")).absolute())


@app.callback([Output("checklist-screeners", "options")], Input("store-config", "data"))
def load_screeners(config: dict) -> list[dict]:
    if not config:
        return [None]
    screeners = config.get("config").get("screeners")
    if not screeners:
        return [None]
    return [screeners]


@app.callback([Output("checklist-indicators", "options")], Input("store-config", "data"))
def load_indicators(config: dict) -> list[dict]:
    if not config:
        return [None]
    indicators = config.get("config").get("indicators")
    if not indicators:
        return [None]
    return [indicators]


@app.callback([Output("checklist-indicators", "value")], Input("store-config", "data"))
def load_default_indicators(config: dict) -> list[dict]:
    if not config:
        return [None]
    indicators = config.get("config").get("indicators")
    if not indicators:
        return [None]
    return [list(indicators.keys())]


@app.callback(
    [Output("textarea-info", "value")],
    [Input("radio-symbols", "value")],
    [
        State("store-config", "data"),
        State("store-hdf-path", "data"),
    ],
)
def load_info(symbol: str, config: dict, path: Path) -> dict:
    if not symbol or not config or not path:
        return [""]
    return [
        get_symbol_info(path=path, symbol=symbol, table_key=config.get("config").get("hdf_keys").get("symbol_info"))
    ]


@app.callback(
    [Output("radio-symbols", "options")],
    [
        Input("store-hdf-path", "data"),
        State("store-config", "data"),
        Input("radio-period", "value"),
        Input("checklist-screeners", "value"),
        Input("slider-periods", "value"),
        Input("radio-trend", "value"),
    ],
)
def load_symbols(
    path: Path, config: dict, period: str, screeners: list[str], screener_lookback: int, trend: int
) -> list[list]:
    """Load the symbols"""
    if path is None or config is None or period is None or screeners is None or screener_lookback is None:
        return [[]]

    print(path)
    print(config)

    df = read_hdf(path, key=config.get("config").get("hdf_keys").get("screener_symbols"), mode="r").sort_values(
        "symbol", ascending=True
    )

    if len(screeners) == 0 or df.empty:
        return [df.symbol.drop_duplicates().to_list()]

    df_signals = load_screener_data(path=path, period=period, lookback=screener_lookback)

    if df_signals is None:
        return [df.symbol.drop_duplicates().to_list()]

    df_screener_symbols = (
        Screener().apply_screeners(screeners=screeners, df=df_signals, trend=trend).loc[:, ["symbol", "Date"]]
    )

    if not df.empty and not df_screener_symbols.empty and df_screener_symbols is not None and df is not None:
        print(df.columns)
        print(df_screener_symbols.columns)
        df = df.merge(df_screener_symbols, how="inner", on=["symbol"])
    else:
        return [[]]
    return [df.symbol.sort_values().to_list()]


@app.callback(
    [Output("graph-candlestick", "figure")],
    [
        Input("radio-symbols", "value"),
        Input("radio-period", "value"),
        Input("radio-candle", "value"),
        Input("checklist-indicators", "value"),
    ],
    [State("store-hdf-path", "data"), State("store-config", "data")],
)
def load_plot(symbol: str, period: str, candle: str, indicators: list, path: Path, config: dict) -> list[Figure]:

    df = get_candle_data(path=path, symbol=symbol, period=period, candle=candle)
    indicators = make_indicators(path=path, period=period, symbol=symbol, indicators=indicators)
    fig = create_figure(df=df, symbol=symbol, indicators=indicators)

    return [fig]


if __name__ == "__main__":
    app.run_server(debug=True)
