from dash import Dash, Input, Output, State
import dash_bootstrap_components as dbc

from app_utilities import get_candle_data, load_screener_data, get_symbol_info
from app_graphing import create_figure
from screener import Screener

from indicators import make_indicators
from lines import make_lines
from patterns import get_patterns

from pandas import read_parquet
from pathlib import Path
from plotly.graph_objects import Figure
from app_layout import layout


app = Dash(name="Screener", suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP])


app.layout = layout()


@app.callback(
    Output("store-data-path", "data"),
    Input("store-config", "data"),
)
def load_data_path(config: dict) -> str:
    if not config:
        return ""
    return config.get("config").get("paths").get("data_path")
    # print(data_path)
    # path = str(Path(data_path).absolute())
    # print("path", path)
    # return path


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


@app.callback([Output("checklist-lines", "options")], Input("store-config", "data"))
def load_lines(config: dict) -> list[dict]:
    if not config:
        return [None]

    lines = config.get("config").get("lines")

    if not lines:
        return [None]

    return [list(lines.keys())]


@app.callback(
    [Output("textarea-info", "value")],
    [Input("radio-symbols", "value")],
    [
        State("store-config", "data"),
        State("store-data-path", "data"),
    ],
)
def load_info(symbol: str, config: dict, path: str) -> dict:
    if not symbol or not config or not path:
        return [""]
    return [get_symbol_info(path=Path(path), symbol=symbol)]


@app.callback(
    [Output("radio-symbols", "options")],
    [
        Input("store-data-path", "data"),
        State("store-config", "data"),
        Input("radio-period", "value"),
        Input("checklist-screeners", "value"),
        Input("slider-periods", "value"),
        Input("radio-trend", "value"),
    ],
)
def load_symbols(
    path: str, config: dict, period: str, screeners: list[str], screener_lookback: list[int], trend: int
) -> list[list]:
    """Load the symbols"""
    if path is None or config is None or period is None:
        return [[]]

    df = read_parquet(path=Path(path) / "symbols" / "data.parquet").sort_values("symbol", ascending=True)

    if len(screeners) == 0:
        return [df.symbol.drop_duplicates().sort_values().to_list()]

    df_signals = load_screener_data(path=Path(path), period=period, lookback=screener_lookback)
    print(df_signals, screener_lookback, "df_signals")
    if df_signals is None:
        return [[]]
        # return [df.symbol.drop_duplicates().to_list()]

    df_screener_symbols = (
        Screener().apply_screeners(screeners=screeners, df=df_signals, trend=trend).loc[:, ["symbol"]].drop_duplicates()
    )

    if df_screener_symbols.empty or df_screener_symbols is None:
        return [[]]
        # print(df.columns)
        # print(df_screener_symbols.columns)
        # df = df.merge(df_screener_symbols, how="inner", on=["symbol"])
    return [df_screener_symbols.symbol.drop_duplicates().sort_values().to_list()]

    # return [df.symbol.drop_duplicates().sort_values().to_list()]


@app.callback(
    [Output("graph-candlestick", "figure")],
    [
        Input("button-refresh", "n_clicks"),
    ],
    [
        State("radio-symbols", "value"),
        State("radio-period", "value"),
        State("radio-candle", "value"),
        State("checklist-indicators", "value"),
        State("checklist-lines", "value"),
        State("checklist-patterns", "value"),
        State("store-data-path", "data"),
        # State("store-config", "data"),
    ],
)
def load_plot(
    n_clicks: int,
    symbol: str,
    period: str,
    candle: str,
    indicators: list,
    lines: list,
    show_patterns: bool,
    path: str,
    # config: dict,
) -> list[Figure]:
    print("show_patterns", show_patterns)

    if path is None:
        return [create_figure(df=None, symbol=None, indicators=None, lines=None, patterns=None)]

    df = get_candle_data(path=Path(path), symbol=symbol, period=period, candle=candle)
    indicators = make_indicators(path=Path(path), period=period, symbol=symbol, indicators=indicators)
    lines = make_lines(path=Path(path), period=period, symbol=symbol, lines=lines)
    patterns = get_patterns(path=Path(path), symbol=symbol, period=period) if len(show_patterns) > 0 else None
    return [create_figure(df=df, symbol=symbol, indicators=indicators, lines=lines, patterns=patterns)]


if __name__ == "__main__":
    app.run_server(debug=True)
