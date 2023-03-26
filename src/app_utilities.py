# import dash_bootstrap_components as dbc
# import dash_core_components as dcc
# from dash import dcc
# from dash.dependencies import Output, Input, State
# from plotly.subplots import make_subplots
# import plotly.graph_objects as go

from pandas import HDFStore, DataFrame, read_hdf
from pathlib import Path
import yaml


def load_config(path: Path) -> dict:
    """Load the configuration file"""
    return yaml.load(open(path, "r"), Loader=yaml.Loader)


def open_hdf(path: Path):
    # data_path = Path("./data/").absolute()
    return HDFStore(path.absolute(), mode="r")


def get_candle_data(path: Path, symbol: str, period: str, candle: str) -> DataFrame:

    if not path:
        return None  # DataFrame({"Date": [], "Open": [], "High": [], "Low": [], "Close": [], "Volume": []})

    ohlc_key = f"/OHLCV/{period}/"
    print("OHLC_key", ohlc_key)
    ohlc_df = read_hdf(path_or_buf=str(path), key=ohlc_key, mode="r").query(f"symbol == '{symbol}'")
    if candle == "ohlc":
        return ohlc_df.sort_values("Date", ascending=False).reset_index(drop=True)  # .drop("Index", axis=1)

    if candle == "ha":
        ha_key = f"/Signals/{period}/heikin_ashi/"
        ha_df = read_hdf(path_or_buf=str(path), key=ha_key, mode="r").query(f"symbol == '{symbol}'")
        print("ha_df", ha_df)
        print("ohlc_df", ohlc_df)

        return (
            ha_df.drop("Volume", axis=1)
            .merge(ohlc_df, how="inner", on=["Date", "symbol"])
            .loc[:, ["Date", "symbol", "HA_Open", "HA_High", "HA_Low", "HA_Close", "Volume"]]
            .rename(
                columns={
                    "HA_Open": "Open",
                    "HA_High": "High",
                    "HA_Low": "Low",
                    "HA_Close": "Close",
                }
            )
            .sort_values("Date", ascending=False)
            .reset_index(drop=True)
        )


def load_screener_data(path: Path, period: str, lookback: int = 5) -> DataFrame:

    if not path:
        return None  # DataFrame({"Date": [], "Open": [], "High": [], "Low": [], "Close": [], "Volume": []})

    screener_key = f"Signals/{period}/merged"

    return (
        read_hdf(path_or_buf=str(path), key=screener_key, mode="r")
        .sort_values("Date", ascending=True)
        .groupby("symbol")
        .tail(lookback)
    )


def get_symbol_info(path: Path, table_key: str, symbol: str) -> str:
    if not path or not table_key:
        return ""

    row = read_hdf(path_or_buf=str(path), key=table_key, mode="r").query(f'symbol == "{symbol}"').iloc[0]
    return "\n".join([f"{k}: {v}" for k, v in row.to_dict().items()])


# def screen_symbols(path: Path, period: str, screeners: list[str], lookback: int) -> list[str]:

#     if 'ha' in screeners:


# USFEDHOLIDAYS = USFederalHolidayCalendar()
# USFEDHOLIDAYS.merge(GoodFriday, inplace=True)
# MARKET_HOLIDAYS = [
#     i.astype(datetime.datetime).strftime("%Y-%m-%d")
#     for i in list(pd.offsets.CustomBusinessDay(calendar=USFEDHOLIDAYS).holidays)
# ][200:700]

# FUNCTION_LOOKUP = {
#     1: "TIME_SERIES_MONTHLY_ADJUSTED",
#     2: "TIME_SERIES_WEEKLY_ADJUSTED",
#     3: "TIME_SERIES_DAILY_ADJUSTED",
#     4: "TIME_SERIES_INTRADAY",
#     5: "TIME_SERIES_INTRADAY",
#     6: "TIME_SERIES_INTRADAY",
#     7: "TIME_SERIES_INTRADAY",
# }

# INTERVAL_LOOKUP = {4: "60min", 5: "30min", 6: "15min", 7: "5min"}


# def make_rangebreaks(function):
#     """Set the range breaks for x axis"""
#     if "INTRADAY" in function:
#         return [
#             dict(bounds=["sat", "mon"]),  # hide weekends
#             dict(values=MARKET_HOLIDAYS),
#             dict(pattern="hour", bounds=[16, 9.5]),  # hide Christmas and New Year's
#         ]

#     if "DAILY" in function:
#         return [
#             dict(bounds=["sat", "mon"]),  # ,  # hide weekends
#             dict(values=MARKET_HOLIDAYS),
#         ]
#     return None


# def process_symbol_input(symbols):
#     symbols = [
#         i.strip(" ").upper()
#         for i in symbols.replace("\n", " ")
#         .replace(",", " ")
#         .replace(";", " ")
#         .replace("'", " ")
#         .replace('"', " ")
#         .strip(" ")
#         .split(" ")
#     ]
#     symbols = [i for i in symbols if i != ""]
#     return sorted(list({i for i in symbols if i is not None}))


# def get_price_data(n_clicks, symbol, function, interval, no_api=False):
#     """Get the data from main
#     """
#     if (('INTRADAY' in function) & (interval is None)) | \
#             (n_clicks == 0) | \
#             (symbol is None) | \
#             (function is None):
#         return pd.DataFrame({'datetime': [],
#                              'open': [],
#                              'high': [],
#                              'low': [],
#                              'close': [],
#                              'volume': []})

#     return main(
#         {'function': [function],
#          'symbol': [symbol.upper()],
#          'interval': [interval],
#          'config': None,
#          'get_all': False,
#          'no_return': False,
#          'force_update': False,
#          'data_status': False,
#          'get_symbols': False,
#          'no_api': no_api})['prices']
