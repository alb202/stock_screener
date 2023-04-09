from pandas import HDFStore, DataFrame, read_hdf
from pathlib import Path
import yaml


def load_config(path: Path) -> dict:
    """Load the configuration file"""
    return yaml.load(open(path, "r"), Loader=yaml.Loader)


def open_hdf(path: Path):
    return HDFStore(path.absolute(), mode="r")


def get_candle_data(path: Path, symbol: str, period: str, candle: str) -> DataFrame:

    if not path:
        return None

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


def load_screener_data(path: Path, period: str, lookback: list[int] = [0, 1]) -> DataFrame:

    if not path:
        return None

    screener_key = f"Signals/{period}/merged"
    # print("lookback", lookback)
    return (
        read_hdf(path_or_buf=str(path), key=screener_key, mode="r")
        .sort_values("Date", ascending=True)
        .groupby("symbol")
        .tail(lookback[1])
        .groupby("symbol")
        .head(lookback[1] - lookback[0])
        .drop("index", axis=1)
    )


def get_symbol_info(path: Path, table_key: str, symbol: str) -> str:
    if not path or not table_key:
        return ""

    row = read_hdf(path_or_buf=str(path), key=table_key, mode="r").query(f'symbol == "{symbol}"').iloc[0]
    return "\n".join([f"{k}: {v}" for k, v in row.to_dict().items()])
