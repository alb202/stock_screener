from pandas import DataFrame, read_parquet, read_hdf, HDFStore, to_datetime, read_parquet
from pathlib import Path
from numpy import where


def make_indicators(path: Path, period: str, symbol: str, indicators: list[str]):
    if indicators is None or path is None or period is None or symbol is None:
        return {}

    indicator_list = {}

    if "ha" in indicators:
        indicator_list["ha"] = (
            ha_indicator(path=path, period=period, symbol=symbol)
            .sort_values("Date", ascending=True)
            .reset_index(drop=True)
        )
    if "st" in indicators:
        indicator_list["st"] = (
            supertrend_indicator(path=path, period=period, symbol=symbol)
            .sort_values("Date", ascending=True)
            .reset_index(drop=True)
        )
    if "macd" in indicators:
        indicator_list["macd"] = (
            macd_indicator(path=path, period=period, symbol=symbol)
            .sort_values("Date", ascending=True)
            .reset_index(drop=True)
        )
    if "srsi" in indicators:
        indicator_list["stochastic_rsi"] = (
            stochastic_rsi_indicator(path=path, period=period, symbol=symbol)
            .sort_values("Date", ascending=True)
            .reset_index(drop=True)
        )
    if "sma" in indicators:
        indicator_list["SMA"] = (
            sma_crossover_indicator(path=path, period=period, symbol=symbol)
            .sort_values("Date", ascending=True)
            .reset_index(drop=True)
        )
    return indicator_list


def ha_indicator(path: Path, period: str, symbol: str):
    """Heikin Ashi indicator"""
    df = (
        read_parquet(path=path / f"signals/{period}/heikin_ashi.parquet")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date", "HA_Trend"]]
        .rename(columns={"HA_Trend": "value"})
    )
    df.name = "Heikin Ashi"
    return df


def supertrend_indicator(path: Path, period: str, symbol: str):
    """Supertrend indicator"""
    df = (
        read_parquet(path=path / f"signals/{period}/supertrend.parquet")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date", "supertrend"]]
        .rename(columns={"supertrend": "value"})
    )
    df.name = "Supertrend"
    return df


def macd_indicator(path: Path, period: str, symbol: str):
    """MACD trend indicator"""
    df = (
        read_parquet(path=path / f"signals/{period}/macd.parquet")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date", "macdtrend"]]
        .rename(columns={"macdtrend": "value"})
    )
    df.name = "MACD"
    return df


def stochastic_rsi_indicator(path: Path, period: str, symbol: str):
    """Stochastic RSI trend indicator"""
    df = (
        read_parquet(path=path / f"signals/{period}/stochastic_rsi.parquet")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date", "stochastic_rsi_K", "stochastic_rsi_D", "stochastic_rsi_crossover"]]
        .rename(columns={"stochastic_rsi_crossover": "value"})
    )
    df.name = "Stochastic RSI"
    return df


def sma_crossover_indicator(path: Path, period: str, symbol: str):
    """SMA Crossover indicator"""
    df = (
        read_parquet(path=path / f"signals/{period}/sma.parquet")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date", "sma20", "sma50"]]
    )
    df["value"] = where(df["sma20"] >= df["sma50"], 1, -1)
    df.name = "SMA 20/50 Crossover"
    return df
