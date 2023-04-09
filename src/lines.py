from pandas import read_hdf  # DataFrame, read_parquet, , HDFStore, to_datetime
from pathlib import Path

# from numpy import where


def make_lines(path: Path, period: str, symbol: str, lines: list[str]) -> dict:

    if lines is None or path is None or period is None or symbol is None:
        return None
    if len(lines) == 0:
        return None

    sma_df = (
        read_hdf(path_or_buf=str(path), key=f"/Signals/{period}/sma")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date"] + [line for line in lines if "sma" in line]]
    )

    ema_df = (
        read_hdf(path_or_buf=str(path), key=f"/Signals/{period}/ema")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date"] + [line for line in lines if "ema" in line]]
    )
    df = sma_df.merge(ema_df, how="inner", on="Date")
    print(df)
    return df

    # if "ma20" in lines:

    #     line_list["sma"] = (
    #         simple(path=path, period=period, symbol=symbol).sort_values("Date", ascending=True).reset_index(drop=True)
    #     )
    # if "st" in indicators:
    #     indicator_list["st"] = (
    #         supertrend_indicator(path=path, period=period, symbol=symbol)
    #         .sort_values("Date", ascending=True)
    #         .reset_index(drop=True)
    #     )
    # if "macd" in indicators:
    #     indicator_list["macd"] = (
    #         macd_indicator(path=path, period=period, symbol=symbol)
    #         .sort_values("Date", ascending=True)
    #         .reset_index(drop=True)
    #     )
    # if "srsi" in indicators:
    #     indicator_list["stochastic_rsi"] = (
    #         stochastic_rsi_indicator(path=path, period=period, symbol=symbol)
    #         .sort_values("Date", ascending=True)
    #         .reset_index(drop=True)
    #     )
    # return indicator_list


# def ha_indicator(path: Path, period: str, symbol: str):
#     """Heikin Ashi indicator"""
#     df = (
#         read_hdf(path_or_buf=str(path), key=f"/Signals/{period}/heikin_ashi")
#         .query(f'symbol == "{symbol}"')
#         .loc[:, ["Date", "HA_Trend"]]
#         .rename(columns={"HA_Trend": "value"})
#     )
#     df.name = "Heikin Ashi"
#     return df


# def supertrend_indicator(path: Path, period: str, symbol: str):
#     """Supertrend indicator"""
#     df = (
#         read_hdf(path_or_buf=str(path), key=f"/Signals/{period}/supertrend")
#         .query(f'symbol == "{symbol}"')
#         .loc[:, ["Date", "supertrend"]]
#         .rename(columns={"supertrend": "value"})
#     )
#     df.name = "Supertrend"
#     return df


# def macd_indicator(path: Path, period: str, symbol: str):
#     """MACD trend indicator"""
#     df = (
#         read_hdf(path_or_buf=str(path), key=f"/Signals/{period}/macd")
#         .query(f'symbol == "{symbol}"')
#         .loc[:, ["Date", "macdtrend"]]
#         .rename(columns={"macdtrend": "value"})
#     )
#     df.name = "MACD"
#     return df


# def stochastic_rsi_indicator(path: Path, period: str, symbol: str):
#     """Stochastic RSI trend indicator"""
#     df = (
#         read_hdf(path_or_buf=str(path), key=f"/Signals/{period}/stochastic_rsi")
#         .query(f'symbol == "{symbol}"')
#         .loc[:, ["Date", "stochastic_rsi_K", "stochastic_rsi_D", "stochastic_rsi_crossover"]]
#         .rename(columns={"stochastic_rsi_crossover": "value"})
#     )
#     df.name = "Stochastic RSI"
#     return df
