from pandas import DataFrame
from numpy import where


def heikin_ashi(df: DataFrame) -> DataFrame:
    """Heikin Ashi Algorithm"""
    cols = ["symbol", "Open", "High", "Low", "Close", "Volume"]
    date_col = "Date"
    df = df.sort_values(date_col, ascending=True).loc[:, [date_col] + cols].reset_index(drop=True)

    df["HA_Close"] = df.loc[:, ["Open", "High", "Low", "Close"]].apply(sum, axis=1).divide(4)

    df["HA_Open"] = float(0)
    df.loc[0, "HA_Open"] = df.loc[0, "Open"]
    for index in range(1, len(df)):
        df.at[index, "HA_Open"] = (df["HA_Open"][index - 1] + df["HA_Close"][index - 1]) / 2

    df["HA_High"] = df.loc[:, ["HA_Open", "HA_Close", "Low", "High"]].apply(max, axis=1)
    df["HA_Low"] = df.loc[:, ["HA_Open", "HA_Close", "Low", "High"]].apply(min, axis=1)
    df.name = "Heiken_Ashi"
    return df.loc[:, ["Date", "symbol", "HA_Open", "HA_High", "HA_Low", "HA_Close", "Volume"]]


def heikin_ashi_signals(df: DataFrame) -> DataFrame:
    """Heiken Ashi Signals"""
    is_increasing = df["HA_Close"] > df["HA_Open"]
    is_increasing_yday = df["HA_Close"].shift(1) > df["HA_Open"].shift(1)
    buy_signal = (is_increasing & ~is_increasing_yday).replace({True: 1, False: 0})
    sell_signal = (~is_increasing & is_increasing_yday).replace({True: -1, False: 0})

    df["HA_Signal"] = where(buy_signal == 1, buy_signal, sell_signal)
    df["HA_Trend"] = where(df["HA_Close"] >= df["HA_Open"], 1, -1)
    df["HA_Streak"] = df["HA_Trend"].groupby((df["HA_Trend"] != df["HA_Trend"].shift()).cumsum()).cumcount() + 1
    df.name = "Heiken_Ashi_Signals"
    return df.loc[
        :,
        ["Date", "symbol", "HA_Open", "HA_High", "HA_Low", "HA_Close", "Volume", "HA_Signal", "HA_Trend", "HA_Streak"],
    ]

