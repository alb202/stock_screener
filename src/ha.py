from pandas import DataFrame
from numpy import where
from ta_utils import validate_columns


# def heikin_ashi(df: pls.DataFrame) -> pls.DataFrame:
#     """Heikin Ashi Algorithm"""

#     df = pls.from_pandas(df)

#     cols = ["symbol", "Open", "High", "Low", "Close", "Volume"]

#     df = df.sort("Date").select(["Date"] + cols)  # .reset_index(drop=True)

#     df = df.with_columns(
#         pls.col("HA_Close"), df.select(["Open", "High", "Low", "Close"]).apply(lambda cols: sum(cols) / 4)
#     )

#     df["HA_Open"] = pls.Series.from_vec([float(0)] * len(df))

#     df["HA_Open"].set(0, df["Open"].get(0))

#     for index in range(1, len(df)):
#         df["HA_Open"].set(index, (df["HA_Open"].get(index - 1) + df["HA_Close"].get(index - 1)) / 2)

#     df["HA_High"] = df.select(["HA_Open", "HA_Close", "Low", "High"]).apply(lambda cols: cols.max())

#     df["HA_Low"] = df.select(["HA_Open", "HA_Close", "Low", "High"]).apply(lambda cols: cols.min())

#     df = df.rename("Heiken_Ashi")

#     return df.select(["Date", "symbol", "HA_Open", "HA_High", "HA_Low", "HA_Close", "Volume"])


def heikin_ashi(df: DataFrame) -> DataFrame:
    """Heikin Ashi Algorithm"""

    cols = ["symbol", "Date", "Open", "High", "Low", "Close", "Volume"]

    validate_columns(df_columns=df.columns, required_columns=cols)

    df = df.sort_values("Date", ascending=True).loc[:, ["Date"] + cols].reset_index(drop=True)

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

    # df = pls.DataFrame(df)

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


# from pandas import read_parquet
# from pathlib import Path

# data_path = Path("/Users/ab/Data/stock_data/").absolute()

# df_ = read_parquet(data_path / "OHLCV/D/data.parquet")

# print(df_)

# # ha = heikin_ashi(df=df_.filter(pls.col("symbol") == "MSFT")).to_)
# ha = heikin_ashi(df=df_.query("symbol == 'MSFT'"))

# print(ha)
