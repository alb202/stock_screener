# from pandas import HDFStore
# from pathlib import Path
from pandas import DataFrame  # , read_parquet, read_hdf, HDFStore, to_datetime

# from datetime import datetime
# from utilities import back_in_time
# from pathlib import Path
# from typing import Optional


# def make_signal_table(hdf_path: Path, period: str, signals: list[str]) -> DataFrame:
#     """Merge the signal data for each period"""
#     # signals = ["heikin_ashi", "bollinger_bands", "rsi", "macd", "natr", "supertrend"]

#     df = read_hdf(path_or_buf=hdf_path, key=f"/OHLCV/{period}/")
#     for signal in signals:
#         df = df.merge(
#             read_hdf(path_or_buf=hdf_path, key=f"/Signals/{period}/{signal}"),
#             how="left",
#             on=["Date", "symbol"],
#         )
#     return df.loc[:, ["Date", "symbol"] + [col for col in df.columns if col not in ["symbol", "Date", "index"]]]


class Screener:
    def __init__(
        self,
        # path: Optional[Path] = None,
        #  merge_data: bool = False
    ):
        pass
        # self.hdf_path = path
        # self.keys = self.get_hdf_keys()
        # self.info = self.get_stock_info()
        # self.symbols = self.info.symbol.drop_duplicates().to_numpy()
        # if merge_data:
        #     self.daily = self.merge_signals(period="D")
        #     self.weekly = self.merge_signals(period="W")
        #     self.monthly = self.merge_signals(period="M")
        # else:
        #     self.daily = None
        #     self.weekly = None
        #     self.monthly = None

    # def get_hdf_keys(self) -> list:
    #     """Get the keys from the HDF file"""
    #     store = HDFStore(path=self.hdf_path)
    #     keys = store.keys()
    #     store.close()
    #     return keys

    # def get_stock_info(self) -> DataFrame:
    #     """Get the stock info from the HDF file"""
    #     return read_hdf(path_or_buf=self.hdf_path, key="Info")

    # def merge_signals(self, period: str) -> DataFrame:
    #     """Merge the signal data for each period"""
    #     signals = ["heikin_ashi", "bollinger_bands", "rsi", "macd", "natr", "supertrend"]

    #     df = read_hdf(path_or_buf=self.hdf_path, key=f"/OHLCV/{period}/")
    #     for signal in signals:
    #         df = df.merge(
    #             read_hdf(path_or_buf=self.hdf_path, key=f"/Signals/{period}/{signal}"),
    #             how="left",
    #             on=["Date", "symbol"],
    #         )
    #     return df.loc[:, ["Date", "symbol"] + [col for col in df.columns if col not in ["symbol", "Date", "index"]]]

    # def get_screener_data(self, period: str, num_periods: int = 20) -> DataFrame:
    #     """Get the subset of screener data"""
    #     if period == "D" and self.daily is not None:
    #         cutoff_date = back_in_time(days=num_periods)
    #         return self.daily.query(f"Date > '{cutoff_date}'")
    #     if period == "W" and self.weekly is not None:
    #         cutoff_date = back_in_time(weeks=num_periods)
    #         return self.weekly.query(f"Date > '{cutoff_date}'")
    #     if period == "M" and self.monthly is not None:
    #         cutoff_date = back_in_time(days=num_periods * 30)
    #         return self.monthly.query(f"Date > '{cutoff_date}'")
    #     return None
    #     # print(type(date))
    #     # df = df.sort_values("Date").groupby("symbol").tail(30)
    #     # print(df)
    #     # return df

    def apply_screeners(self, df: DataFrame, screeners: list[str], num_periods: int = 1) -> DataFrame:
        """Apply screeners"""
        if df is None:
            # df = self.get_screener_data(period=period, num_periods=num_periods)
            return None
        if "ha" in screeners and df is not None:
            df = self.ha_streak_screener(df=df)
        if "st" in screeners and df is not None:
            df = self.supertrend_screener(df=df)
        if "macd" in screeners and df is not None:
            df = self.macd_screener(df=df)
        return df

    def ha_streak_screener(self, df: DataFrame, min_streak: int = 1, max_streak: int = 2, trend: int = 1) -> DataFrame:
        """Run the HA Streak screener"""
        cols = ["HA_Signal", "HA_Streak"]
        if df is None:
            print(1)
            return None
        if any([col for col in cols if col not in df.columns]):
            print(2)
            return None
        return (
            df.query(f"HA_Trend == {trend}")
            .query(f"HA_Streak >= {min_streak}")
            .query(f"HA_Streak <= {max_streak}")
            .reset_index(drop=True)
        )

    def supertrend_screener(self, df: DataFrame, min_streak: int = 1, max_streak: int = 2, trend: int = 1) -> DataFrame:
        """Run the HA Streak screener"""
        cols = ["supertrend", "supertrend_streak"]
        if df is None:
            return None
        print(df.columns)
        if any([col for col in cols if col not in df.columns]):
            return None
        return (
            df.query(f"supertrend == {trend}")
            .query(f"supertrend_streak >= {min_streak}")
            .query(f"supertrend_streak <= {max_streak}")
            .reset_index(drop=True)
        )

    def macd_screener(self, df: DataFrame, min_streak: int = 1, max_streak: int = 2, trend: int = 1) -> DataFrame:
        """Run the MACD Streak screener"""
        cols = ["macdtrend", "macdstreak"]
        if df is None:
            return None
        print(df.columns)
        if any([col for col in cols if col not in df.columns]):
            return None
        return (
            df.query(f"macdtrend == {trend}")
            .query(f"macdstreak >= {min_streak}")
            .query(f"macdstreak <= {max_streak}")
            .reset_index(drop=True)
        )

    def stoch_rsi_screener(self, df: DataFrame, trend: int = 1, rsi_k_min: int = 60) -> DataFrame:
        """Run the RSI Stochastic screener"""
        cols = ["stochastic_rsi_crossover", "stochastic_rsi_K", "stochastic_rsi_D"]
        if df is None:
            return None
        print(df.columns)
        if any([col for col in cols if col not in df.columns]):
            return None
        return (
            df.query(f"stochastic_rsi_crossover == {trend}")
            .query(f"stochastic_rsi_K >= {rsi_k_min}")
            .reset_index(drop=True)
        )


# data_path = Path("/Users/ab/Data/stock_data/").absolute()
# hdf_store = HDFStore(data_path / "store.h5", mode="r")
# df__ = hdf_store.get("/Signals/D/merged/")  # .query('symbol == "qqq"')
# # print(df)
# df__.loc[df__.symbol.isin(df__.symbol.drop_duplicates().sort_values().head(200))].to_csv("./data/temp.tsv", sep="\t")
# a = Screener()
# # df = a.apply_screeners(screeners=["st"], period="D", num_periods=5, df=None)
# df_ = a.ha_streak_screener(df=df__, min_streak=2, max_streak=5, trend=-1)
# print(df_)
# print(df_.columns)
# # print(df__.value_counts("HA_Streak"))
# # print(df__.query("HA_Streak == 299").to_dict())

# hdf_store.close()
