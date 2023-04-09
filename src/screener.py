from pandas import DataFrame
from numpy import where


class Screener:
    def __init__(
        self,
    ):
        pass

    def apply_screeners(self, df: DataFrame, screeners: list[str], trend: int = 1) -> DataFrame:
        """Apply screeners"""
        if df is None:
            return None
        if "ha" in screeners and df is not None:
            df = self.ha_streak_screener(df=df, trend=trend, max_streak=1000)
        if "st" in screeners and df is not None:
            df = self.supertrend_screener(df=df, trend=trend, max_streak=1000)
        if "macd" in screeners and df is not None:
            df = self.macd_screener(df=df, trend=trend, max_streak=1000)
        if "srsi" in screeners and df is not None:
            df = self.stoch_rsi_screener(df=df, trend=trend)
        if "sma" in screeners and df is not None:
            df = self.sma_screener(df=df, trend=trend)
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

    def sma_screener(self, df: DataFrame, trend: int = 1) -> DataFrame:
        """Run the SMA 20/50 Crossover"""
        cols = ["sma20", "sma50"]
        if df is None:
            return None
        print(df.columns)
        if any([col for col in cols if col not in df.columns]):
            return None
        df["tmp"] = where(df["sma20"] >= df["sma50"], 1, -1)
        return (
            df.query(f"tmp == {trend}")
            # .loc[df["sma20"] >= df["sma50"], 1, -1) == trend, :]
            .sort_values("Date", ascending=True).reset_index(drop=True)
        )

    def stoch_rsi_screener(self, df: DataFrame, rsi_k_min: int = 60, trend: int = 1) -> DataFrame:
        """Run the RSI Stochastic screener"""
        cols = ["stochastic_rsi_crossover", "stochastic_rsi_K", "stochastic_rsi_D"]
        if df is None:
            return None
        # print(df.columns)
        if any([col for col in cols if col not in df.columns]):
            return None
        if trend == 1:
            return (
                df.query(f"stochastic_rsi_crossover == {trend}")
                .query(f"stochastic_rsi_K >= {rsi_k_min}")
                .reset_index(drop=True)
            )
        else:
            return (
                df.query(f"stochastic_rsi_crossover == {trend}")
                .query(f"stochastic_rsi_K <= {100-rsi_k_min}")
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
