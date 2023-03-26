from pandas import DataFrame, read_csv
from urllib3.exceptions import HTTPError
from numpy import where


class Nasdaq:

    URL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt"

    def __init__(self) -> None:

        self.df = self._get_data()

    def _get_data(self) -> DataFrame:

        df = read_csv(filepath_or_buffer=self.URL, sep="|", header=0)
        df = df.query('ETF != " "').query("Symbol == Symbol")
        df["type"] = where(df["ETF"] == "Y", "etf", where(df["ETF"] == "N", "stock", "other"))
        df = (
            df.rename(columns={"Symbol": "symbol", "Security Name": "name"})
            .loc[:, ["symbol", "name", "type"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        df = df.loc[~df.symbol.str.contains(r"\.")]
        df = df.loc[~df.symbol.str.contains(r"\$")]
        df = df.loc[df.symbol.str.len() <= 4]
        return df.sort_values("symbol", ascending=True).reset_index(drop=True)

    def __call__(self) -> DataFrame:
        return self.df


# a = Nasdaq()
# print(a())
