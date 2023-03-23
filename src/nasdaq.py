from pandas import DataFrame, read_csv
from urllib3.exceptions import HTTPError


class Nasdaq:

    URL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt"

    def __init__(self) -> None:

        self.df = self._get_data()

    def _get_data(self) -> DataFrame:

        df = read_csv(filepath_or_buffer=self.URL, sep="|", header=0)
        df = df.query('ETF != " "').query("Symbol == Symbol")
        df.loc[df["ETF"] == "Y", "type"] = "etf"
        df.loc[df["ETF"] == "N", "type"] = "stock"
        df.loc[df["ETF"] == "", "type"] = "other"
        df = (
            df.rename(columns={"Symbol": "symbol", "Security Name": "name"})
            .loc[:, ["symbol", "name", "type"]]
            .drop_duplicates()
            .sort_values("symbol")
            .reset_index(drop=True)
        )
        df = df.loc[~df.symbol.str.contains(r"\.")]
        df = df.loc[~df.symbol.str.contains(r"\$")]
        df = df.loc[df.symbol.str.len() <= 4]
        return df.reset_index(drop=True)

    def __call__(self) -> DataFrame:
        return self.df


# a = Nasdaq()
# print(a())
