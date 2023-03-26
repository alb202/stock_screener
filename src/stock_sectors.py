from pandas import DataFrame, read_html, concat
from tqdm.auto import tqdm


class Sectors:

    ETF_TABLE_URLS = [
        "https://stockmarketmba.com/listofetfsforasponsor.php?s=22",
        "https://stockmarketmba.com/listofetfsforasponsor.php?s=20",
        "https://stockmarketmba.com/listofetfsforasponsor.php?s=14",
        "https://stockmarketmba.com/listofetfsforasponsor.php?s=63",
        "https://stockmarketmba.com/listofetfsforasponsor.php?s=16",

    ]

    def __init__(self) -> None:
        self.top_stock_url = "https://stockmarketmba.com/listofindustries.php"
        self.stock_sector_table = self._get_sectors()
        self.all_stocks = self._get_stocks()
        self.all_etfs = self._get_etfs()

    def _get_sector_table(self, url: str) -> DataFrame:
        df = read_html(url, header=0)[0].iloc[:-1]
        return df

    def _get_sectors(self) -> DataFrame:

        df = read_html(self.top_stock_url, header=0, extract_links="all")[0].iloc[:-1]
        df.columns = [col[0].lower().replace(" ", "_").replace(".", "") for col in df.columns]
        df["industry"] = df["industry"].apply(lambda i: i[0])
        df["market_cap"] = df["market_cap"].apply(lambda i: i[0].replace(",", "").replace("$", ""))
        df["links"] = df["us_stock_count"].apply(lambda i: f"https://stockmarketmba.com/{i[1]}")
        return df

    def _get_stocks(self) -> DataFrame:

        all_sectors = []
        for url in tqdm(self.stock_sector_table.links[:], desc="Stock Sectors"):
            # print(url)
            df = self._get_sector_table(url=url)
            df["sector"] = url.split("=")[1]
            all_sectors.append(df)
        df = concat(all_sectors, axis=0).drop("Actions", axis=1)
        df.columns = [col.lower().replace(" ", "_").replace(".", "") for col in df.columns]
        df["market_cap"] = df["market_cap"].apply(lambda i: int(i.replace(",", "").replace("$", "")))
        df["average_volume"] = df["average_volume"].apply(lambda i: int(i))
        df["sector"] = df["sector"].apply(lambda i: i.replace("+", " ").replace("%26", "&").replace("%97", "-"))
        df = df.rename(
            columns={
                "symbol": "symbol",
                "description": "description",
                "gics_sector": "sector",
                "category1": "region",
                "category2": "equity_type",
                "category3": "size",
                "market_cap": "market_cap",
                "average_volume": "average_volume",
                "sector": "industry",
            }
        )
        df["type"] = "stock"
        return df.sort_values("symbol").reset_index(drop=True).fillna("")

    def _get_etfs(self) -> DataFrame:

        all_sectors = []
        for url in tqdm(self.ETF_TABLE_URLS[:], desc="ETF Sectors"):

            df = self._get_sector_table(url=url).drop("Actions", axis=1)


            all_sectors.append(df)
        df = concat(all_sectors, axis=0)  # .drop("Actions", axis=1).drop("Action", axis=1)
        df.columns = [col.lower().replace(" ", "_").replace(".", "") for col in df.columns]
        df["market_cap"] = df["market_cap"].apply(lambda i: int(i.replace(",", "").replace("$", "")))
        df["average_volume"] = df["average_volume"].apply(lambda i: int(i))

        df = df.rename(
            columns={
                "symbol": "symbol",
                "description": "description",
                "inception_date": "inception_date",
                "leverage_factor": "leverage_factor",
                "index": "sector",
                "category1": "region",
                "category2": "equity_type",
                "category3": "size",
                "market_cap": "market_cap",
                "average_volume": "average_volume",
                "fees": "fees",
            }
        )
        df["type"] = "etf"
        return df.sort_values("symbol").drop_duplicates().reset_index(drop=True).fillna("")



# a = Sectors()
# # a.all_etfs.to_csv("./data/temp.tsv", sep="\t", header=True)
# print(a.all_etfs)
# # # print(read_html("https://stockmarketmba.com/listofstocksinanindustry.php?i=Advertising+Agencies"))
