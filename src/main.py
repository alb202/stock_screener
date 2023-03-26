from nasdaq import Nasdaq
from yahoo import Yahoo
from yahoo_info import YahooInfo
from pandas import DataFrame, concat, read_parquet, HDFStore, read_hdf
from tqdm.auto import tqdm
from pathlib import Path
from joblib import Parallel, delayed
from multiprocessing import cpu_count
from typing import Optional
from calcs import calculations
from stock_sectors import Sectors

# from screener import make_signal_table


# def yahoo_stock_info(symbols: list) -> DataFrame:
#     """Get the stock info from Yahoo"""
#     n_jobs = cpu_count() * 2
#     yahoo_info = YahooInfo()
#     yahoo_stock_info_list = Parallel(n_jobs=n_jobs)(
#         delayed(yahoo_info.ticker_info)(symbol) for symbol in tqdm(symbols, desc="Symbol info")
#     )
#     print("yahoo_stock_info_list", len(yahoo_stock_info_list), yahoo_stock_info_list)
#     yahoo_stock_info_df = DataFrame(yahoo_stock_info_list)
#     yahoo_stock_info_df = yahoo_stock_info_df.query('sector != "Shell Companies"').query("industry == industry")
#     # yahoo_stock_info_df["employees"] = yahoo_stock_info_df["employees"].apply(lambda n: int(n) if n else None)
#     print("q", yahoo_stock_info_df)
#     return yahoo_stock_info_df


def yahoo_stock_prices(symbols: list, interval: str = "1d", period: str = "5y") -> DataFrame:
    """Get the stock prices from Yahoo"""
    yahoo = Yahoo()
    n_jobs = cpu_count() * 1
    print("symbols-", symbols)
    yahoo_stock_prices = Parallel(n_jobs=n_jobs)(
        delayed(yahoo.get_ticker_data)(symbol=symbol, interval=interval, period=period)
        for symbol in tqdm(symbols, desc=f"Prices - Interval:{interval} Period:{period}")
    )
    return concat(yahoo_stock_prices).reset_index(drop=False).astype({"Date": str, "symbol": str})


def main(
    refresh_info: bool = False,
    refresh_prices: bool = False,
    refresh_signals: bool = False,
    n_test: Optional[int] = None,
) -> None:
    """Main"""

    data_path = Path("/Users/ab/Data/stock_data/").absolute()
    hdf_path = data_path / "store.h5"
    hdf_store = str(hdf_path)

    # indicies = DataFrame(
    #     [["SPY", "Index", "Index", 0, "S&P500", "ETF"],
    #      ["QQQ", "Index", "Index", 0, "Nasdaq", "ETF"]],
    #     columns=["symbol", "industry", "sector", "employees", "name", "type"],
    # )
    # indicies = DataFrame(
    #     [["SPY", "Index", "Index", "S&P500", "ETF"], ["QQQ", "Index", "Index", "Nasdaq", "ETF"]],
    #     columns=["symbol", "industry", "sector", "name", "type"],
    # )

    if refresh_info:
        nasdaq = Nasdaq()
        all_nasdaq = nasdaq().loc[:, ["symbol"]]  # .query('type == "stock"').drop(["name", "type"], axis=1)[:n_test]
        # etfs = nasdaq().query('type == "etf"').drop(["name", "type"], axis=1)
        print(all_nasdaq)
        sectors = Sectors()
        stock_info = sectors.all_stocks.merge(all_nasdaq, how="inner", on="symbol")
        etf_info = sectors.all_etfs.merge(all_nasdaq, how="inner", on="symbol")

        # nasdaq.to_parquet(data_path / "symbols.parquet")
        stock_info.to_parquet(data_path / "stock_info.parquet")
        etf_info.to_parquet(data_path / "etf_info.parquet")
        # yahoo_stock_info_df = yahoo_stock_info(symbols=stocks.symbol)
        # print("a", yahoo_stock_info_df)
        # yahoo_stock_info_df = yahoo_stock_info_df.merge(stocks, how="inner", on="symbol")
        # print("b", yahoo_stock_info_df)
        # yahoo_stock_info_df.to_parquet(data_path / "stock_info.parquet")
    else:
        stock_info = read_parquet(data_path / "stock_info.parquet")  # [:n_test]
        etf_info = read_parquet(data_path / "etf_info.parquet")  # [:n_test]

    # yahoo_stock_info_df = concat([yahoo_stock_info_df, indicies], axis=0)
    # print("yahoo_stock_info_df", yahoo_stock_info_df)
    # nasdaq = Nasdaq()
    symbol_info = (
        concat([stock_info[:n_test], etf_info[:n_test]], axis=0).sort_values("symbol").reset_index(drop=True)[:n_test]
    )
    symbol_info.to_hdf(path_or_buf=hdf_store, key="Info/", format="fixed", mode="a")
    # stock_info.to_hdf(path_or_buf=hdf_store, key="StockInfo/", format="fixed", mode="a")
    # etf_info.to_hdf(path_or_buf=hdf_store, key="ETFInfo/", format="fixed", mode="a")

    data_symbols = symbol_info.loc[
        (
            (
                (symbol_info["type"] == "stock") & (symbol_info["market_cap"] > 25000000)
                | (symbol_info["type"] == "etf") & (symbol_info["market_cap"] > 10000000)
            )
            & (symbol_info["industry"] != "Shell Companies")
            & (symbol_info["equity_type"] != "ADRs")
            & (symbol_info["equity_type"] != "Units")
        ),
        ["symbol"],
    ]
    # print("symbols", len(symbols))

    if refresh_prices:

        # symbols = symbol_info.symbol  # concat([stock_info.symbol, etf_info.symbol], axis=0)

        prices_d_df = yahoo_stock_prices(symbols=data_symbols.symbol, interval="1d", period="2y")
        prices_d_df.to_hdf(path_or_buf=hdf_store, key="OHLCV/D/", format="fixed", mode="a")

        prices_wk_df = yahoo_stock_prices(symbols=data_symbols.symbol, interval="1wk", period="3y")
        prices_wk_df.to_hdf(path_or_buf=hdf_store, key="OHLCV/W/", format="fixed", mode="a")

        prices_mo_df = yahoo_stock_prices(symbols=data_symbols.symbol, interval="1mo", period="5y")
        prices_mo_df.to_hdf(path_or_buf=hdf_store, key="OHLCV/M/", format="fixed", mode="a")
    else:
        prices_d_df = read_hdf(path_or_buf=hdf_store, key="OHLCV/D/")
        prices_wk_df = read_hdf(path_or_buf=hdf_store, key="OHLCV/W/")
        prices_mo_df = read_hdf(path_or_buf=hdf_store, key="OHLCV/M/")

    signal_symbols = (
        prices_d_df.sort_values("Date")
        .drop_duplicates(subset=["symbol"], keep="first")
        .query("Close >= 2.5")
        .loc[:, ["symbol"]]
    )
    prices_d_df = prices_d_df.merge(signal_symbols, on="symbol", how="inner").merge(
        data_symbols, on="symbol", how="inner"
    )
    prices_wk_df = prices_wk_df.merge(signal_symbols, on="symbol", how="inner").merge(
        data_symbols, on="symbol", how="inner"
    )
    prices_mo_df = prices_mo_df.merge(signal_symbols, on="symbol", how="inner").merge(
        data_symbols, on="symbol", how="inner"
    )

    screener_symbols = prices_d_df.loc[:, ["symbol"]].sort_values("symbol").drop_duplicates().reset_index(drop=True)
    screener_symbols.to_hdf(hdf_store, key="/DataSymbols", format="fixed", mode="a")

    if refresh_signals:
        daily_calcs = calculations(df=prices_d_df, calc_set="D")
        df = prices_d_df.copy(deep=True)
        for key, value in tqdm(daily_calcs.items(), desc="Daily to HDF"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_hdf(path_or_buf=hdf_store, key=f"/Signals/D/{key}", format="fixed", mode="a")
        df.to_hdf(path_or_buf=hdf_store, key=f"/Signals/D/merged", format="fixed", mode="a")

        weekly_calcs = calculations(df=prices_wk_df, calc_set="W")
        df = prices_wk_df.copy(deep=True)
        for key, value in tqdm(weekly_calcs.items(), desc="Weekly to HDF"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_hdf(path_or_buf=hdf_store, key=f"/Signals/W/{key}", format="fixed", mode="a")
        df.to_hdf(path_or_buf=hdf_store, key=f"/Signals/W/merged", format="fixed", mode="a")

        monthly_calcs = calculations(df=prices_mo_df, calc_set="M")
        df = prices_mo_df.copy(deep=True)
        for key, value in tqdm(monthly_calcs.items(), desc="Monthly to HDF"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_hdf(path_or_buf=hdf_store, key=f"/Signals/M/{key}", format="fixed", mode="a")
        df.to_hdf(path_or_buf=hdf_store, key=f"/Signals/M/merged", format="fixed", mode="a")

    # if merge_signals:
    #     for period in ["D", "W"]:
    #         merged_signals = make_signal_table(
    #             hdf_path=hdf_path,
    #             period=period,
    #             signals=["heikin_ashi", "bollinger_bands", "rsi", "macd", "natr", "supertrend"],
    #         )
    #         merged_signals.to_hdf(path_or_buf=hdf_store, key=f"/Signals/{period}/merged", format="fixed", mode="a")
    # hdf_store.close()


if __name__ == "__main__":
    main(refresh_info=False, refresh_prices=False, refresh_signals=False, n_test=None)
