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

# from screener import make_signal_table


def yahoo_stock_info(symbols: list) -> DataFrame:
    """Get the stock info from Yahoo"""
    n_jobs = cpu_count() * 2
    yahoo_info = YahooInfo()
    yahoo_stock_info_list = Parallel(n_jobs=n_jobs)(
        delayed(yahoo_info.ticker_info)(symbol) for symbol in tqdm(symbols, desc="Symbol info")
    )
    yahoo_stock_info_df = DataFrame(yahoo_stock_info_list)
    yahoo_stock_info_df = yahoo_stock_info_df.query('sector != "Shell Companies"').query("industry == industry")
    yahoo_stock_info_df["employees"] = yahoo_stock_info_df["employees"].apply(lambda n: int(n) if n else None)
    return yahoo_stock_info_df


def yahoo_stock_prices(symbols: list, interval: str = "1d", period: str = "5y") -> DataFrame:
    """Get the stock prices from Yahoo"""
    yahoo = Yahoo()
    n_jobs = cpu_count() * 1
    yahoo_stock_prices = Parallel(n_jobs=n_jobs)(
        delayed(yahoo.get_ticker_data)(symbol=symbol, interval=interval, period=period)
        for symbol in tqdm(symbols, desc=f"Prices - Interval:{interval} Period:{period}")
    )
    return concat(yahoo_stock_prices).reset_index(drop=False).astype({"Date": str, "symbol": str})


def main(
    info_refresh: bool = False,
    get_stock_data: bool = False,
    calculate_signals: bool = False,
    merge_signals: bool = False,
    n_test: Optional[int] = None,
) -> None:
    """Main"""

    data_path = Path("/Users/ab/Data/stock_data/").absolute()
    hdf_path = data_path / "store.h5"
    hdf_store = HDFStore(hdf_path, mode="a")

    indicies = DataFrame(
        [["SPY", "Index", "Index", 0, "S&P500", "ETF"], ["QQQ", "Index", "Index", 0, "Nasdaq", "ETF"]],
        columns=["symbol", "industry", "sector", "employees", "name", "type"],
    )

    if info_refresh:
        nasdaq = Nasdaq()
        stocks = nasdaq().query('type == "stock"')[:n_test]
        # etfs = nasdaq().query('type == "etf"')

        yahoo_stock_info_df = yahoo_stock_info(symbols=stocks.symbol)
        yahoo_stock_info_df = yahoo_stock_info_df.merge(stocks, how="inner", on="symbol")
        yahoo_stock_info_df.to_parquet(data_path / "stock_info.parquet")
    else:
        yahoo_stock_info_df = read_parquet(data_path / "stock_info.parquet")[:n_test]

    yahoo_stock_info_df = concat([yahoo_stock_info_df, indicies], axis=0)
    yahoo_stock_info_df.to_hdf(path_or_buf=hdf_store, key="Info/", format="fixed", mode="a")

    if get_stock_data:
        yahoo_stock_prices_d_df = yahoo_stock_prices(symbols=yahoo_stock_info_df.symbol, interval="1d", period="2y")
        yahoo_stock_prices_d_df.to_hdf(path_or_buf=hdf_store, key="OHLCV/D/", format="fixed", mode="a")

        yahoo_stock_prices_wk_df = yahoo_stock_prices(symbols=yahoo_stock_info_df.symbol, interval="1wk", period="3y")
        yahoo_stock_prices_wk_df.to_hdf(path_or_buf=hdf_store, key="OHLCV/W/", format="fixed", mode="a")

        yahoo_stock_prices_mo_df = yahoo_stock_prices(symbols=yahoo_stock_info_df.symbol, interval="1mo", period="5y")
        yahoo_stock_prices_mo_df.to_hdf(path_or_buf=hdf_store, key="OHLCV/M/", format="fixed", mode="a")
    else:
        yahoo_stock_prices_d_df = read_hdf(path_or_buf=hdf_store, key="OHLCV/D/")
        yahoo_stock_prices_wk_df = read_hdf(path_or_buf=hdf_store, key="OHLCV/W/")
        yahoo_stock_prices_mo_df = read_hdf(path_or_buf=hdf_store, key="OHLCV/M/")

    keep_symbols = (
        yahoo_stock_prices_d_df.sort_values("Date")
        .drop_duplicates(subset=["symbol"], keep="first")
        .query("Close >= 2.5")
        .loc[:, ["symbol"]]
    )
    yahoo_stock_prices_d_df = yahoo_stock_prices_d_df.merge(keep_symbols, on="symbol", how="inner")
    yahoo_stock_prices_wk_df = yahoo_stock_prices_wk_df.merge(keep_symbols, on="symbol", how="inner")
    yahoo_stock_prices_mo_df = yahoo_stock_prices_mo_df.merge(keep_symbols, on="symbol", how="inner")

    symbols = (
        concat([yahoo_stock_prices_d_df, yahoo_stock_prices_wk_df, yahoo_stock_prices_mo_df], axis=0)
        .loc[:, ["symbol"]]
        .drop_duplicates()
    )
    symbols.to_hdf(hdf_store, key="symbols", format="fixed", mode="a")

    if calculate_signals:
        daily_calcs = calculations(df=yahoo_stock_prices_d_df, calc_set="D")
        df = yahoo_stock_prices_d_df.copy(deep=True)
        for key, value in tqdm(daily_calcs.items(), desc="Daily to HDF"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_hdf(path_or_buf=hdf_store, key=f"/Signals/D/{key}", format="fixed", mode="a")
        df.to_hdf(path_or_buf=hdf_store, key=f"/Signals/D/merged", format="fixed", mode="a")

        weekly_calcs = calculations(df=yahoo_stock_prices_wk_df, calc_set="W")
        df = yahoo_stock_prices_wk_df.copy(deep=True)
        for key, value in tqdm(weekly_calcs.items(), desc="Weekly to HDF"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_hdf(path_or_buf=hdf_store, key=f"/Signals/W/{key}", format="fixed", mode="a")
        df.to_hdf(path_or_buf=hdf_store, key=f"/Signals/W/merged", format="fixed", mode="a")

        monthly_calcs = calculations(df=yahoo_stock_prices_mo_df, calc_set="M")
        df = yahoo_stock_prices_mo_df.copy(deep=True)
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
    hdf_store.close()


if __name__ == "__main__":
    main(info_refresh=False, get_stock_data=False, calculate_signals=True, n_test=None)
