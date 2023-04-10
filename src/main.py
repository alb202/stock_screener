from nasdaq import Nasdaq
from yahoo import Yahoo
from yahoo_info import YahooInfo
from pandas import DataFrame, concat, read_parquet  # , HDFStore, read_hdf
from tqdm.auto import tqdm
from pathlib import Path
from joblib import Parallel, delayed
from multiprocessing import cpu_count
from typing import Optional
from calcs import calculations
from stock_sectors import Sectors
from utilities import make_directories


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
    make_directories(data_path=data_path)

    if refresh_info:
        nasdaq = Nasdaq()
        all_nasdaq = nasdaq().loc[:, ["symbol"]]

        sectors = Sectors()
        stock_info = sectors.all_stocks.merge(all_nasdaq, how="inner", on="symbol")
        etf_info = sectors.all_etfs.merge(all_nasdaq, how="inner", on="symbol")

        stock_info.to_parquet(data_path / "info/stock_info.parquet")
        etf_info.to_parquet(data_path / "info/etf_info.parquet")

    else:
        stock_info = read_parquet(data_path / "info/stock_info.parquet")
        etf_info = read_parquet(data_path / "info/etf_info.parquet")

    symbol_info = (
        concat([stock_info[:n_test], etf_info[:n_test]], axis=0).sort_values("symbol").reset_index(drop=True)[:n_test]
    )
    symbol_info.to_parquet(path=data_path / "info/merged_info.parquet")

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
    ].reset_index(drop=True)

    if refresh_prices:
        prices_d_df = yahoo_stock_prices(symbols=data_symbols.symbol, interval="1d", period="2y")
        prices_d_df.to_parquet(path=data_path / "OHLCV/D/data.parquet")

        prices_wk_df = yahoo_stock_prices(symbols=data_symbols.symbol, interval="1wk", period="3y")
        prices_wk_df.to_parquet(path=data_path / "OHLCV/W/data.parquet")

        prices_mo_df = yahoo_stock_prices(symbols=data_symbols.symbol, interval="1mo", period="5y")
        prices_mo_df.to_parquet(path=data_path / "OHLCV/M/data.parquet")
    else:
        prices_d_df = read_parquet(path=data_path / "OHLCV/D/data.parquet")
        prices_wk_df = read_parquet(path=data_path / "OHLCV/W/data.parquet")
        prices_mo_df = read_parquet(path=data_path / "OHLCV/M/data.parquet")

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
    screener_symbols.to_parquet(path=data_path / "symbols/data.parquet")

    if refresh_signals:
        daily_calcs = calculations(df=prices_d_df, calc_set="D")
        df = prices_d_df.copy(deep=True)
        for key, value in tqdm(daily_calcs.items(), desc="Daily to disk"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_parquet(path=data_path / f"signals/D/{key}.parquet")
        df.to_parquet(path=data_path / "signals/D/merged.parquet")

        weekly_calcs = calculations(df=prices_wk_df, calc_set="W")
        df = prices_wk_df.copy(deep=True)
        for key, value in tqdm(weekly_calcs.items(), desc="Weekly to disk"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_parquet(path=data_path / f"signals/W/{key}.parquet")
        df.to_parquet(path=data_path / "signals/W/merged.parquet")

        monthly_calcs = calculations(df=prices_mo_df, calc_set="M")
        df = prices_mo_df.copy(deep=True)
        for key, value in tqdm(monthly_calcs.items(), desc="Monthly to disk"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.to_parquet(path=data_path / f"signals/M/{key}.parquet")
        df.to_parquet(path=data_path / "signals/M/merged.parquet")


if __name__ == "__main__":
    main(refresh_info=False, refresh_prices=True, refresh_signals=True, n_test=None)
