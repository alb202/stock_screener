from nasdaq import Nasdaq
from yahoo import Yahoo
from yahoo_info import YahooInfo

from pandas import DataFrame, concat, read_parquet

from tqdm.auto import tqdm
from pathlib import Path
from joblib import Parallel, delayed
from multiprocessing import cpu_count
from typing import Optional
from calcs import calculations
from stock_sectors import Sectors
from sp_symbols import SP
from utilities import make_directories, camelcase
import os


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


def get_sector_etfs() -> DataFrame:
    sector_etfs = {
        "SPY": "S&P 500",
        "XLC": "COMMUNICATION SERVICES",
        "XLY": "CONSUMER DISCRETIONARY",
        "XLP": "CONSUMER STAPLES",
        "XLE": "ENERGY",
        "XLF": "FINANCIALS",
        "XLV": "HEALTH CARE",
        "XLI": "INDUSTRIALS",
        "XLB": "MATERIALS",
        "XLRE": "REAL ESTATE",
        "XLK": "INFORMATION TECHNOLOGY",
        "XLU": "UTILITIES",
    }
    sector_etfs = {k: [camelcase(string=v)] for k, v in sector_etfs.items()}
    df = (
        DataFrame(sector_etfs)
        .transpose()
        .reset_index(drop=False)
        .rename(columns={"index": "index_symbol", 0: "sector"})
    )
    return df


DATA_PATH = "/Users/ab/Data/stock_data/"


def main(
    refresh_info: bool = False,
    refresh_prices: bool = False,
    refresh_signals: bool = False,
    minimum_share_price: float = 1,
    maximum_share_price: float = 20000,
    s_and_p_only: bool = True,
    n_test: Optional[int] = None,
) -> None:
    """Main"""

    data_path = Path(DATA_PATH).absolute()
    make_directories(data_path=data_path)

    if refresh_info:
        nasdaq = Nasdaq()
        all_nasdaq = nasdaq().loc[:, ["symbol"]]

        sectors = Sectors()
        stock_info = sectors.all_stocks.merge(all_nasdaq, how="inner", on="symbol")
        etf_info = sectors.all_etfs.merge(all_nasdaq, how="inner", on="symbol")

        stock_info.to_parquet(data_path / "info/stock_info.parquet")
        etf_info.to_parquet(data_path / "info/etf_info.parquet")
        # s_and_p_info.to_parquet(data_path / "info/s_and_p_info.parquet")
    else:
        stock_info = read_parquet(data_path / "info/stock_info.parquet")
        etf_info = read_parquet(data_path / "info/etf_info.parquet")
        # s_and_p_info = read_parquet(data_path / "info/s_and_p_info.parquet")

    symbol_info = concat([stock_info, etf_info], axis=0).drop_duplicates().sort_values("symbol").reset_index(drop=True)

    ### Get the index ETFS for each symbol for Mansfield RSI
    sector_etfs_df = get_sector_etfs()
    symbol_info["sector"] = symbol_info["sector"].apply(lambda s: camelcase(s))
    symbol_info = symbol_info.merge(right=sector_etfs_df, how="left", on="sector")
    symbol_info["index_symbol"] = symbol_info["index_symbol"].fillna("SPY")
    symbol_info.to_parquet(path=data_path / "info/merged_info.parquet")

    data_symbols = symbol_info.loc[
        (
            (
                (symbol_info["type"] == "stock") & (symbol_info["market_cap"] > 2500000)
                | (symbol_info["type"] == "etf") & (symbol_info["market_cap"] > 1000000)
            )
            & (symbol_info["industry"] != "Shell Companies")
            & (symbol_info["equity_type"] != "ADRs")
            & (symbol_info["equity_type"] != "Units")
        ),
        ["symbol"],
    ].reset_index(drop=True)

    if s_and_p_only:
        sp = SP()
        s_and_p_info = sp().Symbol.to_list() + sector_etfs_df.index_symbol.to_list()
        data_symbols = data_symbols.loc[data_symbols.symbol.isin(s_and_p_info), ["symbol"]].reset_index(drop=True)

    if n_test is not None:
        n_test = n_test if n_test <= len(data_symbols) else len(data_symbols)
        # s_and_p_info = sp().Symbol.to_list() + sector_etfs_df.index_symbol.to_list()
        data_symbols = (
            concat([data_symbols.head(n_test).symbol, sector_etfs_df.index_symbol])
            .reset_index(drop=False)
            .rename(columns={0: "symbol"})
            .loc[:, ["symbol"]]
        )
        # print(data_symbols)

    if refresh_prices:
        prices_d_df = yahoo_stock_prices(symbols=data_symbols.symbol, interval="1d", period="5y")

        ticker_filter = (
            prices_d_df.sort_values("Date")
            .drop_duplicates(subset=["symbol"], keep="first")
            .query(f"Close >= {minimum_share_price}")
            .query(f"Close <= {maximum_share_price}")
            .loc[:, ["symbol"]]
        )

        prices_d_df = prices_d_df.loc[prices_d_df.symbol.isin(ticker_filter.symbol), :].reset_index(drop=True)
        prices_d_df.to_parquet(path=data_path / "OHLCV/D/data.parquet")

        prices_wk_df = yahoo_stock_prices(symbols=ticker_filter.symbol, interval="1wk", period="5y")
        # prices_wk_df = prices_wk_df.loc[prices_wk_df.symbol.isin(ticker_filter.symbol), :].reset_index(drop=True)
        prices_wk_df.to_parquet(path=data_path / "OHLCV/W/data.parquet")

        prices_mo_df = yahoo_stock_prices(symbols=ticker_filter.symbol, interval="1mo", period="5y")
        # prices_mo_df = prices_mo_df.loc[prices_mo_df.symbol.isin(ticker_filter.symbol), :].reset_index(drop=True)
        prices_mo_df.to_parquet(path=data_path / "OHLCV/M/data.parquet")

        screener_symbols = prices_d_df.loc[:, ["symbol"]].sort_values("symbol").drop_duplicates().reset_index(drop=True)
        screener_symbols.to_parquet(path=data_path / "symbols/data.parquet")

    else:
        prices_d_df = read_parquet(path=data_path / "OHLCV/D/data.parquet")
        prices_wk_df = read_parquet(path=data_path / "OHLCV/W/data.parquet")
        prices_mo_df = read_parquet(path=data_path / "OHLCV/M/data.parquet")
        prices_d_df = prices_d_df.loc[prices_d_df.symbol.isin(data_symbols.symbol)].reset_index(drop=True)
        prices_wk_df = prices_wk_df.loc[prices_wk_df.symbol.isin(data_symbols.symbol)].reset_index(drop=True)
        prices_mo_df = prices_mo_df.loc[prices_mo_df.symbol.isin(data_symbols.symbol)].reset_index(drop=True)

    # signal_symbols = (
    #     prices_d_df.sort_values("Date")
    #     .drop_duplicates(subset=["symbol"], keep="first")
    #     .query("Close >= 2.5")
    #     .loc[:, ["symbol"]]
    # )

    # prices_d_df = prices_d_df.merge(signal_symbols, on="symbol", how="inner").merge(
    #     data_symbols, on="symbol", how="inner"
    # )
    # prices_wk_df = prices_wk_df.merge(signal_symbols, on="symbol", how="inner").merge(
    #     data_symbols, on="symbol", how="inner"
    # )
    # prices_mo_df = prices_mo_df.merge(signal_symbols, on="symbol", how="inner").merge(
    #     data_symbols, on="symbol", how="inner"
    # )

    if refresh_signals:
        daily_calcs = calculations(
            df=prices_d_df.merge(
                symbol_info.loc[:, ["symbol", "index_symbol"]].drop_duplicates(), how="inner", on="symbol"
            ),
            calc_set="D",
            n_jobs=4,
        )
        df = prices_d_df.copy(deep=True)
        for key, value in tqdm(daily_calcs.items(), desc="Daily to disk"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.reset_index(drop=True).to_parquet(path=data_path / f"signals/D/{key}.parquet")
        df.to_parquet(path=data_path / "signals/D/merged.parquet")

        weekly_calcs = calculations(
            df=prices_wk_df.merge(
                symbol_info.loc[:, ["symbol", "index_symbol"]].drop_duplicates(), how="inner", on="symbol"
            ),
            calc_set="W",
        )
        df = prices_wk_df.copy(deep=True)
        for key, value in tqdm(weekly_calcs.items(), desc="Weekly to disk"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.reset_index(drop=True).to_parquet(path=data_path / f"signals/W/{key}.parquet")
        df.to_parquet(path=data_path / "signals/W/merged.parquet")

        monthly_calcs = calculations(
            df=prices_mo_df.merge(
                symbol_info.loc[:, ["symbol", "index_symbol"]].drop_duplicates(), how="inner", on="symbol"
            ),
            calc_set="M",
        )
        df = prices_mo_df.copy(deep=True)
        for key, value in tqdm(monthly_calcs.items(), desc="Monthly to disk"):
            df = df.merge(
                value.drop(columns=value.columns.intersection(["Volume"]), axis=1), how="left", on=["Date", "symbol"]
            )
            value.reset_index(drop=True).to_parquet(path=data_path / f"signals/M/{key}.parquet")
        df.to_parquet(path=data_path / "signals/M/merged.parquet")


if __name__ == "__main__":
    main(refresh_info=False, refresh_prices=False, refresh_signals=True, s_and_p_only=True, n_test=20)
