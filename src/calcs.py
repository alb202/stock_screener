# from nasdaq import Nasdaq
# from yahoo import Yahoo
# from yahoo_info import YahooInfo
from pandas import DataFrame, concat
from tqdm.auto import tqdm
from icecream import ic

tqdm.pandas(bar_format="{desc}{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}, iter={n}]")
# from pathlib import Path
# from joblib import Parallel, delayed
# from multiprocessing import cpu_count
# from typing import Optional
from ta_lib import *
from ha import heikin_ashi, heikin_ashi_signals
from future_calcs import future_max, future_end

# from joblib import Parallel, delayed


def calculations(df: DataFrame, n_jobs: int = 8, calc_set: str = "D") -> dict[DataFrame]:
    results = {}
    # symbols = df.symbol.drop_duplicates().unique()

    # ha_tmp = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(heikin_ashi)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="heikin ashi")
    #     )
    # )

    ic("Index Close")
    index_tmp = (
        df.loc[df.symbol.isin(df.index_symbol.unique()), ["symbol", "Date", "Close"]]
        .drop_duplicates()
        .rename(columns={"symbol": "index_symbol", "Close": "index_close"})
    )
    # ic(index_tmp)
    index_close_df = (
        df.merge(index_tmp, how="inner", on=["Date", "index_symbol"])
        .loc[:, ["symbol", "Date", "index_close"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    results["index_close"] = index_close_df

    ic("Mansfield RSI")
    results["mansfield_rsi"] = (
        df.merge(right=index_close_df, how="left", on=["symbol", "Date"])
        .groupby("symbol", group_keys=False)
        .progress_apply(lambda data: mansfield_rsi(df=data))
    )

    ic("Heikin Ashi")
    ha_tmp = df.groupby("symbol", group_keys=False).progress_apply(lambda data: heikin_ashi(df=data))
    ic("Heikin Ashi signals")
    results["heikin_ashi"] = ha_tmp.groupby("symbol", group_keys=False).progress_apply(
        lambda data: heikin_ashi_signals(df=data)
    )

    # results["heikin_ashi"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(heikin_ashi_signals)(df=ha_tmp.query(f'symbol == "{symbol}"'))
    #         for symbol in tqdm(symbols, desc="heikin ashi signals")
    #     )
    # )

    # results["bollinger_bands"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(bollinger_bands)(df=df.query(f'symbol == "{symbol}"'))
    #         for symbol in tqdm(symbols, desc="bollinger bands")
    #     )
    # )
    ic("Bollinger bands")
    results["bollinger_bands"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: bollinger_bands(df=data)
    )

    # results["rsi"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(rsi)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="rsi")
    #     )
    # )
    ic("RSI")
    results["rsi"] = df.groupby("symbol", group_keys=False).progress_apply(lambda data: rsi(df=data))

    # results["stochastic_rsi"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(stochastic_rsi)(df=df.query(f'symbol == "{symbol}"'))
    #         for symbol in tqdm(symbols, desc="stochastic_rsi")
    #     )
    # )
    ic("Stochastic RSI")
    results["stochastic_rsi"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: stochastic_rsi(df=data)
    )

    # results["macd"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(macd)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="macd")
    #     )
    # )
    ic("MACD")
    results["macd"] = df.groupby("symbol", group_keys=False).progress_apply(lambda data: macd(df=data))

    # results["natr_14"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(natr)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="natr")
    #     )
    # )
    ic("NATR 14")
    results["natr_14"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: natr(df=data, timeperiod=14)
    )

    # results["volume_ema_10"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(volume_ema)(df=df.query(f'symbol == "{symbol}"'), timeperiod=10)
    #         for symbol in tqdm(symbols, desc="volume_ema")
    #     )
    # )
    ic("Volume EMA 10")
    results["volume_ema_10"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: volume_ema(df=data, timeperiod=10)
    )

    ic("Volume SMA 21")
    results["volume_sma_21"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: volume_sma(df=data, timeperiod=21)
    )

    # results["obv"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(natr)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="obv")
    #     )
    # )

    ic("OBV")
    results["obv"] = df.groupby("symbol", group_keys=False).progress_apply(lambda data: obv(df=data))

    # results["sma"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(simple_moving_averages)(df=df.query(f'symbol == "{symbol}"'))
    #         for symbol in tqdm(symbols, desc="simple moving averages")
    #     )
    # )
    ic("SMA")
    results["sma"] = df.groupby("symbol", group_keys=False).progress_apply(lambda data: simple_moving_averages(df=data))

    ic("EMA")
    results["ema"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: exponential_moving_averages(df=data)
    )

    ic("SATA moving averages")
    results["sata_moving_averages"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: sata_moving_averages(df=data)
    )

    # results["ema"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(exponential_moving_averages)(df=df.query(f'symbol == "{symbol}"'))
    #         for symbol in tqdm(symbols, desc="exponential moving averages")
    #     )
    # )

    # results["supertrend"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(supertrend)(df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="supertrend")
    #     )
    # )
    ic("Supertrend")
    results["supertrend"] = df.groupby("symbol", group_keys=False).progress_apply(lambda data: supertrend(df=data))

    # results["pattern_recognition"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(pattern_recognition)(df=df.query(f'symbol == "{symbol}"'))
    #         for symbol in tqdm(symbols, desc="Pattern Recognition")
    #     )
    # )
    # ic("Patterns")
    # results["pattern_recognition"] = df.groupby("symbol", group_keys=False).progress_apply(
    #     lambda data: pattern_recognition(df=data)
    # )

    # results["periods_since_bottom_10"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(periods_since_bottom)(df=df.query(f'symbol == "{symbol}"'), num_periods=10, low_column="Low")
    #         for symbol in tqdm(symbols, desc="Periods since 10 period bottom")
    #     )
    # )
    ic("Periods since bottom 10")
    results["periods_since_bottom_10"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: periods_since_bottom(df=data, num_periods=10, low_column="Low")
    )
    ic("Periods since bottom 20")
    results["periods_since_bottom_20"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: periods_since_bottom(df=data, num_periods=20, low_column="Low")
    )
    ic("Periods since bottom 40")
    results["periods_since_bottom_40"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: periods_since_bottom(df=data, num_periods=40, low_column="Low")
    )

    # results["periods_since_bottom_20"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(periods_since_bottom)(df=df.query(f'symbol == "{symbol}"'), num_periods=20, low_column="Low")
    #         for symbol in tqdm(symbols, desc="Periods since 20 period bottom")
    #     )
    # )

    # results["periods_since_bottom_40"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(periods_since_bottom)(df=df.query(f'symbol == "{symbol}"'), num_periods=40, low_column="Low")
    #         for symbol in tqdm(symbols, desc="Periods since 40 period bottom")
    #     )
    # )

    # results["periods_since_top_10"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(periods_since_top)(df=df.query(f'symbol == "{symbol}"'), num_periods=10, high_column="High")
    #         for symbol in tqdm(symbols, desc="Periods since 10 period top")
    #     )
    # )

    # results["periods_since_top_20"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(periods_since_top)(df=df.query(f'symbol == "{symbol}"'), num_periods=20, high_column="High")
    #         for symbol in tqdm(symbols, desc="Periods since 20 period top")
    #     )
    # )

    # results["periods_since_top_40"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(periods_since_top)(df=df.query(f'symbol == "{symbol}"'), num_periods=40, high_column="High")
    #         for symbol in tqdm(symbols, desc="Periods since 30 period top")
    #     )
    # )
    ic("Periods since top 10")
    results["periods_since_top_10"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: periods_since_top(df=data, num_periods=10, high_column="High")
    )
    ic("Periods since top 20")
    results["periods_since_top_20"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: periods_since_top(df=data, num_periods=20, high_column="High")
    )
    ic("Periods since top 40")
    results["periods_since_top_40"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: periods_since_top(df=data, num_periods=40, high_column="High")
    )

    # results["find_slope_10"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(find_slope)(df=df.query(f'symbol == "{symbol}"'), num_periods=10)
    #         for symbol in tqdm(symbols, desc="Find the 10 period slope")
    #     )
    # )

    # results["find_slope_20"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(find_slope)(df=df.query(f'symbol == "{symbol}"'), num_periods=20)
    #         for symbol in tqdm(symbols, desc="Find the 20 period slope")
    #     )
    # )

    # results["find_slope_40"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(find_slope)(df=df.query(f'symbol == "{symbol}"'), num_periods=40)
    #         for symbol in tqdm(symbols, desc="Find the 40 period slope")
    #     )
    # )
    ic("Find slope 10")
    results["find_slope_10"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: find_slope_(df=data, num_periods=10)
    )
    ic("Find slope 20")
    results["find_slope_20"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: find_slope_(df=data, num_periods=20)
    )
    ic("Find slope 40")
    results["find_slope_40"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: find_slope_(df=data, num_periods=40)
    )

    ic("Change ratio 1")
    results["change_ratio_1"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: change_ratio(df=data, num_periods=1, col="Close")
    )
    ic("Change ratio 2")
    results["change_ratio_2"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: change_ratio(df=data, num_periods=2, col="Close")
    )
    ic("Change ratio 5")
    results["change_ratio_5"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: change_ratio(df=data, num_periods=5, col="Close")
    )
    ic("Change ratio 10")
    results["change_ratio_10"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: change_ratio(df=data, num_periods=10, col="Close")
    )
    ic("Change ratio 20")
    results["change_ratio_20"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: change_ratio(df=data, num_periods=20, col="Close")
    )

    # results["close_5_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=5)
    #         for symbol in tqdm(symbols, desc="close_5_max")
    #     )
    # )
    # results["close_10_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=10)
    #         for symbol in tqdm(symbols, desc="close_10_max")
    #     )
    # )
    # results["close_20_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=20)
    #         for symbol in tqdm(symbols, desc="close_20_max")
    #     )
    # )

    ic("Close max 2")
    results["close_2_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="Close", periods=2)
    )
    ic("Close max 5")
    results["close_5_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="Close", periods=5)
    )
    ic("Close max 10")
    results["close_10_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="Close", periods=10)
    )
    ic("Close max 20")
    results["close_20_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="Close", periods=20)
    )
    ic("Close max 40")
    results["close_40_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="Close", periods=40)
    )

    # results["close_40_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=40)
    #         for symbol in tqdm(symbols, desc="close_40_max")
    #     )
    # )
    # results["high_5_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=5)
    #         for symbol in tqdm(symbols, desc="high_5_max")
    #     )
    # )
    # results["high_10_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=10)
    #         for symbol in tqdm(symbols, desc="high_10_max")
    #     )
    # )
    # results["high_20_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=20)
    #         for symbol in tqdm(symbols, desc="high_20_max")
    #     )
    # )
    ic("High max 2")
    results["high_2_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="High", periods=2)
    )
    ic("High max 5")
    results["high_5_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="High", periods=5)
    )
    ic("High max 10")
    results["high_10_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="High", periods=10)
    )
    ic("High max 20")
    results["high_20_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="High", periods=20)
    )
    ic("High max 40")
    results["high_40_max"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_max(df=data, value_col="High", periods=40)
    )

    # results["high_40_max"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=40)
    #         for symbol in tqdm(symbols, desc="high_40_max")
    #     )
    # )
    # results["close_5_end"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=5)
    #         for symbol in tqdm(symbols, desc="close_5_end")
    #     )
    # )
    # results["close_10_end"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=10)
    #         for symbol in tqdm(symbols, desc="close_10_end")
    #     )
    # )
    # results["close_20_end"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=20)
    #         for symbol in tqdm(symbols, desc="close_20_end")
    #     )
    # )
    ic("Close end 2")
    results["close_2_end"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_end(df=data, value_col="Close", periods=2)
    )
    ic("Close end 5")
    results["close_5_end"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_end(df=data, value_col="Close", periods=5)
    )
    ic("Close end 10")
    results["close_10_end"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_end(df=data, value_col="Close", periods=10)
    )
    ic("Close end 20")
    results["close_20_end"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_end(df=data, value_col="Close", periods=20)
    )
    ic("Close end 40")
    results["close_40_end"] = df.groupby("symbol", group_keys=False).progress_apply(
        lambda data: future_end(df=data, value_col="Close", periods=40)
    )
    # results["close_40_end"] = concat(
    #     Parallel(n_jobs=n_jobs)(
    #         delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=40)
    #         for symbol in tqdm(symbols, desc="close_40_end")
    #     )
    # )

    return results
