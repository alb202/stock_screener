# from nasdaq import Nasdaq
# from yahoo import Yahoo
# from yahoo_info import YahooInfo
from pandas import DataFrame, concat
from tqdm.auto import tqdm

# from pathlib import Path
# from joblib import Parallel, delayed
# from multiprocessing import cpu_count
# from typing import Optional
from ta_lib import bollinger_bands, rsi, macd, natr, supertrend, pattern_recognition
from ha import heikin_ashi, heikin_ashi_signals
from future_calcs import future_max, future_end
from joblib import Parallel, delayed


def calculations(df: DataFrame, n_jobs: int = 8, calc_set: str = "D") -> dict[DataFrame]:
    results = {}
    symbols = df.symbol.drop_duplicates().unique()

    ha_tmp = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(heikin_ashi)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="heikin ashi")
        )
    )

    results["heikin_ashi"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(heikin_ashi_signals)(df=ha_tmp.query(f'symbol == "{symbol}"'))
            for symbol in tqdm(symbols, desc="heikin ashi signals")
        )
    )

    results["bollinger_bands"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(bollinger_bands)(df=df.query(f'symbol == "{symbol}"'))
            for symbol in tqdm(symbols, desc="bollinger bands")
        )
    )

    results["rsi"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(rsi)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="rsi")
        )
    )

    results["macd"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(macd)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="macd")
        )
    )

    results["natr"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(natr)(df=df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="natr")
        )
    )

    results["supertrend"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(supertrend)(df.query(f'symbol == "{symbol}"')) for symbol in tqdm(symbols, desc="supertrend")
        )
    )

    results["close_5_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=5)
            for symbol in tqdm(symbols, desc="close_5_max")
        )
    )
    results["close_10_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=10)
            for symbol in tqdm(symbols, desc="close_10_max")
        )
    )
    results["close_20_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=20)
            for symbol in tqdm(symbols, desc="close_20_max")
        )
    )
    results["close_40_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=40)
            for symbol in tqdm(symbols, desc="close_40_max")
        )
    )
    results["high_5_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=5)
            for symbol in tqdm(symbols, desc="high_5_max")
        )
    )
    results["high_10_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=10)
            for symbol in tqdm(symbols, desc="high_10_max")
        )
    )
    results["high_20_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=20)
            for symbol in tqdm(symbols, desc="high_20_max")
        )
    )
    results["high_40_max"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_max)(df=df.query(f'symbol == "{symbol}"'), value_col="High", periods=40)
            for symbol in tqdm(symbols, desc="high_40_max")
        )
    )
    results["close_5_end"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=5)
            for symbol in tqdm(symbols, desc="close_5_end")
        )
    )
    results["close_10_end"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=10)
            for symbol in tqdm(symbols, desc="close_10_end")
        )
    )
    results["close_20_end"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=20)
            for symbol in tqdm(symbols, desc="close_20_end")
        )
    )
    results["close_40_end"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=40)
            for symbol in tqdm(symbols, desc="close_40_end")
        )
    )

    results["pattern_recognition"] = concat(
        Parallel(n_jobs=n_jobs)(
            delayed(pattern_recognition)(df=df.query(f'symbol == "{symbol}"'))
            for symbol in tqdm(symbols, desc="Pattern Recognition")
        )
    )

    # if calc_set = 'D':
    #     results["close_40_end"] = concat(
    #         Parallel(n_jobs=n_jobs)(
    #             delayed(future_end)(df=df.query(f'symbol == "{symbol}"'), value_col="Close", periods=40)
    #             for symbol in tqdm(symbols, desc="close_40_end")
    #         )
    #     )

    return results
