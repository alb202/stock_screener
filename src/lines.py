from pandas import read_parquet
from pathlib import Path


def make_lines(path: Path, period: str, symbol: str, lines: list[str]) -> dict:
    if lines is None or path is None or period is None or symbol is None:
        return None
    if len(lines) == 0:
        return None

    sma_df = (
        read_parquet(path=path / f"signals/{period}/sma.parquet")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date"] + [line for line in lines if "sma" in line]]
    )

    ema_df = (
        read_parquet(path=path / f"signals/{period}/ema.parquet")
        .query(f'symbol == "{symbol}"')
        .loc[:, ["Date"] + [line for line in lines if "ema" in line]]
    )
    df = sma_df.merge(ema_df, how="inner", on="Date")
    print(df)
    return df
