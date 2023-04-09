from pandas import read_hdf, read_parquet
from pathlib import Path


def get_patterns(path: Path, period: str, symbol: str) -> dict:
    """Read the pattern data from disk"""
    df = (
        read_parquet(path=path / f"signals/{period}/pattern_recognition.parquet")
        .query(f'symbol == "{symbol}"')
        .reset_index(drop=True)
    )
    value_cols = [col for col in df.columns if col not in ["Date", "symbol"]]

    df = (
        df.melt(id_vars=["Date"], value_name="value", value_vars=value_cols, var_name="indicator")
        .sort_values("Date")
        .groupby(["Date", "value"])
        .agg(list)
    )
    df["indicator"] = df["indicator"].apply(lambda l: ";".join(l))

    return {
        "up": df.query("value == 100").reset_index(drop=False).loc[:, ["Date", "indicator"]].to_dict(orient="list"),
        "down": df.query("value == -100").reset_index(drop=False).loc[:, ["Date", "indicator"]].to_dict(orient="list"),
    }


# path = "/Users/ab/Data/stock_data/store.h5"
# up, down = get_patterns(path=path, period="D", symbol="AAPL")
# print(down)
