from pandas import DataFrame
from ta_utils import validate_columns


def future_max(df: DataFrame, value_col: str = "Close", periods: int = 5) -> DataFrame:
    """Calculate max value of column over n future days"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", value_col])

    new_col = f"{value_col}_{periods}_max"
    new_col_ratio = f"{value_col}_{periods}_max_close_ratio"
    df = df.sort_values(by="Date", ascending=False)
    df = df.assign(**{new_col: df[value_col].rolling(window=periods).max()})
    df = df.assign(**{new_col_ratio: df[new_col] / df["Close"]})
    return df.loc[:, ["Date", "symbol", new_col]].sort_values(by="Date", ascending=True)


def future_end(df: DataFrame, value_col: str = "Close", periods: int = 5) -> DataFrame:
    """Calculate max value of column over n future days"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", value_col])

    new_col = f"{value_col}_{periods}_end"
    new_col_ratio = f"{value_col}_{periods}_end_close_ratio"

    df = df.sort_values(by="Date", ascending=False)
    df = df.assign(**{new_col: df[value_col].shift(periods)})
    df = df.assign(**{new_col_ratio: df[new_col] / df["Close"]})
    return df.loc[:, ["Date", "symbol", new_col]].sort_values(by="Date", ascending=True)
