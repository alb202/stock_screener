from pandas import DataFrame
from numpy import where
from swifter import swifter, set_defaults
set_defaults(
    npartitions=None,
    dask_threshold=1,
    scheduler="processes",
    progress_bar=False,
    progress_bar_desc=None,
    allow_dask_on_strings=False,
    force_parallel=False,
)


def heikin_ashi(df: DataFrame) -> DataFrame:
    """Heikin Ashi Algorithm"""
    cols = ["symbol", "Open", "High", "Low", "Close", "Volume"]
    date_col = "Date"
    df = df.sort_values(date_col, ascending=True).loc[:, [date_col] + cols].reset_index(drop=True)

    df["HA_Close"] = df.loc[:, ["Open", "High", "Low", "Close"]].swifter.apply(sum, axis=1).divide(4)

    df["HA_Open"] = float(0)
    df.loc[0, "HA_Open"] = df.loc[0, "Open"]
    for index in range(1, len(df)):
        df.at[index, "HA_Open"] = (df["HA_Open"][index - 1] + df["HA_Close"][index - 1]) / 2

    df["HA_High"] = df.loc[:, ["HA_Open", "HA_Close", "Low", "High"]].swifter.apply(max, axis=1)
    df["HA_Low"] = df.loc[:, ["HA_Open", "HA_Close", "Low", "High"]].swifter.apply(min, axis=1)
    df.name = "Heiken_Ashi"
    return df.loc[:, ["Date", "symbol", "HA_Open", "HA_High", "HA_Low", "HA_Close", "Volume"]]


def heikin_ashi_signals(df: DataFrame) -> DataFrame:
    """Heiken Ashi Signals"""
    is_increasing = df["HA_Close"] > df["HA_Open"]
    is_increasing_yday = df["HA_Close"].shift(1) > df["HA_Open"].shift(1)
    buy_signal = (is_increasing & ~is_increasing_yday).replace({True: 1, False: 0})
    sell_signal = (~is_increasing & is_increasing_yday).replace({True: -1, False: 0})

    df["HA_Signal"] = where(buy_signal == 1, buy_signal, sell_signal)
    df["HA_Trend"] = where(df["HA_Close"] >= df["HA_Open"], 1, -1)
    df["HA_Streak"] = df["HA_Trend"].swifter.groupby((df["HA_Trend"] != df["HA_Trend"].shift()).cumsum()).cumcount() + 1
    df.name = "Heiken_Ashi_Signals"
    return df.loc[
        :,
        ["Date", "symbol", "HA_Open", "HA_High", "HA_Low", "HA_Close", "Volume", "HA_Signal", "HA_Trend", "HA_Streak"],
    ]


# from pandas import HDFStore, read_hdf
# from pathlib import Path

# data_path = Path("/Users/ab/Data/stock_data/").absolute()
# hdf_store = HDFStore(data_path / "store.h5", mode="r")
# df = read_hdf(path_or_buf=hdf_store, key="/OHLCV/D/", mode="r").query('symbol == "A"')
# print(df)
# ha_df = heikin_ashi(df=df)
# print(ha_df)
# ha_dfs = heikin_ashi_signals(df=ha_df)
# print(ha_dfs)

# hdf_store.close()


# b = heikin_ashi(a)
# c = heikin_ashi_signals(b)
# print(c)


#     ### Weekly HA
#     first_open = (df['open'].shift(14) + df['close'].shift(10)) / 2
#     first_close = (df['open'].shift(9) + df['high'].shift(5).rolling(window=5).max() + df['low'].shift(5).rolling(window=5).min() + df['close'].shift(5)) / 4
#     second_open = (first_open + first_close) / 2
#     second_high_ = df['high'].shift(0).rolling(window=5).max()
#     second_low_ = df['low'].shift(0).rolling(window=5).min()
#     second_close = (df['open'].shift(5) + second_high_ + second_low_ + df['close']) / 4
#     df['weekly_ha_high'] = np.where(second_high_ > df['open'].shift(5),
#                                     np.where(second_high_ > df['close'], second_high_, df['close']),
#                                     np.where(df['open'].shift(5) > df['close'], df['open'].shift(5), df['close']))
#     df['weekly_ha_low'] = np.where(second_low_ < df['open'].shift(5),
#                                    np.where(second_low_ < df['close'], second_low_, df['close']),
#                                    np.where(df['open'].shift(5) < df['close'], df['open'].shift(5), df['close']))
#     df['weekly_ha_trend'] = np.where(second_close >= second_open, 1, 0)
#     df['weekly_ha_open'] = second_open
#     df['weekly_ha_close'] = second_close

#     ### Monthly HA
#     first_open = (df['open'].shift(59) + df['close'].shift(40)) / 2
#     first_close = (df['open'].shift(39) + df['high'].shift(20).rolling(window=20).max() + df['low'].shift(20).rolling(window=20).min() + df['close'].shift(20)) / 4
#     second_open = (first_open + first_close) / 2
#     second_high_ = df['high'].shift(0).rolling(window=20).max()
#     second_low_ = df['low'].shift(0).rolling(window=20).min()
#     second_close = (df['open'].shift(20) + second_high_ + second_low_ + df['close']) / 4
#     df['monthly_ha_high'] = np.where(second_high_ > df['open'].shift(20),
#                                     np.where(second_high_ > df['close'], second_high_, df['close']),
#                                     np.where(df['open'].shift(20) > df['close'], df['open'].shift(20), df['close']))
#     df['monthly_ha_low'] = np.where(second_low_ < df['open'].shift(20),
#                                    np.where(second_low_ < df['close'], second_low_, df['close']),
#                                    np.where(df['open'].shift(20) < df['close'], df['open'].shift(20), df['close']))
#     df['monthly_ha_trend'] = np.where(second_close >= second_open, 1, 0)
#     df['monthly_ha_open'] = second_open
#     df['monthly_ha_close'] = second_close

#     ### Indicator
#     current_change = ha_df['ha_close'] > ha_df['ha_open']
#     last_change = ha_df['ha_close'].shift(1) > ha_df['ha_open'].shift(1)
#     buy_indicator = (current_change & ~last_change).replace({True: 1, False: 0})
#     sell_indicator = (~current_change & last_change).replace({True: -1, False: 0})

#     ha_df['ha_indicator'] = np.where(buy_indicator == 1, buy_indicator, sell_indicator)
#     ha_df['ha_trend'] = np.where(ha_df['ha_close'] >= ha_df['ha_open'], 1, 0)

#     ### HA daily price patterns
#     ha_lower_val = np.where(ha_df['ha_close'] > ha_df['ha_open'], ha_df['ha_open'], ha_df['ha_close'])
#     ha_higher_val = np.where(ha_df['ha_close'] > ha_df['ha_open'], ha_df['ha_close'], ha_df['ha_open'])
#     ha_df['ha_bottom_shadow'] = np.where(ha_df['ha_low'] < ha_lower_val, 1, 0)
#     ha_df['ha_top_shadow'] = np.where(ha_df['ha_high'] > ha_higher_val, 1, 0)
#     ha_df['ha_body_size'] = np.where(ha_df['ha_close']>ha_df['ha_open'], ha_df['ha_close'], ha_df['ha_open'])-np.where(ha_df['ha_close']<ha_df['ha_open'], ha_df['ha_close'], ha_df['ha_open'])
#     ha_df['ha_body_ratio'] = ha_df['ha_body_size'] / (ha_df['ha_high'] - ha_df['ha_low'])
#     ha_df['ha_body_price_ratio'] = ha_df['ha_body_size'] / ha_df['close']

#     ### Find beginning and end of streaks
#     streaks = ha_df.loc[:, ['open', 'close', 'ha_trend']].reset_index(drop=True)
#     streaks['start_of_streak'] = streaks['ha_trend'].ne(streaks['ha_trend'].shift())
#     streaks['end_of_streak'] = streaks['start_of_streak'].shift(-1)

#     ### Create ID and counter for streaks
#     streaks['streak_id'] = streaks.start_of_streak.cumsum()
#     streaks['streak_counter'] = streaks.groupby('streak_id').cumcount() + 1

#     ### Calculate the changes in the streaks
#     streaks['streak_id__calc'] = streaks['streak_id'].shift(1)
#     streaks['streak_id__calc'] = streaks['streak_id__calc'].fillna(1)
#     streak_start_close = streaks.drop_duplicates(['streak_id__calc'], keep='first').loc[:, ['open', 'streak_id__calc']].rename(columns={'open': 'start_price'})
#     streak_end_close = streaks['open'].shift(-1).rename({'open': 'end_price'})
#     streaks = streaks.merge(streak_start_close, how='left', left_on='streak_id', right_on='streak_id__calc')
#     streaks['start_price'] = np.where(streaks['start_price'].isnull(), streaks['open'], streaks['start_price'])
#     streaks['end_price'] = streak_end_close
#     streaks['daily_change'] = (streaks['end_price'] - streaks['start_price']) / streaks['open']
#     streak_change = streaks.drop_duplicates(['streak_id'], keep='last').loc[:, ['daily_change', 'streak_id']].rename(columns={'daily_change': 'streak_change'})
#     streak_length = streaks.drop_duplicates(['streak_id'], keep='last').loc[:, ['streak_counter', 'streak_id']].rename(columns={'streak_counter': 'streak_length'})
#     streak_change['streak_counter'] = 1
#     streaks = streaks.merge(streak_change, how='left', on=['streak_counter', 'streak_id']).merge(streak_length, how='left', on='streak_id')
#     streaks['streak_change'] = streaks['streak_change'].fillna(0)
#     streaks['streak_length'] = streaks['streak_length'].fillna(0)
#     streaks['end_of_streak'] = streaks['end_of_streak'].fillna(False)
#     streaks['end_price'] = streaks['end_price'].fillna(0)
#     streaks['daily_change'] = streaks['daily_change'].fillna(0)
#     new_ha_df = pd.concat([ha_df,
#                            streaks['streak_counter'],
#                            streaks['streak_id'],
#                            streaks['start_of_streak'],
#                            streaks['end_of_streak'],
#                            streaks['start_price'],
#                            streaks['end_price'],
#                            streaks['daily_change'],
#                            streaks['streak_change'],
#                            streaks['streak_length']], axis=1)
# #     new_ha_df['start_price'] = np.where(new_ha_df['start_price'].isnull(), new_ha_df['open'], new_ha_df['start_price'])
# #     new_ha_df['end_price'] = np.where(new_ha_df['end_price'] == 0, new_ha_df['close'], new_ha_df['end_price'])
#     return new_ha_df.drop(in_cols, axis=1).reset_index(drop=True)
