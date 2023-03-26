import talib
from pandas import DataFrame, concat
from ta_utils import validate_columns

from numpy import where, array



__all__ = [
    "bollinger_bands",
    "rsi",
    "stochastic_rsi",
    "macd",
    "natr",
    "volume_ema",
    "obv",
    "supertrend",
    # "stages",
    "pattern_recognition",
]


def bollinger_bands(df: DataFrame, timeperiod: int = 5) -> DataFrame:
    """Calculate bollinger bands"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Close"])

    upperband, middleband, lowerband = talib.BBANDS(df.Close, timeperiod=timeperiod, nbdevup=2, nbdevdn=2, matype=0)
    df = df.assign(upperband_bb=upperband)
    df = df.assign(middleband_bb=middleband)
    df = df.assign(lowerband_bb=lowerband)

    return df.loc[:, ["Date", "symbol", "upperband_bb", "middleband_bb", "lowerband_bb"]]


def rsi(df: DataFrame, timeperiod: int = 14) -> DataFrame:
    """Calculate RSI"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Close"])

    rsi = talib.RSI(df.Close, timeperiod=timeperiod)
    col_name = f"rsi_{timeperiod}"
    df = df.assign(**{col_name: rsi})
    return df.loc[:, ["Date", "symbol", col_name]]


def stochastic_rsi(
    df: DataFrame, timeperiod: int = 14, fastk_period: int = 5, fastd_period: int = 3, fastd_matype: int = 0
) -> DataFrame:
    """Calculate Stochastic RSI"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Close"])

    fastk, fastd = talib.STOCHRSI(
        df.Close, timeperiod=timeperiod, fastk_period=fastk_period, fastd_period=fastd_period, fastd_matype=fastd_matype
    )
    
    df = df.assign(
        **{
            "stochastic_rsi_K": fastk,
            "stochastic_rsi_D": fastd,
        }
    )
    stochastic_rsi_crossover = where(df["stochastic_rsi_K"] >= df["stochastic_rsi_D"], 1, -1)
    df = df.assign(stochastic_rsi_crossover=stochastic_rsi_crossover)

    return df.loc[:, ["Date", "symbol", "stochastic_rsi_K", "stochastic_rsi_D", "stochastic_rsi_crossover"]]


def macd(df: DataFrame, fastperiod: int = 12, slowperiod: int = 26, signalperiod: int = 9) -> DataFrame:
    """Calculate MACD"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Close"])

    macd, macdsignal, macdhist = talib.MACD(
        df.Close, fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod
    )

    df = df.assign(macd=macd)
    df = df.assign(macdsignal=macdsignal)
    df = df.assign(macdhist=macdhist)
    macdtrend = where(df["macd"] > df["macd"].shift(1), 1, where(df["macd"] < df["macd"].shift(1), -1, 0))
    df = df.assign(macdtrend=macdtrend)
    macd_streak = df["macdtrend"].groupby((df["macdtrend"] != df["macdtrend"].shift(1)).cumsum()).cumcount() + 1
    df = df.assign(macdstreak=macd_streak)
    return df.loc[:, ["Date", "symbol", "macd", "macdsignal", "macdhist", "macdtrend", "macdstreak"]]


def natr(df: DataFrame, timeperiod: int = 14) -> DataFrame:
    """Calculate MACD"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Close", "High", "Low"])

    natr = talib.NATR(high=df.High, low=df.Low, close=df.Close, timeperiod=timeperiod)
    col_name = f"natr_{timeperiod}"
    df = df.assign(**{col_name: natr})

    return df.loc[:, ["Date", "symbol", col_name]]


def volume_ema(df: DataFrame, timeperiod: int = 14) -> DataFrame:
    """Calculate volume ema"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Volume"])

    vol_ema = talib.EMA(high=df.High, low=df.Low, close=df.Close, timeperiod=timeperiod)
    col_name = f"vol_ema_{timeperiod}"
    df = df.assign(**{col_name: vol_ema})

    return df.loc[:, ["Date", "symbol", col_name]]


def obv(df: DataFrame) -> DataFrame:
    """Calculate volume ema"""

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Volume"])

    obv = talib.OBV(real=df.Close, volume=df.Volume)
    df = df.assign(**{"obv": obv})

    return df.loc[:, ["Date", "symbol", "obv"]]


def supertrend(df: DataFrame, timeperiod: int = 10, multiplier: float = 2.0) -> DataFrame:

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Close", "High", "Low"])

    high = df.High
    low = df.Low
    close = df.Close

    st_up = 1
    st_down = -1

    price_diffs = [high - low, high - close.shift(1), close.shift(1) - low]
    true_range = concat(price_diffs, axis=1)
    true_range = true_range.abs().max(axis=1)

    atr = true_range.ewm(alpha=1 / timeperiod, min_periods=timeperiod).mean()

    hl2 = (high + low) / 2

    upperband = (hl2 + (multiplier * atr)).to_numpy()
    lowerband = (hl2 - (multiplier * atr)).to_numpy()

    supertrend = array([st_up] * len(df))

    for i in range(1, len(df.index)):
        curr = i
        prev = i - 1

        # if current close price crosses above upperband
        if close.iloc[curr] > upperband[prev]:
            supertrend[curr] = st_up
        # if current close price crosses below lowerband
        elif close.iloc[curr] < lowerband[prev]:
            supertrend[curr] = st_down
        # else, the trend continues
        else:
            supertrend[curr] = supertrend[prev]

            # adjustment to the final bands
            if supertrend[curr] == st_up and lowerband[curr] < lowerband[prev]:
                lowerband[curr] = lowerband[prev]
            if supertrend[curr] == st_down and upperband[curr] > upperband[prev]:
                upperband[curr] = upperband[prev]

        # to remove bands according to the trend direction
        if supertrend[curr] == st_up:
            upperband[curr] = None
        else:
            lowerband[curr] = None

    df = df.loc[:, ["Date", "symbol"]]

    supertrend[supertrend == 1] = 1
    supertrend[supertrend == 0] = -1
    df = df.assign(supertrend=supertrend)

    # df["supertrend_change"] = where(
    #     df["supertrend"] > df["supertrend"].shift(1), 1, where(df["supertrend"] < df["supertrend"].shift(1), -1, 0)
    # )
    supertrend_streak = df["supertrend"].groupby((df["supertrend"] != df["supertrend"].shift()).cumsum()).cumcount() + 1
    df = df.assign(supertrend_streak=supertrend_streak)

    # df.loc[:, "lowerband"] = lowerband
    # df.loc[:, "upperband"] = upperband
    df = df.assign(lowerband_st=lowerband)
    df = df.assign(upperband_st=upperband)

    return df.loc[:, ["Date", "symbol", "supertrend", "supertrend_streak", "lowerband_st", "upperband_st"]]


def pattern_recognition(df: DataFrame) -> DataFrame:

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Open", "High", "Open", "Close"])

    df["CDL2CROWS"] = talib.CDL2CROWS(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDL3BLACKCROWS"] = talib.CDL3BLACKCROWS(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDL3INSIDE"] = talib.CDL3INSIDE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDL3LINESTRIKE"] = talib.CDL3LINESTRIKE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDL3OUTSIDE"] = talib.CDL3OUTSIDE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDL3STARSINSOUTH"] = talib.CDL3STARSINSOUTH(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDL3WHITESOLDIERS"] = talib.CDL3WHITESOLDIERS(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLABANDONEDBABY"] = talib.CDLABANDONEDBABY(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLADVANCEBLOCK"] = talib.CDLADVANCEBLOCK(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLBELTHOLD"] = talib.CDLBELTHOLD(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLBREAKAWAY"] = talib.CDLBREAKAWAY(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLCLOSINGMARUBOZU"] = talib.CDLCLOSINGMARUBOZU(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLCONCEALBABYSWALL"] = talib.CDLCONCEALBABYSWALL(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLCOUNTERATTACK"] = talib.CDLCOUNTERATTACK(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLDARKCLOUDCOVER"] = talib.CDLDARKCLOUDCOVER(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLDOJI"] = talib.CDLDOJI(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLDOJISTAR"] = talib.CDLDOJISTAR(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLDRAGONFLYDOJI"] = talib.CDLDRAGONFLYDOJI(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLENGULFING"] = talib.CDLENGULFING(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLEVENINGDOJISTAR"] = talib.CDLEVENINGDOJISTAR(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLEVENINGSTAR"] = talib.CDLEVENINGSTAR(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLGAPSIDESIDEWHITE"] = talib.CDLGAPSIDESIDEWHITE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLGRAVESTONEDOJI"] = talib.CDLGRAVESTONEDOJI(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHAMMER"] = talib.CDLHAMMER(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHANGINGMAN"] = talib.CDLHANGINGMAN(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHARAMI"] = talib.CDLHARAMI(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHARAMICROSS"] = talib.CDLHARAMICROSS(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHIGHWAVE"] = talib.CDLHIGHWAVE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHIKKAKE"] = talib.CDLHIKKAKE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHIKKAKEMOD"] = talib.CDLHIKKAKEMOD(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLHOMINGPIGEON"] = talib.CDLHOMINGPIGEON(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLIDENTICAL3CROWS"] = talib.CDLIDENTICAL3CROWS(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLINNECK"] = talib.CDLINNECK(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLINVERTEDHAMMER"] = talib.CDLINVERTEDHAMMER(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLKICKING"] = talib.CDLKICKING(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLKICKINGBYLENGTH"] = talib.CDLKICKINGBYLENGTH(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLLADDERBOTTOM"] = talib.CDLLADDERBOTTOM(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLLONGLEGGEDDOJI"] = talib.CDLLONGLEGGEDDOJI(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLLONGLINE"] = talib.CDLLONGLINE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLMARUBOZU"] = talib.CDLMARUBOZU(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLMATCHINGLOW"] = talib.CDLMATCHINGLOW(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLMATHOLD"] = talib.CDLMATHOLD(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLMORNINGDOJISTAR"] = talib.CDLMORNINGDOJISTAR(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLMORNINGSTAR"] = talib.CDLMORNINGSTAR(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLONNECK"] = talib.CDLONNECK(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLPIERCING"] = talib.CDLPIERCING(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLRICKSHAWMAN"] = talib.CDLRICKSHAWMAN(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLRISEFALL3METHODS"] = talib.CDLRISEFALL3METHODS(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLSEPARATINGLINES"] = talib.CDLSEPARATINGLINES(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLSHOOTINGSTAR"] = talib.CDLSHOOTINGSTAR(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLSHORTLINE"] = talib.CDLSHORTLINE(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLSPINNINGTOP"] = talib.CDLSPINNINGTOP(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLSTALLEDPATTERN"] = talib.CDLSTALLEDPATTERN(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLSTICKSANDWICH"] = talib.CDLSTICKSANDWICH(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLTAKURI"] = talib.CDLTAKURI(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLTASUKIGAP"] = talib.CDLTASUKIGAP(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLTHRUSTING"] = talib.CDLTHRUSTING(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLTRISTAR"] = talib.CDLTRISTAR(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLUNIQUE3RIVER"] = talib.CDLUNIQUE3RIVER(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLUPSIDEGAP2CROWS"] = talib.CDLUPSIDEGAP2CROWS(high=df.High, low=df.Low, open=df.Open, close=df.Close)
    df["CDLXSIDEGAP3METHODS"] = talib.CDLXSIDEGAP3METHODS(high=df.High, low=df.Low, open=df.Open, close=df.Close)

    return df.loc[
        :,
        [
            "Date",
            "symbol",
            "CDL2CROWS",
            "CDL3BLACKCROWS",
            "CDL3INSIDE",
            "CDL3LINESTRIKE",
            "CDL3OUTSIDE",
            "CDL3STARSINSOUTH",
            "CDL3WHITESOLDIERS",
            "CDLABANDONEDBABY",
            "CDLADVANCEBLOCK",
            "CDLBELTHOLD",
            "CDLBREAKAWAY",
            "CDLCLOSINGMARUBOZU",
            "CDLCONCEALBABYSWALL",
            "CDLCOUNTERATTACK",
            "CDLDARKCLOUDCOVER",
            "CDLDOJI",
            "CDLDOJISTAR",
            "CDLDRAGONFLYDOJI",
            "CDLENGULFING",
            "CDLEVENINGDOJISTAR",
            "CDLEVENINGSTAR",
            "CDLGAPSIDESIDEWHITE",
            "CDLGRAVESTONEDOJI",
            "CDLHAMMER",
            "CDLHANGINGMAN",
            "CDLHARAMI",
            "CDLHARAMICROSS",
            "CDLHIGHWAVE",
            "CDLHIKKAKE",
            "CDLHIKKAKEMOD",
            "CDLHOMINGPIGEON",
            "CDLIDENTICAL3CROWS",
            "CDLINNECK",
            "CDLINVERTEDHAMMER",
            "CDLKICKING",
            "CDLKICKINGBYLENGTH",
            "CDLLADDERBOTTOM",
            "CDLLONGLEGGEDDOJI",
            "CDLLONGLINE",
            "CDLMARUBOZU",
            "CDLMATCHINGLOW",
            "CDLMATHOLD",
            "CDLMORNINGDOJISTAR",
            "CDLMORNINGSTAR",
            "CDLONNECK",
            "CDLPIERCING",
            "CDLRICKSHAWMAN",
            "CDLRISEFALL3METHODS",
            "CDLSEPARATINGLINES",
            "CDLSHOOTINGSTAR",
            "CDLSHORTLINE",
            "CDLSPINNINGTOP",
            "CDLSTALLEDPATTERN",
            "CDLSTICKSANDWICH",
            "CDLTAKURI",
            "CDLTASUKIGAP",
            "CDLTHRUSTING",
            "CDLTRISTAR",
            "CDLUNIQUE3RIVER",
            "CDLUPSIDEGAP2CROWS",
            "CDLXSIDEGAP3METHODS",
        ],
    ]


def stages(
    df: DataFrame,
    # timeperiod: int = 14,
    short_moving_average: int = 50,
    long_moving_average: int = 200,
    trend_smoothing: int = 5,
) -> DataFrame:

    validate_columns(df_columns=df.columns, required_columns=["Date", "symbol", "Close"])

    # Stage 1 10-Week Line is rising and is just below the 40-Week Line. The 40-Week Line is flattening out.
    # Stage 2 10-Week Line is rising and is above the 40-Week Line. The 40-Week Line is also rising.
    # Stage 3 10 Week Line is heading down and the 40-Week Line is flattening.
    # Stage 4 The 10 and 40-Week Lines are both down trending. The 10-Week Line is below the 40-Week Line.

    df = df.sort_values("Date", ascending=False)

    # short_moving_average = 50  # The short moving average
    # long_moving_average = 200  # the long moving average
    # weekly_MA_smoothing = 5
    # trend_smoothing = 5
    trend_null_zone = 0.0005
    rel_trend_null_zone = 0.1
    # close_rel_KeyMA_null_zone = 0.03
    # KeyMA = 50

    # # Add week index
    # df = df.merge(generate_week_ids(), how="left", on="Date")

    # weekly_high = (
    #     DataFrame(
    #         df.set_index("Date")
    #         .loc[:, ["High", "week_id"]]
    #         .sort_values("Date", ascending=True)
    #         .groupby("week_id")["High"]
    #         .expanding(min_periods=1)
    #         .max()
    #     )
    #     .reset_index(drop=False)
    #     .rename(columns={"High": "weekly_high"})
    # )
    # weekly_low = (
    #     DataFrame(
    #         df.set_index("Date")
    #         .loc[:, ["Low", "week_id"]]
    #         .sort_values("Date", ascending=True)
    #         .groupby("week_id")["Low"]
    #         .expanding(min_periods=1)
    #         .min()
    #     )
    #     .reset_index(drop=False)
    #     .rename(columns={"Low": "weekly_low"})
    # )
    # weekly_open = (
    #     DataFrame(
    #         df.set_index("Date")
    #         .loc[:, ["Open", "week_id"]]
    #         .sort_values("Date", ascending=True)
    #         .groupby("week_id")
    #         .head(1)
    #     )
    #     .reset_index(drop=True)
    #     .rename(columns={"Open": "weekly_open"})
    # )
    # df = (
    #     df.merge(weekly_low, how="inner", on=["Date", "week_id"])
    #     .merge(weekly_high, how="inner", on=["Date", "week_id"])
    #     .merge(weekly_open, how="inner", on=["week_id"])
    # )
    # df["close_w"] = df["close"]
    # weekly_close_ShortMA = (
    #     pd.DataFrame(
    #         df.sort_values("datetime", ascending=True)
    #         .set_index("week_id")
    #         .groupby("week_id")["close_w"]
    #         .tail(1)
    #         .rolling(window=ShortMA, min_periods=ShortMA)
    #         .mean()
    #     )
    #     .reset_index(drop=False)
    #     .rename(columns={"close_w": "close_w_ShortMA"})
    # )
    # weekly_close_LongMA = (
    #     pd.DataFrame(
    #         df.sort_values("datetime", ascending=True)
    #         .set_index("week_id")
    #         .groupby("week_id")["close_w"]
    #         .tail(1)
    #         .rolling(window=LongMA, min_periods=LongMA)
    #         .mean()
    #     )
    #     .reset_index(drop=False)
    #     .rename(columns={"close_w": "close_w_LongMA"})
    # )

    # df = (
    #     df.merge(weekly_close_ShortMA, how="left", on="week_id")
    #     .merge(weekly_close_LongMA, how="left", on="week_id")
    #     .sort_values("datetime", ascending=True)
    # )
    # df["close_w_ShortMA_"] = df["close_w_ShortMA"].rolling(window=weekly_MA_smoothing, min_periods=1).mean()
    # df["close_w_LongMA_"] = df["close_w_LongMA"].rolling(window=weekly_MA_smoothing, min_periods=1).mean()
    df["close_ShortMA"] = df["Close"].rolling(window=short_moving_average, min_periods=1).mean()
    df["close_ShortMA_ratio"] = df["Close"] / df["close_ShortMA"]

    df["close_LongMA"] = df["Close"].rolling(window=long_moving_average, min_periods=1).mean()
    df["close_LongMA_ratio"] = df["Close"] / df["close_LongMA"]

    df["close_ShortMA_trend"] = (
        (df["close_ShortMA"] / df["close_ShortMA"].shift(1)).rolling(window=trend_smoothing, min_periods=1).mean()
    )
    df["close_LongMA_trend"] = (
        (df["close_LongMA"] / df["close_LongMA"].shift(1)).rolling(window=trend_smoothing, min_periods=1).mean()
    )
    df["close_ShortMA_trend_val"] = where(
        df["close_ShortMA_trend"] < (1 - trend_null_zone),
        -1,
        where(df["close_ShortMA_trend"] > (1 + trend_null_zone), 1, 0),
    )
    df["close_LongMA_trend_val"] = where(
        df["close_LongMA_trend"] < (1 - trend_null_zone),
        -1,
        where(df["close_LongMA_trend"] > (1 + trend_null_zone), 1, 0),
    )

    df["close_ShortMA_LongMA_trend"] = (df["close_ShortMA"] / df["close_LongMA"]).fillna(1)
    df["close_ShortMA_LongMA_trend_val"] = where(
        (df["close_ShortMA_LongMA_trend"] < (1 - trend_null_zone)),
        -2,
        where(
            ((df["close_ShortMA_LongMA_trend"] >= (1 - trend_null_zone)) & (df["close_ShortMA_LongMA_trend"] < 1)),
            -1,
            where(
                ((df["close_ShortMA_LongMA_trend"] >= 1) & (df["close_ShortMA_LongMA_trend"] <= (1 + trend_null_zone))),
                1,
                where((df["close_ShortMA_LongMA_trend"] > (1 + rel_trend_null_zone)), 2, 0),
            ),
        ),
    )

    # df["close_w_ShortMA_trend"] = (
    #     (df["close_w_ShortMA_"] / df["close_w_ShortMA_"].shift(1))
    #     .rolling(window=trend_smoothing, min_periods=1)
    #     .mean()
    # )
    # df["close_w_LongMA_trend"] = (
    #     (df["close_w_LongMA_"] / df["close_w_LongMA_"].shift(1))
    #     .rolling(window=trend_smoothing, min_periods=1)
    #     .mean()
    # )

    # df["close_w_ShortMA_LongMA_trend"] = (df["close_w_ShortMA"] / df["close_w_LongMA"]).fillna(1)

    # df["close_w_ShortMA_trend_val"] = where(
    #     df["close_w_ShortMA_trend"] < (1 - MA_trend_null_zone),
    #     -1,
    #     where(df["close_w_ShortMA_trend"] > (1 + MA_trend_null_zone), 1, 0),
    # )
    # df["close_w_LongMA_trend_val"] = where(
    #     df["close_w_LongMA_trend"] < (1 - MA_trend_null_zone),
    #     -1,
    #     where(df["close_w_LongMA_trend"] > (1 + MA_trend_null_zone), 1, 0),
    # )
    # df["close_d_KeyMA_trend_val"] = where(
    #     df["close_d_KeyMA_trend"] < (1 - trend_null_zone),
    #     -1,
    #     where(df["close_d_KeyMA_trend"] > (1 + trend_null_zone), 1, 0),
    # )
    # df["close_d_KeyMA_rel_close_val"] = where(
    #     df["close_d_KeyMA_rel_close"] < (1 - close_rel_KeyMA_null_zone),
    #     -1,
    #     where(df["close_d_KeyMA_rel_close"] > (1 + close_rel_KeyMA_null_zone), 1, 0),
    # )
    # df["close_w_ShortMA_LongMA_trend_val"] = where(
    #     (df["close_w_ShortMA_LongMA_trend"] < (1 - trend_null_zone)),
    #     -2,
    #     where(
    #         (
    #             (df["close_w_ShortMA_LongMA_trend"] >= (1 - trend_null_zone))
    #             & (df["close_w_ShortMA_LongMA_trend"] < 1)
    #         ),
    #         -1,
    #         where(
    #             (
    #                 (df["close_w_ShortMA_LongMA_trend"] >= 1)
    #                 & (df["close_w_ShortMA_LongMA_trend"] <= (1 + trend_null_zone))
    #             ),
    #             1,
    #             where((df["close_w_ShortMA_LongMA_trend"] > (1 + rel_trend_null_zone)), 2, 0),
    #         ),
    #     ),
    # )

    stage_table = DataFrame(
        [
            [1, 0, -1, 1],
            [0, 0, 1, 2],
            [0, 0, 2, 2],
            [1, 0, 1, 2],
            [1, 0, 2, 2],
            [0, 1, 1, 2],
            [0, 1, 2, 2],
            [1, 1, 1, 2],
            [1, 1, 2, 2],
            [-1, 0, -2, -1],
            [-1, 0, 1, -1],
            [-1, 0, 1, -1],
            [-1, 0, 2, -1],
            [-1, -1, -2, -2],
            [-1, -1, -1, -2],
        ],
        columns=["close_ShortMA_trend_val", "close_LongMA_trend_val", "close_ShortMA_LongMA_trend_val", "stage"],
    )
    df = df.merge(
        stage_table,
        how="left",
        on=["close_ShortMA_trend_val", "close_LongMA_trend_val", "close_ShortMA_LongMA_trend_val"],
    )
    df["stage"] = df["stage"].fillna(0).astype(int)
    df = df.rename(
        columns={
            "close_ShortMA_trend_val": "Short_trend",
            "close_LongMA_trend_val": "Long_trend",
            "close_ShortMA_LongMA_trend_val": "Short_Long_trend",
        }
    )
    return df.sort_values("Date", ascending=True).loc[
        :, ["Date", "symbol", "stage", "Short_trend", "Long_trend", "Short_Long_trend"]
    ]


# # a = talib.get_function_groups()
# # print(a)
