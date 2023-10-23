from datetime import datetime, timedelta
from pandas.tseries.holiday import USFederalHolidayCalendar, GoodFriday
from pandas.tseries.offsets import CustomBusinessDay
from pandas import to_datetime, DataFrame, date_range
from numpy import ceil
from typing import Optional
from pathlib import Path

USFEDHOLIDAYS = USFederalHolidayCalendar()
USFEDHOLIDAYS.merge(GoodFriday, inplace=True)
MARKET_HOLIDAYS = [
    i.astype(datetime).strftime("%Y-%m-%d") for i in list(CustomBusinessDay(calendar=USFEDHOLIDAYS).holidays)
][200:700]

rangebreaks = [dict(bounds=["sat", "mon"]), dict(values=MARKET_HOLIDAYS)]


def back_in_time(
    d: Optional[str] = None, days: Optional[int] = None, weeks: Optional[int] = None, years: Optional[int] = None
):
    d = datetime.now().strftime("%Y-%m-%d") if d is None else d
    d = datetime.strptime(d, "%Y-%m-%d")
    days = 0 if days is None else days
    weeks = 0 if weeks is None else weeks
    days = days if years is None else days + (365 * years)
    d += timedelta(days=-days, weeks=-weeks)
    return d.strftime("%Y-%m-%d")


def generate_week_ids(start_date="2015-01-01"):
    start_day = to_datetime(start_date).weekday()
    df = DataFrame({"Date": date_range(start=start_date, end=datetime.datetime.now().date())})
    df = df.iloc[7 - start_day :]
    df["week_id"] = [j for k in [[i] * 7 for i in range(0, int(ceil(len(df) / 7)))] for j in k][: len(df)]
    df["Date"] = df["Date"].astype(str)
    return df.loc[:, ["Date", "week_id"]]


def make_directories(data_path: Path) -> None:
    for path in ["", "symbols/", "OHLCV/D/", "OHLCV/W/", "OHLCV/M/", "signals/D/", "signals/W/", "signals/M/"]:
        (data_path / path).mkdir(parents=True, exist_ok=True)


def camelcase(string: str, delimiter: str = " ") -> str:
    return delimiter.join([word.capitalize() for word in string.split(delimiter)])
