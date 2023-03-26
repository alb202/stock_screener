import yfinance as yf
from pandas import DataFrame, isna
from joblib import Parallel, delayed
from enum import Enum
from proxy import Proxy


class IntervalEnum(Enum):
    _1h = "1h"
    _1d = "1d"
    _1wk = "1wk"
    _1mo = "1mo"


class PeriodEnum(Enum):
    _1m = "1m"
    _6m = "6m"
    _1y = "1y"
    _2y = "2y"
    # _3y = "3y"
    _5y = "5y"
    _10y = "10y"
    max = "max"


class Yahoo:

    SECTIONS = ["_info", "_recommendations"]

    def __init__(self, save_data: bool = True, n_jobs: int = 1):

        self.save_data = save_data
        self.n_jobs = n_jobs
        self.proxy = Proxy()

    @staticmethod
    def format_symbols(symbols: str | list, delim: str = " ") -> str:
        if isinstance(symbols, str):
            symbols = symbols.split(delim)

        if isinstance(symbols, list):
            symbols = list(filter(lambda value: value.strip() != "" and not isna(value), symbols))
            # symbols = " ".join(symbols).upper()
            symbols = [symbol.upper() for symbol in symbols]  # " ".join(symbols).upper()
        else:
            raise TypeError(f"Tickers must be a string or a list of strings, not type {type(symbols)}")

        return symbols

    def get_batch_data(self, symbols: list, interval: IntervalEnum = "1d", period: PeriodEnum = "2y") -> DataFrame:
        symbols = self.format_symbols(symbols)
        return Parallel(n_jobs=self.n_jobs)(
            delayed(self.get_ticker_data)(symbol=symbol, interval=interval, period=period) for symbol in symbols
        )

    def get_batch_info(self, symbols: str | list) -> DataFrame:
        symbols = self.format_symbols(symbols)
        return Parallel(n_jobs=self.n_jobs)(delayed(self.get_ticker_info)(symbol) for symbol in symbols)

    def get_ticker_data(self, symbol: str, interval: IntervalEnum = "1d", period: PeriodEnum = "2y") -> DataFrame:

        tries = 0
        while tries < 5:
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(period=period, interval=interval, proxy=self.proxy())
                df["symbol"] = symbol
                df = df.reset_index(drop=False)
                df.Date = df.Date.dt.date
                return df
            except (AttributeError, Exception) as err:
                print(err)
                tries += 1
                pass
        return None

    @staticmethod
    def get_ticker_info(symbol: str) -> dict:
        return yf.Ticker(symbol).fast_info.__dict__

        # tries = 0
        # while True:
        #     try:
        #         data = yf.Ticker(ticker)
        #         # data = {key: value for key, value in data.__dict__.items() if key in self.SECTIONS}
        #         # if len(data["_info"]) > 15:
        #         return data
        #     except (ConnectionError, HTTPError) as e:
        #         print(e)
        #         tries += 1
        #         sleep(5 + (2 * tries))
        #         continue


# a = Yahoo(save_data=False)
# b = a.get_ticker_data(symbol="CETU", interval="1wk", period="5d")
# print(b)
# print(a.format_symbols(["AAPL", "msft", "ntst", "Akdf"]))
# b = a.get_ticker_info(symbols=["AAPL", "msft"])
# b = a.get_batch_info(symbols="AAPL MSFT")
# b = a.get_batch_data(symbols="AAPL MSFT", period="5y", interval="1d")
# print(b)
# c = a.get_ticker_info(symbol="aapl")
# print(concat(c))
# b = a.make_tickers(["AAPL  msft ntst"])
# print(b)
# print(b.info)
# a = yf.download(tickers=["AAPL"], period="2y", interval="1h")
# print(a)
# tickers = yf.Tickers("AAPL")
# print(tickers.tickers["AAPL"].history(period="1d", interval="1h"))


# print(b.get("_quote")())
# print(b.get("_data")())


#                     tmp.info
#                     print(tmp.__dict__.keys())
#                     tmp_ = {k:v for k, v in tmp.__dict__.items() if k in sections}
#                     if len(tmp_['_info'].keys()) > 15:
#                         if save_data:
#                             with open(STOCK_TEMP_DIR+company+'_ticker.pkl', 'wb') as handle:
#                                 pickle.dump(tmp_, handle, protocol=pickle.HIGHEST_PROTOCOL)
#                             return
#                         else:
#                             return tmp_
#                     else:
#                         tries += 1
#                         sleep(5+(2*tries))
#                         continue
#                     print('No data found for '+company)
#                 except (ConnectionError, Exception) as e:
#                     print(e)
#                     tries += 1
#                     sleep(5+(2*tries))
#                     continue

#     ''' The function to create the pickled finance ticker '''
# # def pickle_yfinance_ticker(company,  sections: list = ['_info', '_recommendations']):
#     tries = 0
#     while tries < 10:
#         try:
#             tmp = yf.Ticker(company)
#             tmp.info
#             print(tmp.__dict__.keys())
#             tmp_ = {k:v for k, v in tmp.__dict__.items() if k in sections}
#             if len(tmp_['_info'].keys()) > 15:
#                 if save_data:
#                     with open(STOCK_TEMP_DIR+company+'_ticker.pkl', 'wb') as handle:
#                         pickle.dump(tmp_, handle, protocol=pickle.HIGHEST_PROTOCOL)
#                     return
#                 else:
#                     return tmp_
#             else:
#                 tries += 1
#                 sleep(5+(2*tries))
#                 continue
#             print('No data found for '+company)
#         except (ConnectionError, Exception) as e:
#             print(e)
#             tries += 1
#             sleep(5+(2*tries))
#             continue
