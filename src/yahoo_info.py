from pandas import DataFrame
from requests.sessions import Session
from requests.adapters import HTTPAdapter
from requests.models import Response
from bs4 import BeautifulSoup
from proxy import Proxy
from urllib3 import disable_warnings, exceptions

disable_warnings(exceptions.InsecureRequestWarning)


class YahooInfo:

    URL = "https://finance.yahoo.com/quote/"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10)"}

    def __init__(self):
        """Initialize yahoo info class"""
        http = HTTPAdapter(max_retries=10)
        self.session = Session()
        self.session.mount("https://", http)
        self.proxies = Proxy()

    def _get_info(self, ticker: str) -> Response:
        """Get yahoo info"""
        parameters = {"p": ticker}
        proxy = f"http://{self.proxies()}"
        return self.session.get(
            url=self._make_url(ticker=ticker),
            headers=self.HEADERS,
            params=parameters,
            proxies={"http": proxy},
            verify=False,
        )

    def _make_url(self, ticker: str) -> str:
        """Make the URL"""
        url = f"{self.URL}{ticker}/profile?p={ticker}"
        # print(url)
        return url

    def ticker_info(self, ticker: str) -> dict:
        """Get sector info for ticker"""
        class_id = "Fw(600)"
        class_type = "span"
        soup = BeautifulSoup(self._get_info(ticker=ticker.upper())._content.decode("utf-8"), features="lxml")
        info = soup.find_all(class_type, class_=class_id)[1:]
        # print(ticker, info)
        ticker_profile = {"symbol": ticker.upper()}
        if len(info) < 2:
            return ticker_profile
        ticker_profile.update(
            # {"industry": info[0].text, "sector": info[1].text, "employees": info[2].text.replace(",", "")}
            {"industry": info[0].text, "sector": info[1].text}
        )
        return ticker_profile


# a = YahooInfo()
# print(a.ticker_info("ENPH"))
