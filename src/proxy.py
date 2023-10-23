from requests.sessions import Session


class Proxy:
    URL = "https://api.proxyscrape.com/v2/?"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    PARAMETERS = {
        "request": "displayproxies",
        "protocol": "http",
        "timeout": 1000,
        "country": "us",
        "ssl": "all",
        "anonymity": "all",
    }

    def __init__(self) -> None:
        self.session = Session()
        self.proxies = self.refresh_proxy_list()
        self.proxy_index = 0

    def refresh_proxy_list(self) -> list:
        """Refresh the proxy list"""
        # return DataFrame(self.get_proxy_table().json().get("data")).ip.to_list()
        try:
            r = [proxy.strip() for proxy in self.get_proxy_table().text.split("\n")]
        except:
            return []
        return r

    def get_proxy_table(self):
        """Get the proxy table"""
        return self.session.get(self.URL, headers=self.HEADERS, params=self.PARAMETERS)

    def __iter__(self):
        """Create the proxy iterator"""
        self.proxy_index = 0
        return self

    def __next__(self) -> str:
        """Iterate through the proxy list"""
        try:
            proxy = self.proxies[self.proxy_index]
            self.proxy_index += 1
            return proxy
        except IndexError:
            self.proxies = self.refresh_proxy_list()
            self.proxy_index = 0
            proxy = self.proxies[self.proxy_index]
            self.proxy_index += 1
            return proxy

    def __call__(self) -> str:
        """Get the next proxy"""
        return self.__next__()


# a = Proxy()
# print(next(a))
# print(next(a))
# print(next(a))
# print(next(a))
# # print(a())
# # print(a())
# # print(a())
# # print(a())
# # print(a())
# # print(a())
