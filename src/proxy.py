from pandas import DataFrame
from requests.sessions import Session


class Proxy:
    URL = "https://proxylist.geonode.com/api/proxy-list?"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10)"}
    PARAMETERS = {"limit": 500, "sort_by": "lastChecked", "sort_type": "desc", "speed": "medium", "protocols": "http"}

    def __init__(self) -> None:
        self.session = Session()
        self.proxies = self.refresh_proxy_list()
        self.proxy_index = 0

    def refresh_proxy_list(self) -> list:
        """Refresh the proxy list"""
        return DataFrame(self.get_proxy_table().json().get("data")).ip.to_list()

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
# print(a())
# print(a())
# print(a())
# print(a())
# print(a())
# print(a())
