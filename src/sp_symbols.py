from pandas import read_html, DataFrame


class SP:
    PAGE_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    def __init__(self) -> None:
        self.company_table = self._get_company_table()

    def _get_company_table(self) -> DataFrame:
        return read_html(self.PAGE_URL)[0]

    def __call__(self) -> DataFrame:
        return self.company_table


# a = SP()
# print(a())
