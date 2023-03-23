from pathlib import Path
from pandas import HDFStore, read_parquet
from screener import Screener


data_path = Path("./data/").absolute()
hdf_store = HDFStore(data_path / "store.h5", mode="r")
stock_info = read_parquet("./data/stock_info.parquet")

screener = Screener(hdf=hdf_store, stock_info=stock_info)
hdf_store.close()

picks = screener.ha_streak_screener(period="D", num_periods=25)
print(picks)
