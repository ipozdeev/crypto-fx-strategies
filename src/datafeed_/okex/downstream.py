import pandas as pd
import os

data_dir = os.path.join(os.environ.get("PROJECT_ROOT"), "data/")


def get_perpetual(symbol: str) -> pd.Series:
    data_path = os.path.join(data_dir, "perp")
    res = pd.read_feather(f"{data_path}/perp-ohlc-okex.ftr")\
        .query(f"tradeable == '{symbol}'")\
        .set_index("timestamp")\
        .loc[:, "close"]

    return res
