import pandas as pd
import os
import datetime
from joblib import Memory

cachedir = os.path.join(os.environ.get("PROJECT_ROOT"), "data/")
memory = Memory(cachedir, verbose=0)


def get_funding_rate() -> pd.Series:
    """Fetch BTCUSDT funding rate history.

    Download the csv from https://www.binance.com/en/futures/funding-history/1
    and store it as data/btcusdt-funding-rate-bnc.csv first.

    The raw data is sampled at 10, 18 and 02 hours and refers to funding rate,
    in percent per 8 hours, to be applied to USDT notional.

    Returns
    -------
    pandas.Series
        funding rate, in frac of 1 per 8 hours

    """
    fname = "btcusdt-funding-rate-bnc.csv"

    if fname not in os.listdir("../data/"):
        raise ValueError(
            f"first download data from "
            f"https://www.binance.com/en/futures/funding-history/1 "
            f"and save under data/{fname}"
        )

    data = pd.read_csv(f"data/{fname}", index_col=0, parse_dates=True)\
        .loc[:, "Funding Rate"]\
        .sort_index()\
        .str.strip("%").astype(float)\
        .div(100)

    return data


@memory.cache
def get_perpetual() -> pd.DataFrame:
    root_url = "https://data.binance.vision/data/futures/cm/monthly/klines/" \
               "BTCUSD_PERP/1h/"
    ms = pd.period_range("2020-08", "2021-03", freq="M")

    data = list()
    for m in ms:
        chunk = pd.read_csv(
            root_url + f"BTCUSD_PERP-1h-{m}.zip",
            compression="zip", index_col=0, header=None, parse_dates=True,
            date_parser=lambda x: datetime.datetime.fromtimestamp(int(x)/1000)
        )
        chunk.columns = [
            "Open", "High", "Low", "Close", "Volume",
            "Close time", "Quote asset volume", "Number of trades",
            "Taker buy base asset volume",
            "Taker buy quote asset volume",
            "Ignore"
        ]
        data.append(chunk)

    data = pd.concat(data, axis=0)

    return data

