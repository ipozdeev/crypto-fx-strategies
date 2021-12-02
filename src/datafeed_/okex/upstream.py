import pandas as pd
import requests
import time
import pytz
import datetime

from .setup import ROOT_URL


def save_perpetual(symbol: str):
    """Get 4-hour klines from OKEX.

    Parameters
    ----------
    symbol
        3-letter xymbol, one of (BTC, ETH, XRP, LTC)

    """
    endpoint = "market/history-candles"

    data = list()
    end_dt = int(datetime.datetime(2021, 6, 30).timestamp() * 1000)

    while True:
        print(f"doing timestamp {end_dt}")
        parameters = f"instId={symbol.upper()}-USD-SWAP&after={end_dt}&bar=1H"
        u = f"{ROOT_URL}/{endpoint}?{parameters}"

        resp = requests.get(u)

        chunk = pd.DataFrame.from_records(resp.json()["data"])

        if len(chunk) < 1:
            break

        data.append(chunk)

        end_dt = chunk[0].min()

        time.sleep(2 / 19)

    res = pd.concat(data, axis=0).iloc[:, :-1]
    res[0] = res[0].map(int).div(1000)\
        .map(lambda x: datetime.datetime.fromtimestamp(x, tz=pytz.UTC))
    res.columns = ["timestamp", "open", "high", "low", "close", "vol"]
    res = res.set_index("timestamp").sort_index(axis=0).astype(float)

    res.reset_index().to_feather(f"data/perp/perp-ohlc-okex-{symbol}.ftr")

    return res

