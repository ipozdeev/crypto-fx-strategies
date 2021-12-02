import pandas as pd
import numpy as np
import datetime
from joblib import Memory
import os

data_dir = os.path.join(os.environ.get("PROJECT_ROOT"), "data/")
memory = Memory(data_dir, verbose=0)


@memory.cache(ignore=["save"])
def save_spot(symbol="BTCUSDT", kline_size="1m", save=True, **kwargs):
    """Query historical data from binance spot market.

    Parameters
    ----------
    symbol: str
        binance symbol
    kline_size: str
        sampling frequency of open-high-low-close bars; see
        https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#enum-definitions
    save: bool
        Whether to save the results on the disk in 'DATA_PATH' dir. Default is
        true
    kwargs
        parameters passed to `CLIENT.get_historical_klines`

    Returns
    -------
    data: pd.DataFrame
        with OHLC bars sampled at 'kline_size' frequency.
        The data also includes volume, volume of market orders and number of
        trades

    """
    filename = "{}_{}.csv".format(symbol, kline_size)
    # Try to reach the file in the data directory
    try:
        data = pd.read_csv(os.path.join(data_dir, filename),
                           header=0, index_col=0,
                           parse_dates=True)
    except FileNotFoundError:
        data = pd.DataFrame()

    # If the data is not empty locate the last open timestamp, else query all
    if len(data) > 0:
        s_dt = data.index[-1]
    else:
        s_dt = datetime.datetime.strptime("2017-01-01", "%Y-%m-%d")

    kwargs["start_str"] = kwargs.get("start_str",
                                     s_dt.strftime("%d %b %Y %H:%M:%S"))

    e_dt = pd.to_datetime(
        CLIENT.get_klines(symbol=symbol, interval=kline_size)[-1][0],
        unit="ms")

    kwargs["end_str"] = kwargs.get("end_str",
                                   e_dt.strftime("%d %b %Y %H:%M:%S"))

    # Load and process the data
    klines = CLIENT.get_historical_klines(
        symbol=symbol, interval=kline_size, **kwargs
    )

    new_data = pd.DataFrame(
        klines,
        columns=["timestamp_open", "open", "high", "low", "close", "volume",
                 "timestamp_close", "quote_volume", "trades",
                 "taker_buy_base_volume", "taker_buy_quote_volume",
                 "ignore"]
        )

    for c in ["open", "high", "low", "close", "volume", "quote_volume",
              "taker_buy_base_volume", "taker_buy_quote_volume"]:
        new_data.loc[:, c] = new_data.loc[:, c].astype(np.float32)

    for c in ["timestamp_open", "timestamp_close"]:
        new_data.loc[:, c] = pd.to_datetime(new_data.loc[:, c], unit="ms")

    new_data.set_index("timestamp_open", inplace=True)

    if len(data) > 0:
        # Drop duplicate index from the old data, the last candle there might
        # be incomplete
        common_idx = new_data.index.intersection(data.index)
        data.drop(common_idx, axis=0, inplace=True)

        # Concatenate and dump and/or return
        data = pd.concat([data, new_data], axis=0, verify_integrity=True)
    else:
        data = new_data

    if save:
        # TODO: bad practice, use os.path.join to avoid problems w/slashes
        data.to_csv(os.path.join(data_dir, filename))

    return data
