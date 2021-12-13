import pandas as pd
import requests
import zipfile
import pytz
import datetime
import time
import os
from joblib import Memory

from .setup import ROOT_URL, ROOT_URL_PERP, ROOT_URL_SPOT

data_dir = os.path.join(os.environ.get("PROJECT_ROOT"), "data/")
memory = Memory(data_dir, verbose=0)


@memory.cache
def get_funding_rate() -> pd.DataFrame:
    """Get abd and rel funding rates of several ccur.

    Works via an API call to kraken's website.

    The relative rate is the one displayed at the front end; the absolute
    rate is calculated as (rel rate / price), denominated in the
    cryprocurrency and to be used for accounting purposes.

    Returns
    -------
    DataFrame
        with time zone-aware index, containing absolute (in fractions of 1)
        and relative funding rates, per hour.
    """
    endpoint = "historicalfundingrates"

    res = dict()

    for c in ["XBT", "BCH", "LTC", "ETH", "XRP"]:
        # request
        parameters = f"symbol=PI_{c}USD"
        u = f"{ROOT_URL}/{endpoint}?{parameters}"
        resp = requests.get(u)

        # convert to DataFrame
        chunk = pd.DataFrame.from_records(resp.json()["rates"])
        chunk.index = chunk.pop("timestamp").map(pd.to_datetime)
        chunk = chunk.rename(columns={"fundingRate": "absolute",
                                      "relativeFundingRate": "relative"})
        res[c] = chunk

    res = pd.concat(res, axis=1, names=["asset", "rate"])\
        .stack().reset_index() \
        .rename(columns=lambda x: x.lower())

    return res


def get_perpetual(mid=False) -> pd.DataFrame:
    """Get prices of perpetual contracts, in USD.

    Kraken perps are coin-margined.

    Returns
    -------
    DataFrame
        with time zone-aware index

    """
    data_path = os.path.join(data_dir, "perp")
    data = pd.read_feather(f"{data_path}/perp-mba-1h-kraken.ftr")

    res = data \
        .pivot(index="timestamp", columns=["asset", "side"], values="price")

    if mid:
        res = res.xs("mid", axis=1, level="side")

    return res


@memory.cache
def get_spot_ohlc() -> pd.DataFrame:
    """Get spot ohlcvt data from .csv files saved from kraken.

    The .csv files can be found on Google Drive, link here:
    https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data

    Download a separate file for each currency of interest; files are in
    format '<XXX>USD_60.csv, where <XXX> s the 3-letter code, such as XBT,
    and 60 refers to the frequency of bars, in minutes. Save the files under
    "../data/spot" for the functions to access them.
    """
    data_path = os.path.join(data_dir, "spot")
    zips = [f for f in os.listdir(data_path) if f.endswith("zip")]
    columns = ["timestamp", "open", "high", "low", "close", "volume", "trades"]

    res = dict()

    for z in zips:

        zip_fname = f"{data_path}/{z}"
        base_c = z.split("_")[0]
        csv_fname = f"{base_c}USD_60.csv"

        # unzip if not yet
        with zipfile.ZipFile(zip_fname, mode='r') as uz:
            uz.extract(member=csv_fname, path=data_path)

        # read in chunks, columns are timestamp, price, volume
        chunk = pd.read_csv(f"{data_path}/{csv_fname}",
                            header=None,
                            dtype={0: int, 6: int})
        os.remove(f"{data_path}/{csv_fname}")

        chunk.columns = columns
        chunk.index = chunk.pop("timestamp") \
            .map(lambda x: datetime.datetime.fromtimestamp(x, tz=pytz.UTC))

        res[base_c] = chunk["close"]

    res = pd.concat(res, axis=1).rename(columns=lambda x: x.lower())

    return res


def _process_perpetual_api_call(request_str) -> (dict, pd.Timestamp):
    """Process result of one call to the perpetual prices API.

    Parameters
    ----------
    request_str : str

    """
    response = requests.get(request_str).json()

    chunk = []

    # each element is one execution: collect price, qty etc.
    for e_ in response["elements"]:
        exe = e_["event"]["Execution"]["execution"]
        timestamp = exe["timestamp"]
        price = exe["price"]
        quantity = exe["quantity"]
        side = exe["takerOrder"].get("direction")

        chunk.append({"timestamp": timestamp, "p": price, "side": side,
                      "q": quantity})

    # final time to start the next iteration
    t_final = max(
        [pd.Timestamp(exe_["timestamp"], unit="ms")
         for exe_ in response["elements"]]
    )

    return chunk, t_final


@memory.cache
def get_perpetual_from_api(currency, start_dt, end_dt) -> pd.DataFrame:
    """Get perpetual bid/ask prices using the API.

    Assumes USD as the counter currency.

    Pulls perpetual futures price data in chunks of size 1000 until 1000
    such chunks have been collected (for a total of 1'000'000 executions),
    then aggregates at hourly frequeny by taking the volume-weighted average
    of prices, also separating into the buy and sell transactions; continues
    in this fashion until `end_dt` is reached.

    Parameters
    ----------
    currency : str
        3-letter ISO such as 'xrp', lowercase
    start_dt : datetime-like
    end_dt : datetime-like

    Returns
    -------
    pandas.DataFrame
        with columns 'timestamp', 'side', 'price'
    """
    # pair, e.g. ethusd; 'pi_' means perpetual contracts
    pair = f"pi_{currency}usd"

    # init time count
    t = start_dt.tz_localize(None)
    end_dt = end_dt.tz_localize(None)

    res_raw = []

    while t < end_dt:
        print(f"timestamp: {t}")

        # request string: timestamp on Kraken is in ms NB: must be integer!
        parameters = f"since={(t.timestamp() * 1000):.0f}&sort=asc"
        request_str = f"{ROOT_URL_PERP.format(pair)}?{parameters}"

        chunk_, t = _process_perpetual_api_call(request_str)

        res_raw += chunk_

    # calculate price as weighted mean by buy/sell
    res_df = pd.DataFrame.from_records(res_raw)
    res_df.loc[:, ["p", "q"]] = \
        res_df.loc[:, ["p", "q"]].astype(float)
    res_df.loc[:, "timestamp"] = res_df["timestamp"]\
        .map(lambda x: pd.Timestamp(x, unit="ms", tz="UTC"))

    res_df.loc[:, "pq"] = res_df["p"].mul(res_df["q"])

    # construct 10-min windows (centered on :00, :10 etc.)
    res_df.loc[:, "timestamp"] -= pd.to_timedelta("5T")

    # calculate size-weighted mean price in those windows
    data_agg = res_df.set_index("timestamp").groupby(
        [pd.Grouper(freq="10T", label="right"), "side"],
    ).sum()
    price_ = data_agg["pq"] / data_agg["q"]

    # rename, reindex
    res = price_.rename("price").reset_index()
    res.loc[:, "side"] = res["side"].map({"Buy": "ask", "Sell": "bid"})

    return res


def _process_spot_api_call(request_str) -> (pd.DataFrame, pd.Timestamp):
    """Process result of one call to the spot prices API.

    Parameters
    ----------
    request_str : str
    """
    resp = requests.get(request_str).json()

    # convert to DataFrame
    k = [k_ for k_ in resp["result"].keys() if k_ != "last"][0]

    chunk = pd.DataFrame.from_records(resp["result"][k]).iloc[:, :4]

    t_final = pd.Timestamp(chunk[2].max(), unit="s")

    return chunk, t_final


@memory.cache
def get_spot_from_api(currency, start_dt, end_dt) -> pd.DataFrame:
    """Get spot prices of usd pairs from Kraken using API.

    Introduces delay of 1.9 sec after each API call.

    Parameters
    ----------
    currency : str
        3-letter ISO, e.g. 'xrp'
    start_dt : pd.Timestamp
    end_dt : pd.Timestamp
    """
    # parameters
    cols = ["price", "volume", "timestamp", "side"]
    endpoint = "Trades"

    t = start_dt.tz_localize(None)
    end_dt = end_dt.tz_localize(None)

    chunks = []

    while t < end_dt:
        print(f"{currency} - {t}")
        # timestamp is in seconds
        parameters = f"pair={currency}usd&since={(t.timestamp()):.4f}"
        request_str = f"{ROOT_URL_SPOT}/{endpoint}?{parameters}"

        chunk_, t = _process_spot_api_call(request_str)

        # idle time
        time.sleep(1.9)

        chunks.append(chunk_)

    data = pd.concat(chunks)
    data.columns = cols
    data.loc[:, ["price", "volume"]] = \
        data.loc[:, ["price", "volume"]].astype(float)
    data.loc[:, "timestamp"] = data["timestamp"] \
        .map(lambda x: pd.Timestamp(x, unit="s", tz="UTC"))
    data.loc[:, "side"] = data["side"].astype("category")

    # we will calculate volume-weighted mean in what follows
    data.loc[:, "pv"] = data["price"].mul(data["volume"])

    # construct 10-min windows (centered on :00, :10 etc.)
    data.loc[:, "timestamp"] -= pd.to_timedelta("5T")

    # calculate size-weighted mean price in those windows
    res_agg = data.set_index("timestamp").groupby(
        [pd.Grouper(freq="10T", label="right"), "side"],
    ).sum()

    res = res_agg["pv"] / res_agg["volume"]

    res = res.rename("price").reset_index()

    return res
