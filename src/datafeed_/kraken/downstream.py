import pandas as pd
import requests
import zipfile
import pytz
import datetime
import os
from joblib import Memory

from .setup import ROOT_URL

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
    data_path = "data/perp"
    data = pd.read_feather(f"{data_path}/perp-mba-1h-kraken.ftr")

    res = data \
        .pivot(index="timestamp", columns=["asset", "side"], values="price")

    if mid:
        res = res.xs("mid", axis=1, level="side")

    return res


@memory.cache
def get_spot() -> pd.DataFrame:
    """Get spot ohlcvt data from .csv files saved from kraken.

    The .csv files can be found on Google Drive, link here:
    https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data

    Download a separate file for each currency of interest; files are in
    format '<XXX>USD_60.csv, where <XXX> s the 3-letter code, such as XBT,
    and 60 refers to the frequency of bars, in minutes. Save the files under
    "../data/spot" for the functions to access them.
    """
    data_path = "data/spot"
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


def get_spot_mba() -> pd.DataFrame:

    data_path = "data/spot"

    res = dict()

    for c in ["xbt", "bch", "ltc", "eth", "xrp"]:
        res[c] = pd.read_feather(f"{data_path}/spot-{c}-kraken.ftr")\
            .drop_duplicates(subset=["timestamp"], keep="last")\
            .set_index("timestamp")

    res = pd.concat(res, axis=1, names=["asset", "mba"])
    res.index = res.index.tz_localize(pytz.utc)

    return res
