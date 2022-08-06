import re
import time
import zipfile
from typing import Tuple, List
import pandas as pd
import datetime
import os
import requests
from joblib import Memory
import logging

from ..utilities import aggregate_data

from .setup import ROOT_URL, ROOT_URL_PERP, ROOT_URL_SPOT

data_dir = os.path.join(os.environ.get("PROJECT_ROOT"), "data")

# cache; will use or create folder 'joblib'
memory = Memory(cachedir=data_dir, verbose=False)

logger = logging.getLogger(__name__)


def save_spot_from_ohlcv() -> None:
    """Save spot prices from 1-min OHLCV data.

    Wrapper around `get_spot_from_ohlcv` (loop over currencies) - do read its
    docstring!

    """
    currencies = ["xbt", "bch", "xrp", "ltc", "eth"]

    data = dict()

    for c_ in currencies:
        logger.info(f"saving spot rates for {c_}...")
        data[c_] = _get_spot_from_ohlcv(c_)

    # concat everything and store in feather format
    res = pd.concat(data, axis=0, names=["asset", "index"]) \
        .reset_index(level="asset").reset_index(drop=True)

    path_to_out = os.path.join(data_dir, "prepared/spot/kraken",
                               "spot-close-kraken.ftr")
    res.to_feather(path_to_out)
    logger.info(f"spot rates saved to {path_to_out}")


def save_spot_from_api() -> None:
    """Save bid/ask spot prices using the API.

    This breaks down because of an API issue: in some requests, there are
    huge jumps in time.

    """
    start_dt = pd.Timestamp("2018-06-01")
    end_dt = pd.Timestamp(datetime.date.today())

    data = {}

    for c_ in ["xrp", "eth", "xbt", "bch", "ltc"]:

        data_c = _get_spot_from_api(c_, start_dt, end_dt)

        data[c_] = data_c

    data = pd.concat(data, axis=0, names=["asset", "index"]) \
        .reset_index(level="asset").reset_index(drop=True)

    # save in feather format
    data.to_feather(
        os.path.join(data_dir, "prepared/spot/kraken",
                     "spot-bidask-api-kraken.ftr")
    )


def update_spot_from_api() -> None:
    """Update bid/ask spot prices using the API."""
    path_to_ftr = os.path.join(data_dir, "prepared/spot/kraken")

    data_old = pd.read_feather(
        os.path.join(path_to_ftr, "spot-bidask-api-kraken.ftr")
    )

    # those are the currencies to fetch data on
    currencies = data_old["asset"].unique()

    # start date is the last date of `data_old`
    start_dt = data_old \
        .pivot(index="timestamp", columns=["asset", "side"], values="price") \
        .last_valid_index()

    # end date is the start of today
    end_dt = pd.Timestamp(datetime.date.today(), tz="UTC")

    data = dict()
    for c_ in currencies:
        data_c = _get_spot_from_api(c_, start_dt, end_dt)
        data[c_] = data_c

    data_new = pd.concat(data, axis=0, names=["asset", "index"]) \
        .reset_index(level="asset").reset_index(drop=True)

    data_upd = pd.concat((data_old, data_new)) \
        .drop_duplicates(subset=["asset", "side", "timestamp"]) \
        .reset_index(drop=True)

    data_upd.to_feather(
        os.path.join(path_to_ftr, "spot-bidask-api-kraken.ftr")
    )


def save_perpetual_from_csv() -> None:
    """Process .csv files with perp prices downloadable from Kraken.

    The files keep actual trades, so the data must be aggregated at some
    frequency. Given its sparcity, we resample at the frequency of 10 min
    (:00, :10, :20 etc.), and take the volume-weighted average price around
    that stamp. This means that the resulting DF contains prices indexed with
    10-min stamps, but possibly occurring the maximum of 5 min earlier or
    later than that.

    This also means that the individual files must be processed in pairs of
    two consecutive ones.

    The zip files are downloadable from 'matches_history' folder within the
    Dropbox folder to be found here:
    https://support.kraken.com/hc/en-us/articles/360022835871-Historical-Data

    Download all of them, saving to $PROJECT_ROOT/data/perp/
    """
    logger.info("saving perpetual prices...")

    data_src = os.path.join(data_dir, "raw/perpetual/kraken")
    data_tgt = os.path.join(data_dir, "prepared/perpetual/kraken")

    # find all .csv (sometimes compressed as .zip)
    fs = [f for f in os.listdir(data_src) if f.endswith(("csv", "zip"))]

    # process files in pairs
    months = sorted([re.search("[0-9]{4}-[0-9]{2}", f_).group() for f_ in fs])

    if len(months) < 1:
        raise FileNotFoundError(
            "No files of name 'matches_history_yyyy-mm...' have been "
            "found in 'data/raw/perpetual/kraken'"
        )

    res = list()

    # function to parse one file, possibly a .zip
    @memory.cache
    def parser(f_) -> pd.DataFrame:
        if "csv" in f_:
            if f_ == "matches_history_2020-10_rti.csv":
                # this one is special somehow
                res_ = pd.read_csv(
                    f"{data_src}/{f_}",
                    header=None, usecols=[1, 2, 3, 4, 5]
                )
                res_.columns = ["timestamp", "tradeable", "price", "size",
                                "aggressor"]
            else:
                res_ = pd.read_csv(
                    f"{data_src}/{f_}",
                    usecols=["timestamp", "tradeable", "price",
                             "size", "aggressor"]
                )
        else:
            res_ = pd.read_csv(
                f"{data_src}/{f_}", compression="zip",
                sep="[,\t]",
                usecols=["timestamp", "tradeable", "aggressor", "price",
                         "size"]
            )

        res_ = res_.loc[res_["tradeable"].str.startswith("PI_")]

        return res_

    # take pairs of files, parse, concat, resample
    logger.info("files found, starting iteration over month-pairs...")
    for m1, m2 in zip(months[:-1], months[1:]):

        logger.info(f"{m1} & {m2}")

        f1 = [f_ for f_ in fs if str(m1) in f_][0]
        f2 = [f_ for f_ in fs if str(m2) in f_][0]

        d1, d2 = parser(f1), parser(f2)

        # skip if no PI_ prices are found
        if (len(d1) < 1) & (len(d2) < 1):
            continue

        data_ = pd.concat((d1, d2)).dropna()

        # drop duplicated trades
        data_ = data_ \
            .sort_values(["timestamp", "tradeable", "aggressor", "size"]) \
            .drop_duplicates(subset=["timestamp", "tradeable", "aggressor"],
                             keep="last")

        data_.loc[:, "timestamp"] = \
            data_["timestamp"].map(pd.Timestamp).dt.tz_localize("UTC")

        chunk = aggregate_data(data_, agg_freq="10T", offset_freq="5T",
                               datetime_col="timestamp",
                               objective_col="price", weight_col="size",
                               other_cols=["tradeable", "aggressor"])

        res.append(chunk)

    to_save = pd.concat(res, axis=0, ignore_index=True)
    to_save = to_save.drop_duplicates(
        subset=["aggressor", "timestamp", "tradeable"]
    )

    # rename columns and map values
    to_save.insert(
        0, "side",
        to_save.pop("aggressor").map({"buyer": "ask", "seller": "bid"})
    )
    to_save.insert(
        0, "asset",
        to_save.pop("tradeable").str.replace("PI_", "").str.replace("USD", "")\
            .str.lower()
    )

    path_to_out = f"{data_tgt}/perp-bidask-kraken.ftr"

    to_save.reset_index(drop=True).to_feather(path_to_out)
    logger.info(f"perpetual prices saved to {path_to_out}")


def update_perpetual_from_api() -> None:
    """Update feather with perpetual prices."""
    # fetch old data first
    path_to_ftr = os.path.join(data_dir, "prepared", "perpetual", "kraken")

    if "perp-bidask-kraken.ftr" not in os.listdir(path_to_ftr):
        raise ValueError("make sure 'perp-bidask-kraken.ftr' is in data/perp")

    data_old = pd.read_feather(f"{path_to_ftr}/perp-bidask-kraken.ftr")

    # those are the currencies to fetch data on
    currencies = data_old["asset"].unique()

    # start date is the last date of `data_old`
    start_dt = data_old\
        .pivot(index="timestamp", columns=["asset", "side"], values="price")\
        .last_valid_index()

    # end date is the start of today
    end_dt = pd.Timestamp(datetime.date.today(), tz="UTC")

    data = dict()
    for c_ in currencies:
        data_c = _get_perpetual_from_api(c_, start_dt, end_dt)
        data[c_] = data_c

    data_new = pd.concat(data, axis=0, names=["asset", "index"])\
        .reset_index(level="asset").reset_index(drop=True)

    data_upd = pd.concat((data_old, data_new))\
        .drop_duplicates(subset=["asset", "side", "timestamp"])\
        .reset_index(drop=True)

    # save
    data_upd.to_feather(os.path.join(path_to_ftr, "perp-bidask-kraken.ftr"))

    return


def save_funding_rates() -> None:
    """Save abs and rel funding rates using the API.

    Works via an API call to kraken's website.

    The relative rate is the one displayed at the front end; the absolute
    rate is calculated as (rel rate / price), denominated in the
    cryprocurrency and to be used for accounting purposes.

    columns:
        'timestamp' (pd.Timestamp, tz-aware),
        'which' (str, one of 'relative', 'absolute'),
        'asset' (str, 3-letter iso e.g. 'xrp'),
        'rate' (float)

    """
    logger.info("saving funding rates...")

    endpoint = "historicalfundingrates"

    res = dict()

    for c in ["XBT", "BCH", "LTC", "ETH", "XRP"]:
        logger.info(f"saving funding rates for {c}...")

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

    res = pd.concat(res, axis=1, names=["asset", "which"])\
        .stack(level=[0, 1]).rename("rate")\
        .reset_index()

    res.loc[:, "asset"] = res.loc[:, "asset"].str.lower()

    path_to_out = os.path.join(data_dir, "prepared/funding/kraken",
                               "funding-r-kraken.ftr")
    res.to_feather(path_to_out)
    logger.info(f"funding rates saved to {path_to_out}")


def _get_spot_from_ohlcv(currency) -> pd.DataFrame:
    """Get spot ohlcvt data from .csv files saved from kraken.

    The .csv files can be found on Kraken's Google Drive, link here:
    https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data

    Download a separate file for each currency of interest; files are in
    format '<XXX>USD_TT.csv, where <XXX> is the 3-letter code, such as XBT,
    and TT is the frequency of bars, in minutes. Save the files under
    "../data/raw/spot/kraken" for the functions to access them.

    This function uses 1-min close prices and volumes to aggregate to 10-min
    intervals around 10-minute marks: this way, the value at :20 refers to
    the volume-weighted average price over the :15-:25 interval. This is to
    account for the uncertain execution times on the CEX when running
    backtests.

    Returns
    -------
    pandas.DataFrame
        with columns 'timestamp' (UTC-aware Timestamp), 'price' (float)
    """
    data_src = os.path.join(data_dir, "raw/spot/kraken")

    # detect the .zip file
    z = [
        f for f in os.listdir(data_src)
        if f.endswith("zip") and f.startswith(currency.upper())
    ]

    if len(z) < 1:
        raise FileNotFoundError("Download the .zip with spot data from Kraken "
                                "to data/raw/kraken first!")

    z = z[0]
    zip_fname = f"{data_src}/{z}"
    base_c = z.split("_")[0]
    csv_fname = f"{base_c}USD_1.csv"

    # unzip if not yet
    with zipfile.ZipFile(zip_fname, mode='r') as uz:
        uz.extract(member=csv_fname, path=data_src)

    # read in chunks, columns are timestamp, price, volume
    chunk = pd.read_csv(f"{data_src}/{csv_fname}",
                        header=None,
                        dtype={0: int, 6: int})
    os.remove(f"{data_src}/{csv_fname}")

    # rename cols from ordinal to meaningful, rows to Timestamp
    columns = ["timestamp", "open", "high", "low", "close", "volume", "trades"]
    chunk.columns = columns
    chunk.loc[:, "timestamp"] = chunk["timestamp"]\
        .map(lambda x: pd.Timestamp(x, unit="s", tz="UTC"))

    res = aggregate_data(chunk, agg_freq="10T", offset_freq="5T",
                         datetime_col="timestamp", objective_col="close",
                         weight_col="volume")

    return res


def _process_perpetual_api_call(request_str) -> Tuple[List, pd.Timestamp]:
    """Process result of one call to the perpetual prices API.

    Parameters
    ----------
    request_str : str

    Returns
    -------
    chunk : dict
        of data
    t_final : pd.Timestamp
        tz-agnostic timestamp of the latest data point to use for further calls
    """
    response = requests.get(request_str).json()

    chunk = []
    timestamps = []

    # each element is one execution: collect price, qty etc.
    for e_ in response["elements"]:
        exe = e_["event"]["Execution"]["execution"]
        timestamp = exe["timestamp"]
        price = exe["price"]
        quantity = exe["quantity"]
        side = exe["takerOrder"].get("direction")

        chunk.append({"timestamp": timestamp, "price": price,
                      "side": side, "quantity": quantity})

        timestamps.append(timestamp)

    # final time to start the next iteration
    t_final = pd.Timestamp(max(timestamps), unit="ms")

    return chunk, t_final


@memory.cache
def _get_perpetual_from_api(currency, start_dt, end_dt) -> pd.DataFrame:
    """Get perpetual bid/ask prices using the API.

    Assumes USD as the counter currency.

    Pulls perpetual futures price data in chunks of size 1000, using the
    last timestamp of each call as the new start date until `end_dt` is
    reached. Then aggregates at the 10-min frequency using the same
    methodology as `get_spot_from_ohlcv`.

    Parameters
    ----------
    currency : str
        3-letter ISO such as 'xrp', lowercase
    start_dt : datetime-like
    end_dt : datetime-like

    Returns
    -------
    pandas.DataFrame
        with columns 'timestamp' (tz-aware Timestamp), 'side' (bid/ask),
        'price' (float)
    """
    # pair, e.g. ethusd; 'pi_' means perpetual contracts
    pair = f"pi_{currency}usd"

    # init time count
    t = start_dt.tz_localize(None)
    end_dt = end_dt.tz_localize(None)

    res_raw = []

    while t < end_dt:
        print(f"timestamp: {t}")

        # request string: timestamp on Kraken is in ms and must be integer!
        parameters = f"since={(t.timestamp() * 1000):.0f}&sort=asc"
        request_str = f"{ROOT_URL_PERP.format(pair)}?{parameters}"

        chunk_, t = _process_perpetual_api_call(request_str)

        res_raw += chunk_

    # calculate price as weighted mean by buy/sell
    data_df = pd.DataFrame.from_records(res_raw)
    data_df.loc[:, ["price", "quantity"]] = \
        data_df[["price", "quantity"]].astype(float)
    data_df.loc[:, "timestamp"] = data_df["timestamp"]\
        .map(lambda x: pd.Timestamp(x, unit="ms", tz="UTC"))
    data_df.loc[:, "side"] = data_df["side"].map({"Buy": "ask", "Sell": "bid"})

    # aggregate
    res = aggregate_data(data_df, agg_freq="10T", offset_freq="5T",
                         datetime_col="timestamp", objective_col="price",
                         weight_col="quantity", other_cols=["side"])

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
def _get_spot_from_api(currency, start_dt, end_dt) -> pd.DataFrame:
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
        data[["price", "volume"]].astype(float)
    data.loc[:, "timestamp"] = data["timestamp"] \
        .map(lambda x: pd.Timestamp(x, unit="s", tz="UTC"))
    data.loc[:, "side"] = data["side"].astype("category")

    res = aggregate_data(data, agg_freq="10T", offset_freq="5T",
                         datetime_col="timestamp", objective_col="price",
                         weight_col="volume", other_cols=["side"])

    return res
