import re
import pandas as pd
import datetime
import os
import time
import requests
from joblib import Memory

from .setup import ROOT_URL_SPOT

from .downstream import get_perpetual_from_api, get_spot_from_api

data_dir = os.path.join(os.environ.get("PROJECT_ROOT"), "data/")
memory = Memory(
    cachedir=os.path.join(os.environ.get("PROJECT_ROOT"), "data")
)


def save_spot() -> None:
    """Save spot prices of 5 cryptocurrencies from 2018-06."""
    start_dt = pd.Timestamp("2018-06-01")
    end_dt = pd.Timestamp(datetime.date.today())

    data = {}

    for c_ in ["xrp", "eth", "xbt", "bch", "ltc"]:

        data_c = get_spot_from_api(c_, start_dt, end_dt)

        data[c_] = data_c

    data = pd.concat(data, axis=0, names=["asset", "index"]) \
        .reset_index(level="asset").reset_index(drop=True)

    # save
    data_path = os.path.join(os.environ.get("PROJECT_ROOT"), "data", "spot")
    data.to_feather(os.path.join(data_path, "spot-mba-kraken.ftr"))


def update_spot() -> None:
    """Update spot prices of 5 cryptocurrencies."""
    data_path = os.path.join(os.environ.get("PROJECT_ROOT"), "data", "spot")
    data_old = pd.read_feather(f"{data_path}/spot-mba-kraken.ftr")

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
        data_c = get_perpetual_from_api(c_, start_dt, end_dt)
        data[c_] = data_c

    data_new = pd.concat(data, axis=0, names=["asset", "index"]) \
        .reset_index(level="asset").reset_index(drop=True)

    data_upd = pd.concat((data_old, data_new)) \
        .drop_duplicates(subset=["asset", "side", "timestamp"]) \
        .reset_index(drop=True)


def save_spot_(currency, start_dt, end_dt):
    """
    """
    # everything better be timestamps
    start_dt = start_dt.timestamp()
    end_dt = end_dt.timestamp()

    # parameters
    cols = ["price", "volume", "timestamp", "side"]
    data_path = os.path.join(data_dir, "spot")
    endpoint = "Trades"

    res = []  # whole dataset
    c_res = list()  # chunks of 1000000 rows

    t = start_dt

    while True:
        print(f"timestamp: {datetime.datetime.fromtimestamp(t)}")
        # request
        parameters = f"pair={currency}usd&since={t}"
        u = f"{ROOT_URL_SPOT}/{endpoint}?{parameters}"
        resp = requests.get(u)

        # convert to DataFrame
        k = [k_ for k_ in resp.json()["result"].keys() if k_ != "last"][0]
        chunk = pd.DataFrame.from_records(resp.json()["result"][k]) \
            .iloc[:, :4]
        c_res.append(chunk)

        if (len(c_res) > 999) | (t > end_dt):
            res_interm = pd.concat(c_res)
            res_interm.columns = cols
            res_interm = res_interm\
                .sort_values(by=["timestamp", "side", "volume"])\
                .drop_duplicates(subset=["timestamp", "side"], keep="last")\
                .pivot(index="timestamp", columns="side", values="price")\
                .astype(float)
            res_interm.index = res_interm.index\
                .map(datetime.datetime.fromtimestamp)

            # nearest price
            mid = res_interm \
                .interpolate(method="nearest") \
                .resample("1H", label="right").last() \
                .mean(axis=1) \
                .to_frame("mid")

            # bid, ask
            bidask = res_interm.copy()
            bidask.index -= pd.to_timedelta("15T")
            bidask = bidask.resample("30T", label="right").quantile(
                [0.25, 0.75])
            bidask = pd.concat(
                (bidask.xs(0.25, axis=0, level=1).loc[:, "s"],
                 bidask.xs(0.75, axis=0, level=1).loc[:, "b"]),
                axis=1, keys=["bid", "ask"]
            ).resample("1H").last()

            mba = pd.concat((mid, bidask), axis=1).reset_index()

            res.append(mba)

            # cache
            to_cache = pd.concat(res, ignore_index=True).reset_index(drop=True)
            to_cache.to_feather(
                f"{data_path}/spot-{currency}-kraken-cached.ftr"
            )

            c_res = list()

        if int(t) > end_dt:
            break

        t = chunk.pivot(index=2, columns=3)[0].last_valid_index()

        time.sleep(2)

    pd.concat(res, ignore_index=True).reset_index(drop=True)\
        .to_feather(f"{data_path}/spot-{currency}-kraken-cached.ftr")


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
    data_path = os.path.join(data_dir, "perp")

    # find all .csv (sometimes compressed as .zip)
    fs = [f for f in os.listdir(data_path) if f.endswith(("csv", "zip"))]

    # process files in pairs
    months = sorted([re.search("[0-9]{4}-[0-9]{2}", f_).group() for f_ in fs])

    data = list()

    # function to parse one file, possibly a .zip
    @memory.cache
    def parser(f_) -> pd.DataFrame:
        if "csv" in f_:
            if f_ == "matches_history_2020-10_rti.csv":
                # this one is special somehow
                res_ = pd.read_csv(
                    f"{data_path}/{f_}",
                    header=None, usecols=[1, 2, 3, 4, 5]
                )
                res_.columns = ["timestamp", "tradeable", "price", "size",
                                "aggressor"]
            else:
                res_ = pd.read_csv(
                    f"{data_path}/{f_}",
                    usecols=["timestamp", "tradeable", "price",
                             "size", "aggressor"]
                )
        else:
            res_ = pd.read_csv(
                f"{data_path}/{f_}", compression="zip",
                sep="[,\t]",
                usecols=["timestamp", "tradeable", "aggressor", "price",
                         "size"]
            )

        res_ = res_.loc[res_["tradeable"].str.startswith("PI_")]

        return res_

    # take pairs of files, parse, concat, resample
    for m1, m2 in zip(months[:-1], months[1:]):

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

        # construct 10-min windows (centered on :00, :10 etc.)
        data_.loc[:, "timestamp"] = pd.to_datetime(data_.loc[:, "timestamp"],
                                                   utc=True)
        data_.loc[:, "timestamp"] -= pd.to_timedelta("5T")

        # calculate size-weighted mean price in those windows
        data_.insert(data_.shape[-1], "pq", data_["price"]*data_["size"])
        data_agg = data_.set_index("timestamp").groupby(
            [pd.Grouper(freq="10T", label="right"), "tradeable", "aggressor"],
        ).sum()
        price_ = data_agg["pq"] / data_agg["size"]
        chunk = price_.rename("price").reset_index()

        data.append(chunk)

    to_save = pd.concat(data, axis=0, ignore_index=True)
    to_save = to_save.drop_duplicates(
        subset=["aggressor", "timestamp", "tradeable"]
    )
    to_save.insert(
        0, "side",
        to_save.pop("aggressor").map({"buyer": "ask", "seller": "bid"})
    )
    to_save.insert(
        0, "asset",
        to_save.pop("tradeable").str.replace("PI_", "").str.replace("USD", "")\
            .str.lower()
    )

    to_save.reset_index(drop=True) \
        .to_feather(f"{data_path}/perp-mba-kraken.ftr")


def update_perpetual() -> None:
    """Update feather with perpetual prices."""
    # fetch old data first
    data_path = os.path.join(os.environ.get("PROJECT_ROOT"), "data", "perp")

    if "perp-mba-kraken.ftr" not in os.listdir(data_path):
        raise ValueError("make sure 'perp-mba-kraken.ftr' is in data/perp")

    data_old = pd.read_feather(f"{data_path}/perp-mba-kraken.ftr")

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
        data_c = get_perpetual_from_api(c_, start_dt, end_dt)
        data[c_] = data_c

    data_new = pd.concat(data, axis=0, names=["asset", "index"])\
        .reset_index(level="asset").reset_index(drop=True)

    data_upd = pd.concat((data_old, data_new))\
        .drop_duplicates(subset=["asset", "side", "timestamp"])\
        .reset_index(drop=True)

    # save
    data_upd.to_feather(os.path.join(data_path, "perp-mba-kraken.ftr"))

    return
