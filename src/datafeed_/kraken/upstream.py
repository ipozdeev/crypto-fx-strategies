import pandas as pd
import datetime
import os
import time
import requests

from .setup import ROOT_URL_SPOT

data_dir = os.path.join(os.environ.get("PROJECT_ROOT"), "data/")


def save_spot(currency):
    """
    """
    cols = ["price", "volume", "timestamp", "side"]
    data_path = "../../../data/spot"
    endpoint = "Trades"

    # res = [
    #     pd.read_feather(f"{data_path}/spot-{currency}-kraken.ftr")
    # ]
    res = []
    c_res = list()
    # t = res[-1]["timestamp"].max().timestamp()
    t = datetime.datetime(2019, 1, 1).timestamp()
    t_last = int(datetime.datetime(2021, 7, 1).timestamp())

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

        if (len(c_res) > 1000) | (int(t) > t_last):
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

            pd.concat(res, ignore_index=True).reset_index(drop=True)\
                .to_feather(f"{data_path}/spot-{currency}-kraken.ftr")

            c_res = list()

        if int(t) > t_last:
            break

        t = chunk[2].iloc[-1]

        time.sleep(1.9)

    pd.concat(res, ignore_index=True).reset_index(drop=True)\
        .to_feather(f"{data_path}/spot-{currency}-kraken.ftr")


def save_perpetual() -> None:
    """Process .csv files with perp prices downloadable from kraken.

    The zip files are downloadable from 'matches_history' folder within the
    Dropbox folder to be found here:
    https://support.kraken.com/hc/en-us/articles/360022835871-Historical-Data

    Download all of them, saving to ../data/perp/
    """
    data_path = os.path.join(data_dir, "perp")
    fs = [f for f in os.listdir(data_path) if f.endswith(("csv", "zip"))]
    months = pd.period_range("2018-01", "2021-02", freq="M")

    data = list()

    def parser(f_):
        if "csv" in f_:
            if f_ == "matches_history_2020-10_rti.csv":
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

    for m1, m2 in zip(months[:-1], months[1:]):

        print(m2)

        f1 = [f_ for f_ in fs if str(m1) in f_][0]
        f2 = [f_ for f_ in fs if str(m2) in f_][0]

        d1, d2 = parser(f1), parser(f2)

        if (len(d1) < 1) | (len(d2) < 1):
            continue

        data_ = pd.concat((d1, d2)).dropna()

        data_ = data_ \
            .sort_values(["timestamp", "tradeable", "aggressor", "size"]) \
            .drop_duplicates(subset=["timestamp", "tradeable", "aggressor"],
                             keep="last") \
            .pivot(index="timestamp",
                   columns=["tradeable", "aggressor"],
                   values="price")
        data_.index = pd.to_datetime(data_.index, utc=True)
        data_ = data_.rename(columns=lambda x: x[3:-3].lower(), level=0)

        # nearest price
        mid = data_\
            .interpolate(method="nearest") \
            .resample("1H", label="right").last() \
            .mean(axis=1, level="tradeable") \
            .reset_index() \
            .melt(id_vars=["timestamp"], var_name="asset", value_name="price")
        mid.insert(3, "side", "mid")

        # bid, ask
        bidask = data_.copy()
        bidask.index -= pd.to_timedelta("15T")
        bidask = bidask.resample("30T", label="right").quantile([0.25, 0.75])
        bidask = pd.concat(
            (bidask.xs(0.25, axis=0, level=1).xs("seller", axis=1, level=1),
             bidask.xs(0.75, axis=0, level=1).xs("buyer", axis=1, level=1)),
            axis=1, keys=["bid", "ask"], names=["side", "asset"]
        )
        bidask = bidask \
            .resample("1H").last() \
            .melt(ignore_index=False, value_name="price").reset_index()

        chunk = pd.concat((mid, bidask), ignore_index=True).dropna().iloc[:-2]

        data.append(chunk)

    to_save = pd.concat(data, axis=0, ignore_index=True)\
        .drop_duplicates(subset=["timestamp", "asset", "side"],
                         keep="first")

    # # some dupls
    # d1 = to_save.loc[to_save.duplicated(subset=["timestamp", "asset"],
    #                                     keep="first")]
    # d2 = to_save.loc[to_save.duplicated(subset=["timestamp", "asset"],
    #                                     keep="last")]
    # d = d2.set_index(["timestamp", "asset"])\
    #     .fillna(d1.set_index(["timestamp", "asset"])).reset_index()
    #
    # to_save = pd.concat(
    #     (to_save.drop_duplicates(subset=["timestamp", "asset"], keep=False),
    #      d)
    # )

    to_save.reset_index(drop=True) \
        .to_feather(f"{data_path}/perp-mba-1h-kraken.ftr")
