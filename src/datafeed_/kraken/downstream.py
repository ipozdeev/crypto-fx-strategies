import pandas as pd
import os

data_dir = os.path.join(os.environ.get("PROJECT_ROOT"), "data/")


def get_perpetual(mid=False) -> pd.DataFrame:
    """Get prices of perpetual contracts, in USD.

    Kraken perps are coin-margined.

    Parameters
    ----------
    mid : bool
        True to retrieve mid quotes

    """
    data_path = os.path.join(data_dir, "prepared/perpetual/kraken")
    data = pd.read_feather(
        os.path.join(data_path, "perp-bidask-kraken.ftr")
    )

    res = data \
        .pivot(index="timestamp", columns=["asset", "side"], values="price")

    if mid:
        ba = (res.xs("ask", 1, 1) - res.xs("bid", 1, 1))\
            .rolling(6 * 24, min_periods=6).mean() / 2
        res = res.xs("ask", 1, 1).sub(ba)\
            .fillna(res.xs("bid", 1, 1).add(ba))

    return res


def get_spot(which="close"):
    """Get spot prices."""
    data_path = os.path.join(data_dir, "prepared/spot/kraken")

    if which == "close":
        res = pd.read_feather(
            os.path.join(data_path, "spot-close-kraken.ftr")
        )
    else:
        raise NotImplementedError

    res = res.pivot(index="timestamp", columns="asset", values=which)

    return res


def get_funding_rates() -> pd.DataFrame:
    """Get abs and rel funding rates.

    with time zone-aware index, containing absolute (in fractions of 1)
    and relative funding rates, per hour

    columns:
        'timestamp' (pd.Timestamp, tz-aware),
        'which' (str, one of 'relative', 'absolute'),
        'asset' (str, 3-letter iso e.g. 'xrp'),
        'rate' (float)
    """
    data_path = os.path.join(data_dir, "prepared/funding/kraken")
    res = pd.read_feather(os.path.join(data_path, "funding-r-kraken.ftr"))

    return res