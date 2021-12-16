import pandas as pd


def aggregate_data(data, agg_freq: str, offset_freq: str, datetime_col: str,
                   objective_col: str, weight_col: str,
                   other_cols: list = None):
    """Aggregate data at a frequency, with offset and weighting."""
    to_agg = pd.concat((
        data[datetime_col].sub(pd.to_timedelta(offset_freq)),
        data[[objective_col, weight_col]],
        data[objective_col].mul(data[weight_col]).rename("_aggw"),
    ), axis=1)

    # calculate size-weighted mean price in those windows
    group_list = [pd.Grouper(freq=agg_freq, label="right"), ]

    if other_cols is not None:
        group_list += other_cols
        to_agg = pd.concat((to_agg, data[other_cols]), axis=1)

    data_agg = to_agg\
        .set_index("timestamp")\
        .groupby(group_list)\
        .sum()

    res = data_agg["_aggw"] / data_agg[weight_col]

    # rename, reindex
    res = res.rename(objective_col).reset_index()

    return res
