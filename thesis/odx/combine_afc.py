import datetime
import pandas as pd
from . import config


def load_afc(mode, path):
    df = pd.read_feather(path)
    df["mode"] = mode
    return df


def filter_df(
    df,
    start_date=None,
    end_date=None,
    start_time=None,
    end_time=None,
):
    if start_date and end_date:
        if start_time and end_time:
            df = df[
                (
                    df.timestamp
                    >= datetime.datetime.combine(start_date, start_time)
                )
                & (
                    df.timestamp
                    <= datetime.datetime.combine(end_date, end_time)
                )
            ]
        else:
            df = df[
                (df.timestamp.dt.date >= start_date)
                & (df.timestamp.dt.date <= end_date)
            ]
    df = df.reset_index().drop("index", axis=1)
    return df


def get_combined_afc(
    afc_sources={
        "bus": config.PROCESSED_BUS_AFC_PATH,
        "metro": config.PROCESSED_METRO_AFC_PATH,
    },
    start_date=datetime.date(2019, 10, 7),
    end_date=datetime.date(2019, 10, 9),
    start_time=None,
    end_time=None
    # start_time=datetime.time(4,0,0),
    # end_time = datetime.time(3,59,59)
):

    dfs = []
    for mode, path in afc_sources.items():
        print(f"Loading {mode} AFC from {path}")
        dfs.append(load_afc(mode, path))

    print(f"Concating and sorting {len(dfs)} AFC dataframes..")
    afc = pd.concat(dfs, ignore_index=True)
    afc = afc.sort_values(by="timestamp", ignore_index=True)

    print(f"Filtering AFC for rows between {start_date} and {end_date}")
    subset_afc = filter_df(afc, start_date, end_date, start_time, end_time)
    return subset_afc
