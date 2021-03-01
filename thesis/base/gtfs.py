import functools
import pandas as pd
from loguru import logger
from pathlib import Path


class RawGTFSReaderMeta(type):
    @functools.lru_cache(maxsize=10, typed=False)
    def __call__(cls, *args, **kwargs):
        return super(RawGTFSReaderMeta, cls).__call__(*args, **kwargs)


class RawGTFSReader(metaclass=RawGTFSReaderMeta):
    def __init__(self, gtfs_path):
        self.path = Path(gtfs_path)

        if not self.path.exists():
            raise RuntimeError(f"No such folder: {self.path}")
        logger.info(f"Initialized RawGTFSReader for path {gtfs_path}")

    def __getattr__(self, attr):
        val = self.file_to_df(attr)
        setattr(self, attr, val)
        return val

    def file_to_df(self, attr):
        file_ = self.path / f"{attr}.txt"

        if not file_.exists():
            raise RuntimeError(f"No such file {attr}")

        return pd.read_csv(file_, parse_dates=True)
