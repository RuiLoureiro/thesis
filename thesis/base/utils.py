import datetime
import yaml
from . import config


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, force=False, **kwargs):
        if force:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        try:
            return cls._instances[cls]
        except KeyError:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


def ddict2dict(d):
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = ddict2dict(v)
    return dict(d)


def nested_dict_to_int(d):
    dd = {}
    for k in d:
        dd[int(k)] = {}
        for kk, v in d[k].items():
            dd[int(k)][int(kk)] = v
    return dd


def load_yaml(path):
    f = open(path, "r")
    return yaml.safe_load(f)
