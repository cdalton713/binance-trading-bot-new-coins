from pathlib import Path
from logging.config import dictConfig
from typing import Dict, Optional, Any, List, NoReturn, Union, Type
import json
import pickle
from util.config import Config
import requests
from requests import Response
from pydantic import BaseModel
from datetime import date, datetime
from util.types import Sold, Order
from json.decoder import JSONDecodeError

class Util:
    FORMAT = "[%(levelname)s] %(asctime)s: %(message)s."
    DATE_FORMAT = None

    @staticmethod
    def setup_logging(name, level="INFO", fmt=FORMAT):
        formatted = fmt.format(app=name)
        log_dir = Path(__file__).parent.parent.joinpath("logs")
        log_dir.mkdir(exist_ok=True)

        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"standard": {"format": formatted}},
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                    "level": level,
                    "stream": "ext://sys.stdout",
                },
                "error_file": {
                    "class": "logging.handlers.TimedRotatingFileHandler",
                    "when": "midnight",
                    "utc": True,
                    "backupCount": 5,
                    "level": level,
                    "filename": "{}/errors.log".format(log_dir),
                    "formatter": "standard",
                },
                "verbose_file": {
                    "class": "logging.handlers.TimedRotatingFileHandler",
                    "when": "midnight",
                    "utc": True,
                    "backupCount": 5,
                    "level": level,
                    "filename": "{}/verbose_log.log".format(log_dir),
                    "formatter": "standard",
                },
            },
            "loggers": {
                "": {"handlers": ["default"], "level": level},
                "error_log": {"handlers": ["default", "error_file"], "level": 'ERROR'},
                "verbose_log": {
                    "handlers": ["default", "verbose_file"],
                    "level": "DEBUG",
                },
            },
        }

        dictConfig(logging_config)

    @staticmethod
    def load_json(file: Path, model: Type[Union[Order, Sold]]) -> Dict[str, BaseModel]:
        try:
            with open(file.absolute(), "r+") as f:
                dict_ = json.load(f)
        except JSONDecodeError:
            return {}
        else:
            for key, value in dict_.items():
                dict_[key] = model.parse_obj(value)
            return dict_

    @staticmethod
    def dump_json(file: Path, obj: Dict[str, BaseModel]) -> NoReturn:
        dict_ = {}
        for key, value in obj.items():
            dict_[key] = value.dict()

        with open(file.absolute(), "w") as f:
            json.dump(dict_, f, indent=4, default=json_serial)

    @staticmethod
    def percent_change(value: float, percent: float) -> float:
        return (percent / 100 * value) + value

    @staticmethod
    def load_pickle(file: Path):
        with open(file.absolute(), "rb") as f:
            return pickle.load(f)

    @staticmethod
    def dump_pickle(obj: Any, obj_desc: str, directory: Optional[Path] = None) -> NoReturn:
        if directory is None:
            file = Config.TEST_DIR.joinpath(
                f'{obj_desc}{datetime.now().strftime("%Y%m%d%H%M%S")}'
            )
        else:
            file = directory.joinpath(f"{obj_desc}{datetime.now().timestamp()}")
        with open(file.absolute(), "wb") as f:
            pickle.dump(obj, f)

    @staticmethod
    def compare_dicts(d1: Dict, d2: Dict, ignore_keys: List[str]) -> bool:
        return {k: v for k, v in d1.items() if k not in ignore_keys} == {
            k: v for k, v in d2.items() if k not in ignore_keys
        }

    @staticmethod
    def post_pipedream(obj: BaseModel) -> Response:
        resp = requests.post(Config.PIPEDREAM_URL, data=obj.json())
        return resp


def convert_ticker(value: str, to_broker: str, pairing: str) -> str:
    if to_broker == "FTX":
        value = value.split(pairing)[0]

        if "PERP" not in value:
            # ETH / USD
            value = value.split("/")[0].strip() + "-PERP"
        return value
    elif to_broker == "Universal":
        value = value.split("USDT")[0]
        return value
    else:
        return value


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
