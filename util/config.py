import logging
from pathlib import Path
from typing import NoReturn, Tuple
import sys
import requests
import yaml
import git
import json

try:
    from multiNotification import Notification
except ImportError:
    repo = git.Repo(Path(__file__).parent.parent.joinpath('.git'))
    for submodule in repo.submodules:
        submodule.update(init=True)
    print("NEW UPDATE USES SUBMODULES!! These were just installed. Rerun program.")
    sys.exit(1)
from notification.notification import ALL_NOTIFICATIONS_ON, parse_settings
from notification.notification import CustomNotificationSettings
from util.models import BrokerType, BROKERS

logger = logging.getLogger(__name__)
errLogger = logging.getLogger("error_log")
errLogger.propagate = False

verboseLogger = logging.getLogger("verbose_log")
verboseLogger.propagate = False


class Config:
    # Default global config values
    total_time = 0
    total_iter = 0
    PIPEDREAM_URL = "https://e853670d8092ce2689bf7fe37c7b4830.m.pipedream.net"
    VERSION_URL = "https://raw.githubusercontent.com/cdalton713/trading-bot-new-coins/main/version.json"

    SHARE_DATA = True

    ROOT_DIR = Path(__file__).parent.parent
    AUTH_DIR = ROOT_DIR.joinpath("auth")
    TEST_DIR = ROOT_DIR.joinpath("tests")

    FREQUENCY_SECONDS = 10

    FRONTLOAD_ENABLED: True
    FRONTLOAD_DURATION = 7

    TEST = True
    BINANCE_TESTNET = False

    ENABLED_BROKERS = []

    PROGRAM_OPTIONS = {"LOG_LEVEL": "INFO", "LOG_INFO_UPDATE_INTERVAL": 2}

    NOTIFICATION_SERVICE = Notification()

    # Command line is always required.
    NOTIFICATION_SERVICE.add_logger('CMD', logger, ALL_NOTIFICATIONS_ON)

    NOTIFICATION_SERVICE.add_logger('ERROR_FILE', errLogger,
                                    CustomNotificationSettings(message=False, error=True, warning=False, info=False,
                                                               debug=False, entry=False, close=False))

    NOTIFICATION_SERVICE.add_logger("VERBOSE_FILE", verboseLogger,
                                    CustomNotificationSettings(message=False, error=True, warning=False, info=False,
                                                               debug=False, entry=False, close=False))

    def __init__(self, broker: BrokerType, file: str = None) -> NoReturn:
        # Default config values
        self.ENABLED = False
        self.SUBACCOUNT = None
        self.QUANTITY = 30
        self.QUOTE_TICKER = "USDT"
        self.STOP_LOSS_PERCENT = 20
        self.TAKE_PROFIT_PERCENT = 30
        self.ENABLE_TRAILING_STOP_LOSS = True
        self.TRAILING_STOP_LOSS_PERCENT = 10
        self.TRAILING_STOP_LOSS_ACTIVATION = 35

        self.load_broker_config(broker, file)
        self.CURRENT_VERSION, self.LATEST_VERSION, self.OUTDATED = self.load_version()

    def load_version(self) -> Tuple[int, int, bool]:
        with open(self.ROOT_DIR.joinpath('version.json'), 'r') as f:
            current_versions = json.load(f)
        current_version = int(current_versions['tradingBotNewCoins'])
        current_version_mn = int(current_versions['multiNotification'])

        latest_versions = json.loads(requests.get(self.VERSION_URL).text)
        latest_version = int(latest_versions['tradingBotNewCoins'])
        latest_version_mn = int(latest_versions['multiNotification'])

        if latest_version_mn > current_version_mn:
            repo_ = git.Repo(Path(__file__).parent.parent.joinpath('.git'))
            for submodule_ in repo_.submodules:
                submodule_.update(init=True)
            print("NEW UPDATES FOR SUBMODULES!! These were just installed. Rerun program.")
            sys.exit(1)

        return current_version, latest_version, latest_version > current_version

    @classmethod
    def load_global_config(cls, file: str = None) -> NoReturn:
        with open(
                Config.ROOT_DIR.joinpath("config.yml") if file is None else file
        ) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

            for key, value in config.items():
                if key == "PROGRAM_OPTIONS":
                    setattr(Config, key, value)
                elif key == "TRADE_OPTIONS":
                    for trade_key, trade_option in value.items():
                        if trade_key == "BROKERS":
                            for broker_key, broker_options in trade_option.items():
                                if broker_options["ENABLED"]:
                                    Config.ENABLED_BROKERS.append(broker_key)
                        elif trade_key == 'FRONTLOAD_REQUESTS':
                            for frontload_key, frontload_option in trade_option.items():
                                setattr(Config, frontload_key, frontload_option)
                        else:
                            if not hasattr(Config, trade_key):
                                logger.warning(
                                    "Extra/incorrect broker setting [{}] in [{}]".format(
                                        trade_key, trade_option
                                    )
                                )
                            setattr(Config, trade_key, trade_option)
                elif key == "NOTIFICATION_OPTIONS":
                    for notification_key, notification_option in value.items():
                        if notification_option["ENABLED"]:
                            if notification_key == 'DISCORD':
                                Config.NOTIFICATION_SERVICE.add_discord(
                                    notification_option['NAME'] if 'NAME' in notification_option else 'DISCORD',
                                    notification_option['AUTH']['ENDPOINT'],
                                    parse_settings(notification_option['SETTINGS']))
                            elif notification_key == "TELEGRAM":
                                Config.NOTIFICATION_SERVICE.add_telegram(
                                    notification_option['NAME'] if 'NAME' in notification_option else 'TELEGRAM',
                                    notification_option['AUTH']['ENDPOINT'],
                                    notification_option['AUTH']['CHAT_ID'],
                                    parse_settings(notification_option['SETTINGS']))

    def load_broker_config(self, broker: BrokerType, file: str = None) -> NoReturn:
        with open(
                Config.ROOT_DIR.joinpath("config.yml") if file is None else file
        ) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

            for key, value in config.items():
                if key == "TRADE_OPTIONS":
                    for trade_key, trade_option in value.items():
                        if trade_key == "BROKERS":
                            for broker_key, broker_options in trade_option.items():
                                if broker_key not in BROKERS:
                                    logger.warning(
                                        "Extra/incorrect broker [{}]".format(broker_key)
                                    )
                                elif broker_key == broker:
                                    for (
                                            broker_setting,
                                            broker_value,
                                    ) in broker_options.items():
                                        if not hasattr(self, broker_setting):
                                            logger.warning(
                                                "Extra/incorrect broker setting [{}] in [{}]".format(
                                                    broker_setting, broker_value
                                                )
                                            )
                                        if "PERCENT" in broker_setting:
                                            broker_value = abs(broker_value)
                                            if broker_value < 1:
                                                broker_value = broker_value * 100
                                            if broker_value > 100:
                                                errLogger.error(
                                                    "Invalid value for [{}]".format(
                                                        broker_setting
                                                    )
                                                )
                                        setattr(self, broker_setting, broker_value)

        if self.ENABLE_TRAILING_STOP_LOSS:
            self.TAKE_PROFIT_PERCENT = float("inf")
