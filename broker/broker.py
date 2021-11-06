import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import NoReturn, List, Tuple
from typing import Union, Dict

import binance.exceptions
import math
import requests
import yaml
from binance.client import Client as BinanceClient
from dateutil.parser import parse
from ftx.api import FtxClient

from util import Config, Util
from util.decorators import retry
from util.exceptions import *
from util.models import BrokerType, Ticker, Order

logger = logging.getLogger(__name__)


class Broker(ABC):
    def __init__(self) -> NoReturn:
        self.brokerType = None

    @staticmethod
    def factory(broker: BrokerType, subaccount: Union[str, None] = None) -> any:
        with open(Config.AUTH_DIR.joinpath("auth.yml")) as file:
            auth = yaml.load(file, Loader=yaml.FullLoader)

            if broker == "FTX":
                return FTX(
                    subaccount=subaccount,
                    key=auth["FTX"]["key"],
                    secret=auth["FTX"]["secret"],
                )
            if broker == "BINANCE":
                # TODO - SUBACCOUNTS FOR BINANCE IS NOT IMPLEMENTED YET
                if Config.BINANCE_TESTNET:
                    return Binance(
                        subaccount="",
                        key=auth["BINANCE"]["testnetkey"],
                        secret=auth["BINANCE"]["testnetsecret"],
                        testnet=True,
                    )
                else:
                    return Binance(
                        subaccount="",
                        key=auth["BINANCE"]["key"],
                        secret=auth["BINANCE"]["secret"],
                    )

    @abstractmethod
    def get_tickers(self, quote_ticker: str, **kwargs) -> Tuple[List[Ticker], Dict]:
        """
        Returns all coins from Broker
        """
        raise NotImplementedError

    @abstractmethod
    def get_current_price(self, ticker: Ticker) -> float:
        """
        Get the current price for a coin
        """
        raise NotImplementedError

    @abstractmethod
    def place_order(self, config: Config, *args, **kwargs) -> Order:
        raise NotImplementedError

    @abstractmethod
    def convert_size(self, config: Config, ticker: Ticker, price: float) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_rate_limit(self) -> int:
        raise NotImplementedError

class FTX(FtxClient, Broker):
    def __init__(self, subaccount: str, key: str, secret: str) -> NoReturn:
        self.brokerType = "FTX"

        super().__init__(
            api_key=key,
            api_secret=secret,
            subaccount_name=subaccount,
        )

    @retry(
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            NoBrokerResponseException,
            Exception,
        ),
        2,
        0,
        None,
        1,
        0,
        logger,
    )
    def get_tickers(self, quote_ticker: str, **kwargs) -> Tuple[List[Ticker], Dict]:
        try:
            api_resp = super(FTX, self).get_markets()

            test_retry = kwargs.get("test_retry", False)
            if test_retry:
                raise requests.exceptions.ConnectionError

            resp = []
            for ticker in api_resp:
                if (
                    ticker["type"] == "spot"
                    and ticker["enabled"]
                    and ticker["quoteCurrency"] == quote_ticker
                ):
                    if ticker["type"] == "spot" and ticker["enabled"]:
                        resp.append(
                            Ticker(
                                ticker=ticker["name"],
                                base_ticker=ticker["baseCurrency"],
                                quote_ticker=ticker["quoteCurrency"],
                            )
                        )
            return resp, {}
        except Exception as e:
            if len(e.args) > 0 and "FTX is currently down" in e.args[0]:
                raise BrokerDownException(e.args[0])
            else:
                raise

    @retry(
        (Exception,),
        2,
        3,
        None,
        1,
        0,
        logger,
    )
    @FtxClient.authentication_required
    def get_current_price(self, ticker: Ticker):
        Config.NOTIFICATION_SERVICE.debug(
            "Getting latest price for [{}]".format(ticker.ticker)
        )
        try:
            resp = float(self.get_market(market=ticker.ticker)["last"])

            if resp is None:
                raise GetPriceNoneResponse("None Response from Get Price")
            Config.NOTIFICATION_SERVICE.info(
                "FTX Price - {} {}".format(ticker.ticker, round(resp, 4))
            )
            return resp
        except LookupError as e:
            pass

    # @retry(
    #     (
    #             Exception,
    #     ),
    #     2,
    #     3,
    #     None,
    #     1,
    #     0,
    #     logger,
    # )
    @FtxClient.authentication_required
    def place_order(self, config: Config, *args, **kwargs) -> Order:
        if Config.TEST:
            price = kwargs.get(
                "current_price", self.get_current_price(kwargs["ticker"])
            )
            return Order(
                broker="FTX",
                ticker=kwargs["ticker"],
                purchase_datetime=datetime.now(),
                price=price,
                side=kwargs["side"],
                size=kwargs["size"],
                type="market",
                status="TEST_MODE",
                take_profit=Util.percent_change(price, config.TAKE_PROFIT_PERCENT),
                stop_loss=Util.percent_change(price, -config.STOP_LOSS_PERCENT),
                trailing_stop_loss_activated=False,
                trailing_stop_loss_max=Util.percent_change(
                    price, config.TRAILING_STOP_LOSS_ACTIVATION
                ),
                trailing_stop_loss=Util.percent_change(
                    price, -config.TRAILING_STOP_LOSS_PERCENT
                ),
            )

        else:
            kwargs["market"] = kwargs["ticker"]
            del kwargs["ticker"]
            api_resp = super(FTX, self).place_order(*args, *kwargs)
            return Order(
                broker="FTX",
                ticker=kwargs["ticker"],
                purchase_datetime=parse(api_resp["createdAt"]),
                price=api_resp["price"],
                side=api_resp["side"],
                size=api_resp["size"],
                type="market",
                status="LIVE",
                take_profit=Util.percent_change(
                    api_resp["price"], config.TAKE_PROFIT_PERCENT
                ),
                stop_loss=Util.percent_change(
                    api_resp["price"], -config.STOP_LOSS_PERCENT
                ),
                trailing_stop_loss_activated=False,
                trailing_stop_loss_max=Util.percent_change(
                    api_resp["price"], config.TRAILING_STOP_LOSS_ACTIVATION
                ),
                trailing_stop_loss=Util.percent_change(
                    api_resp["price"], -config.TRAILING_STOP_LOSS_PERCENT
                ),
            )

    def convert_size(self, config: Config, ticker: Ticker, price: float) -> float:
        size = config.QUANTITY / price
        return size

    def get_rate_limit(self) -> int:
        return 1000


class Binance(BinanceClient, Broker):
    def __init__(
        self, subaccount: str, key: str, secret: str, testnet: bool = False
    ) -> NoReturn:
        self.brokerType = "BINANCE"

        super().__init__(api_key=key, api_secret=secret, testnet=testnet)

    @retry(
        (
            binance.exceptions.BinanceAPIException,
            Exception,
        ),
        2,
        3,
        None,
        1,
        0,
        logger,
    )
    def get_current_price(self, ticker: Ticker) -> float:
        Config.NOTIFICATION_SERVICE.debug(
            "Getting latest price for [{}]".format(ticker)
        )
        return float(self.get_symbol_ticker(symbol=ticker.ticker)["price"])

    # @retry(
    #     (
    #             binance.exceptions.BinanceAPIException,
    #             Exception,
    #     ),
    #     2,
    #     3,
    #     None,
    #     1,
    #     0,
    #     logger,
    # )
    def place_order(self, config: Config, *args, **kwargs) -> Order:
        kwargs["symbol"] = kwargs["ticker"].ticker
        kwargs["type"] = "market"
        kwargs["side"] = kwargs["side"].upper()

        params = {}
        if kwargs["side"] == "BUY":
            kwargs["quoteOrderQty"] = float(config.QUANTITY)
            for p in ["quoteOrderQty", "side", "symbol", "type"]:
                params[p] = kwargs[p]
        else:
            kwargs["quantity"] = kwargs["size"]

            # Check lot size requirements
            symbol_info = self.get_symbol_info(kwargs["symbol"])
            lot_size = symbol_info["filters"][2]

            if kwargs["quantity"] <= float(lot_size["minQty"]):
                raise TradingBotException(
                    """The remaining quantity available to sell is too low.  Binance requires ~$10.30 USDT worth of 
                    coin per trade.  If the coin decreased in value below this it cannot be sold through this app.  
                    Sell as dust on Binance.com."""
                )
            if kwargs["quantity"] >= float(lot_size["maxQty"]):
                raise TradingBotException(
                    """The remaining quantity is too high.  This is probably in error, 
                send logs to GitHub repo."""
                )

            step_size = float(lot_size["stepSize"])
            precision = int(round(-math.log(step_size, 10), 0))

            kwargs["quantity"] = round(kwargs["quantity"] * (1 if config.USE_BNB_FOR_FEES else 0.9995), precision)

            for p in ["quantity", "side", "symbol", "type"]:
                params[p] = kwargs[p]

        if Config.TEST:
            # does not return anything.  No error mean request was good.
            api_resp = super(Binance, self).create_test_order(**params)
            price = self.get_current_price(kwargs["ticker"])

            return Order(
                broker="BINANCE",
                ticker=kwargs["ticker"],
                purchase_datetime=datetime.now(),
                price=price,
                side=kwargs["side"],
                size=config.QUANTITY / price,
                type="market",
                status="TEST_MODE",
                take_profit=Util.percent_change(price, config.TAKE_PROFIT_PERCENT),
                stop_loss=Util.percent_change(price, -config.STOP_LOSS_PERCENT),
                trailing_stop_loss_activated=False,
                trailing_stop_loss_max=Util.percent_change(
                    price, config.TRAILING_STOP_LOSS_ACTIVATION
                ),
                trailing_stop_loss=Util.percent_change(
                    price, -config.TRAILING_STOP_LOSS_PERCENT
                ),
            )
        else:
            api_resp = super(Binance, self).create_order(**params)
            Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(api_resp)
            fill_sum = 0
            fill_count = 0

            for fill in api_resp["fills"]:
                fill_sum += (float(fill["price"]) - float(fill["commission"])) * float(
                    fill["qty"]
                )
                fill_count += float(fill["qty"])

            avg_fill_price = float(fill_sum / fill_count)

            return Order(
                broker="BINANCE",
                ticker=kwargs["ticker"],
                purchase_datetime=datetime.now(),
                price=avg_fill_price,
                side=api_resp["side"],
                size=api_resp["executedQty"],
                type="market",
                status="TESTNET"
                if Config.BINANCE_TESTNET
                else "TEST_MODE"
                if Config.TEST
                else "LIVE",
                take_profit=Util.percent_change(
                    avg_fill_price, config.TAKE_PROFIT_PERCENT
                ),
                stop_loss=Util.percent_change(
                    avg_fill_price, -config.STOP_LOSS_PERCENT
                ),
                trailing_stop_loss_activated=False,
                trailing_stop_loss_max=Util.percent_change(
                    avg_fill_price, config.TRAILING_STOP_LOSS_ACTIVATION
                ),
                trailing_stop_loss=Util.percent_change(
                    avg_fill_price, -config.TRAILING_STOP_LOSS_PERCENT
                ),
            )

    @retry(
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            NoBrokerResponseException,
            Exception,
        ),
        2,
        0,
        None,
        1,
        0,
        logger,
    )
    def get_tickers(self, quote_ticker: str, **kwargs) -> Tuple[List[Ticker], Dict]:
        api_resp = super(Binance, self).get_exchange_info()

        test_retry = kwargs.get("test_retry", False)
        if test_retry:
            raise requests.exceptions.ConnectionError

        resp = []
        for ticker in api_resp["symbols"]:
            if ticker["isSpotTradingAllowed"] and ticker["quoteAsset"] == quote_ticker:
                resp.append(
                    Ticker(
                        ticker=ticker["symbol"],
                        base_ticker=ticker["baseAsset"],
                        quote_ticker=ticker["quoteAsset"],
                    )
                )

        return resp, self.response.headers

    def get_rate_limit(self) -> int:
        api_resp = super(Binance, self).get_exchange_info()
        return api_resp['rateLimits'][0]['limit']

    def convert_size(self, config: Config, ticker: Ticker, price: float) -> float:

        info = super(Binance, self).get_symbol_info(symbol=ticker.ticker)
        step_size = info["filters"][2]["stepSize"]
        lot_size = step_size.index("1") - 1
        lot_size = max(lot_size, 0)

        # calculate the volume in coin from QUANTITY in USDT (default)
        size = config.QUANTITY / price

        # if lot size has 0 decimal points, make the volume an integer
        if lot_size == 0:
            size = int(size)
        else:
            size = float("{:.{}f}".format(size, lot_size))

        return size
