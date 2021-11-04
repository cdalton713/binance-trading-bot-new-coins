import logging
import os
import unittest.mock as mock
from typing import Dict
from unittest import TestCase
from datetime import datetime
from binance.exceptions import BinanceAPIException

import util.models
from bot import Bot
from util import Config
from util import Util
from util.models import Ticker

# setup logging
Util.setup_logging(name="new-coin-bot", level="DEBUG")

logger = logging.getLogger(__name__)


class TestBot(TestCase):
    def setUp(self) -> None:
        # Config.load_global_config()

        Config.TEST = True
        Config.BINANCE_TESTNET = True
        self.FTX = Bot("FTX")
        self.Binance = Bot("BINANCE")
        self.maxDiff = None

        self.FTX.config.STOP_LOSS_PERCENT = 3
        self.FTX.config.TAKE_PROFIT_PERCENT = 3
        self.FTX.config.TRAILING_STOP_LOSS_PERCENT = 2
        self.FTX.config.TRAILING_STOP_LOSS_PERCENT = 2

        self.Binance.config.STOP_LOSS_PERCENT = 3
        self.Binance.config.TAKE_PROFIT_PERCENT = 3
        self.Binance.config.TRAILING_STOP_LOSS_PERCENT = 2
        self.Binance.config.TRAILING_STOP_LOSS_PERCENT = 2

    # GENERAL TEST
    def test_get_new_tickers(self):
        expected = len(self.FTX.ticker_seen_dict)
        self.FTX.ticker_seen_dict = {"BTC/USDT": True}
        actual = self.FTX.get_new_tickers()
        self.assertEqual(len(actual), expected - 1)

        expected = len(self.Binance.ticker_seen_dict)
        self.Binance.ticker_seen_dict = {"BTCUSDT": True}
        actual = self.Binance.get_new_tickers()
        self.assertEqual(len(actual), expected - 1)

    def test_purchase_invalid_symbol(self):
        tickers, ticker_dict = self.Binance.get_starting_tickers()
        self.Binance.all_tickers = [t for t in tickers if t.ticker != "BTCUSDT"]
        ticker_dict.pop("BTCUSDT")
        self.Binance.ticker_seen_dict = ticker_dict
        new_tickers = self.Binance.get_new_tickers()

        try:
            new_tickers[0].ticker = 'INVALIDUSDT'
            for new_ticker in new_tickers:
                self.Binance.process_new_ticker(new_ticker)
        except BinanceAPIException:
            self.assertEqual(True, True)

    def test_convert_size(self):
        ticker = Ticker(ticker="BTC/USDT", base_ticker="BTC", quote_ticker="USDT")

        actual = self.FTX.broker.convert_size(
            config=self.FTX.config, ticker=ticker, price=40000
        )
        self.assertEqual(actual, 0.00075)

        actual = self.FTX.broker.convert_size(
            config=self.FTX.config, ticker=ticker, price=0.008675309
        )
        self.assertEqual(actual, 3458.0900807106696)

        ticker = Ticker(ticker="BTCUSDT", base_ticker="BTC", quote_ticker="USDT")
        actual = self.Binance.broker.convert_size(
            self.Binance.config, ticker=ticker, price=48672.73020676
        )
        self.assertEqual(actual, 0.00062)

    # BINANCE TESTS
    def test_binance_process_new_ticker(self):
        self.Binance.ticker_seen_dict = {}
        ticker = Ticker(ticker="BTCUSDT", base_ticker="BTC", quote_ticker="USDT")
        self.Binance.process_new_ticker(ticker)

    def test_binance_purchase_btc_usdt(self):
        try:
            os.remove(Config.ROOT_DIR.joinpath('BINANCE_open_orders.json'))
        except FileNotFoundError:
            pass

        Config.TEST = False
        tickers, ticker_dict = self.Binance.get_starting_tickers()
        self.Binance.all_tickers = [t for t in tickers if t.ticker != "BTCUSDT"]
        ticker_dict.pop("BTCUSDT")
        self.Binance.ticker_seen_dict = ticker_dict
        new_tickers = self.Binance.get_new_tickers()

        for new_ticker in new_tickers:
            self.Binance.process_new_ticker(new_ticker)

        self.assertEqual(self.Binance.open_orders['BTCUSDT'].dict(), {"broker": "BINANCE",
                                                                      "ticker": {"ticker": "BTCUSDT",
                                                                                 "base_ticker": "BTC",
                                                                                 "quote_ticker": "USDT"},
                                                                      "purchase_datetime": mock.ANY,
                                                                      "price": mock.ANY, "side": "BUY",
                                                                      "size": mock.ANY,
                                                                      "type": "market", "status": "TESTNET",
                                                                      "take_profit": mock.ANY,
                                                                      "stop_loss": mock.ANY,
                                                                      "trailing_stop_loss_max": float('-inf'),
                                                                      "trailing_stop_loss": mock.ANY}
                         )
        self.assertTrue(
            self.Binance.config.QUANTITY - self.Binance.open_orders['BTCUSDT'].price * self.Binance.open_orders[
                'BTCUSDT'].size < 0.50)

        self.Binance.save()

    def test_binance_update_below_sl(self):
        self.test_binance_purchase_btc_usdt()

        for key, value in self.Binance.open_orders.items():
            self.Binance.update(key, value, current_price=self.Binance.open_orders['BTCUSDT'].price - 5000)
            self.assertDictEqual(self.Binance.sold['BTCUSDT'].dict(), {'broker': 'BINANCE',
                                                                       'ticker': {'ticker': 'BTCUSDT',
                                                                                  'base_ticker': 'BTC',
                                                                                  'quote_ticker': 'USDT'},
                                                                       'purchase_datetime': mock.ANY,
                                                                       'price': mock.ANY, 'side': 'SELL',
                                                                       'size': mock.ANY, 'type': 'market',
                                                                       'status': 'TESTNET', 'take_profit': mock.ANY,
                                                                       'stop_loss': mock.ANY,
                                                                       'trailing_stop_loss_max': float('-inf'),
                                                                       'trailing_stop_loss': mock.ANY,
                                                                       'profit': mock.ANY,
                                                                       'profit_percent': mock.ANY,
                                                                       'sold_datetime': mock.ANY})

        # remove pending removals
        [self.Binance.open_orders.pop(o) for o in self.Binance._pending_remove]
        self.Binance._pending_remove = []
        self.Binance.save()

    # FTX TESTS
    def test_ftx_process_new_ticker(self):
        self.FTX.ticker_seen_dict = {}
        ticker = Ticker(ticker="BTC/USDT", base_ticker="BTC", quote_ticker="USDT")
        self.FTX.process_new_ticker(ticker)

    def test_ftx_purchase(self):
        Config.TEST = True
        tickers, ticker_dict = self.FTX.get_starting_tickers()
        self.FTX.all_tickers = [t for t in tickers if t.ticker != "BTC/USDT"]
        ticker_dict.pop("BTC/USDT")
        self.FTX.ticker_seen_dict = ticker_dict
        new_tickers = self.FTX.get_new_tickers()

        for new_ticker in new_tickers:
            self.FTX.process_new_ticker(new_ticker)

        self.assertTrue("BTC/USDT" in self.FTX.open_orders)

    def test_ftx_update_below_sl(self):
        self.FTX.open_orders = Util.load_json(Config.TEST_DIR.joinpath("FTX_order_test.json"),
                                              util.models.Order)

        for key, value in self.FTX.open_orders.items():
            self.FTX.update(key, value, current_price=30000)

            expected = Util.load_json(
                Config.TEST_DIR.joinpath("FTX_order_test_update_below_sl_expected.json"), util.models.Sold
            )
            expected["BTC/USDT"].sold_datetime = self.FTX.sold["BTC/USDT"].sold_datetime
            self.assertDictEqual(expected, self.FTX.sold)

    def test_ftx_update_above_max(self):
        self.FTX.open_orders = Util.load_json(Config.TEST_DIR.joinpath("FTX_order_test.json"),
                                              util.models.Order)

        self.FTX.config.TRAILING_STOP_LOSS_PERCENT = 2

        for key, value in self.FTX.open_orders.items():
            self.FTX.update(key, value, current_price=60000)

            expected = Util.load_json(Config.TEST_DIR.joinpath("FTX_order_test_update_above_max_expected.json"),
                                      util.models.Order)
            self.assertDictEqual(expected, self.FTX.open_orders)

    def test_ftx_update_above_tp(self):
        self.FTX.config.ENABLE_TRAILING_STOP_LOSS = False

        self.FTX.open_orders = Util.load_json(
            Config.TEST_DIR.joinpath("FTX_order_test_tsl_off.json"), util.models.Order
        )

        for key, value in self.FTX.open_orders.items():
            self.FTX.update(key, value, current_price=60000)

            expected: Dict[str, util.models.Sold] = Util.load_json(
                Config.TEST_DIR.joinpath("FTX_order_test_update_above_tp_expected.json"), util.models.Sold)

            expected["BTC/USDT"].sold_datetime = self.FTX.sold["BTC/USDT"].sold_datetime
            self.assertDictEqual(expected, self.FTX.sold)

    def test_ftx_update_below_tsl(self):
        self.FTX.open_orders = Util.load_json(Config.TEST_DIR.joinpath("FTX_order_test.json"),
                                              util.models.Order)

        for key, value in self.FTX.open_orders.items():
            self.FTX.update(key, value, current_price=60000)
            self.FTX.update(key, value, current_price=25000)

            expected: Dict[str, util.models.Sold] = Util.load_json(
                Config.TEST_DIR.joinpath("FTX_order_test_update_below_tsl_expected.json"),
                util.models.Sold)

            expected["BTC/USDT"].sold_datetime = self.FTX.sold["BTC/USDT"].sold_datetime
            self.assertDictEqual(expected, self.FTX.sold)

    # def test_notifications(self):
    #     Config.load_global_config()
    #
    #     Config.NOTIFICATION_SERVICE.message("send_message test")
    #     sleep(0.1)
    #     Config.NOTIFICATION_SERVICE.error("send_error test")
    #     sleep(0.1)
    #     Config.NOTIFICATION_SERVICE.get_service('VERBOSE_FILE').error("send_verbose test")
    #     sleep(0.1)
    #     Config.NOTIFICATION_SERVICE.warning("send_warning test")
    #     sleep(0.1)
    #     Config.NOTIFICATION_SERVICE.info("send_info test")
    #     sleep(0.1)
    #     Config.NOTIFICATION_SERVICE.debug("send_debug test")
    #     sleep(0.1)
    #
    #     tickers, ticker_dict = self.FTX.get_starting_tickers()
    #     self.FTX.all_tickers = [t for t in tickers if t.ticker != "BTC/USDT"]
    #     ticker_dict.pop("BTC/USDT")
    #     self.FTX.ticker_seen_dict = ticker_dict
    #     new_tickers = self.FTX.get_new_tickers()
    #
    #     for new_ticker in new_tickers:
    #         self.FTX.process_new_ticker(new_ticker)
    #     Config.NOTIFICATION_SERVICE.message('ENTRY', pretty_entry, (self.FTX.orders["BTC/USDT"],))
    #     sleep(0.1)
    #     Config.NOTIFICATION_SERVICE.message('CLOSE', pretty_close, (self.FTX.orders["BTC/USDT"],))
    #     sleep(0.1)
    #     Config.NOTIFICATION_SERVICE.message('ENTRY', pretty_entry, (
    #         self.FTX.orders["BTC/USDT"],), fn_kwargs={'custom': True, 'comment': "Custom Entry Comment"})

    # LEAVE OFF, PLEASE DON'T SPAM MY ACCOUNT :)

    # def test_pipedream(self):
    #     tickers, ticker_dict = self.FTX.get_starting_tickers()
    #     self.FTX.all_tickers = [t for t in tickers if t.ticker != 'BTC/USDT']
    #     ticker_dict.pop('BTC/USDT')
    #     self.FTX.ticker_seen_dict = ticker_dict
    #     new_tickers = self.FTX.get_new_tickers()
    #
    #     for new_ticker in new_tickers:
    #         self.FTX.process_new_ticker(new_ticker)
    #
    #     resp = Util.post_pipedream(self.FTX.orders['BTC/USDT'])
    #     self.assertTrue(resp.status_code == 200)
