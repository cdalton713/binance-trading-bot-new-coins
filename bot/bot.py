import traceback
from datetime import datetime
from typing import List, Dict, NoReturn, Tuple

import math

from broker import Broker
from notification.notification import pretty_entry, pretty_close
from util import Config
from util import Util
from util.models import BrokerType, Ticker, Order, Sold


class Bot:
    def __init__(self, broker: BrokerType) -> NoReturn:
        self.broker = Broker.factory(broker)
        self.config = Config(self.broker.brokerType)

        self._pending_remove = []

        self.ticker_seen_dict = []
        self.all_tickers, self.ticker_seen_dict = self.get_starting_tickers()

        # create / load files
        self.open_orders: Dict[str, Order] = {}
        self.open_orders_file = None

        self.sold: Dict[str, Sold] = {}
        self.sold_file = None

        self.order_history: List[Dict[str, Order]] = []
        self.order_history_file = None

        for f in ["open_orders", "sold", "order_history"]:
            file = Config.ROOT_DIR.joinpath(f"{self.broker.brokerType}_{f}.json")
            self.__setattr__(f"{f}_file", file)
            if file.exists():
                self.__setattr__(
                    f,
                    Util.load_json(
                        file, Order if f in ["open_orders", "order_history"] else Sold
                    ),
                )

        # Meta info
        self.time = datetime.now()
        self.periodic_update_sent = False

    async def run_async(self) -> NoReturn:
        """
        Sells, adjusts TP and SL according to trailing values
        and buys new tickers
        """
        try:
            self.periodic_update()

            # basically the sell block and update TP and SL logic
            if len(self.open_orders) > 0:
                Config.NOTIFICATION_SERVICE.debug(
                    f"[{self.broker.brokerType}]\tActive Order Tickers: [{self.open_orders}]"
                )

                for key, stored_order in self.open_orders.items():
                    if key not in self.sold:
                        self.update(key, stored_order)

            # remove pending removals
            [self.open_orders.pop(o) for o in self._pending_remove]
            self._pending_remove = []

            # check if new tickers are listed
            new_tickers = self.get_new_tickers()

            if len(new_tickers) > 0:
                Config.NOTIFICATION_SERVICE.info(
                    f"[{self.broker.brokerType}]\tNew tickers detected: {new_tickers}"
                )

                for new_ticker in new_tickers:
                    self.process_new_ticker(new_ticker)
            else:
                Config.NOTIFICATION_SERVICE.debug(
                    f"[{self.broker.brokerType}]\tNo new tickers found"
                )

        except Exception as e:
            self.save()
            Config.NOTIFICATION_SERVICE.error(traceback.format_exc())

        finally:
            self.save()

    def _update(self, order, current_price) -> str:
        # if the price is decreasing and is below the stop loss
        if current_price < order.stop_loss:
            return "PRICE_BELOW_SL"

        # if the price is increasing and is higher than the old stop-loss maximum, update trailing stop loss
        elif (
            current_price > order.trailing_stop_loss_max
            and self.config.ENABLE_TRAILING_STOP_LOSS
        ):
            return "UPDATE_TRAILING_STOP_LOSS"

        # if the price is decreasing and has fallen below the trailing stop loss minimum
        elif (
            current_price < order.trailing_stop_loss
            and self.config.ENABLE_TRAILING_STOP_LOSS
            and order.trailing_stop_loss_activated is True
        ):
            return "PRICE_BELOW_TSL"

        # if price is increasing and is higher than the take profit maximum
        elif (
            current_price > order.take_profit
            and self.config.ENABLE_TRAILING_STOP_LOSS is False
        ):
            return "PRICE_ABOVE_TP"

    def update(self, ticker, order, **kwargs) -> NoReturn:
        # This is for testing
        current_price = kwargs.get(
            "current_price", self.broker.get_current_price(order.ticker)
        )

        action = self._update(order, current_price)

        if action in ["PRICE_BELOW_SL", "PRICE_ABOVE_TP", "PRICE_BELOW_TSL"]:
            self.close_trade(order, current_price, order.price, action)

        elif action == "UPDATE_TRAILING_STOP_LOSS":
            self.open_orders[ticker] = self.update_trailing_stop_loss(
                order, current_price
            )

    def upgrade_update(self) -> NoReturn:
        self.config.check_version()
        if self.config.OUTDATED:
            Config.NOTIFICATION_SERVICE.warning(
                """\n*******************************************\nNEW UPDATE AVAILABLE. PLEASE UPDATE!\n*******************************************"""
            )

    def periodic_update(self) -> NoReturn:
        """
        log an update about every LOG_INFO_UPDATE_INTERVAL minutes
        also re-saves files
        """
        minutes_past = math.floor(((datetime.now() - self.time).total_seconds() / 60))
        if minutes_past > 0 and minutes_past % Config.PROGRAM_OPTIONS[
            "LOG_INFO_UPDATE_INTERVAL"
        ] == 0 and not self.periodic_update_sent:
            Config.NOTIFICATION_SERVICE.info(
                f"[{self.broker.brokerType}] ORDERS UPDATE:\n\t{self.open_orders}"
            )
            Config.NOTIFICATION_SERVICE.info(f"[{self.broker.brokerType}]\tSaving..")
            self.save()
            self.upgrade_update()
            self.periodic_update_sent = True
        elif minutes_past > 0 and minutes_past % Config.PROGRAM_OPTIONS[
            "LOG_INFO_UPDATE_INTERVAL"
        ] > 0 and self.periodic_update_sent:
            self.periodic_update_sent = False

    def get_starting_tickers(self) -> Tuple[List[Ticker], Dict[str, bool]]:
        """
        This method should be used once before starting the loop.
        The value for every ticker detected before the loop is set to True in the ticker_seen_dict.
        All the new tickers detected during the loop will have a value of False.
        """

        tickers, headers = self.broker.get_tickers(self.config.QUOTE_TICKER)

        self.config.RATE_LIMIT = self.broker.get_rate_limit()
        ticker_seen_dict: Dict[str, bool] = {}

        for ticker in tickers:
            ticker_seen_dict[ticker.ticker] = True

        return tickers, ticker_seen_dict

    def get_new_tickers(self, **kwargs) -> List[Ticker]:
        """
        This method checks if there are new tickers listed and returns them in a list.
        The value of the new tickers in ticker_seen_dict will be set to True to make them not get detected again.
        """
        new_tickers = []
        Config.NOTIFICATION_SERVICE.debug(
            f"[{self.broker.brokerType}]\tGetting all tickers"
        )
        all_tickers_recheck, headers = self.broker.get_tickers(self.config.QUOTE_TICKER)

        Config.auto_rate_current_weight = int(headers['x-mbx-used-weight-1m'])

        if (
            all_tickers_recheck is not None
            and len(all_tickers_recheck) != self.ticker_seen_dict
        ):
            new_tickers = [
                i for i in all_tickers_recheck if i.ticker not in self.ticker_seen_dict
            ]

            for new_ticker in new_tickers:
                self.ticker_seen_dict[new_ticker.ticker] = True

        return new_tickers

    def update_trailing_stop_loss(self, order: Order, current_price: float) -> Order:

        # increase as absolute value for TP
        order.trailing_stop_loss_activated = True
        order.trailing_stop_loss_max = max(current_price, order.price)
        order.trailing_stop_loss = Util.percent_change(
            order.trailing_stop_loss_max, -self.config.TRAILING_STOP_LOSS_PERCENT
        )

        Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
            f"[{self.broker.brokerType}]\t[{order.ticker.ticker}] Updated:\n\tTrailing Stop-Loss: {round(order.trailing_stop_loss, 3)} "
        )
        Config.NOTIFICATION_SERVICE.info(
            f"[{self.broker.brokerType}]\t[{order.ticker.ticker}] Updated:\n\tTrailing Stop-Loss: {round(order.trailing_stop_loss, 3)} "
        )

        return order

    def close_trade(
        self, order: Order, current_price: float, stored_price: float, reason: str
    ) -> NoReturn:
        Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
            "CLOSING Order:\n{}".format(order.json())
        )
        Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
            "Current Price:\t{}".format(current_price)
        )
        Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
            "Stored Price:\t{}".format(stored_price)
        )

        sell: Order = self.broker.place_order(
            self.config,
            ticker=order.ticker,
            side="sell",
            size=order.size,
            current_price=current_price,
        )

        Config.NOTIFICATION_SERVICE.message("CLOSE", pretty_close, (order,))

        # pending remove order from json file
        self.order_history.append({order.ticker.ticker: order})
        self._pending_remove.append(order.ticker.ticker)

        # store sold trades data
        sold = Sold(
            broker=sell.broker,
            ticker=order.ticker,
            purchase_datetime=order.purchase_datetime,
            price=sell.price,
            side=sell.side,
            size=sell.size,
            type=sell.type,
            status=sell.status,
            take_profit=order.take_profit,
            stop_loss=order.stop_loss,
            trailing_stop_loss_activated=order.trailing_stop_loss_activated,
            trailing_stop_loss_max=order.trailing_stop_loss_max,
            trailing_stop_loss=order.trailing_stop_loss,
            profit=(current_price * sell.size) - (stored_price * order.size),
            profit_percent=((current_price * sell.size) - (stored_price * order.size))
            / (stored_price * order.size)
            * 100,
            sold_datetime=sell.purchase_datetime,
            reason=reason,
        )

        Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
            "SOLD:\n{}".format(sold.json())
        )

        self.sold[order.ticker.ticker] = sold
        if not Config.TEST and Config.SHARE_DATA:
            Util.post_pipedream(sold)

        self.save()

    def process_new_ticker(self, new_ticker: Ticker, **kwargs) -> NoReturn:
        # buy if the ticker hasn't already been bought
        Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
            "PROCESSING NEW TICKER:\n{}".format(new_ticker.json())
        )

        if (
            new_ticker.ticker not in self.open_orders
            and self.config.QUOTE_TICKER in new_ticker.quote_ticker
        ):
            Config.NOTIFICATION_SERVICE.info(
                f"[{self.broker.brokerType}]\tPreparing to buy {new_ticker.ticker}"
            )
            Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
                f"[{self.broker.brokerType}]\tPreparing to buy {new_ticker.ticker}"
            )

            try:

                Config.NOTIFICATION_SERVICE.info(
                    f"[{self.broker.brokerType}]\tPlacing [{'TEST' if self.config.TEST else 'LIVE'}] Order.."
                )
                Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
                    f"[{self.broker.brokerType}]\tPlacing [{'TEST' if self.config.TEST else 'LIVE'}] Order.."
                )

                if self.broker.brokerType == "FTX":
                    price = self.broker.get_current_price(new_ticker)
                    size = self.broker.convert_size(
                        config=self.config, ticker=new_ticker, price=price
                    )

                    order = self.broker.place_order(
                        self.config, ticker=new_ticker, side="BUY", size=size, **kwargs
                    )

                else:
                    order = self.broker.place_order(
                        self.config, ticker=new_ticker, side="BUY", **kwargs
                    )

                Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
                    "ORDER RESPONSE:\n{}".format(order.json())
                )
                self.open_orders[new_ticker.ticker] = order
                if not Config.TEST and Config.SHARE_DATA:
                    Util.post_pipedream(order)

                Config.NOTIFICATION_SERVICE.message("ENTRY", pretty_entry, (order,))
            except Exception as e:
                Config.NOTIFICATION_SERVICE.error(traceback.format_exc())
            finally:
                self.save()

        else:
            Config.NOTIFICATION_SERVICE.error(
                f"[{self.broker.brokerType}]\tNew new_ticker detected, but {new_ticker.ticker} is currently in "
                f"portfolio, or {self.config.QUOTE_TICKER} does not match"
            )
            Config.NOTIFICATION_SERVICE.get_service("VERBOSE_FILE").error(
                f"[{self.broker.brokerType}]\tNew new_ticker detected, but {new_ticker.ticker} is currently in "
                f"portfolio, or {self.config.QUOTE_TICKER} does not match.\n{new_ticker.json()}"
            )

    def save(self) -> NoReturn:
        Util.dump_json(self.open_orders_file, obj=self.open_orders)
        Util.dump_json(self.order_history_file, obj=self.order_history)
        Util.dump_json(self.sold_file, obj=self.sold)
