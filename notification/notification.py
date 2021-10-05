from typing import Dict, Tuple, Optional
from multiNotification import Notification
from multiNotification.notification.models import NotificationSettings
from util.types import Order


def pretty_format_message(order: Order) -> str:
    return """
    Broker: {broker}
    Datetime: {datetime}
    Status: {status}

    Ticker: {ticker}
    Amount: {amount}
    Price: {price}""".format(
        broker=order.broker,
        datetime=order.purchase_datetime,
        status=order.status,
        ticker=order.ticker.ticker,
        amount=round(order.size, 4),
        price=round(order.price, 4),
    )


def pretty_entry(service: Notification, message: Optional[str] = None, fn_args: Optional[Tuple] = None,
                 fn_kwargs: Optional[Dict] = None) -> str:
    if service.settings.entry:
        if fn_kwargs is not None and "custom" in fn_kwargs and fn_kwargs["custom"] and "comment" in fn_kwargs:
            return fn_kwargs["comment"]
        else:
            msg = """\nNEW POSITION\n"""

            if fn_kwargs is not None and "comment" in fn_kwargs:
                msg += "\n{comment}\n".format(comment=fn_kwargs["comment"])

            msg += pretty_format_message(fn_args[0])
            return msg
    return ''


def pretty_close(service: Notification, message: Optional[str] = None, fn_args: Optional[Tuple] = None,
                 fn_kwargs: Optional[Dict] = None) -> str:
    if service.settings.close:
        if fn_kwargs is not None and "custom" in fn_kwargs and fn_kwargs["custom"] and "comment" in fn_kwargs:
            return fn_kwargs["comment"]
        else:
            msg = """POSITION CLOSED\n"""

            if fn_kwargs is not None and "comment" in fn_kwargs:
                msg += "\n{comment}\n".format(comment=fn_kwargs["comment"])

            msg += pretty_format_message(fn_args[0])
            return msg
    return ''


class CustomNotificationSettings(NotificationSettings):
    entry: bool
    close: bool


ALL_NOTIFICATIONS_ON = CustomNotificationSettings(
    message=True,
    error=True,
    warning=True,
    info=True,
    debug=True,
    entry=True,
    close=True
)

DEFAULT_NOTIFICATIONS = CustomNotificationSettings(
    message=True,
    error=True,
    warning=False,
    info=False,
    debug=False,
    entry=True,
    close=True
)


def parse_settings(settings: Dict):
    try:
        settings = CustomNotificationSettings(
        message=settings['SEND_MESSAGE'] if 'SEND_MESSAGE' in settings else DEFAULT_NOTIFICATIONS.message,
        error=settings['SEND_ERROR'] if 'SEND_ERROR' in settings else DEFAULT_NOTIFICATIONS.error,
        warning=settings['SEND_WARNING'] if 'SEND_WARNING' in settings else DEFAULT_NOTIFICATIONS.warning,
        info=settings['SEND_INFO'] if 'SEND_INFO' in settings else DEFAULT_NOTIFICATIONS.info,
        debug=settings['SEND_DEBUG'] if 'SEND_DEBUG' in settings else DEFAULT_NOTIFICATIONS.debug,
        entry=settings['SEND_ENTRY'] if 'SEND_ENTRY' in settings else DEFAULT_NOTIFICATIONS.entry,
        close=settings['SEND_CLOSE'] if 'SEND_CLOSE' in settings else DEFAULT_NOTIFICATIONS.close,
        )
    except Exception as e:
        print("Error with config file.  Do you have commas after each SEND_XXX?  If so, remove them.")
    return settings

