from datetime import datetime
from main import get_sleep_time
from util import Config
from util import Util
import time

# setup logging
Util.setup_logging(name="new-coin-bot", level="DEBUG")


if __name__ == '__main__':


    while True:
        current_time = datetime.now()
        get_sleep_time(current_time)
        Config.auto_rate_current_weight += 1
        # time.sleep(1)



