import asyncio
import logging
import traceback
from datetime import datetime
from typing import List

from bot import Bot
from util import Config, Util

Config.load_global_config()

# setup logging
Util.setup_logging(name="new-coin-bot", level=Config.PROGRAM_OPTIONS["LOG_LEVEL"])
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def setup() -> List[Bot]:
    Config.NOTIFICATION_SERVICE.info("Creating bots..")

    # Create bots based on config
    b = []
    for broker in Config.ENABLED_BROKERS:
        Config.NOTIFICATION_SERVICE.info("Creating bot [{}]".format(broker))
        b.append(Bot(broker))

    if len(b) > 0:
        b[0].upgrade_update()
    return b


async def forever(routines: List):
    while True:
        current_time = datetime.now()

        if Config.FRONTLOAD_ENABLED:
            while (
                current_time.second >= Config.FRONTLOAD_START
                or current_time.second
                <= Config.FRONTLOAD_DURATION - (60 - Config.FRONTLOAD_START)
            ) and Config.auto_rate_current_weight < (Config.auto_rate_limit * 0.9):

                # FRONTLOAD PERIOD
                await main(routines, current_time)

                current_time = datetime.now()

        # STANDARD PERIOD
        await main(routines, current_time)

        sleep_time = get_sleep_time(current_time)
        await asyncio.sleep(sleep_time)


async def main(bots_: List, current_time: datetime):
    await _main(bots_)
    time_taken = datetime.now() - current_time
    Config.NOTIFICATION_SERVICE.debug(
        "Loop finished in [{}] seconds".format(time_taken.microseconds / 1000000)
    )

    Config.total_time += time_taken.microseconds / 1000000
    Config.total_iter += 1
    Config.NOTIFICATION_SERVICE.debug(
        "Request Weight: {}".format(Config.auto_rate_current_weight)
    )


async def _main(bots_: List):
    coroutines = [b.run_async() for b in bots_]

    # This returns the results one by one.
    for future in asyncio.as_completed(coroutines):
        await future


def get_sleep_time(current_time: datetime) -> int:
    if (
        Config.auto_rate_current_weight
        >= Config.auto_rate_limit * Config.RATE_INTERVENTION_PERCENTAGE / 100
    ):
        increase_time = True
        if Config.auto_rate_current_weight >= Config.auto_rate_limit * 0.85:
            resume_time = datetime(
                current_time.year,
                current_time.month,
                current_time.day,
                current_time.hour,
                current_time.minute + 1,
                0,
                0,
            )
            Config.NOTIFICATION_SERVICE.info(f"Bot request count above [85%] of rate limit")

            if current_time.minute != Config.auto_rate_increased_minute:
                Config.auto_rate_increased_minute = current_time.minute
                increase_time = True
            else:
                increase_time = False

        else:
            resume_time = datetime(
                current_time.year,
                current_time.month,
                current_time.day,
                current_time.hour,
                current_time.minute
                if Config.FRONTLOAD_ENABLED
                else current_time.minute + 1,
                Config.FRONTLOAD_START if Config.FRONTLOAD_ENABLED else 0,
                500000 if Config.FRONTLOAD_START else 0,
            )
            Config.NOTIFICATION_SERVICE.info(f"Bot request count above [{Config.RATE_INTERVENTION_PERCENTAGE}%] of "
                                             f"rate limit")

        sleep_time = min(max((resume_time - current_time).seconds, 1), 59)

        if Config.AUTO_INCREASE_FREQUENCY and increase_time:
            Config.NOTIFICATION_SERVICE.info(
                f"Increasing FREQUENCY from [{Config.FREQUENCY_SECONDS}] to "
                f"[{Config.FREQUENCY_SECONDS + Config.AUTO_INCREASE_AMOUNT}] seconds"
            )
            Config.FREQUENCY_SECONDS += Config.AUTO_INCREASE_AMOUNT

        Config.NOTIFICATION_SERVICE.info(
            "Sleeping for [{}] seconds until [{}] to avoid exceeding rate limits".format(
                sleep_time, resume_time
            )
        )
    else:
        sleep_time = Config.FREQUENCY_SECONDS

    Config.NOTIFICATION_SERVICE.debug("Sleeping for [{}] seconds".format(sleep_time))
    return sleep_time


if __name__ == "__main__":
    Config.NOTIFICATION_SERVICE.info("Starting..")
    loop = asyncio.get_event_loop()
    bots = setup()
    try:
        loop.create_task(forever(bots))
        loop.run_forever()
    except KeyboardInterrupt as e:
        Config.NOTIFICATION_SERVICE.info("Exiting program..")
    except Exception as e:
        Config.NOTIFICATION_SERVICE.error(traceback.format_exc())
    finally:
        for bot in bots:
            bot.save()
        print("AVG TIME PER LOOP: {}".format(Config.total_time / Config.total_iter))
        print("TOTAL LOOPS: {}".format(Config.total_iter))
