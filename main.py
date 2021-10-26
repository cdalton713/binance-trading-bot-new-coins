import asyncio
from typing import List
import time
from util import Config, Util
from bot import Bot
import traceback
from pathlib import Path

Config.load_global_config()

# setup logging
Util.setup_logging(name="new-coin-bot", level=Config.PROGRAM_OPTIONS["LOG_LEVEL"])

total_time = 0
total_iter = 0

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
        t = time.time()
        await main(routines)
        time_taken = time.time() - t
        Config.NOTIFICATION_SERVICE.debug(
            "Loop finished in [{}] seconds".format(time_taken)
        )
        Config.NOTIFICATION_SERVICE.debug(
            "Sleeping for [{}] seconds".format(Config.FREQUENCY_SECONDS)
        )

        Config.total_time += time_taken
        Config.total_iter += 1

        await asyncio.sleep(Config.FREQUENCY_SECONDS)


async def main(bots_: List):
    coroutines = [b.run_async() for b in bots_]

    # This returns the results one by one.
    for future in asyncio.as_completed(coroutines):
        await future


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
