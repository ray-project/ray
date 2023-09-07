import signal
import os
import asyncio
def sigterm_handler():
    logger.warning("Exiting with SIGTERM immediately...")
    # Exit code 0 will be considered as an expected shutdown
    os._exit(signal.SIGTERM)
async def run():
    await asyncio.sleep(3600)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())




