import os
from loguru import logger


def is_jupyter():
    """
    True if file is ran from a jupyter notebook
    Must be ran before importing tornado
    """
    return "JPY_PARENT_PID" in os.environ


if is_jupyter():
    logger.info("Imported from a jupyter notebook, importing nest_asyncio..")
    import nest_asyncio

    nest_asyncio.apply()


import argparse  # noqa E402
import json  # noqa E402
import time  # noqa E402

import tornado  # noqa E402
from tornado import gen  # noqa E402
from tornado.httpclient import AsyncHTTPClient  # noqa E402
from tornado.simple_httpclient import SimpleAsyncHTTPClient  # noqa E402
from tornado.log import gen_log  # noqa E402


def convert_time(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(seconds))


class NoQueueTimeoutHTTPClient(SimpleAsyncHTTPClient):
    def fetch_impl(self, request, callback):
        key = object()

        self.queue.append((key, request, callback))
        self.waiting[key] = (request, callback, None)

        self._process_queue()

        if self.queue:
            gen_log.debug(
                "max_clients limit reached, request queued. %d active, %d queued requests."
                % (len(self.active), len(self.queue))
            )


@gen.coroutine
def fetch_and_handle(urls):
    """Fetches the urls and handles/processes the response"""

    tables = {}
    nurls = len(urls)
    done = 0
    AsyncHTTPClient.configure(NoQueueTimeoutHTTPClient)
    http_client = AsyncHTTPClient(defaults=dict(request_timeout=180))

    start = time.time()

    if isinstance(urls, dict):
        waiter = gen.WaitIterator(
            **{k: http_client.fetch(v) for k, v in urls.items()}
        )
    elif isinstance(urls, list):
        waiter = gen.WaitIterator(*[http_client.fetch(url) for url in urls])

    while not waiter.done():
        try:
            result = yield waiter.next()
            tables[waiter.current_index] = json.loads(result.body)
        except Exception as e:
            print("Error {} from {}".format(e, waiter.current_future))
            print(waiter.current_index)
            continue

        else:
            done += 1
            estimated_time = (time.time() - start) * (nurls - done) / done
            print(
                f"{round((done/nurls)*100, 2)}% ({convert_time(estimated_time)})",
                end="\r",
            )

    return tables


def make_requests(urls):
    loop = tornado.ioloop.IOLoop.current()
    logger.info(f"Making {len(urls)} requests..")

    return loop.run_sync(lambda: fetch_and_handle(urls))
