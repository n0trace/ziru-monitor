import json
import logging
import os
from multiprocessing import Process, Queue
import scrapy
from scrapy.crawler import Crawler, CrawlerProcess, CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor

from ziruspider.spiders.monitor import MonitorSpider

# the wrapper to make it run more times


def run_spider(spider, settings):
    def f(q):
        try:
            runner = CrawlerProcess(settings)
            runner.crawl(spider)
            runner.start()
            q.put(None)
        except Exception as e:
            q.put(e)
    q = Queue()
    p = Process(target=f, args=(q,))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result


def initializer(context):
    global access_key_id, access_key_secret, security_token
    access_key_id = context.credentials.access_key_id
    access_key_secret = context.credentials.access_key_secret
    security_token = context.credentials.security_token


def run(event, context):
    os.environ['TZ'] = 'Asia/Shanghai'
    global access_key_id, access_key_secret, security_token
    settings = get_project_settings()
    settings.set("ACCESS_KEY_ID", access_key_id)
    settings.set("ACCESS_KEY_SECRET", access_key_secret)
    settings.set("SECURITY_TOKEN", security_token)
    if "triggerName" in json.loads(event):
        evt = json.loads(json.loads(event)["payload"])
    else:
        evt = json.loads(event)
        settings.set("OTS_ENDPOINT", evt["otsEndpoint"])
        settings.set("OTS_INSTANCE", evt["otsInstance"])
    configure_logging(install_root_handler=True)
    logging.disable(30)  # WARNING = 50
    run_spider(MonitorSpider, settings)


if __name__ == '__main__':
    run(None, None)
