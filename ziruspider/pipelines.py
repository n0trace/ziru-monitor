# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from tablestore import OTSClient, Row, Condition, RowExistenceExpectation, OTSClientError, OTSServiceError
import os
import logging


class ZiruspiderPipeline:
    item_mapping = {}
    items = set()
    last_items = set()
    last_item_mapping = {}
    ots_client = None

    def __init__(self, access_key_id,
                 access_key_secret,
                 security_token, endpoint, instance):

        self.ots_client = OTSClient(
            endpoint, access_key_id,
            access_key_secret, instance,
            sts_token=security_token,
        )
        self.ots_client.logger.setLevel(logging.WARN)

    @ classmethod
    def from_settings(cls, settings):
        params = {

        }
        if settings.get('ACCESS_KEY_ID'):
            params['access_key_id'] = settings['ACCESS_KEY_ID']
        if settings.get('ACCESS_KEY_SECRET'):
            params['access_key_secret'] = settings['ACCESS_KEY_SECRET']
        if settings.get('SECURITY_TOKEN'):
            params['security_token'] = settings['SECURITY_TOKEN']
        if settings.get('SECURITY_TOKEN'):
            params['endpoint'] = settings['OTS_ENDPOINT']
        if settings.get('OTS_INSTANCE'):
            params['instance'] = settings['OTS_INSTANCE']
        return cls(**params)

    @ classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        self.item_mapping[item['link']] = item
        self.items.add(item['link'])
        return item

    def close_spider(self, spider):
        add_keys = self.items.difference(self.last_items)
        del_keys = self.last_items.difference(self.items)
        print(add_keys)
        pass
