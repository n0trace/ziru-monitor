# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from tablestore import OTSClient, Row, Condition, RowExistenceExpectation, OTSClientError, OTSServiceError
import os
import logging
import json


class ZiruspiderPipeline:
    items = []
    item_keys = set()
    last_items = []
    last_item_keys = set()
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
        primary_key = [('key', "last")]
        columns_to_get = ['value']

        consumed, return_row, next_token = self.ots_client.get_row(
            "kvs", primary_key, columns_to_get, None, 1)
        value = None
        for att in return_row.attribute_columns:
            value = att[1]
        self.last_items = json.loads(value)
        self.last_item_keys = {item['link'] for item in self.last_items}

    @classmethod
    def from_settings(cls, settings):
        params = {}
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
        self.item_keys.add(item['link'])
        self.items.append(item)
        return item

    def close_spider(self, spider):
        # add_keys = self.item_keys.difference(self.last_item_keys)
        # del_keys = self.last_item_keys.difference(self.item_keys)
        primary_key = [('key', 'last')]
        attribute_columns = [('value', json.dumps(self.items))]
        row = Row(primary_key, attribute_columns)
        condition = Condition('IGNORE')
        try:
            self.ots_client.put_row("kvs", row, condition)
        except Exception as e:
            print(e)
