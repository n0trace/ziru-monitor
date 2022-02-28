# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import json
import logging
import os
import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
from tablestore import (Condition, OTSClient, OTSClientError, OTSServiceError,
                        Row, RowExistenceExpectation)


class ZiruspiderPipeline:
    items = {}
    item_keys = set()
    last_items = {}
    last_item_keys = set()
    ots_client = None
    ding_talk_secret = ""
    ding_talk_access_token = ""

    def __init__(self,
                 access_key_id, access_key_secret, security_token,
                 endpoint, instance,
                 ding_talk_access_token, ding_talk_secret):

        self.ots_client = OTSClient(
            endpoint, access_key_id,
            access_key_secret, instance,
            sts_token=security_token,
        )
        self.ding_talk_access_token = ding_talk_access_token
        self.ding_talk_secret = ding_talk_secret
        self.ots_client.logger.setLevel(logging.WARN)
        primary_key = [('key', "last")]
        columns_to_get = ['value']

        consumed, return_row, next_token = self.ots_client.get_row(
            "kvs", primary_key, columns_to_get, None, 1)
        value = None
        for att in return_row.attribute_columns:
            value = att[1]
        self.last_items = json.loads(value)
        self.last_item_keys = {key for key in self.last_items}

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
        if settings.get('DING_TALK_SECRET'):
            params['ding_talk_secret'] = settings['DING_TALK_SECRET']
        if settings.get('DING_TALK_ACCESS_TOKEN'):
            params['ding_talk_access_token'] = settings['DING_TALK_ACCESS_TOKEN']
        return cls(**params)

    @ classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def fix_url(self, url):
        if url.startswith('//'):
            return "https:" + url
        return url

    def sign(self, secret):
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc,
                             digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return [timestamp, sign]

    def process_item(self, item, spider):
        self.item_keys.add(item['link'])
        self.items[item['link']] = item
        return item

    def close_spider(self, spider):
        add_keys = self.item_keys.difference(self.last_item_keys)
        del_keys = self.last_item_keys.difference(self.item_keys)
        primary_key = [('key', 'last')]
        attribute_columns = [('value', json.dumps(self.items))]
        row = Row(primary_key, attribute_columns)
        condition = Condition('IGNORE')
        try:
            self.ots_client.put_row("kvs", row, condition)
        except Exception as e:
            print(e)
        for key in add_keys:
            item = self.items[key]
            text = "#### 新增房源\n##### [{title}]({link})\n\n > {desc}\n\n![]({thumb})".format(
                title=item['title'],
                link=self.fix_url(item['link'].replace(
                    'www.ziroom.com/x/', 'm.ziroom.com/bj/x/')) + "?inv_no=733998694",
                desc=item['desc'],
                thumb=self.fix_url(item['thumb']))

            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "新增房源",
                    "text": text
                },
            }
            timestamp, sign = self.sign(self.ding_talk_secret)
            url = 'https://oapi.dingtalk.com/robot/send?access_token={}'.format(
                self.ding_talk_access_token)
            headers = {
                'Content-Type': 'application/json',
                'timestamp': timestamp,
                'sign': sign,
            }

            r = requests.post(
                url, data=json.dumps(data),
                headers=headers)
        for key in del_keys:
            item = self.last_items[key]
            text = "#### 下架房源\n##### [{title}]({link})\n\n > {desc}\n\n![]({thumb})".format(
                title=item['title'],
                link=self.fix_url(item['link'].replace(
                    'www.ziroom.com/x/', 'm.ziroom.com/bj/x/')) + "?inv_no=733998694",
                desc=item['desc'],
                thumb=self.fix_url(item['thumb']))

            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "下架房源",
                    "text": text
                },
            }
            timestamp, sign = self.sign(self.ding_talk_secret)
            url = 'https://oapi.dingtalk.com/robot/send?access_token={}'.format(
                self.ding_talk_access_token)
            headers = {
                'Content-Type': 'application/json',
                'timestamp': timestamp,
                'sign': sign,
            }

            r = requests.post(
                url, data=json.dumps(data),
                headers=headers)
