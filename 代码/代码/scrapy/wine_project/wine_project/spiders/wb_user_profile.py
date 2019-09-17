# -*- coding: utf-8 -*-
import re
import csv
from urllib.parse import quote

from MySQLdb.cursors import DictCursor
from lxml import etree
import scrapy
from scrapy.http import Request

from wine_project.libs.wb_utils import parse_create_at
import MySQLdb

try:
    import simplejson as json
except ImportError:
    import json

# 实时查询接口
real_time_search_url = 'https://m.weibo.cn/api/container/getIndex?containerid={containerid}&page_type=searchall&page={page}'
# 用户个人信息接口（手机端信息不是很全面）
user_profile_url_mobile = 'https://m.weibo.cn/api/container/getIndex?containerid=230283{uid}_-_INFO&title=%25E5%259F%25BA%25E6%259C%25AC%25E4%25BF%25A1%25E6%2581%25AF&luicode=10000011&lfid=230283{uid}'
# 用户个人信息接口（web端）
user_profile_url_web = 'https://weibo.com/p/100505{uid}/info?mod=pedit'

user_profile_ptn = re.compile('domid":"Pl_Official_PersonalInfo__.*?"html":"(.*?)"}')


class WBUserProfileSpider(scrapy.Spider):
    name = 'wb_user_profile'

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 200,
            'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 350,
            'wine_project.middlewares.HTTPProxyMiddleware': 400,
            'wine_project.middlewares.WBCookieMiddleware': 401},
        'HTTPERROR_ALLOWED_CODES': [403, 418, 414, 302],
    }

    web_header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
    }

    def __init__(self, crawler, settings):
        self.crawler = crawler
        db_params = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',  # 编码要加上，否则可能出现中文乱码问题
            cursorclass=DictCursor,
            use_unicode=False,
        )
        self.db = MySQLdb.connect(**db_params)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        settings = crawler.settings
        return cls(crawler, settings)

    def start_requests(self):
        c = self.db.cursor()
        c.execute("SELECT w.data FROM wb_search w;")
        uids = {}
        for i in c.fetchall():
            json_data = json.loads(i['data'].decode('utf-8'))
            uids[str(json_data['uid'])] = json_data['keyword']  # 这里根据业务，关联的是关键词，当然你也可以关联微博ID

        c.execute('select id from wb_user_profile;')
        fetched_uids = set()
        for item in c.fetchall():
            fetched_uids.add(item[0])
        for fetched_uid in fetched_uids:
            if fetched_uid in uids:
                uids.pop(fetched_uid)

        for uid, keyword in uids.items():
            yield Request(user_profile_url_web.format(uid=uid), headers=self.web_header,
                          meta={'id': uid, 'keyword': keyword},
                          callback=self.parse_user_profile_web)

    def parse_user_profile_mobile(self, response):
        """解析用户个人信息"""
        user = response.meta
        json_data = json.loads(response.text)
        profile_dict = {}
        for card in json_data['cards']:
            for item in card['card_group']:
                if item['card_type'] == 41:
                    profile_dict[item['item_name']] = item['item_content']

    def parse_user_profile_web(self, response):
        ret = response.meta
        matcher = user_profile_ptn.findall(response.text)
        profile_dict = {}
        if not matcher:
            print('============not matched, url:{}'.format(response.url))
            return
        matcher = matcher[0].strip().replace("\\r", "").replace("\\n", "").replace("\\", "")
        selector = etree.HTML(matcher)
        for i in selector.xpath('//li'):
            key = i.xpath('./span[1]/text()')
            if i.xpath('./span[2]/a'):  # 主动发现带有链接的标签值
                value = i.xpath('string(./span[2])')
                value = '|'.join([i.strip() for i in value.split()]) if value else None
            else:
                value = i.xpath('./span[2]/text()')
                value = '|'.join([i.strip() for i in value if i.strip()]) if value else ''
            key = key[0].strip().replace('：', '') if key else None
            profile_dict[key] = value
        ret['profile'] = profile_dict
        yield ret
