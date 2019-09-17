# -*- coding: utf-8 -*-
import re
import csv
from urllib.parse import quote

import scrapy
from scrapy.http import Request

from wine_project.libs.wb_utils import parse_create_at

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


class WBSearchSpider(scrapy.Spider):
    name = 'wb_search'

    custom_settings = {
        'HTTPERROR_ALLOWED_CODES': [418, 403],
    }

    web_header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': 'Ugrow-G0=5b31332af1361e117ff29bb32e4d8439; SUB=_2AkMriaY8f8NxqwJRmP0QyGnjb4h_zAjEieKd1VfnJRMxHRl-yT9jqnEAtRB6AAmI00KhY3UMPhnEjJISXWoRqSMGKzGu; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WFk1F.LVgXDFYo6ZGhQVYJa; _s_tentry=passport.weibo.com; Apache=7751511007368.586.1557473547027; SINAGLOBAL=7751511007368.586.1557473547027; ULV=1557473547067:1:1:1:7751511007368.586.1557473547027:; TC-Page-G0=7f6863db1952ff8adac0858ad5825a3b|1557473625|1557473543',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
    }

    def start_requests(self):
        page = 1
        with open('./wine_project/data/红酒品牌.csv', 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for zh_name, en_name in csv_reader:
                print('++++++++++crawling:{}-{}'.format(zh_name, en_name))
                if zh_name.strip():
                    wine_name = zh_name + ' 红酒'
                    q_string = '100103type=61&q={kw}&t=0'.format(kw=wine_name)
                    yield Request(real_time_search_url.format(containerid=quote(q_string), page=page),
                                  meta={'kw': wine_name, 'page': 1}, callback=self.parse_search_result)
                if en_name.strip():
                    wine_name = en_name + ' 红酒'
                    q_string = '100103type=61&q={kw}&t=0'.format(kw=wine_name)
                    yield Request(real_time_search_url.format(containerid=quote(q_string), page=page),
                                  meta={'kw': wine_name, 'page': 1}, callback=self.parse_search_result)

    def parse_search_result(self, response):
        """解析实时查询返回的数据"""
        kw = response.meta['kw']
        page = response.meta['page']

        json_data = json.loads(response.text)
        has_next_page = False
        if 'data' in json_data and 'cards' in json_data['data']:
            for card in json_data['data']['cards']:
                if card['card_type'] != 11:
                    continue
                for item in card['card_group']:
                    if item['card_type'] == 9 and item['mblog'].get('user'):
                        has_next_page = True
                        blog = item['mblog']
                        u = blog['user']
                        uid = u.get('id')
                        ret = {}
                        ret['id'] = blog['id']
                        ret['url'] = 'https://weibo.com/{uid}/{bid}'.format(uid=uid, bid=blog['bid'])
                        ret['keyword'] = kw

                        ret['uid'] = uid
                        ret['post_time'] = parse_create_at(blog['created_at'])
                        ret['post_text'] = blog.get('text') or blog.get('raw_text')
                        ret['post_source'] = blog['source']
                        ret['description'] = u['description']
                        ret['follow_count'] = u['follow_count']
                        ret['followers_count'] = u['followers_count']
                        ret['gender'] = u['gender']
                        ret['name'] = u['screen_name']
                        yield ret

        # 获取下一页的数据
        if has_next_page:
            q_string = '100103type=61&q={kw}&t=0'.format(kw=kw)
            yield Request(real_time_search_url.format(containerid=quote(q_string), page=page + 1),
                          meta={'kw': kw, 'page': page + 1}, callback=self.parse_search_result)
