# -*- coding: utf-8 -*-
import scrapy
import csv
import re
from scrapy.http import Request

try:
    import simplejson as json
except ImportError:
    import json

# 京东搜索链接（手机端）
jd_search_url = 'https://so.m.jd.com/ware/search._m2wq_list?keyword={kw}&page={page}&pagesize=10'
# 京东红酒参数信息链接
jd_wine_info_url = 'https://wq.jd.com/commodity/itembranch/getspecification?callback=commParamCallBackA&skuid={skuid}&t=0.8241453845232118'

search_result_ptn = re.compile('searchCB\((.+)\)', re.DOTALL)
wine_info_ptn = re.compile('commParamCallBackA\((.+)\)', re.DOTALL)
illegal_character_ptn = re.compile('\\\\x[0-9A-Z]{2}')


class JdSearchSpider(scrapy.Spider):
    name = 'jd_search'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://item.m.jd.com/product/27584395169.html?sku=27584395169&price=318.00&fs=1&sid=&sf=newM&pos=2&csid=24b08ef98b46bf21786d58b35b4763a8_1557459569456_1_1557459569457&ss_symbol=10&ss_mtest=m-search-none&key=%E7%BA%A2%E9%AD%94%E9%AC%BC',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'
    }

    def start_requests(self):
        page = 1
        with open('./wine_project/data/红酒品牌.csv', 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for zh_name, en_name in csv_reader:
                print('++++++++++crawling:{}-{}'.format(zh_name, en_name))
                if zh_name.strip():
                    wine_name = zh_name.strip() + ' 红酒'
                    yield Request(jd_search_url.format(kw=wine_name, page=page), headers=self.headers,
                                  meta={'kw': wine_name, 'page': page}, callback=self.parse_search_result)
                if en_name.strip():
                    wine_name = en_name.strip() + ' 红酒'
                    yield Request(jd_search_url.format(kw=wine_name, page=page), headers=self.headers,
                                  meta={'kw': wine_name, 'page': page}, callback=self.parse_search_result)

    def parse_search_result(self, response):
        """解析京东搜索后的商品列表"""
        kw = response.meta['kw']
        page = response.meta['page']
        if '您访问的页面不存在！' in response.text:
            print('+++++++您访问的页面不存在！{}:{}'.format(kw, response.url))
            yield Request(jd_search_url.format(kw=kw, page=page), headers=self.headers,dont_filter=True,
                          meta={'kw': kw, 'page': page}, callback=self.parse_search_result)
            return

        content = re.sub(illegal_character_ptn, ' ', response.text)
        matcher = search_result_ptn.findall(content)
        if not matcher:
            print('***********************not match, kw:{}, url:{}'.format(kw, response.url))
            print(response.text)
            return
        json_data = json.loads(matcher[0])
        has_next_page = False
        if 'data' in json_data and 'searchm' in json_data['data'] and json_data['data']['searchm']['Paragraph']:
            for item in json_data['data']['searchm']['Paragraph']:
                has_next_page = True
                ret = {}
                ret['name'] = item['Content']['warename']
                ret['sku_id'] = item['wareid']
                ret['id'] = item['wareid']
                ret['price'] = item['dredisprice']
                ret['shop_name'] = item['shop_name']
                ret['shop_id'] = item['shop_id']
                ret['url'] = 'https://item.jd.com/{}.html'.format(item['wareid'])
                ret['keyword'] = kw
                yield Request(jd_wine_info_url.format(skuid=ret['sku_id']), headers=self.headers,
                              meta=ret, callback=self.parse_wine_info)
            # 抓取下一页数据
            if has_next_page:
                yield Request(jd_search_url.format(kw=kw, page=page + 1), headers=self.headers,
                              meta={'kw': kw, 'page': page + 1}, callback=self.parse_search_result)

    def parse_wine_info(self, response):
        """解析红酒属性信息"""
        ret = response.meta
        matcher = wine_info_ptn.findall(response.text)
        if not matcher:
            print('*************get wine info error')
            return
        json_data = json.loads(matcher[0])

        # 红酒属性信息，这里直接将属性的中文作为key，方便理解！！
        prop_dict = {}
        for prop_group in json_data['data']['propGroups']:
            for attr in prop_group['atts']:
                prop_dict[attr['attName']] = '|'.join(attr['vals'])
        ret['prop'] = prop_dict
        yield ret
