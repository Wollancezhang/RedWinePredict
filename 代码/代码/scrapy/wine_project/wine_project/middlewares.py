# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import re
import logging
from urllib.parse import urlencode

from scrapy import signals, FormRequest
from scrapy.exceptions import CloseSpider
from scrapy.exceptions import CloseSpider
from scrapy.http import Request
from scrapy.downloadermiddlewares.cookies import CookiesMiddleware
from scrapy.exceptions import IgnoreRequest, NotConfigured
from scrapy.http.cookies import CookieJar
from scrapy.utils.reqser import request_to_dict, request_from_dict
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.error import TimeoutError, ConnectionRefusedError, ConnectError
from twisted.web._newclient import ResponseNeverReceived
from twisted.internet.error import ConnectError
from twisted.web._newclient import ResponseNeverReceived

from wine_project.libs.proxy import ProxyRedis
from wine_project.libs.wb_login import WeiboCookie

try:
    import simplejson as json
except ImportError:
    import json

logger = logging.getLogger(__name__)


class WineProjectSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class WineProjectDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class HTTPProxyMiddleware(object):
    # 遇到这些类型的错误直接当做代理不可用处理掉, 不再传给retrymiddleware
    DONT_RETRY_ERRORS = (TimeoutError, ConnectionRefusedError, ResponseNeverReceived, ConnectError, ValueError)

    def __init__(self, settings, crawler):
        self.max_proxy_retries = 3
        self.crawler = crawler
        self.mogu_proxy = ProxyRedis()
        self.current_proxy = self.mogu_proxy.get()
        self.mogu_proxy.remove(self.current_proxy)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler)

    def process_request(self, request, spider):
        # print('--->into process request')
        if self.current_proxy is None:
            self.current_proxy = self.mogu_proxy.get()
            self.mogu_proxy.remove(self.current_proxy)
        print('using proxy:{}'.format(self.current_proxy))
        request.meta['proxy'] = self.current_proxy

    def _retry(self, request, spider):
        retries = request.meta.get('proxy_retries', 1)
        req_proxy = request.meta.get('proxy')
        if retries < self.max_proxy_retries:
            self.current_proxy = self.mogu_proxy.get()
            self.mogu_proxy.remove(self.current_proxy)
            new_request = request.copy()
            new_request.meta['proxy'] = self.current_proxy
            new_request.meta['proxy_retries'] = retries + 1
            new_request.dont_filter = True
            return new_request
        else:
            logger.debug("Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': 'max retry'},
                         extra={'spider': spider})

    def process_response(self, request, response, spider):
        # print('--->into process response')
        if response.status != 200:
            logger.warning("+++++[%d]%s %s" % (response.status, request.meta['proxy'], request.url))
            return self._retry(request, spider)
        else:
            req_proxy = request.meta.get('proxy')
            self.mogu_proxy.add(req_proxy)
            return response

    def process_exception(self, request, exception, spider):
        """处理由于使用代理导致的连接异常 --- 继续使用该代理重试
            另外一种错误是被微博403 --- 换个代理重试
        """
        # proxy = request.meta.get('proxy')
        # print('--->into process exception')
        if isinstance(exception, CloseSpider):
            print('==========close spider')
            self.crawler.engine.close_spider(spider, 'ip pool is empty, exit!')
        else:
            logger.info('+++++process exception:%r', exception)
            new_request = self._retry(request, spider)
            return new_request


class WBCookieMiddleware:
    def __init__(self):
        self.max_retry = 3
        self.cookie_generator = WeiboCookie()
        self.current_cookie = None

    def process_request(self, request, spider):
        if self.current_cookie is None:
            self.current_cookie = self.cookie_generator.get_cookie()
        else:
            request.cookies.update(self.current_cookie)

    def process_response(self, request, response, spider):
        if response.status == 200:
            return response
        retries = request.meta.get('cookie_retries', 1)
        if retries <= self.max_retry:
            print('***********Change Cookies, url:{}'.format(response.url))
            self.current_cookie = self.cookie_generator.get_cookie()
            del request.headers[b'Cookie']
            request.cookies = self.current_cookie
            request.dont_filter = True
            request.meta['cookie_retries'] = retries + 1
            return response
