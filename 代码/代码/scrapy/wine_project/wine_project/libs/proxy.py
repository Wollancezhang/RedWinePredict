# -*- coding:utf-8 -*-
"""存储和获取代理接口方法
* 直接运行本文件将通过API获取代理并存储到Redis中，每次API请求获取5个代理，最小时间间隔：10s
* 通过模块引入代理类，实现代理的获取和删除
"""
import time
import logging
import random
from threading import Thread

import redis
import requests
from tornado.ioloop import IOLoop, PeriodicCallback

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

timestamp_10 = lambda: int(time.time())


class ProxyRedis(object):
    def __init__(self, host='127.0.0.1', port=6379, password=None, key='datatom.proxies', expire=6 * 60,
                 db=0):
        pool = redis.ConnectionPool(host=host, port=port, password=password, db=db, decode_responses=True)
        self.redis_cli = redis.Redis(connection_pool=pool)
        self.key = key
        self.expire = expire
        # self.remove_expire_item()

    def remove_expire_item(self):
        """删除过期的代理"""
        expire_timestamp = timestamp_10() - self.expire
        return self.redis_cli.zremrangebyscore(self.key, 0, expire_timestamp)

    def add(self, proxy):
        """添加一个代理
            redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
            1.1, 'name1'：score在前 name在后
        """
        # 防止同一时间存入代理，取出代理时取出同一个
        score = timestamp_10() - random.randrange(10, 30)
        return self.redis_cli.zadd(self.key, proxy, score)

    def add_many(self, proxy_list):
        """添加多个代理"""
        kwargs = dict().fromkeys(proxy_list, timestamp_10())
        return self.redis_cli.zadd(self.key, **kwargs)

    def get(self, num=1):
        """获取代理
            默认是取出最近添加的数据，注意两个 inf 的顺序
        """
        result = self.redis_cli.zrevrangebyscore(self.key, '+inf', '-inf', start=0, num=num)
        return result[0] if len(result) == 1 else result

    def remove(self, proxy):
        """移除一个代理"""
        self.remove_expire_item()
        return self.redis_cli.zrem(self.key, proxy)

    def size(self):
        return self.redis_cli.zcard(self.key)


class BaseProxy(object):
    url = None

    def __init__(self, url, **kwargs):
        self.api = url
        self.session = requests.Session()
        self.max_retry = 3
        self.threshold = 15
        self.pr = ProxyRedis(**kwargs)
        # self.fetch_proxies()

    def fetch_proxies(self):
        raise NotImplementedError

    def get(self, num=1):
        return self.pr.get(num)

    def remove(self, proxy):
        self.pr.remove_expire_item()
        if self.pr.size() < self.threshold:
            self.fetch_proxies()
        return self.pr.remove(proxy)


class MoguProxy(BaseProxy):
    # TODO 更换蘑菇代理的app key
    url = 'http://mvip.piping.mogumiao.com/proxy/api/get_ip_bs?appKey=b14de4c4daee42a69064adf852e21612&count=5&expiryDate=0&format=1&newLine=2'

    def __init__(self, **kwargs):
        super(MoguProxy, self).__init__(self.url, **kwargs)

    def fetch_proxies(self):
        retry = 0
        while retry < self.max_retry:
            try:
                resp = self.session.get(self.url, timeout=10)
                json_data = resp.json()
            except Exception as e:
                logger.exception('fetch proxies error, e:{}'.format(e))
                retry += 1
                time.sleep(retry * 2)
            else:
                if not isinstance(json_data, dict):
                    logger.exception('response type error, e:{}'.format(json_data))
                    return
                elif isinstance(json_data.get('msg'), str):
                    logger.warning('retrieve proxy error, msg:{}'.format(json_data.get('msg')))
                    return
                else:
                    for item in json_data.get('msg', []):
                        proxy = 'http://{}:{}'.format(item['ip'], item['port'])
                        # logger.info('dump proxy:{}'.format(proxy))
                        self.pr.add(proxy)
                    logger.info('Current proxy pool size:{}'.format(self.pr.size()))
                    break


def run_crawl(**kwargs):
    ep = MoguProxy(**kwargs)
    io_loop = IOLoop()
    PeriodicCallback(ep.fetch_proxies, 20000).start()
    io_loop.start()


if __name__ == '__main__':
    run_crawl()
