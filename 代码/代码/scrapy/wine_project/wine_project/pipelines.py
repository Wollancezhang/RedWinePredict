# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import csv
from twisted.enterprise import adbapi
from MySQLdb.cursors import  DictCursor

try:
    import simplejson as json
except ImportError:
    import json


class WineProjectPipeline(object):
    def process_item(self, item, spider):
        return item


class CSVPipeline(object):
    def __init__(self, file_folder):
        self.file_folder = file_folder

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            file_folder=crawler.settings.get('CSV_FOLDER', './win_project/data/'),
        )

    def open_spider(self, spider):
        self.file_path = os.path.join(self.file_folder, '{}.csv'.format(spider.name))
        self.f = open(self.file_path, 'a', encoding='utf-8', newline='')

    def close_spider(self, spider):
        self.f.close()

    def process_item(self, item, spider):
        self.write_item(item)
        return item

    def write_item(self, item):
        # print('++++=pipeline write line')
        for k in ['depth', 'download_timeout', 'download_slot', 'download_latency', 'proxy',
                  'proxy_retries', 'cookie_retries', 'redirect_times',
                  'redirect_urls', 'redirect_ttl', 'confirm_exit']:
            if k in item:
                item.pop(k)
        json_str = json.dumps(item)
        self.f.write(json_str)
        self.f.write('\n')


class MySQLPipeline(object):
    '''保存到数据库中对应的class
       1、在settings.py文件中配置
       2、在自己实现的爬虫类中yield item,会自动执行'''

    def __init__(self, db_pool):
        self.db_pool = db_pool

    @classmethod
    def from_settings(cls, settings):

        '''1、@classmethod声明一个类方法，而对于平常我们见到的叫做实例方法。
           2、类方法的第一个参数cls（class的缩写，指这个类本身），而实例方法的第一个参数是self，表示该类的一个实例
           3、可以通过类来调用，就像C.f()，相当于java中的静态方法'''
        #读取settings中配置的数据库参数
        db_params = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',  # 编码要加上，否则可能出现中文乱码问题
            cursorclass=DictCursor,
            use_unicode=False,
        )
        db_pool = adbapi.ConnectionPool('MySQLdb', **db_params)  # **表示将字典扩展为关键字参数,相当于host=xxx,db=yyy....
        return cls(db_pool)  # 相当于dbpool付给了这个类，self中可以得到

    # pipeline默认调用
    def process_item(self, item, spider):
        query = self.db_pool.runInteraction(self._conditional_insert, item, spider)  # 调用插入的方法
        query.addErrback(self._handle_error, item, spider)  # 调用异常处理方法
        return item

    # 写入数据库中
    # SQL语句在这里
    def _conditional_insert(self, tx, item, spider):
        for k in ['depth', 'download_timeout', 'download_slot', 'download_latency', 'proxy',
                  'proxy_retries', 'cookie_retries', 'redirect_times',
                  'redirect_urls', 'redirect_ttl', 'confirm_exit']:
            if k in item:
                item.pop(k)

        sql = 'REPLACE into {}(id, keyword, data) values (%s, %s, %s)'.format(spider.name)
        tx.execute(sql, (item['id'], item['keyword'], json.dumps(item)))

    # 错误处理方法
    def _handle_error(self, failue, item, spider):
        print(failue)
