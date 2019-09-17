# -*- coding:utf-8 -*-
import re
from datetime import datetime
from datetime import timedelta

#
# 工具方法
#

MINUTE_PTN = re.compile('(\d+)分钟前')
HOUR_PTN = re.compile('(\d+)小时前')
DAY_PTN = re.compile('昨天 (\d+):(\d+)')
MOUTH_PTN = re.compile('\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
YEAR_PTN = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

html_ptn = re.compile('<.+?>', re.DOTALL)


def clean_html_tag(text, default=''):
    """简单的清除html标签"""
    if not text:
        return default
    try:
        return html_ptn.sub('', text)
    except Exception as e:
        return default


def parse_create_at(create_at):
    """ 规则化微博的创建时间
    是今年：
        是今天：
            1分钟内：
                刚刚
            1小时内：
                xx分钟前
            其他：
                xx小时前
        是昨天：
            昨天 HH:mm
        其他：
            MM-dd HH:mm:ss
    不是今年：
        yyyy-MM-dd HH:mm

    :param create_at: 创建时间
    :return: Datetime
    """
    now = datetime.now()
    if create_at == '刚刚':
        result = now - timedelta(seconds=60)
    elif MINUTE_PTN.match(create_at):
        result = now - timedelta(minutes=int(MINUTE_PTN.findall(create_at)[0]))
    elif HOUR_PTN.match(create_at):
        result = now - timedelta(hours=int(HOUR_PTN.findall(create_at)[0]))
    elif DAY_PTN.match(create_at):
        yesterday = now - timedelta(days=1)
        matcher = DAY_PTN.findall(create_at)[0]
        result = yesterday.replace(hour=int(matcher[0]), minute=int(matcher[1]))
    elif YEAR_PTN.match(create_at):
        result = datetime.strptime(create_at, '%Y-%m-%d %H:%M:%S')
    elif MOUTH_PTN.match(create_at):
        result = datetime.strptime(create_at, '%m-%d %H:%M:%S')
    else:
        result = now
    return result.strftime('%Y-%m-%d %H:%M:%S')
