# -*- coding:utf-8 -*-
import re
import time
import logging
import requests

from wine_project.libs.proxy import MoguProxy

try:
    import simplejson as json
except ImportError:
    import json

logger = logging.getLogger(__name__)
STEP_2_GEN_VISITOR_URL = "https://passport.weibo.com/visitor/genvisitor"
STEP_2_VISITOR_URL = "https://passport.weibo.com/visitor/visitor"

STEP_1_PTN = re.compile('gen_callback\(({\S+})\);')
STEP_2_PTN = re.compile('cross_domain\(({.*?})\);', re.DOTALL)


class GetCookieResponseError(Exception):
    pass


class WeiboCookie:

    def __init__(self, max_retry=3):
        self.max_retry = max_retry
        self.session = requests.Session()
        self.mogu_proxy = MoguProxy()

    def get_cookie(self):
        retries = 0
        while retries < self.max_retry:
            try:
                self.passprot_url = 'https://passport.weibo.com/visitor/visitor?entry=miniblog&a=enter&url=https%3A%2F%2Fweibo.com%2Fp%2F1005052252493910%2Finfo%3Fmod%3Dpedit&domain=.weibo.com&ua=php-sso_sdk_client-0.6.28&_rand={}'.format(int(time.time()*1000))
                p = self.mogu_proxy.get()
                proxies = {'http': p, 'https': p}
                d = self._post_gen_visitor(proxies)
                tid = d['data']['tid']
                w = '3' if d['data']['new_tid'] else '2'
                c = '100'
                cookie_str = self._get_visitor(tid, w, c, proxies)
                time.sleep(2)
                return cookie_str
            except Exception:
                retries += 1

    def _post_gen_visitor(self, proxies):
        """
        'window.gen_callback && gen_callback({"retcode":20000000,"msg":"succ","data":{"tid":"GQ7+cDmhyci3G+Yae9\\/+raGl6fUCITGIVGXx8T16Rqg=","new_tid":true}});'
        :return:
        """
        headers = {
            'Accept': '*/*',
            'Origin': 'https://passport.weibo.com',
            'Referer': self.passprot_url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
        }
        params = {
            'cb': 'gen_callback',
            'fp': '{"os": "1", "browser": "Chrome73,0,3683,86", "fonts": "undefined", "screenInfo": "1366*768*24", "plugins": "Portable Document Format::internal-pdf-viewer::Chrome PDF Plugin|::mhjfbmdgcfjbbpaeojofohoefgiehjai::Chrome PDF Viewer|::internal-nacl-plugin::Native Client"}'
        }

        resp = self.session.get(STEP_2_GEN_VISITOR_URL, headers=headers, params=params, proxies=proxies)
        matcher = STEP_1_PTN.findall(resp.text)
        if not matcher:
            raise GetCookieResponseError
        return json.loads(matcher[0])

    def _get_visitor(self, tid, w, c, proxies):
        """
        :param tid:
        :param w:
        :param c:
        :return:cross_domain({"retcode":20000000,"msg":"succ","data":{"sub":"_2AkMr7d","subp":"003Jbxhhal"}});
        """
        params = {
            'a': 'incarnate',
            't': tid,
            'w': w,
            'c': c,
            'gc': None,
            'cb': 'cross_domain',
            'from': 'weibo',
            '_rand': '0.99{}'.format(int(time.time()*1000))
        }
        headers = {
            'Accept': '*/*',
            'Origin': 'https://passport.weibo.com',
            'Host': 'passport.weibo.com',
            'Cookie': 'tid={}__0{}'.format(tid, c),
            'Referer': self.passprot_url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
        }
        resp = self.session.get(STEP_2_VISITOR_URL, headers=headers, params=params, proxies=proxies)
        matcher = STEP_2_PTN.findall(resp.text)
        if not matcher:
            raise GetCookieResponseError
        json_data = json.loads(matcher[0])
        sub = json_data['data']['sub']
        subp = json_data['data']['subp']
        print('gene cookie success:{}'.format(sub))
        return {'TC-Page-G0': '', 'SUB': sub, 'SUBP': subp}


def get_profile_page(cookie):
    url = 'https://weibo.com/p/1005052252493910/info?mod=pedit'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': ';'.join(['{}={}'.format(k, v) for k, v in cookie.items()]),
        'Referer': '',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
    }
    resp = requests.get(url, headers=headers, allow_redirects=False)
    print(resp.text)


if __name__ == '__main__':
    cookie = WeiboCookie().get_cookie()
    get_profile_page(cookie)
