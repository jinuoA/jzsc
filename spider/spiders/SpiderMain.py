from aiohttp import ClientSession
import aiohttp, asyncio
from spider.util.getHeaders import getToken
from spider.util.decrypt import decrypts
from spider.db.Mysql_db import MySQLClient
from spider.db.Redis_db import RedisClient
from spider.spiders.config import *
import requests
import json
import time

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Host': 'jzsc.mohurd.gov.cn',
    'Upgrade-Insecure-Requests': '1',
    'accessToken': '',
    'Connection': 'close',
}


class SpiderMain(object):

    def __init__(self):
        self._redis = RedisClient()
        self._mysql = MySQLClient()
        self._HEADERS = HEADERS
        self.ip = None
        self.port = None

    async def get_one_page(self, url):
        try:
            if self.ip is None:
                ip, port = self.__getProxy__()
                self.ip = ip
                self.port = port
            real_proxy = 'http://' + str(self.ip) + ":" + str(self.port)
            async with asyncio.Semaphore(MAX_ID):
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                    async with session.get(url, proxy=real_proxy, headers=self._HEADERS, timeout=15) as r:
                        if r.status == 200 or r.status == 408:
                            return await r.text()
                        else:
                            return await self.get_one_page(url)
        except Exception as e:
            print('请求异常： ' + str(e))
            ip, port = self.__getProxy__()
            self.ip = ip
            self.port = port
            await self.get_one_page(url)

    # 并发爬取
    async def main(self, urls, comp_id=None):
        try:
            # 任务列表
            tasks = [self.get_one_page(url) for url in urls]
            # 并发执行并保存每一个任务的返回结果
            results = await asyncio.gather(*tasks)
            # 返回解析为字典的数据
            if len(results) > 0:
                if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
                   '4f4019b64d9a050272bd5914634d030aab69' in results or results[0] is False:
                    #  获取动态ip 传入
                    ip, port = self.__getProxy__()
                    self.ip = ip
                    self.port = port
                    print("动态 ip 为" + str(ip) + ", 端口：" + str(port))
                    my_proxy = 'http://' + str(ip) + ":" + str(port)
                    access_token = getToken(my_proxy)
                    while access_token is None:
                        access_token = getToken()
                    self._HEADERS = {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'accessToken': access_token}
                    print(access_token)
                    await self.main(urls, comp_id)
                # 保存数据
                await self.__saveJsonData__(data=results, comp_id=comp_id)
        except Exception as e:
            print(e)

    def __getMaxPage__(self, url):
        try:
            if self.ip is None:
                ip, port = self.__getProxy__()
                self.ip = ip
                self.port = port
            proxyMeta = "http://%(host)s:%(port)s" % {
                "host": self.ip,
                "port": self.port,
            }
            proxies = {
                "http": proxyMeta,
            }
            response = requests.get(url, proxies=proxies, headers=self._HEADERS, verify=False, timeout=10)
            if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
               '4f4019b64d9a050272bd5914634d030aab69' in response.text:
                access_token = getToken()
                while access_token is None:
                    access_token = getToken()
                self._HEADERS = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'accessToken': access_token}
                return self.__getMaxPage__(url)
            res = decrypts(response.text)
            res = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
            data_json = json.loads(res)
            if data_json[0]['code'] == 401:
                time.sleep(60)
                return self.__getMaxPage__(url)
            elif data_json[0]['code'] == 200:
                return data_json
            else:
                return self.__getMaxPage__(url)
        except Exception as e:
            print(e)
            ip, port = self.__getProxy__()
            self.ip = ip
            self.port = port
            return self.__getMaxPage__(url)

    def __getID__(self, rediskey=None):
        return self._redis.batch(rediskey=rediskey)

    def __findOneID__(self, idx=None, rediskey=None):
        return self._redis.exists(idx=idx, rediskey=rediskey)

    def __saveOneID__(self, idx=None, rediskey=None, score=None):
        if score is not None:
            self._redis.add(idx=idx, rediskey=rediskey, score=score)
        else:
            self._redis.add(idx=idx, rediskey=rediskey)

    def __saveOneID__(self, idx=None, rediskey=None, score=None):
        if score is not None:
            self._redis.add(idx=idx, rediskey=rediskey, score=score)
        else:
            self._redis.add(idx=idx, rediskey=rediskey)

    def __deleteID__(self, idx=None, rediskey=None):
        return self._redis.deletes(idx=idx, rediskey=rediskey)

    def __saveListID__(self, list_id, rediskey=None):
        for idx in list_id:
            self._redis.add(idx=idx, rediskey=rediskey)

    def __saveOneData__(self, table_name, data):
        print(data)
        return self._mysql.__insertData__(table_name=table_name, data=data)

    def __closeMysql__(self):
        try:
            self._mysql.__closeDB__()
        except Exception as e:
            print('Close Mysql failed!', e)

    def __asyncSpider__(self, list_id=None, comp_id=None):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main(list_id, comp_id))

    """
    获取代理ip
    """
    def __getProxy__(self):
        url = 'http://http.tiqu.qingjuhe.cn/getip?num=1&type=2&pack=42599&port=1&ts=1&lb=1&pb=4&regions='
        response = requests.get(url=url)
        json_str = json.loads(response.text)
        ip = json_str["data"][0]["ip"]
        port = json_str["data"][0]["port"]
        return (ip, port)

    def __getYunProxy__(self):
        url = 'http://gec.ip3366.net/api/?key=20191204153949621&getnum=1&anonymoustype=3&filter=1&area=1&order=2&formats=2'
        response = requests.get(url=url)
        json_str = json.loads(response.text)
        ip = json_str[0]["Ip"]
        port = json_str[0]["Port"]
        return (ip, port)


    def run(self, data_list):
        for data in data_list:
            self.__asyncSpider__(list_id=data)
        self.__closeMysql__()
