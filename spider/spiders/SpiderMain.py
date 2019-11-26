from aiohttp import ClientSession
import aiohttp, asyncio
from spider.util.getHeaders import getToken
from spider.util.decrypt import decrypts
from spider.db.Mysql_db import MySQLClient
from spider.db.Redis_db import RedisClient
from spider.spiders.config import *
import requests
import json

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Host': 'jzsc.mohurd.gov.cn',
    'Upgrade-Insecure-Requests': '1',
    'accessToken': '',
    'Connection':'close',
}


class SpiderMain(object):

    def __init__(self):
        self._redis = RedisClient()
        self._mysql = MySQLClient()
        self._HEADERS = HEADERS

    async def get_one_page(self, url):
        try:
            async with asyncio.Semaphore(MAX_ID):
                async with ClientSession() as session:
                    async with session.get(url, headers=self._HEADERS, timeout=15) as r:
                        # res = decrypts(r.text)
                        return await r.text()
        except Exception as e:
            print('请求异常： ' + str(e))
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
                    access_token = getToken()
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
            response = requests.get(url, headers=self._HEADERS, verify=False, timeout=15)
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
            return data_json
        except Exception as e:
            print(e)
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

    def run(self, data_list):
        for data in data_list:
            self.__asyncSpider__(list_id=data)
        self.__closeMysql__()
