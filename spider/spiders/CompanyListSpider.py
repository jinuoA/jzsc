from .SpiderMain import SpiderMain
import grequests
import json
from urllib import parse
from spider.spiders.config import *
import aiohttp, asyncio
from aiohttp import ClientSession
from spider.util.decrypt import decrypts
import datetime as dt
import time

"""
抓取公司列表信息
"""


class CompanyListSpider(SpiderMain):

    def runs(self, list_id):
        try:
            list_urls = [list_id[x:x + 5] for x in range(0, len(list_id), 5)]
            for list_url in list_urls:
                urls = []
                for name in list_url:
                    if self.__findOneID__(idx=name[1], rediskey='CompName'):
                        print(name[1], ' comp info is spiders ')
                    else:
                        company_name = parse.quote(name[1])
                        # 公司信息的url
                        urls.append(MAINURL + company_name + "&pg=0&pgsz=15&total=0")
                self.__asyncSpider__(urls, '')



                # req = (grequests.get(u) for u in urls)
                # resp = grequests.imap(req, size=COMPANY_MAX)  # 5个并发
                # for i in resp:
                #     res = decrypts(i.text)
                #     res = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                #     data_json = json.loads(res)
                #


        except Exception as e:
            print(e)

    # 保存数据
    async def __saveJsonData__(self, data=None, comp_id=None):
        for data_jsons in data:
            if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
               '4f4019b64d9a050272bd5914634d030aab69' in data_jsons:
                continue

            res = decrypts(data_jsons)
            res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
            data_json = json.loads(res_json)
            if data_json[0]['data']['total'] == 0:
                continue
            comp_id = data_json[0]['data']['list'][0]['QY_ID']
            company_name = data_json[0]['data']['list'][0]['QY_NAME']
            # 判断是否存在
            if self.__findOneID__(idx=company_name, rediskey='CompName'):
                print(company_name, ' comp info is spiders')
            else:
                # 把公司url 标示存入 redis
                self.__saveOneID__(idx=comp_id, rediskey='TempCompInfoID')
                print(comp_id, ' save in TmepcompInfoID')
                self.__saveOneID__(idx=company_name, rediskey='CompName')
