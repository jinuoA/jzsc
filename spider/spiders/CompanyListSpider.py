from .SpiderMain import SpiderMain
import json
from urllib import parse
from spider.spiders.config import *
from spider.util.decrypt import decrypts
import datetime as dt
import time

"""
抓取公司列表信息
"""


class CompanyListSpider(SpiderMain):

    def runs(self, list_id):
        try:
            urls = []
            company_name = parse.quote(list_id)
            # 公司信息的url
            urls.append(MAINURL + company_name + "&pg=0&pgsz=15&total=0")
            self.__asyncSpider__(urls, '')
        except Exception as e:
            print(e)

    # 保存数据
    async def __saveJsonData__(self, data=None, comp_id=None):
        date = dt.datetime.now().date()
        for data_jsons in data:
            if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
               '4f4019b64d9a050272bd5914634d030aab69' in data_jsons:
                continue
            res = decrypts(data_jsons)
            res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
            data_json = json.loads(res_json)
            if data_json[0]['data']['total'] == 0:
                continue
            now = int(time.time())
            comp_json = data_json[0]['data']['list'][0]
            comp_id = comp_json['QY_ID'] + '_' + str(now)
            company_name = comp_json['QY_NAME']
            enterprise_code = comp_json['QY_ORG_CODE']
            country = comp_json['QY_REGION_NAME']
            legal = comp_json['QY_FR_NAME']
            corp_code = comp_json['OLD_CODE']
            # 把公司信息存入数据库
            item = dict(
                insert_time=date,  # 获取时间
                company_ID=comp_id,  # '建设部企业ID'
                company_name=company_name,  # '企业名称'
                enterprise_code=enterprise_code,  # '统一社会信用代码'
                corp_code=corp_code,  # 组织机构代码
                legal=legal,  # '法人代表'
                country=country,  # '企业注册属地'
            )
            # 把公司url 标示存入 redis
            if self.__saveOneData__(table_name='BuildCompanyInfo', data=item):
                self.__saveOneID__(idx=comp_id, rediskey='CompInfoID')
                self.__saveOneID__(idx=company_name, rediskey='CompName')
