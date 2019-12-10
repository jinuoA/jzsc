from .SpiderMain import SpiderMain
from spider.spiders.config import *
import asyncio
import datetime as dt
from spider.util.decrypt import decrypts
import json
import math
import time


# 保存公司详情信息
class CompanyInfoSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            for data in fen_list:
                company_info_url = []
                if self.__findOneID__(idx=data, rediskey='CompInfoID'):
                    print(data, 'comp info had spiders')
                else:
                    comp_id = str(data).split("_")[0]
                    url = COMPINFOAPI + comp_id + "&pg=0&pgsz=15&total=0"
                    company_info_url.append(url)
                self.__asyncSpider__(company_info_url, data)

    # 保存数据
    async def __saveJsonData__(self, data=None, comp_id=None):
        try:
            if len(data) > 0:
                date = dt.datetime.now().date()
                for data_jsons in data:
                    if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
                       '4f4019b64d9a050272bd5914634d030aab69' in data_jsons:
                        continue
                    res = decrypts(data_jsons)
                    res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                    data_json = json.loads(res_json)
                    if data_json[0]['data'] is not None:
                        # company_id = data_json[0]['data']['compMap']['QY_ID']+'_'+str(now)
                        enterprise_code = data_json[0]['data']['compMap']['QY_ORG_CODE']
                        company_name = data_json[0]['data']['compMap']['QY_NAME']
                        country = data_json[0]['data']['compMap']['QY_REGION_NAME']
                        address = data_json[0]['data']['compMap']['QY_ADDR']
                        company_type = data_json[0]['data']['compMap']['QY_JJXZ']
                        legal = data_json[0]['data']['compMap']['QY_FR_NAME']
                        corp_code = data_json[0]['data']['compMap']['OLD_CODE']
                        telephone = data_json[0]['data']['compMap']['QY_OFFICE_TEL']
                        license_code = data_json[0]['data']['compMap']['QY_YYZZH']
                        # 把公司信息存入数据库
                        item = dict(
                            insert_time=date,  # 获取时间
                            company_ID=comp_id,  # '建设部企业ID'
                            company_name=company_name,  # '企业名称'
                            enterprise_code=enterprise_code,  # '统一社会信用代码'
                            corp_code=corp_code,  # 组织机构代码
                            license_code=license_code,  # 营业执照号
                            legal=legal,  # '法人代表'
                            company_type=company_type,  # '企业类型'
                            country=country,  # '企业注册属地'
                            address=address,  # '企业经营地址'
                            telephone=telephone,
                        )
                        if self.__saveOneData__(table_name='BuildCompanyInfo', data=item):
                            self.__saveOneID__(idx=comp_id, rediskey='CompInfoID')
                    else:
                        self.__saveOneID__(idx=company_name, rediskey="ComInfoIDisNull")  # 把无详情的公司存入redis中
        except Exception as e:
            print(e)
