from .SpiderMain import SpiderMain
from spider.spiders.config import *
import asyncio
from spider.util.decrypt import decrypts
import json
import datetime as dt
import math
import time


# 公司资质
class QualificationSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            for comp_id in fen_list:
                qualification_info_url = []
                if self.__findOneID__(idx=comp_id, rediskey='QualificationInfoID'):
                    print(comp_id, 'comp info had spiders')
                else:
                    # 最大页数
                    comp = str(comp_id).split("_")[0]
                    url = QUALIFICATIONAPI + comp + "&pg=0&pgsz=15&total=0"
                    json_page = self.__getMaxPage__(url)
                    max_page = math.ceil(int(json_page[0]['data']['pageList']['total']) / 15)  # 向上取整

                    if int(json_page[0]['data']['pageList']['total']) == 0:
                        self.__saveOneID__(idx=comp_id, rediskey='QualificationInfoIDIsNull')
                    # if int(json_page[0]['data']['pageList']['total']) <= 15 and max_page == 1:
                    #     self.__save_qualification_data__(json_page, comp_id)
                    if max_page >= 1:
                        for page in range(0, max_page):
                            url = QUALIFICATIONAPI + comp + "&pg=" + str(page) + "&pgsz=15"
                            qualification_info_url.append(url)
                    if len(qualification_info_url) > 0:
                        self.__asyncSpider__(qualification_info_url, comp_id)

    # 保存数据
    async def __saveJsonData__(self, data=None, comp_id=None):
        try:
            date = dt.datetime.now().date()
            for data_jsons in data:
                if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
                   '4f4019b64d9a050272bd5914634d030aab69' in data_jsons:
                    continue
                res = decrypts(data_jsons)
                res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                data_json = json.loads(res_json)
                if data_json[0]['data'] is not None:
                    qualification_list = data_json[0]['data']['pageList']['list']
                    for qualification in qualification_list:
                        name = qualification['APT_NAME']
                        qualification_type = qualification['APT_TYPE_NAME']
                        certification_code = qualification['APT_CERTNO']
                        certification_date = qualification['APT_GET_DATE']
                        validity_term = qualification['APT_EDATE']
                        certification_authority = qualification['APT_GRANT_UNIT']
                        time_get = time.localtime(int(certification_date) / 1000)
                        certification_date = time.strftime("%Y-%m-%d", time_get)
                        time_end = time.localtime(int(validity_term) / 1000)
                        validity_term = time.strftime("%Y-%m-%d", time_end)
                        item = dict(
                            insert_time=date,  # 获取时间
                            company_ID=comp_id,  # 建设部企业ID
                            type=qualification_type,  # '资质类别'
                            certification_code=certification_code,  # '资质证书号'
                            name=name,  # '资质名称'
                            certification_date=certification_date,  # '发证日期'
                            validity_term=validity_term,  # '证书有效期'
                            certification_authority=certification_authority,  # '发证机关'
                        )
                        if self.__saveOneData__(table_name='EnterpriseQualification', data=item):
                            self.__saveOneID__(idx=comp_id, rediskey='QualificationInfoID')
        except Exception as e:
            print(e)
