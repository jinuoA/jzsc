from .SpiderMain import SpiderMain
import math
from spider.spiders.config import *
import asyncio
from spider.util.decrypt import decrypts
import json
import datetime as dt


# 人员信息
class PersonSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for data in new_list:
            for comp_id in data:
                person_list_url = []
                if self.__findOneID__(idx=comp_id, rediskey='PersonInfoID'):
                    print(comp_id, 'project had spiders.')
                elif self.__findOneID__(idx=comp_id, rediskey='TempPersonID'):
                    print(comp_id, 'project is spiders.')
                elif self.__findOneID__(idx=comp_id, rediskey='TempPersonIDIsNull'):
                    print(comp_id, 'project is null')
                else:
                    # 最大页数
                    comp = str(comp_id).split("_")[0]
                    url = STAFFAPI + comp + '&pg=0&pgsz=15'
                    json_page = self.__getMaxPage__(url)
                    max_page = math.ceil(int(json_page[0]['data']['pageList']['total']) / 15)  # 向上取整
                    if int(json_page[0]['data']['pageList']['total']) == 0:
                        self.__saveOneID__(idx=comp_id, rediskey='TempPersonListIDIsNull')
                    if max_page >= 1:
                        for page in range(0, max_page):
                            url = STAFFAPI + comp + "&pg=" + str(page) + "&pgsz=15"
                            person_list_url.append(url)
                    if len(person_list_url) > 0:
                        self.__asyncSpider__(person_list_url, comp_id)

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
                    person_list = data_json[0]['data']['pageList']['list']
                    for person in person_list:
                        person_ID = person["RY_ID"]
                        name = person['RY_NAME']
                        cid = person['RY_CARDNO']
                        compile_data = person['REG_PROF_NAME']
                        regist_type = person['REG_TYPE_NAME']
                        regist_code = person['REG_SEAL_CODE']
                        item = dict(
                            company_ID=comp_id,  # 企业ID
                            insert_time=date,  # 获取时间
                            person_ID=person_ID,  # 人员ID
                            name=name,  # 姓名
                            cid=cid,  # 身份证号
                            regist_type=regist_type,  # 注册类别
                            regist_code=regist_code,  # 注册号（执业印章号）
                            registered_major=compile_data,  # 注册专业
                        )

                        if self.__saveOneData__(table_name='personInfo', data=item):
                            self.__saveOneID__(idx=comp_id, rediskey='PersonInfoID')
        except Exception as e:
            print(e)
