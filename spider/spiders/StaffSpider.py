from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
import json
from spider.util.decrypt import decrypts
import math
import time

"""
通过人员信息，获取公司名称
"""


class StaffSpider(SpiderMain):

    def run(self, list_id):
        try:
            new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
            for cityNo in new_list:
                for city_no in cityNo:
                    for i in range(1955, 2000):
                        for month in MONTHS:

                            cid = city_no[1] + str(i) + month
                            if self.__findOneID__(idx=cid, rediskey='TempStaffInfoID'):
                                print(cid, ' comp info is spiders')
                                continue
                            time.sleep(0.1)
                            staff_list = []
                            url = PERSON_LIST + cid + '&pg=0&pgsz=15&total=0'
                            json_page = self.__getMaxPage__(url)
                            if json_page[0]['data'] is not None:
                                total = json_page[0]['data']['total']
                                if total != 0 and total != 450:
                                    max_page = math.ceil(int(total) / 15)
                                    for page in range(0, max_page):
                                        href = PERSON_LIST + cid + "&pg=" + str(page) + "&pgsz=15&total=" + str(total)
                                        staff_list.append(href)
                                elif total == 0:
                                    self.__saveOneID__(idx=cid, rediskey='TempStaffInfoID')
                                    time.sleep(0.1)
                                    continue
                                elif total == 450:
                                    print("增加查询长度")
                                    for d in DAYS:
                                        staff_list = []
                                        url = PERSON_LIST + cid + d
                                        print(url)
                                        json_day = self.__getMaxPage__(url)
                                        if json_day[0]['data'] is not None:
                                            total = json_day[0]['data']['total']
                                            if total != 0 and total != 450:
                                                max_page = math.ceil(int(total) / 15)
                                                for page in range(0, max_page):
                                                    href = url + "&pg=" + str(page) + "&pgsz=15&total=" + str(total)
                                                    staff_list.append(href)
                                            elif total == 0:
                                                continue
                                            if len(staff_list) > 0:
                                                self.__asyncSpider__(staff_list, cid + d)
                                        self.__saveOneID__(idx=cid + d, rediskey='TempStaffInfoID')
                                if len(staff_list) > 0:
                                    self.__asyncSpider__(staff_list, cid)
                            self.__saveOneID__(idx=cid, rediskey='TempStaffInfoID')
        except Exception as e:
            print(e)

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
                if data_json[0]['data'] is not None and len(data_json) > 0:
                    person_list = data_json[0]['data']['list']
                    for person in person_list:
                        name = person['RY_NAME']
                        company_name = person['REG_QYMC']
                        cid = person['RY_CARDNO']
                        compile_data = person['profs']
                        regist_type = person['REG_TYPE_NAME']
                        regist_code = person['REG_SEAL_CODE']
                        person_ID = person["RY_ID"]
                        item = dict(
                            companyName=company_name
                        )
                        item_persion = dict(
                            insert_time=date,  # 获取时间
                            person_ID=person_ID,  # 人员ID
                            name=name,  # 姓名
                            cid=cid,  # 身份证号
                            company_name=company_name,  # 公司名称
                            regist_type=regist_type,  # 注册类别
                            regist_code=regist_code,  # 注册号（执业印章号）
                            registered_major=compile_data,  # 注册专业
                        )
                        self.__saveOneData__(table_name="personInfo", data=item_persion)
                        if self.__findOneID__(idx=company_name, rediskey='companyName'):
                            pass
                        else:
                            self.__saveOneID__(idx=company_name, rediskey='companyName')
                            self.__saveOneData__(table_name='companyName', data=item)


        except Exception as e:
            print(e)
