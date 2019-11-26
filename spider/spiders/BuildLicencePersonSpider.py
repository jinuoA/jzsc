from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
from spider.util.decrypt import decrypts
import json
import time

"""
施工许可人员
"""


class BuildLicencePersonSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            for data in fen_list:
                licence_info_url = []
                licence_id = str(data).split("_", 1)[0]
                comp_id = str(data).split("_", 1)[1]
                if self.__findOneID__(idx=licence_id, rediskey='BuildLicencePersonID'):
                    print(licence_id, 'buildLicencePerson info had spiders')
                else:
                    url = LICENCEMANAGEINFOPERSON + licence_id + "&pg=0&pgsz=15&total=0"
                    licence_info_url.append(url)
                self.__asyncSpider__(licence_info_url, comp_id)

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
                datas = data_json[0]['data']['list']
                if len(datas) > 0:
                    for data in datas:
                        if len(data) > 0:
                            buildliseID = data['BUILDERLICENCENUM']
                            user_id = data['USERID']
                            cid = data['IDCARD']
                            user_name = data['USERNAME']
                            company_name = data['CORPNAME']
                            company_type = data['CORPROLE']
                            per_type = data['PERTYPE']
                            company_id = comp_id
                            item = dict(
                                insert_time=date,  # 插入时间
                                construction_permit_ID=buildliseID,  # 施工许可信息ID
                                user_id=user_id,  # 人员id
                                cid=cid,  # 身份证
                                user_name=user_name,  # 人员姓名
                                company_name=company_name,  # 公司名称
                                company_type=company_type,  # 公司类型
                                per_type=per_type,  # 人员类型
                                company_id=company_id,  # 公司id

                            )
                            value = str(buildliseID) + '_' + comp_id
                            if self.__saveOneData__(table_name='ConstructionPermitPerson', data=item):
                                self.__saveOneID__(idx=value, rediskey='BuildLicencePersonID')
        except Exception as e:
            print(e)
