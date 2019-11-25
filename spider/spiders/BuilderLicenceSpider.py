"""
施工许可
"""

from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
from spider.util.decrypt import decrypts
import json
import time


class BuildLicenceSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            for data in fen_list:
                licence_info_url = []
                licence_id = str(data).split("_", 1)[0]
                comp_id = str(data).split("_", 1)[1]
                if self.__findOneID__(idx=licence_id, rediskey='BuildLicenceInfoID'):
                    print(licence_id, 'buildLicence info had spiders')
                else:
                    url = LICENCEMANAGEINFO + licence_id + "&pg=0&pgsz=15&total=0"
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
                data = data_json[0]['data']
                if data is not None:
                    # comp_id = data['TENDERCORPID']
                    buildliseID = comp_id
                    project_ID = data['PRJNUM']
                    province_permit_code = data['PROVINCEPRJNUM']
                    permit_code = data['BUILDERLICENCENUM']
                    check_plans_code = data['CENSORNUM']
                    contract_money = data['CONTRACTMONEY']
                    area = data['AREA']
                    release_time = data['RELEASEDATE']  # '发证日期'
                    recod_time = data['CREATEDATE']
                    time_get = time.localtime(int(release_time) / 1000)
                    release_time = time.strftime("%Y-%m-%d", time_get)
                    time_end = time.localtime(int(recod_time) / 1000)
                    recod_time = time.strftime("%Y-%m-%d", time_end)
                    item = dict(
                        company_ID=comp_id,  # '建设部企业ID'
                        insert_time=date,  # 获取时间
                        construction_permit_ID=buildliseID,  # 施工许可信息ID
                        project_ID=project_ID,  # 项目编号
                        permit_code=permit_code,  # 施工许可证编号
                        province_permit_code=province_permit_code,  # 省级施工许可证编号
                        check_plans_code=check_plans_code,  # 施工图审查合格证书编号
                        contract_money=contract_money,  # 合同金额
                        area=area,  # 面积
                        recod_time=recod_time,  # 记录登记时间
                        release_time=release_time,

                    )
                    if self.__saveOneData__(table_name='ConstructionPermit', data=item):
                        self.__saveOneID__(idx=comp_id, rediskey='BuildLicenceInfoID')
        except Exception as e:
            print(e)
