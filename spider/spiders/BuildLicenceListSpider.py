from .SpiderMain import SpiderMain
from spider.spiders.config import *
from spider.util.decrypt import decrypts
import json
import time
import datetime as dt


class BuildLicenceListSpider(SpiderMain):

    def run(self, list_id, main_url, key):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for data in new_list:
            for project_id in data:
                list_url = []
                for main in main_url:
                    comp_id = project_id
                    project_id = str(project_id).split("_", 1)[0]
                    url = main + project_id + "&pg=0&pgsz=15&total=0"
                    list_url.append(url)
                if len(list_url) > 0:
                    temp = key + '_' + comp_id
                    self.__asyncSpider__(list_url, temp)

    # 保存数据 redis
    async def __saveJsonData__(self, data=None, comp_id=None):
        try:
            date = dt.datetime.now().date()
            for data_jsons in data:
                res = decrypts(data_jsons)
                res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                data_json = json.loads(res_json)
                aa = data_json[0]['data']['list']
                if len(aa) > 0 and aa != '':
                    comp = str(comp_id).split("_", 1)[1].split("_", 1)[1]
                    for dataJson in aa:
                        buildliseID = dataJson['ID']
                        project_ID = dataJson['PRJNUM']
                        province_permit_code = dataJson['PROVINCEBUILDERLICENCENUM']
                        permit_code = dataJson['BUILDERLICENCENUM']
                        contract_money = dataJson['CONTRACTMONEY']
                        area = dataJson['AREA']
                        release_time = dataJson['RELEASEDATE']  # '发证日期'
                        if release_time is not None and int(release_time) < 0:
                            release_time = None
                        if release_time is not None:
                            time_get = time.localtime(int(release_time) / 1000)
                            release_time = time.strftime("%Y-%m-%d", time_get)
                        item = dict(
                            company_ID=comp,  # '建设部企业ID'
                            insert_time=date,  # 获取时间
                            construction_permit_ID=buildliseID,  # 施工许可信息ID
                            project_ID=project_ID,  # 项目编号
                            permit_code=permit_code,  # 施工许可证编号
                            province_permit_code=province_permit_code,  # 省级施工许可证编号
                            contract_money=contract_money,  # 合同金额
                            area=area,  # 面积
                            release_time=release_time,  # 发证日期

                        )
                        value = str(buildliseID) + '_' + comp
                        if self.__saveOneData__(table_name='ConstructionPermit', data=item):
                            self.__saveOneID__(idx=value, rediskey='BuildLicenceInfoID')
        except Exception as e:
            print(e)
