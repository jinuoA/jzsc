"""
竣工验收备案信息
"""

from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
from spider.util.decrypt import decrypts
import json
import time

class ProjectFinishSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            for data in fen_list:
                project_finish_url = []
                project_finish_id = str(data).split("_", 1)[0]
                comp_id = str(data).split("_", 1)[1]
                if self.__findOneID__(idx=project_finish_id, rediskey='ProjectFinishInfoID'):
                    print(project_finish_id, 'projectFinish info had spiders')
                else:
                    url = FINISHMANAGEINFO + project_finish_id + "&pg=0&pgsz=15&total=0"
                    project_finish_url.append(url)
                    self.__asyncSpider__(project_finish_url, comp_id)

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
                if len(data) > 0:
                    bafinishID = data['ID']
                    project_ID = data['PRJNUM']
                    completed_code = data['PRJFINISHNUM']
                    province_completed_code = data['PROVINCEPRJFINISHNUM']
                    money = data['FACTCOST']
                    build_scale = data['FACTSIZE']
                    building_system = data['PRJSTRUCTURETYPE']
                    area = data['FACTAREA']
                    build_licence_code = data['BUILDERLICENCENUM']
                    begin_date = data['BDATE']
                    end_date = data['EDATE']
                    recod_time = data['CREATEDATE']
                    if begin_date is not None:
                        time_begin = time.localtime(int(begin_date) / 1000)
                        begin_date = time.strftime("%Y-%m-%d", time_begin)
                    if end_date is not None:
                        time_end = time.localtime(int(end_date) / 1000)
                        end_date = time.strftime("%Y-%m-%d", time_end)
                    if recod_time is not None:
                        time_recod = time.localtime(int(recod_time) / 1000)
                        recod_time = time.strftime("%Y-%m-%d", time_recod)
                    item = dict(
                        company_ID=comp_id,  # '建设部企业ID'
                        insert_time=date,  # 获取时间
                        completed_verification_ID=bafinishID,  # 竣工验收备案信息ID
                        project_ID=project_ID,  # 项目编号
                        completed_code=completed_code,  # 竣工备案编号
                        province_completed_code=province_completed_code,  # 省级竣工备案编号
                        money=money,  # 实际造价
                        area=area,  # 面积
                        build_scale=build_scale,  # 建设规模
                        building_system=building_system,  # 结构体系
                        begin_date=begin_date,  # 实际开工日期
                        end_date=end_date,  # 实际竣工日期
                        recod_time=recod_time,  # 记录登记时间
                        build_licence_code=build_licence_code,
                    )
                    if self.__saveOneData__(table_name='CompletionVerification', data=item):
                        self.__saveOneID__(idx=comp_id, rediskey='ProjectFinishInfoID')
        except Exception as e:
            print(e)
