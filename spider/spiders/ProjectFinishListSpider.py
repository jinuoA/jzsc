from .SpiderMain import SpiderMain
from spider.spiders.config import *
from spider.util.decrypt import decrypts
import json
import time
import datetime as dt


class ProjectFinishListSpider(SpiderMain):

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
                        bafinishID = dataJson['ID']
                        project_ID = dataJson['PRJNUM']
                        completed_code = dataJson['PRJFINISHNUM']
                        province_completed_code = dataJson['PROVINCEPRJFINISHNUM']
                        build_licence_code = dataJson['BUILDERLICENCENUM']
                        money = dataJson['FACTCOST']
                        begin_date = dataJson['BDATE']
                        end_date = dataJson['EDATE']
                        area = dataJson['FACTAREA']
                        build_scale = dataJson['FACTSIZE']
                        building_system = dataJson['PRJSTRUCTURETYPE']
                        recod_time = dataJson['CREATEDATE']
                        if begin_date is not None and int(begin_date) < 0:
                            begin_date = None
                        if end_date is not None and int(end_date) < 0:
                            end_date = None
                        if recod_time is not None and int(recod_time) < 0:
                            recod_time = None
                        if begin_date is not None:
                            time_begin = time.localtime(int(begin_date) / 1000)
                            begin_date = time.strftime("%Y-%m-%d", time_begin)
                        if end_date is not None:
                            time_end = time.localtime(int(end_date) / 1000)
                            end_date = time.strftime("%Y-%m-%d", time_end)
                        if recod_time is not None:
                            time_recod = time.localtime(int(recod_time) / 1000)
                            recod_time = time.strftime("%Y-%m-%d", time_recod)
                        if bafinishID is None:
                            bafinishID = ''
                        item = dict(
                            company_ID=comp,  # '建设部企业ID'
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
                            self.__saveOneID__(idx=bafinishID, rediskey='ProjectFinishInfoID')
        except Exception as e:
            print(e)
