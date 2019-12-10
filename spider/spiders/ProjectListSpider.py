from .SpiderMain import SpiderMain
import math
from spider.spiders.config import *
from spider.util.decrypt import decrypts
import json
import datetime as dt
"""
抓取项目信息
"""


class ProjectListSpider(SpiderMain):

    def run(self, list_id):

        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for data in new_list:
            for comp_id in data:
                project_list_url = []
                if self.__findOneID__(idx=comp_id, rediskey='ProjectListID'):
                    print(comp_id, 'project had spiders.')
                elif self.__findOneID__(idx=comp_id, rediskey='TempProjectListID'):
                    print(comp_id, 'project is spiders.')
                elif self.__findOneID__(idx=comp_id, rediskey='TempProjectListIDIsNull'):
                    print(comp_id, 'project is null')
                else:
                    # 最大页数
                    comp_id_url = str(comp_id).split("_")[0]
                    url = MAX_PAGE + comp_id_url
                    json_page = self.__getMaxPage__(url)
                    max_page = math.ceil(int(json_page[0]['data']['proTotal']) / 15)  # 向上取整
                    if int(json_page[0]['data']['proTotal']) == 0:
                        self.__saveOneID__(idx=comp_id, rediskey='TempProjectListIDIsNull')
                    if max_page >= 1:
                        for page in range(0, max_page):
                            # 这里可能会有问题
                            url = PROJECTLISTAPI + comp_id_url + "&pg=" + str(page) + "&pgsz=15&total=0"
                            project_list_url.append(url)
                    if len(project_list_url) > 0:
                        self.__asyncSpider__(project_list_url, comp_id)

    # 保存数据 redis
    async def __saveJsonData__(self, data=None, comp_id=None):
        try:
            date = dt.datetime.now().date()
            for data_jsons in data:
                res = decrypts(data_jsons)
                res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                data_json = json.loads(res_json)
                if data_json[0]['data'] is not None:
                    projcet_list = data_json[0]['data']['list']
                    for project in projcet_list:
                        project_num = str(project['PRJNUM']) + "_" + comp_id
                        project_ID = project["PRJNUM"]
                        project_name = project["PRJNAME"]
                        if project['COUNTY'] is None and project['CITY'] is not None:
                            project_region = project['PROVINCE'] + '-' + project['CITY']
                        elif project['CITY'] is None:
                            project_region = project['PROVINCE']
                        else:
                            project_region = project['PROVINCE'] + '-' + project['CITY'] + '-' + project['COUNTY']
                        build_company = project['BUILDCORPNAME']
                        project_type = project['PRJTYPENUM']
                        item = dict(
                            company_ID=comp_id,  # '建设部企业ID'
                            insert_time=date,  # 获取时间
                            project_name=project_name,  # 项目名称
                            project_ID=project_ID,  # 项目编号
                            project_region=project_region,  # 所在区划
                            build_company=build_company,  # 建设单位
                            project_type=project_type,  # 项目分类
                        )
                        if self.__saveOneData__(table_name='ProjectInfo', data=item):
                            self.__saveOneID__(idx=project_ID, rediskey='ProjectID')
                            self.__saveOneID__(idx=project_num, rediskey='TempTenderListID')
                            self.__saveOneID__(idx=project_num, rediskey='TempContractListID')
                            self.__saveOneID__(idx=project_num, rediskey='TempProFinishListID')
                            self.__saveOneID__(idx=project_num, rediskey='TempProCensorListID')
                            self.__saveOneID__(idx=project_num, rediskey='TempProjectCorpInfoID')
                            self.__saveOneID__(idx=project_num, rediskey='TempBuildLicenceListID')

        except Exception as e:
            print(e)
