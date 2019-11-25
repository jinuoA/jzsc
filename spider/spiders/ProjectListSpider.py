from .SpiderMain import SpiderMain
import math
from spider.spiders.config import *
import asyncio
from spider.util.decrypt import decrypts
import json
import time
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
            now = int(time.time())
            for data_jsons in data:
                if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
                   '4f4019b64d9a050272bd5914634d030aab69' in data_jsons:
                    continue
                res = decrypts(data_jsons)
                res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                data_json = json.loads(res_json)
                if data_json[0]['data'] is not None:
                    projcet_list = data_json[0]['data']['list']
                    for project in projcet_list:
                        projcet_id = str(project["ID"]) + "_" + comp_id
                        project_num = str(project['PRJNUM']) + "_" + comp_id
                        self.__saveOneID__(idx=projcet_id, rediskey='TempProjectListID')
                        self.__saveOneID__(idx=project_num, rediskey='TempTenderListID')
                        self.__saveOneID__(idx=project_num, rediskey='TempContractListID')
                        self.__saveOneID__(idx=project_num, rediskey='TempProFinishListID')
                        self.__saveOneID__(idx=project_num, rediskey='TempProCensorListID')
                        self.__saveOneID__(idx=project_num, rediskey='TempBuildLicenceListID')

        except Exception as e:
            print(e)
