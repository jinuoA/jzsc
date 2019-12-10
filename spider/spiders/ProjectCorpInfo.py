from .SpiderMain import SpiderMain
from spider.spiders.config import *
from spider.util.decrypt import decrypts
import json
import time
import datetime as dt


class ProjectCorpInfo(SpiderMain):

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
                if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
                   '4f4019b64d9a050272bd5914634d030aab69' in data_jsons:
                    continue
                res = decrypts(data_jsons)
                res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                data_json = json.loads(res_json)
                if data_json[0]['data'] is not None:
                    datas = data_json[0]['data']['list']
                    comp = str(comp_id).split("_", 1)[1].split("_", 1)[1]
                    for data in datas:
                        if len(data) > 0:
                            user_name = data['PERSONNAME']
                            project_id = str(data['PRJNUM'])
                            cid = data['PERSONIDCARD']
                            company_name = data['CORPNAME']
                            company_type = data['CORPROLENUM']
                            user_id = data['PERSONID']
                            corpcode = data['CORPCODE']
                            per_type = data['IDCARDTYPENUM']
                            company_id = comp
                            buildliseID = data['CORPID']
                            if buildliseID is None:
                                buildliseID = ''
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
                                corpcode=corpcode,
                                project_id=project_id,
                            )
                            if self.__saveOneData__(table_name='ConstructionPermitPerson', data=item):
                                self.__saveOneID__(idx=buildliseID, rediskey='ProjectCorpInfoID')
        except Exception as e:
            print(e)
