from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
from spider.util.decrypt import decrypts
import json
import math


class SaveWithinProjectSpider(SpiderMain):

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
                    temp = key+'_' + comp_id
                    self.__asyncSpider__(list_url, temp)

    # 保存数据 redis
    async def __saveJsonData__(self, data=None, comp_id=None):
        try:
            for data_jsons in data:
                if '4bd02be856577e3e61e83b86f51afca55280b5ee9ca16beb9b2a65406045c9497c089d5e8ff97c63000f62b011a6' \
                   '4f4019b64d9a050272bd5914634d030aab69' in data_jsons:
                    continue
                res = decrypts(data_jsons)
                res_json = str(res).replace("'", "").split('success')[0] + 'success":true}' + "]"
                data_json = json.loads(res_json)
                aa = data_json[0]['data']['list']
                print(aa)
                if len(aa) > 0 and aa != '':
                    _list = data_json[0]['data']['list']
                    for project in _list:
                        _id = project["ID"]
                        if str(comp_id).split("_", 1)[0] == 'BuildLicenceListID':
                            _id = project["BUILDERLICENCENUM"]
                        key_del = 'Temp' + str(comp_id).split("_", 1)[0]
                        del_id = str(comp_id).split("_", 1)[1]
                        comp = str(comp_id).split("_", 1)[1].split("_", 1)[1]
                        self.__saveOneID__(idx=str(_id)+"_"+comp, rediskey=str(comp_id).split("_", 1)[0])
                        self.__deleteID__(idx=del_id, rediskey=key_del)
                else:
                    project_id = str(comp_id).split("_", 1)[1]
                    key_null = str(comp_id).split("_", 1)[0] +'isNULL'
                    key_del = 'Temp' + str(comp_id).split("_", 1)[0]
                    self.__saveOneID__(idx=project_id, rediskey=key_null)
                    self.__deleteID__(idx=project_id, rediskey=key_del)
        except Exception as e:
            print(e)
