from .SpiderMain import SpiderMain
from spider.spiders.config import *
from spider.util.decrypt import decrypts
import json
import time
import datetime as dt


class TenderListSpider(SpiderMain):

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
                        terderID = dataJson['ID']
                        bidder_name = dataJson['TENDERCORPNAME']
                        winningbid_ID = dataJson['TENDERNUM']
                        project_ID = dataJson['PRJNUM']
                        province_winningbid_ID = dataJson['PROVINCETENDERNUM']
                        tender_type = dataJson['TENDERCLASSNUM']
                        tender_method = dataJson['TENDERTYPENUM']
                        winningbid_date = dataJson['TENDERRESULTDATE']  # '中标日期'
                        winningbid_money = dataJson['TENDERMONEY']
                        if winningbid_date is not None and int(winningbid_date) < 0:
                            winningbid_date = None
                        if winningbid_date is not None:
                            time_get = time.localtime(int(winningbid_date) / 1000)
                            winningbid_date = time.strftime("%Y-%m-%d", time_get)
                        item = dict(
                            company_ID=comp,  # '建设部企业ID'
                            insert_time=date,  # 获取时间
                            tender_ID=terderID,  # 招投标信息ID
                            project_ID=project_ID,  # '项目编号'
                            winningbid_ID=winningbid_ID,  # '中标通知书编号'
                            province_winningbid_ID=province_winningbid_ID,  # '省级中标通知书编号'
                            tender_type=tender_type,  # '招标类型'
                            tender_method=tender_method,  # '招标方式'
                            winningbid_date=winningbid_date,  # '中标日期'
                            winningbid_money=winningbid_money,  # '中标金额'
                            bidder_name=bidder_name,  # '中标单位名称'
                        )
                        if self.__saveOneData__(table_name='BiddingInfo', data=item):
                            self.__saveOneID__(idx=terderID, rediskey='TenderInfoID')
        except Exception as e:
            print(e)
