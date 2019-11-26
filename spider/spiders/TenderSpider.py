"""
招投标信息
"""

from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
from spider.util.decrypt import decrypts
import json
import time


class TenderSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            for data in fen_list:
                tender_info_url = []
                tender_id = str(data).split("_", 1)[0]
                comp_id = str(data).split("_", 1)[1]
                if self.__findOneID__(idx=tender_id, rediskey='TenderInfoID'):
                    print(tender_id, 'tender info had spiders')
                else:
                    url = TENDERINFOAPI + tender_id + "&pg=0&pgsz=15&total=0"
                    tender_info_url.append(url)
                    self.__asyncSpider__(tender_info_url, comp_id)

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
                    terderID = data['ID']
                    project_ID = data['PRJNUM']
                    winningbid_ID = data['TENDERNUM']
                    province_winningbid_ID = data['PROVINCETENDERNUM']
                    tender_type = data['TENDERCLASSNUM']
                    tender_method = data['TENDERTYPENUM']
                    winningbid_date = data['TENDERRESULTDATE']  # '中标日期'
                    winningbid_money = data['TENDERMONEY']
                    build_scale = ''  # '建设规模'
                    area = data['AREA']
                    bidding_agent = data['AGENCYCORPNAME']
                    bidder_name = data['TENDERCORPNAME']
                    bidder_corp_code = data['TENDERCORPCODE']
                    manager_name = data['CONSTRUCTORNAME']
                    manager_cid = data['CONSTRUCTORIDCARD']
                    recod_time = data['CREATEDATE']
                    persion_ID = ''
                    bidding_agent_group = data['AGENCYCORPCODE']
                    if winningbid_date is not None:
                        time_get = time.localtime(int(winningbid_date) / 1000)
                        winningbid_date = time.strftime("%Y-%m-%d", time_get)
                    if recod_time is not None:
                        time_end = time.localtime(int(recod_time) / 1000)
                        recod_time = time.strftime("%Y-%m-%d", time_end)
                    item = dict(
                        company_ID=comp_id,  # '建设部企业ID'
                        insert_time=date,  # 获取时间
                        tender_ID=terderID,  # 招投标信息ID
                        project_ID=project_ID,  # '项目编号'
                        winningbid_ID=winningbid_ID,  # '中标通知书编号'
                        province_winningbid_ID=province_winningbid_ID,  # '省级中标通知书编号'
                        tender_type=tender_type,  # '招标类型'
                        tender_method=tender_method,  # '招标方式'
                        winningbid_date=winningbid_date,  # '中标日期'
                        winningbid_money=winningbid_money,  # '中标金额'
                        build_scale=build_scale,  # '建设规模'
                        area=area,  # '面积'
                        bidding_agent=bidding_agent,  # '招标代理单位'
                        bidding_agent_group=bidding_agent_group,  # '招标代理单位组织机构代码'
                        bidder_name=bidder_name,  # '中标单位名称'
                        bidder_corp_code=bidder_corp_code,  # '中标单位组织机构代码'
                        manager_name=manager_name,  # '项目经理/总监理工程师姓名'
                        manager_cid=manager_cid,  # '项目经理/总监理工程师身份证号码'
                        recod_time=recod_time,  # '记录登记时间'
                        persion_ID=persion_ID,  # '人员id'

                    )
                    if self.__saveOneData__(table_name='BiddingInfo', data=item):
                        self.__saveOneID__(idx=terderID, rediskey='TenderInfoID')
        except Exception as e:
            print(e)
