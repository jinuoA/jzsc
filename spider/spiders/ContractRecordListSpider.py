from .SpiderMain import SpiderMain
from spider.spiders.config import *
from spider.util.decrypt import decrypts
import json
import time
import datetime as dt


class ContractRecordListSpider(SpiderMain):

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
                        contractID = dataJson['CONTRACTORCORPID']
                        project_ID = dataJson['PRJNUM']
                        contract_filing_code = dataJson['RECORDNUM']
                        province_contract_filing_code = dataJson['PROVINCERECORDNUM']
                        contract_code = dataJson['CONTRACTNUM']
                        contract_type = dataJson['CONTRACTTYPENUM']
                        contract_money = dataJson['CONTRACTMONEY']
                        contractor_out_name = dataJson['PROPIETORCORPNAME']
                        contractor_name = dataJson['CONTRACTORCORPNAME']
                        contract_signing_date = dataJson['CONTRACTDATE']
                        recod_time = dataJson['CREATEDATE']
                        if recod_time is not None and int(recod_time) < 0:
                            recod_time = None
                        if contract_signing_date is not None and int(contract_signing_date) < 0:
                            contract_signing_date = None
                        if recod_time is not None:
                            time_end = time.localtime(int(recod_time) / 1000)
                            recod_time = time.strftime("%Y-%m-%d", time_end)
                        if contract_signing_date is not None:
                            time_get = time.localtime(int(contract_signing_date) / 1000)
                            contract_signing_date = time.strftime("%Y-%m-%d", time_get)
                        item = dict(
                            company_ID=comp,  # '建设部企业ID'
                            insert_time=date,  # 获取时间
                            contract_filing_ID=contractID,  # 合同备案信息ID
                            project_ID=project_ID,  # 项目编号
                            contract_filing_code=contract_filing_code,  # '合同备案编号'
                            province_contract_filing_code=province_contract_filing_code,  # 省级合同备案编号
                            contract_code=contract_code,  # 合同编号
                            contract_type=contract_type,  # 合同类别
                            contract_money=contract_money,  # 合同金额
                            contract_signing_date=contract_signing_date,  # 合同签订日期
                            contractor_out_name=contractor_out_name,  # 发包单位名称
                            contractor_name=contractor_name,  # 承包单位名称
                            recod_time=recod_time,  # 记录登记时间
                        )
                        if self.__saveOneData__(table_name='ContractFiling', data=item):
                            self.__saveOneID__(idx=contractID, rediskey='ContractInfoID')
        except Exception as e:
            print(e)
