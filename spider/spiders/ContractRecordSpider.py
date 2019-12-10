"""
合同等级信息
"""
from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
from spider.util.decrypt import decrypts
import json
import time

class ContractRecordSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            for data in fen_list:
                con_info_url=[]
                con_id = str(data).split("_", 1)[0]
                comp_id = str(data).split("_", 1)[1]
                if self.__findOneID__(idx=con_id, rediskey='ContractInfoID'):
                    print(con_id, 'contract info had spiders')
                else:
                    url = CONTRACTRECORDINFO + con_id + "&pg=0&pgsz=15&total=0"
                    con_info_url.append(url)
                    time.sleep(0.8)
                    self.__asyncSpider__(con_info_url, comp_id)

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
                    contractID = data['ID']
                    project_ID = data['PRJNUM']
                    contract_code = data['CONTRACTNUM']
                    contract_filing_code = data['RECORDNUM']
                    contract_type = data['CONTRACTTYPENUM']
                    contract_group = ''
                    contract_money = data['CONTRACTMONEY']
                    build_scale = data['PRJSIZE']
                    contractor_out_name = data['PROPIETORCORPNAME']
                    contractor_out_code = data['PROPIETORCORPCODE']
                    contractor_name = data['CONTRACTORCORPNAME']
                    contractor_code = data['CONTRACTORCORPCODE']
                    union_contractor_name = data['UNIONCORPNAME']
                    union_contractor_code = data['UNIONCORPCODE']
                    province_contract_filing_code = ''
                    recod_time = data['CREATEDATE']
                    contract_signing_date = data['CONTRACTDATE']
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
                        company_ID=comp_id,  # '建设部企业ID'
                        insert_time=date,  # 获取时间
                        contract_filing_ID=contractID,  # 合同备案信息ID
                        project_ID=project_ID,  # 项目编号
                        contract_filing_code=contract_filing_code,  # '合同备案编号'
                        province_contract_filing_code=province_contract_filing_code,  # 省级合同备案编号
                        contract_code=contract_code,  # 合同编号
                        contract_group=contract_group,  # 合同分类
                        contract_type=contract_type,  # 合同类别
                        contract_money=contract_money,  # 合同金额
                        build_scale=build_scale,  # 建设规模
                        contract_signing_date=contract_signing_date,  # 合同签订日期
                        contractor_out_name=contractor_out_name,  # 发包单位名称
                        contractor_out_code=contractor_out_code,  # 发包单位组织机构代码
                        contractor_name=contractor_name,  # 承包单位名称
                        contractor_code=contractor_code,  # 承包单位组织机构代码
                        union_contractor_name=union_contractor_name,  # 联合体承包单位名称
                        union_contractor_code=union_contractor_code,  # 联合体承包单位组织机构代码
                        recod_time=recod_time,  # 记录登记时间
                    )
                    if self.__saveOneData__(table_name='ContractFiling', data=item):
                        self.__saveOneID__(idx=contractID, rediskey='ContractInfoID')
        except Exception as e:
            print(e)
