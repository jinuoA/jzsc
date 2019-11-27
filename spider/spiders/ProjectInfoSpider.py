from .SpiderMain import SpiderMain
from spider.spiders.config import *
import datetime as dt
from spider.util.decrypt import decrypts
import json
"""
项目详情
"""


class ProjectInfoSpider(SpiderMain):

    def run(self, list_id):
        new_list = [list_id[x:x + MAX_NUM] for x in range(0, len(list_id), MAX_NUM)]
        for fen_list in new_list:
            projet_info_url = []
            for data in fen_list:
                pro_id = str(data).split('_', 1)[0]
                comp_id = str(data).split('_', 1)[1]
                if self.__findOneID__(idx=pro_id, rediskey='ProjectID'):
                    print(pro_id, 'project info had spiders')
                else:
                    url = PROJECTINFOAPI + pro_id
                    projet_info_url.append(url)
            self.__asyncSpider__(projet_info_url, comp_id)

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
                    pro_id = data['ID']
                    project_name = data['PRJNAME']
                    project_ID = data['PRJNUM']
                    project_code = data['PROVINCEPRJNUM']
                    if data['COUNTY'] is None and data['CITY'] is not None:
                        project_region = data['PROVINCE'] + '-' + data['CITY']
                    elif data['CITY'] is None:
                        project_region = data['PROVINCE']
                    elif data['COUNTY'] is None and data['CITY'] is not None and data['PROVINCE'] is not None:
                        project_region = data['PROVINCE']+'-'+data['CITY']+'-'+data['COUNTY']
                    build_company = data['BUILDCORPNAME']
                    build_company_code = data['BUILDCORPCODE']
                    project_type = data['PRJTYPENUM']
                    construction_nature = data['PRJPROPERTYNUM']
                    engineering_purpose = data['PRJFUNCTIONNUM']
                    total_money = data['ALLINVEST']
                    total_area = data['ALLAREA']
                    approval_level = data['PRJAPPROVALLEVELNUM']
                    approval_num = data['PRJAPPROVALNUM']
                    item = dict(
                        company_ID=comp_id,  # '建设部企业ID'
                        insert_time=date,  # 获取时间
                        project_name=project_name,  # 项目名称
                        project_ID=project_ID,  # 项目编号
                        project_code=project_code,  # 省级项目编号
                        project_region=project_region,  # 所在区划
                        build_company=build_company,  # 建设单位
                        build_company_code=build_company_code,  # 建设单位组织机构代码（统一社会信用代码）
                        project_type=project_type,  # 项目分类
                        construction_nature=construction_nature,  # 建设性质
                        engineering_purpose=engineering_purpose,  # 工程用途
                        total_money=total_money,  # 总投资
                        total_area=total_area,  # 总面积
                        approval_level=approval_level,  # 立项级别
                        approval_num=approval_num,  # 立项文号
                    )
                    if self.__saveOneData__(table_name='ProjectInfo', data=item):
                        self.__saveOneID__(idx=pro_id, rediskey='ProjectID')
        except Exception as e:
            print(e)
