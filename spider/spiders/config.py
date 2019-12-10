SPIDER_CYCLE = 5
MAX_ID = 5  # asyncio 最大并发数
COMPANY_MAX = 10  # 抓取公司最大并发数
MAX_NUM = 5  # list按种类分组
NUM = 10000  # 从第几条开始
PRE = 5000  # 每次抓取数量

# URL API
MAINURL = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/comp/list?complexname='  # 查询公司信息
COMPLISTAPI = '/dataservice/query/comp/list'
COMPINFOAPI = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/comp/compDetail?compId='
QUALIFICATIONAPI = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/comp/caDetailList?qyId='  # 企业资质
PROJECTLISTAPI = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/comp/compPerformanceListSys?qy_id='  # 项目列表
STAFFAPI = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/comp/regStaffList?qyId='  # 人员信息
PROJECTINFOAPI = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/projectDetail?id='  # 项目详情信息
TENDERAPI = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/tenderInfo?jsxmCode='  # 投标信息 （项目编号）
TENDERINFOAPI = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/tenderInfoDetail?id='  # 招投标信息详情（招标ID A2A6A5A1A2A4）
CONTRACTRECORD = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/contractRecordManage?jsxmCode='  # 合同登记信息
CONTRACTRECORDINFO = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/contractRecordManageDetail?id='  # 合同登记详情
# CENSORINFO = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/projectCensorInfo?jsxmCode='  # 施工图审查?
LICENCEMANAGE = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/builderLicenceManage?jsxmCode='  # 施工许可列表
LICENCEMANAGEINFO = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/builderLicenceManageDetail?BuilderLicenceNum='  # 施工许可详情
LICENCEMANAGEINFOPERSON = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/builderRelation?BuilderLicenceNum='  # 施工许可人员

PROJECTCORPINFO = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/projectCorpInfo?jsxmCode='  # 参与单位与人员

FINISHMANAGE = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/projectFinishManage?jsxmCode='  # 竣工验收列表
FINISHMANAGEINFO = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/project/projectFinishManageDetail?id='  # 竣工验收详情
MAX_PAGE = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/comp/getTotal?qyId='  # 获取最大页数

PERSON_LIST = 'http://jzsc.mohurd.gov.cn/api/webApi/dataservice/query/staff/list?complexname='  # 获取人员列表，并保存数据库

# 开关配置
COMPLIST_ENABLED = False
COMPINFO_ENABLED = False
PROJECTLIST_ENABLED = False

PROJECTINFO_ENABLED = False
PERSON_ENABLED = False
QUALIFICATION_ENABLE = False

TENDER_ENABLE = False
CONTRACT_ENABLED = False
LICENCE_ENABLED = False
FINISH_ENABLED = False
# SAVEWITHIN_ENABLED = True
LICENCE_PERSON_ENABLED = False

STAFF_ENABLED = False

ALL_ENABLED = True

MONTHS = [
    '01',
    '02',
    '03',
    '04',
    '05',
    '06',
    '07',
    '08',
    '09',
    '10',
    '11',
    '12',
]

DAYS = [
    '01',
    '02',
    '03',
    '04',
    '05',
    '06',
    '07',
    '08',
    '09',
    '10',
    '11',
    '12',
    '13',
    '14',
    '15',
    '16',
    '17',
    '18',
    '19',
    '20',
    '21',
    '22',
    '23',
    '24',
    '25',
    '26',
    '27',
    '28',
    '29',
    '30',
    '31',
]
