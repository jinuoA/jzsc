from multiprocessing import Process
from spider.spiders.config import *
from spider.db.Mysql_db import MySQLClient
from spider.db.Redis_db import RedisClient
from spider.spiders.QualificationSpider import QualificationSpider
from spider.spiders.CompanyListSpider import CompanyListSpider
from spider.spiders.ProjectListSpider import ProjectListSpider
from spider.spiders.PersonSpider import PersonSpider
from spider.spiders.CompanyInfoSpider import CompanyInfoSpider
from spider.spiders.ProjectInfoSpider import ProjectInfoSpider
from spider.spiders.TenderSpider import TenderSpider
from spider.spiders.ContractRecordSpider import ContractRecordSpider
from spider.spiders.BuilderLicenceSpider import BuildLicenceSpider
from spider.spiders.ProjectFinishSpider import ProjectFinishSpider
from spider.spiders.BuildLicencePersonSpider import BuildLicencePersonSpider
from spider.spiders.SaveWithinProjectSpider import SaveWithinProjectSpider
from spider.spiders.StaffSpider import  StaffSpider
import time
import math


class Scheduler(object):

    def __divList__(self, list_id=None):
        new_list = []
        if len(list_id) > 0:
            div = math.ceil(len(list_id) / MAX_ID)
            for i in range(div):
                new_list.append(list_id[i * MAX_ID:(i + 1) * MAX_ID])
            new_list.append(list_id[(i + 1) * MAX_ID:])
        return new_list[:10]

    def __scheduleCompListInfo__(self, cycle=SPIDER_CYCLE):

        try:
            mysql = MySQLClient()
            sql = 'select * from buildcompanyinfo_copy order by company_ID limit 50,50'
            list_id = mysql.getAll(sql)
            spider = CompanyListSpider()
            spider.runs(list_id)
            time.sleep(cycle)
        except Exception as e:
            print(e)

    def __scheduleCompInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = CompanyInfoSpider()
            print('开始获取公司信息')
            print('当前以获取公司信息量为：', conn.count(rediskey='CompInfoID'))
            list_id = list(set(conn.all(rediskey='TempCompInfoID')) - set(conn.all(rediskey='CompInfoID')))
            # new_list = self.__divList__(list_id=list_id)
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider comp info", e)

    def __scheduleQualificationInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司资质信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = QualificationSpider()
            print('开始获取公司资质信息')
            print('当前以获取公司资质信息量为：', conn.count(rediskey='QualificationInfoID'))
            list_id = list(
                set(conn.all(rediskey='CompInfoID')) - set(conn.all(rediskey='QualificationInfoID')))
            # new_list = self.__divList__(list_id=list_id)
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider Qualification info", e)

    def __scheduleProjectList__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司项目列表信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = ProjectListSpider()
            print('开始获取公司项目列表信息')
            print('当前以获取项目ID量为：', conn.count(rediskey='ProjectID'))
            list_id = list(set(conn.all(rediskey='CompInfoID')) - set(conn.all(rediskey='ProjectID')))
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleProjectInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司项目信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = ProjectInfoSpider()
            print('开始获取公司项目信息')
            print('当前以获取项目ID量为：', conn.count(rediskey='ProjectInfoID'))
            list_id = list(set(conn.all(rediskey='TempProjectListID')) - set(conn.all(rediskey='ProjectInfoID')))
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleTenderInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取项目招标信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            print('开始获取公司项目招标信息')
            print('当前以获取招标招标ID量为：', conn.count(rediskey='TenderInfoID'))
            self.__scheduleWithinProjectList__(main_url=[TENDERAPI], TempList='TenderListID')
            list_id = list(set(conn.all(rediskey='TenderListID')) - set(conn.all(rediskey='TenderInfoID')))
            spider = TenderSpider()
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleWithinProjectList__(self, cycle=SPIDER_CYCLE, main_url=None, TempList=None):
        """
        获取项目内的各个类型list
        :param cycle:
        :param main_url:
        :param TempList:
        :return:
        """
        try:
            conn = RedisClient()
            key = 'Temp' + TempList
            list_id = list(set(conn.all(rediskey=key)))
            spider = SaveWithinProjectSpider()
            spider.run(list_id, main_url, TempList)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleContractRecordInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司项目合同登记信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = ContractRecordSpider()
            print('开始获取公司项目合同登记信息')
            print('当前以获取项目合同登记ID量为：', conn.count(rediskey='ContractRecordInfoID'))
            self.__scheduleWithinProjectList__(main_url=[CONTRACTRECORD], TempList='ContractListID')
            list_id = list(set(conn.all(rediskey='ContractListID')) - set(conn.all(rediskey='ContractInfoID')))
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleBuildLicenceInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司项目施工许可信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = BuildLicenceSpider()
            print('开始获取公司项目施工许可信息')
            print('当前以获取项目施工许可ID量为：', conn.count(rediskey='BuildLicenceInfoID'))
            self.__scheduleWithinProjectList__(main_url=[LICENCEMANAGE], TempList='BuildLicenceListID')
            list_id = list(set(conn.all(rediskey='BuildLicenceListID')) - set(conn.all(rediskey='BuildLicenceInfoID')))
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleBuildLicencePerson__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司项目施工许可人员信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = BuildLicencePersonSpider()
            print('开始获取公司项目施工许可人员信息')
            print('当前以获取项目施工许可人员ID量为：', conn.count(rediskey='BuildLicencePersonID'))
            list_id = list(set(conn.all(rediskey='BuildLicenceInfoID')) - set(conn.all(rediskey='BuildLicencePersonID')))
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleProjectFinishInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司项目竣工信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = ProjectFinishSpider()
            print('开始获取公司项目竣工信息')
            print('当前以获取项目竣工ID量为：', conn.count(rediskey='ProjectFinishInfoID'))
            self.__scheduleWithinProjectList__(main_url=[FINISHMANAGE], TempList='ProFinishListID')
            list_id = list(set(conn.all(rediskey='ProFinishListID')) - set(conn.all(rediskey='ProjectFinishInfoID')))
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider project list", e)

    def __schedulePersonInfo__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司人员信息
        :param cycle:
        :return:
        """
        try:
            conn = RedisClient()
            spider = PersonSpider()
            print('开始获取公司人员信息')
            print('当前以获取公司人员信息量为：', conn.count(rediskey='PersonInfoID'))
            list_id = list(set(conn.all(rediskey='CompInfoID')) - set(conn.all(rediskey='PersonInfoID')))
            # new_list = self.__divList__(list_id=list_id)
            spider.run(list_id)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider Staff info", e)

    def __scheduleStaff__(self, cycle=SPIDER_CYCLE):
        """
        定时爬取公司人员信息
        :param cycle:
        :return:
        """
        try:
            # conn = RedisClient()
            spider = StaffSpider()
            print('开始获取公司人员信息')
            # new_list = self.__divList__(list_id=list_id)
            mysql = MySQLClient()
            sql = 'select * from cityNo where flag is NULL'
            city_list = mysql.getAll(sql)
            spider.run(city_list)
            time.sleep(cycle)
        except Exception as e:
            print("Error Spider Staff info", e)

    def run(self):
        print("爬虫开始运行")

        # if COMPLIST_ENABLED:
        #     self.__scheduleCompListInfo__()
        #
        # if COMPINFO_ENABLED:
        #     self.__scheduleCompInfo__()
        #
        # if PROJECTLIST_ENABLED:
        #     self.__scheduleProjectList__()
        #
        # if PROJECTINFO_ENABLED:
        #     self.__scheduleProjectInfo__()
        #
        # if PERSON_ENABLED:
        #     self.__schedulePersonInfo__()
        #
        # if QUALIFICATION_ENABLE:
        #     self.__scheduleQualificationInfo__()
        #
        # if TENDER_ENABLE:
        #     self.__scheduleTenderInfo__()
        #
        # if CONTRACT_ENABLED:
        #     self.__scheduleContractRecordInfo__()
        #
        # if LICENCE_ENABLED:
        #     self.__scheduleBuildLicenceInfo__()
        #
        # if LICENCE_PERSON_ENABLED:
        #     self.__scheduleBuildLicencePerson__()
        #
        # if FINISH_ENABLED:
        #     self.__scheduleProjectFinishInfo__()

        # 抓取人员信息获取公司名称
        if STAFF_ENABLED:
            self.__scheduleStaff__()
        """
        并行
        """
        if COMPLIST_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleCompListInfo__())
            ComInfo_Process.start()

        if COMPINFO_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleCompInfo__())
            ComInfo_Process.start()

        if PROJECTLIST_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleProjectList__())
            ComInfo_Process.start()

        if PROJECTINFO_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleProjectInfo__())
            ComInfo_Process.start()

        if PERSON_ENABLED:
            ComInfo_Process = Process(target=self.__schedulePersonInfo__())
            ComInfo_Process.start()

        if QUALIFICATION_ENABLE:
            ComInfo_Process = Process(target=self.__scheduleQualificationInfo__())
            ComInfo_Process.start()

        if TENDER_ENABLE:
            ComInfo_Process = Process(target=self.__scheduleTenderInfo__())
            ComInfo_Process.start()

        if CONTRACT_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleContractRecordInfo__())
            ComInfo_Process.start()

        if LICENCE_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleBuildLicenceInfo__())
            ComInfo_Process.start()

        if LICENCE_PERSON_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleTenderInfo__())
            ComInfo_Process.start()

        if FINISH_ENABLED:
            ComInfo_Process = Process(target=self.__scheduleProjectFinishInfo__())
            ComInfo_Process.start()

# if __name__ == '__main__':
#     sc = Scheduler()
#     sc.__scheduleCompInfo__()
