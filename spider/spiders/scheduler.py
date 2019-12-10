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
from spider.spiders.TenderListSpider import TenderListSpider
from spider.spiders.ProjectCorpInfo import ProjectCorpInfo
from spider.spiders.ContractRecordListSpider import ContractRecordListSpider
from spider.spiders.BuildLicenceListSpider import BuildLicenceListSpider
from spider.spiders.ProjectFinishListSpider import ProjectFinishListSpider
from spider.spiders.BuildLicencePersonSpider import BuildLicencePersonSpider
from spider.spiders.SaveWithinProjectSpider import SaveWithinProjectSpider
from spider.spiders.StaffSpider import StaffSpider
import time
import math
import datetime


class Scheduler(object):

    def __divList__(self, list_id=None):
        new_list = []
        if len(list_id) > 0:
            div = math.ceil(len(list_id) / MAX_ID)
            for i in range(div):
                new_list.append(list_id[i * MAX_ID:(i + 1) * MAX_ID])
            new_list.append(list_id[(i + 1) * MAX_ID:])
        return new_list[:5]

    def __scheduleCompListInfo__(self):

        try:
            conn = RedisClient()
            mysql = MySQLClient()
            sql = 'select * from companyName where flag is null order by id limit %d, %d' % (NUM, PRE)
            list_name = mysql.getAll(sql)
            for list_id in list_name:
                if conn.exists(idx=list_id[1], rediskey='CompName'):
                    print(list_id[1], ' comp info is spiders')
                else:
                    conn.delete_key(rediskey='CompName')
                    conn.delete_key(rediskey='TempCompInfoID')
                    conn.delete_key(rediskey='CompInfoID')
                    conn.delete_key(rediskey='QualificationInfoID')
                    conn.delete_key(rediskey='ProjectID')
                    conn.delete_key(rediskey='TempProjectListID')
                    conn.delete_key(rediskey='ProjectInfoID')
                    conn.delete_key(rediskey='TenderInfoID')
                    conn.delete_key(rediskey='TenderListID')
                    conn.delete_key(rediskey='ContractListID')
                    conn.delete_key(rediskey='ContractInfoID')
                    conn.delete_key(rediskey='BuildLicenceInfoID')
                    conn.delete_key(rediskey='BuildLicenceListID')
                    conn.delete_key(rediskey='BuildLicencePersonID')
                    conn.delete_key(rediskey='ProFinishListID')
                    conn.delete_key(rediskey='ProjectFinishInfoID')
                    conn.delete_key(rediskey='ProjectCorpInfoID')
                    conn.delete_key(rediskey='TempTenderListID')
                    conn.delete_key(rediskey='TempContractListID')
                    conn.delete_key(rediskey='TempProFinishListID')
                    conn.delete_key(rediskey='TempProCensorListID')
                    conn.delete_key(rediskey='TempBuildLicenceListID')
                    conn.delete_key(rediskey='TempProjectCorpInfoID')

                    nowTime_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 获取当前时间
                    five_time = datetime.datetime.now().strftime('%Y-%m-%d') + " 05:00:00"  # 每天5点
                    six_time = datetime.datetime.now().strftime('%Y-%m-%d') + " 06:00:00"  # 每天6点
                    if nowTime_str < five_time or nowTime_str > six_time:
                        spider = CompanyListSpider()
                        spider.runs(list_id[1])
                        # time.sleep(cycle)
                        self.__scheduleProjectList__()
                        # self.__scheduleQualificationInfo__()
                        self.__scheduleTenderInfo__()
                        self.__scheduleContractRecordInfo__()
                        self.__scheduleProjectCorpInfo__()
                        self.__scheduleBuildLicenceInfo__()
                        # self.__scheduleBuildLicencePerson__()
                        self.__scheduleProjectFinishInfo__()
                        update_sql = 'update companyName set flag = 1 where companyName = "%s" ' % list_id[1]
                        end_time = datetime.datetime.now().strftime('%Y-%m-%d') + " 07:00:00"  # 每天7点
                        if nowTime_str > end_time or nowTime_str < six_time:
                            mysql.__updateData__(update_sql)

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
        except Exception as e:
            print("Error Spider project list", e)

    def __scheduleProjectCorpInfo__(self):
        try:
            conn = RedisClient()
            spider = ProjectCorpInfo()
            print('开始获取项目')
            key = 'TempProjectCorpInfoID'
            list_id = list(set(conn.all(rediskey=key)))
            spider.run(list_id, main_url=[PROJECTCORPINFO], key=key)
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
            spider = TenderListSpider()
            key = 'TempTenderListID'
            list_id = list(set(conn.all(rediskey=key)))
            spider.run(list_id, main_url=[TENDERAPI], key=key)
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
            print('开始获取公司项目合同登记信息')
            spider = ContractRecordListSpider()
            key = 'TempContractListID'
            list_id = list(set(conn.all(rediskey=key)))
            spider.run(list_id, main_url=[CONTRACTRECORD], key=key)
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
            print('开始获取公司项目施工许可信息')
            spider = BuildLicenceListSpider()
            key = 'TempBuildLicenceListID'
            list_id = list(set(conn.all(rediskey=key)))
            spider.run(list_id, main_url=[LICENCEMANAGE], key=key)
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
            list_id = list(
                set(conn.all(rediskey='BuildLicenceInfoID')) - set(conn.all(rediskey='BuildLicencePersonID')))
            spider.run(list_id)
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
            print('开始获取公司项目竣工信息')
            spider = ProjectFinishListSpider()
            key = 'TempProFinishListID'
            list_id = list(set(conn.all(rediskey=key)))
            spider.run(list_id, main_url=[FINISHMANAGE], key=key)
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

        if ALL_ENABLED:
            self.__scheduleCompListInfo__()

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
            CompList_Process = Process(target=self.__scheduleCompListInfo__())
            CompList_Process.start()

        if COMPINFO_ENABLED:
            CompInfo_Process = Process(target=self.__scheduleCompInfo__())
            CompInfo_Process.start()

        if PROJECTLIST_ENABLED:
            ProList_Process = Process(target=self.__scheduleProjectList__())
            ProList_Process.start()

        if PROJECTINFO_ENABLED:
            ProInfo_Process = Process(target=self.__scheduleProjectCorpInfo__())
            ProInfo_Process.start()

        if PERSON_ENABLED:
            PersonInfo_Process = Process(target=self.__schedulePersonInfo__())
            PersonInfo_Process.start()

        if QUALIFICATION_ENABLE:
            QualificationInfo_Process = Process(target=self.__scheduleQualificationInfo__())
            QualificationInfo_Process.start()

        if TENDER_ENABLE:
            TenderInfo_Process = Process(target=self.__scheduleTenderInfo__())
            TenderInfo_Process.start()

        if CONTRACT_ENABLED:
            CRInfo_Process = Process(target=self.__scheduleContractRecordInfo__())
            CRInfo_Process.start()

        if LICENCE_ENABLED:
            BLInfo_Process = Process(target=self.__scheduleBuildLicenceInfo__())
            BLInfo_Process.start()

        if LICENCE_PERSON_ENABLED:
            BLPInfo_Process = Process(target=self.__scheduleBuildLicencePerson__())
            BLPInfo_Process.start()

        if FINISH_ENABLED:
            FinishInfo_Process = Process(target=self.__scheduleProjectFinishInfo__())
            FinishInfo_Process.start()

# if __name__ == '__main__':
#     sc = Scheduler()
#     sc.__scheduleCompInfo__()
