from selenium import webdriver
from browsermobproxy import Server
from spider.util.CrackFontCheck import HandlePic
from selenium.webdriver.chrome.options import Options
import time


# 获取accessToken
def getToken():
    try:
        server = Server(r'F:\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat')
        server.start()
        proxy = server.create_proxy()

        chrome_options = Options()
        chrome_options.add_experimental_option('w3c', False)
        chrome_options.add_argument('--proxy-server={0}'.format(proxy.proxy))
        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.maximize_window()
        base_url = "http://jzsc.mohurd.gov.cn/data/company/detail?id=C5C5C4C3C5C2C7C7C5C5C0C2C7CCC7C7C5C6"
        proxy.new_har("douyin", options={'captureHeaders': True, 'captureContent': True})
        driver.get(base_url)
        count = 0
        while '验证已过期，是否重新重新进行验证或停留在当前页面？' in driver.page_source:
            driver.find_element_by_xpath('//*[@id="app"]/div/header/div[5]/div/div[3]/div/button[1]').click()
            time.sleep(2)
            driver.refresh()
            time.sleep(1.5)
            # count += 1
            # if count >= 2:

            if '验证已过期，是否重新重新进行验证或停留在当前页面？' in driver.page_source:
                driver.find_element_by_xpath('//*[@id="app"]/div/header/div[5]/div/div[3]/div/button[1]').click()
                hp = HandlePic()
                hp.click_pic(driver)
        result = proxy.har
        token = set()
        for entry in result['log']['entries']:
            _url = entry['request']['url']
            if "api/webApi/dataservice/query/comp/caDetailList?qyId" in str(_url):
                _response = entry['request']
                _accessToken = entry['request']['headers'][4]['value']
                if _accessToken != '':
                    token.add(_accessToken)
        server.stop()
        return list(token)[0]
    except Exception as e:
        print(e)
    finally:
        driver.quit()
        server.stop()
