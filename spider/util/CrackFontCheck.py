#  字体验证码点击

from selenium import webdriver
import time
from PIL import Image
from selenium.webdriver.common.action_chains import ActionChains
from spider.util.chaojiying import *
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.command import Command
import pyautogui
import os


class HandlePic(object):

    def get_points(self, captcha_result):
        """
        解析识别结果
        :param captcha_result: 识别结果
        :return: 转化后的结果
        """
        groups = captcha_result.get('pic_str').split('|')
        locations = [[int(number) for number in group.split(',')] for group in groups]
        return locations

    def touch_click_words(self, locations):
        """
        点击验证图片
        :param locations: 点击位置
        :return: None
        """
        for location in locations:
            print(location)
            element, url_http = self.get_touclick_element()
            ActionChains(self.browser).move_to_element_with_offset(element, location[0],
                                                                   location[1]).click().perform()
            time.sleep(0.5)

    def get_pics(self, driver):
        """
         截取图片
         :param driver:
         :return:
         """
        try:
            current_time = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
            current_time1 = time.strftime("%Y-%m-%d", time.localtime(time.time()))
            print(current_time)
            print(current_time1)
            # above = driver.find_element_by_class_name("yidun_tips__icon")
            # ActionChains(driver).move_to_element(above).perform()
            time.sleep(0.5)
            # 获取区域块
            imgelement = driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/div[2]')
            # 图片坐标
            locations = imgelement.location
            # 图片大小
            sizes = imgelement.size
            # 构造指数的位置
            rangle = (
                int(locations['x']+20), int(locations['y']+20), int(locations['x'] + sizes['width']-20),
                int(locations['y'] + sizes['height']-20))
            print(rangle)
            pfilename = u'.\\image'
            save_path = pfilename + '\\' + current_time1 + '_' + current_time + '.png'
            time.sleep(1.5)
            driver.save_screenshot(save_path)
            # 打开截图切割
            img = Image.open(save_path)
            jpg = img.convert('RGB')
            jpg = img.crop(rangle)
            path = pfilename + '\\' + current_time1 + '_' + current_time + '.png'
            time.sleep(1)
            jpg.save(path)
            print("图片截取成功!")
            return (path, locations)
        except Exception as e:
            print(e)

    def click_pic(self, driver):
        """
         点击验证
         :param driver:
         :return:
         """
        try:
            chaojiying = Chaojiying_Client('yixunbang', 'yixunbang', '902224')  # 请求获取点击的坐标
            while True:
                time.sleep(2.5)
                save_path, locations = self.get_pics(driver)
                im = open(save_path, 'rb').read()
                coordinates = chaojiying.PostPic(im, 9103)
                locations_chaojiying = self.get_points(coordinates)
                if len(locations_chaojiying) > 0:  # 判断是否获取到坐标
                    element = WebDriverWait(driver, 5, 0.5).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'yidun_bg-img')))  # 获取图片元素
                    ActionChains(driver).move_to_element(element)
                    time.sleep(0.5)
                    location_x = 0
                    location_y = 0
                    pyautogui.moveTo(locations['x']+27, int(locations['y']+125), duration=0.1)  # 鼠标定位到图片左上角
                    for location in locations_chaojiying:
                        pyautogui.moveRel(location[0] - location_x, location[1] - location_y, duration=0.5)  # 根据获取的坐标移动
                        driver.execute(Command.MOVE_TO, {'xoffset': location[0], 'yoffset': location[1]})
                        print(" 点击坐标 " + str(location[0]), str(location[1]))
                        ActionChains(driver).move_to_element_with_offset(element, location[0],
                                                                         location[1]-8).click().perform()  # 模拟点击
                        time.sleep(0.9)
                        location_x = location[0]
                        location_y = location[1]
                    if '验证成功' in driver.page_source:
                        os.remove(save_path)  # 删除截图图片
                        break
                    else:
                        os.remove(save_path)
        except Exception as e:
            print(e)
        finally:
            os.remove(save_path)


if __name__ == '__main__':
    opt = webdriver.ChromeOptions()
    opt.add_experimental_option('w3c', False)
    driver = webdriver.Chrome(chrome_options=opt)
    driver.maximize_window()
    start_url = "http://dun.163.com/trial/picture-click"
    driver.get(start_url)
    hp = HandlePic()
    hp.click_pic(driver)
    driver.quit()
