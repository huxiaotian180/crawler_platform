# coding=utf8
import time, traceback
from selenium import webdriver
import os, datetime
from lxml import etree
from util.mysql_operate import update
from util.sql_utils import updata_craw_state
import logging
from util.kafka_utils import send_report_kafka

"""
数据魔盒PDF报告生成
"""


def get_page_source(url, data, targetPath):
    logger = logging.getLogger('root')

    # 数据魔盒报告保存位置
    year = datetime.datetime.now().strftime('%Y')
    month = datetime.datetime.now().strftime('%m')
    day = datetime.datetime.now().strftime('%d')
    file_path = targetPath + os.path.sep + year + os.path.sep + month + os.path.sep + day + os.path.sep + \
                data["apply_no"] + os.path.sep + 'sjmh'

    # 报告输出位置
    file_path_sql = targetPath + "/" + year + "/" + month + "/" + day + "/" + data["apply_no"] + "/" + 'sjmh'

    # 判断文件夹是否存在，不存在创建文件夹
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    chromeOptions = webdriver.ChromeOptions()
    prefs = {"download.default_directory": file_path}
    chromeOptions.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(chrome_options=chromeOptions)
    driver.implicitly_wait(15)
    start_time = datetime.datetime.now()
    driver.get(url)
    while True:
        try:
            text = driver.page_source
            tree = etree.HTML(text)
            # 获取报告姓名
            str1 = '//*[@id="reportContant"]/div/div[2]/div/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]'
            customer_name = tree.xpath(str1 + '//text()')

            if len(customer_name) > 0:
                driver.find_element_by_xpath('//*[@id="pdfArea"]/div/div[1]/button[1]').click()
                break
        except Exception as e:
            logger.info("网页超时")
            traceback.print_exc()
        finally:
            if (datetime.datetime.now() - start_time).seconds > 30:
                break

    code = "F"
    # 修改文件命名规则
    modify_name = data['apply_no'] + '_sjmh_' + str(data["file_seq_no"]) + '.pdf'
    if len(customer_name) > 0:
        logger.info("数据魔盒数据获取成功")

        try:
            if is_file(file_path, modify_name, data):
                # 爬虫成功任务结束,更新数据库
                code = "S"

                send_report_kafka(data, file_path, modify_name)
        except Exception as e:
            logger.info("写入文件错误")
            traceback.print_exc()
        finally:
            driver.close()
    else:
        driver.close()

    updata_craw_state(code,data,file_path_sql,modify_name)


def is_file(targetPath, modify_name, data):
    logger = logging.getLogger('root')

    # 获取当前系统时间
    today = datetime.date.today()
    fileStr = '运营商报告'
    fileNameStr = '%s%s%s' % (data['cust_name'], today, fileStr)
    fileName = fileNameStr + '.pdf'
    filePath = targetPath + os.path.sep + fileName

    print(filePath)
    renamePath = targetPath + os.path.sep + modify_name
    start_time = datetime.datetime.now()
    while True:
        end_time = datetime.datetime.now()
        stop_time = (end_time - start_time).seconds
        try:
            if os.path.exists(filePath):
                os.rename(filePath, renamePath)
                logger.info("数据魔盒PDF生成")
                return True
            else:
                if stop_time > 100:
                    break
                time.sleep(2)
        except Exception as e:
            logger.info("文件路径错误")
            traceback.print_exc()
    return False


# 数据魔盒爬虫
def crew_shujumohe(url, data, targetPath):
    get_page_source(url, data, targetPath)
