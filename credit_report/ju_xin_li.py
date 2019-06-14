# coding=utf8
import pdfkit, traceback
from selenium import webdriver
import requests, os, datetime
from lxml import etree
import shutil
from util.mysql_operate import update
from config.config import file_fonts_path, wkhtmltopdf_path
import logging
from util.kafka_utils import send_report_kafka
import re

"""
聚立信PDF报告生成
"""


def get_page_source(url, data, targetPath):
    logger = logging.getLogger('root')

    driver = webdriver.Chrome()
    driver.implicitly_wait(30)
    start_time = datetime.datetime.now()
    driver.get(url)
    logger.info("聚信立数据爬取中")
    while True:
        try:
            text = driver.page_source
            tree = etree.HTML(text)
            str1 = '/html/body/ui-view/div/div/div[2]/div[2]/table/tbody[1]/tr[1]/td[1]/em'
            customer_name = tree.xpath(str1 + '//text()')
            if len(customer_name) > 0 or (datetime.datetime.now() - start_time).seconds > 30:
                driver.close()
                break
        except Exception as e:
            logger.info("网页超时")
            traceback.print_exc()

    if len(customer_name) > 0:
        logger.info("聚信立数据写入HTML")
        # 写入HTML文件
        year = datetime.datetime.now().strftime('%Y')
        month = datetime.datetime.now().strftime('%m')
        day = datetime.datetime.now().strftime('%d')

        file_path = targetPath + os.path.sep + year + os.path.sep + month + os.path.sep + day + os.path.sep + \
                    data["apply_no"] + os.path.sep + 'jxl'

        # 报告位置
        file_path_sql = targetPath + "/" + year + "/" + month + "/" + day + "/" + data["apply_no"] + "/" + 'jxl'

        if not os.path.exists(file_path):
            os.makedirs(file_path)

        file_path_css = targetPath + os.path.sep + year + os.path.sep + month + os.path.sep + day + os.path.sep + \
                        data["apply_no"] + os.path.sep + 'jxl' + os.path.sep + 'css'

        if not os.path.exists(file_path_css):
            os.makedirs(file_path_css)

        # 获取HTML文本，匹配css文件名，写入css文件
        css_name = re.findall(r'"css/(.*?)"', text)
        if len(css_name) > 0:
            for name in css_name:
                get_css(file_path_css, name)

        # 复制字体
        copy_fonts(file_fonts_path, file_path)

        html_file_name = data['apply_no'] + '_jxl_' + str(data["file_seq_no"]) + '.html'
        pdf_file_name = data['apply_no'] + '_jxl_' + str(data["file_seq_no"]) + '.pdf'

        with open(file_path + os.path.sep + html_file_name, 'w', encoding="utf-8") as f_write:
            f_write.write(text)

        pdf_parse(file_path + os.path.sep + html_file_name, file_path, pdf_file_name, data,file_path_sql)
    else:
        # 爬虫成功任务结束,更新数据库
        code = "F"
        end_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        sql = 'update craw_record set crawl_status="{crawl_status}",end_time="{end_time}" where apply_no="{apply_no}" and task_seq_no= "{task_seq_no}";' \
            .format(crawl_status=code, end_time=end_time, apply_no=data["apply_no"], task_seq_no=data["task_seq_no"])
        update(sql)
        driver.close()


def pdf_parse(htmlPath, pdfFilePath, pdfFileName, data,file_path_sql):
    logger = logging.getLogger('root')
    try:

        pdfPath = pdfFilePath + os.path.sep + pdfFileName
        cmd = '{wkhtmltopdf_path} -n --encoding UTF-8 {htmlPath} {pdfPath}'.format(wkhtmltopdf_path=wkhtmltopdf_path,
                                                                                   htmlPath=htmlPath, pdfPath=pdfPath)
        os.system(cmd)

        # 爬虫成功任务结束,更新数据库
        code = "S"
        logger.info("聚信立pdf报告生成")

        send_report_kafka(data, pdfFilePath, pdfFileName)

    except Exception as e:
        traceback.print_exc()
        # 爬虫成功任务结束,更新数据库
        code = "F"

    try:
        end_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        sql = 'update craw_record set crawl_status="{crawl_status}",end_time="{end_time}", update_time="{update_time}" ' \
              ', report_file_path="{report_file_path}", report_file_name="{report_file_name}" ' \
              'where apply_no="{apply_no}" and task_seq_no= "{task_seq_no}";' \
            .format(crawl_status=code, end_time=end_time, update_time=end_time, apply_no=data["apply_no"],
                    task_seq_no=data["task_seq_no"], report_file_path=file_path_sql, report_file_name=pdfFileName)
        print(sql)
        update(sql)
    except:
        pass


def get_css(filepath, name):
    url = "https://dev.juxinli.com/report/css/{css_requetes}".format(css_requetes=name)
    headers = {
        'Host': "dev.juxinli.com",
        'Connection': "keep-alive",
        'Pragma': "no-cache",
        'Cache-Control': "no-cache",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        'Accept': "text/css,*/*;q=0.1",
        'Referer': "https://dev.juxinli.com/report/",
        'Accept-Encoding': "gzip, deflate, br",
        'Accept-Language': "zh-CN,zh;q=0.9,und;q=0.8",
        'cache-control': "no-cache"
    }
    payload = ""
    response = requests.request("GET", url, data=payload, headers=headers)
    text = response.text
    if response.status_code == 200:
        with open(filepath + os.path.sep + name, 'w', encoding="utf-8") as f:
            f.write(text)


# 复制文件字体文件
def copy_fonts(fonts_path, addr_fonts_path):
    logger = logging.getLogger('root')

    logger.info(fonts_path)
    logger.info(addr_fonts_path)
    f_list = os.listdir(fonts_path)
    address_fonts = addr_fonts_path + os.path.sep + "fonts"
    if os.path.exists(address_fonts) == False:
        os.makedirs(address_fonts)
        logger.info("创建成功")
    for fonts_file in f_list:
        oldname = fonts_path + os.path.sep + fonts_file
        newname = address_fonts + os.path.sep + fonts_file
        shutil.copyfile(oldname, newname)


# 聚信立爬虫
def crew_juxinli(url, data, targetPath):
    get_page_source(url, data, targetPath)
