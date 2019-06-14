# coding=utf8
import urllib.request
import os, datetime
from util.mysql_operate import update
from util.sql_utils import updata_craw_state
import logging
from util.kafka_utils import send_report_kafka


def callback(block_num, block_size, total_size):
    logger = logging.getLogger('root')
    if block_num * block_size > total_size:
        logger.info('file write success!')


def from_url_ceg(url, data, target_path):
    logger = logging.getLogger('root')

    pdf_file_name = data['apply_no'] + '_ceg_100.pdf'

    # w文件命名规则
    year = datetime.datetime.now().strftime('%Y')
    month = datetime.datetime.now().strftime('%m')
    day = datetime.datetime.now().strftime('%d')
    file_path = target_path + os.path.sep + year + os.path.sep + month + os.path.sep + day + os.path.sep + data[
        "apply_no"] + os.path.sep + 'ceg'

    file_path_sql = target_path + "/" + year + "/" + month + "/" + day + "/" + data["apply_no"] + "/" + 'ceg'

    if  not os.path.exists(file_path):
        os.makedirs(file_path)

    local_file_path = file_path + os.path.sep + pdf_file_name
    urllib.request.urlretrieve(url, local_file_path, callback)
    logger.info('success')
    code = "S"
    updata_craw_state(code,data,file_path_sql,pdf_file_name)

    send_report_kafka(data, file_path, pdf_file_name)

# 百融爬虫
def crew_ceg(url, data, target_path):
    # 调用爬虫程序
    from_url_ceg(url, data, target_path)
