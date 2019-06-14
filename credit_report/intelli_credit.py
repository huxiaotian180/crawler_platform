#coding=utf8
import pdfkit
import os,datetime
from util.mysql_operate import update
from util.sql_utils import updata_craw_state
from config.config import file_target_path
from util.kafka_utils import send_report_kafka


#中智诚征信报告
def from_url_zhongzc(url,data,targetPath):
    # w文件命名规则
    year = datetime.datetime.now().strftime('%Y')
    month = datetime.datetime.now().strftime('%m')
    day = datetime.datetime.now().strftime('%d')
    file_path = file_target_path + os.path.sep + year + os.path.sep + month + os.path.sep + day + os.path.sep + data[
        "apply_no"] + os.path.sep + 'zhongzc'
    pdf_file_name = data['apply_no'] + '_zhongzc_' + str(data["file_seq_no"]) + '.pdf'

    # 报告输出位置
    file_path_sql = file_target_path + "/" + year + "/" + month + "/" + day + "/" + data["apply_no"] + "/" + 'zhongzc'

    if not os.path.exists(file_path):
        os.makedirs(file_path)

    try:
        print("中智成PDF转化中")
        pdfkit.from_url(url, file_path + os.path.sep + pdf_file_name)
        print("中智成PDF转化成功")
        # 爬虫成功任务结束,更新数据库
        code = "S"
        updata_craw_state(code,data,file_path_sql,pdf_file_name)

        send_report_kafka(data, file_path, pdf_file_name)

    except Exception as e:
        # 爬虫成功任务结束,更新数据库
        code = "F"
        updata_craw_state(code, data, file_path_sql, pdf_file_name)

def crew_zhongzc(url,data,targetPath):
    from_url_zhongzc(url,data,targetPath)