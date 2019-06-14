import time, json
from config.config import kafka_conf, file_target_path
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import datetime, hashlib
from util.mysql_operate import select, insert,update
from credit_report.shu_ju_mo_he import crew_shujumohe
from credit_report.ju_xin_li import crew_juxinli
from credit_report.bx_spb import crew_spb
from credit_report.bai_rong_inc import crew_bairong
from credit_report.tian_xing_shu_ke import crew_txsk
from credit_report.py_credit_applicant import crew_py
from credit_report.py_credit_card_auth import crew_card_auth
from credit_report.intelli_credit import crew_zhongzc
from credit_report.credit_91 import crew_91
from kafka import KafkaConsumer,KafkaProducer
import traceback
import logging,uuid
from util.image_utils import upload_credit_reports
from concurrent.futures import ThreadPoolExecutor
from util.sql_utils import update_upload_file_state
from credit_report.bx_ceg import crew_ceg
from util.kafka_producer import kafkaProduct
from util.craw_check import check_craw_state,repetition_report
from util.craw_route import route

executor = ThreadPoolExecutor(max_workers=100)


def kafka_consumer():
    # 创建一个日志器logger
    logger = logging.getLogger('root')

    conf = kafka_conf
    consumer = KafkaConsumer(bootstrap_servers=conf['host'], group_id=conf['group_id'])
    logger.info('connection kafka start...')
    consumer.subscribe((conf['topic']))
    logger.info('connection kafka successful...')
    #check_craw_state()
    while True:
        try:
            for message in consumer:
                #获取失败流水记录发送报文
                check_craw_state()
                data = message.value.decode('utf-8')
                logger.debug("kafka解析参数%s",data)
                if  message.topic == 'crawler_request':
                    executor.submit(request, data)
                elif message.topic == 'crawler_reports':
                    executor.submit(report, data)
                    logger.info('文件上传结束')
                else:
                    logger.warning('消息无法处理')

        except Exception as e:
            logger.info(e)
            traceback.print_exc()


def report(data):
    data_dict = json.loads(data)
    doc_code = data_dict['doc_code']
    buss_type = data_dict['buss_type']
    sub_type = data_dict['sub_type']
    local_file_path = data_dict['local_file_path']
    local_file_name = data_dict['local_file_name']
    server_file_path = data_dict['server_file_path']
    task_seq_no = data_dict["task_seq_no"]
    # 文件上传次数
    count_upload = 1
    while count_upload <= 3:

        update_upload_file_state('P', task_seq_no)

        result = upload_credit_reports(doc_code, buss_type, sub_type, local_file_path, local_file_name, server_file_path)

        status_code = result.status_code

        if status_code == 200:
            update_upload_file_state('S', task_seq_no)
            logging.info("任务[%s]文件上传成功", task_seq_no)
            break
        else:
            logging.info("任务%s第%s次文件上传失败", task_seq_no, count_upload)
            count_upload = count_upload + 1
            update_upload_file_state('F', task_seq_no)


def request(data):
    logger = logging.getLogger('root')
    data_dict = json.loads(data)

    # 商户秘钥
    merch_sign = data_dict["sign"]
    # 任务编号
    task_no = data_dict["task_no"]
    # 商户
    merch_no = data_dict["merch_no"]
    # 回调地址
    callback_url = data_dict["callback_url"]

    # 商户请求参数
    paramers = data_dict["data"]
    # 客户姓名
    cust_name = paramers["name"]
    # 申请编号
    apply_no = paramers["apply_no"]
    # 身份证号
    id_no = paramers["id_no"]
    # 电话信息
    phone_no = paramers["phone_no"]
    # 爬虫信息task和url
    credit_report_list = paramers["credit_report_list"]

    # 爬虫状态
    crawl_status = 'N'

    # 文件上传影像平台状态
    upload_file_status = 'N'

    data = select(
        "SELECT MERCH_KEY from merch_info WHERE MERCH_NO = '{merch_no}';".format(merch_no=merch_no))
    if len(data) <= 0:
        logger.info('商户不存在')
        return

    merch_key = data[0][0]

    md5 = hashlib.md5()
    md5.update((merch_no + task_no + merch_key).encode("utf8"))
    sign = md5.hexdigest()

    if merch_sign != sign:
        logger.info('签名不匹配')
        return

    # 签名验证通过后获取爬虫URL，爬虫信息
    for crawl_info in credit_report_list:
        rule_no = crawl_info["rule_no"]
        url = crawl_info["url"]
        create_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        # 任务流水号使用uuid
        uid = str(uuid.uuid4())
        suid = ''.join(uid.split('-'))
        task_seq_no = suid

        sql_dict = {
            "task_no": task_no,
            "task_seq_no": task_seq_no,
            "merch_no": merch_no,
            "rule_no": rule_no,
            "task_url": url,
            "apply_no": apply_no,
            "cust_name": cust_name,
            "id_no": id_no,
            "phone_no": phone_no,
            "callback_url": callback_url,
            "crawl_status": crawl_status,
            "upload_file_status": upload_file_status,
            "create_time": create_time,
            "update_time": create_time,
            "start_time": create_time,
            "end_time": create_time
        }

        # 文件序号
        file_seq_no = select_file_no(sql_dict)
        sql_dict["file_seq_no"] = file_seq_no


        #失败报文重新爬取机制
        if  "craw_repetition" in data_dict.keys():
            logger.info("重爬机制触发")

            task_seq_no = data_dict['craw_repetition']['task_seq_no']
            #跟新task流水号
            sql_dict['task_seq_no'] = task_seq_no

            #查询爬虫规则
            sql = "update craw_record set file_seq_no = '{file_seq_no}' where task_seq_no = '{task_seq_no}';".format(file_seq_no = file_seq_no,task_seq_no=task_seq_no)
            moth_rule = sql_dict["rule_no"]

            #获取文件重爬次数
            re_count = data_dict['craw_repetition']['re_upload_count']
            re_upload_count = re_count+1

            #更新上传次数
            sql_upload_count = "UPDATE craw_record set re_upload_count={re_upload_count} where task_seq_no = '{task_no}';".format(re_upload_count = re_upload_count,task_no = task_seq_no)
            update(sql_upload_count)


            # 签名通过后执行爬虫任务，获取执行爬虫脚本
            sql = "select METHODS from rule_info where RULE_NO='{rule_no}';".format(rule_no=rule_no)
            data = select(sql)
            if len(data) <= 0:
                logger.info('没有找到对应规则')
                break
            method = data[0][0]
            #调用路由规则
            route(logger,method,url, sql_dict, file_target_path)
            break

        #判断是否重复爬取
        flag = repetition_report(apply_no, rule_no,url)
        #print(flag)
        if  flag:
            print('报告存在无需爬取')
            continue

        sql = "INSERT INTO craw_record " \
              "( task_no,task_seq_no,merch_no,rule_no,task_url,apply_no,cust_name,id_no,phone_no,file_seq_no,callback_url,crawl_status,upload_file_status,create_time,update_time,start_time,end_time) " \
              "VALUES" \
              "( '{task_no}', '{task_seq_no}','{merch_no}','{rule_no}','{task_url}','{apply_no}','{cust_name}','{id_no}','{phone_no}','{file_seq_no}','{callback_url}' ,'{crawl_status}','{upload_file_status}','{create_time}','{update_time}','{start_time}','{end_time}');" \
            .format(task_no=sql_dict["task_no"],
                    task_seq_no=sql_dict["task_seq_no"],
                    merch_no=sql_dict["merch_no"],
                    rule_no=sql_dict["rule_no"],
                    task_url=sql_dict["task_url"],
                    apply_no=sql_dict["apply_no"],
                    cust_name=sql_dict["cust_name"],
                    id_no=sql_dict["id_no"],
                    phone_no=sql_dict["phone_no"],
                    file_seq_no=sql_dict["file_seq_no"],
                    callback_url=sql_dict["callback_url"],
                    crawl_status=sql_dict["crawl_status"],
                    upload_file_status=sql_dict["upload_file_status"],
                    create_time=sql_dict["create_time"],
                    update_time=sql_dict["update_time"],
                    start_time=sql_dict["start_time"],
                    end_time=sql_dict["end_time"])
        insert(sql)

        # 签名通过后执行爬虫任务，获取执行爬虫脚本
        sql = "select METHODS from rule_info where RULE_NO='{rule_no}';".format(rule_no=rule_no)
        data = select(sql)
        if len(data) <= 0:
            logger.info('没有找到对应规则')
            break

        method = data[0][0]
        # 调用路由规则
        route(logger, method, url, sql_dict, file_target_path)


def select_file_no(data):
    # 获取文件序号
    file_no_sql = 'select file_seq_no from craw_record where apply_no = "{apply_no}" AND rule_no = "{rule_no}" ORDER BY create_time DESC LIMIT 0,1;'.format(
        apply_no=data["apply_no"], rule_no=data["rule_no"])
    data = select(file_no_sql)
    if len(data) == 0:
        return 100
    file_no = int(data[0][0]) + 1
    return file_no
