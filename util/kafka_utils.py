# coding=utf8
import datetime
from util.sql_utils import get_image_type_by_rule_no
from time import sleep
from util.kafka_producer import kafka_report
import logging


def send_report_kafka(data, file_path, file_name):
    logger = logging.getLogger('root')

    sleep(1)
    server_file_path = '/reports/' + datetime.datetime.now().strftime('%Y/%m/%d')

    result = get_image_type_by_rule_no(data['rule_no'])
    if len(result) == 0:
        logger.error('规则%s没有配置影像单证', data['rule_no'])
        return

    params = {
        'doc_code': data['apply_no'],
        'buss_type': result[0][0],
        'sub_type': result[0][1],
        'local_file_path': file_path,
        'local_file_name': file_name,
        'server_file_path': server_file_path,
        'task_seq_no': data['task_seq_no']
    }
    kafka_report(params)
