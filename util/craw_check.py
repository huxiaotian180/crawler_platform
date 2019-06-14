from selenium import webdriver
from util.mysql_operate import select,update
import time,datetime,hashlib
from lxml import etree
from util.kafka_producer import kafkaProduct
import logging


def check_craw_state():
    check_data = datetime.datetime.now().strftime('%Y%m%d')
    #跟新失败流水爬虫状态为0
    sql_in = "UPDATE craw_record a, (select task_seq_no from craw_record where task_no like '{data}%' and crawl_status in ('F') and rule_no not in ('R0009')) b SET a.crawl_status = '0' WHERE a.task_seq_no = b.task_seq_no;".format(data=check_data)
    update(sql_in)

    #获取需要重新爬虫的流水记录
    craw_sql = "select rule_no,task_url,apply_no,cust_name,id_no,phone_no,task_seq_no,crawl_status,re_upload_count from craw_record where  crawl_status ='0';"
    data = select(craw_sql)

    #跟新任务流水状态为1，处理中
    craw_now = "UPDATE craw_record a, (select rule_no,task_url,apply_no,cust_name,id_no,phone_no,task_seq_no,crawl_status from craw_record where  crawl_status ='0') b SET a.crawl_status = '1' WHERE a.task_seq_no = b.task_seq_no;"
    update(craw_now)

    for date in data:
        #客户征信信息
        dict_ckeck = {'rule_no': date[0],
                'craw_url': date[1],
                'apply_no': date[2],
                'cust_name':date[3],
                'cust_id':date[4],
                'phone':date[5],
                'task_seq_no':date[6],
                'crawl_status':date[7],
                're_upload_count':date[8]
        }
        #kafka报文
        task_no = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        merch_no = '100000'
        merch_key = '8ujijg6tr4des345fgbv67yhuj890olk'
        md5 = hashlib.md5()
        md5.update((merch_no + task_no + merch_key).encode("utf8"))
        sign = md5.hexdigest()
        data_kafka = {
            "merch_no": "100000",
            "rule_group_no": "rule001",
            "task_no": task_no,
            "callback_url": "http://www.baixin.net.cn/a/b/",
            "data": {
                "apply_no": dict_ckeck["apply_no"],
                "name": dict_ckeck["cust_name"],
                "id_no": dict_ckeck["cust_id"],
                "phone_no": dict_ckeck["phone"],
                "credit_report_list": [
                    {
                        "rule_no": dict_ckeck["rule_no"],
                        "url": dict_ckeck["craw_url"]
                    }
                ]
            },
            "sign": sign,
            "craw_repetition":{"task_seq_no":dict_ckeck['task_seq_no'],
                               "re_upload_count":dict_ckeck['re_upload_count']
                               }
        }
        count = data_kafka['craw_repetition']['re_upload_count']
        task_seq_no = data_kafka['craw_repetition']['task_seq_no']
        if count <= 3:
            kafkaProduct(data_kafka)
            #print('kafka报文发送成功')
        else:
            sql_re_upload = "UPDATE craw_record set crawl_status='F' where task_seq_no = '{task_seq}';".format(task_seq=task_seq_no)
            update(sql_re_upload)
            #print("爬取规则超过3次，无需爬取")


def upload_count(task_seq):
    #查询流程爬虫次数
    sql_count = "select re_upload_count from  craw_record where crawl_status = 'F' and task_seq_no = '{task_seq_no}';".format(task_seq_no = task_seq)
    data = select(sql_count)
    if len(data) > 0:
        #获取爬虫次数
        upload_count = data[0][0]
    else:
        upload_count = 0
    #print(upload_count)
    return upload_count



def repetition_report(apply_no,rule_no,url):
    #查询数据库流水是否存在该记录
    # sql_load = "select * from craw_record where apply_no = '{apply_no}' and rule_no = '{rule_no}' and task_url = '{task_url}';".format(apply_no=apply_no,rule_no=rule_no,task_url = url)
    # data_load = select(sql_load)
    # #print(sql_load)

    if rule_no == 'R0001':
        #聚信力重复数据判断规则
        sql_load = "select * from craw_record where apply_no = '{apply_no}' and rule_no = '{rule_no}' and task_url = '{task_url}';".format(apply_no=apply_no,rule_no=rule_no,task_url = url)
        data_load = select(sql_load)

    else:
        sql_load = "select * from craw_record where apply_no = '{apply_no}' and rule_no = '{rule_no}';".format(apply_no=apply_no, rule_no=rule_no)
        data_load = select(sql_load)

    if  len(data_load) > 0 :

        return True
    else:
        return False



# #check_craw_state()
#
# flag = repetition_report('BC132586','R0002')
# print(flag)