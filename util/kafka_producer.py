from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config.config import kafka_conf
import json, datetime
import hashlib


class Kafka_producer():
    '''
    使用kafka的生产模块
    '''

    def __init__(self, kafkahost, kafkaport, kafkatopic):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
        ))

    def sendjsondata(self, params):
        try:
            parmas_message = json.dumps(params)
            producer = self.producer
            producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(e)


class Kafka_consumer():
    '''
    使用Kafka—python的消费模块
    '''

    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.groupid = groupid
        self.consumer = KafkaConsumer(self.kafkatopic, group_id=self.groupid,
                                      bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                                          kafka_host=self.kafkaHost,
                                          kafka_port=self.kafkaPort))

    def consume_data(self):
        try:
            for message in self.consumer:
                # print json.loads(message.value)
                yield message
        except KeyboardInterrupt as e:
            print(e)


def kafkaProduct(params):
    conf = kafka_conf
    producer = KafkaProducer(bootstrap_servers=conf['host'])

    try:
        parmas_message = json.dumps(params)
        producer = producer
        producer.send(conf['topic'][0], parmas_message.encode('utf-8'))
        producer.flush()
    except KafkaError as e:
        print(e)

def kafka_report(params):
    conf = kafka_conf
    producer = KafkaProducer(bootstrap_servers=conf['host'])

    try:
        parmas_message = json.dumps(params)
        producer = producer
        producer.send('crawler_reports', parmas_message.encode('utf-8'))
        producer.flush()
    except KafkaError as e:
        print(e)


if __name__ == '__main__':
    task_no = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    merch_no = '100000'
    merch_key = '8ujijg6tr4des345fgbv67yhuj890olk'
    md5 = hashlib.md5()
    md5.update((merch_no + task_no + merch_key).encode("utf8"))
    sign = md5.hexdigest()
    data = {
        "merch_no": "100000",
        "rule_group_no": "rule001",
        "task_no": task_no,
        "callback_url": "http://www.baixin.net.cn/a/b/",
        "aaaa":"222",
        "data": {
            "apply_no": "BC896453",
             "id_no": "232324197810116739",
    "name": "李海望2",
    "phone_no": "15811887778",
            "credit_report_list": [

            {
                "rule_no":"R0001",
                "url":"https://dev.juxinli.com/report/#/report?token=6a417219daee48a19603ef2e0083e8fd&org=GELS"
            },
            {
                "rule_no":"R0001",
                "url":"https://dev.juxinli.com/report/#/report?token=632d079dd7fc4e54b19e85f0d52e3575&org=GELS"
            },{
                    "rule_no": "R0007",
                    "url": "http://117.78.42.227:8080/api/report/py?applyNo=BC130840&method=PY_sqr&token=d08ca695-5142-4403-9894-215e001e2475"
                }
        ]
        },
        "sign": sign
    }
    kafkaProduct(data)


    """
    
    {
                    "rule_no": "R0001",
                    "url": "https://dev.juxinli.com/report/#/report?token=dc3c25d28db44245a28736d80ab87655&org=wanlong"
                },{
                    "rule_no": "R0002",
                    "url": "https://report.shujumohe.com/report/TASKYYS100000201903071447210700793743/2ACE0DADC20B471E87C5EDD8F240F10B"
                },{
                    "rule_no": "R0003",
                    "url": "http://117.78.42.227:8080/api/report/bairong?applyNo=BC130840&method=BAIRONG_sqr&token=3f8d55a8-5cc1-4b3e-a593-0fd8258ac187"
                },{
                    "rule_no": "R0004",
                    "url": "http://117.78.42.227:8080/api/report/ct91?applyNo=BC130840&method=91ZHENGXIN_sqr&token=b6f9b7cb-08b9-4510-84d3-7f84d0c22a02"
                },{
                    "rule_no": "R0005",
                    "url": "http://117.78.42.227:8080/api/report/tianxing?applyNo=BC130840&method=TXSK_sqr&token=952418b1-01f9-4158-abe9-0b2a9bcf3a5b"
                },{
                    "rule_no": "R0006",
                    "url": "http://117.78.42.227:8080/api/report/py?applyNo=BC130840&method=PY2_po&token=2cbe0fb4-77de-4c32-96d8-20a75128903f"
                },{
                    "rule_no": "R0007",
                    "url": "http://117.78.42.227:8080/api/report/py?applyNo=BC130840&method=PY_sqr&token=d08ca695-5142-4403-9894-215e001e2475"
                },{
                    "rule_no": "R0008",
                    "url": "http://117.78.42.227:8080/api/report/intelli?applyNo=BC130840&method=ZZC_sqr&token=870c6543-9ce9-41d1-ac80-964f5d35dee8"
                },{
                    "rule_no": "R0009",
                    "url": "http://117.78.28.76:8070/cp-service/v1/contract/spb/download?contractNo=BC130836&sign=336627E8587CD473FE7D5E2D0EBC92EC"
                },
    """