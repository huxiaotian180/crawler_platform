import logging.config
from util.kafka_consumer import kafka_consumer
from util.image_utils import test

if __name__ == "__main__":
    # 读取日志配置文件内容
    logging.config.fileConfig('logging.conf')

    # 创建一个日志器logger
    logger = logging.getLogger('root')

    logger.info('crawler platform start ...')

    kafka_consumer()


