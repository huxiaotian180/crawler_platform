from credit_report.gong_xin_bao import crew_gxb
from credit_report.shu_ju_mo_he import crew_shujumohe
from credit_report.ju_xin_li import crew_juxinli
from credit_report.bx_spb import crew_spb
from credit_report.bai_rong_inc import crew_bairong
from credit_report.tian_xing_shu_ke import crew_txsk
from credit_report.py_credit_applicant import crew_py
from credit_report.py_credit_card_auth import crew_card_auth
from credit_report.intelli_credit import crew_zhongzc
from credit_report.credit_91 import crew_91
from concurrent.futures import ThreadPoolExecutor
from credit_report.bx_ceg import crew_ceg


executor = ThreadPoolExecutor(max_workers=100)


def route(logger,method,url, sql_dict, file_target_path):
    if "craw_juxinli" == method:
        logger.info("聚信立开始爬虫")
        executor.submit(crew_juxinli, url, sql_dict, file_target_path)
        # wait(task, return_when=FIRST_COMPLETED)
        logger.info("聚信立爬虫结束")
    elif "craw_shujumohe" == method:
        logger.info("数据魔盒开始爬虫")
        executor.submit(crew_shujumohe, url, sql_dict, file_target_path)
        logger.info("数据魔盒爬虫结束")
    elif "craw_baorong" == method:
        logger.info("百融开始爬虫")
        executor.submit(crew_bairong, url, sql_dict, file_target_path)
        logger.info("百融爬虫结束")
    elif "craw_txsk" == method:
        logger.info("天行数科开始爬虫")
        executor.submit(crew_txsk, url, sql_dict, file_target_path)
        logger.info("天行数科爬虫结束")
    elif "craw_py" == method:
        logger.info("鹏元开始爬虫")
        executor.submit(crew_py, url, sql_dict, file_target_path)
        logger.info("鹏元爬虫结束")
    elif "craw_card_auth" == method:
        logger.info("鹏元开始爬虫")
        executor.submit(crew_card_auth, url, sql_dict, file_target_path)
        logger.info("鹏元爬虫结束")
    elif "craw_zhongzc" == method:
        logger.info("中智成开始爬虫")
        executor.submit(crew_zhongzc, url, sql_dict, file_target_path)
        logger.info("中智成爬虫结束")
    elif "craw_91" == method:
        logger.info("91征信开始爬虫")
        executor.submit(crew_91, url, sql_dict, file_target_path)
        logger.info("91征信爬虫结束")
    elif "craw_bx_spb" == method:
        logger.info("业务审批表开始爬虫")
        executor.submit(crew_spb, url, sql_dict, file_target_path)
        logger.info("业务审批表爬虫结束")
    elif "craw_bx_ceg" == method:
        logger.info("业务车e估报告开始爬虫")
        executor.submit(crew_ceg, url, sql_dict, file_target_path)
        logger.info("业务车e估报告爬虫结束")
    # elif "craw_gxb" == method:
    #     logger.info("公信宝报告开始爬虫")
    #     executor.submit(crew_gxb, url, sql_dict, file_target_path)
    #     logger.info("公信宝报告爬虫结束")
    else:
        logger.info("参数错误")