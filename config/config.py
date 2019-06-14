# kafka 配置
kafka_conf = {
    'host': 'xxx.xxx.xxx.xxx:9092,xxx.xxx.xxx.xx:9092,xxx.xxx.x.xxx:9092',
    'topic': ['crawler_request', 'crawler_reports'],
    'group_id': 'craw'
}

# MySQL 配置
mysql_conf = {
    'host': '',
    'database': '',
    'user': '',
    'password': '',
    'charset': ''
}

images_conf = {
    'fileUpUrl': 'https://open.baixin.net.cn/wlzbimg/EasyScanAdapterAction_execute.action',
    'indexUpUrl': 'https://open.baixin.net.cn/wlzbimg/servlet/EasyScanServlet',
    'key': '04320dbced2d12969bc922d63d690edea78b0dd742b38c7a3bfa3b5a7b894b1b'
}

# 报告文件输出路径
file_target_path = r'C:\wkhtml\crawler_report'

# 聚信立HTML字体路径
file_fonts_path = r'C:\wkhtml\fonts'

# PDF解析路径
wkhtmltopdf_path = r'C:\wkhtml\wkhtmltopdf\bin\wkhtmltopdf.exe'
