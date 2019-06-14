from util.mysql_operate import update, select
import datetime

def update_upload_file_state(upload_file_status, task_seq_no):
    sql = "update craw_record set upload_file_status='{upload_file_status}' where " \
          "task_seq_no = '{task_seq_no}';".format(upload_file_status=upload_file_status, task_seq_no=task_seq_no)
    update(sql)


def get_image_type_by_rule_no(rule_no):
    sql = 'select image_buss_type, image_sub_type from rule_info' \
          ' where rule_no = "{rule_no}" ;'.format(rule_no=rule_no)
    return select(sql)


def updata_craw_state(updata_status,data,file_path_sql,pdf_file_name):
    end_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    sql = 'update craw_record set crawl_status="{crawl_status}",end_time="{end_time}", update_time="{update_time}" ' \
          ', report_file_path="{report_file_path}", report_file_name="{report_file_name}" ' \
          'where apply_no="{apply_no}" and task_seq_no= "{task_seq_no}";' \
        .format(crawl_status=updata_status, end_time=end_time, update_time=end_time, apply_no=data["apply_no"],
                task_seq_no=data["task_seq_no"], report_file_path=file_path_sql, report_file_name=pdf_file_name)
    update(sql)