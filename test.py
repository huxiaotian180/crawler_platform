from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import time

import pdfkit

'''

def get_html(times):
    time.sleep(times)
    print("get page {} success".format(times))
    return times

executor = ThreadPoolExecutor(max_workers=5)
# 通过submit函数提交执行的函数到线程池中，submit是立即返回
# task1 = executor.submit(get_html,(5))
# task2 = executor.submit(get_html,(4))
# task3 = executor.submit(get_html,(1))
# time.sleep(6)
urls = [1, 1.5, 1.9]
all_task = [executor.submit(get_html,url) for url in urls]
print(all_task)
wait(all_task, return_when=FIRST_COMPLETED)
for future in as_completed(all_task):
    data = future.done()
    print("get page {}".format(data))
'''


pdfkit.from_string(r"C:\crawler_report\12.html",r"X:\crawler_platform\logs\1.pdf")
