import time,re
from lxml import etree
import requests
from util.mysql_operate import select,insert,update
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=20)


def craw_page_count(url_end):

    #获取源码
    url = "https://www.guazi.com/"+url_end
    print(url)
    payload = ""
    headers = {
        'Host': "www.guazi.com",
        'Connection': "keep-alive",
        'Pragma': "no-cache",
        'Cache-Control': "no-cache",
        'Upgrade-Insecure-Requests': "1",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'Accept-Encoding': "gzip, deflate, br",
        'Accept-Language': "zh-CN,zh;q=0.9,und;q=0.8",
        'Cookie': "uuid=ec79ea93-4303-4055-d98e-dbe4f1efa481; antipas=095e25390y7j5q95071242951; clueSourceCode=10103000312%2300; ganji_uuid=6849016731480343009252; sessionid=d5917c16-af36-4458-ee25-aa5d659d4929; lg=1; _gl_tracker=%7B%22ca_source%22%3A%22-%22%2C%22ca_name%22%3A%22-%22%2C%22ca_kw%22%3A%22-%22%2C%22ca_id%22%3A%22-%22%2C%22ca_s%22%3A%22self%22%2C%22ca_n%22%3A%22-%22%2C%22ca_i%22%3A%22-%22%2C%22sid%22%3A35817864454%7D; cainfo=%7B%22ca_s%22%3A%22pz_baidu%22%2C%22ca_n%22%3A%22tbmkbturl%22%2C%22ca_medium%22%3A%22-%22%2C%22ca_term%22%3A%22-%22%2C%22ca_content%22%3A%22%22%2C%22ca_campaign%22%3A%22%22%2C%22ca_kw%22%3A%22%25e7%2593%259c%25e5%25ad%2590%25e4%25ba%258c%25e6%2589%258b%25e8%25bd%25a6%22%2C%22keyword%22%3A%22-%22%2C%22ca_keywordid%22%3A%22-%22%2C%22scode%22%3A%2210103000312%22%2C%22ca_transid%22%3A%22%22%2C%22platform%22%3A%221%22%2C%22version%22%3A1%2C%22ca_i%22%3A%22-%22%2C%22ca_b%22%3A%22-%22%2C%22ca_a%22%3A%22-%22%2C%22display_finance_flag%22%3A%22-%22%2C%22client_ab%22%3A%22-%22%2C%22guid%22%3A%22ec79ea93-4303-4055-d98e-dbe4f1efa481%22%2C%22sessionid%22%3A%22d5917c16-af36-4458-ee25-aa5d659d4929%22%7D; cityDomain=jian; user_city_id=221; preTime=%7B%22last%22%3A1555036136%2C%22this%22%3A1555034211%2C%22pre%22%3A1555034211%7D",
        'cache-control': "no-cache",
        'Postman-Token': "92c2f771-d938-48cb-bd92-3a2700b15818"
    }
    response = requests.request("GET", url, data=payload, headers=headers)
    page_source=response.text

    url_xpath = "/html/body/div[6]/div[7]"
    #获取页面源代码
    tree = etree.HTML(page_source)
    try:
        # 获取汽车信息
        url_list = list((tree.xpath(url_xpath + '//text()')))
        # 获取页面页数
        page_url_count = str(url_list[-3])
        if  page_url_count.isdigit():
            count = int(page_url_count)
        else:
            count = 1
        print("共需要爬取页面为{count_craw}".format(count_craw=count))
        return count
    except Exception as e:
        return 0


def craw_url_info(url):
    try:
        payload = ""
        headers = {
            'Host': "www.guazi.com",
            'Connection': "keep-alive",
            'Pragma': "no-cache",
            'Cache-Control': "no-cache",
            'Upgrade-Insecure-Requests': "1",
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
            'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9,und;q=0.8",
            'Cookie': "uuid=ec79ea93-4303-4055-d98e-dbe4f1efa481; antipas=095e25390y7j5q95071242951; clueSourceCode=10103000312%2300; ganji_uuid=6849016731480343009252; sessionid=d5917c16-af36-4458-ee25-aa5d659d4929; lg=1; _gl_tracker=%7B%22ca_source%22%3A%22-%22%2C%22ca_name%22%3A%22-%22%2C%22ca_kw%22%3A%22-%22%2C%22ca_id%22%3A%22-%22%2C%22ca_s%22%3A%22self%22%2C%22ca_n%22%3A%22-%22%2C%22ca_i%22%3A%22-%22%2C%22sid%22%3A35817864454%7D; cainfo=%7B%22ca_s%22%3A%22pz_baidu%22%2C%22ca_n%22%3A%22tbmkbturl%22%2C%22ca_medium%22%3A%22-%22%2C%22ca_term%22%3A%22-%22%2C%22ca_content%22%3A%22%22%2C%22ca_campaign%22%3A%22%22%2C%22ca_kw%22%3A%22%25e7%2593%259c%25e5%25ad%2590%25e4%25ba%258c%25e6%2589%258b%25e8%25bd%25a6%22%2C%22keyword%22%3A%22-%22%2C%22ca_keywordid%22%3A%22-%22%2C%22scode%22%3A%2210103000312%22%2C%22ca_transid%22%3A%22%22%2C%22platform%22%3A%221%22%2C%22version%22%3A1%2C%22ca_i%22%3A%22-%22%2C%22ca_b%22%3A%22-%22%2C%22ca_a%22%3A%22-%22%2C%22display_finance_flag%22%3A%22-%22%2C%22client_ab%22%3A%22-%22%2C%22guid%22%3A%22ec79ea93-4303-4055-d98e-dbe4f1efa481%22%2C%22sessionid%22%3A%22d5917c16-af36-4458-ee25-aa5d659d4929%22%7D; cityDomain=jian; user_city_id=221; preTime=%7B%22last%22%3A1555036136%2C%22this%22%3A1555034211%2C%22pre%22%3A1555034211%7D",
            'cache-control': "no-cache"
        }
        response = requests.request("GET", url, data=payload, headers=headers)
        page_source = response.text

        # 匹配信息
        pattern = re.compile(r'/.{2,10}/.{17}.htm')
        match_list = re.findall(pattern, page_source)
        return match_list
    except Exception as e:
        return []



def main(url):
    #获取爬虫url
    craw_host = "https://www.guazi.com/"
    sql_select = 'select craw_url from craw_guazi_car_info where state=0 limit 0,1;'
    data = select(sql_select)
    if len(data) == 1:
        craw_url = data[0][0]
        sql_updata = "update craw_guazi_car_info set state=1 where craw_url='{url}';".format(url=craw_url)
        update(sql_updata)

        url_end = str(craw_url).strip('/#bread')
        count_url = craw_page_count(url_end)
        url = craw_host + url_end

    for i in range(1,count_url+1):
        print("正在爬取第{count}页面".format(count = i))
        craw_url_link = url+'/o{count}/'.format(count=i)
        print(craw_url_link)
        car_list = craw_url_info(craw_url_link)
        for line in car_list:
            #爬取url插入数据库
            #a.append(line)
            sql_url = "INSERT INTO craw_url_info ( craw_url) VALUES ( '{url}');".format(url=line)
            insert(sql_url)
        print("爬取结束第{count}页面".format(count=i))
    #更新流水成功
    sql_updata_suss = "update craw_guazi_car_info set state=2 where craw_url='{url}';".format(url=craw_url)
    update(sql_updata_suss)

#/zhuhai/699b5a230d07ffafx.htm

if __name__ == "__main__":
    # while True:
    #     url="https://www.guazi.com"
    #     executor.submit(main,url)
    #     time.sleep(1)

    url = "https://www.guazi.com"
    main(url)





