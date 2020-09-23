# 基于Python+Flask+Echarts
# 122.51.251.91
# 1.Python网络爬虫
# 2.使用python与mysql交互
# 3.使用Flask构建web项目
# 4.基于Echarts数据可视化展示
# 5.在Linux上部署web项目及爬虫
import requests
import json
import time
import pymysql
import traceback

from bs4 import BeautifulSoup


def get_conn():
    # 建立连接
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='1234',
                           db='cov', charset='utf8')
    # #创建游标，默认是元组型
    cursor = conn.cursor()
    return conn, cursor


def close_conn(conn, cursor):
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def update_detail():
    cursor = None
    conn = None
    try:
        li = get_tencent_data()[1]  # 0是历史数据字典，1是最新详细数据列表
        conn, cursor = get_conn()
        sql = 'insert into details (update_time, province, city, confirm, confirm_add, heal, dead) values (%s, %s, %s, %s, %s, %s, %s)'
        sql_query = 'select %s = (select update_time from details order by id desc limit 1)'  # 对比当前最大时间戳
        cursor.execute(sql_query, li[0][0])
        if not cursor.fetchone()[0]:
            print(f'{time.asctime()}开始更新最新数据')
            for item in li:
                cursor.execute(sql, item)
            conn.commit  # 提交事务 update delete insert操作
            print(f'{time.asctime()}更新最新数据完毕')
        else:
            print(f'{time.asctime()}已是最新数据!')
    except:
        traceback.print_exc()

    finally:
        close_conn(conn, cursor)


# res = cursor.fetchall()
# print(res)


def insert_history():
    cursor = None
    conn = None
    try:
        dic = get_tencent_data()[0]  # 0是历史数据字典，1是最新详细数据列表
        print(f'{time.asctime()}开始插入历史数据')
        conn, cursor = get_conn()
        sql = 'insert into history values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for k, v in dic.items():
            # item格式：{'2020-01-13':{'confirm':41,'suspect':0,'heal' : 0,'dead':1}
            cursor.execute(sql, [k, v.get('confirm'), v.get('confirm_add'), v.get('suspect'), v.get('suspect_add'),
                                 v.get('heal'), v.get('heal_add'), v.get('dead'), v.get('dead_add')])
        conn.commit  # 提交事务 update delete insert操作
        print(f'{time.asctime()}插入历史数据完毕')

    except:
        traceback.print_exc()
    finally:
        close_conn(conn, cursor)


def update_history():
    cursor = None
    conn = None
    try:
        dic = get_tencent_data()[0]  # 0是历史数据字典，1是最新详细数据列表
        print(f'{time.asctime()}开始更新历史数据')
        conn, cursor = get_conn()
        sql = 'insert into history values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        sql_query = 'select confirm from history where ds = %s'
        for k, v in dic.items():
            # item格式：{'2020-01-13':{'confirm':41,'suspect':0,'heal' : 0,'dead':1}
            if not cursor.execute(sql_query, k):
                cursor.execute(sql, [k, v.get('confirm'), v.get('confirm_add'), v.get('suspect'), v.get('suspect_add'),
                                     v.get('heal'), v.get('heal_add'), v.get('dead'), v.get('dead_add')])
        conn.commit  # 提交事务 update delete insert操作
        print(f'{time.asctime()}历史数据更新完毕')

    except:
        traceback.print_exc()
    finally:
        close_conn(conn, cursor)


def get_tencent_data():
    url1 = 'https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5'
    url2 = 'https://view.inews.qq.com/g2/getOnsInfo?name=disease_other'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15'}

    r = requests.get(url2, headers)
    s = requests.get(url1, headers)
    res1 = json.loads(r.text)  # 转换成字典形式
    res2 = json.loads(s.text)
    data_all = json.loads(res1['data'])
    datas = json.loads(res2['data'])

    history = {}  # 历史数据
    for i in data_all['chinaDayList']:
        ds = '2020.' + i['date']
        tup = time.strptime(ds, "%Y.%m.%d")
        ds = time.strftime("%Y-%m-%d", tup)  # 改变时间格式，不然插入数据库会报错，数据库是datetime型
        confirm = i['confirm']
        suspect = i['suspect']
        heal = i['heal']
        dead = i['dead']
        history[ds] = {'confirm': confirm, 'suspect': suspect, 'heal': heal, 'dead': dead}

    for i in data_all['chinaDayAddList']:
        ds = '2020.' + i['date']
        tup = time.strptime(ds, "%Y.%m.%d")
        ds = time.strftime("%Y-%m-%d", tup)
        confirm = i['confirm']
        suspect = i['suspect']
        heal = i['heal']
        dead = i['dead']
        history[ds].update({'confirm_add': confirm, 'suspect_add': suspect, 'heal_add': heal, 'dead_add': dead})

    details = []  # 当日详细数据
    update_time = datas['lastUpdateTime']
    data_province = datas['areaTree'][0]['children']
    for pro_infos in data_province:
        province = pro_infos['name']  # 省名
        for city_infos in pro_infos['children']:
            city = city_infos['name']
            confirm = city_infos['total']['confirm']
            confirm_add = city_infos['today']['confirm']
            heal = city_infos['total']['heal']
            dead = city_infos['total']['dead']
            details.append([update_time, province, city, confirm, confirm_add, heal, dead])
    return history, details


def get_xinlang_hot():
    url = 'https://s.weibo.com/top/summary?cate=realtimehot'
    res = requests.get(url)
    res.encoding = 'utf-8'
    html = res.text
    soup = BeautifulSoup(html, 'lxml')
    s = soup.find_all('td', class_='td-02')
    list = []
    for i in s:
        mail = i.text.strip()
        list.append(mail)
    return list


def update_hotsearch():
    cursor = None
    conn = None
    try:
        text = get_xinlang_hot()
        print(f'{time.asctime()}开始更新热搜数据')
        conn, cursor = get_conn()
        sql = 'insert into hotsearch(dt, content) values (%s, %s)'
        ts = time.strftime("%Y-%m-%d %X")
        for i in text:
            cursor.execute(sql, (ts, i))
        conn.commit  # 提交事务 update delete insert操作
        print(f'{time.asctime()}数据更新完毕')

    except:
        traceback.print_exc()
    finally:
        close_conn(conn, cursor)


update_hotsearch()