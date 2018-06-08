#загружаем типы населенных пунктов и их сокращения с сайта
#http://www.oktmo.ru/list_localitytypes/
import requests
import pandas as pd
from configobj import ConfigObj
from bs4 import BeautifulSoup
import sqlite3

def get_html(url, proxy=None):
    try:
        response = requests.get(url, proxies=proxy)
    except requests.exceptions.ConnectionError as e:
        print(e)
    return response.text
def get_list(html):
    soup = BeautifulSoup(html, 'lxml')
    spisok = soup.find('div', {"id": "container"})#.find_next_sibling('table')
    df = pd.read_html(str(spisok))
    return df

def get_tnp_id(short_name_str, full_name_str):
    try:
        conn = sqlite3.connect('oktmo.db3')
        cur = conn.cursor()
        sql = 'select id from tnp where short_name = "%s"' % short_name_str
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            sql = '''insert into tnp (short_name,full_name) values ("%s", "%s");''' % (short_name_str, full_name_str)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from tnp where short_name = "%s"' % short_name_str
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def main():
    config = ConfigObj('call.cfg')
    proxy_list = config['Proxy']['Proxy']
    url = 'http://www.oktmo.ru/list_localitytypes/'
    proxy = {'http': 'http://' + proxy_list}
    html = get_html(url, proxy)
    #print(html)
    t = get_list(html)
    for row in t[0].values:
        tnp_id = get_tnp_id(row[0].strip(), row[1].strip())
        print(tnp_id)
    print(t)

if __name__ == '__main__':
    main()