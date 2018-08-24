#загружаем типы населенных пунктов и их сокращения с сайта
#http://www.oktmo.ru/municipality_registry/
import requests
import pandas as pd
from configobj import ConfigObj
from bs4 import BeautifulSoup
import sqlite3

okrug_limit = 1 # если -1 то обрабатываем все федеральные округа, иначе только указанный

def get_html(url, proxy=None):
    try:
        response = requests.get(url, proxies=proxy)
    except requests.exceptions.ConnectionError as e:
        print(e)
    return response.text

def get_html_option(url, proxy=None, option=None):
    try:
        response = requests.get(url, proxies=proxy, params=option)
    except requests.exceptions.ConnectionError as e:
        print(e)
    return response.text

def get_list(html, Tag=None, option=None):
    soup = BeautifulSoup(html, 'lxml')
    spisok = soup.find_all(Tag, option)
    return spisok

def get_okrug_id(name_str):
    try:
        conn = sqlite3.connect('oktmo.db3')
        cur = conn.cursor()
        sql = 'select id from okrug where text = "%s"' % name_str
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            sql = '''insert into okrug (text) values ("%s");''' % (name_str)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from okrug where text = "%s"' % name_str
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def get_region_id(okrug_id,name_str,short_name, cod_region):
    try:
        conn = sqlite3.connect('oktmo.db3')
        cur = conn.cursor()
        sql = 'select id from region where text = "%s"' % name_str
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            sql = '''update region
                set okrug_id = %s,
                text = "%s",
                short_name = "%s",
                cod = "%s"
                where id = %s
            ''' % (okrug_id, name_str, short_name, cod_region, r[0])
            cur.execute(sql)
            conn.commit()
            return r[0]
        else:
            sql = '''insert into region (okrug_id,text,short_name,cod) values (%s,"%s","%s","%s");''' % (okrug_id, name_str, short_name, cod_region)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from region where text = "%s"' % name_str
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def update_municipalitet(region_id, text, cod):
    try:
        conn = sqlite3.connect('oktmo.db3')
        cur = conn.cursor()
        sql = 'select id from mo where cod = "%s" and region_id=%s' % (cod, region_id)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            sql = '''update mo
                set region_id = %s,
                text = "%s",
                cod = "%s"
                where id = %s
            ''' % (region_id, text, cod, r[0])
            cur.execute(sql)
            conn.commit()
            return r[0]
        else:
            sql = '''insert into mo (region_id,text,cod) values (%s,"%s","%s");''' % (region_id, text, cod)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from mo where cod = "%s" and region_id=%s' % (cod, region_id)
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def update_np(region_id, text, cod, tnp):
    try:
        conn = sqlite3.connect('oktmo.db3')
        cur = conn.cursor()
        sql = 'select id from np where cod = "%s" and region_id=%s' % (cod, region_id)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            sql = '''update np
                set region_id = %s,
                name = "%s",
                cod = "%s",
                tnp_id = %s
                where id = %s
            ''' % (region_id, text, cod, tnp, r[0])
            cur.execute(sql)
            conn.commit()
            return r[0]
        else:
            sql = '''insert into np (region_id,name,cod,tnp_id) values (%s,"%s","%s",%s);''' % (region_id, text, cod, tnp)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from np where cod = "%s" and region_id=%s' % (cod, region_id)
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def get_tnp(text):
    try:
        conn = sqlite3.connect('oktmo.db3')
        cur = conn.cursor()
        sql = 'select id, short_name from tnp where full_name = "%s"' % (text)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return {0: r[0], 1: r[1]}
        else:
            return None
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()


def main():
    config = ConfigObj('call.cfg')
    proxy_list = config['Proxy']['Proxy']
    okrug_id = 0
    url = 'http://www.oktmo.ru/municipality_registry/'
    proxy = {'http': 'http://' + proxy_list}
    html = get_html(url, proxy)
    #print(html)
    t = get_list(html, 'div', {"id": "container"})
    check = False
    for row in t[0]:
        if check:
            #обработка
            if row.name == 'p' and row.next.name == 'b':
                text = row.contents[1]
                cod = row.contents[0].next.contents[0][:-3]
                #получаем аналитику - количество и типы населенных пунктов в субъекте РФ
                url2 = 'http://www.oktmo.ru/analytics_localitytypesbysubjects/?subject=' + cod
                html2 = get_html(url2, proxy)
                t2 = get_list(html2, 'h2')[0].text #короткое наименование субъекта РФ
                region_id = get_region_id(okrug_id, text, t2, cod)
                #проверяем условие на лимит обработки по ФО
                if okrug_limit != -1:
                    if okrug_id == okrug_limit:
                        run = True
                    else:
                        run = False
                else:
                    run = True

                if run:
                    #Получаем список муниципальных образований субъекта РФ
                    url3 = 'http://www.oktmo.ru/municipality_registry/?code=' + cod
                    html3 = get_html(url3, proxy)
                    t4 = get_list(html3, 'div', {"id": "container"})
                    for mo_row in t4[0]:
                        if mo_row.name == 'p' and mo_row.next.name == 'b':
                            mo_text = mo_row.contents[1].strip()
                            mo_cod = mo_row.contents[0].next[:-4]
                            update_municipalitet(region_id, mo_text, mo_cod)
                            print(mo_row)

                    #список типов н.п. и их количество
                    t3 = get_list(html2, 'table')[1]
                    df = pd.read_html(str(t3))[0]
                    df.columns = ['count', 'name']
                    snames = []
                    tnp_id = []
                    #добавляем в список столбцы - коротное название типа н.п. и его ID
                    for tnp_row in df['name']:
                        tnp = get_tnp(tnp_row)
                        if tnp != None:
                            snames.append(tnp[1])
                            tnp_id.append(tnp[0])
                        else:
                            snames.append('')
                            tnp_id.append(0)
                    df['sname'] = snames
                    df['id'] = tnp_id

                    #обрабатываем населенные пункты субъектов РФ
                    url4 = 'http://www.oktmo.ru/locality_registry/?code=' + cod.ljust(11,"0")
                    html4 = get_html(url4, proxy)
                    t5 = get_list(html4, 'div', {"id": "container"})
                    for np_row in t5[0]:
                        if np_row.name == 'p' and np_row.next.name == 'b':
                            np_text = np_row.contents[1].strip()
                            np_cod = np_row.contents[0].next[0:11]
                            for tnp_row in df.values:
                                #ищем вхождение короткого имени типа в название НП
                                #print(tnp_row)
                                #print(np_text.find(tnp_row[2]))
                                if np_text.find(tnp_row[2]) == 0 and np_text[len(tnp_row[2])+1:].find('.') == -1:
                                    #print('есть вхождение')
                                    np_text = np_text[len(tnp_row[2])+1:]
                                    np_id = tnp_row[3]
                                    break
                            update_np(region_id, np_text, np_cod, np_id)
                            print(np_row)
                print(row)
            else:
                check = False
                #break
        else:
            if row.name == 'h2' and row.next.name == None:
                #нашли федеральный округ
                check = True
                print(row)
                okrug_id = get_okrug_id(row.text)

    #print(t)

if __name__ == '__main__':
    main()
