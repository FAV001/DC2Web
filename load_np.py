# Загрузка область, район, муниципальное образование, населенный пункт из файла из ССФО в базу
import csv
from configobj import ConfigObj
import pandas as pd
import sqlite3


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)
    return None

def create_region(conn,region_str):
    sql = '''insert into region (text) value "?"'''
    cur = conn.cursor()
    cur.execute(sql,region_str)
    return cur.lastrowid


def get_region_id(region_str):
    try:
        conn = sqlite3.connect('ssfo.db3')
        cur = conn.cursor()
        sql = 'select id from region where text = "%s"' % region_str
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            sql = '''insert into region (text) values ("%s");''' % region_str
            cur.execute(sql)
            conn.commit()
            sql = 'select id from region where text = "%s"' % region_str
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def get_municipalitet_id(region_id, municipalitet_str):
    try:
        conn = sqlite3.connect('ssfo.db3')
        cur = conn.cursor()
        sql = 'select id from municipalitet where region_id = %s and text = "%s"' % (region_id, municipalitet_str)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            sql = '''insert into municipalitet (region_id,text) values (%s,"%s");''' % (region_id,municipalitet_str)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from municipalitet where region_id = %s and text = "%s"' % (region_id, municipalitet_str)
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def get_poselenie_id(region_id, municipalitet_id, poselenie_str):
    try:
        conn = sqlite3.connect('ssfo.db3')
        cur = conn.cursor()
        sql = 'select id from poselenie where region_id = %s and municipalitet_id = %s and text = "%s"' % (region_id, municipalitet_id, poselenie_str)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            sql = '''insert into poselenie (region_id,municipalitet_id,text) values (%s,%s,"%s");''' % (region_id, municipalitet_id, poselenie_str)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from poselenie where region_id = %s and municipalitet_id = %s and text = "%s"' % (region_id, municipalitet_id, poselenie_str)
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def get_np_id(region_id, municipalitet_id, poselenie_id, np_str, np_fias, np_count):
    try:
        conn = sqlite3.connect('ssfo.db3')
        cur = conn.cursor()
        sql = 'select id from np where region_id = %s and municipalitet_id = %s and poselenie_id = %s and text = "%s"' % (region_id, municipalitet_id, poselenie_id, np_str)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            sql = '''insert into np (region_id,municipalitet_id,poselenie_id,text,fias,count_ckd) values (%s,%s,%s,"%s","%s",%s);''' % (region_id, municipalitet_id, poselenie_id, np_str, np_fias, np_count)
            cur.execute(sql)
            conn.commit()
            sql = 'select id from np where region_id = %s and municipalitet_id = %s and poselenie_id = %s and text = "%s"' % (region_id, municipalitet_id, poselenie_id, np_str)
            cur.execute(sql)
            r = cur.fetchone()
            return r[0]
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

#df = pd.read_csv('af.csv', sep = ';', header=None, skiprows=1, encoding= 'cp1251')
df = pd.read_csv('mrf.csv', sep = ';', header=None, skiprows=1, encoding= 'cp1251').fillna('н/д')

#print(df)
region = df[1].drop_duplicates()
if region.size > 0:
    for row_region in region:
        reg_id = get_region_id(row_region)
        print(row_region)
        municipalitet = df.loc[df[1] == row_region][2].drop_duplicates()
        if municipalitet.size > 0:
            for row_municipalitet in municipalitet:
                mun_id = get_municipalitet_id(reg_id, row_municipalitet)
                print(row_municipalitet)
                poselenie = df.loc[(df[1] == row_region) & (df[2] == row_municipalitet)][3].drop_duplicates()
                if poselenie.size > 0:
                    for row_poselenie in poselenie:
                        print(row_poselenie)
                        pos_id = get_poselenie_id(reg_id, mun_id, row_poselenie)
                        n_p = df[(df[1] == row_region) & (df[2] == row_municipalitet) & (df[3] == row_poselenie)]
                        if n_p.values.itemsize > 0:
                            for row_np in n_p.values:
                                print(row_np)
                                print(row_np[4])
                                print(row_np[5])
                                print(row_np[6])
                                np_id = get_np_id(reg_id, mun_id, pos_id, row_np[4], row_np[5], row_np[6])