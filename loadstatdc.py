#загружаем статистика за день переданный в аргументе либо предыдущий данные из DC по дням из SSFO
import firebirdsql as fdb
from configobj import ConfigObj
import sqlite3
import os
import logging
from datetime import datetime, timedelta
import sys
#import getopt
import argparse

cur_date = datetime.now()
LogFile = os.getcwd() + "\\Logs\\loadstatdc" + cur_date.strftime("%d.%m.%Y") + ".log"
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)

def countotzvon(phone_id, date):
    global server
    global db_stat
    global user
    global userpass
    con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    cur_stat = con_stat.cursor()
    d = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    sql = "select count(*) from STATCONNECTJOURNAL where phone_id = %s and (DATETIME > '%s 00:00' and DATETIME < '%s 23:59')" % (phone_id, d, d)
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Код отзвона - %s' % r[0])
            return r[0]
        else:
            return 0

def put_to_db(phone, data, cod, kol, cod_error):
    global con
    try:
        #conn_f = sqlite3.connect('dc.db3')
        cur = con.cursor()
        sql = 'select id from result_day_dc where phone_id = {} and date = "{}"'.format(phone, data)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            log_text = u'Выставили данные phone_id {},date {},res {},statconnect {}'.format(phone, data, cod, kol)
            logging.info(log_text)
            print(log_text)
            sql = 'insert into result_day_dc (phone_id,date,res,statconnect,cod_error) values ("{}", "{}", "{}", {}, {});'.format(phone, data, cod, kol, cod_error)
            cur.execute(sql)
            con.commit()
            return 'new'
    except sqlite3.Error as e:
        print(e)

def checktaksofonotzvon(phone_id, date):
    global server
    global db_stat
    global user
    global userpass
    con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    with con_stat:
        cur_stat = con_stat.cursor()
        d = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, d, d)
        cur_stat.execute(sql)
        cur_stat.fetchone()
        logging.debug('phone_id - %4s, count - %s' % (phone_id, cur_stat.rowcount))
        if cur_stat.rowcount > 0:
            return True
        else:
            return False

def check_stat_ssfo(date):
    con = sqlite3.connect('dc.db3')
    with con:
        cur = con.cursor()
        sql = "SELECT count(*) from result_day_ssfo where date = '{}'".format(date)
        cur.execute(sql)
        r = cur.fetchone()
        if r[0] == 0:
            return False
        else:
            return True

def get_list_phone_from_ssfo(date):
    con = sqlite3.connect('dc.db3')
    with con:
        cur = con.cursor()
        sql = "SELECT phone_id from result_day_ssfo where date = '{}' order by phone_id".format(date)
        cur.execute(sql)
        return cur.fetchall()

def get_list_phones_from_dc():
    config = ConfigObj('call.cfg')
    global server
    global db_stat
    global user
    global userpass
    server = config['ServerDB']['ServerName']
    db_ib = config['ServerDB']['DB_IB']
    user = config['ServerDB']['user']
    userpass = config['ServerDB']['pass']
    list_lot = config['WorkConfig']['Lot']
    s_Lot = ""
    i = 1
    len_lot = len(list_lot)
    for single_lot in list_lot:
        if i !=len_lot:
            s_Lot = s_Lot + "'" + str(single_lot) + "',"
            i = i + 1
            continue
        else:
            s_Lot = s_Lot + "'" + str(single_lot) + "'"
            break

    con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
    cur_ib = con_ib.cursor()
    sql = "Select phone_id from PHONES where LOT_NUMB in ({}) order by phone_id".format(s_Lot)
    with cur_ib.execute(sql):
        return cur_ib.fetchall()

def codotzvona(phone_id, date):
    global server
    global db_stat
    global user
    global userpass
    con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    cur_stat = con_stat.cursor()
    d = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') order by DATE_TIME desc" % (phone_id, d, d)
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Код отзвона - %s' % r[2])
            return r[2]
        else:
            return 1009

def createParam():
    param = argparse.ArgumentParser(description="loadstatdc.py [--year] [--month] [--day] [--help]")
    param.add_argument('-y', '--year', type=str,
       help = u'год, статистики. пример: --year 2018')
    param.add_argument('-m', '--month', type=str,
       help = u'месяц, статистики. пример: --month 01')
    param.add_argument('-d', '--day', type=str,
       help = u'день, статистики. пример: --day 22')
    return param

def main():
    log_text = u'Запустили скрипт'
    logging.info(log_text)
    param = createParam()
    namespace = param.parse_args(sys.argv[1:])

    #получаем год получения статистики
    if namespace.year is not None:
        year = namespace.year.strip()
    else:
        year = (datetime.now() - timedelta(days=1)).year

    #получаем месяц получения статистики
    if namespace.month is not None:
        month = namespace.month.strip()
    else:
        month = (datetime.now() - timedelta(days=1)).month

    #получаем день получения статистики
    if namespace.day is not None:
        day = namespace.day.strip()
    else:
        day = (datetime.now() - timedelta(days=1)).day
    print("{}/{}/{}".format(year, month, day))
    d = datetime.strftime(datetime.strptime("{}{}{}".format(year, month, day), "%Y%m%d"),"%Y-%m-%d")
    #проверяем есть ли статистика из ССФО за d день
    #если существует, то будем список таксофонов брать из нее
    #если отсуствует, то будем брать текущий список таксофов по лотам из базы и по нему строить статистику
    if check_stat_ssfo(d):
        log_text = u'Статистика в базе ССФО за день {} существует.'.format(d)
        logging.info(log_text)
        phones = get_list_phone_from_ssfo(d)
        pass
    else:
        log_text = u'Статистика в базе ССФО за день {} отсутствует.'.format(d)
        logging.info(log_text)
        phones = get_list_phones_from_dc()
        pass
    config = ConfigObj('call.cfg')
    global server
    global db_stat
    global user
    global userpass
    server = config['ServerDB']['ServerName']
    db_stat = config['ServerDB']['DB_Stat']
    user = config['ServerDB']['user']
    userpass = config['ServerDB']['pass']
    global con
    con = sqlite3.connect('dc.db3')
    with con:
        for phone in phones:
            print(phone[0])
            check = checktaksofonotzvon(phone[0], d)
            kol = countotzvon(phone[0], d)
            cod_error = codotzvona(phone[0], d)
            if check:
                if (cod_error == 32 or cod_error == 0 or cod_error == 128) and (kol != 0):
                    put_to_db(phone[0], d, True, kol, cod_error)
                else:
                    put_to_db(phone[0], d, False, kol, cod_error)
            else:
                put_to_db(phone[0], d, False, kol, cod_error)

if __name__ == '__main__':
    main()