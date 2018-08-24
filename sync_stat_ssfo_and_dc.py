#Синхронизируем данные из DC по дням из SSFO
import firebirdsql as fdb
from configobj import ConfigObj
import sqlite3
import os
import logging
import datetime


def put_to_db(phone, data, cod, kol):
    global con
    try:
        #conn_f = sqlite3.connect('dc.db3')
        cur = con.cursor()
        sql = 'select id from result_day_dc where phone_id = %s and date = "%s"' % (phone, data)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            return r[0]
        else:
            sql = '''insert into result_day_dc (phone_id,date,res,statconnect) values ("%s", "%s", "%s", %s);''' % (phone, data, cod, kol)
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
        d = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, d, d)
        cur_stat.execute(sql)
        cur_stat.fetchone()
        logging.debug('phone_id - %4s, count - %s' % (phone_id, cur_stat.rowcount))
        if cur_stat.rowcount > 0:
            return True
        else:
            return False

def codotzvona(phone_id, date):
    global server
    global db_stat
    global user
    global userpass
    con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    cur_stat = con_stat.cursor()
    d = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') and ((fail_code in (0,32,128)) and (fail_code not in (1,2,4)))" % (phone_id, d, d)
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Код отзвона - %s' % r[2])
            return r[2]
        else:
            return 1009

def countotzvon(phone_id, date):
    global server
    global db_stat
    global user
    global userpass
    con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    cur_stat = con_stat.cursor()
    d = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    sql = "select count(*) from STATCONNECTJOURNAL where phone_id = %s and (DATETIME > '%s 00:00' and DATETIME < '%s 23:59')" % (phone_id, d, d)
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Количество отзвонов таксофона {} за день {} -> {}'.format(phone_id, date, r[0]))
            return r[0]
        else:
            return 0

def main():
    cur_date = datetime.datetime.now()
    LogFile = os.getcwd() + "\\Logs\\syncstat-(" + cur_date.strftime("%d.%m.%Y") + ").log"
    logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)
    logging.info(u'Запустили скрипт' )

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
        cur = con.cursor()
        cur2 = con.cursor()
        sql = "select * from result_day_ssfo  where (phone_id || ' ' || date)  not in (select phone_id || ' ' || date from result_day_dc) order by phone_id;"
        cur.execute(sql)
        #cur.execute('SELECT * from result_day_ssfo')
        while True:
            row = cur.fetchone()
            if row == None:
                break
            print(row)
            cur2.execute('SELECT * from result_day_dc where phone_id = %s and date = "%s"' % (row[1], row[2]))
            if cur2.fetchone() == None:
                check = checktaksofonotzvon(row[1], row[2])
                kol = countotzvon(row[1], row[2])
                if check:
                    cod_error = codotzvona(row[1], row[2])
                    if cod_error == 32 or cod_error == 0:
                        put_to_db(row[1], row[2], True, kol)
                    else:
                        put_to_db(row[1], row[2], False, kol)
                else:
                    put_to_db(row[1], row[2], False, kol)
                #ищем код отзвона и помещаем в таблицу результат
                pass
            else:
                print('значение есть в базе')
    logging.info(u'Закончили выполнять скрипт' )

if __name__ == '__main__':
    main()