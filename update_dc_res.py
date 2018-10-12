#Синхронизируем данные из DC по дням из SSFO
import firebirdsql as fdb
from configobj import ConfigObj
import sqlite3
import os
import logging
import datetime

cur_date = datetime.datetime.now()
LogFile = os.getcwd() + "\\Logs\\update-dc-res-(" + cur_date.strftime("%d.%m.%Y") + ").log"
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)

def update_to_db(phone, data, res, cod_error):
    global con
    try:
        #conn_f = sqlite3.connect('dc.db3')
        cur = con.cursor()
        sql = 'select id from result_day_dc where phone_id = %s and date = "%s"' % (phone, data)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            sql = '''update result_day_dc set res = "%s", cod_error = %s where phone_id = %s and date = "%s";''' % (res, cod_error, phone, data)
            cur.execute(sql)
            con.commit()
            return 'update'
    except sqlite3.Error as e:
        print(e)

def codotzvona(cursor,phone_id, date):
    cur_stat = cursor
    d = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    sql = "select * from allfails where phone_id = %s and (DATE_TIME > '%s 00:00' and DATE_TIME < '%s 23:59') order by DATE_TIME desc" % (phone_id, d, d)
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Код отзвона - %s' % r[2])
            return r[2]
        else:
            return 1009

def main():
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
    con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    cur_stat = con_stat.cursor()
    with con:
        cur = con.cursor()
        cur.execute("select * from result_day_dc where date > '2018-05-13' order by date, phone_id")
        cur_d=''
        while True:
            row = cur.fetchone()
            if row == None:
                break
            if cur_d != row[2]:
                cur_d = row[2]
                logging.info(u'Обрабатываем {} дату'.format(cur_d))
            cod_error = codotzvona(cur_stat, row[1], row[2])
            if (cod_error == 32 or cod_error == 0 or cod_error == 128):
                update_to_db(row[1], row[2], True, cod_error)
                print(u"update phone {}, date {}, res {}, cod_error {}".format(row[1], row[2], True, cod_error))
            else:
                update_to_db(row[1], row[2], False, cod_error)
                print(u"skip update phone {}, date {}, res {}, cod_error {}".format(row[1], row[2], False, cod_error))
    con_stat.close()

    logging.info(u'Закончили выполнять скрипт' )

if __name__ == '__main__':
    main()