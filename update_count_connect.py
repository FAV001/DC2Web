#Синхронизируем данные из DC по дням из SSFO
import firebirdsql as fdb
from configobj import ConfigObj
import sqlite3
import os
import logging
import datetime

cur_date = datetime.datetime.now()
LogFile = os.getcwd() + "\\Logs\\update-(" + cur_date.strftime("%d.%m.%Y") + ").log"
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)

def update_to_db(phone, data, kol):
    global con
    try:
        #conn_f = sqlite3.connect('dc.db3')
        cur = con.cursor()
        sql = 'select id from result_day_dc where phone_id = %s and date = "%s"' % (phone, data)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            sql = '''update result_day_dc set statconnect = %s where phone_id = %s and date = "%s";''' % (kol, phone, data)
            cur.execute(sql)
            con.commit()
            return 'update'
    except sqlite3.Error as e:
        print(e)

def countotzvon(cursor,phone_id, date):
    global server
    global db_stat
    global user
    global userpass
    #con_stat = fdb.connect(host=server, database=db_stat, user=user, password=userpass)
    #cur_stat = con_stat.cursor()
    d = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    sql = "select count(*) from STATCONNECTJOURNAL where phone_id = %s and (DATETIME > '%s 00:00' and DATETIME < '%s 23:59')" % (phone_id, d, d)
    #with cur_stat.execute(sql):
    cur_stat = cursor
    with cur_stat.execute(sql):
        r = cur_stat.fetchone()
        if r != None:
            logging.debug('Код отзвона - %s' % r[0])
            return r[0]
        else:
            return 0

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
        cur2 = con.cursor()
        #cur.execute("SELECT * from result_day_dc where (date > '2018-01-18' and date < '2018-01-20') and statconnect = 0")
        #cur.execute("SELECT * from result_day_dc where statconnect = 0 and phone_id=1536")
        cur.execute("select * from result_day_dc where statconnect=0 and res='True' and date > '2018-08-20'order by date, phone_id")
        cur_d=''
        while True:
            row = cur.fetchone()
            if row == None:
                break
            if cur_d != row[2]:
                cur_d = row[2]
                logging.info(u'Обрабатываем {} дату'.format(cur_d))
            kol = countotzvon(cur_stat, row[1], row[2])
            if kol != 0:
                update_to_db(row[1], row[2], kol)
                print(u"update phone {}, date {}, count {}".format(row[1], row[2], kol))
            else:
                print(u"skip update phone {}, date {}, count {}".format(row[1], row[2], kol))
    con_stat.close()

    logging.info(u'Закончили выполнять скрипт' )

if __name__ == '__main__':
    main()