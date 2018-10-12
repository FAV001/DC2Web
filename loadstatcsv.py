#загружаем статистику отзвонки из файлов csv, выгруженые из ССФО
import sqlite3
from configobj import ConfigObj
import os
import logging
import datetime
import csv

def put_to_db(phone, data, cod):
    try:
        conn = sqlite3.connect('dc.db3')
        cur = conn.cursor()
        sql = 'select id from result_day_ssfo where phone_id = %s and date = "%s"' % (phone, data)
        cur.execute(sql)
        r = cur.fetchone()
        if (r != None):
            sql = '''update result_day_ssfo
                    set phone_id = {},
                    date = "{}",
                    res = {}
                    where id = {};'''.format(phone, data, cod, r[0])
            cur.execute(sql)
            conn.commit()
            return r[0]
        else:
            sql = '''insert into result_day_ssfo (phone_id,date,res) values ("%s", "%s", "%s");''' % (phone, data, cod)
            cur.execute(sql)
            conn.commit()
            return 'new'
    except sqlite3.Error as e:
        print(e)
    finally:
        conn.close()

def main():
    cur_date = datetime.datetime.now()
    LogFile = os.getcwd() + "\\Logs\\loadstatcsv" + cur_date.strftime("%d.%m.%Y") + ".log"
    logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)
    logging.info(u'Запустили скрипт' )
    config = ConfigObj('call.cfg')
    path_ssfo_csv = config['WorkConfig']['PathSSFO']
    path_loaded_csv = config['WorkConfig']['PathLoaded']
    print(path_ssfo_csv)
    #проверяем в каталоге наличие файлов статистики для загрузки
    listdir = os.listdir(path_ssfo_csv)
    if len(listdir) != 0:
        #файлы есть начинаем перебирать
        for f in listdir:
            logging.info(u'Загружаем файл %s' % f)
            print(f)
            cd_date = datetime.datetime.strptime(f[:-4],"%Y%m%d")
            cd_str = cd_date.strftime("%Y-%m-%d")
            #cd = datetime.datetime.strftime(,""%Y-%m-%d"")
            with open(path_ssfo_csv + f, 'r') as csvfile:
                csv_dict = csv.DictReader(csvfile, delimiter=';') #, fieldnames=csv_columns)
                for row in csv_dict:
                    if row['Уровень'] != 'итого по Оператору':
                        if len(row['УУС']) == 17:
                            phone_id = int((row['УУС'][:-1])[-4:])
                        else:
                            phone_id = int((row['УУС'])[-4:])
                        cod = row['Фактическое количество дней (Df)']
                        print(phone_id)
                        put_to_db(phone_id, cd_str, cod)
                        logging.info(u'   Обрабатываем таксофон %4s код отзвона %s' % (phone_id, cod))
                    else:
                        logging.info(u'Загрузили данные по %s таксофонам, процент %s' % (row['Плановое количество дней (Dp)'], row['Коэффициент доступности (Arf)']))
            try:
                if (os.path.isfile(path_loaded_csv + f)):
                    os.remove(path_loaded_csv + f)
                os.rename(path_ssfo_csv + f, path_loaded_csv + f)
                logging.info(u'перенесли файл %s в обработанные' % f)
            except Exception as identifier:
                logging.error(u'при переносе возникала ошибка %s' % identifier)


if __name__ == '__main__':
    main()