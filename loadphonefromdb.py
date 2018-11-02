#загружаем список таксофонов из DC в базу
import firebirdsql as fdb
from configobj import ConfigObj
import sqlite3
import os
import logging
from datetime import datetime, timedelta
import sys
import re

cur_date = datetime.now()
LogFile = os.getcwd() + "\\Logs\\loadphone(" + cur_date.strftime("%d.%m.%Y") + ").log"
logging.basicConfig(format = u'[LINE:%(lineno)3d]# %(levelname)-8s [%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level = logging.INFO, filename = LogFile)

def get_district_id(name:str):
    con = sqlite3.connect('dc.db3')
    with con:
        cur = con.cursor()
        sql = "SELECT id from spr_district where name = '{}'".format(name)
        cur.execute(sql)
        r = cur.fetchone()
        if r != None:
            return r[0]
        else:
            sql = "insert into spr_district (name, use) values ('{}', 1)".format(name)
            cur.execute(sql)
            print(cur.lastrowid)
            return cur.lastrowid

def get_versionprog_id(name:str):
    con = sqlite3.connect('dc.db3')
    with con:
        cur = con.cursor()
        sql = "SELECT id from spr_versionporg where version = '{}'".format(name)
        cur.execute(sql)
        r = cur.fetchone()
        if r != None:
            return r[0]
        else:
            sql = "insert into spr_versionporg (version) values ('{}')".format(name)
            cur.execute(sql)
            print(cur.lastrowid)
            return cur.lastrowid

def get_serviceman_id(name:str):
    con = sqlite3.connect('dc.db3')
    with con:
        cur = con.cursor()
        sql = "SELECT id from spr_serviceman where name = '{}'".format(name)
        cur.execute(sql)
        r = cur.fetchone()
        if r != None:
            return r[0]
        else:
            sql = "insert into spr_serviceman (name, use) values ('{}', 1)".format(name)
            cur.execute(sql)
            print(cur.lastrowid)
            return cur.lastrowid

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
    sql = """Select PH.phone_id,
                PH.address,
                PH.WORK_SAM,
                PH.VERSIONPROG,
                PH.LOT_NUMB,
                PH.COMPETITION_NUMB,
                PH.ABC_NUMB,
                PH.LINE_NUMB,
                PH.SLOT_ID,
                (SELECT TEXT_DISTRICT from district d where PH.DISTRICT_ID = d.DISTRICT_ID) as SERVICEMAN,
                (SELECT NAME_SERVICEMAN from SERVICEMAN s where PH.SERVICEMAN_ID = s.SERVICEMAN_ID) as DISTRICT
            from PHONES PH
            where PH.LOT_NUMB in ({})
            order by PH.phone_id""".format(s_Lot)
    with cur_ib.execute(sql):
        return cur_ib.fetchall()

def set_phone(phone_id:int,
     address:str,
     work_sam:bool,
     versionprog:int,
     LOT_NUMB:str,
     COMPETITION_NUMB:str,
     ABC_NUMB:str,
     LINE_NUMB:str,
     SLOT_ID:int,
     serviceman:int,
     district:int,
     fias:str,
     postcode:str):
    con = sqlite3.connect('dc.db3')
    with con:
        cur = con.cursor()
        sql = "SELECT * from phone where phone_id = {}".format(phone_id)
        cur.execute(sql)
        r = cur.fetchone()
        if r != None:
            sql = 'update phone set '
            if r[2] != address:
                sql += ' address = "{}",'.format(address)
                log_text = u'  Для таксофона {} изменилось поле address с {} на {}'.format(phone_id, r[2], address)
                logging.info(log_text)
            if r[3] != fias:
                sql += ' fias = "{}",'.format(fias)
                log_text = u'  Для таксофона {} изменилось поле fias с {} на {}'.format(phone_id, r[3], fias)
                logging.info(log_text)
            if r[10] != work_sam:
                sql += ' work_sam = {},'.format(work_sam)
                log_text = u'  Для таксофона {} изменилось поле work_sam с {} на {}'.format(phone_id, r[10], work_sam)
                logging.info(log_text)
            if r[6] != versionprog:
                sql += ' versionprog_id = {},'.format(versionprog)
                log_text = u'  Для таксофона {} изменилось поле versionprog_id с {} на {}'.format(phone_id, r[6], versionprog)
                logging.info(log_text)
            if r[12] != LOT_NUMB:
                sql += ' LOT_NUMB = "{}",'.format(LOT_NUMB)
                log_text = u'  Для таксофона {} изменилось поле LOT_NUMB с {} на {}'.format(phone_id, r[12], LOT_NUMB)
                logging.info(log_text)
            if r[11] != COMPETITION_NUMB:
                sql += ' COMPETITION_NUMB = "{}",'.format(COMPETITION_NUMB)
                log_text = u'  Для таксофона {} изменилось поле COMPETITION_NUMB с {} на {}'.format(phone_id, r[11], COMPETITION_NUMB)
                logging.info(log_text)
            if r[13] != ABC_NUMB:
                sql += ' ABC_NUMB = "{}",'.format(ABC_NUMB)
                log_text = u'  Для таксофона {} изменилось поле ABC_NUMB с {} на {}'.format(phone_id, r[13], ABC_NUMB)
                logging.info(log_text)
            if r[14] != LINE_NUMB:
                sql += ' LINE_NUMB = "{}",'.format(LINE_NUMB)
                log_text = u'  Для таксофона {} изменилось поле LINE_NUMB с {} на {}'.format(phone_id, r[14], LINE_NUMB)
                logging.info(log_text)
            if r[8] != SLOT_ID:
                sql += ' SLOT_ID = {},'.format(SLOT_ID)
                log_text = u'  Для таксофона {} изменилось поле SLOT_ID с {} на {}'.format(phone_id, r[8], SLOT_ID)
                logging.info(log_text)
            if r[5] != serviceman:
                sql += ' serviceman_id = {},'.format(serviceman)
                log_text = u'  Для таксофона {} изменилось поле serviceman_id с {} на {}'.format(phone_id, r[5], serviceman)
                logging.info(log_text)
            if r[4] != district:
                sql += ' district_id = {},'.format(district)
                log_text = u'  Для таксофона {} изменилось поле district_id с {} на {}'.format(phone_id, r[4], district)
                logging.info(log_text)
            if r[15] != postcode:
                sql += ' postcode = "{}",'.format(postcode)
                log_text = u'  Для таксофона {} изменилось поле postcode с {} на {}'.format(phone_id, r[15], postcode)
                logging.info(log_text)
            sql += ' dateupdate = "{}"'.format(cur_date.strftime("%Y-%m-%d"))
            sql += ' where phone_id = {}'.format(phone_id)
            cur.execute(sql)
            return r[1]
        else:
            sql = """insert into phone (
                        phone_id,
                        address,
                        work_sam,
                        versionprog_id,
                        LOT_NUMB,
                        COMPETITION_NUMB,
                        ABC_NUMB,
                        LINE_NUMB,
                        SLOT_ID,
                        serviceman_id,
                        district_id,
                        fias,
                        postcode,
                        dateupdate)
                    values ({}, '{}',{},{},'{}','{}','{}','{}',{},{},{},'{}','{}','{}')""".format(
                        phone_id,
                        address,
                        work_sam,
                        versionprog,
                        LOT_NUMB,
                        COMPETITION_NUMB,
                        ABC_NUMB,
                        LINE_NUMB,
                        SLOT_ID,
                        serviceman,
                        district,
                        fias,
                        postcode,
                        cur_date.strftime("%Y-%m-%d"))
            cur.execute(sql)
            log_text = u'  Добавили таксофон {}'.format(phone_id)
            logging.info(log_text)
            print(cur.lastrowid)
            return cur.lastrowid

def close_phone():
    con = sqlite3.connect('dc.db3')
    with con:
        cur = con.cursor()
        sql = 'update phone set dateclose = "{0}" where dateupdate < "{0}" and dateclose is Null'.format(cur_date.strftime("%Y-%m-%d"))
        cur.execute(sql)

def get_fias_from_address(text:str):
    r = re.compile(r".{8}-.{4}-.{4}-.{4}-.{12}")
    t = r.search(text)
    if t:
        return t.group(0).lower()
    else:
        return ''

def get_postcode_from_address(text:str):
    r = re.compile(r"^\d{6}")
    t = r.search(text)
    if t:
        return t.group(0)
    else:
        return ''

def main():
    log_text = u'Запустили скрипт'
    logging.info(log_text)
    phones = get_list_phones_from_dc()
    for phone in phones:
        district_id = get_district_id(phone[9])
        serviceman_id = get_serviceman_id(phone[10])
        versionprog_id = get_versionprog_id(phone[3])
        adr = phone[1]
        post = get_postcode_from_address(adr)
        adr = adr.replace(post,'')
        fias = get_fias_from_address(adr)
        adr = adr.replace(fias,'')
        set_phone(phone_id=phone[0],
            address=phone[1],
            work_sam=phone[2],
            versionprog=versionprog_id,
            LOT_NUMB=phone[4],
            COMPETITION_NUMB=phone[5],
            ABC_NUMB=phone[6],
            LINE_NUMB=phone[7],
            SLOT_ID=phone[8],
            serviceman=serviceman_id,
            district=district_id,
            fias=fias,
            postcode=post)
        #logging.info(phone)
        print(phone)
    close_phone()

    log_text = u'Завершили скрипт'
    logging.info(log_text)

if __name__ == '__main__':
    main()
