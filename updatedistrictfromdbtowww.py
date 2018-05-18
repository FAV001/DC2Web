import firebirdsql as fdb
from configobj import ConfigObj
import sqlite3

config = ConfigObj('call.cfg')
server = config['ServerDB']['ServerName']
db_ib = config['ServerDB']['DB_IB']
user = config['ServerDB']['user']
userpass = config['ServerDB']['pass']

#читаем информацию о районах из базы
con = sqlite3.connect('dc.sqlite')
cur = con.cursor()
cur.execute('SELECT id FROM district')
sql_district = cur.fetchall()

con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_ib = con_ib.cursor()
sql = 'select distinct district_id from phones'
with cur_ib.execute(sql):
    phones_district_id = cur_ib.fetchall()
for row in phones_district_id:
    if row not in sql_district:
        print('Значения %s нет в базе, необходимо добавить' % row)
        sql = 'select * from district where district_id=%s' % row
        with cur_ib.execute(sql):
            district = cur_ib.fetchone()
        name = district[1]
        s = "insert into district values (%s,'%s',1);" % (row[0], name)
        cur.execute(s)
        con.commit()
con.close()

