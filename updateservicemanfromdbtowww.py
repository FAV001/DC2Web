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
cur.execute('SELECT id FROM serviceman')
sql_serviceman = cur.fetchall()

con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_ib = con_ib.cursor()
sql = 'select distinct serviceman_id from phones'
with cur_ib.execute(sql):
    phones_serviceman_id = cur_ib.fetchall()
for row in phones_serviceman_id:
    if row not in sql_serviceman:
        print('Значения %s нет в базе, необходимо добавить' % row)
        sql = 'select * from serviceman where serviceman_id=%s' % row
        with cur_ib.execute(sql):
            seviceman = cur_ib.fetchone()
        name = seviceman[1]
        s = "insert into serviceman values (%s,'%s',0);" % (row[0], name)
        cur.execute(s)
        con.commit()
con.close()

