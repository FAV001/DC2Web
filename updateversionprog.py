import firebirdsql as fdb
from configobj import ConfigObj
import sqlite3

config = ConfigObj('call.cfg')
server = config['ServerDB']['ServerName']
db_ib = config['ServerDB']['DB_IB']
user = config['ServerDB']['user']
userpass = config['ServerDB']['pass']

#читаем информацию о version из базы
con = sqlite3.connect('dc.sqlite')
cur = con.cursor()
cur.execute('SELECT version FROM versionporg')
ver = cur.fetchall()

con_ib = fdb.connect(host=server, database=db_ib, user=user, password=userpass)
cur_ib = con_ib.cursor()
sql = 'select distinct versionprog from phones'
with cur_ib.execute(sql):
    rows = cur_ib.fetchall()
for row in rows:
    if row not in ver:
        print('Значения %s нет в базе, необходимо добавить' % row)
        cur.execute("insert into versionporg (version) values (%s);" % row)
        con.commit()
con.close()

