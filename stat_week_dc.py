import datetime
import sqlite3


def datefromiso(year, week, day):
    return datetime.datetime.strptime("%d%02d%d" % (year, week, day), "%Y%W%w")


print(datefromiso(2018, 2, 3))

#cur = datetime.datetime(2018,12,31)
cur = datetime.datetime.now()
print(cur.isocalendar()[1])
con = sqlite3.connect('dc.db3')
with con:
    curs = con.cursor()
    for week in range(1, cur.isocalendar()[1]):
        start_d = datefromiso(cur.year, week, 3)
        end_d = start_d + datetime.timedelta(days=6)
        print('неделя № {} начало {} конец {}'.format(week, start_d, end_d))
        sql = """select name, round((cast(sum(result) as REAL)/cast(count(result) as real))*100,2) as 'proc' from (select
                rds.phone_id,
                rds.date,
                rds.res,
                rds.statconnect,
                s.name,
                case
                    when rds.statconnect>0 then 1
                    else 0
                end result
            from result_day_dc as rds
            inner join phone as ph on ph.phone_id=rds.phone_id
            inner join spr_serviceman as s on ph.serviceman_id=s.id
            where rds.date >= '{0}' and rds.date <= '{1}')
            group by name
            Union all
            select 'Итого', round((cast(sum(result) as REAL)/cast(count(result) as real))*100,2) as 'proc' from (select
                rds.phone_id,
                rds.date,
                rds.res,
                rds.statconnect,
                s.name,
                case
                    when rds.statconnect>0 then 1
                    else 0
                end result
            from result_day_dc as rds
            inner join phone as ph on ph.phone_id=rds.phone_id
            inner join spr_serviceman as s on ph.serviceman_id=s.id
            where rds.date >= '{0}' and rds.date <= '{1}')""".format(start_d.date(), end_d.date())
        #print(sql)
        curs.execute(sql)
        for row in curs.fetchall():
            print(row)
