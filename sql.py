'''import mysql.connector as mysql

mydb = mysql.connect(
  host="192.168.1.201",
  port="3306",
  user="root",
  password="root_password",
  auth_plugin='mysql_native_password'
)
cursor = mydb.cursor()
#cursor.execute("DROP DATABASE stocksFundamentals")'''
from pandas import util

import asyncio
import aiomysql

loop = asyncio.get_event_loop()

'''user='root'
password='root_password'
host='192.168.1.201'
'''

df=util.testing.makeDataFrame()
print(df.head())

async def test_example():
    conn = await aiomysql.connect(host='192.168.1.201', port=3306,
                                       user='root', password='root_password', db='mysql', loop=loop)

    await df.to_sql(name = 'Tables_in_new_schema', con = conn, if_exists = 'append', index = False)

    cur = await conn.cursor()
    await cur.execute("Show tables from new_schema")
    print(cur.description)
    r = await cur.fetchall()
    print(r)
    await cur.close()
    conn.close()

loop.run_until_complete(test_example())

#engine = create_engine('mysql://root:root_password@192.168.1.201:3306/stocksFundamentalsAnnual')
#engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}:3306/stocksFundamentalsAnnual')
