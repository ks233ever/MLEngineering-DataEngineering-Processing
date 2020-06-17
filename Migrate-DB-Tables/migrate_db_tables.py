import pandas as pd
import psycopg2
import sqlalchemy as sa
import pyodbc
import urllib


# Connecting to Postgres Database
hostname = 'XXXXXXXXXXXXXXX'
username = 'XXXXX'
password = 'XXXXX'
database = 'XXXXX'


myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)

# Creating SQL Server Engine

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0}
                                 SERVER=XXXXXXXXXX
                                 DATABASE=XXXXXXXXXX
                                 UID=XXXXX
                                 PWD=XXXXX)

engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# Connecting to SQL Server

conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0}
                      SERVER=XXXXXXXXXX
                      DATABASE=XXXXXXXXXX
                      UID=XXXXX
                      PWD=XXXXX)

# creating a loop for the tables

tablenames = ['public."Table1"',
              'public."Table2"',
              'public."Table3"',
              'public."Table4"',
              'public."Table5"',
              'public."Table6"',
              'public."Table7"',
              'public."Table8"',
              'public."Table9"',
              'public."Table10"',
              'public."Table11"',
              'public."Table12"',
              'public."Table13"',
              'public."Table14"',
              'public."Table15"',
              'public."Table16"',
              'public."Table17"',
              'public."Table18"',
              'public."Table19"',
              'public."Table20"',
              'public."Table21"',
              'public."Table22"',
              'public."Table23"',
              'public."Table24"',
              'public."Table25"',
              'public."Table26"',
              'public."Table27"',
              'public."Table28"']

for table in tablenames:

    # reading in the Postgres table as a df
    cur = myConnection.cursor()
    cur.execute('SELECT * FROM {}'.format(table))
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(data=cur.fetchall(), columns=colnames, dtype='str')

    # check out the df
    print(df.head())

    # for sql import we just want tablename not schema
    sql_table = table.split('.')[1].replace('"', '')

    # drop the table if it exists in SQL Server
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS dbo.[{}]'.format(sql_table))
    conn.commit()

    # write the dataframe into a SQL Server table
    df.to_sql('{}'.format(sql_table), engine, dtype={col_name: sa.types.VARCHAR(length=1000) for col_name in df})


myConnection.close()
conn.close()
