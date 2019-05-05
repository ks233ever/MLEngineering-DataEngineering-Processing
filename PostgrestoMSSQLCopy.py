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

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=XXXXXXXXXX;DATABASE=XXXXXXXXXX;UID=XXXXX;PWD=XXXXX)

engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# Connecting to SQL Server

conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=server;DATABASE=db;UID=userid;PWD=pwd)

# creating a loop for the tables

tablenames=['public."BillingAccount"',
'public."Farm"',
'public."Garden"',
'public."Generation"',
'public."GenerationStateType"',
'public."Investor"',
'public."Invoice"',
'public."InvoiceTransaction"',
'public."InvoiceType"',
'public."MasterAccount"',
'public."MasterAccountType"',
'public."PaymentMethod"',
'public."PaymentPreference"',
'public."PaymentStateType"',
'public."PendingTransaction"',
'public."PendingTransactionState"',
'public."PendingTransactionStateType"',
'public."PendingTransactionType"',
'public."Portfolio"',
'public."Premise"',
'public."ProductEnrollment"',
'public."Project"',
'public."RatePlan"',
'public."Transaction"',
'public."TransactionType"',
'public."Utility"',
'public."UtilityMeterStatus"',
'public."UtilitySubscriber"']

for table in tablenames:


    # reading in the Postgres table as a df
    cur = myConnection.cursor()
    cur.execute('SELECT * FROM {}'.format(table))
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(data = cur.fetchall(), columns = colnames, dtype = 'str')

    # check out the df
    print(df.head())

    # for sql import we just want tablename not schema
    sql_table= table.split('.')[1].replace('"', '')

    # drop the table if it exists in SQL Server
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS dbo.{}'.format(sql_table))
    conn.commit()


    # write the dataframe into a SQL Server table
    df.to_sql('{}'.format(sql_table), engine, dtype={col_name: sa.types.VARCHAR(length=1000) for col_name in df})


myConnection.close()
conn.close()
