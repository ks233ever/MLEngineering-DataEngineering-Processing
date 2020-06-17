import pandas as pd
import numpy as np
import os.path
import time

import vaex
import boto3
import psycopg2
import psycopg2.extras

get_ipython().run_line_magic('matplotlib', 'inline')

import matplotlib
import matplotlib.pyplot as plt
from sklearn.externals import joblib
from db_connections import parms


path = os.path.join(os.getcwd(), 'data/')
plot_dir = os.path.join(os.getcwd(), 'plots/')

pd.set_option('max_columns', 400)
pd.set_option('max_rows', 400)


redshift = parms['redshift']

model_data = {
    'mfile': 'path.joblib.dat',
    'ifile': 'path/imputer.joblib.dat',
}


models = [model_data]


for model in models:
    model['model'] = joblib.load(model['mfile'])
    model['imputer'] = joblib.load(model['ifile'])


# connect to Redshift
hostname = redshift['hostname']
username = redshift['uid']
password = redshift['password']
database = redshift['db']

con = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)


# unload Redshift table to S3 as parquet files
sql = """UNLOAD ('select * from my_table') TO 's3://bucket/path' \
    credentials 'aws_access_key_id= access_key;aws_secret_access_key= secret_access_key' FORMAT PARQUET;"""

cur = con.cursor()
cur.execute(sql)


# grab list of the parquet files
session = boto3.Session(region_name='us-east-1')


s3_client = session.client('s3')

s3_resource = session.resource('s3')

s3 = boto3.client('s3')

bucket = s3_resource.Bucket('bucket')
obj_names = []
for obj in bucket.objects.filter(Prefix='path/'):
    obj_name = obj.key
    if 'part' in obj_name:
        print(obj_name)
        obj_names.append(obj_name)


# download parquet files locally
df_list = []

for b in obj_names:

    # download
    s3.download_file('bucket', b, '{}'.format(b.split('/')[-1:][0]))
    print('downloaded {}'.format(b))

    # open with vaex
    df = vaex.open('{}'.format(b.split('/')[-1:][0]))

    df_list.append(df)


# score the files
for df_final in df_list:

    # impute null values
    X_pred = model['imputer'].transform(df_final)
    print('imputed')

    # score
    df_final['preds'] = model['model'].predict_proba(X_pred)[:, 1]
    print('scored')

    out = df_final[['preds', 'unique_id']]
    print('subset')

    df_scored = out.to_pandas_df()

    # load preds to Redshift
    df_cols = list(df_scored)
    cols = ",".join(df_cols)

    values = "VALUES({})".format(",".join(["%s" for _ in df_cols]))
    insert_stmt = "INSERT INTO {} ({}) {}".format(table, cols, values)

    psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
    con.commit()
    cur.close()


# remove S3 and local files that were created
for b in obj_names:

    # remove locally
    os.remove(b.split('/')[-1:][0])

    # remove up in S3
    for obj in bucket.objects.filter(Prefix='path/'):
        obj.delete()
