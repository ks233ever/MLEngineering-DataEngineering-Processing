import stripe
import pandas as pd

from pandas.io.json import json_normalize
from keys import parms


stripe.api_key = parms['stripe']

cust_list = pd.read_csv('customer_list.csv')

cust_id = list(cust_list['id'])

dfs = []
for i in cust_id:
    customer = stripe.Customer.retrieve(i)
    df1 = json_normalize(customer)
    df1 = df1[['delinquent', 'email', 'id']]
    df2 = json_normalize(list(customer['sources']))
    df = pd.concat([df1, df2], axis=1)
    dfs.append(df)
    print(len(dfs))

all_records = prd.concat(dfs)

all_records.head()

all_records.to_csv(customer_combined.csv)
