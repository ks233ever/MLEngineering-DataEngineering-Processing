#import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

from keys import params

aws = params['aws']

def load_staging_tables(cur, conn):
    
    """
    Load data from S3 files to the staging tables using the queries within sql_queries.py 
    """
        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    Transform the data from staging tables into the dimensional tables using the queries within sql_queries.py
    """
        
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Extract songs and user data from S3, 
    Transform it using the staging table
    Load into dimensional tables
    """
        
    #config = configparser.ConfigParser()
    #config.read('dwh.cfg')

    conn = psycopg2.connect(host=aws['HOST'], dbname=aws['DB_NAME'], user=aws['DB_USER'], password=aws['DB_PASSWORD'], port=aws['DB_PORT'])
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()