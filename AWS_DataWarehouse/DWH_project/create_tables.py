#import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from keys import params

aws = params['aws']


def drop_tables(cur, conn):
    
    """
    Delete existing tables 
    """
        
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
    Create tables specified within the sql_queries.py script
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    create the database tables with specified columns and design
    """
    
    #config = configparser.ConfigParser()
    #config.read('dwh.cfg')
    
    print('attempting to connect')
    
    print(aws)
    print(aws['DB_PORT'])

    conn = psycopg2.connect(host=aws['HOST'], dbname=aws['DB_NAME'], user=aws['DB_USER'], password=aws['DB_PASSWORD'], port=aws['DB_PORT'])

    cur = conn.cursor()
                            
    print('connected')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()