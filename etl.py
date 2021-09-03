import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')
HOST = config.get("CLUSTER","HOST")
DWH_DB= config.get("CLUSTER","DB_NAME")
DWH_DB_USER= config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DB_PORT")

def load_staging_tables(cur, conn):
    '''
    Copies data from S3 to Redshift
    
        Parameters:
                    cur  : a database object to retrieve data
                    conn : connection to database

        Excecute:
                    copy_table_queries list
    
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Transforms data and insert to dimensional tables created from create_tables.py file
    
        Parameters:
                    cur  : a database object to retrieve data
                    conn : connection to database

        Excecute:
                    insert_table_queries list
    
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connects to the database and excecute load_staging_tables and insert_tables functions
    
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()