import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')
HOST = config.get("CLUSTER","HOST")
DWH_DB= config.get("CLUSTER","DB_NAME")
DWH_DB_USER= config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DB_PORT")

def drop_tables(cur, conn):
    '''
    Drops existing tables 
    
        Parameters:
                    cur  : a database object to retrieve data
                    conn : connection to database

        Excecute:
                    drop_table_queries list
    
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates fact, dimension and staging tables 
    
        Parameters:
                    cur  : a database object to retrieve data
                    conn : connection to database

        Excecute:
                    create_table_queries list
    
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    '''
    Connects to the database and excecute drop_tables and create_tables functions
    
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()