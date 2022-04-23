import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import datetime

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(query)
        print(datetime.datetime.now())


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(query)
        print(datetime.datetime.now())


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f'''
            host={config['CLUSTER']['HOST']} 
            dbname={config['CLUSTER']['DB_NAME']} 
            user={config['CLUSTER']['DB_USERNAME']} 
            password={config['CLUSTER']['DB_PASSWORD']} 
            port={config['CLUSTER']['PORT']}
    ''')
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
