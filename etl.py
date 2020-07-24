import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''Copies data from the S3 buckets into the events and songs staging tables'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as error:
            print(error)
    print("Successfully completed the loading of staging tables")

'''Inserts data from the staging tables into the fact and dimension tables'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as error:
            print(error)
    print("Successfully inserted data into tables")


def main():
    '''Reads AWS configuration parameters and establishes connection to Postgresql database to insert data into
       previously created tables'''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()