import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''All tables dropped in case there are pre-exisiting tables of the same name'''
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except:
            print("There is an issue dropping the tables")


def create_tables(cur, conn):
    '''Creates the staging and star schema tables with respective fields'''
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as error:
            print(error)

def main():
    '''Reads the AWS configuration parameters'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    '''Establishes connection for a Postgresql database using AWS Redshift Cluster'''
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()