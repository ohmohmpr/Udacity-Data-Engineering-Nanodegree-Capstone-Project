import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - load data from s3.
    - into staging_songs and staging_events.
    """
    print("loading staging tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - load data from staging tables.
    - insert into new tables.
    """
    print("Inserting tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - load redshift config from dwh.cfg.
    - connect to redshift.
    - load data from s3 into staging tables.
    - insert data from staging tables into dimentional tables and fact table.
    - close connect to redshift.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()