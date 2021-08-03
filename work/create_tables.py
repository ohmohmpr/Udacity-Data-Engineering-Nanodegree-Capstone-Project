import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Drop tables (Staging tables, Dimension tables, and Fact table) to ensure there are no existing tables.
    """
    print("drop tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - Create tables (Staging tables, Dimention tables and Fact table).
    """
    print("create tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Load redshift config from dwh.cfg.
    - Connect to redshift.
    - Drop tables (Staging tables, Dimension tables, and Fact table) to ensure there are no existing tables.
    - Create tables (Staging tables, Dimention tables and Fact table).
    - Close connect to redshift.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()