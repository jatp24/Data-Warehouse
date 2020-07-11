import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy data from S3 to staging tables on Redshift.
    
    * cur - connection to db.
    * conn - parameters to connect the DB.
    Output:
    * log_data in staging_events table.
    * song_data in staging_songs table.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from Redshift staging tables to analytics tables on Redshift.
    
    * cur - connection to db.
    * conn - parameters to connect the DB.
    Output:
    * Inserts data into Fact and Dimension Tables.
    
    """

    for query in insert_table_queries:
      
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()