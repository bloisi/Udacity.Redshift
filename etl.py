import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, eliminate_dup_queries, alter_table

# Function loads data from JASON files to staging tables  
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# Function loads data from staging tables to star schema tables        
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# Function eliminates all duplicate records  
def dup_tables(cur, conn):
    for query in eliminate_dup_queries:
        cur.execute(query)
        conn.commit()

# Function creates primary keys is users and artists tables        
def alter_tables(cur, conn):
    for query in alter_table:
        cur.execute(query)
        conn.commit()
        
        
# Main function        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    dup_tables (cur,conn)
    alter_tables (cur,conn)

    conn.close()


if __name__ == "__main__":
    main()