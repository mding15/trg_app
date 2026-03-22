# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 10:42:26 2024

@author: mgdin
"""
import threading
from database import ms_sql_server
from database import pg_create_connection

def sync_delta_thread():
    
    thread = threading.Thread(target=sync_delta)
    thread.start()

# sync the changes: (1) delete the deleted rows, (2) add new rows
# cases to call sync_delta
#   a. new user
#   b. change user role
#   c. change entitlement
#   d. new report
def sync_delta():
        
    # connect to PostgreSQL
    pg_conn = pg_create_connection()
    
    try:
        # delete data that is in the mapping_table but not in the view
        del_data =  delete_data(pg_conn)   
        
        # insert new data that is in the view but not in the mapping table
        new_data = get_new_data(pg_conn)

        # sync sql server        
        sync_sql_server(new_data, del_data)
    
    finally:
        pg_conn.close()

# compare the whole table in postgress and sql server
def sync_whole():
    table_name = 'user_report_mapping_table'
    
    pg_conn = pg_create_connection()
    ss_conn = ms_sql_server.create_connection()
    
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"SELECT * FROM {table_name}")
    pg_data = pg_cursor.fetchall()
    
    ss_cursor = ss_conn.cursor()
    ss_cursor.execute(f"SELECT * FROM {table_name}")
    ss_data = ss_cursor.fetchall()
    
    # delete data in ss but not in pg
    pg_ids = [row[0] for row in pg_data]
    del_ids = [str(row[0]) for row in ss_data if row[0] not in pg_ids]
    if len(del_ids) > 0:
        print("deleting data from sql server:", del_ids)
        del_query = f"DELETE FROM {table_name} WHERE id in ({', '.join(del_ids)})"
        print("deleting data from sql server")
        ss_cursor.execute(del_query)
        ss_conn.commit()
    
    # add data in pg but not in ss
    ss_ids = [row[0] for row in ss_data]
    new_data = [row for row in pg_data if row[0] not in ss_ids]
    if len(new_data) > 0:
        print(f'insert new data to sql server: {len(new_data)}')
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in pg_data[0]])})"
        for row in new_data:
            print(row)
            ss_cursor.execute(insert_query, row)
        ss_conn.commit()
        
    pg_cursor.close()
    ss_cursor.close()
    pg_conn.close()
    ss_conn.close()
    

# 1. find new data that is in the view but not in the mapping_table
# 2. insert the new data to the mapping_table
# 3. return the new data
def get_new_data(pg_conn):
    sql = """
    WITH new_data AS (
    SELECT v.client_id, v.pgroup_id, v.report_id, v.email, v.report_name
    FROM user_report_mapping_view v
    LEFT JOIN user_report_mapping_table t
    on v.report_id = t.report_id and v.email = t.email 
    where t.report_id is null 
    )
    INSERT INTO user_report_mapping_table (client_id, pgroup_id, report_id, email, report_name)
    SELECT client_id, pgroup_id, report_id, email, report_name
    FROM new_data
    RETURNING id, client_id, pgroup_id, report_id, email, report_name;
    """
    
    with pg_conn.cursor() as cursor:
        cursor.execute(sql)
        new_data = cursor.fetchall()
        pg_conn.commit()
        
    return new_data

# 1. find extra data that is in the the mapping_table but not in the view
# 2. delete these data from the mapping_table
# 3. return ids of the deleted data
def delete_data(pg_conn):
    sql = """
    WITH del_data AS (
        SELECT t.id
        FROM user_report_mapping_table t
        LEFT JOIN user_report_mapping_view v
        on v.report_id = t.report_id and v.email = t.email 
        where v.report_id is null 
    )
    DELETE FROM user_report_mapping_table
    WHERE id in (select id from del_data)
    RETURNING id
    """
    
    with pg_conn.cursor() as cursor:
        cursor.execute(sql)
        del_data = cursor.fetchall()
        pg_conn.commit()
        
    return del_data

# sync sql server report_mapping_table
def sync_sql_server(new_data, del_data):
    del_ids = [str(row[0]) for row in del_data]
    for row in new_data:
        del_ids.append(str(row[0]))
    
    if len(del_ids) == 0:
        return
    
    # connect to SQL Server
    sql_server_conn = ms_sql_server.create_connection()
    table_name = 'user_report_mapping_table'

    try:
        cursor = sql_server_conn.cursor()
        
        # delete data 
        del_query = f"DELETE FROM {table_name} WHERE id in ({', '.join(del_ids)})"
        cursor.execute(del_query)

        # insert data
        if len(new_data) > 0:
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in new_data[0]])})"
            for row in new_data:
                cursor.execute(insert_query, row)
        
        # commit
        sql_server_conn.commit()
    
    finally:
        sql_server_conn.close()


def test():
    
    # sync delta
    sync_delta()
    
    # sync whole table
    sync_whole()
    

    # other adhoc tests    
    pg_conn = pg_create_connection()
    pg_cursor = pg_conn.cursor()
    
    #
    # test get_new_data(pg_conn)
    #
    sql = "delete from user_report_mapping_table where id=21"
    pg_cursor.execute(sql)
    pg_conn.commit()
    new_data = get_new_data(pg_conn)
    for row in new_data:
        print(row)

    #
    # test delete_data(pg_conn)
    # 
    sql = "insert into user_report_mapping_table values (22, 1, 5, 1150, 'mding', 'mding@trg.com');"
    pg_cursor.execute(sql)
    pg_conn.commit()
    del_data =  delete_data(pg_conn)   
    for row in del_data:
        print(row)
    
    
    # 
    # test sync_delta()
    #
    sql = "update portfolio_info set report_id=1137 where port_id=49"
    pg_cursor.execute(sql)
    pg_conn.commit()
    
    sync_delta()
    
    # close database    
    pg_cursor.close()
    pg_conn.close()
    
    