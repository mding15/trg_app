# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 16:23:22 2024

@author: Allen Zhan
"""
import pyodbc as pyodbc
import threading
import time
from database import ms_sql_server



# heartbeat internval seconds
keep_alive_internval = 1200


def send_heartbeat(conn):
    cursor = conn.cursor()
    cursor.execute("select 1")
    cursor.close()

def keep_alive(conn):
    while True:
        time.sleep(keep_alive_internval)
        try:
            send_heartbeat(conn)
            print("Heartbeat sent")
        except pyodbc.Error as e:
            print(f"Error sending heartbeat: {e}")

def close_connection(conn):
    if conn:
        conn.close()
        
def main():
    conn = ms_sql_server.create_connection()
    print("Connection established")

    # start heartbeat
    heartbeat_thread = threading.Thread(target=keep_alive, args=(conn,))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("Shutting down")
        
    close_connection(conn)
    
if __name__ == '__main__':
    main()
    