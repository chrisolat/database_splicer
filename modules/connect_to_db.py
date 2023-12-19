"""
This module is used to connect to the database
"""

import psycopg2
import os



def connect():
    
    HOST = os.getenv('db_host')
    DATABASE = os.getenv('db_database')
    USER = os.getenv('db_user')
    PASSWORD = os.getenv('db_password')

    con = psycopg2.connect(host = HOST,
                            database = DATABASE,
                            user = USER,
                            password = PASSWORD)
                            
    
    return con

