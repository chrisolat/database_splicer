"""
This module is used to connect to the database
"""

import psycopg2
import os
import sys



def connect(host="", database="", user="", password=""):
    
    HOST = os.getenv('db_host')
    DATABASE = os.getenv('db_database')
    USER = os.getenv('db_user')
    PASSWORD = os.getenv('db_password')

    if not host: host = HOST
    if not database: database = DATABASE
    if not user: user = USER
    if not password: password = PASSWORD
    try:
        con = psycopg2.connect(host = host,
                                database = database,
                                user = user,
                                password = password)
    except:
        print("Unable to connect to database server. Please enter valid credentials.")
        sys.exit(0)
    
    return con

