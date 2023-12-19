"""
This module is used to create and validate the json 
of the schema of a given table.
"""




import psycopg2
import os
import json
from . import connect_to_db



HOST = os.getenv('db_host')
DATABASE = os.getenv('db_database')
USER = os.getenv('db_user')
PASSWORD = os.getenv('db_password')

cur = connect_to_db.connect().cursor()





"""
create schema json of passed in database schema name.
Schema data is written to json file.
"""
def create_schema(table_schema):
    
    schemas = {}
    table_list = []
    cur.execute("""
    select table_name 
    from information_schema.tables
    where table_schema = '{}'
    """.format(table_schema))
    tables = cur.fetchall()
    for table in tables:
        table_list.append(table[0])
        
    for table in table_list:
        cur.execute("select * from portal.{} limit 0".format(table))
        schemas[table] = []
        for desc in cur.description:
            schemas[table].append(desc[0])
    
    with open("schemas.json", 'w') as file:
        json.dump(schemas,file)
    


"""
create schema json and return schema data.
data is not written to file.
"""
def get_schema_data(table_schema):
    
    schemas = {}
    schemas[table_schema] = []
    table_list = []
    cur.execute("""
    select table_name 
    from information_schema.tables
    where table_schema = '{}'
    """.format(table_schema))
    tables = cur.fetchall()
    for table in tables:
        schemas[table_schema].append(table[0])
    
    return schemas

"""
gets schema name of passed in table
"""
def get_table_schema_name(table):
    abs_table_split = table.split(".")
    if len(abs_table_split) > 1:
        return abs_table_split[0]
    query = """
    SELECT table_schema 
    FROM   information_schema.tables 
    WHERE  table_name = '{}'
    """.format(table)
    cur.execute(query)
    return cur.fetchall()[0][0]



"""
compares passed in json schema with current 
database schema
"""
def validate_schema(cur,table_schema,schema_json):
    
    if schema_json == []:
        return False
    
    print('validating schema')
    schemas = {}
    table_list = []
    cur.execute("""
    select table_name
    from information_schema.tables
    where table_schema = '{}'
    """.format(table_schema))
    tables = cur.fetchall()
    for table in tables:
        table_list.append(table[0])
    
    for table in table_list:
        cur.execute("select * from portal.{} limit 0".format(table))
        schemas[table] = []
        for desc in cur.description:
            schemas[table].append(desc[0])
    
    for table in schemas.keys():
        if table not in schema_json:
            return False
        
        if tuple(schemas[table]) != tuple(schema_json[table]):
            return False
    return True

