"""
This script is used to create a txt file 
of a database schema
"""
import sys
sys.path.append("../modules")
import get_columns
import argparse
import sys
import connect_to_db


def schema(schema_name):
    try:
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure the credentials are correct")
        return
    
    try:
        cur.execute("select * from information_schema.tables where table_schema = %s", (schema_name,))
    except:
        print("schema not found in database")
        return
    
    
    
    database = {}
    database[schema_name] = []
    
    for table in cur.fetchall():
        database[schema_name].append(table[2])
    
    
    
    
    write_to_txt(database[schema_name],schema_name)

def write_to_txt(schema_data,schema_name):
    
    table_list = schema_data
    table_schema = schema_name
    
    table_column_list = []
    
    for table_name in table_list:
    
        column_list = get_columns.get_columns_list(table_schema,table_name)
        
        for column in column_list:
            
            table_column_list.append(table_name+"."+column)
            
    
    
    # txt_name = schema_name + ".txt"
    # with open(txt_name,'w') as file:
        # for table in table_column_list:
            # file.write(table+"\n")
    
    for table in table_column_list:
        print(table)


def start():
    parser = argparse.ArgumentParser(description="schema_text.py")
    
    if len(sys.argv) <= 1:
        print("enter schema name")
        return
    schema_name = sys.argv[1]
    
    schema(schema_name)

if __name__ == "__main__":
    start()