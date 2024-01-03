"""
This module is used to get all 
columns of a given table
"""

import psycopg2
import os
import json
import connect_to_db




"""
get list of columns of passed in table
"""

def get_columns_list(table_schema,table_name):
    try:
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure the credentials are correct")
        return
    
    cur.execute("""
    select column_name
    from information_schema.columns
    where table_schema = '{}'
    and table_name = '{}'
    """.format(table_schema,table_name))
    
    res = cur.fetchall()
    
    column_list = []
    
    
    for column_tup in res:
        column_list.append(column_tup[0])
    
    
    return column_list



def get_query_columns_list(query):
    
    output = []
    split_query = query.split(",")
    new_split_query = []
    for q in split_query:
        if "AS" in q:
            new_split_query.append(q.replace(" ", "").strip())
    
    for q in range(len(new_split_query)-1):
        split_query = new_split_query[q].split("AS")
        output.append(split_query[1])
    
    split_query = new_split_query[-1].split("\n")
    output.append(split_query[0].split("AS")[1])
    output[-1] = output[-1].split("from")[0] # quick fix for bug adding "from" to the end of last column value
    
    return output



def get_query_table_list(query):
    output = []
    query = query.replace("select", "")
    split_query = query.split(",")
    new_split_query = []
    for q in split_query:
        if "AS" in q:
            new_split_query.append(q.replace(" ", "").strip())
    
    for q in range(len(new_split_query)-1):
        split_query = new_split_query[q].split("AS")
        table_name = split_query[0].split(".")[0]
        if table_name not in output:
            output.append(table_name)
    
    return output



"""
get json structure of row of passed in table row without contraints
"""
def get_json_struct(view_column_list,row,view_contraints):
    
    view_data = {}
    
    
    
    for column in range(len(row)):
        if view_column_list[column] not in view_contraints["NOT-NEEDED"]:
            view_data[view_column_list[column]] = str(row[column])
        
    return view_data




def get_json_struct_with_contraints(view_column_list,row):
    view_data = {}
    
    
    
    for column in range(len(row)):
        
        view_data[view_column_list[column]] = str(row[column])
        
    return view_data