"""
This module is used to 
generate sql queries that can be used to extract data from tables
"""

import sys
from modules import get_columns
from modules import connect_to_db
from modules import schema as schema_module
import json
import argparse

# connect to database
cur = connect_to_db.connect().cursor()
    
# function to generate sql query of passed in table or tables
def generate_sql(args):
    
    # open detailed schema file
    try:
        with open(args.schemajson.name, 'r') as file:
            json_data = json.load(file)
    except:
        print("Schema file not found")
        return
    
    table_input = []
    if args.tables:
        for table in args.tables:
            if len(table.split(".")) < 2:
                print("Please enter table name in the format: <schema>.<table>.\ni.e courses should be portal.courses.")
                return
        table_input = args.tables
    column_and_data_list = []
    if args.columns:
        for entry in args.columns:
            column_and_data = entry.split("=")
            column_and_data_list.append(column_and_data)

    
    Trees = json_data["Trees"]
    seen_tables = set()
    result_query = []

    # add table friend to table input
    # if table_input:
    #     for table in table_input:
    #         table_friend = json_data[table]["FRIEND-TABLE"]
    #         if len(table_friend) > 0 and table_friend[0] not in table_input:
    #             table_input.append(table_friend)
    
    if table_input:
        table_input = add_friend_table(table_input, json_data)
    use_column_list = False
    if table_input:
        for table in table_input:
            # boolean variable to decide if column_data_list should be used when generating sql for current table.
            # this is needed because this function may generate sql for unrelated tables
            use_column_list = column_exists_in_table(table,column_and_data_list)
            if use_column_list:
                for i in range(len(column_and_data_list)):
                    column_and_data_list[i][0] = table.split(".")[1] +"."+ column_and_data_list[i][0]
            table_tree = get_table_tree(table, json_data)
            tree_schema = schema_module.get_table_schema_name(table)
            if args.schema:
                tree_schema = args.schema
            query = build_query(table_tree,tree_schema,json_data,column_and_data_list,use_column_list)
            result_query.append(query)
                
    else:
        for tree in Trees.keys():
            if tree != "adjacency_list":
                tree_schema = schema_module.get_table_schema_name(Trees[tree][0])
                query = build_query(Trees[tree],tree_schema,json_data)
                if query:
                    result_query.append(query)
    # result_query = list(set(result_query))
    if table_input and not result_query:
        print("Tables entered are not in same schema")
        return
    
    
    for query in result_query:
        for statement in query.split('\n'):
            print(statement.strip())
        
    with open(args.outfile.name, 'w') as file:
        # print(result_query)
        print(len(result_query), "query(s)")
        if args.columns and not use_column_list:
            print("Warning: specified columns were not found in any tables!")
        file.write(str(result_query))
    

# check to see if column really exists in table   
def column_exists_in_table(table_name,column_and_data_list):
    if not column_and_data_list:
        return False
    table_split = table_name.split(".")
    columns = [x[0] for x in column_and_data_list]
    table_columns = get_columns.get_columns_list(table_split[0],table_split[1])
    table_columns = set(table_columns)
    for cols in columns:
        if cols not in table_columns:
            return False
    return True

"""
Traverses tree and adds friend tables to collection 
"""
def add_friend_table(table_input,json_data):
    result = table_input.copy()
    cycle_check = set(result)
    for table in table_input:
        
        friend_table_stack = [table]
        table_tree = get_table_tree(table, json_data)
        table_parent = ""
        while(friend_table_stack):
            table = friend_table_stack.pop(0)
            
            if table in json_data: # avoid tables from different schema
                if len(json_data[table]["FRIEND-TABLE"]) > 0:
                    for ftable in json_data[table]["FRIEND-TABLE"]:
                        if ftable not in table_tree and ftable in json_data:
                            if ftable in cycle_check:
                                return list(set(result))
                            cycle_check.add(ftable)
                            result.insert(0,ftable)
                            table_tree.extend(get_table_tree(ftable,json_data))
                    friend_table_stack.extend(json_data[table]["FRIEND-TABLE"])
                if json_data[table]["PARENT-TABLE"]:
                    table_parent = json_data[table]["PARENT-TABLE"][0]
                    if table_parent in cycle_check:
                        return result
                    cycle_check.add(table_parent)
                else:
                    table_parent = ""
                while(table_parent != ""):
                    if len(json_data[table_parent]["FRIEND-TABLE"]) > 0:
                        for ftable in json_data[table_parent]["FRIEND-TABLE"]:
                            if ftable not in table_tree and ftable in json_data:
                                if ftable in cycle_check:
                                    return list(set(result))
                                cycle_check.add(ftable)
                                result.insert(0,ftable)
                                table_tree.extend(get_table_tree(ftable,json_data))
                        friend_table_stack.extend(json_data[table_parent]["FRIEND-TABLE"])
                    if json_data[table_parent]["PARENT-TABLE"]:
                        table_parent = json_data[table_parent]["PARENT-TABLE"][0]
                        if table_parent in cycle_check:
                            return result
                        cycle_check.add(table_parent)
                    else:
                        table_parent = ""
    
    return result    

"""
Gets tree of passed in table using schema file
"""
def get_table_tree(table,json_data):
    table_tree = []
    cycle_check = set()
    cycle_check.add(table)
    table_parent = ""
    if table in json_data:
        if json_data[table]["PARENT-TABLE"]:
            table_parent = json_data[table]["PARENT-TABLE"][0]
        table_tree.insert(0,table)
        if table_parent and table_parent in cycle_check:
            print("cycle found:", table_parent)
            return table_tree
        if table_parent:
            cycle_check.add(json_data[table]["PARENT-TABLE"][0])
            table_tree.insert(0,json_data[table]["PARENT-TABLE"][0])
    
    while(table_parent != ""):
        if json_data[table_parent]["PARENT-TABLE"]:
            table_parent = json_data[table_parent]["PARENT-TABLE"][0]
        else:
            table_parent = ""
        if table_parent:
            if table_parent in cycle_check:
                print("cycle found:",table_parent)
                return table_tree
            if table_parent:
                cycle_check.add(table_parent)
                table_tree.insert(0,table_parent)
    return table_tree



# function to build sql query
def build_query(table_list,DB_SCHEMA,json_data,column_and_data_list,use_columns_list):
    if not table_list:
        print("Table(s) not found")
        sys.exit(0)
    DB_SCHEMA = table_list[0].split(".")[0]
    view_pkeys = []
    table_json = {}
    # building sql query
    sql_query = "select "
    # get the columns of each table and append
    # them to dictionary where table name is key
    # and the list of columns is value
    for table in table_list:
        table_json[table] = []
        get_cols = get_columns.get_columns_list(DB_SCHEMA,table.split(".")[1])
        for cols in get_cols:
            table_json[table].append(cols)

    for table in table_json.keys(): # Iterate through tables
        for cols in table_json[table]: # Iterate through columns of table in dictionary
            # this is to make column names unique 
            # i.e. table -> courses, column -> course_name
            # identifier -> courses.course_name
            # unique name -> courses_course_name
            table_name = table.split(".")[1] + "." + cols
            new_table_name = table.split(".")[1] + "_" + cols
            sql_query += "\n {} AS {},".format(table_name,new_table_name)
            
            # If column is a primary key column,
            # store in list for later use
            if cols in json_data[table]["PRIMARY-KEY"]:
                view_pkeys.append(new_table_name)
    
    #  Sql query end
    #sql_query = sql_query[:-2]
    query_len = len(sql_query)-1
    while(sql_query[query_len] == " "):
        query_len -= 1
    if sql_query[query_len] == ",":
        sql_query = sql_query[:-1]
    sql_query += "\n from {};".format(table_list[0])
    
    # if more than one table is passed in
    if len(table_list) > 1 and table_list[1] != "organizations": # TODO - remove hardcoding
        sql_query = sql_query[:-1]
        # Iterate through the rest of the tables
        for table in range(1,len(table_list)):
            
            parent_table = table_list[table-1]
            cur_table = table_list[table]
            
            if json_data[cur_table]["PARENT-TABLE"]:
                if cur_table == json_data[cur_table]["PARENT-TABLE"][0]:
                    print("cycle found:", cur_table)
                    return ""

            # If the parent table is not a foreign key
            # of the child table, print error and return
            if parent_table not in json_data[cur_table]["FOREIGN-KEY"]:
                
                print("tables not connected:", cur_table, parent_table )
                return ""

            # cur_table_fkey = json_data[cur_table]["FOREIGN-KEY"][parent_table]
            cur_table_pkey = json_data[cur_table]["PRIMARY-KEY"][0]
            
            parent_table_pkey = json_data[parent_table]["PRIMARY-KEY"][0]
            
            cur_table_fkey = json_data[cur_table]["FOREIGN-KEY"][parent_table]
            
            # sql footer to join multiple tables together
            sql_query += "\n right join {} on {}.{} = {}.{}".format(table_list[table],
            parent_table.split(".")[1],parent_table_pkey,cur_table.split(".")[1],cur_table_fkey)
        
        if use_columns_list:
            sql_query += "\nwhere "
            for column,data in column_and_data_list:
                sql_query += "{} = '{}' and ".format(column,data)
            sql_query = sql_query[:-4]

        if view_pkeys:
            footer = "\n order by "
            for table in view_pkeys:
                footer += table + ", "
            
            sql_query += footer
    
    # Add ; to the end of the query
    query_len = len(sql_query)-1
    while(sql_query[query_len] == " "):
        query_len -= 1
    if sql_query[query_len] == ",":
        sql_query = sql_query[:-1]
    sql_query = sql_query[:query_len] + ";"
    
    
    return sql_query

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="extract_questions.py")
    parser.add_argument("--tables",nargs='+',help="pass in leaf tables to extract", required=True)
    parser.add_argument("--columns",nargs='+',help="specify columns to extract")
    parser.add_argument("--extract_ancestor_trees",default=False,action="store_true",help="extract all ancestor trees")
    parser.add_argument("--extract_full_tree",default=False,action="store_true",help="extract full tree")
    parser.add_argument('--outfile',nargs='?',type=argparse.FileType('w'),required=True)
    parser.add_argument('--schemajson',nargs='?',type=argparse.FileType('r'),default=r"../modules/Schema_Json", required=True)
    parser.add_argument('--schema',default="",help="specify schema name")
    args = parser.parse_args()
    generate_sql(args)
 
 
 
 