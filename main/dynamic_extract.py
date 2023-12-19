
"""
This module is used to read data 
from a view/table or sql query passed in from create_view.py
"""
# add function to handle extraction of tables with no primary/unique keys
import sys

sys.path.append("../modules")
from coursesmodules import get_columns
from coursesmodules import connect_to_db
from coursesmodules import schema as schema_module
import json
import argparse
import ast
import datetime




IDENTIFIER_CACHE = {}
PAYLOAD_CACHE = {}

# Dynamic extract main function. Uses sequencial write algorithm(reads chunk of data and writes to file, and repeat until all data is processed)
def extract_view_new_algorithm_with_seq_write(args):
    try:
        con = connect_to_db.connect()
        # cur = con.cursor() # normal way to initialize cursor
        cur = con.cursor("my_cursor") # not sure if or why this works. needed to extract data in chuncks. example seen on stackoverflow
        cur.itersize = 20000
    except:
        print("could not connect to database. please ensure the credentials are correct")
        return
    
    with open(args.schemajson.name,"r") as file:
        json_data = json.load(file)
    

    with open(args.queryfile.name) as file:
        sqlquery = file.read()
        queries = ast.literal_eval(sqlquery)

    
    result = []
    init_query = 0
    print("writing to file")
    with open(args.outfile.name,"w") as file:
        file.writelines("[\n")
        for query in queries:
            if query == "":
                continue
            if init_query > 0:
                file.writelines(",\n")
            file.writelines("[\n")

            # take out \n in query
            query = remove_query_newline(query)
            # get columns from query
            try:
                view_column_list = get_columns.get_query_columns_list(query)
            except:
                print("failed to get columns from: ", query)
                continue

            table_list = get_columns.get_query_table_list(query)
            table_schema = get_columns.get_query_schema(query)
            table_list = [table_schema+"."+i for i in table_list]
            trav = True
            for table in table_list:
                if json_data[table]["PRIMARY-KEY"][0] == "":
                    print("Table: %s has no primary key", table) # TODO - add functionality to extract tables with no primary keys
                    trav = False
            if not trav:
                continue
            output = {}
            
            cur.execute(query) # execute query.
            prev_pkeys = {} # variable to store primary keys of previous row
            cur_pkeys = {} # variable to store primary keys of current row
            counter = 0 # used only to check if we are at first row
            current = 0 # variable to keep track of what column of the row we are on
            
            
            # recursive algorithm to traverse columns
            # current is increased by 1 on each recall. This is how column is traversed recursively.
            # Output is populated as columns are traversed
            def Traverse(output,current,table_list,view_row,prev_pkeys,cur_pkeys):
                
                # if table previous primary-key is same as its current primary-key, move current to next column and call traverse
                if (prev_pkeys[table_list[current]] == cur_pkeys[table_list[current]]):
                    
                    
                    child_data = Traverse(output["children"][-1],current+1,table_list,view_row,prev_pkeys,cur_pkeys)
                    
                    if child_data != None:
                    
                        output["children"].append(child_data)
                    
                
                else:
                    # if current column primary key is not the same as previous column(row above current) primary key, 
                    # create heirarchical json of current column and remaining tables in table list
                    return get_row_json(view_row,table_list[current:],view_column_list,json_data)
        
            
            
            
            
            # get rows in chuncks. amount of rows per chunck is stored in itersize
            view_chunck = cur.fetchmany(cur.itersize)
            # while chunck is not empty, do processing
            while(len(view_chunck) > 0):
                for view_row in view_chunck:
                    # If we are at the first row of the table, initialize all variables
                    if counter == 0:
                        
                        cur_pkeys = get_pkeys_with_table_name(view_column_list,json_data,view_row,table_list)
                        if not cur_pkeys:
                            print("Table has no primary or foreign keys")
                            break
                        output = get_row_json(view_row,table_list,view_column_list,json_data)
                        
                        prev_pkeys = cur_pkeys.copy()
                        counter+=1
                        continue
                    
                    # get primary keys of all tables in row
                    cur_pkeys = get_pkeys_with_table_name(view_column_list,json_data,view_row,table_list)
                    if not cur_pkeys:
                        break
                    # If current column primary key is not the same as previous(row above) column's,
                    # Then write output to file and reinitialize it
                    if cur_pkeys[table_list[current]] != prev_pkeys[table_list[current]]:
                        
                        output_data = json.dumps(output,default=str)
                        file.writelines(output_data)
                        file.writelines("\n")
                        file.writelines(",\n")
                        output = get_row_json(view_row,table_list,view_column_list,json_data)

                    # If primary key is the same as previous column's,
                    # call traverse until mismatch is found and populate output
                    else:
                        Traverse(output,current,table_list,view_row,prev_pkeys,cur_pkeys)
                    
                    # Update previous row -> current row
                    prev_pkeys = cur_pkeys.copy()
                
                if not cur_pkeys:
                    break
                # get next chunck
                view_chunck = cur.fetchmany(cur.itersize)
            
            # If anything is left in output after traversal,
            # write remaining data to file
            if output:
                
                output_data = json.dumps(output,default=str)
                file.writelines(output_data)

            file.writelines("\n]\n")

            init_query+=1
            cur = con.cursor() # not sure if or why this works. needed to extract data in chuncks. example seen on stackoverflow
            cur.itersize = 20000
        file.writelines("]\n")
        
    print("done.")









# recursive function to convert view_row into heirarchical json
# *Quite a complex algorithm. Look in file test_algorithm.py for a simple example
def get_row_json(view_row,table_list,view_column_list,json_data):
    separated_table_json = separate_table_into_json(view_row,table_list,view_column_list,json_data)
    def singleton(table_list,separated_table_json):
        if len(table_list) > 0:
            essential_columns = get_essential_columns(json_data,table_list[0])
            json_data_with_constraints = get_columns.get_json_struct_with_contraints(view_column_list, view_row)
            table_id = get_table_id(json_data[table_list[0]]['PRIMARY-KEY'][0], json_data_with_constraints, table_list[0],json_data)
            
            if table_list[0] not in IDENTIFIER_CACHE:
                IDENTIFIER_CACHE[table_list[0]] = {}
            IDENTIFIER_CACHE[table_list[0]][table_id] = essential_columns
            if table_list[0] not in PAYLOAD_CACHE:
                PAYLOAD_CACHE[table_list[0]] = {}
            PAYLOAD_CACHE[table_list[0]][table_id] = json_data_with_constraints
            friend_essential_columns = get_friend_essential_columns(json_data,table_list[0])
            friend_tree_payloads = get_friend_payloads(json_data, table_list[0], separated_table_json[table_list[0]])
            
            
            # print(IDENTIFIER_CACHE)
            if len(table_list) > 1:
                rest_data = singleton(rest(table_list, 0),separated_table_json)
                return {
                    "Schema": schema_module.get_table_schema_name(table_list[0]),
                    "table_name": table_list[0],
                    "essential_columns":essential_columns,
                    "friend_essential_columns":friend_essential_columns,
                    "friend_payload": friend_tree_payloads,
                    "payload": separated_table_json[table_list[0]],
                    "children": [rest_data]
                    }
            else:
                return {
                    "Schema": schema_module.get_table_schema_name(table_list[0]),
                    "table_name": table_list[0],
                    "essential_columns":essential_columns,
                    "friend_essential_columns":friend_essential_columns,
                    "friend_payload": friend_tree_payloads,
                    "payload": separated_table_json[table_list[0]],
                    "children": []
                    }
    
    def rest(table_list, current):
        return table_list[current+1:]
    
    return singleton(table_list,separated_table_json)





def get_essential_columns(json_data,abs_table_name):
    
    _, table_name = abs_table_name.split(".")
    essential_columns = []
    for _, column in json_data[abs_table_name]["FOREIGN-KEY"].items():
        column_name = table_name + "." + column
        essential_columns.append(column_name)
    for column in json_data[abs_table_name]["UNIQUE-KEY"]:
        column_name = table_name + "." + column
        essential_columns.append(column_name)
    return essential_columns


def get_friend_essential_columns_old(view_row,view_column_list,json_data,abs_table_name,json_data_with_constraints):
    
    zipped_columns = dict(zip(view_column_list,view_row))
    # print(zipped_columns)
    friend_essential_columns = {}
    if len(json_data[abs_table_name]["FRIEND-TABLE"])>0:
        for friend_abs_name in json_data[abs_table_name]["FRIEND-TABLE"]:
            friend = friend_abs_name.split(".")[1]
            friend_id_column_name = json_data[friend_abs_name]["PRIMARY-KEY"][0]
            friend_table_id = get_friend_table_id(friend_id_column_name,json_data_with_constraints,abs_table_name,json_data,friend_abs_name)
            identifier = friend+"_"+friend_id_column_name
            if friend_abs_name in IDENTIFIER_CACHE:
                if friend_table_id == "None":
                    friend_essential_columns[friend] = "None"
                else:
                    friend_essential_columns[friend] = IDENTIFIER_CACHE[friend_abs_name][friend_table_id]
    return friend_essential_columns

def get_friend_essential_columns(json_data,abs_table_name):
    friend_essential_columns = {}

    for abs_table_name in json_data[abs_table_name]["FRIEND-TABLE"]:
        _, table_name = abs_table_name.split(".")
        essential_columns = []
        for parent_abs_name, column in json_data[abs_table_name]["FOREIGN-KEY"].items():
            _, parent_table_name = parent_abs_name.split(".")
            column_name = parent_table_name + "." + column
            essential_columns.append(column_name)
        for column in json_data[abs_table_name]["UNIQUE-KEY"]:
            column_name = table_name + "." + column
            essential_columns.append(column_name)
        friend_essential_columns[abs_table_name] = essential_columns
    return friend_essential_columns

def get_friend_payloads(json_data, abs_table_name, table_payload):
    friend_tree_payloads = {}
    for abs_table_name in json_data[abs_table_name]["FRIEND-TABLE"]:
        friend_tree_payloads[abs_table_name] = get_friend_parent_payloads(json_data, abs_table_name, table_payload)

    return friend_tree_payloads

def get_table_abs_name(table_name, schema_json):
    for abs_table_name in schema_json.keys():
        if table_name == abs_table_name.split(".")[1]:
            return abs_table_name
    return ""

def get_friend_id_from_payload(json_data,abs_friend_name, table_payload):
    table_name = list(table_payload.keys())[0].split(".")[0]
    abs_table_name = get_table_abs_name(table_name, json_data)
    if not abs_table_name:
        print("Could not get table name in schema json")
        sys.exit(0)
    id_column_name = json_data[abs_table_name]["FOREIGN-KEY"][abs_friend_name]
    for column, value in table_payload.items():
        if column.split(".")[1] == id_column_name:
            return value
    return ""




def get_friend_parent_payloads(json_data, abs_friend_table_name, table_payload):
    friend_id = get_friend_id_from_payload(json_data,abs_friend_table_name,table_payload)
    if not friend_id:
        print("Could not get friend ID from payload")
        sys.exit(0)
    payload_data = PAYLOAD_CACHE[abs_friend_table_name][friend_id]
    return payload_data

# get list of parent when tree is a cycle
def get_recursive_parents(essential_columns,zipped_columns):
    con = connect_to_db.connect()
    cur = con.cursor()
    parent_list = [zipped_columns['organizations_name']]
    for column in zipped_columns:
        # TODO - removed hardcoding (contacts.organizations is always a pain)
        if column == "organizations_parent":
            org_parent_Id = zipped_columns[column]
            
            while(org_parent_Id != None):
                cur.execute("select * from contacts.organizations where id = '{}'".format(org_parent_Id))
                org_parent = cur.fetchall()
                parent_list.insert(0,org_parent[0][1])
                org_parent_Id = org_parent[0][3]
    return parent_list




# convert row into dictionary containing table_name as key and payload as value
def separate_table_into_json(view_row,table_list,view_column_list,json_data):
    table_schema = table_list[0].split(".")[0]
    if not table_list:
        return {}
    view_contraints = {
            "NOT-NEEDED" : []
        }
    for table in table_list:
        if table in json_data:
            view_contraints["NOT-NEEDED"].extend(json_data[table]["NOT-NEEDED"])
    
    data = {}
    json_struct = get_columns.get_json_struct(view_column_list,view_row,view_contraints)
    # print(json_struct)
    for table in table_list:
        data[table] = {}
    for col in json_struct.keys():
        table_name = separate_table_name(col,json_data,table_schema)
        # print(col)
        act_table_name = table_name.split(".")[1]
        table_name_col = act_table_name + "." + col.replace(act_table_name,"")[1:]
        if table_name in data:
            data[table_name][table_name_col] = json_struct[col]
    # print(data)
    return data
    
    


# separates table name. i.e. courses_course_name -> [courses, course_name] 
def separate_table_name(table,json_data,table_schmea):
    split_data = table.split("_")
    pnt = len(split_data)
    abs_table_name = ""
    while(abs_table_name not in json_data and pnt > 0):
        table_name = "_".join(split_data[:pnt])
        abs_table_name = table_schmea+"."+table_name
        pnt-=1
    
    return abs_table_name
    







# returns a dictionary containing table_name as key and its primary-key as value
def get_pkeys_with_table_name(view_column_list,json_data,view_row,table_list):
    view_pkeys = []
    
    table_name = []
    for cols in range(len(view_column_list)):
        table_schema = table_list[0].split(".")[0]
        # break into table name and column name
        for table in table_list:
            if view_column_list[cols].startswith(table.split(".")[1]):
                table_name = view_column_list[cols].split(table.split(".")[1]+"_")
                table_name[0] = table.split(".")[1]
                break
        
        if table_name and table_schema+"."+table_name[0] in json_data:
            
            if json_data[table_schema+"."+table_name[0]]["PRIMARY-KEY"][0] == table_name[1]:
                
                if (table_schema+"."+table_name[0], view_column_list[cols]) not in view_pkeys:
                    view_pkeys.append((table_schema+"."+table_name[0], view_row[cols]))
    
    return dict(view_pkeys)



# get table names from identifiers
def get_identifier_tables(table,json_data):
    table_identifiers = json_data[table]["IDENTIFIER"]
    identifier_tables = []
    for table_name in json_data[table]["IDENTIFIERS"]:
        identifier_tables.append(table_name.split(".")[0])
    

# get table id from list of data with constraints
def get_table_id(id_column_name, json_data_with_constraints, abs_table_name, json_data):
    table_name = abs_table_name.split(".")[1]
    table_name_id_column = table_name+"_"+id_column_name
    table_id = ""
    if table_name_id_column in json_data_with_constraints:
        table_id = json_data_with_constraints[table_name_id_column]
    return table_id
    

# get friend table id from a list of constraints
def get_friend_table_id(id_column_name, json_data_with_constraints, abs_table_name, json_data, abs_friend_name):
    table_name = abs_table_name.split(".")[1]
    friend_name = abs_friend_name.split(".")[1]
    friend_table_id = ""
    table_foreign_key_column = json_data[abs_table_name]["FOREIGN-KEY"][abs_friend_name]
    table_name_id_column = table_name + "_" + table_foreign_key_column
    if table_name_id_column in json_data_with_constraints:
        friend_table_id = json_data_with_constraints[table_name_id_column]
    return friend_table_id

def remove_query_newline(query):
    new_query = ""
    split_query = query.split("\n")
    for statement in split_query:
        new_query += statement + " "
    return new_query

def start():
    
    parser = argparse.ArgumentParser(description="extract_questions.py")
    parser.add_argument('--schemajson',nargs='?',type=argparse.FileType('r'),required=True)
    parser.add_argument('--queryfile',nargs='?',type=argparse.FileType('r'),required=True)
    parser.add_argument('--outfile',nargs='?',type=argparse.FileType('w'),required=True)
    args = parser.parse_args()

    # try:
    extract_view_new_algorithm_with_seq_write(args)
#     except:
#         print("Something went wrong. Please enter input correctly\n\
# i.e. Python3 create_view.py courses | Python3 read_view.py\
# <filename.json>\nOR <query> | Python3 read_view.py <filename.json>")

now = datetime.datetime.now()
start()
now2 = datetime.datetime.now()
print("start time - day: %s   hour: %s   min: %s   sec: %s" % (now.day,now.hour,now.minute,now.second))
print("end time - day: %s   hour: %s   min: %s   sec: %s" % (now2.day,now2.hour,now2.minute,now2.second))
