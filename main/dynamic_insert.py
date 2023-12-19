"""
This module is used to generate sql to insert extracted 
data back into the database
"""
# add table essential columns to stack, then query table id from stack
# issue -> 'None' values in json should be converted to NULL
import sys

sys.path.append("../modules")
# sys.stdout.reconfigure(encoding='utf8')
from modules import get_columns
from modules import connect_to_db
from modules import schema as schema_module
import json
import argparse
import ast
import datetime
import os
from psycopg2 import sql

con = connect_to_db.connect()
cur = con.cursor()
ESSENTIAL_COLUMNS_VALUE_CACHE = {}
ESSENTIAL_COLUMNS_CACHE = {}
IDENTIFIER_RESOLVE_CACHE = {}
TABLE_ID_MAP = {}
GEN_SQL_SEEN_TABLES = set()

# TODO: add all sql data types. the key is what the datatype is called when retrieved from information schema, value is sql clause
DATA_TYPE_RESOLVE = {
    "character varying": "varchar",
    "bigint": "bigint",
    "timestamp without time zone": "timestamp"
}

# variable checks whether or not the time taken should be printed
is_auto_insert = False

def dynamic_insert(args):
    if not args.generate_sql and not args.auto_insert:
        print("Please specify option: --generate_sql or --auto_insert")
        return
    if args.generate_sql:
        if not args.outfile:
            print("Please specify name of output file.")
            return
        with open(args.outfile, 'w') as file:
            file.write("\n")
    with open(args.schemajson.name, "r") as sch_file:
        schema_json = json.load(sch_file)

        with open(args.datafile.name, "r") as file:
            for data in file.readlines():
                if data != "," and data != "[" and data != "\n":
                    try:
                        json_data = json.loads(data)
                    except:
                        continue
                    if args.generate_sql:
                        generate_sql(json_data, schema_json, args)
                    if args.auto_insert:
                        auto_insert(json_data, schema_json)
    if args.generate_sql:
        drop_temporary_tables(args)
    if args.auto_insert:
        con.commit()
    con.close()
                   


def auto_insert(json_data, schema_json):
    global is_auto_insert
    is_auto_insert = True
    traverse_and_insert_children(json_data, schema_json)

def traverse_and_insert_children(json_data, schema_json):
    if not json_data:
        return
    # print(json_data)
    do_insert(json_data, schema_json)
    for children in json_data["children"]:
        traverse_and_insert_children(children, schema_json)

# TODO - complete generate sql function

def generate_sql(json_data, schema_json, args):
    with open(args.outfile, 'a', encoding='utf-8') as file:
        result_query = traverse_and_generate_sql_query(json_data, schema_json)
        file.write(result_query)

def traverse_and_generate_sql_query(json_data, schema_json):
    if not json_data:
        return
    result_query = ""
    if json_data['table_name'] not in GEN_SQL_SEEN_TABLES:
        create_stack_query = get_stack_query(json_data,schema_json)
        # print(create_stack_query)
        GEN_SQL_SEEN_TABLES.add(json_data['table_name'])
        result_query += create_stack_query + "\n"
    result_query += generate_sql_query(json_data, schema_json)
    # print(query)
    for children in json_data['children']:
        result_query += traverse_and_generate_sql_query(children, schema_json) + "\n"
    return result_query

def drop_temporary_tables(args):
    with open(args.outfile, 'a', encoding='utf-8') as file:
        for table in GEN_SQL_SEEN_TABLES:
            table_name_with_underscore = "_".join(table.split("."))
            query = "drop table {}_stack_table;\n".format(table_name_with_underscore)
            # print(query)
            file.write(query + "\n")

def get_stack_query(json_data,schema_json):
    table_name_with_underscore = "_".join(json_data['table_name'].split("."))
    # clear stack table 
    query = ""
    # query += "drop table {}_stack_table;\n".format(table_name_with_underscore)
    query += "CREATE TEMPORARY TABLE {}_stack_table (\n".format(table_name_with_underscore)
    query += "id serial PRIMARY KEY,\n"
    query += "table_id "
    table_primary_key_column = schema_json[json_data['table_name']]["PRIMARY-KEY"][0]
    column_data_type = get_column_data_type(table_primary_key_column,json_data,schema_json)
    query += "{},\n".format(DATA_TYPE_RESOLVE[column_data_type])
    table_foreign_keys = resolve_table_foreign_keys(json_data, schema_json)
    # print("ESSENTIAL_COLUMNS: ", json_data["essential_columns"])
    # print("DATA_TYPE_RESOLVE: ", DATA_TYPE_RESOLVE)
    for column in json_data['essential_columns']:
        if column in table_foreign_keys:
            column_data_type = get_column_data_type(table_foreign_keys[column],json_data,schema_json)
            query += "{} {},".format(table_foreign_keys[column],DATA_TYPE_RESOLVE[column_data_type])
        else:
            column_name = column.split(".")[1]
            column_data_type = get_column_data_type(column_name,json_data,schema_json)
            query += "{} {},".format(column_name,DATA_TYPE_RESOLVE[column_data_type])
    query = query[:-1] + ");\n"
    query += "insert into {}_stack_table (".format(table_name_with_underscore)
    for column in json_data["essential_columns"]:
        if column in table_foreign_keys:
            query += "{},".format(table_foreign_keys[column])
        else:
            query += "{},".format(column.split(".")[1])
    query = query[:-1] + ") values ("
    for column in json_data["essential_columns"]:
        query += "null,"
    query = query[:-1] + ");"
    return query

def generate_friend_friend_sub_query():
    pass

def get_friend_sub_query_recursive(json_data,schema_json,table_friend,current_table):
    if not current_table:
        current_table = table_friend
    table_name_with_underscore = "_".join(current_table.split("."))
    query = "(select table_id from {}_stack_table where ".format(table_name_with_underscore)
    # if len(schema_json[current_table]["FOREIGN-KEY"].keys()) > 1:
    #     print("ERROR: FRIEND TABLE WITH FRIEND FOUND!!")
    #     sys.exit(0)
    #     query += generate_friend_friend_sub_query()
    for parent_table, column_name in schema_json[current_table]["FOREIGN-KEY"].items():
        value = get_friend_sub_query_recursive(json_data,schema_json,table_friend,parent_table)
        # query += "{} = {} and ".format(column_name, value)
        query += "{} in {} and ".format(column_name, value)
    for column in schema_json[current_table]["UNIQUE-KEY"]:
        table_column_name = current_table.split(".")[1] + "_" + column
        data = json_data["friend_payload"][table_friend][table_column_name]
        query += "{} = '{}' and ".format(column, data)
    if query.endswith("where "):
        query = query[:-6]
    if query.endswith("and "):
        query = query[:-4]
    # query += " order by id desc limit 1"
    query += ")"
    return query

def generate_friend_sub_query(json_data, schema_json, table_friend):
    query = get_friend_sub_query_recursive(json_data,schema_json,table_friend,"")
 
    return query

def resolve_table_constraints(json_data, schema_json):
    pass

def remove_quote(string):
    return string.replace("'","''")

def generate_sql_query(json_data, schema_json):
    # Checks if essential columns are present.
    # This check is probably not needed
    has_parent = False
    if not validate_data(json_data):
        print("Essential column data missing for: ", json_data)
        return
    query = ""
    for parent_table_name, column_name in schema_json[json_data['table_name']]['FOREIGN-KEY'].items():
        parent_table_name_with_underscore = "_".join(parent_table_name.split("."))
        if not has_parent:
            stack_query = "with "
        else:
            stack_query = ","
        stack_query += """
        {0}_table_id as (
        select table_id from {0}_stack_table 
        order by id desc
        limit 1
        )
        """.format(parent_table_name_with_underscore)
        query += stack_query
        has_parent = True
    table_foreign_keys = resolve_table_foreign_keys(json_data, schema_json)
    table_friends = resolve_table_friends(json_data, schema_json)
    table_contraints = resolve_table_constraints(json_data, schema_json)
    # if len(table_friends.items()) > 0:
    #     print(table_friends)
    #     sys.exit(0)
    table_name_with_underscore = "_".join(json_data['table_name'].split("."))
    if not has_parent:
        query += "with "
        has_parent = True
    else:
        query += ","
    query += "{}_table_id as ( ".format(table_name_with_underscore)
    # query += "insert into {} (".format(json_data["table_name"])
    query += "insert into {} (".format(json_data["table_name"])
    for column in json_data["payload"].keys():
        query += "{}, ".format(column.split(".")[1])
    query = query[:-2]
    query += ") VALUES ("
    for column_name, column_values in json_data["payload"].items():
        value = ""
        column = column_name.split(".")[1]
        if column in table_foreign_keys:
            if column in table_friends:
                value = generate_friend_sub_query(json_data, schema_json, table_friends[column])
            else:
                parent_name = get_parent_from_column_name(column_name, json_data, schema_json)
                parent_table_name = "_".join(parent_name.split("."))
                value = "(select table_id from {}_table_id)".format(parent_table_name)
            query += "{}, ".format(value)

        else:
            value = column_values
            if value == "None": value = None
            value = cur.mogrify("%s", (value,)).decode('utf-8')
            query +=  "{}, ".format(value)
    query = query[:-2]
    table_id_column_name = schema_json[json_data["table_name"]]["PRIMARY-KEY"][0]
    # query += ") on conflict do nothing returning {})".format(table_id_column_name)
    query += ") on conflict ("

    for _, column_name in schema_json[json_data['table_name']]["FOREIGN-KEY"].items():
        query += "{},".format(column_name)
    for column_name in schema_json[json_data['table_name']]["UNIQUE-KEY"]:
        query += "{},".format(column_name)
    query = query[:-1]

    query += ") do update set "

    for column_name, column_values in json_data["payload"].items():
        value = ""
        column = column_name.split(".")[1]
        if column in table_foreign_keys:
            if column in table_friends:
                # can be stored in cache to skip recomputation
                value = generate_friend_sub_query(json_data, schema_json, table_friends[column])
            else:
                parent_name = get_parent_from_column_name(column_name, json_data, schema_json)
                parent_table_name = "_".join(parent_name.split("."))
                value = "(select table_id from {}_table_id)".format(parent_table_name)
            query += "{} = {}, ".format(column, value)
        if column in schema_json[json_data["table_name"]]["UNIQUE-KEY"]:
            query += "{} = '{}', ".format(column, json_data["payload"][column_name])
    if query.endswith(", "):
        query = query[:-2]
    query += " returning *)\n"
    
    query += "insert into {}_stack_table (table_id,".format(table_name_with_underscore)
    for _, column_name in schema_json[json_data['table_name']]["FOREIGN-KEY"].items():
        query +="{},".format(column_name)
    for column_name in schema_json[json_data['table_name']]["UNIQUE-KEY"]:
        query +="{},".format(column_name)
    query = query[:-1] + ") values ("
    query += "(select {} from {}_table_id),".format(table_id_column_name,table_name_with_underscore)
    for _, column_name in schema_json[json_data['table_name']]["FOREIGN-KEY"].items():
        query +="(select {} from {}_table_id),".format(column_name,table_name_with_underscore)
    for column_name in schema_json[json_data['table_name']]["UNIQUE-KEY"]:
        query +="(select {} from {}_table_id),".format(column_name,table_name_with_underscore)
    query = query[:-1] + ");\n"
    return query

# gets data type of a column. should perhaps be pre-computed and stored in json file
def get_column_data_type(column_name, json_data, schema_json):
    # query = """
    # SELECT pg_typeof({})
    # FROM {};
    # """.format(column_name,json_data['table_name'])
    query = """
    SELECT data_type
    FROM information_schema.columns
    WHERE table_name = '{}'
    and column_name = '{}';
    """.format(json_data['table_name'].split(".")[1],column_name)
    cur.execute(query)
    result = cur.fetchall()
    if not result:
        print("Error occured while fetching column data type:", column_name, json_data['table_name'])
        sys.exit(0)
    return result[0][0]

def resolve_column_data_type(col_data_type):
    return DATA_TYPE_RESOLVE[col_data_type]

def get_table_parent_and_friend_tree(json_data, schema_json):
    table_tree = {}
    table_tree["table"] = json_data["table_name"]
    table_tree["parents"] = []
    table_tree["friends"] = []
    table_name = json_data['table_name']
    while table_name != "":
        table_tree["parent"].insert(0,table_name)
        table_name = schema_json[table_name]["PARENT-TABLE"][0]
    return table_tree


def generate_sql_query_new(table_tree, json_data, schema_json):
    query = ""
    table_foreign_keys = resolve_table_foreign_keys(json_data, schema_json)
    table_friends = resolve_table_friends(json_data, schema_json)
    if len(table_tree["parents"]) > 1:
        query += "with \n"
    for abs_table_name in table_tree["parents"][:-1]:
        table_name_with_underscore = "_".join(abs_table_name.split("."))
        query += "{}_table_id as (\nselect table_id from {0}_stack_table\norder by id desc limit 1),".format(table_name_with_underscore)
    # if query.endswith(","):
    #     query = query[:-1]
    query += "{}_table_id as ( ".format(table_name_with_underscore)
    query += "insert into {} (".format(json_data["table_name"])
    for column in json_data["payload"].keys():
        query += "{}, ".format(column.split(".")[1])
    query = query[:-2]
    query += ") VALUES ("
    for column_name, column_values in json_data["payload"].items():
        value = ""
        column = column_name.split(".")[1]
        if column in table_foreign_keys:
            if column in table_friends:
                value = generate_friend_sub_query(json_data, schema_json, table_friends[column])
            else:
                parent_name = get_parent_from_column_name(column_name, json_data, schema_json)
                parent_table_name = "_".join(parent_name.split("."))
                value = "(select table_id from {}_table_id)".format(parent_table_name)
            query += "{}, ".format(value)

        else:
            value = column_values
            query +=  "'{}', ".format(value)
    query = query[:-2]
    query += ") on conflict do nothing"
        

        

def generate_dynamic_insert_sql(json_data, schema_json):
    if not validate_data(json_data):
        print("Essential column data missing for: ", json_data)
        return
    table_tree = get_table_parent_and_friend_tree(json_data,schema_json)
    query = generate_sql_query(table_tree, schema_json)


def get_parent_from_column_name(column_name,json_data,schema_json):
    col_name = column_name.split(".")[1]
    for parent, column in schema_json[json_data['table_name']]["FOREIGN-KEY"].items():
        if col_name == column:
            return parent
    return ""



def do_insert(json_data, schema_json):
    # Checks if essential columns are present.
    # This check is probably not needed
    if not validate_data(json_data):
        print("Essential column data missing for: ", json_data)
        return
    return_id = -1
    select_query_result = data_already_exists(json_data,schema_json)
    if not select_query_result:
        insert_query, values = get_data_insert_query(json_data,schema_json)
        if not insert_query:
            print("Skipping: ", json_data, "\n")
            return
        return_id = insert_data(insert_query,values)
        update_identifier_resolve_cache_after_insert(json_data,schema_json,return_id)
    else:
        table_name_split = json_data["table_name"].split(".")
        table_columns = get_columns.get_columns_list_with_cursor(cur,table_name_split[0], table_name_split[1])
        table_zipped_columns = dict(zip(table_columns, list(select_query_result[0])))
        return_id = get_primary_key_from_result(json_data["table_name"], table_zipped_columns, schema_json)
        if(return_id == -1):
            print("Failed to get primary key for table: ", json_data["table_name"])
            return
        update_identifier_resolve_cache_after_select(json_data, schema_json, return_id, table_zipped_columns)

def data_already_exists(json_data,schema_json):
    query,values = get_data_check_select_query(json_data,schema_json)
    if query == "":
        return []
    print(query)
    print(values)
    cur.execute(query,values)
    result = cur.fetchall()
    # if result:
    #     print("Already Exists: ", json_data, "\n")
    return result

def get_primary_key_from_result(table_name, table_zipped_columns, schema_json):
    for key, val in table_zipped_columns.items():
        if(key == schema_json[table_name]["PRIMARY-KEY"][0]):
            return val
    

# needs testing
def insert_data(insert_query,values):
    return_id = -1
    print(insert_query)
    print(values)
    cur.execute(insert_query, tuple(values))
    res = cur.fetchall()
    return_id = res[0][0]
    print("res: ",res)
    return return_id
    

# needs testing
def resolve_identifiers():
    essential_columns = ESSENTIAL_COLUMNS_CACHE.keys()
    while tuple(essential_columns) in IDENTIFIER_RESOLVE_CACHE:
        resolved_identifier = IDENTIFIER_RESOLVE_CACHE[tuple(essential_columns)]
        ESSENTIAL_COLUMNS_CACHE.update(resolved_identifier)
        essential_columns = ESSENTIAL_COLUMNS_CACHE.keys()


def update_identifier_resolve_cache_after_insert(json_data, schema_json, return_id):
    table_id_column = ""
    if(schema_json[json_data["table_name"]]["PRIMARY-KEY"]):
        absl_table_name = json_data['table_name']
        table_id_column = absl_table_name.split(".")[1]+ "_" + schema_json[json_data["table_name"]]["PRIMARY-KEY"][0]
    # TODO - change mapping to id -> ident (from schemajson)
    TABLE_ID_MAP[table_id_column] = tuple(schema_json[json_data["table_name"]]["IDENTIFIER"])
    IDENTIFIER_RESOLVE_CACHE[tuple(schema_json[json_data["table_name"]]["IDENTIFIER"])] = return_id

def update_identifier_resolve_cache_after_select(json_data, schema_json, return_id, table_zipped_columns):

    table_id_column = ""
    if(schema_json[json_data["table_name"]]["PRIMARY-KEY"]):
        absl_table_name = json_data['table_name']
        table_id_column = absl_table_name.split(".")[1]+ "_" + schema_json[json_data["table_name"]]["PRIMARY-KEY"][0]
    # TODO - change mapping to id -> ident (from schemajson)
    TABLE_ID_MAP[table_id_column] = tuple(schema_json[json_data["table_name"]]["IDENTIFIER"])
    IDENTIFIER_RESOLVE_CACHE[tuple(schema_json[json_data["table_name"]]["IDENTIFIER"])] = return_id
    for column, _ in json_data['payload'].items():
        table_name = column.split(".")[0]
        abs_table_name = get_table_abs_name(table_name, schema_json)
        if not abs_table_name:
            print("ERROR: Could not find table in schema json: ", table_name)
            sys.exit(0)
        table_id_column = table_name + "_" + schema_json[abs_table_name]["PRIMARY-KEY"][0]
        if column.split(".")[1] == schema_json[abs_table_name]["PRIMARY-KEY"][0] and table_id_column in TABLE_ID_MAP:
                TABLE_ID_MAP[table_id_column] = tuple(schema_json[json_data["table_name"]]["IDENTIFIER"])
                IDENTIFIER_RESOLVE_CACHE[tuple(schema_json[json_data["table_name"]]["IDENTIFIER"])] = table_zipped_columns[column.split(".")[1]]

def get_table_abs_name(table_name, schema_json):
    for abs_table_name in schema_json.keys():
        if table_name == abs_table_name.split(".")[1]:
            return abs_table_name
    return ""

def validate_data(json_data):
    for column_name in json_data["essential_columns"]:
        if column_name not in TABLE_ID_MAP:
            if json_data["payload"][column_name] == "None":
                return False
    return True


def get_data_check_select_query(json_data,schema_json):
    query = "select * from {} where ".format(json_data["table_name"])
    values = []
    query_cut = False
    if json_data["essential_columns"]:
        
        for table_column_name in json_data["essential_columns"]:
            # table_name,column_name = table_column_name.split(".")
            # abs_table_name = json_data["Schema"] + "." + table_name
            
            # if table_name == json_data["table_name"].split(".")[1] or abs_table_name in schema_json[json_data["table_name"]]["FOREIGN-KEY"]:
            #     query_cut = True
            #     query += "{}.{} = '{}' and ".format(json_data["table_name"].split(".")[1],table_column_name.split(".")[1],data)
            data = ""
            column_name = table_column_name.split(".")[1]
            if column_name in TABLE_ID_MAP: # TODO - ensure the right data is in table id map
                data = resolve_table_id(column_name)
                values.append(data)
            else:
                if json_data["payload"][table_column_name] == "None": # TODO - check if there are cases where essential columns are None
                    return ""
                data = json_data["payload"][table_column_name]
                values.append(data)
            query += "{} = %s and ".format(table_column_name)
    
    
    query = query[:-4]
    query += ";"
    return query, values

"""
resolves foreign key identifier into original table primary key column identifier
i.e. lesson_id -> lessons_id, activity_id -> activities_id (needed to match data entered into identifier cache)
"""
def resolve_table_foreign_keys(json_data, schema_json):
    table_foreign_keys = schema_json[json_data["table_name"]]["FOREIGN-KEY"]
    result = {}
    for table_name, foreign_key_column in table_foreign_keys.items():
        result[foreign_key_column] = table_name.split(".")[1] + "_" + schema_json[table_name]["PRIMARY-KEY"][0]
    return result

def resolve_table_friends(json_data, schema_json):
    #print(json_data)
    result = {}
    for table_friend in schema_json[json_data['table_name']]["FRIEND-TABLE"]:
        friend_id_column = schema_json[json_data['table_name']]["FOREIGN-KEY"][table_friend]
        result[friend_id_column] = table_friend
    return result

def get_id_from_payload(payload,schema_json,table_name):
    pass

def get_resolve_select_query(abs_table_name, table_name_with_id,resolve_map,schema_json,json_data,table_friend):
    query = "select * from {} where ".format(abs_table_name)
    values = []
    data = ""
    if len(TABLE_ID_MAP[table_name_with_id]) > 2:
        foreign_tables = schema_json[abs_table_name]["FOREIGN-KEY"]
        
        unique_key_list = schema_json[abs_table_name]["UNIQUE-KEY"]
        # print("ABS_TABLE_NAME: ", abs_table_name)
        # print("UNIQUE_KEY_LIST: ", unique_key_list)
        for table in foreign_tables:
            table_name_id = table.split(".")[1] +"."+ schema_json[table]["PRIMARY-KEY"][0]
            if table_name_id in resolve_map:
                data = resolve_map[table_name_id]
                values.append(data)
                foreign_key_column = schema_json[abs_table_name]["FOREIGN-KEY"][table]
                query += "{} = %s and ".format(foreign_key_column)
        for unique_key in unique_key_list:
            table_column_with_underscore = abs_table_name.split(".")[1] + "_" + unique_key
            # print(json_data)
            # print("TABLE_NAME_WITH_UNDERSCORE: ", table_column_with_underscore)
            # print("TABLE_FRIEND: ", table_friend)
            data = json_data['friend_payload'][table_friend][table_column_with_underscore]
            values.append(data)
            query += "{} = %s and ".format(unique_key)
    else:
        for column in TABLE_ID_MAP[table_name_with_id]:
            
            table_name = column.split(".")[0]
            column_table_abs_name = get_table_abs_name(table_name,schema_json)
            id_column = schema_json[column_table_abs_name]["PRIMARY-KEY"][0]
            table_name_id = table_name +"."+ id_column
            #check that table is indeed a foriegn key
            foreign_tables = schema_json[abs_table_name]["FOREIGN-KEY"]
            foreign_table_list = [x.split(".")[1] for x in foreign_tables.keys()]
            # if(table_name != abs_table_name.split(".")[1] and table_name not in foreign_table_list):
            #     continue
            # data = resolve_map[table_name]
            if table_name_id in resolve_map:
                data = resolve_map[table_name_id]
            else:
                table_column_with_underscore = "_".join(column.split("."))
                data = json_data['friend_payload'][table_friend][table_column_with_underscore]
            values.append(data)
            query += "{} = %s and ".format(column.split(".")[1])
    query = query[:-4]
    query += ";"
    return query, values


def get_friend_id_recursive(json_data,schema_json,table_friend,friend_table_identifiers):
    resolve_map = {}
    current = 0
    # print(friend_table_identifiers)
    # sys.exit(0)
    table_list = [x.split(".")[0] for x in friend_table_identifiers]
    def resolve(abs_table_name,table_name_with_id,json_data,schema_json):
        identifiers = schema_json[abs_table_name]["IDENTIFIER"]
        if (len(identifiers)-1) > len(resolve_map):
            print("Cannot resolve")
            sys.exit(0)
        # make select query to get the id of the table
        # run query
        query, values = get_resolve_select_query(abs_table_name,table_name_with_id,resolve_map,schema_json,json_data,table_friend)
        print(query)
        print(values)
        cur.execute(query,values)
        result = cur.fetchall()
        table_name_split = abs_table_name.split(".")
        table_columns = get_columns.get_columns_list_with_cursor(cur,table_name_split[0], table_name_split[1])
        table_zipped_columns = dict(zip(table_columns,list(result[0])))
        return_id = get_primary_key_from_result(abs_table_name,table_zipped_columns,schema_json)
        id_column = schema_json[abs_table_name]["PRIMARY-KEY"][0]
        resolve_map[table_name_split[1]+"."+id_column] = return_id
        # print("abs_table_name: ", abs_table_name)
        # print("RESULT: ", result)
        # sys.exit(0)

    def generate_resolve_map(table_list):
        if len(table_list) == 0:
            return
        table_name = table_list[0]
        abs_table_name = get_table_abs_name(table_name,schema_json)
        id_column = schema_json[abs_table_name]["PRIMARY-KEY"][0]
        table_name_with_id = table_name + "_" + id_column
        # sys.exit(0)
        if len(TABLE_ID_MAP[table_name_with_id]) > 1:
            # sys.exit(0)
            resolve(abs_table_name,table_name_with_id,json_data,schema_json)
        else:
            resolve_map[table_name+"."+id_column] = json_data["friend_payload"][table_friend][table_name_with_id]
        generate_resolve_map(table_list[current+1:])
    generate_resolve_map(table_list)
    
    table_friend_id = table_friend.split(".")[1] + "." + schema_json[table_friend]["PRIMARY-KEY"][0]
    return resolve_map[table_friend_id]



def get_friend_table_id(json_data,schema_json,table_friend):
    # table_id = get_id_from_payload(json_data['friend_payload'],schema_json,table_friend)\
    friend_table_id_name = table_friend.split(".")[1] + "_" + schema_json[table_friend]["PRIMARY-KEY"][0]
    friend_table_identifiers = TABLE_ID_MAP[friend_table_id_name]
    table_id = get_friend_id_recursive(json_data,schema_json,table_friend,friend_table_identifiers)
    return table_id

def resolve_table_id(column_name):
    identifier_tuple = TABLE_ID_MAP[column_name]
    return IDENTIFIER_RESOLVE_CACHE[identifier_tuple]

def get_data_insert_query(json_data,schema_json):
    table_foreign_keys = resolve_table_foreign_keys(json_data, schema_json)
    table_friend_columns = resolve_table_friends(json_data, schema_json)
    query = "insert into {} (".format(json_data["table_name"])
    for column in json_data["payload"].keys():
        query += "{}, ".format(column.split(".")[1])
    query = query[:-2]
    query += ") VALUES ("
    values = []
    for column_name, column_values in json_data["payload"].items():
        value = ""
        query += "%s,"
        column = column_name.split(".")[1]
        if column in table_foreign_keys:
            if column in table_friend_columns:
                value = get_friend_table_id(json_data,schema_json,table_friend_columns[column])
            else:
                print("table_fore_keys: ", table_foreign_keys, column)
                print("TABLE_ID_MAP: ", TABLE_ID_MAP)
                identifier = TABLE_ID_MAP[table_foreign_keys[column]]
                value = IDENTIFIER_RESOLVE_CACHE[identifier]
                # query +=  cur.mogrify("'{}', ".format(value))
            
        else:
            if column_values == 'None':
                value = None
                # query += cur.mogrify("{}, ".format(value))
            else:
                value = column_values
                # query +=  cur.mogrify("'{}', ".format(value))
        values.append(value)
        
    query = query[:-1]
    table_id_column_name = schema_json[json_data["table_name"]]["PRIMARY-KEY"][0]
    # query += cur.mogrify(") returning {};".format(table_id_column_name))
    query += ") on conflict do nothing returning {};".format(table_id_column_name)
    
    return query, values


def start():
    parser = argparse.ArgumentParser(description="dynamic_insert.py")
    parser.add_argument('--schemajson',nargs='?',type=argparse.FileType('r'),required=True)
    parser.add_argument('--datafile',nargs='?',type=argparse.FileType('r'),required=True)
    parser.add_argument('--auto_insert',default=False, action="store_true", help="specify whether data should be inserted by program")
    parser.add_argument('--generate_sql',default=False, action="store_true", help="specify whether sql queries should be generated. (True by default)")
    parser.add_argument('--outfile', type=str, default="")
    args = parser.parse_args()
    dynamic_insert(args)
now = datetime.datetime.now()
start()
now2 = datetime.datetime.now()
if is_auto_insert:
    print("start time - day: %s   hour: %s   min: %s   sec: %s" % (now.day,now.hour,now.minute,now.second))
    print("end time - day: %s   hour: %s   min: %s   sec: %s" % (now2.day,now2.hour,now2.minute,now2.second))
