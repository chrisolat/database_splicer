"""
This script is used to check whether data remains consistent when
transfered to another database.
"""

import sys
import os

from modules import get_columns
from modules import connect_to_db
from modules import schema as schema_module
import json
import argparse
import datetime
import random

# credentials are obtained from environment variables by default.
# specify here by entering connect function params.
con = connect_to_db.connect(host="",
                            database="",
                             user="",
                             password="")
cursor1 = con.cursor()

# specify database credentials for destination database.
con2 = connect_to_db.connect(host="",
                            database="",
                             user="",
                             password="")
cursor2 = con2.cursor()

DATABASE_1_TABLE_ID_MAP = {}
DATABASE_2_TABLE_ID_MAP = {}
DATABASE_1_IDENTIFIER_RESOLVE_CACHE_WITH_COLUMNS = {}
DATABASE_1_IDENTIFIER_RESOLVE_CACHE_WITH_DATA = {}
DATABASE_2_IDENTIFIER_RESOLVE_CACHE_WITH_COLUMNS = {}
DATABASE_2_IDENTIFIER_RESOLVE_CACHE_WITH_DATA = {}
DATABASE_1_NATURAL_KEYS = {}
DATABASE_2_NATURAL_KEYS = {}

def compare_data(args):
    with open(args.schemajson.name, 'r') as sch_file:
        schema_json = json.load(sch_file)

        with open(args.datafile.name, 'r') as file:
            for data in file.readlines():
                if data != "," and data != "[" and data != "\n":
                    try:
                        json_data = json.loads(data)
                    except:
                        continue
                    
                    # Random sampling can be set to True or False.
                    # Set it to True if data can be randomly checked for consistency. This will
                    # reduce the run time of the program. Note that this
                    # check may fail if the connected tables is not in a linear graph format. 
                    # In essense, if any table in the database has more than one parent, 
                    # random_sampling should be set to False. 
                    random_sampling = False

                    if(do_check(random_sampling)):
                        if not traverse_and_check_consistency(json_data, schema_json):
                            print("Error: data is inconsistent!!")
                            return
    print("Successful!")


def do_check(random_sampling):
    # Random sampling. This decides whether or not data should
    # be processed.
    # 0 for false, 1 for True.
    values = [0, 1]
    weights = [7, 3] # higher chance of being 0.
    result = 1
    # Generate a random number using choice() with assigned weights. More likely will be zero.
    if random_sampling:
        result = random.choices(values, weights=weights)[0]
    return result

def traverse_and_check_consistency(json_data, schema_json):
    if len(json_data) == 0:
        return True
    if not check_consistency(json_data, schema_json):
        return False
    for children in json_data["children"]:
        if not traverse_and_check_consistency(children, schema_json):
            return False
    return True

def check_consistency(json_data, schema_json):

    database_1_query, database_1_values = get_data_check_select_query(json_data, schema_json,DATABASE_1_TABLE_ID_MAP,DATABASE_1_IDENTIFIER_RESOLVE_CACHE_WITH_COLUMNS, DATABASE_1_IDENTIFIER_RESOLVE_CACHE_WITH_DATA)
    database_2_query, database_2_values = get_data_check_select_query(json_data, schema_json,DATABASE_2_TABLE_ID_MAP,DATABASE_2_IDENTIFIER_RESOLVE_CACHE_WITH_COLUMNS, DATABASE_2_IDENTIFIER_RESOLVE_CACHE_WITH_DATA)
    if database_1_query == "":
        print("Database 1 Empty query!!")
        print(json_data)
        sys.exit(0)
    if database_2_query == "":
        print("Database 2 Empty query!!")
        print(json_data)
        sys.exit(0)
    print("QUERY1: ", database_1_query, database_1_values)
    print("QUERY2: ", database_2_query, database_2_values)
    cursor1.execute(database_1_query, database_1_values)
    cursor2.execute(database_2_query, database_2_values)
    result1 = cursor1.fetchall()
    result2 = cursor2.fetchall()
    if(len(result1) > 1 or len(result2) > 1):
        print("Select query returned more than 1 row")
        return False
    print("RESULT1:",result1)
    print("RESULT2:",result2)

    if not result1:
        print("result1 empty", json_data['table_name'])
        return False
    if not result2:
        print("resulst2 empty", json_data['table_name'])
        return False

    database1_data = get_unique_key_from_result(cursor1,result1,json_data,schema_json)
    database2_data = get_unique_key_from_result(cursor2,result2,json_data,schema_json)
    if database1_data:
        DATABASE_1_NATURAL_KEYS[database1_data[0]] = database1_data[1]
    if database2_data:
        DATABASE_2_NATURAL_KEYS[database2_data[0]] = database2_data[1]
    for column in DATABASE_1_NATURAL_KEYS.keys():
        if column not in DATABASE_2_NATURAL_KEYS:
            return False
        if DATABASE_1_NATURAL_KEYS[column] != DATABASE_2_NATURAL_KEYS[column]:
            return False
    

    # logic below is used to populate identifier cache. It may not be needed.
    table_name_split = json_data["table_name"].split(".")
    database_1_table_columns = get_columns.get_columns_list_with_cursor(cursor1,table_name_split[0], table_name_split[1])
    database_2_table_columns = get_columns.get_columns_list_with_cursor(cursor2,table_name_split[0], table_name_split[1])
    database_1_table_zipped_columns = dict(zip(database_1_table_columns, list(result1[0])))
    database_2_table_zipped_columns = dict(zip(database_2_table_columns, list(result2[0])))
    database_1_return_id = get_primary_key_from_result(json_data["table_name"], database_1_table_zipped_columns, schema_json)
    database_2_return_id = get_primary_key_from_result(json_data["table_name"], database_2_table_zipped_columns, schema_json)
    if(database_1_return_id == -1):
        print("Failed to get primary key for table: ", json_data["table_name"])
        sys.exit(0)
    if(database_2_return_id == -1):
        print("Failed to get primary key for table: ", json_data["table_name"])
        sys.exit(0)
    
    update_identifier_resolve_cache_after_select(json_data, schema_json, database_1_return_id, database_1_table_zipped_columns, DATABASE_1_TABLE_ID_MAP, DATABASE_1_IDENTIFIER_RESOLVE_CACHE_WITH_COLUMNS, DATABASE_1_IDENTIFIER_RESOLVE_CACHE_WITH_DATA)
    update_identifier_resolve_cache_after_select(json_data, schema_json, database_2_return_id, database_2_table_zipped_columns, DATABASE_2_TABLE_ID_MAP, DATABASE_2_IDENTIFIER_RESOLVE_CACHE_WITH_COLUMNS, DATABASE_2_IDENTIFIER_RESOLVE_CACHE_WITH_DATA)
    return True



def get_unique_key_from_result(cursor, result, json_data, schema_json):
    database1_table_columns_list = get_columns.get_columns_list_with_cursor(cursor, json_data["table_name"].split(".")[0], json_data["table_name"].split(".")[1])
    database_data = get_columns.get_json_struct_with_contraints(database1_table_columns_list, result[0])
    if(len(schema_json[json_data['table_name']]['UNIQUE-KEY']) == 0):
        return []
    for column, data in database_data.items():
        if(column == schema_json[json_data['table_name']]['UNIQUE-KEY'][0]):
            return [column, data]

# Copied from dynamic_extract.py. Logic gets essential columns and runs select queries. Minor changes made for use case.
#--------------------------------------------------------------------------------------------------------------------------


def get_primary_key_from_result(table_name, table_zipped_columns, schema_json):
    for key, val in table_zipped_columns.items():
        if(key == schema_json[table_name]["PRIMARY-KEY"][0]):
            return val


def update_identifier_resolve_cache_after_select(json_data, schema_json, return_id, table_zipped_columns,table_id_map,identifier_resolve_cache_with_columns,identifier_resolve_cache_with_data):
    
    table_id_column = ""
    if(schema_json[json_data["table_name"]]["PRIMARY-KEY"]):
        absl_table_name = json_data['table_name']
        table_id_column = absl_table_name.split(".")[1]+ "_" + schema_json[json_data["table_name"]]["PRIMARY-KEY"][0]
    
    table_id_map[table_id_column] = frozenset(schema_json[json_data["table_name"]]["IDENTIFIER"])
    identifier_resolve_cache_with_columns[frozenset(schema_json[json_data["table_name"]]["IDENTIFIER"])] = return_id
    identifier_data = []
    
    for table_column in schema_json[json_data["table_name"]]["IDENTIFIER"]:
        column = table_column.split(".")[1]
        # TODO - May need to rewrite logic to update identifier data cache
        # Data may be incorrect due to the decision made in the if-elif branching below
        # Also make changes to ensure the identfier data cache is updated only once, right before the function exits - after data is computed. 
        if column in schema_json[json_data["table_name"]]["UNIQUE-KEY"]:
            if table_zipped_columns[column] not in identifier_data:
                identifier_data.append(table_zipped_columns[column])
        elif frozenset([table_column]) in identifier_resolve_cache_with_columns:
            print("TABLE_COL",table_column)
            identifier_data.append(identifier_resolve_cache_with_columns[frozenset([table_column])])
        elif column in table_zipped_columns:
            print("OTHR_COL", column)
            identifier_data.append(table_zipped_columns[column])
        
    identifier_resolve_cache_with_data[frozenset(identifier_data)] = return_id
    for column, _ in json_data['payload'].items():
        table_name = column.split(".")[0]
        abs_table_name = get_table_abs_name(table_name, schema_json)
        if not abs_table_name:
            print("ERROR: Could not find table in schema json: ", table_name)
            sys.exit(0)
        table_id_column = table_name + "_" + schema_json[abs_table_name]["PRIMARY-KEY"][0]
        if column.split(".")[1] == schema_json[abs_table_name]["PRIMARY-KEY"][0] and table_id_column in table_id_map:
            table_id_map[table_id_column] = frozenset(schema_json[json_data["table_name"]]["IDENTIFIER"])
            identifier_resolve_cache_with_columns[frozenset(schema_json[json_data["table_name"]]["IDENTIFIER"])] = table_zipped_columns[column.split(".")[1]]
            identifier_resolve_cache_with_data[frozenset(identifier_data[:])] = table_zipped_columns[column.split(".")[1]]
        if column.split(".")[1] in schema_json[abs_table_name]["UNIQUE-KEY"]:
            identifier_resolve_cache_with_columns[frozenset([column])] = table_zipped_columns[column.split(".")[1]]


def get_table_abs_name(table_name, schema_json):
    for abs_table_name in schema_json.keys():
        if table_name == abs_table_name.split(".")[1]:
            return abs_table_name
    return ""


def resolve_table_id(json_data,column_name, table_id_map, identifier_resolve_cache_with_columns,identifier_resolve_cache_with_data):
    
    identifier_tuple = table_id_map[column_name]
    print("IN RESOLVE", identifier_tuple, identifier_resolve_cache_with_columns[identifier_tuple])
    return identifier_resolve_cache_with_columns[identifier_tuple]

def resolve_friend_table_id(json_data,column_name,friend_columns_dict,table_id_map,identifier_resolve_cache_with_columns, identifier_resolve_cache_with_data):

    friend_abs_table_name = friend_columns_dict[column_name]
    identifier_set = frozenset(table_id_map[column_name])
    identifier_list = []
    for table_column_name in table_id_map[column_name]:
        table_column_name_with_und = "_".join(table_column_name.split("."))
        if table_column_name_with_und not in json_data['friend_payload'][friend_abs_table_name]:
            print("table column not found!!")
            print("Table name with und", table_column_name_with_und)
            print("F_PAYLOAD:", json_data['friend_payload'][friend_abs_table_name])
            sys.exit(0)
        column_data = json_data['friend_payload'][friend_abs_table_name][table_column_name_with_und]
        identifier_list.append(column_data)

    return identifier_resolve_cache_with_data[frozenset(identifier_list)]


def get_friend_columns(json_data, schema_json):
    column_list = {}
    for abs_table_name in schema_json[json_data["table_name"]]["FRIEND-TABLE"]:
        table_name = abs_table_name.split(".")[1]
        # TODO - could be something other than primary key. add new data to schema json indicating relation
        column_list[table_name + "_" + schema_json[json_data["table_name"]]["PRIMARY-KEY"][0]] = abs_table_name
    return column_list


def get_data_check_select_query(json_data,schema_json,table_id_map,identifier_resolve_cache_with_columns, identifier_resolve_cache_with_data):
    query = "select * from {} where ".format(json_data["table_name"])
    values = []
    query_cut = False
    if json_data["essential_columns"]:
        friend_columns_dict = get_friend_columns(json_data,schema_json)
        for table_column_name in json_data["essential_columns"]:

            data = ""
            table_name, column_name = table_column_name.split(".")
            table_schema_name = ""
            if json_data["Schema"]:
                table_schema_name = json_data["Schema"]
            else:
                table_schema_name = schema_module.get_table_schema_name(table_name)
            abs_table_name = table_schema_name + "." + table_name

            # get identifier if column name is a foreign key id
            # TODO - ensure the right info is being resolved. i.e -> the foreign key may not be a primary key
            for parent_table, parent_column in schema_json[abs_table_name]["FOREIGN-KEY"].items():
                # print("COL_NAME",column_name,"PAR_COL",parent_column)
                if column_name == parent_column:
                    parent_table_name = parent_table.split(".")[1]
                    column_name = parent_table_name + "_" + schema_json[parent_table]["PRIMARY-KEY"][0]

            if column_name in friend_columns_dict:
                data = resolve_friend_table_id(json_data,column_name,friend_columns_dict,table_id_map,identifier_resolve_cache_with_columns, identifier_resolve_cache_with_data)
                values.append(data)
            else:
                print(table_id_map)
                print(identifier_resolve_cache_with_columns)
                print(column_name)
                # TODO - fix logic handling value resolver. avoid reading from payload when id isnt present in table_id_map
                if column_name in table_id_map: # TODO - ensure the right data is in table id map
                    
                    data = resolve_table_id(json_data,column_name,table_id_map,identifier_resolve_cache_with_columns, identifier_resolve_cache_with_data)
                    values.append(data)
                else:
                    data = json_data["payload"][table_column_name]
                    if json_data["payload"][table_column_name] == []: # TODO - check if there are cases where essential columns are None
                        data = None
                    values.append(data)
            query += "{} = %s and ".format(table_column_name)
        
    
    query = query[:-4]
    query += ";"
    return query, values
#--------------------------------------------------------------------------------------------------------------------------

def start():
    parser = argparse.ArgumentParser(description="dynamic_insert.py")
    parser.add_argument('--schemajson',nargs='?',type=argparse.FileType('r'),required=True)
    parser.add_argument('--datafile',nargs='?',type=argparse.FileType('r'),required=True)
    parser.add_argument('--outfile', type=str, default="")
    args = parser.parse_args()
    print("Running data consistency check.")
    compare_data(args)
start()