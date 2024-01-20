# This module is used to generate json file outlining the relationship
# between tables in a schema.

import sys
import json
import connect_to_db
import argparse
import topo_sort
import schema as schema_module # renamed because "schema" may be a variable name used in file
from collections import defaultdict,OrderedDict
cur = connect_to_db.connect().cursor()

# Generates intermediate Schema
def generate_schema(args):
    
    if not (args.infile and args.outfile):
        print("Output or Input file not specified")
        return
    output = {}
    # open schema file with raw schema and tables
    with open(args.infile, 'r') as sch_tables:
        Schema_tables = json.load(sch_tables)
        table_set = set(get_table_list(Schema_tables))
        table_parent = []
        table_natural_key = ""
        external_schema_detect = False
        for schema in Schema_tables.keys():
            for table in Schema_tables[schema]:
                table_parent = get_table_parent(table)
                table_natural_key = get_table_unique_key(table)
                
                # might need fix for external parent. might be better to implement in separate function
                # more context: this check is to handle cases where a table has a parent/friend in a different schema 
                if table_parent:
                    for ptable in table_parent:
                        # if parent is not found in tree
                        if ptable not in table_set:
                            # Get external table data and add it to the output
                            data = {
                                "natural_key": table_natural_key,
                                "parent": table_parent[0],
                                "friend": table_parent[1:]
                            }
                            output[table] = data
                            parent_tables = get_missing_table_tree(ptable) 
                            output = {**output, **parent_tables} 
                            external_schema_detect = True            
                else:
                    table_parent.append("")

                # External Parent table data is already populated. Skip iteration
                if external_schema_detect:
                    external_schema_detect = False
                    continue
                # If more than one parent is found,
                # a field "friend" is used to contain
                # the friend table
                if len(table_parent) > 1:
                    for ptable in table_parent:
                        # if parent not found in tree/schema, get the external table
                        if ptable not in table_set:
                            parent_tables = get_missing_table_tree(ptable) 
                            output = {**output, **parent_tables} 
                    data = {
                        "natural_key": table_natural_key,
                        "parent": table_parent[0],
                        "friend": table_parent[1:]
                    }
                else:
                    data = {
                        "natural_key": table_natural_key,
                        "parent": table_parent[0]
                    }
                output[table] = data
        print("Parent and Friend tables were arbitrarily assigned")
        with open(args.outfile, 'w') as output_file:
            json.dump(output, output_file, indent=2)
            

# function to get missing tables that may be in separate schema
def get_missing_table_tree(table):
    print("Adding table(s) from external schema")
    data = {}
    result = {}
    parent = table
    while(parent[0] != ""):
        parent = get_table_parent(table)
        if "students" in parent:
            print("found")
        if not parent:
            parent = [""]
        natural_key = get_table_unique_key(table)
        if len(parent) > 1:
            data = {
                "natural_key": natural_key,
                "parent": parent[0],
                "friend": parent[1:]
            }
        else:
            data = {
                "natural_key": natural_key,
                "parent": parent[0]
            }

        result[table] = data
        table = parent[0]
    return result
    



# code to generate detailed schema json
def generate_detailed_schema(args):
    if not (args.infile and args.outfile):
        print("Output or Input file not specified")
        return
    output = {}
    with open(args.infile, 'r') as file:
        schema_data = json.load(file)
        for table in schema_data:
            table_parent = [schema_data[table]["parent"]]
            table_friend = []
            if "friend" in schema_data[table]:
                table_friend = schema_data[table]["friend"]
            if table_parent:
                table_parent = table_parent[:1]
            uniqueKey = []
            if schema_data[table]['natural_key']:
                uniqueKey = [schema_data[table]['natural_key']]
            else:
                table_pkey = get_table_primary_key(table)
                if "id" not in table_pkey:
                    uniqueKey = [get_table_primary_key(table)]
            
            # A more detailed information about tables is retrieved and stored.
            data = {
                "PRIMARY-KEY": [get_table_primary_key(table)],
                "FOREIGN-KEY": get_foreign_key(table),
                "UNIQUE-KEY": uniqueKey,
                "NOT-NEEDED": get_not_needed(table),
                "PARENT-TABLE": table_parent,
                "FRIEND-TABLE": table_friend,
                "IDENTIFIER": get_identifiers(table,schema_data,uniqueKey)
            }
            output[table] = data
        Trees = generate_trees(schema_data)
        output["Trees"] = Trees
    with open(args.outfile, 'w') as output_file:
        json.dump(output, output_file, indent=2)


# generate simple data on the relationship between tables in a schema
def generate_schema_tables(args):
    if not (args.outfile):
        print("Output file not specified")
        return
    schema_tables = defaultdict(list)

    # if schema name is not specified, extract all schemas
    if not args.schemas:
        cur.execute(
        """
        SELECT schema_name
        FROM information_schema.schemata;
        """
        )
        result = cur.fetchall()
        for row in result:
            args.schemas.append(row[0])
    
    # retrieve names of tables in schema and write to output
    for schema in set(args.schemas):
        cur.execute(
            """
            SELECT *
            FROM information_schema.tables
            where table_schema = '{}'
            """.format(schema)
        )
        
        result = cur.fetchall()
        if not result:
            print("No tables found for:", schema)
            continue
        for row in result:
            schema_tables[schema].append(row[1]+"."+row[2])

    with open(args.outfile, 'w') as output_file:
        json.dump(schema_tables, output_file, indent=2)

    

# gets the primary key column name of passed in table
def get_table_primary_key(table_name):
    table_schema,table_name = table_name.split(".")
    cur.execute(
        """
        SELECT c.column_name, c.data_type
        FROM information_schema.table_constraints tc 
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
        AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{}'
        and tc.table_schema = '{}';
        """.format(table_name,table_schema)
    )
    result = cur.fetchall()
    if result:
        # if result[0][0] == "id":
        #     return ""
        return result[0][0]
    else:
        return ""

# gets parent name of passed in table
def get_table_parent(table_name):
    schema_name, table_name = table_name.split(".")
    cur.execute(
        """
        SELECT
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
        FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{}'
        AND tc.table_schema = '{}';
        """.format(table_name,schema_name)
    )
    result = []
    data = cur.fetchall()
    for row in data:
        abs_name = row[0]+"."+row[1]
        if len(result) > 0 and len(row[1]) < len(result[0]) and result[0].startswith(row[1]):
            while(len(result) > 0 and result[0].startswith(row[1])):
                result.pop()
            result.append(abs_name)
        else:
            if abs_name not in result:
                result.append(abs_name)
    return result


# gets unique key column from passed in table
def get_table_unique_key(table_name):
    table_schema, table_name = table_name.split(".")
    cur.execute(
        """
        SELECT
        tc.table_schema, 
        tc.constraint_name, 
        tc.table_name, 
        kcu.column_name, 
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
        FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name='{}'
        and tc.table_schema = '{}';
        """.format(table_name,table_schema)
    )
    data = cur.fetchall()
    result = ""
    if data:
        result = data[-1][-1]
    return result


# gets names of columns that should be avoided. returns columns that are called "id" or foreign key columns that have "id" in them 
def get_not_needed(table_name):
    not_needed = []
    table_id = get_table_primary_key(table_name)
    table_parent = get_table_parent(table_name)
    if table_id == "id":
        not_needed.append(table_id)
    if "id" in table_id:
        not_needed.append(table_name.split(".")[1] + "_" + table_id)
    
    for parent in table_parent:
        parent_id = get_table_primary_key(parent)
        if "id" in parent_id and parent_id not in not_needed:
            not_needed.append(parent_id)
            not_needed.append(table_name.split(".")[1] + "_" + parent_id)
    
    return not_needed


# gets lists of tables in schemas
def get_table_list(schmea_json):
    table_list = []
    for schema in schmea_json:
        table_list.extend(schmea_json[schema])
    return table_list


# gets foreign key column names in passed in table name
def get_foreign_key(table_name):
    schema_name, table_name = table_name.split(".")
    cur.execute(
        """
        SELECT
        tc.table_schema, 
        tc.constraint_name, 
        tc.table_name, 
        kcu.column_name, 
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{}'
    AND tc.table_schema = '{}';
        """.format(table_name,schema_name)
    )
    data = cur.fetchall()
    result = {}
    for row in data:
        result[row[0] + "." + row[5]] = row[3]
    return result


# returns list of friend tables
def get_friend_table(table_name):
    table_parent = get_table_parent(table_name)
    if len(table_parent) > 1:
        return table_parent[1:]
    else:
        return []


# tries to get a tables natural keys
def get_identifiers(table_name, schema_data, uniqueKey):
    
    identifiers = []
    for uniquekey in uniqueKey:
        if uniquekey:
            identifiers.append(table_name.split(".")[1] + "." + uniquekey)
    cycle_check = set()
    extended_schema_data = {}
    
    cycle_check.add(table_name)
    table_parent = schema_data[table_name]['parent']
    if table_parent:
        # traverse up tree and get parent table identifiers
        while (table_parent != ""):
            table_natural_key = schema_data[table_parent]['natural_key']
            # if table has no natural keys, its primary/unique key is used if its not an id
            if table_natural_key == "":
                table_natural_key = get_table_primary_key(table_name)
                if  table_natural_key == "id":
                    table_natural_key = ""
                else:
                    if table_parent:
                        ident_name = table_parent.split(".")[1] + "." + table_natural_key
                        if table_natural_key:
                            identifiers.insert(0,table_parent.split(".")[1] + "." + table_natural_key)
            else:
                ident_name = table_parent.split(".")[1] + "." + table_natural_key
                if table_natural_key:
                    identifiers.insert(0,table_parent.split(".")[1] + "." + table_natural_key)
            
            table_name = table_parent
            if table_name in cycle_check:
                
                print("cycle found:", table_name)
                return identifiers
            cycle_check.add(table_name)
            # some tables may not be found in schema
            if table_parent not in set(schema_data.keys()):
                if table_parent not in set(extended_schema_data):
                    extended_schema_data = extend_schema(table_parent)
                    table_parent = extended_schema_data[table_parent]['parent']
                else:
                    table_parent = extended_schema_data[table_parent]['parent']
            else:
                table_parent = schema_data[table_parent]['parent']
          
    return identifiers


# get tree of passed in table
def get_tree(table, schema_data):
    table_parent = schema_data[table]["parent"]
    table_list = [table]
    cycle_check = set()
    extended_schema_data = {}
    while(table_parent != ""):
        table = table_parent
        table_list.insert(0,table)
        cycle_check.add(table)
        if table_parent not in set(schema_data.keys()):
            if table_parent not in set(extended_schema_data.keys()):
                extended_schema_data = extend_schema(table_parent)
                table_parent = extended_schema_data[table_parent]['parent']
            else:
                table_parent = extended_schema_data[table_parent]['parent']
        else:
            table_parent = schema_data[table]["parent"]
        if table_parent in cycle_check:
            print("cycle found:", table_parent)
            break  
    return table_list


# returns related trees of a passed in table
def get_tree_relationship(table, friend, Trees):
    result = []
    for tree in Trees:
        if table in Trees[tree]:
            result.append(tree)
    
    for tree in Trees:
        if friend in Trees[tree]:
            result.append(tree)
    
    return result

# generate detailed json of trees order
def analyze_trees(args):
    output = OrderedDict()
    with open(args.infile, 'r') as file:
        schema_data = json.load(file)
        Trees = generate_trees(schema_data)
        sorted_trees = topo_sort.topological_sort(Trees["adjacency_list"])
        output["Sorted Trees"] = sorted_trees
        for tree in sorted_trees:
            output[tree] = Trees[tree]
    json.dump(output, args.outfile, indent=2)


# generate missing ancestor tree schema data
def extend_schema(table):
    print("Generating new schema data for:",table)
    print("Parent and Friend tables were arbitrarily assigned")
    ancestor_schema_name = schema_module.get_table_schema_name(table)
    ancestor_schema_data = schema_module.get_schema_data(ancestor_schema_name)
    print("Adding table from new schema: ", ancestor_schema_name)
    table_parent = []
    new_schema_data = {}
    table_natural_key = ""
    for schema in ancestor_schema_data.keys():
        for table in ancestor_schema_data[schema]:
            table_parent = get_table_parent(table)
            table_natural_key = get_table_unique_key(table)
            if not table_parent:
                table_parent.append("")

            # If more than one parent is found,
            # a field "friend" is used to contain
            # the other parent tables
            if len(table_parent) > 1:
                data = {
                    "natural_key": table_natural_key,
                    "parent": table_parent[0],
                    "friend": table_parent[1:]
                }
            else:
                data = {
                    "natural_key": table_natural_key,
                    "parent": table_parent[0]
                }
            new_schema_data[table] = data
    
    return new_schema_data

# This function runs all procedures; generating all schema files
def generate_all_schemas(args):

    outfile_temp = args.outfile
    args.outfile = "schema_tables.json"
    generate_schema_tables(args)

    args.infile = "schema_tables.json"
    args.outfile = "intermediate_schema.json"
    generate_schema(args)
    #fix_intermediate(args)

    args.infile = "intermediate_schema.json"
    args.outfile = "detailed.json"
    if outfile_temp:
        args.outfile = outfile_temp
    generate_detailed_schema(args)
    #fix_detailed(args)

    print("Detailed schema file name: ", args.outfile)

# generate tree relationship using intermediate schema file
def generate_trees(schema_data):
    schema_tables = {}
    seen_tables = []
    tree_no = 1
    Trees = {}
    for table in schema_data:
        schema_name = schema_module.get_table_schema_name(table)
        if schema_name not in schema_tables:
            schema_tables[schema_name] = [table]
        else:
            schema_tables[schema_name].append(table)
    
    for schema in schema_tables.keys():
        index = len(schema_tables[schema]) - 1
        while(index > -1):
            leaf_table = schema_tables[schema][index]
            table_list = get_tree(leaf_table, schema_data)
            if leaf_table not in seen_tables:
                Trees["tree" + str(tree_no)] = table_list
                seen_tables.extend(table_list)
                tree_no += 1
            index -= 1

    graph_adj = {}
    for tree in Trees:
        graph_adj[tree] = []
    
    for table in schema_data:
        if len(schema_data[table]) > 2:
            friend_tables = schema_data[table]["friend"]
            for friend_table in friend_tables:
                try:
                    child_tree, ancestor_tree = get_tree_relationship(table, friend_table, Trees)
                except:
                    continue
                if child_tree not in graph_adj[ancestor_tree]:
                    graph_adj[ancestor_tree].append(child_tree)

    Trees["adjacency_list"] = graph_adj
    return Trees


def start():
    parser = argparse.ArgumentParser(description="generate_schema.py")
    # parser.add_argument('--outfile',nargs='?',type=argparse.FileType('w'))
    # parser.add_argument('--infile', nargs='?',type=argparse.FileType('r'))
    parser.add_argument('--outfile', type=str, default="")
    parser.add_argument('--infile', type=str, default="")
    parser.add_argument('--schema_table', nargs='?', type=argparse.FileType('r'))
    parser.add_argument('--generate_schema', default=False, action="store_true", help="generate schema json")
    parser.add_argument('--generate_detailed_schema', default=False, action="store_true", help="generate detailed schema json")
    parser.add_argument('--generate_schema_tables', default=False, action="store_true", help="generate schema tables")
    parser.add_argument('--analyze_trees', default=False, action='store_true', help='generate sorted tree json')
    parser.add_argument('--schemas', nargs='*', default=[], help="specify schema to be included in schema tables")
    parser.add_argument('--generate_all', default=False, action="store_true", help="generate all schema files")
    args = parser.parse_args()
    if args.generate_all:
        generate_all_schemas(args)
    elif args.generate_detailed_schema:
        generate_detailed_schema(args)
    elif args.generate_schema:
        generate_schema(args)
    elif args.generate_schema_tables:
        generate_schema_tables(args)
    elif args.analyze_trees:
        analyze_trees(args)
    else:
        print("To use this script, simply pass the arg --generate_all and specify an output file name. You can also specify what schemas you want to extract with --schemas\n\
              e.g python3 generate_schema.py --generate_all --outfile <output file>\n\
              OR python3 generate_schema.py --generate_all --outfile <output file> --schemas <space separated schema names>")
        


start()

