def separated_table_names(view_column_list):
    for cols in view_column_list:
        
        table_name = cols.split("_")
        if len(table_name) > 2:
            table_name = [table_name[0], "_".join(table_name[1:])]
    
    return table_name
    

def get_pkeys(view_column_list,json_data):
    
    view_pkeys = []
    
    for cols in view_column_list:
        
        table_name = cols.split("_")
        if len(table_name) > 2:
            table_name = [table_name[0], "_".join(table_name[1:])]
        # print(cols.split("_")[1])
        if table_name[0] in json_data:
            
            if json_data[table_name[0]]["PRIMARY-KEY"][0] == table_name[1]:
                # print(table_name[0])
                if cols not in view_pkeys:
                    view_pkeys.append((table_name[0],cols))
    
    # print(view_column_list)
    # print(dict(view_pkeys))
    return dict(view_pkeys)
    
def get_pkey_index(view_column_list, json_data):
    
    view_pkeys = get_pkeys(view_column_list,json_data)
    pkey_list = []
    for pkey in view_pkeys.keys():
        if view_pkeys[pkey] not in pkey_list:
            pkey_list.append(view_pkeys[pkey])
    
    pkey_index = {}
    
    for index in range(len(view_column_list)):
        if view_column_list[index] in pkey_list:
            pkey_index[view_column_list[index]] = index
    
    return pkey_index