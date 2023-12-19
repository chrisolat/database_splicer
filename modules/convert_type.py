"""
This module converts NULL values into empty lists
and empty lists into NULL values
"""


"""
convert NULL values to empty list
"""
def convert_None_to_list(data):
    
    data = list(data)
    for i in range(len(data)):
        if data[i] == None: data[i] = []
    
    return data



"""
convert empty list to NULL values
"""
def convert_list_to_None(data):
    
    
    for i in data.keys():
        if data[i] == str([]) and (i != "submissions" or i != "answers" or i != "uploaded_file"):
            # print(i)
            if i == "answers" or i == "submissions" or i == "uploaded_file":
                return
            data[i] = None
    
    return data