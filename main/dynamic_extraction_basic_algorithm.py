"""Algorithm used in dynamic_extract.py to traverse and extract data dynamically"""

# Assume "AAAB" is a row and 'A' is a table
# The same idea goes for all strings in the list
Table = ["AAAB","ABAC","ACCA","BAAD","BABC","BBAC","BCBD","BCCD"]

current = 0 # variable to traverse column
Output = {} # variable to store output

# Traverse column until we find a mismatching primary key for previous and current row
def Traverse(Output, current, row):
    print(Output)
    if (Table[row][current] == Table[row-1][current]):
        Traverse(Output[Table[row][current]],current+1,row)
    elif(current == len(Table[row])-1):
        Output[Table[row][current]] = {}
    else:
        Output[Table[row][current]] = singleton(rest(Table[row], current))


# Function to convert row in heirarchical json
def singleton(Table_row):
    
    if len(Table_row) > 0:
        # If there is more than one table in the list,
        # recall singleton and pass in the rest of the tables
        # i.e [courses, paths, lessons] -> [paths, lessons]
        if len(Table_row) > 1:
            rest_data = singleton(rest(Table_row, 0))
            
            # For [courses, paths, lessons],
            # {courses: {paths: {lessons: {}}}} is returned
            return {Table_row[0]:rest_data}
        
        # When there is only one table left,
        # return table name and empty dictionary
        # i.e. -> {lessons:{}}
        else:
            return {Table_row[0]:{}}

# Function to return rest of tables in passed in list
def rest(Table_row, current):
    return Table_row[current+1:]

# initialize output so it can be built upon
Output[Table[0][0]] = singleton(rest(Table[0],current))

for row in range(1,len(Table)):
    Traverse(Output,current,row)

print(Output)