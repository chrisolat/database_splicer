sample_schema = {
    "table_order": ['A','B','C'],
    "A": {
        "PARENT-TABLE": [],
        "FRIEND-TABLE": [],
        "FOREIGN-KEY": []
    },
    "B": {
        "PARENT-TABLE": ["A"],
        "FRIEND-TABLE": [],
        "FOREIGN-KEY": ["A.id"] # Assumption
    },
    "C": {
        "PARENT-TABLE": [],
        "FRIEND-TABLE": ["A"],
        "FOREIGN-KEY": ["A.id"]
    }
}

tables = ['A','B','C']

# A -- B
# |
#  --- C

# What happens when tables A,B,C is passed in?
# What happend when tables A,C or B,C is passed in?

# -approach
# If current table has any parent tables, recursively traverse upwards until root parent is found,
# then build query from root parent table until current table is returned to
