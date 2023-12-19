"""
This module is used to insert data into database
"""



"""
insert single row into table
"""
import connect_to_db

con = connect_to_db.connect()
def insert_row(cur,query,querylist):
    
    cur.execute(query, querylist)
    con.commit()
    # print("Done.")





"""
insert multiple rows into table
"""
def insert_many(cur,table,id_column,values,submission_values):
    if not values:
        return []
        
    keys = submission_values
    query = cur.mogrify("INSERT INTO {} ({}) VALUES {} RETURNING {}".format(
    table,
    keys,
    ', '.join(['%s'] * len(values)),
    id_column
    ), [tuple(v) for v in values])
    # print(query)
    cur.execute(query)
    return [t[0] for t in (cur.fetchall())]