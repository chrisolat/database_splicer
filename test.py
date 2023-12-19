from courses2.coursesmodules import get_columns
from courses2.coursesmodules import connect_to_db
from courses2.coursesmodules import schema as schema_module
cur = connect_to_db.connect().cursor()
# cur.execute(
#         """
#         SELECT
#         ccu.table_schema AS foreign_table_schema,
#         ccu.table_name AS foreign_table_name,
#         ccu.column_name AS foreign_column_name,
#         tc.constraint_name AS constraint_name
#         FROM 
#         information_schema.table_constraints AS tc 
#         JOIN information_schema.key_column_usage AS kcu
#         ON tc.constraint_name = kcu.constraint_name
#         AND tc.table_schema = kcu.table_schema
#         JOIN information_schema.constraint_column_usage AS ccu
#         ON ccu.constraint_name = tc.constraint_name
#         AND ccu.table_schema = tc.table_schema
#         WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{}';
#         """.format("submissions")
#     )

# cur.execute("""
# SELECT conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef FROM pg_catalog.pg_constraint r WHERE r.confrelid = 'portal.answers'::regclass
# """)

cur.execute("""
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
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='submissions';
""")

cur.execute("""
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
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='submissions';
""")
data = cur.fetchall()
print(data)
result = {}
# for row in data:
#     result[row[1]] = row[2]
print(result)