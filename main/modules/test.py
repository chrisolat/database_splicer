import psycopg2
import connect_to_db

query = """
SELECT parent.table_name
FROM information_schema.table_constraints AS child
JOIN information_schema.constraint_table_usage AS parent ON child.constraint_name = parent.constraint_name
WHERE child.table_name = 'uploaded_file' AND child.constraint_type = 'FOREIGN KEY';
"""

query = """
SELECT inhparent::regclass::text
FROM   pg_catalog.pg_inherits
WHERE  inhrelid = 'portal.lessons'::regclass;
"""

query = """
SELECT *
FROM   pg_catalog.pg_inherits
"""

# works?
query = """
SELECT relid, relname
FROM pg_catalog.pg_statio_user_tables
WHERE relid = '20291';
"""

query = """
select * from pg_catalog.pg_statio_user_tables
"""

# gets child tables?
query = """
select 
  (select r.relname from pg_class r where r.oid = c.conrelid) as table, 
  (select array_agg(attname) from pg_attribute 
   where attrelid = c.conrelid and ARRAY[attnum] <@ c.conkey) as col, 
  (select r.relname from pg_class r where r.oid = c.confrelid) as ftable 
from pg_constraint c 
where c.confrelid = (   select oid from pg_class   where relname = 'questions'   
AND relnamespace = (select oid from pg_namespace where nspname = 'portal'));
"""


cur = connect_to_db.connect().cursor()

cur.execute(query)
print(cur.fetchall())