"""
This module is used to get the relationship of tables in a schema
This module is not used
"""

import connect_to_db

def get_relationship(schema):
    
    if schema.lower() == 'portal':
        return portal_relationship()


def portal_relationship():
    
    portal = {}
    
    portal['courses'] = ['course_name']
    portal['path'] = ['prefix']
    portal['lessons'] = ['prefix','relative_path']
    portal['activities'] = ['lesson_id','form_code']
    portal['forms'] = ['form_hash','activity_id']
    portal['questions'] = ['form_id','question_code']
    portal['answers'] = ['question_id','submission_id']
    portal['uploaded_file'] = ['answer_id']
    portal['submissions'] = ['student_id','form_id']
    portal['students'] = ['section_code','student_code']
    portal['student_activity'] = ['student_id','activity_id']
    portal['sections'] = ['section_code','course_name']
    portal['section_activity'] = ['section_code','activity_id']
    portal['section_question'] = ['section_code','question_id']
    portal['users'] = ['user_name']
    portal['user_section'] = ['user_name','section_code']
    portal['user_course'] = ['user_name','course_name']
    portal['user_role'] = ['user_name','role_name']
    portal['roles'] = ['role_name']
    
    return portal
    
def create_relationship():
    try:
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure the credentials are correct")
        return
        
    # cur.execute("""
    # SELECT
    # tc.constraint_name, tc.table_name, kcu.column_name, 
    # ccu.table_name AS foreign_table_name,
    # ccu.column_name AS foreign_column_name 
    # FROM 
    # information_schema.table_constraints AS tc 
    # JOIN information_schema.key_column_usage AS kcu
      # ON tc.constraint_name = kcu.constraint_name
    # JOIN information_schema.constraint_column_usage AS ccu
      # ON ccu.constraint_name = tc.constraint_name
    # WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='courses' 
    # and tc.table_schema='portal';
    # """)
    
    # cur.execute("select * from information_schema.referential_constraints")
    
    cur.execute("""
    select * from information_schema.table_constraints
    where table_name = 'activities'
    and table_schema = 'portal'
    and constraint_type in ('FOREIGN KEY', 'UNIQUE')
    """)
    
    
    # cur.execute("select * from pg_catalog.pg_contrainst")
    
    # cur.execute("""
    # SELECT con.*
       # FROM pg_catalog.pg_constraint con
            # INNER JOIN pg_catalog.pg_class rel
                       # ON rel.oid = con.conrelid
            # INNER JOIN pg_catalog.pg_namespace nsp
                       # ON nsp.oid = connamespace
       # WHERE nsp.nspname = '<schema name>'
             # AND rel.relname = '<table name>';
    # """)
    
    
    # cur.execute("select * from pg_constraint")
    print(cur.fetchall())
    
create_relationship()