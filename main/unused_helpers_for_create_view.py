
import sys
sys.path.append("../modules")
import psycopg2
import os
import sys
import json
import get_columns
import argparse
import connect_to_db


 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
    
    











# outdated but useful?------------------------------------------------------------------------------------------
# some methods are used in create_jsons.py. 
# create_view.py and read_view.py combined is basically a better version of create_jsons.py
# create_jsons.py however, is faster at extracting data, but can only be used to extract data from answers and submissions





try:

    con = connect_to_db.connect()
    cur = con.cursor()
except:
    print("could not connect to database. please ensure credentials are correct")
    


def create_view_old(view_name,sql_footer,table_schema,table_name_list,write_to_txt):

    cur = connect_to_db.connect()
    
    database_relation = create_relationship.get_relationship("portal")
    
    
    table_name_list = table_name_list.split(" ")
    
    if type(table_name_list) != list:
        table_name_list = [table_name_list]
    
    table_column_list = []
    
    
    for table_name in table_name_list:
        
        
        column_list = get_columns.get_columns_list(table_schema,table_name)
        
        for column in column_list:
            table_column_list.append(table_name+"."+column)
    
    
    if write_to_txt:
        
        txt_name = view_name + "_view.txt"
        with open(txt_name,'w') as file:
            for table in table_column_list:
                file.write(table+"\n")
                
    
    # sql_header = "CREATE OR REPLACE VIEW {}.{} AS SELECT ".format(table_schema,view_name)
    sql_header = "SELECT "
    table_column_list = rename_columns(table_column_list)
    
    sql_query = sql_header
    
    
    
    for column in table_column_list:
        sql_query += column + ", "
    
    sql_query = sql_query[:-2] + " "
    
    sql_query += sql_footer
    
    
    
    cur.execute(sql_query)
    con.commit()





def create_view_for_section(section_code):
    cur = con.cursor()
    
    
    
    
    if section_code:
        create_section_view = """
        CREATE OR REPLACE VIEW portal.extracted_submissions_modified
        AS SELECT answers.id AS answers_id,
        answers.answer,
        answers.score,
        answers.comment,
        answers.type_text,
        answers.question_id,
        answers.submission_id,
        answers.grader,
        answers.graded_at,
        answers.created_at as answers_created_at,
        submissions.id AS submissions_id ,
        submissions.student_id,
        submissions.ip_address,
        submissions.status,
        submissions.answered_at,
        submissions.form_id,
        questions.id as questions_id,
        questions.question_code,
        questions.question_text,
        forms.id as forms_id,
        forms.form_hash,
        students.id as students_id,
        students.section_code as students_section_code,
        students.student_code,
        activities.id as activities_id,
        activities.form_code,
        sections.section_code as section_code,
        lessons.id as lessons_id,
        lessons.relative_path,
        paths.prefix as paths_prefix,
        courses.course_name as course_name,
        teachers_of_record.user_name,
        students.active,
        students.created_at as students_created_at,
        students.first_name,
        students.last_name,
        students.grade_level,
        students.gender,
        students.ethnicity,
        students.hispanic_latino,
        students.evaluation,
        students.native_language,
        students.certification_active,
        uploaded_file.id as uploaded_file_id,
        uploaded_file.name as uploaded_file_name,
        uploaded_file.location as uploaded_file_location,
        uploaded_file.size as uploaded_file_size,
        uploaded_file.type as uploaded_file_type,
        uploaded_file.answer_id as uploaded_file_answer_id,
        uploaded_file.created_at as uploaded_file_created_at
       FROM portal.submissions
       left join portal.answers on submissions.id = answers.submission_id
       left join portal.questions on answers.question_id = questions.id 
       left join portal.students on submissions.student_id = students.id
       left join portal.forms on submissions.form_id = forms.id
       left join portal.sections on students.section_code = sections.section_code
       left join portal.activities on forms.activity_id = activities.id
       left join portal.lessons on activities.lesson_id = lessons.id
       left join portal.paths on lessons.prefix = paths.prefix
       left join portal.courses on paths.course_name = courses.course_name 
       left join portal.teachers_of_record on sections.section_code = teachers_of_record.section_code
       left join portal.uploaded_file on answers.id = uploaded_file.answer_id
       where sections.section_code = '{}'
       order by user_name,section_code,students.student_code,submission_id;""".format(section_code)
    
    
    
    
    else:
        create_section_view = """
        CREATE OR REPLACE VIEW portal.extracted_submissions_modified
        AS SELECT answers.id AS answers_id,
        answers.answer,
        answers.score,
        answers.comment,
        answers.type_text,
        answers.question_id,
        answers.submission_id,
        answers.grader,
        answers.graded_at,
        answers.created_at as answers_created_at,
        submissions.id AS submissions_id ,
        submissions.student_id,
        submissions.ip_address,
        submissions.status,
        submissions.answered_at,
        submissions.form_id,
        questions.id as questions_id,
        questions.question_code,
        questions.question_text,
        forms.id as forms_id,
        forms.form_hash,
        students.id as students_id,
        students.section_code as students_section_code,
        students.student_code,
        activities.id as activities_id,
        activities.form_code,
        sections.section_code as section_code,
        lessons.id as lessons_id,
        lessons.relative_path,
        paths.prefix as paths_prefix,
        courses.course_name as course_name,
        teachers_of_record.user_name,
        students.active,
        students.created_at as students_created_at,
        students.first_name,
        students.last_name,
        students.grade_level,
        students.gender,
        students.ethnicity,
        students.hispanic_latino,
        students.evaluation,
        students.native_language,
        students.certification_active,
        uploaded_file.id as uploaded_file_id,
        uploaded_file.name as uploaded_file_name,
        uploaded_file.location as uploaded_file_location,
        uploaded_file.size as uploaded_file_size,
        uploaded_file.type as uploaded_file_type,
        uploaded_file.answer_id as uploaded_file_answer_id,
        uploaded_file.created_at as uploaded_file_created_at
       FROM portal.submissions
       left join portal.answers on submissions.id = answers.submission_id
       left join portal.questions on answers.question_id = questions.id 
       left join portal.students on submissions.student_id = students.id
       left join portal.forms on submissions.form_id = forms.id
       left join portal.sections on students.section_code = sections.section_code
       left join portal.activities on forms.activity_id = activities.id
       left join portal.lessons on activities.lesson_id = lessons.id
       left join portal.paths on lessons.prefix = paths.prefix
       left join portal.courses on paths.course_name = courses.course_name
       left join portal.teachers_of_record on sections.section_code = teachers_of_record.section_code
       left join portal.uploaded_file on answers.id = uploaded_file.answer_id
       order by user_name,sections.section_code,students.student_code,submission_id,answers_id;"""
    
    
    cur.execute(create_section_view)
    
    con.commit()
    print("reading view")
    
    
def create_view_for_teacher(section_code_list):
    
    cur = con.cursor()
    
    
    
    section_code_tup = tuple(section_code_list)
    
    
    create_section_view = """
        CREATE OR REPLACE VIEW portal.extracted_submissions_modified
        AS SELECT answers.id AS answers_id,
        answers.answer,
        answers.score,
        answers.comment,
        answers.type_text,
        answers.question_id,
        answers.submission_id,
        answers.grader,
        answers.graded_at,
        answers.created_at as answers_created_at,
        submissions.id AS submissions_id ,
        submissions.student_id,
        submissions.ip_address,
        submissions.status,
        submissions.answered_at,
        submissions.form_id,
        questions.id as questions_id,
        questions.question_code,
        questions.question_text,
        forms.id as forms_id,
        forms.form_hash,
        students.id as students_id,
        students.section_code as students_section_code,
        students.student_code,
        activities.id as activities_id,
        activities.form_code,
        sections.section_code as section_code,
        lessons.id as lessons_id,
        lessons.relative_path,
        paths.prefix as paths_prefix,
        courses.course_name as course_name,
        teachers_of_record.user_name,
        students.active,
        students.created_at as students_created_at,
        students.first_name,
        students.last_name,
        students.grade_level,
        students.gender,
        students.ethnicity,
        students.hispanic_latino,
        students.evaluation,
        students.native_language,
        students.certification_active,
        uploaded_file.id as uploaded_file_id,
        uploaded_file.name as uploaded_file_name,
        uploaded_file.location as uploaded_file_location,
        uploaded_file.size as uploaded_file_size,
        uploaded_file.type as uploaded_file_type,
        uploaded_file.answer_id as uploaded_file_answer_id,
        uploaded_file.created_at as uploaded_file_created_at
       FROM portal.submissions
       left join portal.answers on submissions.id = answers.submission_id
       left join portal.questions on answers.question_id = questions.id 
       left join portal.students on submissions.student_id = students.id
       left join portal.forms on submissions.form_id = forms.id
       left join portal.sections on students.section_code = sections.section_code
       left join portal.activities on forms.activity_id = activities.id
       left join portal.lessons on activities.lesson_id = lessons.id
       left join portal.paths on lessons.prefix = paths.prefix
       left join portal.courses on paths.course_name = courses.course_name 
       left join portal.teachers_of_record on sections.section_code = teachers_of_record.section_code
       left join portal.uploaded_file on answers.id = uploaded_file.answer_id
       where sections.section_code in {}
       order by user_name,sections.section_code,students.student_code,submission_id;""".format(section_code_tup)
    
    
    
    
    cur.execute(create_section_view)
    
    con.commit()
    
    print("reading view")
    
    

def create_view_for_section2(section_code,student_code):
    cur = con.cursor()
    
    
    
    
    
    if section_code:
        create_section_view = """
        CREATE OR REPLACE VIEW portal.extracted_submissions_modified
        AS SELECT answers.id AS answers_id,
        answers.answer,
        answers.score,
        answers.comment,
        answers.type_text,
        answers.question_id,
        answers.submission_id,
        answers.grader,
        answers.graded_at,
        answers.created_at as answers_created_at,
        submissions.id AS submissions_id ,
        submissions.student_id,
        submissions.ip_address,
        submissions.status,
        submissions.answered_at,
        submissions.form_id,
        questions.id as questions_id,
        questions.question_code,
        questions.question_text,
        forms.id as forms_id,
        forms.form_hash,
        students.id as students_id,
        students.section_code as students_section_code,
        students.student_code,
        activities.id as activities_id,
        activities.form_code,
        sections.section_code as section_code,
        lessons.id as lessons_id,
        lessons.relative_path,
        paths.prefix as paths_prefix,
        courses.course_name as course_name,
        teachers_of_record.user_name,
        students.active,
        students.created_at as students_created_at,
        students.first_name,
        students.last_name,
        students.grade_level,
        students.gender,
        students.ethnicity,
        students.hispanic_latino,
        students.evaluation,
        students.native_language,
        students.certification_active,
        uploaded_file.id as uploaded_file_id,
        uploaded_file.name as uploaded_file_name,
        uploaded_file.location as uploaded_file_location,
        uploaded_file.size as uploaded_file_size,
        uploaded_file.type as uploaded_file_type,
        uploaded_file.answer_id as uploaded_file_answer_id,
        uploaded_file.created_at as uploaded_file_created_at
       FROM portal.submissions
       left join portal.answers on submissions.id = answers.submission_id
       left join portal.questions on answers.question_id = questions.id 
       left join portal.students on submissions.student_id = students.id
       left join portal.forms on submissions.form_id = forms.id
       left join portal.sections on students.section_code = sections.section_code
       left join portal.activities on forms.activity_id = activities.id
       left join portal.lessons on activities.lesson_id = lessons.id
       left join portal.paths on lessons.prefix = paths.prefix
       left join portal.courses on paths.course_name = courses.course_name 
       left join portal.teachers_of_record on sections.section_code = teachers_of_record.section_code
       left join portal.uploaded_file on answers.id = uploaded_file.answer_id
       where sections.section_code = '{}' and student_code = '{}'
       order by user_name,sections.section_code,students.student_code,submission_id;""".format(section_code,student_code)
    
    
    
    
    else:
        create_section_view = """
        CREATE OR REPLACE VIEW portal.extracted_submissions_modified
        AS SELECT answers.id AS answers_id,
        answers.answer,
        answers.score,
        answers.comment,
        answers.type_text,
        answers.question_id,
        answers.submission_id,
        answers.grader,
        answers.graded_at,
        answers.created_at as answers_created_at,
        submissions.id AS submissions_id ,
        submissions.student_id,
        submissions.ip_address,
        submissions.status,
        submissions.answered_at,
        submissions.form_id,
        questions.id as questions_id,
        questions.question_code,
        questions.question_text,
        forms.id as forms_id,
        forms.form_hash,
        students.id as students_id,
        students.section_code as students_section_code,
        students.student_code,
        activities.id as activities_id,
        activities.form_code,
        sections.section_code as section_code,
        lessons.id as lessons_id,
        lessons.relative_path,
        paths.prefix as paths_prefix,
        courses.course_name as course_name,
        teachers_of_record.user_name,
        students.active,
        students.created_at as students_created_at,
        students.first_name,
        students.last_name,
        students.grade_level,
        students.gender,
        students.ethnicity,
        students.hispanic_latino,
        students.evaluation,
        students.native_language,
        students.certification_active,
        uploaded_file.id as uploaded_file_id,
        uploaded_file.name as uploaded_file_name,
        uploaded_file.location as uploaded_file_location,
        uploaded_file.size as uploaded_file_size,
        uploaded_file.type as uploaded_file_type,
        uploaded_file.answer_id as uploaded_file_answer_id,
        uploaded_file.created_at as uploaded_file_created_at
       FROM portal.submissions
       left join portal.answers on submissions.id = answers.submission_id
       left join portal.questions on answers.question_id = questions.id 
       left join portal.students on submissions.student_id = students.id
       left join portal.forms on submissions.form_id = forms.id
       left join portal.sections on students.section_code = sections.section_code
       left join portal.activities on forms.activity_id = activities.id
       left join portal.lessons on activities.lesson_id = lessons.id
       left join portal.paths on lessons.prefix = paths.prefix
       left join portal.courses on paths.course_name = courses.course_name 
       left join portal.teachers_of_record on sections.section_code = teachers_of_record.section_code
       left join portal.uploaded_file on answers.id = uploaded_file.answer_id
       order by user_name,sections.section_code,students.student_code,submission_id;"""
    
    
    cur.execute(create_section_view)
    
    con.commit()
    print("reading view")
    
    
def create_view_for_teacher2(section_code_list,section_code):
    
    cur = con.cursor()
    
    
    
    section_code_tup = tuple(section_code_list)
    
    
    create_section_view = """
        CREATE OR REPLACE VIEW portal.extracted_submissions_modified
        AS SELECT answers.id AS answers_id,
        answers.answer,
        answers.score,
        answers.comment,
        answers.type_text,
        answers.question_id,
        answers.submission_id,
        answers.grader,
        answers.graded_at,
        answers.created_at as answers_created_at,
        submissions.id AS submissions_id ,
        submissions.student_id,
        submissions.ip_address,
        submissions.status,
        submissions.answered_at,
        submissions.form_id,
        questions.id as questions_id,
        questions.question_code,
        questions.question_text,
        forms.id as forms_id,
        forms.form_hash,
        students.id as students_id,
        students.section_code as students_section_code,
        students.student_code,
        activities.id as activities_id,
        activities.form_code,
        sections.section_code as section_code,
        lessons.id as lessons_id,
        lessons.relative_path,
        paths.prefix as paths_prefix,
        courses.course_name as course_name,
        teachers_of_record.user_name,
        students.active,
        students.created_at as students_created_at,
        students.first_name,
        students.last_name,
        students.grade_level,
        students.gender,
        students.ethnicity,
        students.hispanic_latino,
        students.evaluation,
        students.native_language,
        students.certification_active,
        uploaded_file.id as uploaded_file_id,
        uploaded_file.name as uploaded_file_name,
        uploaded_file.location as uploaded_file_location,
        uploaded_file.size as uploaded_file_size,
        uploaded_file.type as uploaded_file_type,
        uploaded_file.answer_id as uploaded_file_answer_id,
        uploaded_file.created_at as uploaded_file_created_at
       FROM portal.submissions
       left join portal.answers on submissions.id = answers.submission_id
       left join portal.questions on answers.question_id = questions.id 
       left join portal.students on submissions.student_id = students.id
       left join portal.forms on submissions.form_id = forms.id
       left join portal.sections on students.section_code = sections.section_code
       left join portal.activities on forms.activity_id = activities.id
       left join portal.lessons on activities.lesson_id = lessons.id
       left join portal.paths on lessons.prefix = paths.prefix
       left join portal.courses on paths.course_name = courses.course_name 
       left join portal.teachers_of_record on sections.section_code = teachers_of_record.section_code
       left join portal.uploaded_file on answers.id = uploaded_file.answer_id
       where sections.section_code = '{}'
       order by user_name,sections.section_code,students.student_code,submission_id;""".format(section_code)
    
    
    
    
    cur.execute(create_section_view)
    
    con.commit()
    
    print("reading view")
    


# def start():
    # parser = argparse.ArgumentParser(description="create_view.py")
    # # parser.add_argument('--view_name',type=str,default="unspecified",help="specify view name")
    # # parser.add_argument('--sql_footer',type=str,default="",help="specify sql footer in string")
    # # parser.add_argument('--schema',type=str,default="",help="specify table schema")
    # # parser.add_argument('--tables',type=str,default="",help="specify tables in string separated by space")
    # # parser.add_argument('-write_to_txt',action="store_true",default=False,help="iinclude flag without variables to specify if columns should be written to txt file")
    # # args = parser.parse_args()
    
    # # create_view(args.view_name,args.sql_footer,args.schema,args.table,args.write_to_txt)
    
    # parser.add_argument('--generate_view_sql',action="store_true",default=False,help="generate sql to create view")
    # args = parser.parse_args()
    
    # if args.generate_view_sql:
        # sql_footer = """
        # FROM portal.submissions
        # left join portal.answers on submissions.id = answers.submission_id
        # left join portal.questions on answers.question_id = questions.id 
        # left join portal.students on submissions.student_id = students.id
        # left join portal.forms on submissions.form_id = forms.id
        # left join portal.sections on students.section_code = sections.section_code
        # left join portal.activities on forms.activity_id = activities.id
        # left join portal.lessons on activities.lesson_id = lessons.id
        # left join portal.paths on lessons.prefix = paths.prefix
        # left join portal.courses on paths.course_name = courses.course_name 
        # left join portal.teachers_of_record on sections.section_code = teachers_of_record.section_code
        # left join portal.uploaded_file on answers.id = uploaded_file.answer_id
        # order by user_name,sections.section_code,students.student_code,submission_id;
        # """
        
        # create_view(sql_footer)
    # else:
        # print("pass -h flag to view help")
    
    


