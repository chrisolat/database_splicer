
import sys
sys.path.append("../modules")

import psycopg2
import random
import datetime
import string
import time
import datetime
import json
import os
import argparse
import unused_helpers_for_create_view as create_view
import convert_type
import schema
import insert_data
import connect_to_db





def extract_answers_view(type,code,type2,code2,filename,intermediate):
    
    
    try:
    
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure credentials are correct")
        return
    
    
    
    
    
    section_code_list = []
    
    if type == "":
        print("creating view")
        create_view.create_view_for_section("")
    if type2 != "":
        if type == "section":
            print("creating view")
            create_view.create_view_for_section2(code,code2)
        elif type == "teacher":
            print("creating view")
            
            section_code_list = []
            cur.execute(
            """
            select * 
            from portal.teachers_of_record
            where user_name = (%s)
            """,
            (code,))
            
            res = cur.fetchall()
            
            for rows in res:
                
                section_code_list.append(rows[1])
            
            if len(section_code_list) > 1:
                create_view.create_view_for_teacher2(section_code_list,code2)
            else:
                section_code = section_code_list[0]
                create_view.create_view_for_section(code)
    else:
        if type == "section":
            print("creating view")
            create_view.create_view_for_section(code)
        elif type == "teacher":
            print("creating view")
            
            section_code_list = []
            cur.execute("""
            select * 
            from portal.teachers_of_record 
            where user_name = (%s)
            """, 
            (code,))
            res = cur.fetchall()
            
            for rows in res:
                
                section_code_list.append(rows[1])
                
            
            if len(section_code_list) > 0:
                if len(section_code_list) > 1:
                    create_view.create_view_for_teacher(section_code_list)
                else:
                    section_code = section_code_list[0]
                    create_view.create_view_for_section(section_code)
            else:
                print("teacher not found in database")
                return
    
    
    
    
    
    


    cur.execute("""
    select count(*) 
    from portal.extracted_submissions_modified
    """)
    view_lenght = cur.fetchall()[0][0]
    
    curw = con.cursor()
    curw.execute("""
    select * from 
    portal.extracted_submissions_modified
    """)
    
    
    
    data = {}
    ids = {}
    count = 0
    
    data['sections'] = {}
    ids['sections'] = {}
    
    database_schema = schema.get_schema(cur,"portal",[])
    
    if intermediate:
        print("creating intermediate file")
        data_list = []
        
        for row in curw:
        
            data_list.append({
                'submission_id': str(row[10]),
                'course_name': str(row[30]),
                'user_name': str(row[31]),
                'prefix': str(row[29]),
                'relative_path': str(row[28]),
                'section_code': str(row[22]),
                'student_code': str(row[23]),
                'form_code': str(row[25]),
                'form_hash': str(row[20]),
                'ip_address': str(row[12]),
                'status': str(row[13]),
                'answered_at': str(row[14]),
                'question_code': str(row[17]),
                'question_text': str(row[18]),
                'answer': str(row[1]),
                'score': str(row[2]),
                'comment': str(row[3]),
                'type_text': str(row[4]),
                'grader': str(row[7]),
                'graded_at': str(row[8]),
                'created_at': str(row[9]),
                
            })
        
        filename = filename[:-5] + "_raw.json"
        with open(filename, 'w') as file:
        
            json.dump(data_list, file)
        
        return
    
    
    
    
    if curw == []:
        print("section has no answers")
        return
    
    counter = 0
    
    
    print("reordering and writing to output json")
    
    contect_check = False
    
    with open(filename, "w") as file:
        
        for row in curw:
            
            contect_check = True
            
            row = convert_type.convert_None_to_list(row)
            row = tuple(row)
            
            if counter == 0:
            
                users_list = []
                user_list = []
                section_list = []
                student_list = []
                submission_list = []
                answer_list = []
                submission_counter = 0
                answer_counter = 0
                section_counter = 0
                student_counter = 0
                prevsub = str(row[10])
                prevusername = str(row[31])
                prevsectioncode = str(row[22])
                prevstudentcode = str(row[23])
                prevanswer = str(row[0])
                
                
                cur.execute("""
                select * 
                from portal.user_section
                where section_code = (%s)""",
                (prevsectioncode,))
                
                users = cur.fetchall()
                for user in users:
                    users_list.append((user[1],str(user[3])))
                
                
                
                json_data = {}
                
                if (str(row[43]) ==  str([])):
                    answer_list.append({
                        'answer': str(row[1]),
                        'score': str(row[2]),
                        'comment': str(row[3]),
                        'type_text': str(row[4]),
                        'question_code': str(row[17]),
                        'grader': str(row[7]),
                        'graded_at': str(row[8]),
                        'created_at': str(row[9]),
                        'uploaded_file': []
                    })
                else:
                    answer_list.append({
                        'answer': str(row[1]),
                        'score': str(row[2]),
                        'comment': str(row[3]),
                        'type_text': str(row[4]),
                        'question_code': str(row[17]),
                        'grader': str(row[7]),
                        'graded_at': str(row[8]),
                        'created_at': str(row[9]),
                        'uploaded_file': [{
                            'name': str(row[44]),
                            'location': str(row[45]),
                            'size': str(row[46]),
                            'type': str(row[47]),
                            'created_at': str(row[49])
                        }]
                    })
                
                submission_list.append({
                    'ip_address': str(row[12]),
                    'status': str(row[13]),
                    'answered_at': str(row[14]),
                    'form_hash': str(row[20]),
                    'form_code': str(row[25]),
                    'relative_path': str(row[28]),
                    'prefix': str(row[29]),
                    'course_name': str(row[30]),
                    'answers': answer_list
                })
                
                student_data = {
                    'first_name':str(row[34]),
                    'last_name':str(row[35]),
                    'grade_level':str(row[36]),
                    'gender':str(row[37]),
                    'ethnicity':str(row[38]),
                    'hispanic_latino':str(row[39]),
                    'evaluation':str(row[40]),
                    'native_language':str(row[41]),
                    'created_at':str(row[33])
                }
                
                counter += 1
                continue
            
                
            if counter+1 == view_lenght:
                if prevsub != str(row[10]):
                
                    if (str(row[43]) ==  str([])):
                        submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': []
                            }]
                        })
                    else:
                        submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': [{
                                'name': str(row[44]),
                                'location': str(row[45]),
                                'size': str(row[46]),
                                'type': str(row[47]),
                                'created_at': str(row[49])
                            }]
                            }]
                        })
                    
                    
                    
                    student_list.append({
                        'student_code': prevstudentcode,
                        'student_data': student_data,
                        'submissions': submission_list.copy()
                    })
                    section_list.append({
                        'section_code': prevsectioncode,
                        'other_users': users_list,
                        'section_data': student_list.copy()
                    })
                    user_list.append({
                        'user_name': prevusername,
                        'user_data': section_list.copy()
                    })
                    submission_list = []
                    submission_counter = 0
                    answer_counter = 0
                    student_data = {
                        'first_name':str(row[34]),
                        'last_name':str(row[35]),
                        'grade_level':str(row[36]),
                        'gender':str(row[37]),
                        'ethnicity':str(row[38]),
                        'hispanic_latino':str(row[39]),
                        'evaluation':str(row[40]),
                        'native_language':str(row[41]),
                        'created_at':str(row[33])
                    }
                    break
                elif prevanswer != str(row[0]):
                    if (str(row[43]) ==  str([])):
                        submission_list[submission_counter]['answers'].append({
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': []
                        })
                    else:
                        submission_list[submission_counter]['answers'].append({
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': [{
                                'name': str(row[44]),
                                'location': str(row[45]),
                                'size': str(row[46]),
                                'type': str(row[47]),
                                'created_at': str(row[49])
                            }]
                        })
                    
                    
                    
                    student_list.append({
                        'student_code': prevstudentcode,
                        'student_data': student_data,
                        'submissions': submission_list.copy()
                    })
                    section_list.append({
                        'section_code': prevsectioncode,
                        'other_users': users_list,
                        'section_data': student_list.copy(),
                    })
                    user_list.append({
                        'user_name': prevusername,
                        'user_data': section_list.copy()
                    })
                    submission_list = []
                    submission_counter = 0
                    answer_counter = 0
                    student_data = {
                        'first_name':str(row[34]),
                        'last_name':str(row[35]),
                        'grade_level':str(row[36]),
                        'gender':str(row[37]),
                        'ethnicity':str(row[38]),
                        'hispanic_latino':str(row[39]),
                        'evaluation':str(row[40]),
                        'native_language':str(row[41]),
                        'created_at':str(row[33])
                    }
                    break
                else:
                    submission_list[submission_counter]['answers'][answer_counter]['uploaded_file'].append({
                    'name': str(row[44]),
                    'location': str(row[45]),
                    'size': str(row[46]),
                    'type': str(row[47]),
                    'created_at': str(row[49])
                    })
                    
                    
                    student_list.append({
                        'student_code': prevstudentcode,
                        'student_data': student_data,
                        'submissions': submission_list.copy()
                    })
                    section_list.append({
                        'section_code': prevsectioncode,
                        'other_users': users_list,
                        'section_data': student_list.copy(),
                    })
                    user_list.append({
                        'user_name': prevusername,
                        'user_data': section_list.copy()
                    })
                    submission_list = []
                    submission_counter = 0
                    answer_counter = 0
                    student_data = {
                        'first_name':str(row[34]),
                        'last_name':str(row[35]),
                        'grade_level':str(row[36]),
                        'gender':str(row[37]),
                        'ethnicity':str(row[38]),
                        'hispanic_latino':str(row[39]),
                        'evaluation':str(row[40]),
                        'native_language':str(row[41]),
                        'created_at':str(row[33])
                    }
                    break
            
            
            if prevusername != str(row[31]):
                
                
                
                student_list.append({
                    'student_code': prevstudentcode,
                    'student_data': student_data,
                    'submissions': submission_list.copy()
                })
                section_list.append({
                    'section_code': prevsectioncode,
                    'other_users': users_list,
                    'section_data': student_list.copy()
                })
                user_list.append({
                    'user_name': prevusername,
                    'user_data': section_list.copy()
                })
                
                student_data = {
                    'first_name':str(row[34]),
                    'last_name':str(row[35]),
                    'grade_level':str(row[36]),
                    'gender':str(row[37]),
                    'ethnicity':str(row[38]),
                    'hispanic_latino':str(row[39]),
                    'evaluation':str(row[40]),
                    'native_language':str(row[41]),
                    'created_at':str(row[33])
                }
                prevusername = str(row[31])
                prevsectioncode = str(row[22])
                prevstudentcode = str(row[23])
                section_counter=0
                section_list = []
                student_list = []
                submission_list = []
                submission_counter = 0
                answer_counter = 0
                student_counter = 0
                prevstudentcode = str(row[23])
                prevsub = str(row[10])
                prevanswer = str(row[0])
                
                if (str(row[43]) ==  str([])):
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': []
                        }]
                    })
                else:
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': [{
                                'name': str(row[44]),
                                'location': str(row[45]),
                                'size': str(row[46]),
                                'type': str(row[47]),
                                'created_at': str(row[49])
                            }]
                        }]
                    })
                
                users_list = []
                
                cur.execute("""
                select * 
                from portal.user_section 
                where section_code = (%s)""", 
                (prevsectioncode,))
                
                users = cur.fetchall()
                
                for user in users:
                    users_list.append((user[1],str(user[3])))
            
            elif prevsectioncode != str(row[22]):
                
                
                
                student_list.append({
                    'student_code': prevstudentcode,
                    'student_data': student_data,
                    'submissions': submission_list.copy()
                })
                section_list.append({
                    'section_code': prevsectioncode,
                    'other_users': users_list,
                    'section_data': student_list.copy()
                })
                prevsectioncode = str(row[22])
                prevstudentcode = str(row[23])
                section_counter+=1
                student_list = []
                submission_list = []
                submission_counter = 0
                answers_counter = 0
                student_counter = 0
                prevstudentcode = str(row[23])
                prevsub = str(row[10])
                prevanswer = str(row[0])
                
                if (str(row[43]) ==  str([])):
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': []
                        }]
                    })
                else:
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': [{
                                'name': str(row[44]),
                                'location': str(row[45]),
                                'size': str(row[46]),
                                'type': str(row[47]),
                                'created_at': str(row[49])
                            }]
                        }]
                    })
                users_list = []
                
                cur.execute("""
                select * 
                from portal.user_section 
                where section_code = (%s)""", 
                (prevsectioncode,))
                
                users = cur.fetchall()
                
                for user in users:
                    users_list.append((user[1],str(user[3])))
                
                
            elif prevstudentcode != str(row[23]):
                
                
                student_list.append({
                    'student_code': prevstudentcode,
                    'student_data': student_data,
                    'submissions': submission_list.copy()
                })
                
                student_data = {
                    'first_name':str(row[34]),
                    'last_name':str(row[35]),
                    'grade_level':str(row[36]),
                    'gender':str(row[37]),
                    'ethnicity':str(row[38]),
                    'hispanic_latino':str(row[39]),
                    'evaluation':str(row[40]),
                    'native_language':str(row[41]),
                    'created_at':str(row[33])
                }
                
                student_counter+=1
                prevstudentcode = str(row[23])
                submission_list = []
                submission_counter = 0
                answers_counter = 0
                prevsub = str(row[10])
                prevanswer = str(row[0])
                if (str(row[43]) ==  str([])):
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': []
                        }]
                    })
                else:
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': [{
                                'name': str(row[44]),
                                'location': str(row[45]),
                                'size': str(row[46]),
                                'type': str(row[47]),
                                'created_at': str(row[49])
                            }]
                        }]
                    })
                
            
                
            elif prevsub != str(row[10]):
                submission_counter+=1
                
                if (str(row[43]) ==  str([])):
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': []
                        }]
                    })
                else:
                    submission_list.append({
                        'ip_address': str(row[12]),
                        'status': str(row[13]),
                        'answered_at': str(row[14]),
                        'form_hash': str(row[20]),
                        'form_code': str(row[25]),
                        'relative_path': str(row[28]),
                        'prefix': str(row[29]),
                        'course_name': str(row[30]),
                        'answers': [{
                            'answer': str(row[1]),
                            'score': str(row[2]),
                            'comment': str(row[3]),
                            'type_text': str(row[4]),
                            'question_code': str(row[17]),
                            'grader': str(row[7]),
                            'graded_at': str(row[8]),
                            'created_at': str(row[9]),
                            'uploaded_file': [{
                                'name': str(row[44]),
                                'location': str(row[45]),
                                'size': str(row[46]),
                                'type': str(row[47]),
                                'created_at': str(row[49])
                            }]
                        }]
                    })
                prevsub =str(row[10])
                prevanswer = str(row[0])
                answer_counter = 0
                
            elif prevanswer != str(row[0]):
                answer_counter+=1
                if (str(row[43]) ==  str([])):
                    submission_list[submission_counter]['answers'].append({
                        'answer': str(row[1]),
                        'score': str(row[2]),
                        'comment': str(row[3]),
                        'type_text': str(row[4]),
                        'question_code': str(row[17]),
                        'grader': str(row[7]),
                        'graded_at': str(row[8]),
                        'created_at': str(row[9]),
                        'uploaded_file': []
                    })
                else:
                    submission_list[submission_counter]['answers'].append({
                        'answer': str(row[1]),
                        'score': str(row[2]),
                        'comment': str(row[3]),
                        'type_text': str(row[4]),
                        'question_code': str(row[17]),
                        'grader': str(row[7]),
                        'graded_at': str(row[8]),
                        'created_at': str(row[9]),
                        'uploaded_file': [{
                            'name': str(row[44]),
                            'location': str(row[45]),
                            'size': str(row[46]),
                            'type': str(row[47]),
                            'created_at': str(row[49])
                        }]
                    })
                prevanswer = str(row[0])
            else:
                
                submission_list[submission_counter]['answers'][answer_counter]['uploaded_file'].append({
                    'name': str(row[44]),
                    'location': str(row[45]),
                    'size': str(row[46]),
                    'type': str(row[47]),
                    'created_at': str(row[49])
                })
            counter+=1
        
        
        if not contect_check:
            print("view is empty")
            return
        
        
        json_data = user_list
        
        file_data = json.dumps(json_data)
        file.writelines(file_data)
        
    print("creating schema json")
    table_list = []
    # table_list = ['courses','paths','lessons','activities','forms','submissions','answers']
    schema.create_schema(cur,'portal',table_list)
    print("Done")
    








def reinsert_submission_view10(filename,schema_json):
    now = datetime.datetime.now()
    print("day: %s   hour: %s   min: %s   sec: %s" % (now.day,now.hour,now.minute,now.second))
    
    
    try:
    
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure credentials are correct")
        return
    
    
    if not schema.validate_schema(cur,'portal',schema_json):
        print("schema not valid")
        return
    
    submissionlist = []
    submission_id_list = []
    
    cache = {}
    cache['lessons'] = {}
    cache['activities'] = {}
    cache['forms'] = {}
    cache['students']= {}
    cache['questions'] = {}
    cache['submissions'] = {}
    
    submissionid = 0
    
    
    
    view_insert_query = """
    INSERT INTO portal.extracted_answers 
    (answer,score,comment,type_text,grader,graded_at,created_at,
    ip_address,status,answered_at,question_code,question_text,
    form_hash,students_section_code,student_code,form_code,section_code,
    relative_path,paths_prefix,course_name) VALUES (%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    
    submission_query = """
    INSERT INTO portal.submissions 
    (student_id,ip_address,status,answered_at,form_id) 
    VALUES (%s,%s,%s,%s,%s)"""
    
    answers_query = """
    INSERT INTO portal.answers 
    (answer,score,comment,type_text,question_id,
    submission_id,grader,graded_at,created_at) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    
    submission_query_returnid = """
    INSERT INTO portal.submissions 
    (student_id,ip_address,status,answered_at,form_id) 
    VALUES 
    (%s,%s,%s,%s,%s) returning id"""
    
    answers_query_returnid = """
    INSERT INTO portal.answers 
    (answer,score,comment,type_text,question_id,submission_id,
    grader,graded_at,created_at) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    returning id"""
    
    submission_values = """student_id,ip_address,status,answered_at,form_id"""
    
    answer_values = """answer,score,comment,type_text,question_id,
    submission_id,grader,graded_at,created_at"""
    
    uploaded_file_values = """name,location,size,type,answer_id,created_at"""
    
    cur.execute("select * from portal.lessons")
    lessons = cur.fetchall()
    for i in range(len(lessons)):
        
        lessonid = str(lessons[i][0])
        prefix = str(lessons[i][1])
        relative_path = str(lessons[i][5])
        
        temp_tup = (prefix,relative_path)
        
        cache['lessons'][temp_tup] = lessonid
    
    cur.execute("select * from portal.activities")
    activities = cur.fetchall()
    for i in range(len(activities)):
        
        activitiesid = str(activities[i][0])
        activities_lesson_id = str(activities[i][1])
        form_code = str(activities[i][2])
        
        temp_tup = (activities_lesson_id, form_code)
        
        cache['activities'][temp_tup] = activitiesid
    
    cur.execute("select * from portal.forms")
    forms = cur.fetchall()
    for i in range(len(forms)):
    
        formsid = str(forms[i][0])
        form_hash = str(forms[i][1])
        forms_activity_id = str(forms[i][2])
        
        temp_tup = (form_hash,forms_activity_id)
        
        cache['forms'][temp_tup] = formsid
    
    cur.execute("select * from portal.students")
    students = cur.fetchall()
    for i in range(len(students)):
        
        studentsid = str(students[i][0])
        students_section_code = str(students[i][1])
        students_student_code = str(students[i][2])
        
        temp_tup = (students_section_code,students_student_code)
        
        cache['students'][temp_tup] = studentsid
    
    cur.execute("select * from portal.questions")
    questions  = cur.fetchall()
    for i in range(len(questions)):
        
        questionsid = str(questions[i][0])
        questions_form_id = str(questions[i][1])
        questions_question_code = str(questions[i][2])
        
        temp_tup = (questions_form_id,questions_question_code)
        
        cache['questions'][temp_tup] = questionsid
    
    
    
    submissionvaluetup = []
    count = 1
    json_data = ""
    print("inserting submissions")
    
    with open(filename, "r") as file:
        
        for data in file.readlines():
            if data != "," and data != "[" and data != "\n":
                
                try:
                    json_data = json.loads(data)
                except:
                    continue
                
                if json_data:
                    
                    for users in json_data:
                    
                        for user_data in users['user_data']:
                            
                            section_code = user_data['section_code']
                            
                            for section_data in user_data['section_data']:
                                
                                student_code = section_data['student_code']
                                studentid = cache['students'][(section_code, student_code)]
                                
                                for submissions in section_data['submissions']:
                                    
                                    
                                    """
                                    no other valriables can be moved up
                                    because submissions contain data from
                                    all related tables.. unless you make variables
                                    to store data from each related tables
                                    """
                                    
                                    submissions = convert_type.convert_list_to_None(submissions)
                                    
                                    ip_address = submissions["ip_address"]
                                    status = submissions["status"]
                                    answered_at = submissions["answered_at"]
                                    form_hash = submissions["form_hash"]
                                    
                                    form_code = submissions["form_code"]
                                    relative_path = submissions["relative_path"]
                                    prefix = submissions["prefix"]
                                    course_name = submissions["course_name"]
                                    
                                    
                                    lessonid = cache['lessons'][(prefix,relative_path)]
                                    activityid = cache['activities'][(lessonid,form_code)]
                                    formid = cache['forms'][(form_hash, activityid)]
                                    
                                    
                                    submissionvaluetup.append((studentid,ip_address,status,answered_at,formid))
                                    count+=1
                                    if count % 10000 == 0:
                                        
                                        ids = insert_data.insert_many(cur,'portal.submissions','id',submissionvaluetup,submission_values)
                                        submission_id_list.extend(ids)
                                        
                                        submissionvaluetup = []
                        
                    
        if submissionvaluetup:
            
            ids = insert_data.insert_many(cur,'portal.submissions','id',submissionvaluetup,submission_values)
            
            submission_id_list.extend(ids)
            
        

    
        print("inserting answers")
        answervaluetup = []
        count = 1
        json_data = ""
        file.seek(0)
        count2 = 0
        answers_counter = 0
        
        answer_id_list = []
        
        for data in file.readlines():
            
            try:
                json_data = json.loads(data)
            except:
                continue
            
            if json_data:
                
                for users in json_data:
                
                    for user_data in users['user_data']:
                        
                        section_code = user_data['section_code']
                        
                        for section_data in user_data['section_data']:
                            
                            student_code = section_data['student_code']
                            studentid = cache['students'][(section_code, student_code)]
                            
                            for submissions in section_data['submissions']:
                                
                                ip_address = submissions["ip_address"]
                                status = submissions["status"]
                                answered_at = submissions["answered_at"]
                                form_hash = submissions["form_hash"]
                                
                                form_code = submissions["form_code"]
                                relative_path = submissions["relative_path"]
                                prefix = submissions["prefix"]
                                course_name = submissions["course_name"]
                                
                                
                                lessonid = cache['lessons'][(prefix,relative_path)]
                                activityid = cache['activities'][(lessonid,form_code)]
                                formid = cache['forms'][(form_hash, activityid)]
                                
                                
                                submission_id = str(submission_id_list[count2])
                        
                                for answer in submissions["answers"]:
                                    
                                    answer = convert_type.convert_list_to_None(answer)
                                    
                                    answer_text = answer["answer"]
                                    score = answer["score"]
                                    comment = answer["comment"]
                                    type_text = answer["type_text"]
                                    grader = answer["grader"]
                                    graded_at = answer["graded_at"]
                                    created_at = answer["created_at"]
                                    
                                    question_code = answer["question_code"]
                                
                                    
                                    
                                    if question_code == None:
                                        continue
                                    questionid = cache['questions'][(formid,question_code)]
                                    
                                    if score == "None": score = None
                                    if comment == "None": comment = None
                                    if grader == "None": grader = None
                                    if graded_at == "None": graded_at = None
                                    
                                    
                                    
                                    answervaluetup.append((answer_text,score,comment,type_text,questionid,submission_id,grader,graded_at,created_at))
                                    count+=1
                                    if count % 10000 == 0:
                                        
                                        
                                        ids = insert_data.insert_many(cur,'portal.answers','id',answervaluetup,answer_values)
                                        answer_id_list.extend(ids)
                                        answervaluetup = []
                                    
                                    
                                
                                count2 += 1
        if answervaluetup:
            
            ids = insert_data.insert_many(cur,'portal.answers','id',answervaluetup,answer_values)
            answer_id_list.extend(ids)
            
        # print(answer_id_list)
        # print(len(answer_id_list))
        # return
        print("inserting uploaded files")
        file.seek(0)
        count2 = 0
        uploadedfilevaluetup = []
        count = 1
        
        for data in file.readlines():
            
            try:
                json_data = json.loads(data)
            except:
                continue
            
            if json_data:
                
                for users in json_data:
                
                    for user_data in users['user_data']:
                        
                        section_code = user_data['section_code']
                        
                        for section_data in user_data['section_data']:
                            
                            student_code = section_data['student_code']
                            studentid = cache['students'][(section_code, student_code)]
                            
                            for submissions in section_data['submissions']:
                                
                                ip_address = submissions["ip_address"]
                                status = submissions["status"]
                                answered_at = submissions["answered_at"]
                                form_hash = submissions["form_hash"]
                                
                                form_code = submissions["form_code"]
                                relative_path = submissions["relative_path"]
                                prefix = submissions["prefix"]
                                course_name = submissions["course_name"]
                                
                                
                                lessonid = cache['lessons'][(prefix,relative_path)]
                                activityid = cache['activities'][(lessonid,form_code)]
                                formid = cache['forms'][(form_hash, activityid)]
                                
                                
                                
                                for answer in submissions['answers']:
                                    # print(len(answer),count2)
                                    if answer['question_code'] == str([]):
                                        continue
                                    
                                    if answer:
                                        # print(len(answer_id_list))
                                        answer_id = str(answer_id_list[count2])
                                        
                                        for uploaded_file in answer['uploaded_file']:
                                            
                                            uploaded_file = convert_type.convert_list_to_None(uploaded_file)
                                            
                                            uploaded_file_name = uploaded_file['name']
                                            uploaded_file_location = uploaded_file['location']
                                            uploaded_file_size = uploaded_file['size']
                                            uploaded_file_type = uploaded_file['type']
                                            uploaded_file_created_at = uploaded_file['created_at']
                                            
                                            uploadedfilevaluetup.append((uploaded_file_name,uploaded_file_location,
                                            uploaded_file_size,uploaded_file_type,answer_id,uploaded_file_created_at))
                                            count+=1
                                            
                                            if count % 10000 == 0:
                                                
                                                insert_data.insert_many(cur,'portal.uploaded_file','id',uploadedfilevaluetup,uploaded_file_values)
                                                uploadedfilevaluetup = []
                                        
                                        count2+=1
        
        if uploadedfilevaluetup:
            
            ids = insert_data.insert_many(cur,'portal.uploaded_file','id',uploadedfilevaluetup,uploaded_file_values)
            uploadedfilevaluetup = []
    
    con.commit()
    print("Done.")
    cur.close()
    

    now2 = datetime.datetime.now()
    print("start time - day: %s   hour: %s   min: %s   sec: %s" % (now.day,now.hour,now.minute,now.second))
    print("end time - day: %s   hour: %s   min: %s   sec: %s" % (now2.day,now2.hour,now2.minute,now2.second))













def reinsert_submission_view10_into_course(filename,schema_json,new_course):
    now = datetime.datetime.now()
    print("day: %s   hour: %s   min: %s   sec: %s" % (now.day,now.hour,now.minute,now.second))
    
    
    try:
    
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure credentials are correct")
        return
    
    
    if not schema.validate_schema(cur,'portal',schema_json):
        print("schema not valid")
        return
    
    submissionlist = []
    submission_id_list = []
    
    cache = {}
    cache['lessons'] = {}
    cache['activities'] = {}
    cache['forms'] = {}
    cache['students']= {}
    cache['questions'] = {}
    cache['submissions'] = {}
    
    submissionid = 0
    
    
    
    view_insert_query = """
    INSERT INTO portal.extracted_answers 
    (answer,score,comment,type_text,grader,graded_at,created_at,
    ip_address,status,answered_at,question_code,question_text,
    form_hash,students_section_code,student_code,form_code,section_code,
    relative_path,paths_prefix,course_name) VALUES (%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    
    submission_query = """
    INSERT INTO portal.submissions 
    (student_id,ip_address,status,answered_at,form_id) 
    VALUES (%s,%s,%s,%s,%s)"""
    
    answers_query = """
    INSERT INTO portal.answers 
    (answer,score,comment,type_text,question_id,
    submission_id,grader,graded_at,created_at) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    
    submission_query_returnid = """
    INSERT INTO portal.submissions 
    (student_id,ip_address,status,answered_at,form_id) 
    VALUES 
    (%s,%s,%s,%s,%s) returning id"""
    
    answers_query_returnid = """
    INSERT INTO portal.answers 
    (answer,score,comment,type_text,question_id,submission_id,
    grader,graded_at,created_at) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    returning id"""
    
    submission_values = """student_id,ip_address,status,answered_at,form_id"""
    
    answer_values = """answer,score,comment,type_text,question_id,
    submission_id,grader,graded_at,created_at"""
    
    uploaded_file_values = """name,location,size,type,answer_id,created_at"""
    
    cur.execute("select * from portal.lessons")
    lessons = cur.fetchall()
    for i in range(len(lessons)):
        
        lessonid = str(lessons[i][0])
        prefix = str(lessons[i][1])
        relative_path = str(lessons[i][5])
        
        temp_tup = (prefix,relative_path)
        
        cache['lessons'][temp_tup] = lessonid
    
    cur.execute("select * from portal.activities")
    activities = cur.fetchall()
    for i in range(len(activities)):
        
        activitiesid = str(activities[i][0])
        activities_lesson_id = str(activities[i][1])
        form_code = str(activities[i][2])
        
        temp_tup = (activities_lesson_id, form_code)
        
        cache['activities'][temp_tup] = activitiesid
    
    cur.execute("select * from portal.forms")
    forms = cur.fetchall()
    for i in range(len(forms)):
    
        formsid = str(forms[i][0])
        form_hash = str(forms[i][1])
        forms_activity_id = str(forms[i][2])
        
        temp_tup = (form_hash,forms_activity_id)
        
        cache['forms'][temp_tup] = formsid
    
    cur.execute("select * from portal.students")
    students = cur.fetchall()
    for i in range(len(students)):
        
        studentsid = str(students[i][0])
        students_section_code = str(students[i][1])
        students_student_code = str(students[i][2])
        
        temp_tup = (students_section_code,students_student_code)
        
        cache['students'][temp_tup] = studentsid
    
    cur.execute("select * from portal.questions")
    questions  = cur.fetchall()
    for i in range(len(questions)):
        
        questionsid = str(questions[i][0])
        questions_form_id = str(questions[i][1])
        questions_question_code = str(questions[i][2])
        
        temp_tup = (questions_form_id,questions_question_code)
        
        cache['questions'][temp_tup] = questionsid
    
    
    
    submissionvaluetup = []
    count = 1
    json_data = ""
    print("inserting submissions")
    
    with open(filename, "r") as file:
        
        for data in file.readlines():
            if data != "," and data != "[" and data != "\n":
                
                try:
                    json_data = json.loads(data)
                except:
                    continue
                
                if json_data:
                    
                    for users in json_data:
                    
                        for user_data in users['user_data']:
                            
                            section_code = user_data['section_code']
                            
                            for section_data in user_data['section_data']:
                                
                                student_code = section_data['student_code']
                                studentid = cache['students'][(section_code, student_code)]
                                
                                for submissions in section_data['submissions']:
                                    
                                    
                                    """
                                    no other valriables can be moved up
                                    because submissions contain data from
                                    all related tables.. unless you make variables
                                    to store data from each related tables
                                    """
                                    
                                    submissions = convert_type.convert_list_to_None(submissions)
                                    
                                    ip_address = submissions["ip_address"]
                                    status = submissions["status"]
                                    answered_at = submissions["answered_at"]
                                    form_hash = submissions["form_hash"]
                                    
                                    form_code = submissions["form_code"]
                                    relative_path = submissions["relative_path"]
                                    prefix = new_course
                                    course_name = new_course
                                    
                                    
                                    lessonid = cache['lessons'][(prefix,relative_path)]
                                    activityid = cache['activities'][(lessonid,form_code)]
                                    formid = cache['forms'][(form_hash, activityid)]
                                    
                                    
                                    submissionvaluetup.append((studentid,ip_address,status,answered_at,formid))
                                    count+=1
                                    if count % 10000 == 0:
                                        
                                        ids = insert_data.insert_many(cur,'portal.submissions','id',submissionvaluetup,submission_values)
                                        submission_id_list.extend(ids)
                                        
                                        submissionvaluetup = []
                        
                    
        if submissionvaluetup:
            
            ids = insert_data.insert_many(cur,'portal.submissions','id',submissionvaluetup,submission_values)
            
            submission_id_list.extend(ids)
            
        

    
        print("inserting answers")
        answervaluetup = []
        count = 1
        json_data = ""
        file.seek(0)
        count2 = 0
        answers_counter = 0
        
        answer_id_list = []
        
        for data in file.readlines():
            
            try:
                json_data = json.loads(data)
            except:
                continue
            
            if json_data:
                
                for users in json_data:
                
                    for user_data in users['user_data']:
                        
                        section_code = user_data['section_code']
                        
                        for section_data in user_data['section_data']:
                            
                            student_code = section_data['student_code']
                            studentid = cache['students'][(section_code, student_code)]
                            
                            for submissions in section_data['submissions']:
                                
                                ip_address = submissions["ip_address"]
                                status = submissions["status"]
                                answered_at = submissions["answered_at"]
                                form_hash = submissions["form_hash"]
                                
                                form_code = submissions["form_code"]
                                relative_path = submissions["relative_path"]
                                prefix = new_course
                                course_name = new_course
                                
                                
                                lessonid = cache['lessons'][(prefix,relative_path)]
                                activityid = cache['activities'][(lessonid,form_code)]
                                formid = cache['forms'][(form_hash, activityid)]
                                
                                
                                submission_id = str(submission_id_list[count2])
                        
                                for answer in submissions["answers"]:
                                    
                                    answer = convert_type.convert_list_to_None(answer)
                                    
                                    answer_text = answer["answer"]
                                    score = answer["score"]
                                    comment = answer["comment"]
                                    type_text = answer["type_text"]
                                    grader = answer["grader"]
                                    graded_at = answer["graded_at"]
                                    created_at = answer["created_at"]
                                    
                                    question_code = answer["question_code"]
                                
                                    
                                    
                                    if question_code == None:
                                        continue
                                    questionid = cache['questions'][(formid,question_code)]
                                    
                                    if score == "None": score = None
                                    if comment == "None": comment = None
                                    if grader == "None": grader = None
                                    if graded_at == "None": graded_at = None
                                    
                                    
                                    
                                    answervaluetup.append((answer_text,score,comment,type_text,questionid,submission_id,grader,graded_at,created_at))
                                    count+=1
                                    if count % 10000 == 0:
                                        
                                        
                                        ids = insert_data.insert_many(cur,'portal.answers','id',answervaluetup,answer_values)
                                        answer_id_list.extend(ids)
                                        answervaluetup = []
                                    
                                    
                                
                                count2 += 1
        if answervaluetup:
            
            ids = insert_data.insert_many(cur,'portal.answers','id',answervaluetup,answer_values)
            answer_id_list.extend(ids)
            
        # print(answer_id_list)
        # print(len(answer_id_list))
        # return
        print("inserting uploaded files")
        file.seek(0)
        count2 = 0
        uploadedfilevaluetup = []
        count = 1
        
        for data in file.readlines():
            
            try:
                json_data = json.loads(data)
            except:
                continue
            
            if json_data:
                
                for users in json_data:
                
                    for user_data in users['user_data']:
                        
                        section_code = user_data['section_code']
                        
                        for section_data in user_data['section_data']:
                            
                            student_code = section_data['student_code']
                            studentid = cache['students'][(section_code, student_code)]
                            
                            for submissions in section_data['submissions']:
                                
                                ip_address = submissions["ip_address"]
                                status = submissions["status"]
                                answered_at = submissions["answered_at"]
                                form_hash = submissions["form_hash"]
                                
                                form_code = submissions["form_code"]
                                relative_path = submissions["relative_path"]
                                prefix = new_course
                                course_name = new_course
                                
                                
                                lessonid = cache['lessons'][(prefix,relative_path)]
                                activityid = cache['activities'][(lessonid,form_code)]
                                formid = cache['forms'][(form_hash, activityid)]
                                
                                
                                
                                for answer in submissions['answers']:
                                    # print(len(answer),count2)
                                    if answer['question_code'] == str([]):
                                        continue
                                    
                                    if answer:
                                        # print(len(answer_id_list))
                                        answer_id = str(answer_id_list[count2])
                                        
                                        for uploaded_file in answer['uploaded_file']:
                                            
                                            uploaded_file = convert_type.convert_list_to_None(uploaded_file)
                                            
                                            uploaded_file_name = uploaded_file['name']
                                            uploaded_file_location = uploaded_file['location']
                                            uploaded_file_size = uploaded_file['size']
                                            uploaded_file_type = uploaded_file['type']
                                            uploaded_file_created_at = uploaded_file['created_at']
                                            
                                            uploadedfilevaluetup.append((uploaded_file_name,uploaded_file_location,
                                            uploaded_file_size,uploaded_file_type,answer_id,uploaded_file_created_at))
                                            count+=1
                                            
                                            if count % 10000 == 0:
                                                
                                                insert_data.insert_many(cur,'portal.uploaded_file','id',uploadedfilevaluetup,uploaded_file_values)
                                                uploadedfilevaluetup = []
                                        
                                        count2+=1
        
        if uploadedfilevaluetup:
            
            ids = insert_data.insert_many(cur,'portal.uploaded_file','id',uploadedfilevaluetup,uploaded_file_values)
            uploadedfilevaluetup = []
    
    con.commit()
    print("Done.")
    cur.close()
    

    now2 = datetime.datetime.now()
    print("start time - day: %s   hour: %s   min: %s   sec: %s" % (now.day,now.hour,now.minute,now.second))
    print("end time - day: %s   hour: %s   min: %s   sec: %s" % (now2.day,now2.hour,now2.minute,now2.second))







    




def delete_duplicates():
    
    try:
    
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure credentials are correct")
        return
    
    
    delete_submissions_query = """
    delete from portal.submissions
    where id = (%s)
    """
    query = """
    select * 
    from portal.submissions s1
    join portal.submissions s2
    on s1.id > s2.id 
    where s1.student_id = s2.student_id 
    and s1.form_id  = s2.form_id 
    and s1.answered_at = s2.answered_at;
    """
    cur.execute(query)
    duplicates = cur.fetchall()
    
    if duplicates:
        print("duplicates found")
        print("deleting duplicates")
    else:
        print("no duplicates were found")
    
    for duplicate in duplicates:
        
        cur.execute(delete_submissions_query,(duplicate[0],))
        
    con.commit()
    
    print("done")






    



def delete_submission_view():
    
    print("press CTRL-C to cancel deletion")
    time.sleep(2)
    
    try:
    
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure credentials are correct")
        return
    
    
    print("truncate table portal.submissions cascade")
    cur.execute("truncate table portal.submissions cascade")
    
    con.commit()
    print("Done.")
    cur.close()


def start():
    intermediate = ""
    if len(sys.argv) > 5:
        
        operation = sys.argv[1] # extract or insert
        type = sys.argv[2] # section or teacher
        code = sys.argv[3] # section_code or user_name
        type2 = sys.argv[4] # student or section
        code2 = sys.argv[5] # student_code or section_code
        intermediate = sys.argv[6] # extract intermediate file
    elif len(sys.argv) > 4:
        operation = sys.argv[1] # extract or insert
        type = sys.argv[2] # section or teacher
        code = sys.argv[3] # section_code or user_name
        intermediate = sys.argv[4] # extract intermediate file
    elif len(sys.argv) > 3:
        operation = sys.argv[1]
        type = sys.argv[2]
        intermediate = sys.argv[3]
    elif len(sys.argv) > 2:
        operation = sys.argv[1]
        intermediate = sys.argv[2]
    else:
        with open("courses_doc.md") as file:
            print(file.read())
        print("usage: python3 create_jsons.py <insert/extract> <teacher/section> <user_name/section_code>")
        return
    # try:
    if operation == "extract":
        if len(sys.argv) > 5:
            filename = type2 + "_" + code2 + ".json"
            extract_answers_view(type,code,type2,code2,filename,intermediate)
            
        elif len(sys.argv) > 3:
            filename = type + "_" + code + ".json"
            extract_answers_view(type,code,"","",filename,intermediate)
            
            
        else:
            print("extracting all data")
            print("press CTRL-C to cancel")
            time.sleep(2)
            filename = "all.json"
            extract_answers_view("","","","",filename,intermediate)
            
    elif operation == "insert":
        if type.endswith(".json"):
            
            try:
                with open("schemas.json",'r') as file:
                    schema_json = json.load(file)
            except:
                schema_json = ''
                
            reinsert_submission_view10(type,schema_json)
            # reinsert_submission_view10_without_duplicates(type,schema_json)
    elif operation == "insert-course":
        
        new_course = sys.argv[4]
        
        if type.endswith(".json"):
            
            try:
                with open("schemas.json",'r') as file:
                    schema_json = json.load(file)
            except:
                schema_json = ''
                
            reinsert_submission_view10_into_course(type,schema_json,new_course)
    elif operation == "delete":
        delete_submission_view()
    elif operation == "delete-duplicates":
        delete_duplicates()
    elif operation == "validate-schema":
        con = connect_to_db.connect()
        cur = con.cursor()
        filename = type
        try:
            with open(filename,'r') as file:
                schema_json = json.load(file)
        except:
            print("invalid schema file")
            return
        is_valid = schema.validate_schema(cur,'portal',schema_json)
        
        if not is_valid:
            print("Schema not valid")
            return
        print("Schema is valid")
        return
    # except:
        # print("incorrect usage or values")
        # print("usage: python create_jsons.py <insert/extract> <teacher/section> <user_name/section_code>")



def start2():
    parser = argparse.ArgumentParser(description="extract_questions.py")
    parser.add_argument('-e','--extract', action="store_true",default=False, help="Extract data")
    parser.add_argument('-i','--insert',action="store_true", default=False, help="Insert data")
    parser.add_argument('-d','--delete',action="store_true",default=False,help="Delete data")
    parser.add_argument('--insert_course',action="store_true",default=False,help="Insert course data")
    parser.add_argument('--section',type=str,default="",help="specify section code")
    parser.add_argument('--student',type=str,default="",help="specify student code")
    parser.add_argument('--teacher',type=str,default="",help="specify teacher user_name")
    parser.add_argument('--course',type=str,default="",help="specify course name")
    parser.add_argument('--intermediate',action="store_true",default=False,help="extract intermediate data")
    parser.add_argument('--delete_duplicates',action="store_true",default=False,help="delete duplicate data in database")
    parser.add_argument('--validate_schema',action="store_true",default=False,help="validate database schema")
    parser.add_argument('--infile',nargs='?',type=argparse.FileType('r'),default=sys.stdin)
    parser.add_argument('-o','--outfile',type=str,default="extract.json")
    parser.add_argument('--man',action="store_true",default=False,help="open documentation")
    args = parser.parse_args()
    
    try:
    
        con = connect_to_db.connect()
        cur = con.cursor()
    except:
        print("could not connect to database. please ensure credentials are correct")
        return
    
    if args.extract:
        if args.teacher and args.section:
            if not args.outfile.endswith('.json'):
                print("please use json files to store extract")
            filename = args.outfile
            extract_answers_view("teacher",args.teacher,"section",args.section,filename,args.intermediate)
        elif args.section and args.student:
            if not args.outfile.endswith('.json'):
                print("please use json files to store extract")
            filename = args.outfile
            extract_answers_view("section",args.section,"student",args.student,filename,args.intermediate)
            
        elif args.teacher:
            if not args.outfile.endswith('.json'):
                print("please use json files to store extract")
            filename = args.outfile
            extract_answers_view("teacher",args.teacher,"","",filename,args.intermediate)
            
        elif args.section:
            if not args.outfile.endswith('.json'):
                print("please use json files to store extract")
            filename = args.outfile
            extract_answers_view("section",args.section,"","",filename,args.intermediate)
           
        else:
            if not args.outfile.endswith('.json'):
                print("please use json files to store extract")
            filename = args.outfile
            extract_answers_view("","","","",filename,args.intermediate)
            
    elif args.insert:
        
        if not args.infile:
            print("add json file to be inserted")
            return
        filename = args.infile.name
        try:
            with open("schemas.json",'r') as file:
                schema_json = json.load(file)
        except:
            schema_json = ''
        reinsert_submission_view10(filename,schema_json)
    elif args.insert_course:
        
        if not args.infile or  not args.course:
            print("specify course name and course json file")
            return
        filename = args.infile.name
        course_name = args.course
        try:
            with open("schemas.json",'r') as file:
                schema_json = json.load(file)
        except:
            schema_json = ''
        reinsert_submission_view10_into_course(filename,schema_json,course_name)
    elif args.delete:
        delete_submission_view()
    elif args.delete_duplicates:
        delete_duplicates()
    elif args.validate_schema:
        cur = con.cursor()
        filename = args.infile.name
        try:
            with open(filename,'r') as file:
                schema_json = json.load(file)
        except:
            print("invalid schema file")
            return
        is_valid = schema.validate_schema(cur,'portal',schema_json)
        
        if not is_valid:
            print("Schema not valid")
            return
        print("Schema is valid")
        return
    elif args.man:
        with open("courses_doc.md","r") as file:
            print(file.read())
    else:
        print("use --help flag to see options\n")
        print("""sample usage:\n
        python3 create_jsons.py --extract -o filename.json
        python3 create_jsons.py --extract --teacher ssp -o teacher_ssp.json
        python3 create_jsons.py --extract --section vzmpsv -o section_vzmpsv.json
        python3 create_jsons.py --extract --section vzmpsv --student hvu7rn -o student_hvu7rn.json
        python3 create_jsons.py --extract --section vzmpsv --intermediate -o section_vzmpsv_inter.json
        python3 create_jsons.py --insert --infile all.json
        python3 create_jsons.py --insert_course --course ict/21d --infile filename.json
        python3 create_jsons.py --delete_duplicates
        python3 create_jsons.py --validate_schema --infile schema.json
        """)
    con.close()
# start()
start2()





