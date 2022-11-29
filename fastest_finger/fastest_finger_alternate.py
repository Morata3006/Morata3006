import csv
import datetime
import glob
import os
import shutil
import traceback
from itertools import repeat
from multiprocessing import Pool
import pandas as pd
import numpy as np
# from memory_profiler import profile
from statistics import mean
import logging
import calendar


default_cols = ["Candidate ID", "Zone", "State", "City", "Test Center ID", "Venue Name", "Registration No.",
                "Exam Date", "Exam Time", ]

base_dir = "C:\\Users\\ashetty\\Desktop\\S3_instance\\"
input_dir = "fastestfinger\\raw\\"
output_dir = "C:\\Users\\ashetty\\Desktop\\S3_instance\\"

exception_column_list = ["error"]
master_column_list = ["filename"]
log_path = "./logs/"


def log_setup(name, filename, level):
    logger = logging.getLogger(name)
    formatter = logging.Formatter(
        "%(levelname)s: %(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p"
    )
    fileHandler = logging.FileHandler(log_path + filename, mode="a")
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    if level == "info":
        logger.setLevel(logging.INFO)
    elif level == "error":
        logger.setLevel(logging.ERROR)

    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)
    return logger


def log_message(message, level, name):
    if name == "system_log":
        log = logging.getLogger("INFO")
    if name == "error_log":
        log = logging.getLogger("ERROR")
    if level == "error":
        log.error(message)
    if level == "info":
        log.info(message)


log_setup(
    "INFO",
    datetime.datetime.today().strftime("%d%m%Y") + "_process_executed.log",
    "info",
)
log_setup(
    "ERROR", datetime.datetime.today().strftime("%d%m%Y") + "_errors.log", "error"
)


def process_exod_file(file_path, question_dict, master_dict, registration_dict, col_headers, count_list):
    try:

        client_id = file_path.split("\\")[-4].split('_')[0]
        exam_name = file_path.split("\\")[-4].split('_')[1]
        log_message(
            "Current working directory: " + file_path,
            "info",
            "system_log",
        )
        rack_name = file_path.split('\\')[-1]
        Value = 1

        for file in glob.glob(file_path + "\\*"):  # batch wise logs folder
            file_name = file.split("\\")[-1]
            read_files = []
            output_rows = []
            error_files = []
            pwd_list = []

            for sub_file in glob.glob(file + "\\*.log"):  # individual candidate log file

                try:
                    unique_questions = {}
                    timer_dict = {}
                    response_min_list = []
                    response_sec_list = []
                    exam_id = sub_file.split("\\")[-1].split("-")[0]  # SO711311*** (11 alphanumeric)
                    eed_id = sub_file.split("\\")[-1].split("-")[1]  # 218132 (6 digit)
                    dataframe = (
                        pd.read_csv(
                            sub_file,
                            delimiter="|",
                            names=[
                                "Timestamp",
                                "Section Name",
                                "QuestionID",
                                "CurrentQuestionNumber",
                                "OptionSelected",
                                "AlternateOptionSelected",
                                "Bookmark",
                                "SectionalQuestionNumber",
                                "IPAddress",
                                "Action",
                                "SequenceNumber",
                                "Candidate MachineDateTime",
                                "Timer",
                                "MacAddress",

                            ],
                            low_memory=False,
                        )
                            .reset_index(drop=True)
                    )
                    res = [i for i in list(dataframe['IPAddress']) if 'PC change' in i]
                    dataframe = dataframe.dropna()
                    input_csv_data = dataframe.values.tolist()
                    output_row = []
                    item = []
                    for index, item in enumerate(input_csv_data):
                        item[0] = item[0].replace('INFO - "', "")
                        item[13] = item[13].replace('"', "")
                        unique_key = str(item[2])
                        output_row = [client_id, exam_name, eed_id, exam_id, len(res)]
                        output_row.extend(item)
                        output_row.append(Value)
                        output_row.append('Appeared-Attempted')
                        unique_questions[unique_key] = output_row
                        key_ques = str(client_id) + "_" + str(exam_id) + '_' + str(eed_id) + "_" + str(
                            int(item[2]))  # diamond_examid_eedid_QuestionID
                        key_master = str(client_id) + "_" + str(exam_id)  # diamond_examid
                        if key_ques in question_dict.keys():
                            output_row.append(question_dict[key_ques][-3])  # mdm name
                            output_row.append(question_dict[key_ques][-4])  # crr exam batch
                            output_row.append(question_dict[key_ques][-2])  # crr_crct_key
                            if str(output_row[9]).split('.')[0] in list(str(question_dict[key_ques][-2])) or "8" in list(str(
                                    question_dict[key_ques][-2])):
                                output_row.append('Correct')
                            elif str(output_row[9]).split('.')[0] not in list(str(
                                    question_dict[key_ques][-2])) or "8" not in list(str(question_dict[key_ques][-2])):
                                if str(output_row[9]).split('.')[0] == '-1':
                                    output_row.append('Not Attempted')
                                else:
                                    output_row.append('Incorrect')
                        else:
                            output_row.extend(("nan", "nan", "nan", "nan"))
                        if key_master in master_dict.keys():
                            output_row.append(master_dict[key_master][1])  # zone
                            output_row.append(master_dict[key_master][2])  # state
                            output_row.append(master_dict[key_master][3])  # city
                            output_row.append(master_dict[key_master][4])  # Test Centre ID
                            output_row.append(master_dict[key_master][5])  # venue
                            output_row.append(master_dict[key_master][6])  # reg no.
                            if type(master_dict[key_master][7]) == str:  # exam date
                                format_date = datetime.datetime.strptime(master_dict[key_master][7], '%d/%m/%Y')
                                output_row.append(format_date)
                            else:
                                output_row.append(master_dict[key_master][7])
                            output_row.append(master_dict[key_master][8])  # exam time
                            # registration lookup snippet
                            key_registration = client_id + '_' + master_dict[key_master][6]  # registration key
                            if key_registration in registration_dict.keys():
                                output_row.append( registration_dict[key_registration][col_headers.index('Gender')])  # Gender
                                output_row.append(registration_dict[key_registration][col_headers.index('Category(Caste)')])  # Category
                                if str(registration_dict[key_registration][col_headers.index('Are you differently Abled?')]) != 'nan':
                                    output_row.append("Yes")
                                    output_row.append(30)
                                else:
                                    output_row.append("No")
                                    output_row.append(0)
                                output_row.append(registration_dict[key_registration][col_headers.index('Graduation Degree/Diploma Name')])
                                output_row.append(registration_dict[key_registration][col_headers.index('10th/SSC percentage')])  # SSC
                                output_row.append(registration_dict[key_registration][col_headers.index('12th/HSC percentage')])  # HSC
                                output_row.append(registration_dict[key_registration][col_headers.index('Graduation Percentage')])  # grad
                                output_row.append(registration_dict[key_registration][col_headers.index('Post Applying For:')])  # Post applying
                            else:
                                output_row.extend(["nan"] * 9)
                        else:
                            output_row.extend(["nan"] * 17)
                        if index != 0:  # response time block
                            if input_csv_data[index - 1][-2] == '00:00' and int(eed_id) in pwd_list:  # handling the 00:00 case
                                input_csv_data[index - 1][-2] = '03:40:00'
                            elif input_csv_data[index - 1][-2] == '00:00':
                                input_csv_data[index - 1][-2] = '03:00:00'
                            if datetime.datetime.strptime(item[12], '%H:%M:%S') > datetime.datetime.strptime(
                                    input_csv_data[index - 1][-2], '%H:%M:%S'):
                                error_files.append(sub_file)
                                mod_timer = datetime.datetime.strptime(item[12], '%H:%M:%S')
                            else:
                                mod_timer = datetime.datetime.strptime(input_csv_data[index - 1][-2], '%H:%M:%S')
                            response_time = mod_timer - datetime.datetime.strptime(item[12], '%H:%M:%S')
                            response_time_mins = round(response_time.total_seconds() / 60, 2)
                            response_time_secs = response_time.total_seconds()
                        else:  # handle the first record of the file
                            if int(eed_id) in pwd_list:
                                timer = '03:40:00'
                            else:
                                timer = '03:00:00'
                            if datetime.datetime.strptime(item[12], '%H:%M:%S') > datetime.datetime.strptime(timer,'%H:%M:%S'):
                                error_files.append(sub_file)
                            response_time = datetime.datetime.strptime(timer, '%H:%M:%S') - datetime.datetime.strptime(item[12], '%H:%M:%S')
                            response_time_mins = round(response_time.total_seconds() / 60, 2)
                            response_time_secs = response_time.total_seconds()
                        if str(item[2]) not in timer_dict.keys():  # timer dictionary with question and response time
                            timer_dict[unique_key] = [response_time, response_time_mins, response_time_secs]
                        else:
                            timer_dict[unique_key][0] = timer_dict[unique_key][0] + response_time
                            timer_dict[unique_key][1] = timer_dict[unique_key][1] + response_time_mins
                            timer_dict[unique_key][2] = timer_dict[unique_key][2] + response_time_secs

                    if not output_row and item:  # for not attempted single row addition
                        if item[4] == -1:
                            item[0] = item[0].replace('INFO - "', "")
                            item[13] = item[13].replace('"', "")
                            output_row = [client_id, exam_name, eed_id, exam_id, len(res)]
                            item[12] = '23:00:00'
                            output_row.extend(item)
                            output_row.append(Value)
                            output_row.append('Appeared-Not-Attempted')
                            unique_key = str(item[2])
                            unique_questions[unique_key] = output_row
                            key_ques = str(output_row[0]) + "_" + str(output_row[3]) + "_" + str(
                                int(output_row[7]))  # diamond_examid_QuestionID
                            key_master = str(output_row[0]) + "_" + str(output_row[3])
                            if key_ques in question_dict.keys():
                                output_row.append(question_dict[key_ques][-3])
                                output_row.append(question_dict[key_ques][-4])
                                output_row.append(question_dict[key_ques][-2])
                                output_row.append('NA')
                            else:
                                output_row.extend(("nan", "nan", "nan", "nan"))
                            if key_master in master_dict.keys():
                                output_row.append(master_dict[key_master][1])  # zone
                                output_row.append(master_dict[key_master][2])  # state
                                output_row.append(master_dict[key_master][3])  # city
                                output_row.append(master_dict[key_master][4])  # Test Centre ID
                                output_row.append(master_dict[key_master][5])  # venue
                                output_row.append(master_dict[key_master][6])  # reg no.
                                if type(master_dict[key_master][7]) == str:  # exam date
                                    format_date = datetime.datetime.strptime(master_dict[key_master][7], '%d/%m/%Y')
                                    output_row.append(format_date)
                                else:
                                    output_row.append(master_dict[key_master][7])
                                output_row.append(master_dict[key_master][8])  # exam time
                                key_registration = client_id + '_' + master_dict[key_master][6]  # registration key
                                if key_registration in registration_dict.keys():
                                    output_row.append(registration_dict[key_registration][col_headers.index('Gender')])  # Gender
                                    output_row.append(registration_dict[key_registration][col_headers.index('Category(Caste)')])  # Category
                                    if str(registration_dict[key_registration][col_headers.index('Are you differently Abled?')]) != 'nan':
                                        output_row.append("Yes")
                                        output_row.append("30")
                                    else:
                                        output_row.append("No")
                                        output_row.append("0")
                                    output_row.append(registration_dict[key_registration][col_headers.index('Graduation Degree/Diploma Name')])
                                    output_row.append(registration_dict[key_registration][col_headers.index('10th/SSC percentage')])  # SSC
                                    output_row.append(registration_dict[key_registration][col_headers.index('12th/HSC percentage')])  # HSC
                                    output_row.append(registration_dict[key_registration][col_headers.index('Graduation Percentage')])  # grad
                                    output_row.append(registration_dict[key_registration][col_headers.index('Post Applying For:')])  # Post applying
                                else:
                                    output_row.extend(["nan"] * 9)
                            else:
                                output_row.extend(["nan"] * 17)
                    item.clear()
                    # unique_questions = {
                    #     k: v
                    #     for k, v in sorted(unique_questions.items(), key=lambda item: item[1])
                    # }
                    for k in unique_questions.copy():
                        if unique_questions[k][10] == -1.0:  # removing non attempted questions i.e. questions with -1 as final attempt
                            del unique_questions[k]
                        else:
                            if k in timer_dict.keys():
                                unique_questions[k].extend((timer_dict[k][0], timer_dict[k][1], timer_dict[k][2]))
                    sub_file_output_rows = [value for item, value in unique_questions.items()]
                    for val in sub_file_output_rows:
                        response_min_list.append(val[-2])
                        response_sec_list.append(val[-1])

                    response_avg_min = round(mean(response_min_list), 2) if response_min_list else 0.0
                    response_avg_sec = round(mean(response_sec_list), 2) if response_sec_list else 0.0
                    for line in sub_file_output_rows:
                        line.append(response_avg_min)
                        line.append(response_avg_sec)
                    output_rows.extend(sub_file_output_rows)
                    if sub_file_output_rows:
                        count_list.append(1)

                except Exception as ex:
                    traceback.print_exc()
                    print(eed_id)
                    continue
            output_rows.insert(
                0,
                [
                    "clientid",
                    "exam_name",
                    "eed_id",
                    "examid",
                    "PC_change_request",
                    "Timestamp",
                    "Section Name",
                    "QuestionID",
                    "CurrentQuestionNumber",
                    "OptionSelected",
                    "AlternateOptionSelected",
                    "Bookmark",
                    "SectionalQuestionNumber",
                    "IPAddress",
                    "Action",
                    "SequenceNumber",
                    "Candidate MachineDateTime",
                    "Timer",
                    "MacAddress",
                    "Value",
                    "appeared_status",
                    "mdm_name",
                    "crr_exam_batch",
                    "crr_crct_key",
                    "Status",
                    "Zone",
                    "State",
                    "City",
                    "Test Center ID",
                    "Venue Name",
                    "Registration Number",
                    "Exam Date",
                    "Exam Time",
                    "Gender",
                    "Category",
                    "PWD",
                    "PWD Extra Time",
                    "Graduation degree name",
                    "SSC percentage",
                    "HSC percentage",
                    "Graduation percentage",
                    "Candidate Post details",
                    "response_time",
                    "response_time_mins",
                    "response_time_secs",
                    "avg_response_time_mins",
                    "avg_response_time_secs",
                ],
            )

            read_files.append([file])
            with open(
                    output_dir + client_id + '_' + exam_name + "\\fastestfinger\\transformed\\" + rack_name + "\\" + file_name + ".csv",
                    "w",
                    newline="",
            ) as f:
                writer = csv.writer(f)
                writer.writerows(output_rows)

            with open(output_dir + client_id + '_' + exam_name + "\\fastestfinger\\Master.csv", "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(read_files)
        log_message(
            "Processing ended: " + file_path,
            "info",
            "system_log",
        )
        return count_list
    except Exception as err:
        traceback.print_exc()
        with open(output_dir + client_id + '_' + exam_name + "\\fastestfinger\\Exception.csv", "a", newline="") as f:
            f.write(str(err))


def process_aexod_file(file_path, question_dict, master_dict, registration_dict, col_headers, cal, count_list):
    try:

        client_id = file_path.split("\\")[-4].split('_')[0]
        exam_name = file_path.split("\\")[-4].split('_')[1]
        log_message(
            "Current working directory: " + file_path,
            "info",
            "system_log",
        )
        rack_name = file_path.split('\\')[-1]
        Value = 1

        for file in glob.glob(file_path + "\\*"):  # batch wise logs folder
            file_name = file.split("\\")[-1]
            read_files = []
            output_rows = []

            for sub_file in glob.glob(file + "\\*.log"):  # individual candidate log file

                try:
                    unique_questions = {}
                    membership_no = sub_file.split("\\")[-1].split("-")[0]  # SO711311*** (11 alphanumeric)
                    enrollment_id = sub_file.split("\\")[-1].split("-")[1]  # 218132 (6 digit)
                    dataframe = (
                        pd.read_csv(
                            sub_file,
                            delimiter='|', quoting=3,
                            names=[
                                "Timestamp",
                                "Section Name",
                                "QuestionID",
                                "CurrentQuestionNumber",
                                "OptionSelected",
                                "AlternateOptionSelected",
                                "Bookmark",
                                "SectionalQuestionNumber",
                                "IPAddress",
                                "Action",
                                "SequenceNumber",
                                "Candidate MachineDateTime",
                                "Timer"
                            ],
                            low_memory=False,
                        )
                            .reset_index(drop=True)
                    )
                    dataframe[['IPAddress']] = dataframe[['IPAddress']].fillna('0')
                    res = [i for i in list(dataframe['IPAddress']) if 'PC change' in i]

                    dataframe = dataframe.dropna(subset=['Section Name'])
                    input_csv_data = dataframe.values.tolist()
                    item = []
                    output_row = []
                    for index, item in enumerate(input_csv_data):
                        if item[4] != -1 or item[9] == "Clicked on 'Clear Response' button.":
                            item[0] = item[0].replace('INFO - "', "")
                            item[12] = item[12].replace('"', '')
                            if item[12] == '00:00':
                                item[12] = '00:00:00'
                            unique_key = str(item[2])
                            output_row = [client_id, exam_name, enrollment_id, membership_no, len(res)]
                            output_row.extend(item)
                            output_row.append(Value)
                            output_row.append('Appeared-Attempted')
                            unique_questions[unique_key] = output_row
                            key_master = str(client_id) + "_" + str(membership_no)
                        else:
                            continue
                        if key_master in master_dict.keys():
                            # CRR lookup operation
                            date_split = str(master_dict[key_master][7]).split('-')
                            month_name = cal[date_split[1]]
                            exam_date = date_split[-1].split(' ')[0] + month_name
                            key_ques = str(client_id) + "_" + str(membership_no) + "_" + str(int(item[2])) + "_" + exam_date  # diamond_examid_QuestionID_date
                            if key_ques in question_dict.keys():
                                output_row.append(question_dict[key_ques][3])  # Medium Code
                                output_row.append("NA")  # batch time
                                output_row.append(question_dict[key_ques][-2])  # Correct Answer
                                if str(output_row[8]).split('.')[0] in list(str(question_dict[key_ques][-2])) or "8" in list(str(
                                        question_dict[key_ques][-2])):
                                    output_row.append('Correct')
                                elif str(output_row[8]).split('.')[0] not in list(str(
                                        question_dict[key_ques][-2])) or "8" not in list(str(question_dict[key_ques][-2])):
                                    if str(output_row[8]).split('.')[0] == '-1':
                                        output_row.append('Not Attempted')
                                    else:
                                        output_row.append('Incorrect')
                            else:
                                output_row.extend(("NA", "NA", "NA", "NA"))
                            output_row.append(master_dict[key_master][1])  # zone
                            output_row.append(master_dict[key_master][2])
                            output_row.append(master_dict[key_master][3])  # city
                            output_row.append(master_dict[key_master][4])  # test center id/ venue id
                            output_row.append(master_dict[key_master][5])  # venue name
                            output_row.append(master_dict[key_master][6])
                            output_row.append(master_dict[key_master][7])  # exam date
                            output_row.append(master_dict[key_master][8])  # exam time
                            if registration_dict:
                                key_registration = client_id + '_' + master_dict[key_master][6]  # registration key
                                if key_registration in registration_dict.keys():
                                    output_row.append(
                                        registration_dict[key_registration][col_headers.index('Gender')])  # Gender
                                    output_row.append(registration_dict[key_registration][
                                                          col_headers.index('Category(Caste)')])  # Category
                                    if registration_dict[key_registration][
                                        col_headers.index('Are you differently Abled?')] != '':
                                        output_row.append("Yes")
                                        output_row.append(30)
                                    else:
                                        output_row.append("No")
                                        output_row.append(0)
                                    output_row.append(registration_dict[key_registration][
                                                          col_headers.index('Graduation Degree/Diploma Name')])
                                    output_row.append(registration_dict[key_registration][
                                                          col_headers.index('10th/SSC percentage')])  # SSC
                                    output_row.append(registration_dict[key_registration][
                                                          col_headers.index('12th/HSC percentage')])  # HSC
                                    output_row.append(registration_dict[key_registration][
                                                          col_headers.index('Graduation Percentage')])  # grad
                                    output_row.append(registration_dict[key_registration][
                                                          col_headers.index('Post Applying For:')])  # Post applying
                        else:
                            output_row.extend(("NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA"))
                    if not output_row and item:  # for not attempted single row addition
                        if item[4] == -1:
                            item[0] = item[0].replace('INFO - "', "")
                            item[12] = item[12].replace('"', '')
                            output_row = [client_id, exam_name, enrollment_id, membership_no, len(res)]
                            item[12] = '23:00:00'
                            output_row.extend(item)
                            output_row.append(Value)
                            output_row.append('Appeared-Not-Attempted')
                            unique_key = str(item[2])
                            unique_questions[unique_key] = output_row
                            key_master = str(client_id) + "_" + str(membership_no)
                            if key_master in master_dict.keys():
                                # CRR lookup operation
                                date_split = str(master_dict[key_master][7]).split('-')
                                month_name = cal[date_split[1]]
                                exam_date = date_split[-1].split(' ')[0] + month_name
                                key_ques = str(client_id) + "_" + str(membership_no) + "_" + str(int(item[2])) + "_" + exam_date  # diamond_examid_QuestionID_date
                                if key_ques in question_dict.keys():
                                    output_row.append(question_dict[key_ques][3])  # Medium Code
                                    output_row.append("NA")  # batch time
                                    output_row.append(question_dict[key_ques][-1])  # Correct Answer
                                    output_row.append("NA")
                                else:
                                    output_row.extend(("NA", "NA", "NA", "NA"))
                                output_row.append(master_dict[key_master][1])  # zone
                                output_row.append(master_dict[key_master][2])
                                output_row.append(master_dict[key_master][3])  # city
                                output_row.append(master_dict[key_master][4])  # test center id/ venue id
                                output_row.append(master_dict[key_master][5])  # venue name
                                output_row.append(master_dict[key_master][6])
                                output_row.append(master_dict[key_master][7])  # exam date
                                output_row.append(master_dict[key_master][8])  # exam time
                                if registration_dict:
                                    key_registration = client_id + '_' + master_dict[key_master][6]  # registration key
                                    if key_registration in registration_dict.keys():
                                        output_row.append(
                                            registration_dict[key_registration][col_headers.index('Gender')])  # Gender
                                        output_row.append(registration_dict[key_registration][
                                                              col_headers.index('Category(Caste)')])  # Category
                                        if registration_dict[key_registration][
                                            col_headers.index('Are you differently Abled?')] != '':
                                            output_row.append("Yes")
                                            output_row.append(30)
                                        else:
                                            output_row.append("No")
                                            output_row.append(0)
                                        output_row.append(registration_dict[key_registration][
                                                              col_headers.index('Graduation Degree/Diploma Name')])
                                        output_row.append(registration_dict[key_registration][
                                                              col_headers.index('10th/SSC percentage')])  # SSC
                                        output_row.append(registration_dict[key_registration][
                                                              col_headers.index('12th/HSC percentage')])  # HSC
                                        output_row.append(registration_dict[key_registration][
                                                              col_headers.index('Graduation Percentage')])  # grad
                                        output_row.append(registration_dict[key_registration][
                                                              col_headers.index('Post Applying For:')])  # Post applying
                    item.clear()
                    unique_questions = {
                        k: v
                        for k, v in sorted(unique_questions.items(), key=lambda item: item[1])
                    }
                    sub_file_output_rows = [value for item, value in unique_questions.items()]
                    output_rows.extend(sub_file_output_rows)
                    if sub_file_output_rows:
                        count_list.append(1)

                except Exception as ex:
                    traceback.print_exc()
                    print(sub_file)
                    continue
            output_rows.insert(
                0,
                [
                    "clientid",
                    "exam_name",
                    "eed_id",
                    "examid",
                    "PC_change_request",
                    "Timestamp",
                    "Section Name",
                    "QuestionID",
                    "CurrentQuestionNumber",
                    "OptionSelected",
                    "AlternateOptionSelected",
                    "Bookmark",
                    "SectionalQuestionNumber",
                    "IPAddress",
                    "Action",
                    "SequenceNumber",
                    "Candidate MachineDateTime",
                    "Timer",
                    "Value",
                    "appeared_status",
                    "mdm_name",
                    "crr_exam_batch",
                    "crr_crct_key",
                    "Status",
                    "Zone",
                    "State",
                    "City",
                    "Test Center ID",
                    "Venue Name",
                    "Registration Number",
                    "Exam Date",
                    "Exam Time",
                    "Gender",
                    "Category",
                    "PWD",
                    "PWD Extra Time",
                    "Graduation degree name",
                    "SSC percentage",
                    "HSC percentage",
                    "Graduation percentage",
                    "Candidate Post details",
                ],
            )

            with open(
                    output_dir + client_id + '_' + exam_name + "\\fastestfinger\\transformed\\" + rack_name + "\\" + file_name + ".csv",
                    "w",
                    newline="",
            ) as f:
                writer = csv.writer(f)
                writer.writerows(output_rows)

            with open(output_dir + client_id + '_' + exam_name + "\\fastestfinger\\Master.csv", "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(read_files)
        log_message(
            "Processing ended: " + file_path,
            "info",
            "system_log",
        )
        return count_list
    except Exception as err:
        traceback.print_exc()
        with open(output_dir + client_id + '_' + exam_name + "\\fastestfinger\\Exception.csv", "a", newline="") as f:
            f.write(str(err))


def create_output_files(client_name_value, rack_name_value, exam_name_value):
    if not os.path.exists(
            output_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\transformed\\" + rack_name_value):
        os.mkdir(
            output_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\transformed\\" + rack_name_value)
    if not os.path.exists(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Master.csv"):
        with open(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Master.csv", "w",
                  newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
    if not os.path.exists(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Exception.csv"):
        with open(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Exception.csv", "w",
                  newline="") as f:
            writer = csv.writer(f)
            writer.writerow(exception_column_list)


def crr_dict(c_name, e_name, mapping_dict_crr):
    question_set = glob.glob(base_dir + c_name + '_' + e_name + '\\fastestfinger\\CRR\\*.csv')
    question_dataframe = pd.DataFrame()
    with open(question_set[0]) as one:
        dialect = csv.Sniffer().sniff(one.readline(), [',', ';'])
        df_sep = dialect.delimiter
    for question_template in question_set:
        e_date = question_template.split('\\')[-1].split('.')[0].split('_')[-1]
        question_partial = pd.read_csv(
            question_template,
            delimiter=df_sep,
        ).reset_index(drop=True)
        question_partial = question_partial.assign(file_date=e_date)
        question_dataframe = question_dataframe.append(question_partial)
        # question_dataframe = pd.concat([question_dataframe, question_partial])
    question_dataframe.rename(columns=mapping_dict_crr, inplace=True)
    # question_dataframe = question_dataframe[['eed_id', 'exam_id', 'registration_no', 'question_no', 'exam_batch', 'mdm_name', 'crr_crct_key', 'file_date' ]]
    header_list = question_dataframe.columns.tolist()
    # question_csv_data = question_dataframe.values.tolist()
    return question_dataframe, header_list


def main(list_of_folders):  # list of clients_exams
    start_time = datetime.datetime.now()
    for folder in list_of_folders:
        list_of_files = glob.glob(folder + '\\' + input_dir + "*")
        client_name = folder.split("\\")[-1].split('_')[0]
        exam_name = folder.split("\\")[-1].split('_')[1]
        files = [file for file in list_of_files if "Master" not in file and "Exception" not in file]
        for i in list_of_files:
            diamond_rack_name = i.split('\\')[-1]
            create_output_files(client_name, diamond_rack_name, exam_name)

        calendar_dict = {str(index).zfill(2): month.upper() for index, month in enumerate(calendar.month_abbr) if month}

        mapping_dict_crr = {}
        with open(
                base_dir + client_name + '_' + exam_name + '\\fastestfinger\\mapping_file\\mapping_cols.csv') as map_cols:
            reader = csv.reader(map_cols)
            next(reader)
            for line in reader:
                mapping_dict_crr[line[0]] = line[1]

        mapping_dict_reg = {}
        with open(
                base_dir + client_name + '_' + exam_name + '\\registration\\mapping_file\\mapping_cols.csv') as map_cols:
            reader = csv.reader(map_cols)
            next(reader)
            for line in reader:
                line[3] = line[3].lower()
                if line[2] == exam_name and line[5] == client_name and line[3] == 'yes':
                    mapping_dict_reg[line[0]] = line[1]

        master_set = glob.glob(base_dir + client_name + '_' + exam_name + '\\fastestfinger\\Master\\*.xlsx')
        registration_set = glob.glob(base_dir + client_name + '_' + exam_name + '\\registration\\raw\\*\\*.xlsx')

        master_dataframe = pd.DataFrame(
            columns=default_cols)
        registration_dataframe = pd.DataFrame()

        #  master data lookup
        final_master_dict = {}
        for master_template in master_set:
            master_partial = pd.read_excel(
                master_template,
                usecols=default_cols, engine='openpyxl',
            ).reset_index(drop=True)
            master_dataframe = master_dataframe.append(master_partial)
        master_dataframe = master_dataframe.fillna('')
        master_csv_data = master_dataframe.values.tolist()
        master_temp = [final_master_dict.update({client_name + "_" + str(item[0]): item}) for item in
                       master_csv_data]
        del master_dataframe, master_csv_data, master_temp,

        # Registration lookup
        final_registration_dict = {}
        reg_headers = []
        if registration_set:
            for registration_template in registration_set:
                registration_partial = pd.read_excel(registration_template,engine='openpyxl',
                                                     ).reset_index(drop=True)
                registration_dataframe = registration_dataframe.append(registration_partial)
            # registration_dataframe = registration_dataframe.fillna('')
            registration_dataframe.update(registration_dataframe.select_dtypes(include=[np.number]).fillna(0))
            registration_dataframe.rename(columns=mapping_dict_reg, inplace=True)
            reg_headers = registration_dataframe.columns.tolist()
            registration_csv_data = registration_dataframe.values.tolist()
            reg_idx = registration_dataframe.columns.get_loc('Registration Number')
            reg_temp = [final_registration_dict.update({client_name + "_" + str(item[reg_idx]): item}) for item in
                        registration_csv_data]
            del reg_temp, registration_dataframe, registration_partial, registration_csv_data,

        # CRR lookup
        crr_dataframe, crr_headers = crr_dict(client_name, exam_name, mapping_dict_crr)
        final_question_dict = {}
        count_list = []
        final_count_list = []
        if len(crr_headers) > 10:
            crr_dataframe = crr_dataframe[
                ['eed_id', 'exam_id', 'registration_no', 'question_no', 'exam_batch', 'mdm_name', 'crr_crct_key',
                 'file_date']]
            crr_data_list = crr_dataframe.values.tolist()
            eed_idx = crr_dataframe.columns.get_loc('eed_id')
            examid_idx = crr_dataframe.columns.get_loc('exam_id')
            qst_idx = crr_dataframe.columns.get_loc('question_no')
            crr_temp = [final_question_dict.update(
                {client_name + '_' + str(item[examid_idx]) + "_" + str(item[eed_idx]) + "_" + str(item[qst_idx]): item}) for
                item in crr_data_list]  # diamond_examid_eedid_qstno
            del crr_data_list, crr_dataframe, crr_temp
            # with Pool(2) as pool:
            #     p = pool.starmap(process_file, zip(files, repeat(final_question_dict), repeat(final_master_dict), repeat(final_registration_dict), repeat(reg_headers),  repeat(count_list)))
            #     final_count_list.extend(p)
            #     pool.close()
            #     pool.terminate()
            #     pool.join()
            # flat_list = [item for sublist in final_count_list for item in sublist]
            # print(flat_list.count(1))
            for x in files:
                p = process_exod_file(x, final_question_dict, final_master_dict, final_registration_dict, reg_headers,
                                 count_list)
        else:
            crr_data_list = crr_dataframe.values.tolist()
            examid_idx = crr_dataframe.columns.get_loc('exam_id')
            qst_idx = crr_dataframe.columns.get_loc('question_no')
            date_idx = crr_dataframe.columns.get_loc('file_date')
            crr_temp = [final_question_dict.update(
                {client_name + '_' + str(item[examid_idx]) + "_" + str(item[qst_idx]) + '_' + str(item[date_idx]).upper(): item}) for
                item in crr_data_list]  # diamond_examid_qstno
            del crr_data_list, crr_dataframe, crr_temp
            # with Pool(2) as pool:
            #     p = pool.starmap(process_file, zip(files, repeat(final_question_dict), repeat(final_master_dict), repeat(final_registration_dict), repeat(reg_headers), repeat(calendar_dict), repeat(count_list)))
            #     final_count_list.extend(p)
            #     pool.close()
            #     pool.terminate()
            #     pool.join()
            # flat_list = [item for sublist in final_count_list for item in sublist]
            # print(flat_list.count(1))
            for x in files:
                p = process_aexod_file(x, final_question_dict, final_master_dict, final_registration_dict, reg_headers, calendar_dict,
                                 count_list)

    log_message(
        "Fastest Finger processing. Time taken: " + str(datetime.datetime.now() - start_time),
        "info",
        "system_log", )


def move_to_archive(list_of_folders):
    for folder in list_of_folders:
        for src in glob.glob(folder + '\\raw\\*'):
            dest = folder + '\\archived\\'
            shutil.move(src, dest)
        for src in glob.glob(folder + '\\CRR\\*'):
            dest = folder + '\\archived\\CRR\\'
            shutil.move(src, dest)
        for src in glob.glob(folder + '\\Master\\*'):
            dest = folder + '\\archived\\Master\\'
            shutil.move(src, dest)


folders = glob.glob(base_dir + '\\Konark_cgl')  # [!master]*
main(folders)
strt_time = datetime.datetime.now()
log_message(
    "Moving to Archived Directory ", "info", "system_log", )
# move_to_archive(folders)
log_message(
    "File moved successfully. Time taken: " + str(datetime.datetime.now() - strt_time),
    "info",
    "system_log", )
