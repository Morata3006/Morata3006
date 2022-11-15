import csv
import datetime
import glob
import os
import shutil
import traceback
from itertools import repeat
from multiprocessing import Pool
import pandas as pd
# from memory_profiler import profile
import logging
import json

default_cols = ["Candidate ID", "Zone", "State", "City", "Test Center ID", "Venue Name", "Registration No.",
                "Exam Date", "Exam Time",]

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


def process_file(file_path, question_dict, master_dict, registration_dict, count_list):
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
                        if item[4] != -1:
                            item[0] = item[0].replace('INFO - "', "")
                            item[13] = item[13].replace('"', "")
                            unique_key = str(item[2])
                            output_row = [client_id, eed_id, exam_id, len(res)]
                            output_row.extend(item)
                            output_row.append(Value)
                            output_row.append('Appeared-Attempted')
                            unique_questions[unique_key] = output_row
                            key_ques = str(output_row[0]) + "_" + str(output_row[2]) + "_" + str(
                                int(output_row[6]))  # diamond_examid_QuestionID
                            key_master = str(output_row[0]) + "_" + str(output_row[2])  # diamond_exam_id
                        else:
                            continue
                        if key_ques in question_dict.keys():
                            output_row.append(question_dict[key_ques][2])
                            output_row.append(question_dict[key_ques][3])
                            output_row.append(question_dict[key_ques][-1])
                        else:
                            output_row.extend(("", "", ""))
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
                                output_row.append(registration_dict[key_registration][19])  # Gender
                                output_row.append(registration_dict[key_registration][25])  # Category
                                if registration_dict[key_registration][13] != '':
                                    output_row.append("Yes")
                                else:
                                    output_row.append("No")
                                output_row.append(registration_dict[key_registration][72])  # SSC
                                output_row.append(registration_dict[key_registration][77])  # HSC
                                output_row.append(registration_dict[key_registration][121])  # grad
                                output_row.append(registration_dict[key_registration][3])  # Post applying
                    if not output_row and item:  # for not attempted single row addition
                        if item[4] == -1:
                            item[0] = item[0].replace('INFO - "', "")
                            item[13] = item[13].replace('"', "")
                            output_row = [client_id, eed_id, exam_id, len(res)]
                            item[12] = '23:00:00'
                            output_row.extend(item)
                            output_row.append(Value)
                            output_row.append('Appeared-Not-Attempted')
                            unique_key = str(item[2])
                            unique_questions[unique_key] = output_row
                            key_ques = str(output_row[0]) + "_" + str(output_row[2]) + "_" + str(
                                int(output_row[6]))  # diamond_examid_QuestionID
                            key_master = str(output_row[0]) + "_" + str(output_row[2])
                            if key_ques in question_dict.keys():
                                output_row.append(question_dict[key_ques][2])
                                output_row.append(question_dict[key_ques][3])
                                output_row.append(question_dict[key_ques][-1])
                            else:
                                output_row.extend(("", "", ""))
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
                                    output_row.append(registration_dict[key_registration][19])  # Gender
                                    output_row.append(registration_dict[key_registration][25])  # Category
                                    if registration_dict[key_registration][13] != '':
                                        output_row.append("Yes")
                                    else:
                                        output_row.append("No")
                                    output_row.append(registration_dict[key_registration][72])  # SSC
                                    output_row.append(registration_dict[key_registration][77])  # HSC
                                    output_row.append(registration_dict[key_registration][121])  # grad
                                    output_row.append(registration_dict[key_registration][3])  # Post applying
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
                    print(ex)
                    continue
            output_headers = [
                    "clientid",
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
                    "status",
                    "mdm_name",
                    "crr_exam_batch",
                    "crr_crct_key",
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
                    # "PWD Extra Time",
                    # "Total Questions",
                    # "Non-Attempted Questions",
                    # "Graduation degree name",
                    "SSC percentage",
                    "HSC percentage",
                    "Graduation percentage",
                    "Candidate Post details",
                ]

            read_files.append([file])
            lst = []
            for row in output_rows:
                lst.append(dict(zip(output_headers, row)))

            for item in lst:
                try:
                    item["SequenceNumber"] = str(item["SequenceNumber"])
                    item["AlternateOptionSelected"] = str(item["AlternateOptionSelected"])
                    item["Bookmark"] = str(item["Bookmark"])
                    item["PWD Extra Time"] = str(item["PWD Extra Time"])
                    item["Test Center ID"] = str(item["Test Center ID"])
                except KeyError:
                    pass

            json_object = json.dumps(lst, default=str).strip('[]').replace('}, ', '}\n')

            with open(output_dir + client_id + '_' + exam_name + "\\fastestfinger\\transformed_json\\" + rack_name + "\\" + file_name + ".json", "w",
                      newline="") as outfile:
                outfile.write(json_object)

            # with open(
            #         output_dir + client_id + '_' + exam_name + "\\fastestfinger\\transformed\\" + rack_name + "\\" + file_name + ".csv",
            #         "w",
            #         newline="",
            # ) as f:
            #     writer = csv.writer(f)
            #     writer.writerows(output_rows)

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
    if not os.path.exists(output_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\transformed_json\\" + rack_name_value):
        os.mkdir(output_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\transformed_json\\" + rack_name_value)
    if not os.path.exists(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Master.csv"):
        with open(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Master.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
    if not os.path.exists(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Exception.csv"):
        with open(base_dir + client_name_value + '_' + exam_name_value + "\\fastestfinger\\Exception.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(exception_column_list)


def main(list_of_folders):
    start_time = datetime.datetime.now()
    for folder in list_of_folders:
        list_of_files = glob.glob(folder + '\\' + input_dir + "*")
        client_name = folder.split("\\")[-1].split('_')[0]
        exam_name = folder.split("\\")[-1].split('_')[1]
        files = [file for file in list_of_files if "Master" not in file and "Exception" not in file]
        for i in list_of_files:
            diamond_rack_name = i.split('\\')[-1]
            create_output_files(client_name, diamond_rack_name, exam_name)
        question_set = glob.glob(base_dir + client_name + '_' + exam_name + '\\fastestfinger\\CRR\\*.csv')
        master_set = glob.glob(base_dir + client_name + '_' + exam_name + '\\fastestfinger\\Master\\*.xlsx')

        registration_dataframe = pd.read_csv(base_dir + '\\master_cols.csv', encoding='cp1252')
        registration_dataframe['Is present'] = registration_dataframe['Is present'].str.lower()
        headers = registration_dataframe['client_cols'].loc[
            (registration_dataframe['Is present'] == 'yes') & (registration_dataframe['client_name'] == client_name) & (
                    registration_dataframe['exam_name'] == exam_name)].values.tolist()
        registration_set = glob.glob(base_dir + client_name + '_' + exam_name + '\\registration\\raw\\*\\*.xlsx')

        question_dataframe = pd.DataFrame(
            columns=["crr_eed_id", "eed_exm_id", "mdm_name",
                     "crr_exam_batch", "crr_qst_no", "crr_crct_key"])
        master_dataframe = pd.DataFrame(
            columns=default_cols)
        registration_dataframe = pd.DataFrame(
            columns=headers)

        #  question paper lookup
        for question_template in question_set:
            question_partial = pd.read_csv(
                question_template,
                delimiter=";",
                usecols=[
                    "crr_eed_id", "eed_exm_id", "mdm_name",
                    "crr_exam_batch", "crr_qst_no", "crr_crct_key"
                ],
                low_memory=False,
            ).reset_index(drop=True)
            question_dataframe = question_dataframe.append(question_partial)
        question_csv_data = question_dataframe.values.tolist()
        final_question_dict = {}
        question_temp = [final_question_dict.update({client_name + "_" + str(item[1]) + "_" + str(item[-2]): item}) for
                         item in question_csv_data]  # diamond_eedid_qstno

        #  master data lookup
        for master_template in master_set:
            master_partial = pd.read_excel(
                master_template,
                usecols=default_cols,
            ).reset_index(drop=True)
            master_dataframe = master_dataframe.append(master_partial)
        master_dataframe = master_dataframe.fillna('')
        master_csv_data = master_dataframe.values.tolist()
        final_master_dict = {}
        master_temp = [final_master_dict.update({client_name + "_" + str(item[0]): item}) for item in
                       master_csv_data]

        for registration_template in registration_set:
            registration_partial = pd.read_excel(registration_template,
                                                 usecols=headers).reset_index(drop=True)
            registration_dataframe = registration_dataframe.append(registration_partial)
        registration_dataframe = registration_dataframe.fillna('')
        registration_csv_data = registration_dataframe.values.tolist()
        final_registration_dict = {}
        reg_temp = [final_registration_dict.update({client_name + "_" + str(item[1]): item}) for item in
                       registration_csv_data]

        del question_dataframe, master_dataframe, question_partial, question_csv_data, master_csv_data, question_temp, master_temp
        reg_temp, registration_dataframe, registration_partial, registration_csv_data
        count_list = []
        final_count_list = []
        # with Pool(2) as pool:
        #     p = pool.starmap(process_file, zip(files, repeat(final_question_dict), repeat(final_master_dict), repeat(count_list)))
        #     final_count_list.extend(p)
        #     pool.close()
        #     pool.terminate()
        #     pool.join()
        # flat_list = [item for sublist in final_count_list for item in sublist]
        # print(flat_list.count(1))
        for x in files:
            p = process_file(x, final_question_dict, final_master_dict, final_registration_dict, count_list)
            # print(p)
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


folders = glob.glob(base_dir + '\\PCGT_Tango')
main(folders)
strt_time = datetime.datetime.now()
log_message(
    "Moving to Archived Directory ", "info", "system_log", )
# move_to_archive(folders)
log_message(
    "File moved successfully. Time taken: " + str(datetime.datetime.now() - strt_time),
    "info",
    "system_log", )
