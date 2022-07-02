import csv
import datetime
import glob
import os
import traceback
from itertools import repeat
from multiprocessing import Pool
import pandas as pd
# from memory_profiler import profile
import logging

# base_dir = "/data/Fastest_finger/"
# input_dir = "Diamond/raw/CRL/"
# output_dir = "/data/Fastest_finger/Diamond/transformed/"
base_dir = "C:\\Users\\ashetty\\Desktop\\FastestFinger\\"
input_dir = "*\\raw\\"
output_dir = "C:\\Users\\ashetty\\Desktop\\FastestFinger\\"

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


def process_file(file_path, question_dict, master_dict, count_list):
    try:
        Master_files = pd.read_csv(output_dir + "Master.csv")
        Master_list = Master_files['filename'].tolist()

        client_id = "Diamond"  # file_path.split("/")[-1].split("_")[0].lower()
        log_message(
            "Current working directory: " + file_path,
            "info",
            "system_log",
        )
        rack_name = file_path.split('\\')[-1]
        Value = 1

        for file in glob.glob(file_path + "/*"):  # batch wise logs folder
            if file not in Master_list:
                # file_name = file.split("/")[-1]
                file_name = file.split("\\")[-1]
                read_files = []
                exception_rows = []
                output_rows = []

                for sub_file in glob.glob(file + "/*"):  # individual candidate log file

                    try:
                        # if os.path.getsize(sub_file) > 5000:
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
                        dataframe = dataframe.dropna()
                        input_csv_data = dataframe.values.tolist()
                        output_row = []
                        # item = []
                        for index, item in enumerate(input_csv_data):
                            if item[4] != -1:
                                item[0] = item[0].replace('INFO - "', "")
                                item[13] = item[13].replace('"', "")
                                unique_key = str(item[2])
                                output_row = [client_id, eed_id, exam_id]
                                output_row.extend(item)
                                output_row.append(Value)
                                output_row.append('Appeared-Attempted')
                                unique_questions[unique_key] = output_row
                                key_ques = str(output_row[0]) + "_" + str(output_row[2]) + "_" + str(
                                    int(output_row[5]))  # diamond_examid_QuestionID
                                key_master = str(output_row[0]) + "_" + str(output_row[2])
                            else:
                                continue
                            if key_ques in question_dict.keys():
                                output_row.append(question_dict[key_ques][2])
                                output_row.append(question_dict[key_ques][3])
                                output_row.append(question_dict[key_ques][-1])
                            else:
                                output_row.extend(("", "", ""))
                            if key_master in master_dict.keys():
                                # log_file_name = exam_id + "-" + eed_id
                                # output_row.append(log_file_name)
                                output_row.append(master_dict[key_master][1])
                                output_row.append(master_dict[key_master][2])
                                output_row.append(master_dict[key_master][3])
                                output_row.append(master_dict[key_master][4])
                                output_row.append(master_dict[key_master][5])
                                output_row.append(master_dict[key_master][6])
                                output_row.append(master_dict[key_master][7])
                                output_row.append(master_dict[key_master][8])
                                output_row.append(master_dict[key_master][9])
                                output_row.append(master_dict[key_master][10])
                                output_row.append(master_dict[key_master][11])
                                output_row.append(master_dict[key_master][12])  # PWD Time
                                output_row.append(master_dict[key_master][13])
                                output_row.append(master_dict[key_master][14])
                                output_row.append(master_dict[key_master][15])
                                output_row.append(master_dict[key_master][16])
                                output_row.append(master_dict[key_master][17])
                                output_row.append(master_dict[key_master][18])
                                output_row.append(master_dict[key_master][-1])
                        if not output_row and item:  # for not attempted single row addition
                            if item[4] == -1:
                                item[0] = item[0].replace('INFO - "', "")
                                item[13] = item[13].replace('"', "")
                                output_row = [client_id, eed_id, exam_id]
                                item[12] = '23:00:00'
                                output_row.extend(item)
                                output_row.append(Value)
                                output_row.append('Appeared-Not-Attempted')
                                unique_key = str(item[2])
                                unique_questions[unique_key] = output_row
                                key_ques = str(output_row[0]) + "_" + str(output_row[2]) + "_" + str(
                                    int(output_row[5]))  # diamond_examid_QuestionID
                                key_master = str(output_row[0]) + "_" + str(output_row[2])
                                if key_ques in question_dict.keys():
                                    output_row.append(question_dict[key_ques][2])
                                    output_row.append(question_dict[key_ques][3])
                                    output_row.append(question_dict[key_ques][-1])
                                else:
                                    output_row.extend(("", "", ""))
                                if key_master in master_dict.keys():
                                    # log_file_name = exam_id + "-" + eed_id
                                    # output_row.append(log_file_name)
                                    output_row.append(master_dict[key_master][1])
                                    output_row.append(master_dict[key_master][2])
                                    output_row.append(master_dict[key_master][3])
                                    output_row.append(master_dict[key_master][4])
                                    output_row.append(master_dict[key_master][5])
                                    output_row.append(master_dict[key_master][6])
                                    output_row.append(master_dict[key_master][7])
                                    output_row.append(master_dict[key_master][8])
                                    output_row.append(master_dict[key_master][9])
                                    output_row.append(master_dict[key_master][10])
                                    output_row.append(master_dict[key_master][11])
                                    output_row.append(master_dict[key_master][12])
                                    output_row.append(master_dict[key_master][13])
                                    output_row.append(master_dict[key_master][14])
                                    output_row.append(master_dict[key_master][15])  # Graduation Degree
                                    output_row.append(master_dict[key_master][16])
                                    output_row.append(master_dict[key_master][17])
                                    output_row.append(master_dict[key_master][18])  # Graduation Percentage
                                    output_row.append(master_dict[key_master][-1])
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
                        print(ex)
                        continue
                # output_rows.insert(
                #     0,
                #     [
                #         "clientid",
                #         "eed_id",
                #         "examid",
                #         "Timestamp",
                #         "Section Name",
                #         "QuestionID",
                #         "CurrentQuestionNumber",
                #         "OptionSelected",
                #         "AlternateOptionSelected",
                #         "Bookmark",
                #         "SectionalQuestionNumber",
                #         "IPAddress",
                #         "Action",
                #         "SequenceNumber",
                #         "Candidate MachineDateTime",
                #         "Timer",
                #         "MacAddress",
                #         "Value",
                #         "status",
                #         "mdm_name",
                #         "crr_exam_batch",
                #         "crr_crct_key",
                #         # "Candidate Identification",
                #         "Zone",
                #         "State",
                #         "City",
                #         "Test Center ID",
                #         "Venue Name",
                #         "Registration Number"
                #         "Gender",
                #         "Exam Date",
                #         "Exam Time",
                #         "Category",
                #         "PWD",
                #         "PWD Extra Time"
                #     ],
                # )

                read_files.append([file])
                with open(
                        output_dir + client_id + "_fastest_finger/" + rack_name + "/" + file_name + ".csv",
                        "w",
                        newline="",
                ) as f:
                    writer = csv.writer(f)
                    writer.writerows(output_rows)

                if os.path.isfile(base_dir + input_dir + "Master.csv"):
                    with open(output_dir + "Master.csv", "a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerows(read_files)
                if os.path.isfile(output_dir + "Exception.csv"):
                    with open(output_dir + "Exception.csv", "a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerows(exception_rows)
                else:
                    with open(output_dir + "Exception.csv", "w", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(exception_column_list)
                        writer.writerows(exception_rows)
            else:
                log_message(
                    "Already in Master",
                    "info",
                    "system_log",
                )

        log_message(
            "Processing ended: " + file_path,
            "info",
            "system_log",
        )
        return count_list
    except Exception as err:
        traceback.print_exc()
        print("error")


def create_output_files(client_value, rack_value):
    if not os.path.exists(output_dir + client_value + "_fastest_finger/" + rack_value):
        os.mkdir(output_dir + client_value + "_fastest_finger/" + rack_value)
    if not os.path.exists(output_dir + "Master.csv"):
        with open(output_dir + "Master.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    list_of_files = glob.glob(base_dir + input_dir + "*")
    files = [file for file in list_of_files if "Master" not in file and "Exception" not in file]
    for i in list_of_files:
        diamond_client = i.split("\\")[-3]
        diamond_rack_name = i.split('\\')[-1]
        create_output_files(diamond_client, diamond_rack_name)
    question_set = glob.glob(base_dir + diamond_client)
    master_template = "C:/Users/ashetty/Desktop/DeX/Diamond/sample_master.xlsx"
    question_dataframe = pd.DataFrame(
        columns=["crr_eed_id", "eed_exm_id", "mdm_name",
                "crr_exam_batch", "crr_qst_no","crr_crct_key"])

    #  question paper lookup
    for question_template in question_set:
        question_partial = pd.read_csv(
            question_template,
            delimiter=";",
            usecols=[
                "crr_eed_id", "eed_exm_id", "mdm_name",
                "crr_exam_batch", "crr_qst_no","crr_crct_key"
            ],
            low_memory=False,
        ).reset_index(drop=True)
        question_dataframe = question_dataframe.append(question_partial)
    question_csv_data = question_dataframe.values.tolist()
    final_question_dict = {}
    question_temp = [final_question_dict.update({diamond_client + "_" + str(item[1]) + "_" + str(item[-2]): item}) for
                     item in question_csv_data]  # diamond_eedid_qstno

    #  master data lookup
    master_dataframe = pd.read_excel(
        master_template,
        usecols=["Candidate ID", "Zone", "State", "City", "Test Center ID", "Venue Name", "Registration No.",
                 "Gender",
                 "Exam Date", "Exam Time", "Category", "PWD", "PWD Extra Time", "Total Questions", " Non-Attempted Questions",
                 "Graduation degree name", "SSC percentage", "HSC percentage", "Graduation percentage", "Candidate Post details",],
    ).reset_index(drop=True)
    master_dataframe = master_dataframe.fillna('')
    master_csv_data = master_dataframe.values.tolist()
    final_master_dict = {}
    master_temp = [final_master_dict.update({diamond_client + "_" + str(item[0]): item}) for item in
                   master_csv_data]
    del question_dataframe, master_dataframe, question_partial, question_csv_data, master_csv_data, question_temp, master_temp
    # files = []
    # flg = 0
    # for idx in range(len(list_of_files)):
    #     flg += 1
    #     files.append(list_of_files[idx])
    #     if 4 < flg <= len(list_of_files):
    #         with Pool(5) as pool:
    #             pool.starmap(process_file, zip(files, repeat(final_question_dict), repeat(final_master_dict)))
    #             pool.close()
    #             pool.terminate()
    #             pool.join()
    #         files = []
    #         flg = 0
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
    process_file(files[0], final_question_dict, final_master_dict, count_list)
    log_message(
        "time taken: " + str(datetime.datetime.now() - start_time),
        "info",
        "system_log",
    )
