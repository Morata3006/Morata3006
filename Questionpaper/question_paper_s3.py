import datetime
import glob
import io
import logging

import pandas as pd
import csv
import os
import traceback
import boto3

resource = boto3.resource(
        's3',
        aws_access_key_id = 'AKIA3LIJCIOGB62OOBQP',
        aws_secret_access_key = '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',
        region_name = 'ap-south-1'
    )
bucket = resource.Bucket('exam-data')  # bucket name

base_dir = "C:\\Users\\ashetty\\Desktop\\S3_instance\\"
input_dir = "fastestfinger\\CRR\\"
output_dir = "\\questionpaper\\"
log_path = "./logs/"
exception_column_list = [
    "file_name",
    "row_number",
    "client_id",
    "crr_eed_id",
    "crr_reg_no",
    "eed_exm_id",
    "eed_client_roll_no",
    "eed_mdm_id",
    "mdm_name",
    "eed_tcm_id",
    "tcm_name",
    "crr_exam_date",
    "crr_exam_batch",
    "qtp_sec_seqno",
    "Section Name",
    "crr_qst_no",
    "candidate_seq_no",
    "marks",
    "negative_marks",
    "crr_answer",
    "crr_crct_id",
    "crr_crct_key",
    "error",
]
master_column_list = ["filename"]


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


def process_file(file_name):
    try:
        log_message(
            "Current Processing: " + file_name.key,
            "info",
            "system_log",
        )
        read_files = []
        exception_rows = []
        output_rows = []
        output_name = file_name.key.split('/')[-1].split(".")[0]
        extension = file_name.key.split("/")[-1].split(".")[-1]
        client_id = file_name.key.split("/")[-4].split("_")[0]
        exam_name = file_name.key.split("/")[-4].split("_")[1]
        data = file_name.get()['Body'].read()
        str_data = data.decode()
        if extension == "xlsx":
            dataframe = pd.read_excel(io.BytesIO(data))
        elif extension == "csv":
            if '";"' in str_data:
                dataframe = pd.read_csv(io.BytesIO(data), delimiter=';', low_memory=False).reset_index(drop=True)
            elif '","' in str_data:
                dataframe = pd.read_csv(io.BytesIO(data), delimiter=',', low_memory=False).reset_index(drop=True)
        else:
            print("invalid file")
            return
        input_csv_data = [dataframe.columns.tolist()]
        values = dataframe.values.tolist()
        input_csv_data.extend(values)
        for index, item in enumerate(input_csv_data):
            if "nan" in str(item):
                file_name_list = [file_name, index + 1]
                file_name_list.extend(item)
                file_name_list.append("NaN value found")
                exception_rows.append(file_name_list)
            else:
                if index == 0:
                    # item[12] = "crr_qst_no"
                    item.insert(0, "exam_name")
                    item.insert(0, "clientid")
                    item.append("status")
                    output_rows.append(item)
                    continue
                output_row = [client_id, exam_name]
                output_row.extend(item)
                if len(item) > 9:
                    if str(item[-4]) in list(item[-1]) or "8" in list(item[-1]):
                        output_row.append('Correct')
                    elif str(item[-4]) not in list(item[-1]) or '8' not in list(item[-1]):
                        if str(item[-4]) == '-1':
                            output_row.append('Not Attempted')
                        else:
                            output_row.append('Incorrect')
                    else:
                        output_row.append('NA')
                    output_rows.append(output_row)
                else:
                    if str(item[-2]) in list(str(item[-1])) or "8" in list(str(item[-1])):
                        output_row.append('Correct')
                    elif str(item[-2]) not in list(str(item[-1])) or '8' not in list(str(item[-1])):
                        if str(item[-2]) == '-1':
                            output_row.append('Not Attempted')
                        else:
                            output_row.append('Incorrect')
                    else:
                        output_row.append('NA')
                    output_rows.append(output_row)
        read_files.append([file_name])
        with open(base_dir + client_id + '_' + exam_name + output_dir + 'transformed\\' + output_name + '.csv', "w", newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerows(output_rows)
        with open(base_dir + client_id + '_' + exam_name + output_dir + "Master.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(read_files)
        log_message(
            "Processing Completed: " + file_name.key,
            "info",
            "system_log",
        )
    except Exception as err:
        traceback.print_exc()
        with open(base_dir + client_id + '_' + exam_name + output_dir + "Exception.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(exception_rows)
            f.write(str(err))


def create_output_files(client_name_value, exam_name_value):
    if not os.path.exists(
            base_dir + client_name_value + '_' + exam_name_value + output_dir + "transformed"):
        os.mkdir(
            base_dir + client_name_value + '_' + exam_name_value + output_dir + "transformed")
    if not os.path.exists(base_dir + client_name_value + '_' + exam_name_value + output_dir + "Master.csv"):
        with open(base_dir + client_name_value + '_' + exam_name_value + output_dir + "Master.csv", "w",
                  newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
    if not os.path.exists(base_dir + client_name_value + '_' + exam_name_value + output_dir + "Exception.csv"):
        with open(base_dir + client_name_value + '_' + exam_name_value + output_dir + "Exception.csv", "w",
                  newline="") as f:
            writer = csv.writer(f)
            writer.writerow(exception_column_list)


def main():
    exams = []
    list_of_files = []
    for obj in bucket.meta.client.list_objects(Bucket=bucket.name, Delimiter='/').get('CommonPrefixes'):
        exams.append(obj['Prefix'])
    for val in exams:
        rack_objs = bucket.objects.filter(Prefix=val + 'fastestfinger/CRR/')
        for racks in rack_objs:
            list_of_files.append(racks)
    # list_of_files = glob.glob(base_dir + "\\[!master]*\\" + input_dir + '*')
    for file in list_of_files:
        client_id = file.key.split("/")[0].split("_")[0]
        exam_id = file.key.split("/")[0].split("_")[1]
        create_output_files(client_id, exam_id)
        Master_files = pd.read_csv(base_dir + client_id + '_' + exam_id + output_dir + "Master.csv")
        Master_list = Master_files['filename'].tolist()
        if file not in Master_list:
            process_file(file)
        else:
            log_message(
                "Already in Master.",
                "info",
                "system_log",
            )


start_time = datetime.datetime.now()
main()
log_message(
                "time taken: " + str(datetime.datetime.now() - start_time),
                "info",
                "system_log",
            )
