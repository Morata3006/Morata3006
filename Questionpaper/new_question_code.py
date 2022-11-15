import datetime
import glob
import pandas as pd
import csv
import os

base_dir = "C:\\Users\\Sherinp\\Desktop\\dex\\QuestionPaper\\"
input_dir = "raw\\"
output_dir = "Transformed\\"
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


def process_file(file_name):
    read_files = []
    exception_rows = []
    output_rows = []
    extension = file_name.split("\\")[-1].split(".")[-1]
    client_id = file_name.split("\\")[-1].split("_")[0].lower()
    if extension == "xlsx":
        dataframe = pd.read_excel(file_name)
    elif extension == "csv":
        if "Master" not in file_name and "Exception" not in file_name:
            dataframe = pd.read_csv(file_name,low_memory=False).reset_index(drop=True)
        else:
            return
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
            exception_rows.append(file_name_list)
        else:
            if index == 0:
                item[12] = "crr_qst_no"
                item.insert(0, "client_id")
                output_rows.append(item)
                continue
            output_row = [client_id]
            output_row.extend(item)
            output_rows.append(output_row)
    read_files.append([file_name])
    with open(
        base_dir + output_dir + client_id + "_question_paper.csv", "w", newline=""
    ) as f:
        writer = csv.writer(f)
        writer.writerows(output_rows)
    if os.path.isfile(base_dir + input_dir + "Master.csv"):
        with open(base_dir + input_dir + "Master.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(read_files)
    else:
        with open(base_dir + input_dir + "Master.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
            writer.writerows(read_files)
    if os.path.isfile(base_dir + input_dir + "Exception.csv"):
        with open(base_dir + input_dir + "Exception.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(exception_rows)
    else:
        with open(base_dir + input_dir + "Exception.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(exception_column_list)
            writer.writerows(exception_rows)


def main():
    list_of_files = glob.glob(base_dir + input_dir + "*")
    for file in list_of_files:
        process_file(file)


start_time = datetime.datetime.now()
main()
print("time taken: " + str(datetime.datetime.now() - start_time))
