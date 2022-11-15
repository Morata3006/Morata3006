import datetime
import glob
import traceback
from time import strftime
import pandas as pd
import csv
import os

base_dir = "C:\\Users\\ashetty\\Desktop\\S3_instance\\"
input_dir = "\\scorereport\\raw\\"
output_dir = "\\scorereport\\"
exception_column_list = [
    "file_name",
    "row_number",
    "testdate",
    "testtime",
    "examid",
    "candidatename",
    "registrationnumber",
    "eed_id",
    "testcenter",
    "language",
    "module",
    "status",
    "qtp_eed_id",
    "totalquestion",
    "attemptedquestions",
    "nonattemptedquestions",
    "deletedquestions",
    "correctanswers",
    "wronganswers",
    "totalnewscore",
    "error"

]
master_column_list = ["filename"]


def process_file(file_name):
    try:
        read_files = []
        exception_rows = []
        output_rows = []
        output_name = file_name.split("\\")[-1].split(".")[0]
        extension = file_name.split("\\")[-1].split(".")[-1]
        client_id = file_name.split("\\")[-4].split("_")[0]
        exam_name = file_name.split("\\")[-4].split("_")[1]
        if not os.path.isfile(base_dir + client_id + '_' + exam_name + input_dir + "*"):
            if extension == "xlsx":
                dataframe = pd.read_excel(file_name)
            elif extension == "csv":
                if "Master" not in file_name and "Exception" not in file_name:
                    dataframe = pd.read_csv(file_name, sep=";", low_memory = False).reset_index(drop=True)
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
                    file_name_list.append("NaN value found")
                    exception_rows.append(file_name_list)

                else:
                    if index == 0:
                        item.insert(0, "exam_name")
                        item.insert(0, "clientid")
                        output_rows.append(item)
                        continue
                    output_row = [client_id, exam_name]
                    output_row.extend(item)
                    output_rows.append(output_row)

            read_files.append([file_name])
            with open(base_dir + client_id + '_' + exam_name + output_dir + "transformed\\" + output_name + ".csv", "w", newline=""
                      ) as f:
                writer = csv.writer(f)
                writer.writerows(output_rows)
            if os.path.isfile(base_dir + client_id + '_' + exam_name + output_dir + "Master.csv"):
                with open(base_dir + client_id + '_' + exam_name + output_dir + "Master.csv", "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(read_files)
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
    list_of_files = glob.glob(base_dir + "\\[!master]*\\" + input_dir + '*')
    for file in list_of_files:
        client_id = file.split("\\")[-4].split("_")[0]
        exam_id = file.split("\\")[-4].split("_")[1]
        create_output_files(client_id, exam_id)
        Master_files = pd.read_csv(base_dir + client_id + '_' + exam_id + output_dir + "Master.csv")
        Master_list = Master_files['filename'].tolist()
        if file not in Master_list:
            process_file(file)
        else:
            print("already in master")


start_time = datetime.datetime.now()
main()
print("time taken: " + str(datetime.datetime.now() - start_time))
