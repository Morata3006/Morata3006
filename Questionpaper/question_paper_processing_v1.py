import datetime
import glob
import pandas as pd
import csv
import os
import traceback

base_dir = "C:\\Users\\ashetty\\Desktop\\S3_instance\\"
input_dir = "fastestfinger\\CRR\\"
output_dir = "\\questionpaper\\"
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
    try:
        read_files = []
        exception_rows = []
        output_rows = []
        output_name = file_name.split('\\')[-1].split(".")[0]
        extension = file_name.split("\\")[-1].split(".")[-1]
        client_id = file_name.split("\\")[-4].split("_")[0]
        exam_name = file_name.split("\\")[-4].split("_")[1]
        if not os.path.isfile(base_dir + client_id + '_' + exam_name + '\\' + input_dir + "*"):
            with open(file_name) as one:
                dialect = csv.Sniffer().sniff(one.readline(), [',', ';'])
                df_sep = dialect.delimiter
            if extension == "xlsx":
                dataframe = pd.read_excel(file_name)
            elif extension == "csv":
                if "Master" not in file_name and "Exception" not in file_name:
                    dataframe = pd.read_csv(file_name, delimiter=df_sep, low_memory=False).reset_index(drop=True)
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
