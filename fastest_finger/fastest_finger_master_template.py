import csv
import datetime
import glob
import os
from multiprocessing import Pool
import pandas as pd

base_dir = "/home/nsedex/Fastest_finger/"
exam_dir = "mahadev2_candidates_response_log/"
input_dir = "raw/"
output_dir = "/home/data/fastest_finger_analysis/Transformed/"
exception_column_list = ["error"]
master_column_list = ["filename"]
client_name = base_dir.split("/")[-2].split("_")[0].lower()
master_template="/home/data/master_template/Transformed/mahadev2_master_file.csv"


def process_file(file_path):
    read_files = []
    exception_rows = []
    output_rows = []
    client_id = file_path.split("/")[-2].split("_")[0].lower()
    file_name = file_path.split("/")[-1]
    Value=1
    master_dataframe=pd.read_csv(master_template)
    list_of_files = glob.glob(file_path + "/*")
    for sub_file in list_of_files:
        try:
            unique_questions = {}
            exam_id = sub_file.split("/")[-1].split("-")[0]
            eed_id = sub_file.split("/")[-1].split("-")[1]
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
            for index, item in enumerate(input_csv_data):
                if item[4] != -1:
                    item[0] = item[0].replace('INFO - "', "")
                    item[13] = item[13].replace('"', "")
                    unique_key = str(item[2])
                    output_row = [client_id, eed_id, exam_id]
                    output_row.extend(item)
                    output_row.append(Value)
                    unique_questions[unique_key] = output_row
            unique_questions = {k: v for k, v in sorted(unique_questions.items(), key=lambda item: item[1])}
            sub_file_output_rows = [value for item, value in unique_questions.items()]
            output_rows.extend(sub_file_output_rows)
        except Exception as ex:
            print("filename: " + sub_file)
            continue
    output_rows.insert(
        0,
        [
            "clientid",
            "eed_id",
            "examid",
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
            "Value"
        ],
    )
    read_files.append([file_path])
    output_csv = pd.DataFrame(output_rows[1:],columns=output_rows[0])
    output_csv['QuestionID']=output_csv['QuestionID'].astype(int)    
    
    output_csv['outputcombined']=output_csv['clientid']+'_'+output_csv['examid']+'_'+output_csv['QuestionID'].astype(str)
    master_dataframe['QuestionID']=master_dataframe['QuestionID'].astype(int)
    master_dataframe['mastercombined']=master_dataframe['clientid']+'_'+master_dataframe['eed_exm_id'].astype(str)+'_'+master_dataframe['QuestionID'].astype(str)
    new_df = pd.merge(output_csv, master_dataframe,  how='left', left_on=output_csv['outputcombined'], right_on = master_dataframe['mastercombined'])
    new_df=new_df.drop(['eed_exm_id','outputcombined','mastercombined','QuestionID_y','clientid_y',
    'key_0'],axis=1)
    new_df.rename(columns = {'clientid_x':'clientid','QuestionID_x':'QuestionID'}, inplace = True)
    
    new_df.to_csv(output_dir + file_name + ".csv",index=False,header=False)

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


def timestamp_converter(timestamp_value):
    format = "%d/%b/%Y %I:%M:%S %p"
    timestamp = datetime.datetime.strptime(timestamp_value, format)
    timestamp = timestamp.strftime("%d%m%y%H%M%S")
    return timestamp


def main():
    list_of_files = glob.glob(base_dir + exam_dir + "*")
    with Pool(30) as pool:
        pool.map(process_file, list_of_files)
        pool.close()
        pool.terminate()
        pool.join()


start_time = datetime.datetime.now()
main()
print("time taken: " + str(datetime.datetime.now() - start_time))
