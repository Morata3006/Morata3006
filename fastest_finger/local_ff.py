import csv
import datetime
import glob
import os
from multiprocessing import Pool
import pandas as pd

# base_dir = "/home/nsedex/Fastest_finger/"
# exam_dir = "Diamond/"
# input_dir = "raw/"
# output_dir = "/home/data/fastest_finger_analysis/Transformed/"
exception_column_list = ["error"]
master_column_list = ["filename"]
client_name = "Diamond"

base_dir = 'C:/Users/ashetty/Desktop/DeX/Diamond/CRL/Candidate_Logs/'
exam_dir = '10-Jan-2021_0900-1030_1006/LA611312645-330786-N.log'
output_dir = 'C:/Users/ashetty/Desktop/DeX/Diamond/CRL/Transformed/'

# ques_ppr_template = "/home/nsedex/Master_template/Transformed/29Jan_rk1_CRR2.csv"
# master_template = "/home/nsedex/Master_template/Transformed/Diamond Master Running List.xlsx"

ques_ppr_template = 'C:/Users/ashetty/Desktop/DeX/Diamond/QuestionPaper/29Jan_rk1_CRR2.csv'
master_template = 'C:/Users/ashetty/Desktop/DeX/Diamond/sample_master.xlsx'


def process_file(file_path):
    read_files = []
    exception_rows = []
    output_rows = []
    client_id = "Diamond"
    file_name = file_path.split("/")[-1]
    Value = 1
    ques_ppr_dataframe = pd.read_csv(ques_ppr_template, delimiter=";")
    master_dataframe = pd.read_excel(master_template)
    master_dataframe = master_dataframe[master_dataframe['Gender'].str.contains('MALE','FEMALE')==True]

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
                ).reset_index(drop=True)
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

    output_csv = pd.DataFrame(output_rows[1:], columns=output_rows[0])
    output_csv['QuestionID'] = output_csv['QuestionID'].astype(int)

    # lookup with ques ppr file
    output_csv['outputcombined'] = output_csv['clientid'] + '_' + output_csv['examid'] + '_' + output_csv[
        'QuestionID'].astype(str)
    ques_ppr_dataframe['crr_qst_no'] = ques_ppr_dataframe['crr_qst_no'].astype(int)
    # print(output_csv['outputcombined'])
    ques_ppr_dataframe['quescombined'] = "Diamond" + '_' + ques_ppr_dataframe['eed_exm_id'].astype(
        str) + '_' + ques_ppr_dataframe['crr_qst_no'].astype(str)
    # print(ques_ppr_dataframe['quescombined'])
    new_df = pd.merge(output_csv, ques_ppr_dataframe, how='left', left_on=output_csv['outputcombined'],
                      right_on=ques_ppr_dataframe['quescombined'])
    new_df = new_df.drop(
        ['eed_exm_id', 'outputcombined', 'quescombined', 'key_0', 'crr_answer', 'crr_qst_no', 'Section',
         'crr_eed_id'], axis=1)

    # lookup with master excel file
    new_df['secondcombined'] = new_df['clientid'] + '_' + new_df['examid']
    master_dataframe['mastercombined'] = "Diamond" + "_" + master_dataframe['Candidate ID']

    if not master_dataframe['mastercombined'].empty:
        final_df = pd.merge(new_df, master_dataframe, how='left', left_on=new_df['secondcombined'],
                            right_on=master_dataframe['mastercombined'])
        final_df = final_df.drop(
            ['secondcombined', 'Dummy 1', 'Dummy 2', 'Dummy 3', 'Dummy 4', 'Dummy 5', 'mastercombined', 'key_0',
             'Candidate Name',
             'DOB', 'Candidate ID', 'Candidate ID.1', 'marks', 'negative_marks', 'Module Name', 'Category', 'SequenceNumber'
                , 'IPAddress'], axis=1)

        final_df.to_csv(output_dir + file_name + ".csv", index=False)
        # read_files.append(file_name + ".csv")

    if os.path.isfile(base_dir + "Master.csv"):
        with open(base_dir+ "Master.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(read_files)
    else:
        with open(base_dir + "Master.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
            writer.writerows(read_files)
    if os.path.isfile(base_dir  + "Exception.csv"):
        with open(base_dir  + "Exception.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(exception_rows)
    else:
        with open(base_dir+ "Exception.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(exception_column_list)
            writer.writerows(exception_rows)


def timestamp_converter(timestamp_value):
    format = "%d/%b/%Y %I:%M:%S %p"
    timestamp = datetime.datetime.strptime(timestamp_value, format)
    timestamp = timestamp.strftime("%d%m%y%H%M%S")
    return timestamp


def main():
    list_of_files = glob.glob(base_dir + exam_dir )
    print(list_of_files)
    process_file(list_of_files)
    # with Pool(2) as pool:
    #     pool.map(process_file, list_of_files)
    #     pool.close()
    #     pool.terminate()
    #     pool.join()


start_time = datetime.datetime.now()
main()
print("time taken: " + str(datetime.datetime.now() - start_time))
