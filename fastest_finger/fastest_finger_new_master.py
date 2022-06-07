import glob
import pandas as pd
import numpy as np
import os
from csv import writer


def add_to_master(client_name):
    with open('/home/data/fastest_finger_analysis/Raw/Master.csv', 'a') as master:
        data_writer = writer(master)
        data_writer.writerow([client_name])
        master.close()


for file in glob.glob('/home/data/fastest_finger_analysis/Raw/*'):
    client_id = file.split('/')[-1].split('_')[0].lower()
    #print(client_id)
    master_checker = file.split('/')[-1].split('.')[0]
    if master_checker == 'Master':
        continue
    Master = pd.read_csv('/home/data/fastest_finger_analysis/Raw/Master.csv')
    if file in list(Master['File_name']) or ".csv" in file:
        print(file, 'Skipped :)\n')
        continue
    add_to_master(file)
    print(client_id)
    try:
        os.mkdir('/home/data/fastest_finger_analysis/Transformed/' + client_id + '_fastest_finger')
    except:
        print('Directory exists')
    for sub_file in glob.glob(file + '/*/*'):
        df = pd.read_csv(sub_file, delimiter='|',
                         names=['Timestamp', 'Section Name', 'QuestionID', 'CurrentQuestionNumber', 'OptionSelected',
                                'AlternateOptionSelected', 'Bookmark', 'SectionalQuestionNumber', 'IPAddress', 'Action',
                                'SequenceNumber', 'Candidate MachineDateTime', 'Timer', 'MacAddress'])
        try:
            sub_file = sub_file.strip(file).split('/')[-1].split('-')
            exam_id = sub_file[0]
            eed_id = sub_file[1]
            #queslist = question_paper.crr_eed_id.tolist()
            #print(eed_id)
            #print(question_paper.crr_eed_id[5])
            #if [int(eed_id) in queslist]:
            #print('Hello')
            
                
            df.insert(0, 'exam_id', exam_id)
            df.insert(0, 'eed_id', eed_id)
            df.insert(0, 'Client_id', client_id)
            df["Timestamp"] = df["Timestamp"].str.replace('INFO - "', '')
            df["MacAddress"] = df["MacAddress"].str.replace('"', '')
            df.dropna(inplace=True, axis=0)
            df = df.drop_duplicates(subset=['QuestionID'], keep='last')
            df = df[df['OptionSelected'] != -1]
            if df.empty != True:
                df['value'] = 1  
                df.to_csv(
                    '/home/data/fastest_finger_analysis/Transformed/' + client_id + '_fastest_finger' + '/' + exam_id + '-' + eed_id + '.csv',
                    index=False,header = False)
            else:
                print(eed_id, 'Has not attempted any questions')
                
        except:
            print(eed_id, 'is not present')

            
