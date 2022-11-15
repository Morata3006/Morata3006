import csv
import glob
import os
import io
import traceback
# from itertools import repeat
# from multiprocessing import Pool
import boto3
# from geopy.exc import GeocoderTimedOut
# from geopy.geocoders import Nominatim
import pandas as pd
import datetime
import numpy as np
from csv import writer
# from multiprocessing import Manager
from datetime import datetime, date

# activate_geo_locator: bool = True


# A = [19,-2,-31,45,30,11,121,27]
#
# def insertion_sort(l1):
#     for i in range(1,len(l1)):
#         j = i -1
#         nxt_ele = l1[i]
#         while(l1[j] > nxt_ele and j>=0):
#             l1[j+1] = l1[j]
#             j = j-1
#             l1[j+1] = nxt_ele

# insertion_sort(A)

# x = A[0]
# for i in range(1,len(A)):
#     A[i-1] = A[i]
# A[-1] = x
#
# print(A)
# from itertools import combinations
# nodes = [9,2,3, 7, 4]
# nodes[2:] = nodes[2:][::-1]
# print(nodes)

# max = -9999
# min = 0
#
# for i in range(len(A)):
#     min = min + A[i]
#
#     if max < min:
#         max = min
#     if min < 0:
#         min =0
# print(max)

# for i in range(len(A)):
#     for j in range(i+1, len(A)):
#         if A[i] > A[j]:
#             A[i], A[j] = A[j], A[i]
# print(A)

# i = 0
# j = len(A) - 1

# while(i < j):
#     A[i], A[j] = A[j], A[i]
#     i += 1
#     j -= 1
# print(A)
# B = [12, 13, 6]
# C = []
#
# for j in range(len(B)):
#     for i in range(len(A)):
#         if A[i] == B[j]:
#             C.append(A[i])
#             break
# print(C)

# x = A[0]
# for i in range(len(A)-1, 1, -1):
#     A[i] = A[i-1]
#
# A[len(A) - 1] = x
# print(A)

# mapping_dataframe = pd.read_csv('C:/Users/ashetty/Desktop/Registration/Diamond/mapping_cols.csv')
# mapping_dict = mapping_dataframe.set_index('dest').T.to_dict('list')

# mapping_dict = {}
# with open('C:/Users/ashetty/Desktop/Registration/Diamond/mapping_cols.csv') as map_cols:
#     reader = csv.reader(map_cols)
#     header = next(reader)
#     for line in reader:
#         mapping_dict[line[0]] = line[1]

# client = boto3.client(
#     's3',
#     aws_access_key_id = 'AKIA3LIJCIOGB62OOBQP',
#     aws_secret_access_key = '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',
#     region_name = 'ap-south-1'
# )

resource = boto3.resource(
    's3',
    aws_access_key_id = 'AKIA3LIJCIOGB62OOBQP',
    aws_secret_access_key = '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',
    region_name = 'ap-south-1'
)

bucket = resource.Bucket('exam-data')  # bucket name

# client.download_file(
#     Bucket="tangofastestfinger", Key="Master/Tango PGCT Master Running List_Updated.xlsx", Filename="dir.xlsx"
# )
list_working_dir = []
# Clientname_examname
for obj in bucket.meta.client.list_objects(Bucket=bucket.name, Delimiter='/').get('CommonPrefixes'):
    list_working_dir.append(obj['Prefix'])
print(list_working_dir)

for exam in list_working_dir:
    crl_objs = bucket.objects.filter(Prefix=exam + "fastestfinger/raw/")
    crr_objs = bucket.objects.filter(Prefix=exam + "fastestfinger/CRR/")
    # crl accessing
    for x in crl_objs:
        rack_name = str(x.key).split('/')[-3]
        batch_name = str(x.key).split('/')[-2]
    batch_obj = bucket.objects.filter(Prefix=exam + "fastestfinger/raw/" + rack_name + '/' + batch_name + '/')
    for val in batch_obj:
        print(val.key)

    # crr accessing
    for y in crr_objs:
        body = y.get()['Body'].read()
        crr_df = pd.read_csv(io.BytesIO(body), delimiter=';').reset_index(drop=True)


for obj in crl_objs:  # for getting date wise folder
    list_obj = obj.key.split('/')
    if len(list_obj) == 3 and '.log' not in obj.key:
        month = list_obj[-2]
        month_obj = bucket.objects.filter(Prefix="CRL/" + month + "/")
        for log in month_obj:
            key = log.key
            body = log.get()['Body'].read()
            dataframe = (
                                pd.read_csv(
                                    io.BytesIO(body), encoding='utf8',
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
                                    ]).reset_index(drop=True))
            print(dataframe)