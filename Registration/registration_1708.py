import csv
import glob
import logging
import math
import os
import sys
import traceback
import pandas as pd
import datetime
import numpy as np
from csv import writer
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
import timeit
import calendar

base_dir = "C:\\Users\\ashetty\\Desktop\\S3_instance\\"
input_dir = "*\\registration\\raw\\*\\"
output_dir = "C:\\Users\\ashetty\\Desktop\\S3_instance\\"
log_path = "C:\\Users\\ashetty\\Desktop\\DeX\\logs\\"
# headers = ["Sr.no","Registration Number","oum_user_id","Reference Number","oum_user_pk","sample","Title","Applicant’s First Name:","Middle Name","Last Name","Applicant Full Name","Father's name","Husband's name","Mother's name","Gender"]

exception_column_list = [
    "traceback",
]
master_column_list = ["filename"]
master_cols_header = ['client_cols','default_cols','exam_name','Is present','Display_value','client_name']
activate_geo_locator: bool = True


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


def findGeocode(city: str) -> list:
    city = 'India,' + city
    cities = pd.read_csv(base_dir + 'City.csv')
    loc = list(cities[cities['City_name'] == city]['latitude']) + list(cities[cities['City_name'] == city]['longitude'])

    if not loc:
        try:
            geolocator = Nominatim(user_agent='NSEIT-DEX')
            loc = geolocator.geocode(city)
            loc = [loc.latitude, loc.longitude]
            with open(base_dir + 'City.csv', 'a',
                      newline='\n') as locations:
                data_writer = writer(locations)
                data_writer.writerow([city] + loc)
                locations.close()
            return loc
        except GeocoderTimedOut:
            return findGeocode[city]
    else:
        return loc


def parse_date(txt):
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.datetime.strptime(txt, fmt)
        except ValueError:
            pass


log_setup(
    "INFO",
    datetime.datetime.today().strftime("%d%m%Y") + "_process_executed.log",
    "info",
)
log_setup(
    "ERROR", datetime.datetime.today().strftime("%d%m%Y") + "_errors.log", "error"
)


def append_to_master_cols(c_name, e_name):
    master_cols = open(base_dir + '\\master_cols.csv', 'a', newline='')
    writer_obj = writer(master_cols)
    with open(base_dir + c_name + '_' + e_name + '\\registration\\metadata\\mapping_cols.csv') as map_cols:
        reader = csv.reader(map_cols)
        next(reader)
        for line in reader:
            writer_obj.writerow(line)
    master_cols.close()


def process_file(file_name, cols, calendar):
    try:
        client_name = file_name.split("\\")[-5].split('_')[0]
        exam_name = file_name.split("\\")[-5].split('_')[1]
        log_message("Current working file: " + file_name, "info", "system_log", )
        read_files = []
        output_rows = []
        extension = file_name.split("\\")[-1].split(".")[-1]
        month_year = file_name.split("\\")[-2]
        if extension == "xlsx":
            dataframe = pd.read_excel(file_name, usecols=cols)
        elif extension == "csv":
            if "Master" not in file_name and "Exception" not in file_name and "City" not in file_name:
                dataframe = pd.read_csv(file_name, usecols=cols, low_memory=False).reset_index(drop=True)
            else:
                return
        else:
            log_message("invalid file", "info", "system_log",)
            return
        dataframe = dataframe.fillna('')
        dataframe.insert(0, 'exam_name', exam_name, True)
        dataframe.insert(0, 'Client_name', client_name, True)

        #  mapping col names
        mapping_dict = {}
        with open(base_dir + '\\master_cols.csv') as map_cols:
            reader = csv.reader(map_cols)
            next(reader)
            for line in reader:
                if line[2] == exam_name and line[5] == client_name and line[3] == 'yes':
                    mapping_dict[line[0]] = line[1]
        dataframe.rename(columns=mapping_dict, inplace=True)
        input_csv_data = [dataframe.columns.tolist()]
        values = dataframe.values.tolist()
        input_csv_data.extend(values)

        for index, item in enumerate(input_csv_data):  # index for row count
            if index != 0:
                if item[65] != '':
                    item[65] = parse_date(item[65].split('+')[0])
                else:
                    item[65] = 'NA'
                if item[120] != '' and isinstance(item[120], str):
                    item[120] = str(item[120])
                    item[120] = '01-' + str(calendar[item[120].split(' ')[0][:3]]) + '-' + item[120].split(' ')[-1]

            #     item[37] = item[37].strip('\n')
            output_row = []
            output_row.extend(item)
            output_rows.append(output_row)
        read_files.append([file_name])

        output_csv = pd.DataFrame(output_rows[1:], columns=output_rows[0])
        output_csv.to_csv(
            output_dir + client_name + '_' + exam_name + '\\registration\\transformed\\' + month_year + '\\' + client_name + '_' + exam_name + '_registration.csv',
            header=True, index=False)

        with open(base_dir + client_name + '_' + exam_name + "\\registration\\Master.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(read_files)
    except Exception as err:
        traceback.print_exc()
        print(item)
        with open(output_dir + client_name + '_' + exam_name + "\\registration\\Exception.csv", "a", newline="") as f:
            f.write(str(err))


def create_directories(client_name, month_year):
    if not os.path.exists(output_dir + client_name + '\\registration\\transformed\\' + month_year):
        os.makedirs(output_dir + client_name + '\\registration\\transformed\\' + month_year)
    if not os.path.exists(output_dir + '\\' + client_name + "\\registration\\Master.csv"):
        with open(output_dir + '\\' + client_name + "\\registration\\Master.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
    if not os.path.exists(output_dir + "\\master_cols.csv"):
        with open(output_dir + "\\master_cols.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_cols_header)
    if not os.path.exists(output_dir + '\\' + client_name + "\\registration\\Exception.csv"):
        with open(output_dir + '\\' + client_name + "\\registration\\Exception.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(exception_column_list)


def main():

    try:
        list_of_files = glob.glob(base_dir + input_dir + "*")
        #  creating output files/directories
        calendar_dict = {month: index for index, month in enumerate(calendar.month_abbr) if month}
        for i in list_of_files:
            dir_name = i.split("\\")[-5]
            month_year = i.split("\\")[-2]
            create_directories(dir_name, month_year)
        log_message("Started", "info", "system_log", )
        for file in list_of_files:
            client_name = file.split("\\")[-5].split('_')[0]
            exam_name = file.split("\\")[-5].split('_')[1]
            Master_files = pd.read_csv(output_dir + client_name + '_' + exam_name + "\\registration\\Master.csv")
            Master_list = Master_files['filename'].tolist()
            if file not in Master_list:
                append_to_master_cols(client_name, exam_name)
                master_cols_dataframe = pd.read_csv(base_dir + '\\master_cols.csv', encoding='cp1252')
                master_cols_dataframe['Is present'] = master_cols_dataframe['Is present'].str.lower()
                headers = master_cols_dataframe['client_cols'].loc[
                    (master_cols_dataframe['Is present'] == 'yes') & (master_cols_dataframe['client_name'] == client_name) & (
                            master_cols_dataframe['exam_name'] == exam_name)].values.tolist()
                process_file(file, headers, calendar_dict)
            else:
                log_message("Already in master", "info", "system_log", )
    except Exception as err:
        traceback.print_exc()
        with open(output_dir + client_name + '_' + exam_name + "\\registration\\Exception.csv", "a",
                  newline="") as f:
            f.write(str(err))


start_time = datetime.datetime.now()
main()
log_message("time taken: " + str(datetime.datetime.now() - start_time), "info", "system_log",)
