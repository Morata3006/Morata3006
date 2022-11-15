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


def process_file(file_name, mapper, calendar):
    try:
        client_name = file_name.split("\\")[-5].split('_')[0]
        exam_name = file_name.split("\\")[-5].split('_')[1]
        log_message("Current working file: " + file_name, "info", "system_log", )
        read_files = []
        output_rows = []
        extension = file_name.split("\\")[-1].split(".")[-1]
        month_year = file_name.split("\\")[-2]
        if extension == "xlsx":
            dataframe = pd.read_excel(file_name,)
        elif extension == "csv":
            if "Master" not in file_name and "Exception" not in file_name and "City" not in file_name:
                dataframe = pd.read_csv(file_name, low_memory=False).reset_index(drop=True)
            else:
                return
        else:
            log_message("invalid file", "info", "system_log",)
            return
        dataframe = dataframe.fillna('')
        dataframe.insert(0, 'exam_name', exam_name, True)
        dataframe.insert(0, 'Client_name', client_name, True)

        dataframe.rename(columns=mapper, inplace=True)
        input_csv_data = [dataframe.columns.tolist()]
        values = dataframe.values.tolist()
        input_csv_data.extend(values)

        for index, item in enumerate(input_csv_data):  # index for row count
            if index != 0:
                if 'Application Submitted Date' in input_csv_data[0]:
                    appl_date = input_csv_data[0].index('Application Submitted Date')
                    if item[appl_date] != '':
                        item[appl_date] = parse_date(item[65].split('+')[0])
                    else:
                        item[appl_date] = 'NA'
                if 'Graduation Month & Year of Passing' in input_csv_data[0]:
                    grad_date = input_csv_data[0].index('Graduation Month & Year of Passing')
                    if item[grad_date] != '' and isinstance(item[grad_date], str):
                        item[grad_date] = str(item[grad_date])
                        item[grad_date] = '01-' + str(calendar[item[grad_date].split(' ')[0][:3]]) + '-' + item[grad_date].split(' ')[-1]

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
            mapping_dict = {}
            with open(base_dir + client_name + '_' + exam_name + '\\registration\\mapping_files\\mapping_cols.csv') as map_cols:
                reader = csv.reader(map_cols)
                next(reader)
                for line in reader:
                    line[3] = line[3].lower()
                    if line[2] == exam_name and line[5] == client_name and line[3] == 'yes':
                        mapping_dict[line[0]] = line[1]
            if file not in Master_list:
                process_file(file, mapping_dict, calendar_dict)
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
