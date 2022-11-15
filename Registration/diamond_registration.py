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


base_dir = "C:\\Users\\ashetty\\Desktop\\Registration\\"
input_dir = "*\\raw\\*\\"
output_dir = "C:\\Users\\ashetty\\Desktop\\Registration\\"
log_path = "C:\\Users\\ashetty\\Desktop\\DeX\\logs\\"

exception_column_list = [
    "traceback",
]
master_column_list = ["filename"]
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
        client_name = file_name.split("\\")[-4]
        Master_files = pd.read_csv(output_dir + client_name + "\\Master.csv")
        Master_list = Master_files['filename'].tolist()
        if file_name not in Master_list:
            log_message("Current working file: " + file_name, "info", "system_log", )
            read_files = []
            output_rows = []
            extension = file_name.split("\\")[-1].split(".")[-1]
            month_year = file_name.split("\\")[-2]
            # client_id = "Diamond"  # file_name.split("/")[-1].split("_")[0].lower()
            if not os.path.isfile(output_dir + 'Diamond_registration_new.csv'):
                if extension == "xlsx":
                    dataframe = pd.read_excel(file_name)
                elif extension == "csv":
                    if "Master" not in file_name and "Exception" not in file_name and "City" not in file_name:
                        dataframe = pd.read_csv(file_name, low_memory=False).reset_index(drop=True)
                    else:
                        return
                else:
                    log_message("invalid file", "info", "system_log",)
                    return
                dataframe = dataframe.fillna('')
                dataframe['Client_name'] = client_name
                # mapping_dict = {"Applicant’s First Name:": "First Name:",}
                mapping_dict = {}
                with open('C:/Users/ashetty/Desktop/Registration/Diamond/mapping_cols.csv') as map_cols:
                    reader = csv.reader(map_cols)
                    header = next(reader)
                    for line in reader:
                        mapping_dict[line[0]] = line[1]
                dataframe.rename(columns=mapping_dict, inplace=True)
                input_csv_data = [dataframe.columns.tolist()]
                values = dataframe.values.tolist()
                input_csv_data.extend(values)

                default_columns_names = ['Registration Number', 'oum_user_id', 'Reference Number', 'oum_user_pk',
                                         'Title', 'First Name:', 'Middle Name', 'Last Name', 'Applicant Full Name',
                                         "Father's name", "Husband's name", "Mother's name", 'Gender', 'Nationality',
                                         'Email ID',
                                         'Mobile No.', 'Are you eligible to avail the services of Scribe',
                                         'Type of Transgender (In case of Transgender)',
                                         'Date of Birth', 'Age as On 01-01-2021', 'Age as On 30-11-2020', 'Select PWD Category',
                                         'Preferred Test City -1', 'Preferred Test City -2', 'Preferred Test City -3',
                                         'Application Submitted Date', 'Local language selected for 10th',
                                         'Local language selected for 12th',
                                         'Are you claiming age relaxation as an In-service Contractual Employee vide Odisha Group-B posts (Contractual Appointment) Rules-2013 OR Odisha Group ‘C’ &  Group ‘D’ posts Contractual Appointment Rules-2013 ',
                                         'Identity Card No', 'Do you possess NCC Certificate of type B or C?',
                                         'Type of NCC Certificate',
                                         'Are you an Ex-serviceman who has not served in any of the central or state government department after discharged from defence services?',
                                         'Department', 'ESM - Date of Discharge', 'Duration of Service (in Years-Months-Days)',
                                         'Post Applying for:',
                                         'Are you Domicile of State?', 'ocd_domicilecertificate', 'ocd_domicileserial',
                                         'Category (Caste)', 'ocd_cat_cert_iss_dt', 'ocd_cat_cert_val_dt',
                                         'ocd_cat_serial', 'Are you Dependent of Freedom Fighter?', 'ocd_freedomfighterdt',
                                         'ocd_freedomfighterserial',
                                         'ocd_freedom_authority', 'ocd_governmentemp', 'ocd_governmentempdt',
                                         'Duration of Service ', 'ocd_nocdt', 'ocd_nocserial',
                                         'ocd_noc_authority', 'Are you an Ex-serviceman?', 'ocd_esm_dt_of_enlistment',
                                         'ocd_esm_dt_of_discharge', 'Permanent Address',
                                         'Permanent State', 'Permanent District', 'Permanent City', 'Permanent Pincode',
                                         'Correspondence Address same as Permanent Address',
                                         'Correspondence Address', 'Correspondence State', 'Correspondence District',
                                         'Correspondence City', 'Correspondence Pincode ',
                                         'Post preference -1', 'Post preference -2', 'Post preference -3', 'oaed_doeacc',
                                         'oaed_terri_army', 'oaed_bcerti',
                                         '10th/SSC Name of Board', '10th/SSC Other Board', '10th/SSC Month & Year of Passing',
                                         '10th/SSC Result Status',
                                         '10th/SSC Roll Number', '10th/SSC Certificate Serial No', '12th/HSC Name of Board',
                                         '12th/HSC Other Board',
                                         '12th/HSC Month & Year of Passing', '12th/HSC Result Status', '12th/HSC Roll Number',
                                         '12th/HSC Certificate Serial No',
                                         'Graduation Degree Name', 'Graduation Other Degree', 'Graduation University Name',
                                         'Graduation Other University',
                                         'Graduation Month & Year of Passing', 'Graduation Result Status',
                                         'Graduation Roll Number', 'Graduation Certificate Serial No',
                                         'Payment Status', 'Payement Mode', 'Payment amount', 'SBI / E-Challan Transaction ID',
                                         'NSEIT Transaction ID',
                                         'Payment Date', 'Status', 'Client_name']

                for index, item in enumerate(input_csv_data):  # index for row count
                    if index != 0:
                        #     if (len(str(item[0])) < 5 or len(str(item[0])) > 20):   # client id
                        #         file_name_list = [file_name, index + 1]
                        #         file_name_list.extend(item)
                        #         exception_rows.append(file_name_list)
                        #     if not item[2].isalpha():                               # check for appl id
                        #         file_name_list = [file_name, index + 1]
                        #         exception_rows.append(file_name_list)
                        #     if item[7] is None:                                     # check for address state
                        #         file_name_list = [file_name, index + 1]
                        #         exception_rows.append(file_name_list)
                        #     item[2] = item[2].lower()
                        item[37] = item[37].strip('\n')
                        # print(item[1001])
                    # else:
                    # if index == 0:
                    #     # item.insert(0, "longitude")
                    #     # item.insert(0, "latitude")
                    #     item.insert(0, "clientid")
                    #     output_rows.append(item)
                    #     continue
                    # try:
                    #     if activate_geo_locator:
                    #         location = findGeocode(item[60])
                    #         if not location:
                    #             latitude = 0
                    #             longitude = 0
                    #         else:
                    #             latitude = location[0]
                    #             longitude = location[1]
                    # except Exception as E:
                    #     print(E)
                    #     latitude = 0
                    #     longitude = 0
                    output_row = []
                    output_row.extend(item)
                    output_rows.append(output_row)
                read_files.append([file_name])

                output_csv = pd.DataFrame(output_rows[1:], columns=output_rows[0])
                new_df = pd.DataFrame(columns=default_columns_names)
                for i in range(len(default_columns_names)):
                    if default_columns_names[i] in output_csv.columns:
                        new_df[default_columns_names[i]] = output_csv[default_columns_names[i]]
                        new_df[default_columns_names[i]] = new_df[default_columns_names[i]].replace(['\'-'], None)
                    else:
                        new_df[default_columns_names[i]] = "NaN"
                new_df.to_csv(output_dir + client_name + '\\transformed\\' + month_year + '\\' + client_name + '_registration.csv', header=True, index=False)

                with open(base_dir + client_name + "\\Master.csv", "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(read_files)
        else:
            log_message("Already in master", "info", "system_log", )
    except Exception as err:
        traceback.print_exc()
        with open(output_dir + client_name + "\\Exception.csv", "a", newline="") as f:
            f.write(str(err))


def create_directories(client_name, month_year):
    if not os.path.exists(output_dir + client_name + '\\transformed\\' + month_year):
        os.makedirs(output_dir + client_name + '\\transformed\\' + month_year)
    if not os.path.exists(output_dir + '\\' + client_name + "\\Master.csv"):
        with open(output_dir + '\\' + client_name + "\\Master.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
    if not os.path.exists(output_dir + '\\' + client_name + "\\Exception.csv"):
        with open(output_dir + '\\' + client_name + "\\Exception.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(exception_column_list)


def main():

    list_of_files = glob.glob(base_dir + input_dir + "*")
    for i in list_of_files:
        client_name = i.split("\\")[-4]
        month_year = i.split("\\")[-2]
        create_directories(client_name, month_year)
    log_message("Started", "info", "system_log",)
    for file in list_of_files:
        process_file(file)


start_time = datetime.datetime.now()
main()
log_message("time taken: " + str(datetime.datetime.now() - start_time), "info", "system_log",)
