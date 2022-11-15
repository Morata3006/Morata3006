import csv
import glob
import logging
import os
import pandas as pd
import datetime
from csv import writer


base_dir = "/home/nsedex/Registration/"
input_dir = "temp/dex/full_data/"
output_dir = "/home/nsedex/Registration/temp/dex/full_data/"
log_path = ""

# base_dir = "C:\\Users\\ashetty\\Desktop\\Registration\\"
# input_dir = "*\\raw\\*\\"
# output_dir = "C:\\Users\\ashetty\\Desktop\\Registration\\"
# log_path = "C:\\Users\\ashetty\\Desktop\\DeX\\logs\\"

exception_column_list = [
    "file_name",
    "row_number",
    "error"
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


log_setup(
    "INFO",
    datetime.datetime.today().strftime("%d%m%Y") + "_process_executed.log",
    "info",
)
log_setup(
    "ERROR", datetime.datetime.today().strftime("%d%m%Y") + "_errors.log", "error"
)


def process_file(file_name):
    Master_files = pd.read_csv(output_dir + "Master.csv")
    Master_list = Master_files['filename'].tolist()
    if file_name not in Master_list:
        read_files = []
        exception_rows = []
        output_rows = []
        extension = file_name.split("\\")[-1].split(".")[-1]
        client_name = file_name.split("\\")[-4]
        month_year = file_name.split("\\")[-2]
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
            input_csv_data = [dataframe.columns.tolist()]
            values = dataframe.values.tolist()
            input_csv_data.extend(values)

            default_columns_names = ['Registration Number', 'oum_user_id', 'Reference Number', 'oum_user_pk',
                                     'Title', 'Applicant’s First Name:', 'Middle Name', 'Last Name', 'Applicant Full Name',
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
                                     'Payment Date', 'Status']

            for index, item in enumerate(input_csv_data):  # index for row count
                if index != 0:
                    item[37] = item[37].strip('\n')
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
            new_df.to_csv(output_dir + client_name + '\\transformed\\' + month_year + '\\' + client_name + '_registration.csv', index=False)

            if os.path.isfile(base_dir + "Master.csv"):
                with open(base_dir + "Master.csv", "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(read_files)
            else:
                with open(base_dir + "Master.csv", "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(master_column_list)
                    writer.writerows(read_files)
            if os.path.isfile(base_dir + "Exception.csv"):
                with open(base_dir + "Exception.csv", "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerows(exception_rows)
            else:
                with open(base_dir + "Exception.csv", "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(exception_column_list)
                    writer.writerows(exception_rows)
    else:
        log_message("Already in master", "info", "system_log", )


def main():
    list_of_files = glob.glob(base_dir + input_dir + "*")
    client_name = list_of_files[0].split("\\")[-4]
    month_year = list_of_files[0].split("\\")[-2]
    if not os.path.exists(output_dir + client_name + '\\transformed\\' + month_year):
        os.makedirs(output_dir + client_name + '\\transformed\\' + month_year)
    if not os.path.exists(output_dir + "Master.csv"):
        with open(output_dir + "Master.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(master_column_list)
    log_message("Started", "info", "system_log",)
    for file in list_of_files:
        process_file(file)


start_time = datetime.datetime.now()
main()
log_message("time taken: " + str(datetime.datetime.now() - start_time), "info", "system_log",)
