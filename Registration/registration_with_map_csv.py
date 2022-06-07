import glob
import pandas as pd
import datetime
import numpy as np
from csv import writer
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

activate_geo_locator: bool = True


def findGeocode(city: str) -> list:
    city = 'India,' + city 
    cities = pd.read_csv('/home/data/registration/Raw/City.csv')
    loc = list(cities[cities['City_name'] == city]['latitude']) + list(cities[cities['City_name'] == city]['longitude'])
    if not loc:
        try:
            geolocator = Nominatim(user_agent='NSEIT-DEX')
            loc = geolocator.geocode(city)
            loc = [loc.latitude, loc.longitude]
            with open('/home/data/registration/Raw/City.csv', 'a',
                      newline='\n') as locations:
                data_writer = writer(locations)
                data_writer.writerow([city] + loc)
                locations.close()
            return loc
        except GeocoderTimedOut:
            return findGeocode[city]
    else:
        return loc


def validate_reg_id(id) -> bool:
    return not (len(str(id)) < 5 or len(str(id)) > 20)


def validate_gender(gender) -> bool:
    return gender.isalpha()


def validate_dates(date_text):
    if '/' in date_text:
        date_text = "-".join(date_text.split('/'))
    try:
        print(date_text)
        dt = datetime.datetime.strptime(date_text, '%Y-%m-%d').strftime('%Y-%m-%d')
        return False
    except ValueError as v:
        print(v)
        return True

def validate_Appl_Status(app_st) -> bool:
    return not (app_st is None)


def add_exception(file_name, row_no, error):
    global new_df
    with open('/home/data/registration/Raw/Exceptions.csv', 'a') as exp:
        data_writer = writer(exp)
        data_writer.writerow([file_name, row_no] + list(df.iloc[i]) + [error])
        exp.close()


def add_to_master(file_name):
    with open('/home/data/registration/Raw/Master.csv', 'a') as master:
        data_writer = writer(master)
        data_writer.writerow([file_name])
        master.close()

def extension_validation(files, msg):
    with open('/home/data/registration/Raw/Exceptions.csv', 'a') as exp:
        data_writer = writer(exp)
        data_writer.writerow([files,msg])
        
for files in glob.glob('/home/data/registration/Raw/*'):
    Master = pd.read_csv('/home/data/registration/Raw/Master.csv')
    extension = files.split('/')[-1].split('.')[-1]
    master_checker = files.split('/')[-1].split('.')[0]
    if master_checker == 'Master' or master_checker == 'Exceptions' or master_checker == 'City':
        continue
    filename = files.split('/')[-1].split('_')[1]
    client_id = files.split('/')[-1].split('_')[0].lower()
    if files in list(Master['File_name']):
        print(files, 'Skipped :)\n')
        continue
    if extension == 'xlsx':
        print(files," is Excel\n")
        print('Working on', files)
        df = pd.read_excel(files)
    elif extension == 'csv':
        print(files," is CSV")
        print('Working on', files)
        df = pd.read_csv(files,index_col=False,low_memory=(False))
    else:
        print('Error in File Extension')
        extension_validation(files,'invalid file extension')
        continue
    df['DOB'] = pd.to_datetime(df['DOB'],dayfirst= True)
    print(df.head(5))
    default_columns_names = ['registrationnumber', 'Appl ID', 'Post_Course_Certificate', 'Gender', 'Category', 'DOB',
                             'Address State', 'Address_City_District', 'latitude', 'longitude', 'PWD Status',
                             'Preferred_Test_City 1', 'Preferred_Test_City 2', 'Preferred_Test_City 3', 'Appl_Status',
                             'Reg_Date', 'Pay_Resp_Dt', 'Allotted_Test_Center', 'Exam_Date', 'Exam_Batch']
    new_df = pd.DataFrame(columns=default_columns_names)
    print(new_df.head(5))
    for i in range(len(default_columns_names)):
        if default_columns_names[i] in df.columns:
            new_df[default_columns_names[i]] = df[default_columns_names[i]]
            new_df[default_columns_names[i]] = new_df[default_columns_names[i]].replace(['\'-'], None)
        else:
            new_df[default_columns_names[i]] = None

    new_df['Gender'] = new_df['Gender'].str.lower()
    print(new_df.head(10))
    condition = True
    for i in range(len(new_df['registrationnumber'])):
        # print(i)
        # Checking mandatory fields
        if not validate_reg_id(new_df.iloc[i]['registrationnumber']):
            print('File skipped due to some Error in REG ID field')
            add_exception(files, i, 'Expected length >5 and <20')
            condition = False
            break
        elif not validate_gender(new_df.iloc[i]['Gender']):
            print('File skipped due to some Error in Gender field')
            add_exception(files, i, 'Numerical/special characters found')
            condition = False
            break
        elif not validate_Appl_Status(new_df.iloc[i]['Appl_Status']):
            print('File skipped due to some Error in Appl. Status field')
            add_exception(files, i, 'Null value found')
            condition = False
            break
        
        if validate_dates(str(new_df.iloc[i]['DOB'])[0:10:]):
            print('File skipped due to some Error in DOB field')
            add_exception(files, i, 'PLease enter the Data in dd/mm/yyyy format')
            condition = False
            break
       
        try:
            if activate_geo_locator:
                location = findGeocode(str(new_df.iloc[i]['Address_City_District']))
                if not location:
                    new_df.loc[i, 'latitude'] = 0
                    new_df.loc[i, 'longitude'] = 0
                else:
                    new_df.loc[i, 'latitude'] = location[0]
                    new_df.loc[i, 'longitude'] = location[1]
        except Exception as E:
            print(E)
            new_df.loc[i, 'latitude'] = 0
            new_df.loc[i, 'longitude'] = 0
        #print(i, new_df.loc[i, 'longitude'])
    if condition:
        print(files, 'Transformed\n')
        new_df.insert(0, 'ClientName', client_id)
        #new_df.replace(' ',0.0, inplace =True)
        #new_df['longitude'] = new_df['longitude'].replace('',0.0, inplace = True)
        new_df.to_csv('/home/data/registration/Transformed/' + client_id + '_registration_new.csv',
                      index=False, header=False, mode='a')
        add_to_master(files)
    else:
        print(files,
              ' : Exception occurred, please check the Exception.csv file in your data directory for more info\n')
