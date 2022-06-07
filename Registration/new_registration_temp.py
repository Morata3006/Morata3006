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
    try:
        geolocator = Nominatim(user_agent='NSEIT-DEX')
        loc = geolocator.geocode(city)
        loc = [loc.latitude, loc.longitude]
        with open('dex/City.csv', 'a',
                  newline='\n') as locations:
            data_writer = writer(locations)
            data_writer.writerow([city] + loc)
            locations.close()
        return loc
    except GeocoderTimedOut:
        return findGeocode[city]


for files in glob.glob('dex/client1.xlsx'):
    extension = files.split('/')[-1].split('.')[-1]
    filename = files.split('/')[-1].split('.')[0]

    if extension == 'xlsx':
        print(files," is Excel\n")
        print('Working on', files)
        df = pd.read_excel(files)
        # print(list(df.columns))
    df['Date of Birth'] = pd.to_datetime(df['Date of Birth'], dayfirst=True)
    default_columns_names = ['Registration Number', 'oum_user_id', 'Reference Number', 'oum_user_pk',
                             'Title', 'Applicant’s First Name:', 'Middle Name', 'Last Name', 'Applicant Full Name',
                             'Fathers name', 'Husbands name', 'Mothers name', 'Gender', 'Nationality', 'Email ID',
                             'Mobile No.', 'Are you eligible to avail the services of Scribe', 'Type of Transgender (In case of Transgender)',
                             'Date of Birth', 'Age as On 01-01-2021', 'Age as On 30-11-2020', 'Select PWD Category',
                             'Preferred Test City -1', 'Preferred Test City -2', 'Preferred Test City -3',
                             'Application Submitted Date', 'Local language selected for 10th', 'Local language selected for 12th',
                             'Are you claiming age relaxation as an In-service Contractual Employee vide Odisha Group-B posts (Contractual Appointment) Rules-2013 OR Odisha Group ‘C’ &  Group ‘D’ posts Contractual Appointment Rules-2013 ',
                             'Identity Card No', 'Do you possess NCC Certificate of type B or C?', 'Type of NCC Certificate',
                             'Are you an Ex-serviceman who has not served in any of the central or state government department after discharged from defence services?',
                             'Department', 'ESM - Date of Discharge', 'Duration of Service (in Years-Months-Days)', 'Post Applying for:',
                             'Are you Domicile of State?', 'ocd_domicilecertificate','ocd_domicileserial','Category (Caste)','ocd_cat_cert_iss_dt','ocd_cat_cert_val_dt',
                             'ocd_cat_serial','Are you Dependent of Freedom Fighter?','ocd_freedomfighterdt','ocd_freedomfighterserial	',
                             'ocd_freedom_authority','ocd_governmentemp','ocd_governmentempdt','Duration of Service','ocd_nocdt','ocd_nocserial	',
                             'ocd_noc_authority','Are you an Ex-serviceman?','ocd_esm_dt_of_enlistment','ocd_esm_dt_of_discharge','Permanent Address',
                             'Permanent State','Permanent District','Permanent City','Permanent Pincode','Correspondence Address same as Permanent Address',
                             'Correspondence Address','Correspondence State','Correspondence District','Correspondence City','Correspondence Pincode',
                             'Post preference -1','Post preference -2','Post preference -3','oaed_doeacc','oaed_terri_army','oaed_bcerti',
                             '10th/SSC Name of Board','10th/SSC Other Board','10th/SSC Month & Year of Passing','10th/SSC Result Status',
                             '10th/SSC Roll Number','10th/SSC Certificate Serial No','12th/HSC Name of Board','12th/HSC Other Board',
                             '12th/HSC Month & Year of Passing','12th/HSC Result Status','12th/HSC Roll Number','12th/HSC Certificate Serial No',
                             'Graduation Degree Name','Graduation Other Degree','Graduation University Name','Graduation Other University',
                             'Graduation Month & Year of Passing','Graduation Result Status','Graduation Roll Number','Graduation Certificate Serial No',
                             'Payment Status','Payement Mode','Payment amount','SBI / E-Challan Transaction ID','NSEIT Transaction ID',
                             'Payment Date','Status']

    new_df = pd.DataFrame(columns=default_columns_names)
    for i in range(len(default_columns_names)):
        if default_columns_names[i] in df.columns:
            new_df[default_columns_names[i]] = df[default_columns_names[i]]
            new_df[default_columns_names[i]] = new_df[default_columns_names[i]].replace(['\'-'], None)
        else:
            new_df[default_columns_names[i]] = None
    new_df['Gender'] = new_df['Gender'].str.lower()
    # for geo location of preferred test city -1
    for i in range(len(new_df['Registration Number'])):
        try:
            if activate_geo_locator:
                location = findGeocode(str(new_df.iloc[i]['Preferred Test City -1']))
                if not location:
                    new_df.loc[i, 'latitude'] = 0
                    new_df.loc[i, 'longitude'] = 0
                    new_df.loc[i, 'coordinates'] = str(0) + ' , ' + str(0)
                else:
                    new_df.loc[i, 'latitude'] = location[0]
                    new_df.loc[i, 'longitude'] = location[1]
                    new_df.loc[i, 'coordinates'] = str(location[0]) + ' , ' + str(location[1])
        except Exception as E:
            print(E)
            new_df.loc[i, 'latitude'] = 0
            new_df.loc[i, 'longitude'] = 0
            new_df.loc[i, 'coordinates'] = str(0) + ' , ' + str(0)
    new_df.insert(0, 'ClientName', 'client1')
    new_df.to_csv('dex/client1_registration_new.csv', index=False, header=False, mode='a')


