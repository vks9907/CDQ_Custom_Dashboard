# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import csv
import os
import sys
import datetime
import shutil
import glob

# In case you want to login through Security Assertion Markup Language (SAML) token, please refer this article - https://docs.informatica.com/integration-cloud/cloud-data-integration/current-version/rest-api-reference/platform-rest-api-version-2-resources/loginsaml.html
# Set IICS Credentials & Output file name Variables here
POD_region = ""
username = ""
password = ""
csv_file_path = r"C:\Users\vivesingh\Documents\python_csv_files"
Profile_Metadata_Details = "Profile_Metadata_Details.csv"
Column_Profiling_Result = "Column_Profiling_Result.csv"
Profile_execution_stats = "Profile_Execution_Stats.csv"
Top_n_value_frequency_data = "Top_Value_Frequency_Data.csv"
Profile_rule_output_fields = "Profile_Rule_Output_Fields.csv"

# History data capture duration
history_rec_count = 10

# If Folder doesn't exist then create
if not os.path.exists(csv_file_path):
    print('Folder does not exist. Creating ...')
    os.makedirs(csv_file_path)
    # Change working directory to create CSV files
    os.chdir(csv_file_path)
else:
    # Change working directory to create CSV files
    print('Folder already exist')
    os.chdir(csv_file_path)

CSV_Files = glob.glob('*.{}'.format('csv'))
Archive_Folder = 'CSV_File_Archive_'+ datetime.datetime.today().strftime('%Y%m%d%H%M%S')

# If CSV file exists in the folder then Archive the files to a sub folder
if len(CSV_Files) > 0:
    print('Existing CSV files found. Creating Archive folder with name ' + Archive_Folder)
    os.makedirs(Archive_Folder)
    for file in CSV_Files:
        print('Moving ' + file + 'to ' + Archive_Folder)
        shutil.move(file, os.path.join(Archive_Folder,file))
    print('All files moved. Archiving folder ' + Archive_Folder)
    try:
        shutil.make_archive(Archive_Folder, 'zip', Archive_Folder)
    except:
        print('Archival failed....')
    else:
        shutil.rmtree(Archive_Folder)
        print('Archival Successful....')
else:
    print('No Existing CSV file found. Skipping Archival process...')

# Define variables to use in the process
i_p_stats_result = 0
i_runkey_metadata = 0
i_runkey_outfield = 0
i_val_freq = 0


# Function to return session ID and Generate Profiling URLs
def get_session_id():
 login_api_url = "https://dm-"+POD_region+".informaticacloud.com/ma/api/v2/user/login"
 credentials = {
 "username": username,
 "password": password
 }
 login_headers = {
 "Accept": "application/json",
 "Content-Type": "application/json"
 }
 login_response = requests.request("POST", login_api_url, json=credentials, headers=login_headers)
 serverUrl = login_response.json()['serverUrl']
 get_session_id.profile_api_url = serverUrl.split('.')[0]+"-dqprofile.dm-"+POD_region+".informaticacloud.com/profiling-service/api/v1/profile"
 get_session_id.profile_column_api_url = serverUrl.split('.')[0]+"-dqprofile.dm-"+POD_region+".informaticacloud.com/metric-store/api/v1/odata/Profiles"
 get_session_id.frs_object_api_url = serverUrl.split('.')[0]+".dm-"+POD_region+".informaticacloud.com/frs/v1/Documents('"
 get_session_id.profile_execution_api_url = serverUrl.split('.')[0]+"-dqprofile.dm-"+POD_region+".informaticacloud.com/profiling-service/api/v1/runDetail?profileId="
 return login_response.json()['icSessionId']

# Get Session ID and Profiling URLs using above function
session_id = get_session_id()
profile_api_url = get_session_id.profile_api_url
profile_column_api_url = get_session_id.profile_column_api_url
frs_object_api_url = get_session_id.frs_object_api_url
profile_execution_api_url = get_session_id.profile_execution_api_url


# Function to get list of profiles with metadata information - START
def get_profile_list ():
 profile_list_headers = {
    "Accept": "application/json",
    "IDS-SESSION-ID": session_id
 }
 profile_list_response = requests.request("GET", profile_api_url, headers=profile_list_headers)
 return profile_list_response.json()
# function to get list of profiles with metadata information - END

# function to get frs object details - START
def get_frs_object_details(in_frs_id):
    url = frs_object_api_url + in_frs_id + "')?$expand=userInfo"
    headers = {
        "Accept": "application/json",
        "IDS-SESSION-ID": session_id
    }
    response = requests.request("GET", url, headers=headers).json()
    array_dict = {
        "id" : response["id"],
        "name" : response["name"],
        "createdTime" : response["createdTime"],
        "lastUpdatedTime" : response["lastUpdatedTime"],
        "lastAccessedTime" : response["lastAccessedTime"],
        "Object_Valid_Status" : response["documentState"]
        }
    for loop_1 in response['parentInfo']:
        if loop_1['parentType'] == 'Project':
            array_dict.update({"Project_Name" : loop_1['parentName']})
        elif loop_1['parentType'] == 'Folder':
            array_dict.update({"Folder_Name" : loop_1['parentName']})
    try :
        for loop_2 in response['customAttributes']['stringAttrs']:
            if loop_2['name'] == 'DIMENSION':
                array_dict.update({"Rule_DIMENSION" : loop_2['value']})
    except Exception as e :
        array_dict.update({"Rule_DIMENSION" : ''})
    return array_dict
# function to get frs object details - END

# Function to get Profile latest run key + Rule tagged to a profile, Project, Folder information for a profile/rule
def get_latest_run_key_and_metadata(in_profile_id) :
    global i_runkey_metadata
    global i_runkey_outfield
    profile_api_url_1 = profile_api_url + '/' + in_profile_id
    profile_list1_headers = {
       "Accept": "application/json",
       "IDS-SESSION-ID": session_id
    }
    response = requests.request("GET", profile_api_url_1, headers=profile_list1_headers).json()
    get_frs_object_details_response_ = get_frs_object_details(response["frsId"])
    for loop_1 in response['profileableFields']:
        array_dict = {
        "profile_id" : response["id"],
        "profileKey" : response["profileKey"],
        "Profile_lastRunKey" : response["lastRunKey"],
        "Profile_name" : response["name"],
        "Profile_description" : response["description"],
        "profile_orgId" : response["orgId"],
        #"profile_frsId" : response["frsId"],
        #"profile_frsProjectId" : response["frsProjectId"],
        #"profile_frsFolderId" : response["frsFolderId"],
        "profile_connectionId" : response["connectionId"],
        #"profile_createdBy" : response["createdBy"],
        "profile_createdByName" : response["createdByName"],
        #"profile_updatedBy" : response["updatedBy"],
        "profile_updatedByName" : response["updatedByName"],
        "Profile_createdTime" : datetime.datetime.fromtimestamp(int(response["createTime"])/1000).strftime("%Y-%m-%d %H:%M:%S"),
        "Profile_lastUpdatedTime" : datetime.datetime.fromtimestamp(int(response["updateTime"])/1000).strftime("%Y-%m-%d %H:%M:%S"),
        #"Profile_createdTime":get_frs_object_details_response_['createdTime'],
        #"Profile_lastUpdatedTime":get_frs_object_details_response_['lastUpdatedTime'],
        "Profile_lastAccessedTime":get_frs_object_details_response_['lastAccessedTime'],
        "Profile_Valid_Status":get_frs_object_details_response_['Object_Valid_Status'],
        "Profile_Project_Name":get_frs_object_details_response_['Project_Name'],
        "Profile_Folder_Name":get_frs_object_details_response_.get('Folder_Name',''),
        "profile_isFilterEnabled" : response["isFilterEnabled"],
        "source_dataset" : response["source"]['name'],
        "source_dataset_type" : response["source"]['dataSourceType'],
        "sampling_type" : response["samplingOptions"]['samplingType'],
        "sampling_rows" : response["samplingOptions"]['rows'],
        "drillDownType" : response["drillDownType"]
        }
        header = loop_1.keys()
        if 'inputFieldMappings' in header:
            get_frs_object_details_response = get_frs_object_details(loop_1['frsId'])
            for input_field_mapping in loop_1['inputFieldMappings']:
                array_dict.update({
                "column_id": input_field_mapping['id'],
                "column_name":input_field_mapping['dataSourceFieldName'],
                 "ruleType":loop_1['ruleType'],
                 "rule_frs_id":get_frs_object_details_response['id'],
                 "rule_name":get_frs_object_details_response['name'],
                 "rule_Dimension":get_frs_object_details_response.get('Rule_DIMENSION',''),
                 "rule_createdTime":get_frs_object_details_response['createdTime'],
                 "rule_lastUpdatedTime":get_frs_object_details_response['lastUpdatedTime'],
                 "rule_lastAccessedTime":get_frs_object_details_response['lastAccessedTime'],
                 "rule_Valid_Status":get_frs_object_details_response['Object_Valid_Status'],
                 "rule_Project_Name":get_frs_object_details_response['Project_Name'],
                 "rule_Folder_Name":get_frs_object_details_response.get('Folder_Name',''),
                 "Key_PID_RFRS_ID" : response["id"] + get_frs_object_details_response['id']
                 })
                df = pd.DataFrame(array_dict,index=[0])
                Profile_Metadata_Details_outfile = open(Profile_Metadata_Details, 'a');
                df.to_csv(Profile_Metadata_Details_outfile, index=False, header=(i_runkey_metadata==0),lineterminator='\n')
                Profile_Metadata_Details_outfile.close()
                i_runkey_metadata += 1
            
            for output_field_mapping in loop_1['outputFieldMappings']:
                array_dict_1 = {
                "profile_id" : response["id"],
                "Profile_name" : response["name"],
                "Profile_lastRunKey" : response["lastRunKey"],
                 "rule_frs_id":get_frs_object_details_response['id'],
                 "rule_out_field_id":output_field_mapping['id'],
                 "rule_out_field":output_field_mapping['outFieldName'],
                 "Key_PID_RCOLID": response["id"] + output_field_mapping['id'],
                 "Key_PID_RFRSID": response["id"] + get_frs_object_details_response['id']
                 }
                df = pd.DataFrame(array_dict_1,index=[0])
                Profile_rule_output_fields_outfile = open(Profile_rule_output_fields, 'a');
                df.to_csv(Profile_rule_output_fields_outfile, index=False, header=(i_runkey_outfield==0),lineterminator='\n')
                Profile_rule_output_fields_outfile.close()
                i_runkey_outfield += 1
                
        else:
            array_dict.update({
            "column_id": loop_1["id"],
            "column_name": loop_1["fieldName"],
            "ruleType":'',
            "rule_frs_id":'',
            "rule_name":'',
            "rule_Dimension":'',
            "rule_createdTime":'',
            "rule_lastUpdatedTime":'',
            "rule_lastAccessedTime":'',
            "rule_Valid_Status":'',
            "rule_Project_Name":'',
            "rule_Folder_Name":'',
            "Key_PID_RFRS_ID" : ''
             })
            df = pd.DataFrame(array_dict,index=[0])
            Profile_Metadata_Details_outfile1 = open(Profile_Metadata_Details, 'a');
            df.to_csv(Profile_Metadata_Details_outfile1, index=False, header=(i_runkey_metadata==0),lineterminator='\n')
            Profile_Metadata_Details_outfile1.close()
            i_runkey_metadata += 1
    return response["lastRunKey"]
    #i += 1
# Function to get latest run key + Rule tagged to a profile, Project, Folder information for a profile/rule - END

# Function to get column profiling result - as seen on profiling page - START
def get_column_profiling_result (in_profile_id, in_profile_name, in_run_key, in_i, in_file_name, in_seq):
    column_profiling_api_url = profile_column_api_url + '(%27' + in_profile_id + '%27)' + '/Columns?$top=100&runKey=' + str(in_run_key)
    headers = {
        "Accept": "application/json",
        "IDS-SESSION-ID": session_id
     }
    column_profiling_api_response = requests.request("GET", column_profiling_api_url, headers=headers).json()['value']
    column_profiling_output = json.loads(json.dumps(column_profiling_api_response, sort_keys=True))
    df = pd.DataFrame(column_profiling_output)
    df.insert(0, 'Key_PID_RKEY', in_profile_id + str(in_run_key))
    df.insert(1, 'Record_Type', in_seq)
    df.insert(2, 'Profile_id', in_profile_id)
    df.insert(3, 'Profile_name', in_profile_name)
    # Write only for Table columns, not for Rules (Rule information will be captured in Top N value frequency)
    df_DATASOURCEFIELD = df[df['columnType'] == "DATASOURCEFIELD"]
    in_file_name_outfile = open(in_file_name, 'a');
    df_DATASOURCEFIELD.to_csv(in_file_name_outfile, index=False, mode='a', header=(in_i==0),lineterminator='\n')
    in_file_name_outfile.close()
    df_MAPPLETFIELD = df[df['columnType'] == "MAPPLETFIELD"]
    in_file_name_new = in_file_name + "_temp"
    in_file_name_new_outfile = open(in_file_name_new, 'a');
    df_MAPPLETFIELD.to_csv(in_file_name_new_outfile, index=False, mode='a', header=(in_i==0), columns=['Record_Type','Profile_id','Profile_name','columnId','columnName','runKey'],lineterminator='\n')
    in_file_name_new_outfile.close()
# Function to get column profiling result - as seen on profiling page - END


# Function to Get Top N value frequencies by column id - START
def get_top_n_value_frequency (in_Record_Type,in_profile_id, in_profile_name, in_column_id, in_column_name, in_runkey, in_file_name,in_i):
    value_frequency_api_url =   profile_column_api_url + '(%27' + in_profile_id + '%27)' + '/Columns' + '(%27' + in_column_id + '%27)/ValueFrequencies?runKey=' + str(in_runkey)
    headers = {
        "Accept": "application/json",
        "IDS-SESSION-ID": session_id
    }
    value_frequency_api_response = requests.request("GET", value_frequency_api_url, headers=headers)
    value_frequency_output = json.dumps(value_frequency_api_response.json()['value'], sort_keys=True)
# del value_frequency_output['@odata.context']
    sorted_json2 = json.loads(value_frequency_output)
    df = pd.DataFrame(sorted_json2)
    df.insert(0, 'Key_PID_RKEY', in_profile_id + str(in_runkey)),
    df.insert(1, 'Key_PID_RCOLID', in_profile_id + in_column_id),
    df.insert(2, 'Record_Type', in_Record_Type)
    df.insert(3, 'Profile_id', in_profile_id)
    df.insert(4, 'Profile_name', in_profile_name)
    df.insert(5, 'rule_out_field_id', in_column_id)
    df.insert(6, 'Rule_Out_ColumnName', in_column_name)
    df.insert(7, 'Profile_Run_Key', in_runkey)
    # Write only for Table columns, not for Rules (Rule information will be captured in Top N value frequency)
    in_file_name1_outfile = open(in_file_name, 'a');
    df.to_csv(in_file_name1_outfile, index=False, mode='a', header=(in_i==0),lineterminator='\n')
    in_file_name1_outfile.close()
# Function to Get Top N value frequencies by column id - END

# Function to capture profile execution stats - Start
def get_profile_execution_stats(in_profile_id,in_profile_name,in_run_key,in_i,in_file_name,in_Loop_1):
    in_run_key = str(in_run_key)
    profile_execution_stats_api_url = profile_execution_api_url + in_profile_id
    headers = {
        "Accept": "application/json",
        "IDS-SESSION-ID": session_id,
        "runKey": in_run_key
    }
    response = requests.request("GET", profile_execution_stats_api_url, headers=headers).json()
    profile_start_time = datetime.datetime.fromtimestamp(int(response[0]['startTime'])/1000).strftime("%Y-%m-%d %H:%M:%S")
    profile_end_time = datetime.datetime.fromtimestamp(int(response[0]['endTime'])/1000).strftime("%Y-%m-%d %H:%M:%S")
    profile_execution_Time = response[0]['executionTime']
    rows_Processed = response[0]['rowsProcessed']
    run_status = response[0]['status']
    runCostInMB = response[0]['runCostInMB']
    difference = (datetime.datetime.fromtimestamp(int(response[0]['endTime'])/1000) - datetime.datetime.fromtimestamp(int(response[0]['startTime'])/1000)).total_seconds()
    array_dict = {
        "Record_Type" : in_Loop_1,
        "profile_id" : in_profile_id,
        "profile_name" : in_profile_name,
        "profile_run_key" : in_run_key,
        "profile_start_time" : profile_start_time,
        "profile_end_time" : profile_end_time,
        "profile_execution_Time" : difference,
        #"profile_execution_Time" : profile_execution_Time,
        "rows_Processed" : rows_Processed,
        "run_status" : run_status,
        "runCostInMB" : runCostInMB,
        "Key_PID_RKEY" : in_profile_id + in_run_key
        }
    df = pd.DataFrame(array_dict,index=[0])
    in_file_name2_outfile = open(in_file_name, 'a');
    df.to_csv(in_file_name2_outfile, index=False, mode='a', header=(in_i==0),lineterminator='\n')
    in_file_name2_outfile.close()
    #print(df)
# Function to capture profile execution stats - End



# Call the functions and write data into CSV

# Iterate through each Profile ID
print('Getting list of profiles...')
for profile_id_out in get_profile_list():
    
    # Capture Latest run key with Metadata details for each profile in a CSV file
    print('Capturing Latest run key with Metadata details for each profile in a CSV file...')
    latest_run_key = get_latest_run_key_and_metadata(profile_id_out['id'])
    #i_runkey_metadata += 1
    #i_runkey_outfield += 1
    
    # Capture Colume Profiling Result Data for each profile in a CSV file - Upto Last 10 runs
    print('Capturing Colume Profiling Result Data for each profile in a CSV file - Upto Last 10 runs')
    for Loop_1 in range(min(history_rec_count, latest_run_key)):
            profile_run_key = latest_run_key-Loop_1
            if Loop_1 == 0 :
                Record_Type = "Latest Run"
            elif Loop_1 == 1 :
                Record_Type = "Previous Run"
            else :
                Record_Type = "Previous Run - " + str(Loop_1-1)
            get_profile_execution_stats(profile_id_out['id'], profile_id_out['name'], profile_run_key, i_p_stats_result, Profile_execution_stats,Record_Type)
            try :
                get_column_profiling_result(profile_id_out['id'], profile_id_out['name'], profile_run_key, i_p_stats_result, Column_Profiling_Result, Record_Type)
                i_p_stats_result += 1
            except Exception as e :
                continue

# Capture Top-N Value Frequency Result Data for each profile and only mapplet columns in a CSV file - Upto previous 9 runs
print('Capturing Top-N Value Frequency Result Data for each profile and only mapplet columns in a CSV file - Upto previous 9 runs')
temp_file_name = Column_Profiling_Result + "_temp"
col_list = ["Record_Type","Profile_id", "Profile_name","columnId","columnName","runKey"]
df = pd.read_csv(temp_file_name, usecols=col_list)
for ind in df.index:
    get_top_n_value_frequency(df['Record_Type'][ind],df['Profile_id'][ind],df['Profile_name'][ind],df['columnId'][ind],df['columnName'][ind],df['runKey'][ind],Top_n_value_frequency_data,i_val_freq)
    # Increment variable for skipping header for second record onwards
    i_val_freq += 1

print('Deleting temporary files')
# Delete temporary files
os.remove(temp_file_name)