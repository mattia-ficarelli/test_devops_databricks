# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_police_data_raw.py
DESCRIPTION:
                Databricks notebook with code to ingest new raw data and append to historical
                data for the NHSX Analyticus unit metric: Number of health and care projects shared in open repositories (M027)
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        mattia.ficarelli@gmail.com
CREATED:        24 Nov. 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install requests pandas pathlib azure-storage-file-datalake zipfile36 urllib3 lxml regex pyarrow==5.0.*

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
from io import StringIO, BytesIO, TextIOWrapper
from datetime import datetime

# 3rd party:
import json
import pandas as pd
from pathlib import Path
from urllib.request import urlopen
import zipfile
from azure.storage.filedatalake import DataLakeServiceClient

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONNECTION_STRING")

# COMMAND ----------

def datalake_download(CONNECTION_STRING, file_system, source_path, source_file):
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    directory_client = file_system_client.get_directory_client(source_path)
    file_client = directory_client.get_file_client(source_file)
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    return downloaded_bytes

def datalake_upload(file, CONNECTION_STRING, file_system, sink_path, sink_file):
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    directory_client = file_system_client.get_directory_client(sink_path)
    file_client = directory_client.create_file(sink_file)
    file_length = file_contents.tell()
    file_client.upload_data(file_contents.getvalue(), length=file_length, overwrite=True)
    return '200 OK'
  
def datalake_latestFolder(CONNECTION_STRING, file_system, source_path):
  try:
      service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
      file_system_client = service_client.get_file_system_client(file_system=file_system)
      pathlist = list(file_system_client.get_paths(source_path))
      folders = []
      # remove file_path and source_file from list
      for path in pathlist:
        folders.append(path.name.replace(source_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
        folders.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"), reverse=True)
      latestFolder = folders[0]+"/"
      return latestFolder
  except Exception as e:
      print(e)

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_policedata_crime.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
historical_path = config_JSON['pipeline']['raw']['sink_path']
historical_file = config_JSON['pipeline']['raw']['sink_file']
snapshot_path = config_JSON['pipeline']['raw']['snapshot_path']
snapshot_file = config_JSON['pipeline']['raw']['snapshot_file']

# COMMAND ----------

#Pull historical dataset
#-----------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_path+latestFolder, historical_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

#Load latest data from Police UK
#-------------------------------
resp = urlopen('http://data.police.uk/data/archive/latest.zip')
zip = zipfile.ZipFile(BytesIO(resp.read()))
file_list = zip.namelist()
filtered_list = [street_file for street_file in file_list if 'city-of-london-street' in street_file]
new_df = pd.DataFrame()
for data in filtered_list:
    file = zip.open(data)
    df = pd.read_csv(file)
    new_df = new_df.append(df)
final_df_new = new_df.reset_index(drop = True)

# COMMAND ----------

#If there is new data upload it to the datalake and append it to existing data
#-----------------------------------------------------------------------------
historical_dates = historical_dataframe['Month'].unique()
new_df = final_df_new[~final_df_new["Month"].isin(historical_dates)]
new_df = new_df.reset_index(drop = True)
if len(new_df) != 0:
    # Upload snapshot to datalake
    #-----------------------------------------------------
    current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
    file_contents = io.BytesIO()
    new_df.to_parquet(file_contents, engine="pyarrow")
    datalake_upload(file_contents, CONNECTION_STRING, file_system, snapshot_path+current_date_path, snapshot_file)
    
    # append new snapshot to historical file and upload to datalake
    #--------------------------------------------------------------
    historical_dataframe = historical_dataframe.append(new_df).reset_index(drop = True)
    file_contents = io.BytesIO()
    historical_dataframe.to_parquet(file_contents, engine="pyarrow")
    datalake_upload(file_contents, CONNECTION_STRING, file_system, historical_path+current_date_path, historical_file)
    
else:
    print('Data already exists')
