# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_police_crime_type_month_count.py
DESCRIPTION:
                Databricks notebook with processing code to generate a table with the monthly crime counts by type at a UK street level
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        mattia.ficarelli@gmail.com
CREATED:        10.08.2022
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* geopy

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
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

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
snapshot_path = config_JSON['pipeline']['project']['snapshot_path']
snapshot_file = config_JSON['pipeline']['project']['snapshot_file']
sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file']
table_name = config_JSON['pipeline']['staging']['sink_table']  

# COMMAND ----------

#Pull snapshot dataset
#-----------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, snapshot_path)
snapshot_dataset = datalake_download(CONNECTION_STRING, file_system, snapshot_path+latestFolder, snapshot_file)
snapshot_dataframe = pd.read_parquet(io.BytesIO(snapshot_dataset), engine="pyarrow")

#Transfrom data and get postcodes from Longitude and Latitude
#------------------------------------------------------------
snapshot_dataframe_1 = snapshot_dataframe[['Month','Longitude','Latitude','Crime type',]]
snapshot_dataframe_3 = snapshot_dataframe_1[snapshot_dataframe_1['Longitude'].notna()].reset_index(drop = True)

geolocator = Nominatim(user_agent="open_access_nhs")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def get_zipcode(df, geolocator, latitude_column, longitude_column):
    location = geolocator.reverse((df[latitude_column], df[longitude_column]))
    return location.raw['address']['postcode']
geolocator = geopy.Nominatim(user_agent='dsir group project')

snapshot_dataframe_3['Postcode'] = snapshot_dataframe_3.apply(get_zipcode, axis=1, geolocator=geolocator, latitude_column='Latitude', longitude_column='Longitude')
snapshot_dataframe_4 = snapshot_dataframe_3[['Month', 'Postcode', 'Crime type']]
snapshot_dataframe_5 = snapshot_dataframe_4.groupby(['Month','Postcode', 'Crime type']).size().reset_index(name='Count')
snapshot_dataframe_5['Month'] = pd.to_datetime(snapshot_dataframe_5['Month'])
df_processed = snapshot_dataframe_5.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
df_processed.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

# Create PySpark DataFrame from Pandas DataFrame
# -------------------------------------------------------------------------

sparkDF=spark.createDataFrame(df_processed)

# Write data from databricks to SQL database
# -------------------------------------------------------------------------
server_name = dbutils.secrets.get(scope="sqldatabase", key="SERVER_NAME")
database_name = dbutils.secrets.get(scope="sqldatabase", key="DATABASE_NAME")
url = server_name + ";" + "databaseName=" + database_name + ";"
username = dbutils.secrets.get(scope="sqldatabase", key="USER_NAME")
password = dbutils.secrets.get(scope="sqldatabase", key="PASSWORD")

try:
  sparkDF.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()
except ValueError as error:
    print("Connector write failed", error)
