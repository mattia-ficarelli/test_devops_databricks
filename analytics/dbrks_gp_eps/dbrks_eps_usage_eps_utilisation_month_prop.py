# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_eps_usage_eps_utilisation_month_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: EPS utilisation (% all items) (M084)
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        04 October 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.*

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json

# 3rd party:
import pandas as pd
import numpy as np
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Shared/prod/au-azure-databricks/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_gp_eps_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']  

# COMMAND ----------

#Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df1 = df[['ODS Code', 'EPS Items', 'All Items', 'EPS Utilisation', 'Date']]
df1['EPS Utilisation'] = df1['EPS Utilisation'].str.replace("%", "")
df1['EPS Utilisation'] = (pd.to_numeric(df1['EPS Utilisation'])/100)
df1['Date'] = pd.to_datetime(df1['Date'])
df1.rename(columns = {'ODS Code': 'Practice code', 'EPS Items': 'Number of EPS items', 'All Items':'All items', 'EPS Utilisation': '% EPS utilisation'}, inplace=True)
df1.index.name = "Unique ID"
df_processed = df1.copy()

# COMMAND ----------



# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)

# COMMAND ----------

# Create PySpark DataFrame from Pandas DataFrame
# -------------------------------------------------------------------------

sparkDF=spark.createDataFrame(df_processed)

# Write data from databricks to dev SQL database
# -------------------------------------------------------------------------

server_name = dbutils.secrets.get(scope="sqldatabase", key="SERVER_NAME")
database_name = dbutils.secrets.get(scope="sqldatabase", key="DATABASE_NAME")

url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = config_JSON['pipeline']['staging'][1]['sink_table']
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
