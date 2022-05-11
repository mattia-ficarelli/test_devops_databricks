# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           analytics_github_gitlab_openrepos_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Number of health and care projects shared in open repositories (M027)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        24 Nov 2021
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
file_name_config = "config_openrepos_dbrks.json"
file_system_config = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = dbutils.secrets.get(scope='AzureDataLake', key="DATALAKE_CONTAINER_NAME")
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file']

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_csv(io.BytesIO(file))
df['Date'] = pd.to_datetime(df['Date'])
df['Number of new open repositories'] = df['Number of new open repositories'].astype(int)
df['Cumulative number of open repositories'] = df['Cumulative number of open repositories'].astype(int)
df.sort_values(by=['Date'])
df1 = df.set_index('Unique ID')
df_processed = df1.copy()

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

table_name = config_JSON['pipeline']['staging']['sink_table']
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
