Configure CLI to databricks workspace
-------------------------------------------------------
databricks configure –token
workspace URL - https://adb-6934477382817977.17.azuredatabricks.net/
workspace Token - dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

Configure AzureDataLake for databricks workspace
--------------------------------------------------------
databricks secrets create-scope --scope AzureDataLake --initial-manage-principal "users"

databricks secrets put --scope AzureDataLake --key DATALAKE_CONNECTION_STRING
DefaultEndpointsProtocol=XXXXXXXXXXXXXXX

databricks secrets put --scope AzureDataLake --key DATALAKE_CONTAINER_NAME
teststorageaccountmf

Configure SQL Database for databricks workspace
--------------------------------------------------------
databricks secrets create-scope --scope sqldatabase --initial-manage-principal "users"

databricks secrets put --scope sqldatabase --key SERVER_NAME
jdbc:sqlserver://testsqlservermf.database.windows.net

databricks secrets put --scope sqldatabase --key DATABASE_NAME
testsqldatabase

databricks secrets put --scope sqldatabase --key USER_NAME
mattiaficarelli

databricks secrets put --scope sqldatabase --key PASSWORD
XXXXXXXXXXXXXXXXXX
