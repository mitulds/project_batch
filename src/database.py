import mysql.connector as mysql
from dotenv import load_dotenv
import os
import pandas as pd

# credentials --------------------------
load_dotenv(".env")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")


# parameters --------------------------
database_name = 'sales'
table_name = 'temperature'


#------------------------------------------------------------------------------
# STEP 1: CONNECT TO MYSQL - FUNCTION 01 -> shoule return mydb, cursor
try:
    mydb = mysql.connect(
    host="localhost",
    user=username,
    password=password
    )

    cursor = mydb.cursor()
except Exception as e:
    raise ConnectionError(f'failed to connect to mysql: {e}')


#------------------------------------------------------------------------------
# STEP 2: CREATE NEW DATABASE - FUNCTION 02 -> no return
    ## drop existing database 
drop_query = f'DROP DATABASE IF EXISTS {database_name}'
cursor.execute(drop_query)

create_query = f'CREATE DATABASE {database_name}'
cursor.execute(create_query)

show_db = 'SHOW DATABASES'
cursor.execute(show_db) # push
databases = cursor.fetchall() # pull
print(databases)


#STEP 3: READ CSV - FUNCTION 03 -> return dataframe ---------------------------------------------------------
df = pd.read_csv("data/stock_temp_agg_cleaned_data.csv")
df.info()


#STEP 4: CREATE SCHEMA - FUNCTION 04 -> return colname_dtype, placeholders---------------------------------------------------------
# NEED IN THIS FORMAT 
# CREATE TABLE table_name (
#     column1 datatype,
#     column2 datatype,
#     column3 datatype,
#    ....
# );
## STEP 1: CONVERT PYTHON DTYPES TO SQL DTYPES
## STEP 2: CONCAT COLUMN AND THEIR DATATYPE
## STEP 3: CREATE AS MANY PLACEHOLDERS AS THERE ARE COLUMNS IN THE DATASET
types = ""
for i, col_type in enumerate(df.dtypes):
    col_name = df.columns[i]
    col_name = col_name.replace(".", "_")
    if col_type == 'object':
        types += f'{col_name} VARCHAR(255), '
    elif col_type == 'int64':
        types += f'{col_name} INT, '
    elif col_type == 'float64':
        types += f'{col_name} DECIMAL(6,2), '
colname_dtype = types[:-2]
placeholders = ", ".join(['%s'] * len(df.columns))
 

# ---------------------------------------------------------
# STEP 5: CREATE NEW TABLE IN THE EXISTING DATABASE - FUNCTION 05 -> no return
cursor.execute(f'USE {database_name}') # connected to the database 

drop_table = f'DROP TABLE IF EXISTS {table_name}'
cursor.execute(drop_table)

create_table = f'CREATE TABLE {table_name} ({colname_dtype})'
cursor.execute(create_table)

#------------------------------------------------------------------------------
# STEP 6: INSERT DATA TO THE TABLE - FUNCTION 06 -> return rowcount
rows_inserted = 0
for _, row in df.iterrows():
    insert_data = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.execute(insert_data, tuple(row))
    if cursor.rowcount == 1:
        rows_inserted += 1
mydb.commit()
rows_inserted
df.shape

#------------------------------------------------------------------------------
# closes connections
cursor.close()
mydb.close()
