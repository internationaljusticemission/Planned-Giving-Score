#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 17:02:44 2025

@author: lta
"""
import azure.identity
import struct
import sqlalchemy
import pandas as pd
import os

def get_ods_conn(server, database, intent):
    driver = '{ODBC Driver 18 for SQL Server}'
    try:
        credential = azure.identity.AzureCliCredential()
        database_token = credential.get_token('https://database.windows.net/')
    except azure.identity._exceptions.CredentialUnavailableError:
        print('\nError in Azure credentials (try "az login")')
        exit()

    # get bytes from token obtained
    tokenb = bytes(database_token[0], "UTF-8")
    exptoken = b''
    tokenstruct = None
    for i in tokenb:
        exptoken += bytes({i})
        exptoken += bytes(1)
        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

    conn_string = "Driver=" + driver + ";SERVER=" + server + ";DATABASE=" + database + ";ApplicationIntent=" + intent
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    return sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={0}".format(conn_string),
        connect_args={'attrs_before': {SQL_COPT_SS_ACCESS_TOKEN: tokenstruct}},
        fast_executemany=True,
        pool_size=10,
        max_overflow=20
    )

def get_ods_data(db, sql_input):
    if isinstance(sql_input, list):
        sql_query = ' '.join(sql_input)
    elif os.path.isfile(sql_input):
        with open(sql_input) as fpr:
            sql_query = fpr.read()
    else:
        sql_query = sql_input.strip()
    try:
        with (db.connect().execution_options(stream_results=True) as azure_db_con):
            df = pd.read_sql_query(sqlalchemy.text(sql_query), azure_db_con)
        return df
    except sqlalchemy.exc.ProgrammingError:
        print('\nError in database connection (check VPN) or SQL query (check syntax)')
        exit()
