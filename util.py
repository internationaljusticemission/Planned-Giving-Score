#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 17:02:44 2025

@author: lta
"""
from azure.identity import AzureCliCredential
import struct
import sqlalchemy
import pandas as pd


def get_ods_conn(server, database):
    driver = '{ODBC Driver 18 for SQL Server}'
    credential = AzureCliCredential()
    database_token = credential.get_token('https://database.windows.net/')
    # get bytes from token obtained
    tokenb = bytes(database_token[0], "UTF-8")
    exptoken = b''
    tokenstruct = None
    for i in tokenb:
        exptoken += bytes({i})
        exptoken += bytes(1)
        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
    conn_string = "Driver=" + driver + ";SERVER=" + server + ";DATABASE=" + database + ";ApplicationIntent=ReadWrite"
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    return sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={0}".format(conn_string),
        connect_args={'attrs_before': {SQL_COPT_SS_ACCESS_TOKEN: tokenstruct}},
        fast_executemany=True,
        pool_size=10,
        max_overflow=20
    )


def get_ods_data(db, sql_file):
    sql_path = f'./sql/{sql_file}'
    try:
        with open(sql_path) as fpr:
            query_string = fpr.read()
            with (db.connect().execution_options(stream_results=True) as azure_db_con):
                df = pd.read_sql_query(sqlalchemy.text(query_string), azure_db_con)
        return df
    except FileNotFoundError:
        # print(f'SQL file sql{slash}{sql_file} not found')
        print(f'SQL file {sql_file} not found')
        exit()