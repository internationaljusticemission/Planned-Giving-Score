#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 10:40:45 2024

@author: adampayne
"""

# In[1]:

from azure.identity import AzureCliCredential
import struct
import sqlalchemy
import pandas as pd
import numpy as np



# os.environ['PATH'] = '/opt/homebrew/Cellar/azure-cli/2.59.0/bin'
ods_server = 'ijmorg-sql-bi-ods-p01.database.windows.net'
ods_database = 'db-sfods-prod01'
ods_schema = 'CUBE'
src_table = 'transactions_summary'
ods_table = f'{ods_schema}.{src_table}'



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




# In[]:
    


def get_ods_dataframe(db, sql, table):
    with db.connect().execution_options(stream_results=True) as azure_db_con:
        sql_string = sql
        df = pd.read_sql_query(sqlalchemy.text(sql_string), azure_db_con)
    return df


db_conn = get_ods_conn(ods_server, ods_database)


contact_sql = "SELECT AccountId, Id,Phone, Email, Alumni_IJM__c, Prayer_Partner__c, MailingStreet, MailingCity,MailingState,MailingPostalCode,MailingCountry FROM NAM.VWContact WHERE MailingCountry IN ('United States', 'Canada')"
accounts_sql = "SELECT Id, Years_Donated__c, npo02__FirstCloseDate__c, npo02__LastCloseDate__c, npo02__NumberOfClosedOpps__c, Number_of_Years_Consecutively_Giving__c, npo02__OppsClosedThisYear__c, npo02__OppsClosedLastYear__c, npo02__OppsClosed2YearsAgo__c, Number_of_Gifts_3_Years_Ago__c, Number_of_Gifts_4_Years_Ago__c, Number_of_Gifts_5_Years_Ago__c, PFO_Partnership_Status__c, Family_Foundation__c, [Type], Rolling_Status__c, Freedom_Partner_Status__c,BillingStreet,BillingCity,BillingStateCode,BillingPostalCode,BillingCountryCode FROM NAM.VWAccount Where [Type] = 'Household' and BillingCountryCode IN ('US', 'CA')"
board_members_sql = "SELECT a.npe5__Contact__c, c.Id, c.AccountId FROM (SELECT npe5__Contact__c FROM [CNF].[VWnpe5__Affiliation__c] WHERE [npe5__Organization__c] = '001o00000110h2CAAQ' AND [RecordTypeId] = '012o0000000u0knAAA' AND [Title__c] = 'Board Member' AND [npe5__Status__c] = 'Former') a LEFT JOIN [NAM].[VWContact] c ON a.npe5__Contact__c = c.Id"
programs_sql = "SELECT m.Contact__c, m.Role__c, m.Start_Date__c, m.End_Date__c, m.Program__c, m.Name, m.Type__c, m.RecordTypeId, m.Program_Id, c.Id, c.AccountId FROM (SELECT NAM.VWMember__c.Contact__c, NAM.VWMember__c.Role__c, NAM.VWMember__c.Start_Date__c, NAM.VWMember__c.End_Date__c, NAM.VWMember__c.Program__c, CNF.VWPrograms__c.Name, CNF.VWPrograms__c.Type__c, CNF.VWPrograms__c.RecordTypeId, CNF.VWPrograms__c.Id AS Program_Id FROM NAM.VWMember__c LEFT JOIN CNF.VWPrograms__c ON NAM.VWMember__c.Program__c = CNF.VWPrograms__c.Id) m LEFT JOIN NAM.VWContact c ON m.Contact__c = c.Id"
volunteers_sql = "SELECT m.Contact__c, m.Role__c, m.Start_Date__c, m.End_Date__c, m.Program__c, m.Name, m.Type__c, m.RecordTypeId, m.Program_Id, c.Id, c.AccountId FROM (SELECT NAM.VWMember__c.Contact__c, NAM.VWMember__c.Role__c, NAM.VWMember__c.Start_Date__c, NAM.VWMember__c.End_Date__c, NAM.VWMember__c.Program__c, CNF.VWPrograms__c.Name, CNF.VWPrograms__c.Type__c, CNF.VWPrograms__c.RecordTypeId, CNF.VWPrograms__c.Id AS Program_Id FROM NAM.VWMember__c LEFT JOIN CNF.VWPrograms__c ON NAM.VWMember__c.Program__c = CNF.VWPrograms__c.Id WHERE CNF.VWPrograms__c.Name LIKE '%Volunteer%') m LEFT JOIN NAM.VWContact c ON m.Contact__c = c.Id"
daf_sql = "SELECT DISTINCT AccountID FROM NAM.VWOpportunity WHERE Type = 'Donor Advised Fund'"
stock_sql = "SELECT DISTINCT AccountID FROM NAM.VWOpportunity WHERE Type = 'Stock Gift'"
planned_gift_sql = "SELECT DISTINCT AccountID FROM NAM.VWOpportunity WHERE Type = 'Planned Gift' AND Amount > 0 AND StageName NOT IN ('Withdrawn', 'Closed Lost', 'Closed Won', 'Pledge Completed', 'Contractual Posted','refunded', 'voided', 'To Be Deleted')"
survey_response_sql = "SELECT a.Contact__c, a.Id AS SubmissionId, a.question_14_Would_you_consider_leaving__c, b.AccountId, b.Id AS ContactId FROM NAM.VWForm_Submissions__c a LEFT JOIN NAM.VWContact b ON a.Contact__c = b.Id WHERE a.Contact__c IS NOT NULL AND (a.question_14_Would_you_consider_leaving__c LIKE '%have%' OR a.question_14_Would_you_consider_leaving__c LIKE '%consider%') AND a.question_14_Would_you_consider_leaving__c NOT LIKE '%not%'"
business_owner_sql = "SELECT AccountId FROM NAM.VWContact WHERE Business_Owner__c NOT IN ('Unknown') AND Business_Owner__c IS NOT NULL"


contacts = get_ods_dataframe(db_conn, contact_sql, ods_table)
accounts = get_ods_dataframe(db_conn, accounts_sql, ods_table)
board_members = get_ods_dataframe(db_conn, board_members_sql, ods_table)
programs = get_ods_dataframe(db_conn, programs_sql, ods_table)
daf = get_ods_dataframe(db_conn, daf_sql, ods_table)
stock = get_ods_dataframe(db_conn, stock_sql, ods_table)
planned_gift = get_ods_dataframe(db_conn, planned_gift_sql, ods_table)
programs = get_ods_dataframe(db_conn, programs_sql, ods_table)
survey_response = get_ods_dataframe(db_conn, survey_response_sql, ods_table)
business_owner = get_ods_dataframe(db_conn, business_owner_sql, ods_table)

volunteer = programs[programs['Name'].str.contains('Volunteer')]
alumni = contacts[contacts['Alumni_IJM__c'] == 'Yes']
accounts.rename(columns={'Id':'AccountId'}, inplace=True)
planned_gift.rename(columns={'AccountID':'AccountId'}, inplace=True)
daf.rename(columns={'AccountID':'AccountId'}, inplace=True)
stock.rename(columns={'AccountID':'AccountId'}, inplace=True)
family_foundation = accounts[accounts['Family_Foundation__c'].notna()]




def Rolling_Status_Calc(row):
    if row['Rolling_Status__c'] == 'Active':
        return 5
    elif row['Rolling_Status__c'] =='Lapsed':
        return 3 
    else:
        return 0
accounts['Rolling Status Points'] = accounts.apply(Rolling_Status_Calc, axis=1)


def FP_Status_Calc(row):
    if row['Freedom_Partner_Status__c'] == 'Active':
        return 5
    elif pd.notna(row['Freedom_Partner_Status__c']) == True:
        return 1 
    else:
        return 0
    
accounts['FP Status Points'] = accounts.apply(FP_Status_Calc, axis=1)

def Consec_Giving_Calc(row):
    if row['Number_of_Years_Consecutively_Giving__c'] >=8 and row['Number_of_Years_Consecutively_Giving__c'] <=10:
        return 15
    elif row['Number_of_Years_Consecutively_Giving__c'] >=11 and row['Number_of_Years_Consecutively_Giving__c'] <=14:
        return 20
    elif row['Number_of_Years_Consecutively_Giving__c'] >=15:
        return 25
    else:
        return 0
accounts['Cosecutive_Giving_Points'] = accounts.apply(Consec_Giving_Calc, axis=1)

accounts['Recent Giving Calc'] =(np.where(accounts['npo02__OppsClosedThisYear__c'] >=1, 1, 0)
+ np.where(accounts['npo02__OppsClosedLastYear__c'] >=1, 1, 0)
+ np.where(accounts['npo02__OppsClosed2YearsAgo__c'] >=1, 1, 0)
+ np.where(accounts['Number_of_Gifts_3_Years_Ago__c'] >=1, 1, 0)
+ np.where(accounts['Number_of_Gifts_4_Years_Ago__c'] >=1, 1, 0)
+ np.where(accounts['Number_of_Gifts_5_Years_Ago__c'] >=1, 1, 0))

accounts['Recent Giving Points'] = np.where(accounts['Recent Giving Calc']>=3,5,0)

accounts['Early Donor Points'] = np.where(accounts['npo02__FirstCloseDate__c'] <= pd.to_datetime('2004-12-31'), 5, 0)

accounts['Family Foundation Points'] = np.where(accounts['Family_Foundation__c'].isnull(),0,-15)


def Years_Donated_Points(row):
    if row['Years_Donated__c'] >=10:
        return 13
    elif row['Years_Donated__c'] >= 8:
        return 10
    else:
        return 0




accounts['Years_Donated__c'] = accounts['Years_Donated__c'].str.len()/5 + .2

accounts['Giving Years Points']= accounts.apply(Years_Donated_Points, axis=1)

board_members['Board Member Points']=5
board_members = board_members.drop_duplicates(subset=['AccountId'], keep='first')
board_members.drop(['npe5__Contact__c','Id'], axis = 1, inplace=True)

alumni = alumni.drop_duplicates(subset=['AccountId'], keep='first')
alumni['Alumni Points']=5
alumni = alumni[['AccountId','Alumni Points']]

planned_gift['Planned Gifts Points']=25

stock['Stock Gifts Points']=7

daf = daf.drop_duplicates(subset=['AccountId'], keep='first')
daf['DAF Points']=10

survey_response = survey_response.drop_duplicates(subset=['AccountId'], keep='first')
survey_response['Consider PG Points']=np.where(survey_response['question_14_Would_you_consider_leaving__c'].str.contains('have'), 20, 10)
survey_response.drop(['question_14_Would_you_consider_leaving__c'], axis=1, inplace=True)

business_owner = business_owner.drop_duplicates(subset=['AccountId'], keep='first')
business_owner['Business Owner Points']=10

volunteer = volunteer.drop_duplicates(subset=['AccountId'], keep='first')
volunteer['Volunteer Leader Points']=5
volunteer = volunteer[['AccountId','Volunteer Leader Points']]

family_foundation['Family Foundation Points'] = -15
family_foundation = family_foundation[['AccountId','Family Foundation Points']]



accounts_final=pd.merge(accounts, alumni, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, board_members, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, daf, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, planned_gift, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, stock, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, survey_response, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, business_owner, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, volunteer, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, family_foundation, on='AccountId', how='left')


accounts_final=accounts_final.replace(np.nan,0)

accounts_final['Planned Giving Score']=(accounts_final['Rolling Status Points']+accounts_final['FP Status Points']
+accounts_final['Cosecutive_Giving_Points']+accounts_final['Recent Giving Points']+accounts_final['Early Donor Points']
+accounts_final['Family Foundation Points']+accounts_final['Giving Years Points']+accounts_final['Board Member Points']
+accounts_final['Alumni Points']+accounts_final['Planned Gifts Points']+accounts_final['Stock Gifts Points']+accounts_final['DAF Points']
+accounts_final['Consider PG Points'])

accounts_final=accounts_final[['AccountId','Planned Giving Score']]


# accounts_final.to_csv('{}/OneDrive - International Justice Mission/Python Projects/Planned Giving Score/Planned Giving Score.csv'.format(home))













