#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 10:40:45 2024
@author: adampayne
Account.RE_Planned_Giving_Score__c
"""

# In[1]:

import pandas as pd
import numpy as np
from util import get_ods_conn, get_ods_dataframe

# os.environ['PATH'] = '/opt/homebrew/Cellar/azure-cli/2.59.0/bin'
ods_server = 'ijmorg-sql-bi-ods-p01.database.windows.net'
ods_database = 'db-sfods-prod01'
ods_schema = 'CUBE'

# In[]:
    
### EXTRACT ###
# Data for the planned giving score comes from donation history, gift types, campaign participation, volunteering,
# family foundation status, and more. SQL queries for all parts of the score are below

db_engine = get_ods_conn(ods_server, ods_database)

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

# remove all "ods_table"?
contacts = get_ods_dataframe(db_engine, contact_sql)
accounts = get_ods_dataframe(db_engine, accounts_sql)
board_members = get_ods_dataframe(db_engine, board_members_sql)
programs = get_ods_dataframe(db_engine, programs_sql)
daf = get_ods_dataframe(db_engine, daf_sql)
stock = get_ods_dataframe(db_engine, stock_sql)
planned_gift = get_ods_dataframe(db_engine, planned_gift_sql)
programs = get_ods_dataframe(db_engine, programs_sql)
survey_response = get_ods_dataframe(db_engine, survey_response_sql)
business_owner = get_ods_dataframe(db_engine, business_owner_sql)

# Minor formatting for easier reading
volunteer = programs[programs['Name'].str.contains('Volunteer')]
alumni = contacts[contacts['Alumni_IJM__c'] == 'Yes']
accounts = accounts.rename(columns={'Id':'AccountId'})
planned_gift = planned_gift.rename(columns={'AccountID':'AccountId'})
daf = daf.rename(columns={'AccountID':'AccountId'})
stock = stock.rename(columns={'AccountID':'AccountId'})

# In[]:

### TRANSFORM ###
# Points for each part of the planned giving score are calculated below.
# The accounts df is the only df that contains more than one component of the final score.

# Assign points for donor status. Non-donors get 0 points.
accounts['Rolling Status Points'] = accounts['Rolling_Status__c'].map({'Active': 5, 'Lapsed': 3}).fillna(0).astype(int)

# Assign points for freedom partner status. Non-active FPs get 1 point, non-FPs get 0 points.
accounts['FP Status Points'] = np.where(accounts['Freedom_Partner_Status__c'] == 'Active', 5, np.where(pd.notna(accounts['Freedom_Partner_Status__c']), 1, 0))


# Assign points based on years of consecutive giving.
consec_giving_conditions = [
    (accounts['Number_of_Years_Consecutively_Giving__c'] >= 8) & (accounts['Number_of_Years_Consecutively_Giving__c'] <= 10), # 15 pts if you've given between 8-10 years
    (accounts['Number_of_Years_Consecutively_Giving__c'] >= 11) & (accounts['Number_of_Years_Consecutively_Giving__c'] <= 14), # 20 pts if you've given between 11-14 years
    (accounts['Number_of_Years_Consecutively_Giving__c'] >= 15)] # 25 pts if you've given 15+ years

consec_giving_values = [15, 20, 25]

accounts['Consecutive_Giving_Points'] = np.select(consec_giving_conditions, consec_giving_values, default=0) # 0 pts if you've given for 8 years or less


# Assign points based on how recent your most recent gift is. One point is given for each year you've given in the last five years.
# This is an interim score to calculate the actual score at line 96
accounts['Recent Giving Calc'] = (accounts[['npo02__OppsClosedThisYear__c', 
                                            'npo02__OppsClosedLastYear__c', 
                                            'npo02__OppsClosed2YearsAgo__c', 
                                            'Number_of_Gifts_3_Years_Ago__c', 
                                            'Number_of_Gifts_4_Years_Ago__c', 
                                            'Number_of_Gifts_5_Years_Ago__c']] >= 1).sum(axis=1)
# If you've given in at least 3 of the last 5 years, you get 5 points, else 0 points.
accounts['Recent Giving Points'] = np.where(accounts['Recent Giving Calc']>=3,5,0)


# Bonus points if donor gave within the first 7 years of IJM
accounts['Early Donor Points'] = np.where(accounts['npo02__FirstCloseDate__c'] <= pd.to_datetime('2004-12-31'), 5, 0)

# Having a family foundation is a negative for the planned giving score since their family foundation will receive their inheritance
accounts['Family Foundation Points'] = np.where(accounts['Family_Foundation__c'].isnull(),0,-15)


# Years_Donated__c is an open text field that gets automatically filled in by Salesforce. Writes the actual year and appends for each year (e.g., 2023, 2024)
# This counts the length of the text field to count the number of calendar years listed. 
accounts['Years_Donated__c'] = accounts['Years_Donated__c'].str.len()/5 + .2 # format Years_Donated__c field before applying calculations
# Count the total number of calendar years you've given to IJM. 10+ years = 13 points, 8+ years = 10 points, else 0 points
accounts['Giving Years Points'] = np.where(accounts['Years_Donated__c'] >= 10, 13, np.where(accounts['Years_Donated__c'] >= 8, 10, 0))


# Board members get bonus points
board_members['Board Member Points']=5
board_members = board_members.drop_duplicates(subset=['AccountId'], keep='first')
board_members = board_members.drop(['npe5__Contact__c','Id'], axis=1)


# alumni == former IJM employees
alumni = alumni.drop_duplicates(subset=['AccountId'], keep='first')
alumni['Alumni Points']=5
alumni = alumni[['AccountId','Alumni Points']]

# Bonus points if donor already has a cultivated planned gift on their record
planned_gift['Planned Gifts Points']=25

# Points for giving stock gifts
stock['Stock Gifts Points']=7

# Points for giving a DAF gift
daf = daf.drop_duplicates(subset=['AccountId'], keep='first') # pulled from Opportunity object, only credit donor once for a DAF gift
daf['DAF Points']=10

# Points given depending on how donors answered the "would you consider leaving a planned gift" question from a past survey
survey_response = survey_response.drop_duplicates(subset=['AccountId'], keep='first')
survey_response['Consider PG Points']=np.where(survey_response['question_14_Would_you_consider_leaving__c'].str.contains('have'), 20, 10)
survey_response = survey_response.drop(['question_14_Would_you_consider_leaving__c'], axis=1)

# Points for being a known business owner
business_owner = business_owner.drop_duplicates(subset=['AccountId'], keep='first')
business_owner['Business Owner Points']=10

# Points for being a volunteer
volunteer = volunteer.drop_duplicates(subset=['AccountId'], keep='first')
volunteer['Volunteer Leader Points']=5
volunteer = volunteer[['AccountId','Volunteer Leader Points']]

# Merge all dataframes into one at the account level
accounts_final=pd.merge(accounts, alumni, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, board_members, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, daf, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, planned_gift, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, stock, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, survey_response, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, business_owner, on='AccountId', how='left')
accounts_final=pd.merge(accounts_final, volunteer, on='AccountId', how='left')

# Fill blanks because not all account ids will show in each individual dataframe from above
accounts_final=accounts_final.replace(np.nan,0)

# All points value colums are added up
accounts_final['Planned Giving Score']=(
    accounts_final['Rolling Status Points']
    +accounts_final['FP Status Points']
    +accounts_final['Cosecutive_Giving_Points']
    +accounts_final['Recent Giving Points']
    +accounts_final['Early Donor Points']
    +accounts_final['Family Foundation Points']
    +accounts_final['Giving Years Points']
    +accounts_final['Board Member Points']
    +accounts_final['Alumni Points']
    +accounts_final['Planned Gifts Points']
    +accounts_final['Stock Gifts Points']
    +accounts_final['DAF Points']
)

# Limit dataframe to the relevant columns
accounts_final=accounts_final[['AccountId','Planned Giving Score']]

### EXPORT ###
accounts_final.to_csv('Planned_Giving_Score.csv', index=False)