-- accounts_sql for planned giving
SELECT 
    Id, 
    Years_Donated__c, 
    npo02__FirstCloseDate__c, 
    npo02__LastCloseDate__c, 
    npo02__NumberOfClosedOpps__c, 
    Number_of_Years_Consecutively_Giving__c, 
    npo02__OppsClosedThisYear__c, 
    npo02__OppsClosedLastYear__c, 
    npo02__OppsClosed2YearsAgo__c, 
    Number_of_Gifts_3_Years_Ago__c, 
    Number_of_Gifts_4_Years_Ago__c, 
    Number_of_Gifts_5_Years_Ago__c, 
    PFO_Partnership_Status__c, 
    Family_Foundation__c, 
    [Type], 
    Rolling_Status__c, 
    Freedom_Partner_Status__c,
    BillingStreet,
    BillingCity,
    BillingStateCode,
    BillingPostalCode,
    BillingCountryCode

FROM NAM.VWAccount 

WHERE 
    [Type] = 'Household' 
    AND BillingCountryCode IN ('US', 'CA')