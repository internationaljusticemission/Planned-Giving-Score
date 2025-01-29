-- business_owner_sql for planned giving
SELECT AccountId 
FROM NAM.VWContact 
WHERE 
    Business_Owner__c NOT IN ('Unknown') 
    AND Business_Owner__c IS NOT NULL