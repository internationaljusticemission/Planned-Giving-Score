-- board_members_sql for planned giving

-- CTE Option
WITH a AS (
    SELECT npe5__Contact__c
    FROM CNF.VWnpe5__Affiliation__c
    WHERE 
        npe5__Organization__c = '001o00000110h2CAAQ' 
        AND RecordTypeId = '012o0000000u0knAAA' -- IJM Board/Staff Affiliations
        AND Title__c = 'Board Member' 
        AND npe5__Status__c = 'Former'
)

SELECT 
    a.npe5__Contact__c, 
    c.Id, 
    c.AccountId
FROM a
LEFT JOIN NAM.VWContact AS c ON (a.npe5__Contact__c = c.Id)



-- Original w/ subquery
/*
SELECT 
    a.npe5__Contact__c, 
    c.Id, 
    c.AccountId

FROM (
    SELECT npe5__Contact__c 
    FROM [CNF].[VWnpe5__Affiliation__c] 
    WHERE 
        [npe5__Organization__c] = '001o00000110h2CAAQ' 
        AND [RecordTypeId] = '012o0000000u0knAAA'  -- IJM Board/Staff Affiliations
        AND [Title__c] = 'Board Member' 
        AND [npe5__Status__c] = 'Former'
    ) AS a

LEFT JOIN [NAM].[VWContact] AS c ON a.npe5__Contact__c = c.Id
*/