-- programs_sql for planned giving

-- CTE Option
WITH a AS (
    SELECT 
        m.Contact__c, 
        m.Role__c, 
        m.Start_Date__c, 
        m.End_Date__c, 
        m.Program__c, 
        p.Name, 
        p.Type__c, 
        p.RecordTypeId, 
        p.Id AS Program_Id
    FROM NAM.VWMember__c AS m 
    LEFT JOIN CNF.VWPrograms__c AS p ON (m.Program__c = p.Id)
)
SELECT 
    a.Contact__c, 
    a.Role__c, 
    a.Start_Date__c, 
    a.End_Date__c, 
    a.Program__c, 
    a.Name, 
    a.Type__c, 
    a.RecordTypeId, 
    a.Program_Id, 
    c.Id, 
    c.AccountId
    
FROM a
LEFT JOIN NAM.VWContact AS c ON (a.Contact__c = c.Id)


-- Original w/ subquery
/*
SELECT 
    m.Contact__c, 
    m.Role__c, 
    m.Start_Date__c, 
    m.End_Date__c, 
    m.Program__c, 
    m.Name, 
    m.Type__c, 
    m.RecordTypeId, 
    m.Program_Id, 
    c.Id, 
    c.AccountId
    
FROM (
    SELECT 
        NAM.VWMember__c.Contact__c, 
        NAM.VWMember__c.Role__c, 
        NAM.VWMember__c.Start_Date__c, 
        NAM.VWMember__c.End_Date__c, 
        NAM.VWMember__c.Program__c, 
        CNF.VWPrograms__c.Name, 
        CNF.VWPrograms__c.Type__c, 
        CNF.VWPrograms__c.RecordTypeId, 
        CNF.VWPrograms__c.Id AS Program_Id
    FROM NAM.VWMember__c LEFT JOIN CNF.VWPrograms__c ON NAM.VWMember__c.Program__c = CNF.VWPrograms__c.Id
    ) AS m 

LEFT JOIN NAM.VWContact AS c ON m.Contact__c = c.Id
*/