-- survey_response_sql for planned giving
SELECT 
    a.Contact__c, 
    a.Id AS SubmissionId, 
    a.question_14_Would_you_consider_leaving__c, 
    b.AccountId, 
    b.Id AS ContactId

FROM NAM.VWForm_Submissions__c AS a 
LEFT JOIN NAM.VWContact AS b ON (a.Contact__c = b.Id)

WHERE 
    a.Contact__c IS NOT NULL 
    AND (a.question_14_Would_you_consider_leaving__c LIKE '%have%' OR a.question_14_Would_you_consider_leaving__c LIKE '%consider%') 
    AND a.question_14_Would_you_consider_leaving__c NOT LIKE '%not%'