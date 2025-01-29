-- daf_sql for planned giving
SELECT DISTINCT AccountID 
FROM NAM.VWOpportunity 
WHERE Type = 'Donor Advised Fund'