-- planned_gift_sql for planned giving
SELECT DISTINCT AccountID 
FROM NAM.VWOpportunity 
WHERE
    Type = 'Planned Gift' 
    AND Amount > 0 
    AND StageName NOT IN ('Withdrawn', 'Closed Lost', 'Closed Won', 'Pledge Completed', 'Contractual Posted','refunded', 'voided', 'To Be Deleted')