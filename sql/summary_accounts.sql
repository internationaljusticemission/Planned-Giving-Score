-- Limit to accounts who are donors within the last 10 years (to match Cube definitions) for appending archived planned giving scores
SELECT DISTINCT account_id
FROM CUBE.summary_accounts
WHERE last_gift_date >= DATEADD(YEAR, -10, EOMONTH(GETDATE(), -1));